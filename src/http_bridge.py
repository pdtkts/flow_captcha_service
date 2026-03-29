from __future__ import annotations

import socket
import threading
import time
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterable, List, Tuple

import uvicorn
from fastapi import FastAPI

from .core.logger import debug_logger


_HOP_BY_HOP_HEADERS = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
    "http2-settings",
}


def _connection_tokens(value: str) -> set[str]:
    tokens: set[str] = set()
    for raw_token in str(value or "").split(","):
        token = raw_token.strip().lower()
        if token:
            tokens.add(token)
    return tokens


def sanitize_header_items(header_items: Iterable[Tuple[str, str]]) -> List[Tuple[str, str]]:
    items = [(str(name), str(value)) for name, value in header_items]
    blocked = set(_HOP_BY_HOP_HEADERS)
    for name, value in items:
        if name.lower() == "connection":
            blocked.update(_connection_tokens(value))

    sanitized: List[Tuple[str, str]] = []
    for name, value in items:
        if name.lower() in blocked:
            continue
        sanitized.append((name, value))
    return sanitized


def _append_forwarded_header(headers: List[Tuple[str, str]], name: str, value: str):
    if not value:
        return
    for index, (header_name, header_value) in enumerate(headers):
        if header_name.lower() != name.lower():
            continue
        merged = f"{header_value}, {value}".strip(", ")
        headers[index] = (header_name, merged)
        return
    headers.append((name, value))


def _with_forwarding_headers(
    header_items: Iterable[Tuple[str, str]],
    *,
    client_ip: str,
    forwarded_proto: str,
    forwarded_host: str,
    forwarded_port: int,
) -> List[Tuple[str, str]]:
    headers = sanitize_header_items(header_items)
    _append_forwarded_header(headers, "X-Forwarded-For", client_ip)
    _append_forwarded_header(headers, "X-Forwarded-Proto", forwarded_proto)
    _append_forwarded_header(headers, "X-Forwarded-Host", forwarded_host)
    _append_forwarded_header(headers, "X-Forwarded-Port", str(forwarded_port))
    _append_forwarded_header(headers, "Connection", "close")
    return headers


def _choose_internal_port(public_port: int) -> int:
    candidates = [port for port in range(public_port + 1, min(65536, public_port + 33))]
    candidates.append(0)
    for candidate in candidates:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                sock.bind(("127.0.0.1", candidate))
            except OSError:
                continue
            return int(sock.getsockname()[1])
    raise RuntimeError("无法为内部 uvicorn 分配可用端口")


class HttpBridgeServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self,
        server_address: tuple[str, int],
        *,
        target_host: str,
        target_port: int,
        upstream_timeout_seconds: float = 30.0,
    ):
        super().__init__(server_address, _BridgeRequestHandler)
        self.target_host = target_host
        self.target_port = int(target_port)
        self.upstream_timeout_seconds = float(upstream_timeout_seconds)


class _BridgeRequestHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "flow-captcha-bridge/1.0"

    def do_GET(self):
        self._proxy_request()

    def do_POST(self):
        self._proxy_request()

    def do_PUT(self):
        self._proxy_request()

    def do_PATCH(self):
        self._proxy_request()

    def do_DELETE(self):
        self._proxy_request()

    def do_OPTIONS(self):
        self._proxy_request()

    def do_HEAD(self):
        self._proxy_request()

    def _read_request_body(self) -> bytes:
        transfer_encoding = str(self.headers.get("Transfer-Encoding") or "").lower()
        if "chunked" in transfer_encoding:
            return self._read_chunked_request_body()

        content_length = self.headers.get("Content-Length")
        if content_length is None:
            return b""
        try:
            size = max(0, int(content_length))
        except (TypeError, ValueError):
            size = 0
        if size <= 0:
            return b""
        return self.rfile.read(size)

    def _read_chunked_request_body(self) -> bytes:
        chunks: list[bytes] = []
        while True:
            size_line = self.rfile.readline()
            if not size_line:
                raise ValueError("chunked body 缺少 chunk size")
            chunk_size_text = size_line.split(b";", 1)[0].strip()
            try:
                chunk_size = int(chunk_size_text, 16)
            except ValueError as exc:
                raise ValueError(f"非法 chunk size: {chunk_size_text!r}") from exc

            if chunk_size == 0:
                while True:
                    trailer_line = self.rfile.readline()
                    if trailer_line in {b"", b"\r\n", b"\n"}:
                        break
                break

            chunk = self.rfile.read(chunk_size)
            if len(chunk) != chunk_size:
                raise ValueError("chunked body 长度不足")
            chunks.append(chunk)

            chunk_end = self.rfile.readline()
            if chunk_end not in {b"\r\n", b"\n"}:
                raise ValueError("chunked body 缺少 chunk terminator")

        return b"".join(chunks)

    def _proxy_request(self):
        try:
            request_body = self._read_request_body()
        except ValueError as exc:
            debug_logger.log_warning(f"[http_bridge] bad request body method={self.command} path={self.path}: {exc}")
            self._write_client_error_response(str(exc) or "bad request body")
            return

        forwarded_headers = _with_forwarding_headers(
            self.headers.items(),
            client_ip=str(self.client_address[0] or ""),
            forwarded_proto="http",
            forwarded_host=str(self.headers.get("Host") or ""),
            forwarded_port=int(self.server.server_port),
        )

        connection = HTTPConnection(
            self.server.target_host,
            self.server.target_port,
            timeout=self.server.upstream_timeout_seconds,
        )
        try:
            connection.request(
                self.command,
                self.path,
                body=request_body if request_body else None,
                headers={name: value for name, value in forwarded_headers},
            )
            upstream_response = connection.getresponse()
            response_body = upstream_response.read()
            response_headers = sanitize_header_items(upstream_response.getheaders())
            status = upstream_response.status
            reason = upstream_response.reason
        except Exception as exc:
            debug_logger.log_warning(f"[http_bridge] upstream request failed method={self.command} path={self.path}: {exc}")
            self._write_error_response()
            return
        finally:
            connection.close()

        self.send_response_only(status, reason)
        for name, value in response_headers:
            lower_name = name.lower()
            if lower_name in {"content-length", "server", "date"}:
                continue
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(response_body)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(response_body)

    def _write_error_response(self):
        payload = b'{"detail":"bridge upstream error"}'
        self.send_response_only(502, "Bad Gateway")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def _write_client_error_response(self, detail: str):
        payload = ('{"detail":"' + str(detail).replace('"', '\\"') + '"}').encode("utf-8")
        self.send_response_only(400, "Bad Request")
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(payload)

    def log_message(self, format: str, *args):
        return


class BridgedUvicornRunner:
    def __init__(
        self,
        app: FastAPI,
        *,
        public_host: str,
        public_port: int,
        log_level: str = "info",
        internal_host: str = "127.0.0.1",
        internal_port: int | None = None,
        startup_timeout_seconds: float = 15.0,
        shutdown_timeout_seconds: float = 30.0,
        monitor_interval_seconds: float = 0.5,
    ):
        self._startup_timeout_seconds = float(startup_timeout_seconds)
        self._shutdown_timeout_seconds = float(shutdown_timeout_seconds)
        self._monitor_interval_seconds = max(0.1, float(monitor_interval_seconds))
        self._uvicorn_server = uvicorn.Server(
            uvicorn.Config(
                app,
                host=str(internal_host),
                port=int(internal_port or _choose_internal_port(int(public_port))),
                reload=False,
                log_level=str(log_level or "info").lower(),
            )
        )
        self._uvicorn_thread = threading.Thread(
            target=self._uvicorn_server.run,
            name="internal-uvicorn",
        )
        self._bridge_server = HttpBridgeServer(
            (str(public_host), int(public_port)),
            target_host=str(internal_host),
            target_port=int(self._uvicorn_server.config.port),
        )
        self._bridge_thread = threading.Thread(
            target=self._bridge_server.serve_forever,
            name="http-bridge",
        )

    def start(self):
        self._uvicorn_thread.start()
        deadline = time.monotonic() + self._startup_timeout_seconds
        while time.monotonic() < deadline:
            if getattr(self._uvicorn_server, "started", False):
                return
            if not self._uvicorn_thread.is_alive():
                break
            time.sleep(0.05)
        raise RuntimeError(
            f"内部 uvicorn 启动失败 host={self._uvicorn_server.config.host} port={self._uvicorn_server.config.port}"
        )

    def serve_forever(self):
        debug_logger.log_info(
            f"[http_bridge] public={self._bridge_server.server_address[0]}:{self._bridge_server.server_address[1]} "
            f"-> internal={self._uvicorn_server.config.host}:{self._uvicorn_server.config.port}"
        )
        self._bridge_thread.start()
        while self._bridge_thread.is_alive():
            if not self._uvicorn_thread.is_alive():
                raise RuntimeError(
                    f"内部 uvicorn 已退出 host={self._uvicorn_server.config.host} port={self._uvicorn_server.config.port}"
                )
            time.sleep(self._monitor_interval_seconds)

    def close(self):
        try:
            self._bridge_server.shutdown()
        except Exception:
            pass
        try:
            self._bridge_server.server_close()
        except Exception:
            pass
        if self._bridge_thread.is_alive():
            self._bridge_thread.join(timeout=max(1.0, self._shutdown_timeout_seconds))

        self._uvicorn_server.should_exit = True
        if self._uvicorn_thread.is_alive():
            self._uvicorn_thread.join(timeout=max(1.0, self._shutdown_timeout_seconds))


def run_bridged_app(
    app: FastAPI,
    *,
    public_host: str,
    public_port: int,
    log_level: str = "info",
):
    runner = BridgedUvicornRunner(
        app,
        public_host=public_host,
        public_port=public_port,
        log_level=log_level,
    )
    try:
        runner.start()
        runner.serve_forever()
    finally:
        runner.close()
