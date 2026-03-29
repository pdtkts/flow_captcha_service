from __future__ import annotations

import json
import threading
import unittest
from contextlib import contextmanager
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src.http_bridge import HttpBridgeServer, sanitize_header_items


class _EchoHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(content_length) if content_length > 0 else b""
        self.server.captured_request = {
            "headers": {name.lower(): value for name, value in self.headers.items()},
            "body_text": body.decode("utf-8"),
            "path": self.path,
        }
        payload = json.dumps({"ok": True, "body_text": body.decode("utf-8")}).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args):
        return


@contextmanager
def _running_server(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


class HttpBridgeTests(unittest.TestCase):
    def test_sanitize_header_items_strips_h2c_upgrade_headers(self):
        sanitized = sanitize_header_items(
            [
                ("Host", "127.0.0.1:8060"),
                ("Content-Type", "application/json"),
                ("Connection", "Upgrade, HTTP2-Settings"),
                ("Upgrade", "h2c"),
                ("HTTP2-Settings", "AAMAAABkAARAAAAAAAIAAAAA"),
                ("Keep-Alive", "timeout=5"),
            ]
        )

        self.assertEqual(
            sanitized,
            [
                ("Host", "127.0.0.1:8060"),
                ("Content-Type", "application/json"),
            ],
        )

    def test_bridge_forwards_json_body_after_stripping_upgrade_headers(self):
        upstream = ThreadingHTTPServer(("127.0.0.1", 0), _EchoHandler)
        upstream.captured_request = None

        with _running_server(upstream):
            bridge = HttpBridgeServer(
                ("127.0.0.1", 0),
                target_host="127.0.0.1",
                target_port=upstream.server_port,
            )
            with _running_server(bridge):
                request_body = b'{"clientKey":"demo","task":{"type":"RecaptchaV3TaskProxyless"}}'
                connection = HTTPConnection("127.0.0.1", bridge.server_port, timeout=10)
                try:
                    connection.request(
                        "POST",
                        "/createTask?demo=1",
                        body=request_body,
                        headers={
                            "Host": "127.0.0.1:8060",
                            "Content-Type": "application/json",
                            "Upgrade": "h2c",
                            "Connection": "Upgrade, HTTP2-Settings",
                            "HTTP2-Settings": "AAMAAABkAARAAAAAAAIAAAAA",
                        },
                    )
                    response = connection.getresponse()
                    payload = json.loads(response.read().decode("utf-8"))
                finally:
                    connection.close()

        self.assertEqual(response.status, 200)
        self.assertEqual(payload["body_text"], request_body.decode("utf-8"))
        self.assertIsNotNone(upstream.captured_request)
        self.assertEqual(upstream.captured_request["body_text"], request_body.decode("utf-8"))
        self.assertEqual(upstream.captured_request["path"], "/createTask?demo=1")
        self.assertEqual(upstream.captured_request["headers"].get("connection"), "close")
        self.assertNotIn("upgrade", upstream.captured_request["headers"])
        self.assertNotIn("http2-settings", upstream.captured_request["headers"])

    def test_bridge_rebuilds_chunked_request_body_for_upstream(self):
        upstream = ThreadingHTTPServer(("127.0.0.1", 0), _EchoHandler)
        upstream.captured_request = None

        with _running_server(upstream):
            bridge = HttpBridgeServer(
                ("127.0.0.1", 0),
                target_host="127.0.0.1",
                target_port=upstream.server_port,
            )
            with _running_server(bridge):
                request_body = b'{"clientKey":"demo","mode":"chunked"}'
                connection = HTTPConnection("127.0.0.1", bridge.server_port, timeout=10)
                try:
                    connection.request(
                        "POST",
                        "/getBalance",
                        body=[request_body],
                        headers={
                            "Host": "127.0.0.1:8060",
                            "Content-Type": "application/json",
                            "Transfer-Encoding": "chunked",
                        },
                        encode_chunked=True,
                    )
                    response = connection.getresponse()
                    payload = json.loads(response.read().decode("utf-8"))
                finally:
                    connection.close()

        self.assertEqual(response.status, 200)
        self.assertEqual(payload["body_text"], request_body.decode("utf-8"))
        self.assertIsNotNone(upstream.captured_request)
        self.assertEqual(upstream.captured_request["body_text"], request_body.decode("utf-8"))
        self.assertNotIn("transfer-encoding", upstream.captured_request["headers"])


if __name__ == "__main__":
    unittest.main()
