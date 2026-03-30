from __future__ import annotations

import asyncio
import hashlib
import json
import re
import ssl
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple
import urllib.error
import urllib.parse
import urllib.request

from ..core.config import config
from ..core.database import Database
from ..core.diagnostics import diag_label
from ..core.logger import debug_logger


_HTTP_STATUS_RE = re.compile(r"\bhttp\s*([1-5]\d{2})\b", re.IGNORECASE)


class ClusterManager:
    def __init__(self, db: Database, runtime):
        self.db = db
        self.runtime = runtime
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._subnode_registered = False
        self._dispatch_cursor = 0
        self._dispatch_lock = asyncio.Lock()
        # 记录短时调度预留槽位，用于覆盖“心跳上报滞后”窗口，避免瞬时超发。
        self._dispatch_reservations: Dict[int, List[float]] = {}
        # 记录 bucket -> node 的短期调度亲和，尽量把同类请求稳定发往同一子节点。
        self._dispatch_bucket_affinity: Dict[str, int] = {}
        self._dispatch_bucket_last_used: Dict[str, float] = {}
        self._node_standby_bucket_signatures: Dict[int, set[str]] = {}
        # 主节点本地维护的“已派发但未结束”路由会话，用于幂等回收 active_sessions。
        self._active_routed_sessions: Dict[str, int] = {}
        self._completed_routed_sessions: Dict[str, float] = {}
        self._routed_sessions_lock = asyncio.Lock()

    async def start(self):
        if config.cluster_role == "subnode":
            if self._heartbeat_task is None or self._heartbeat_task.done():
                self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def close(self):
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        self._subnode_registered = False
        async with self._dispatch_lock:
            self._node_standby_bucket_signatures.clear()

    @staticmethod
    def _dispatch_poll_interval_seconds() -> float:
        return 0.35

    def _dispatch_bucket_affinity_ttl_seconds(self) -> float:
        stale_seconds = max(10, int(config.cluster_master_node_stale_seconds))
        return float(max(300, stale_seconds * 6))

    @staticmethod
    def _normalize_dispatch_bucket_key(bucket_key: Optional[str]) -> str:
        return str(bucket_key or "").strip()

    @staticmethod
    def _is_non_retryable_dispatch_error(error: Exception) -> bool:
        text = str(error or "").strip().lower()
        if not text:
            return False

        fatal_markers = (
            "certificate verify failed",
            "hostname mismatch",
            "wrong version number",
            "tlsv1 alert",
            "sslv3 alert",
            "[ssl:",
            " ssl:",
            " tls:",
            "ssl eof",
            "tls handshake",
        )
        return any(marker in text for marker in fatal_markers)

    @staticmethod
    def _should_retry_without_tls_verify(url: str, error: Exception) -> bool:
        parsed = urllib.parse.urlparse(str(url or "").strip())
        if parsed.scheme.lower() != "https":
            return False
        return ClusterManager._is_non_retryable_dispatch_error(error)

    @staticmethod
    def _normalize_solve_action(action: Any) -> str:
        return str(action or "IMAGE_GENERATION").strip().upper() or "IMAGE_GENERATION"

    def _build_solve_dispatch_bucket_key(self, request_payload: Dict[str, Any]) -> str:
        payload = request_payload or {}
        project_id = str(payload.get("project_id") or "").strip()
        action = self._normalize_solve_action(payload.get("action"))
        token_id = int(payload.get("token_id") or 0)
        token_signature = f"token:{token_id}" if token_id > 0 else "token:default"
        return f"solve|{project_id}|{action}|{token_signature}"

    def _build_custom_dispatch_bucket_key(self, request_payload: Dict[str, Any], *, prefix: str = "custom") -> str:
        payload = request_payload or {}
        joined = "\n".join(
            [
                str(payload.get("website_url") or "").strip(),
                str(payload.get("website_key") or "").strip(),
                str(payload.get("action") or "homepage").strip(),
                str(payload.get("captcha_type") or "recaptcha_v3").strip().lower(),
                "1" if bool(payload.get("enterprise")) else "0",
                "1" if bool(payload.get("is_invisible", True)) else "0",
            ]
        )
        digest = hashlib.sha1(joined.encode("utf-8")).hexdigest()[:24]
        return f"{prefix}|{digest}"

    def _build_solve_standby_signature(self, request_payload: Dict[str, Any]) -> str:
        payload = request_payload or {}
        project_id = str(payload.get("project_id") or "").strip()
        action = self._normalize_solve_action(payload.get("action"))
        return f"native|{project_id}|{action}"

    @staticmethod
    def _normalize_bucket_signatures(values: Any) -> set[str]:
        normalized: set[str] = set()
        if not isinstance(values, list):
            return normalized
        for value in values:
            text = str(value or "").strip()
            if text:
                normalized.add(text)
        return normalized

    def _prune_dispatch_bucket_affinity_locked(self, *, valid_node_ids: Optional[set[int]] = None):
        if not self._dispatch_bucket_affinity:
            return

        now_ts = time.time()
        expire_before = now_ts - self._dispatch_bucket_affinity_ttl_seconds()
        stale_keys = [
            bucket_key
            for bucket_key, last_used in self._dispatch_bucket_last_used.items()
            if float(last_used or 0.0) < expire_before
        ]
        for bucket_key in stale_keys:
            self._dispatch_bucket_affinity.pop(bucket_key, None)
            self._dispatch_bucket_last_used.pop(bucket_key, None)

        if valid_node_ids is None:
            return

        invalid_keys = [
            bucket_key
            for bucket_key, node_id in self._dispatch_bucket_affinity.items()
            if int(node_id or 0) not in valid_node_ids
        ]
        for bucket_key in invalid_keys:
            self._dispatch_bucket_affinity.pop(bucket_key, None)
            self._dispatch_bucket_last_used.pop(bucket_key, None)

    async def _mark_bucket_affinity(self, bucket_key: Optional[str], node_id: int):
        normalized = self._normalize_dispatch_bucket_key(bucket_key)
        if not normalized or node_id <= 0:
            return

        async with self._dispatch_lock:
            self._prune_dispatch_bucket_affinity_locked()
            self._dispatch_bucket_affinity[normalized] = int(node_id)
            self._dispatch_bucket_last_used[normalized] = time.time()

    async def _clear_bucket_affinity(self, bucket_key: Optional[str], *, expected_node_id: int = 0):
        normalized = self._normalize_dispatch_bucket_key(bucket_key)
        if not normalized:
            return

        async with self._dispatch_lock:
            current_node_id = int(self._dispatch_bucket_affinity.get(normalized, 0) or 0)
            if expected_node_id > 0 and current_node_id not in {0, int(expected_node_id)}:
                return
            self._dispatch_bucket_affinity.pop(normalized, None)
            self._dispatch_bucket_last_used.pop(normalized, None)

    async def dispatch_solve(self, request_payload: Dict[str, Any]) -> Dict[str, Any]:
        last_error = ""
        bucket_key = self._build_solve_dispatch_bucket_key(request_payload)
        bucket_signature = self._build_solve_standby_signature(request_payload)

        while True:
            nodes = await self._select_candidate_nodes(bucket_key=bucket_key, bucket_signature=bucket_signature)
            if not nodes:
                await asyncio.sleep(self._dispatch_poll_interval_seconds())
                continue

            dispatched_this_round = False
            fatal_error: Optional[Exception] = None
            for node in nodes:
                reserved = await self._try_reserve_dispatch_slot(node)
                if not reserved:
                    continue

                dispatched_this_round = True
                node_id = int(node.get("id") or 0)
                try:
                    result = await self._post_to_node(
                        node=node,
                        path="/api/v1/solve",
                        json_payload=request_payload,
                        timeout=config.cluster_master_dispatch_timeout_seconds,
                    )
                    child_session = str(result.get("session_id") or "").strip()
                    token = str(result.get("token") or "").strip()
                    if not child_session or not token:
                        raise RuntimeError("子节点响应缺少 session_id/token")

                    routed_session_id = f"{node['id']}:{child_session}"
                    # solve 成功后，立即释放临时预留并同步 active_sessions，避免长时间“虚占容量”。
                    await self._release_dispatch_slot(node_id)
                    tracked = await self._mark_dispatch_session_started(routed_session_id, node_id)
                    if tracked:
                        try:
                            await self.db.adjust_cluster_node_sessions(node_id, active_delta=1)
                        except Exception as e:
                            debug_logger.log_warning(
                                f"[ClusterManager] adjust active_sessions(+1) failed node={node.get('node_name')}: {e}"
                            )
                    await self._mark_bucket_affinity(bucket_key, node_id)

                    result["session_id"] = routed_session_id
                    result["node_name"] = node["node_name"]
                    return result
                except Exception as e:
                    await self._release_dispatch_slot(node_id)
                    await self._clear_bucket_affinity(bucket_key, expected_node_id=node_id)
                    last_error = str(e)
                    if fatal_error is None and self._is_non_retryable_dispatch_error(e):
                        fatal_error = e
                    await self.db.mark_cluster_node_error(int(node["id"]), last_error, error_type="dispatch")
                    debug_logger.log_warning(
                        f"[ClusterManager] dispatch solve node={node['node_name']} {diag_label(e)} failed: {last_error}"
                    )

            if fatal_error is not None:
                raise RuntimeError(f"子节点调度失败: {fatal_error}") from fatal_error
            if dispatched_this_round and last_error:
                debug_logger.log_warning(
                    f"[ClusterManager] dispatch solve round failed, will retry: {last_error}"
                )
            await asyncio.sleep(self._dispatch_poll_interval_seconds())

    async def dispatch_finish(self, routed_session_id: str, status: str) -> Dict[str, Any]:
        node, child_session = await self._resolve_routed_session(routed_session_id)
        payload = {"status": status}
        try:
            result = await self._post_to_node(
                node=node,
                path=f"/api/v1/sessions/{child_session}/finish",
                json_payload=payload,
                timeout=20,
            )
        except Exception as e:
            debug_logger.log_warning(
                "[ClusterManager] dispatch finish failed "
                f"session_id={routed_session_id} node={node.get('node_name')} "
                f"status={status} {diag_label(e)}: {e}"
            )
            raise
        node_id = int(node.get("id") or 0)
        adjust_node_id = await self._mark_dispatch_session_finished(routed_session_id, fallback_node_id=node_id)
        if adjust_node_id:
            try:
                await self.db.adjust_cluster_node_sessions(adjust_node_id, active_delta=-1)
            except Exception as e:
                debug_logger.log_warning(
                    f"[ClusterManager] adjust active_sessions(-1) failed node={node.get('node_name')}: {e}"
                )
        return result

    async def dispatch_error(self, routed_session_id: str, error_reason: str) -> Dict[str, Any]:
        node, child_session = await self._resolve_routed_session(routed_session_id)
        payload = {"error_reason": error_reason}
        try:
            result = await self._post_to_node(
                node=node,
                path=f"/api/v1/sessions/{child_session}/error",
                json_payload=payload,
                timeout=20,
            )
        except Exception as e:
            debug_logger.log_warning(
                "[ClusterManager] dispatch error failed "
                f"session_id={routed_session_id} node={node.get('node_name')} "
                f"error_reason={error_reason} {diag_label(e)}: {e}"
            )
            raise
        node_id = int(node.get("id") or 0)
        adjust_node_id = await self._mark_dispatch_session_finished(routed_session_id, fallback_node_id=node_id)
        if adjust_node_id:
            try:
                await self.db.adjust_cluster_node_sessions(adjust_node_id, active_delta=-1)
            except Exception as e:
                debug_logger.log_warning(
                    f"[ClusterManager] adjust active_sessions(-1) failed node={node.get('node_name')}: {e}"
                )
        return result

    async def dispatch_custom_score(self, request_payload: Dict[str, Any]) -> Dict[str, Any]:
        last_error = ""

        while True:
            nodes = await self._select_candidate_nodes()
            if not nodes:
                await asyncio.sleep(self._dispatch_poll_interval_seconds())
                continue

            dispatched_this_round = False
            fatal_error: Optional[Exception] = None
            for node in nodes:
                reserved = await self._try_reserve_dispatch_slot(node)
                if not reserved:
                    continue

                dispatched_this_round = True
                node_id = int(node.get("id") or 0)
                try:
                    result = await self._post_to_node(
                        node=node,
                        path="/api/v1/custom-score",
                        json_payload=request_payload,
                        timeout=config.cluster_master_dispatch_timeout_seconds,
                    )
                    await self._release_dispatch_slot(node_id)
                    return result
                except Exception as e:
                    await self._release_dispatch_slot(node_id)
                    last_error = str(e)
                    if fatal_error is None and self._is_non_retryable_dispatch_error(e):
                        fatal_error = e
                    await self.db.mark_cluster_node_error(int(node["id"]), last_error, error_type="dispatch")
                    debug_logger.log_warning(
                        f"[ClusterManager] dispatch custom-score node={node['node_name']} {diag_label(e)} failed: {last_error}"
                    )

            if fatal_error is not None:
                raise RuntimeError(f"子节点调度失败: {fatal_error}") from fatal_error
            if dispatched_this_round and last_error:
                debug_logger.log_warning(
                    f"[ClusterManager] dispatch custom-score round failed, will retry: {last_error}"
                )
            await asyncio.sleep(self._dispatch_poll_interval_seconds())

    async def dispatch_custom_token(self, request_payload: Dict[str, Any]) -> Dict[str, Any]:
        last_error = ""
        bucket_key = self._build_custom_dispatch_bucket_key(request_payload, prefix="custom")
        bucket_signature = bucket_key

        while True:
            nodes = await self._select_candidate_nodes(bucket_key=bucket_key, bucket_signature=bucket_signature)
            if not nodes:
                await asyncio.sleep(self._dispatch_poll_interval_seconds())
                continue

            dispatched_this_round = False
            fatal_error: Optional[Exception] = None
            for node in nodes:
                reserved = await self._try_reserve_dispatch_slot(node)
                if not reserved:
                    continue

                dispatched_this_round = True
                node_id = int(node.get("id") or 0)
                try:
                    result = await self._post_to_node(
                        node=node,
                        path="/api/v1/custom-token",
                        json_payload=request_payload,
                        timeout=config.cluster_master_dispatch_timeout_seconds,
                    )
                    await self._release_dispatch_slot(node_id)
                    await self._mark_bucket_affinity(bucket_key, node_id)
                    return result
                except Exception as e:
                    await self._release_dispatch_slot(node_id)
                    await self._clear_bucket_affinity(bucket_key, expected_node_id=node_id)
                    last_error = str(e)
                    if fatal_error is None and self._is_non_retryable_dispatch_error(e):
                        fatal_error = e
                    await self.db.mark_cluster_node_error(int(node["id"]), last_error, error_type="dispatch")
                    debug_logger.log_warning(
                        f"[ClusterManager] dispatch custom-token node={node['node_name']} {diag_label(e)} failed: {last_error}"
                    )

            if fatal_error is not None:
                raise RuntimeError(f"子节点调度失败: {fatal_error}") from fatal_error
            if dispatched_this_round and last_error:
                debug_logger.log_warning(
                    f"[ClusterManager] dispatch custom-token round failed, will retry: {last_error}"
                )
            await asyncio.sleep(self._dispatch_poll_interval_seconds())

    async def register_node(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        effective_capacity = self._as_positive_int(
            payload.get("effective_capacity") or payload.get("max_concurrency"),
            1,
        )
        reported_browser_count = self._as_positive_int(payload.get("browser_count"), effective_capacity)
        reported_node_max = self._as_positive_int(payload.get("node_max_concurrency"), effective_capacity)
        standby_token_count = max(0, int(payload.get("standby_token_count") or 0))
        standby_bucket_signatures = self._normalize_bucket_signatures(payload.get("standby_bucket_signatures"))

        try:
            node = await self.db.upsert_cluster_node(
                node_name=payload["node_name"],
                base_url=payload["base_url"],
                node_api_key=payload["node_api_key"],
                weight=int(payload.get("weight") or 100),
                max_concurrency=effective_capacity,
                reported_browser_count=reported_browser_count,
                reported_node_max_concurrency=reported_node_max,
                active_sessions=int(payload.get("active_sessions") or 0),
                cached_sessions=int(payload.get("cached_sessions") or 0),
                standby_token_count=standby_token_count,
                healthy=bool(payload.get("healthy", True)),
            )
        except Exception as e:
            debug_logger.log_warning(
                "[ClusterManager] register node persist failed "
                f"node={payload.get('node_name')} base_url={payload.get('base_url')} {diag_label(e)}: {e}"
            )
            raise
        try:
            await self.db.record_cluster_node_heartbeat(
                node_id=int(node["id"]),
                event_type="register",
                payload=payload,
                healthy=bool(payload.get("healthy", True)),
                reason=str(payload.get("reason") or "") or None,
            )
        except Exception as e:
            debug_logger.log_warning(
                f"[ClusterManager] record register heartbeat failed node={node.get('node_name')} {diag_label(e)}: {e}"
            )
        async with self._dispatch_lock:
            self._node_standby_bucket_signatures[int(node["id"])] = standby_bucket_signatures
        return {
            "success": True,
            "node": node,
            "cluster_role": config.cluster_role,
        }

    async def heartbeat_node(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        effective_capacity = self._as_positive_int(
            payload.get("effective_capacity") or payload.get("max_concurrency"),
            1,
        )
        reported_browser_count = self._as_positive_int(payload.get("browser_count"), effective_capacity)
        reported_node_max = self._as_positive_int(payload.get("node_max_concurrency"), effective_capacity)
        standby_token_count = max(0, int(payload.get("standby_token_count") or 0))
        standby_bucket_signatures = self._normalize_bucket_signatures(payload.get("standby_bucket_signatures"))

        try:
            node = await self.db.heartbeat_cluster_node(
                node_name=payload["node_name"],
                base_url=payload["base_url"],
                max_concurrency=effective_capacity,
                reported_browser_count=reported_browser_count,
                reported_node_max_concurrency=reported_node_max,
                active_sessions=int(payload.get("active_sessions") or 0),
                cached_sessions=int(payload.get("cached_sessions") or 0),
                standby_token_count=standby_token_count,
                healthy=bool(payload.get("healthy", True)),
            )
        except Exception as e:
            debug_logger.log_warning(
                "[ClusterManager] heartbeat persist failed "
                f"node={payload.get('node_name')} base_url={payload.get('base_url')} {diag_label(e)}: {e}"
            )
            raise
        if not node:
            return {
                "success": False,
                "message": "node_not_registered",
            }
        try:
            await self.db.record_cluster_node_heartbeat(
                node_id=int(node["id"]),
                event_type="heartbeat",
                payload=payload,
                healthy=bool(payload.get("healthy", True)),
                reason=str(payload.get("reason") or "") or None,
            )
        except Exception as e:
            debug_logger.log_warning(
                f"[ClusterManager] record heartbeat failed node={node.get('node_name')} {diag_label(e)}: {e}"
            )
        async with self._dispatch_lock:
            self._node_standby_bucket_signatures[int(node["id"])] = standby_bucket_signatures
        return {
            "success": True,
            "node": node,
        }

    async def _resolve_routed_session(self, routed_session_id: str) -> Tuple[Dict[str, Any], str]:
        raw = (routed_session_id or "").strip()
        if ":" not in raw:
            raise RuntimeError("master 模式 session_id 必须为 nodeId:childSessionId")
        node_part, child_session = raw.split(":", 1)
        if not node_part.isdigit() or not child_session:
            raise RuntimeError("session_id 路由格式无效")

        node_id = int(node_part)
        node = await self.db.get_cluster_node(node_id)
        if not node:
            raise RuntimeError("路由节点不存在")
        if not bool(node.get("enabled", 0)):
            raise RuntimeError("路由节点已禁用")

        return node, child_session

    async def _select_candidate_nodes(
        self,
        *,
        bucket_key: Optional[str] = None,
        bucket_signature: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        nodes = await self.db.get_available_cluster_nodes(config.cluster_master_node_stale_seconds)
        if not nodes:
            return []

        filtered_nodes: List[Dict[str, Any]] = []
        for node in nodes:
            base_url = str(node.get("base_url") or "")
            parsed = urllib.parse.urlparse(base_url)
            host = (parsed.hostname or "").strip().lower()
            if parsed.scheme not in {"http", "https"} or not host:
                debug_logger.log_warning(
                    f"[ClusterManager] 跳过无效子节点地址 node={node.get('node_name')} base_url={base_url}"
                )
                continue
            if host in {"0.0.0.0", "127.0.0.1", "localhost", "::1", "::"}:
                debug_logger.log_warning(
                    f"[ClusterManager] 跳过不可达子节点地址 node={node.get('node_name')} base_url={base_url}"
                )
                continue
            filtered_nodes.append(node)

        if not filtered_nodes:
            return []

        async with self._dispatch_lock:
            self._prune_dispatch_reservations_locked()
            valid_node_ids = {int(node.get("id") or 0) for node in filtered_nodes if int(node.get("id") or 0) > 0}
            self._prune_dispatch_bucket_affinity_locked(valid_node_ids=valid_node_ids)
            self._node_standby_bucket_signatures = {
                node_id: signatures
                for node_id, signatures in self._node_standby_bucket_signatures.items()
                if node_id in valid_node_ids
            }
            normalized_bucket_key = self._normalize_dispatch_bucket_key(bucket_key)
            preferred_node_id = int(self._dispatch_bucket_affinity.get(normalized_bucket_key, 0) or 0)
            normalized_bucket_signature = str(bucket_signature or "").strip()

            decorated: List[Dict[str, Any]] = []
            for node in filtered_nodes:
                node_id = int(node.get("id") or 0)
                reserved = len(self._dispatch_reservations.get(node_id, []))
                decorated_node = self.decorate_node_capacity(node, extra_active=reserved)
                signatures = self._node_standby_bucket_signatures.get(node_id, set())
                decorated_node["has_bucket_signature_match"] = (
                    1 if normalized_bucket_signature and normalized_bucket_signature in signatures else 0
                )
                decorated.append(decorated_node)

            with_idle = [node for node in decorated if int(node.get("thread_idle") or 0) > 0]
            if not with_idle:
                # 全部节点都无空闲容量时，直接返回空，避免继续向已满节点灌流量。
                return []

            with_idle.sort(
                key=lambda node: (
                    -int(node.get("has_bucket_signature_match") or 0),
                    -int(node.get("thread_idle") or 0),
                    -int(node.get("standby_token_count") or 0),
                    int(node.get("thread_active") or 0),
                    -int(node.get("weight") or 100),
                    int(node.get("id") or 0),
                )
            )

            weighted_ring: List[Dict[str, Any]] = []
            for node in with_idle:
                idle = max(1, int(node.get("thread_idle") or 0))
                weight = max(1, int(node.get("weight") or 100))
                weight_factor = max(1, round(weight / 100.0))
                tickets = min(200, max(1, idle * weight_factor))
                weighted_ring.extend([node] * tickets)

            start = self._dispatch_cursor % len(weighted_ring)
            self._dispatch_cursor = (self._dispatch_cursor + 1) % len(weighted_ring)

            ordered_idle: List[Dict[str, Any]] = []
            seen_node_ids = set()
            for offset in range(len(weighted_ring)):
                node = weighted_ring[(start + offset) % len(weighted_ring)]
                node_id = int(node.get("id") or 0)
                if node_id in seen_node_ids:
                    continue
                seen_node_ids.add(node_id)
                ordered_idle.append(node)
                if len(ordered_idle) >= len(with_idle):
                    break

            any_bucket_match = any(int(node.get("has_bucket_signature_match") or 0) > 0 for node in ordered_idle)
            if preferred_node_id > 0:
                preferred_index = next(
                    (
                        index
                        for index, node in enumerate(ordered_idle)
                        if int(node.get("id") or 0) == preferred_node_id
                    ),
                    None,
                )
                preferred_has_match = False
                if isinstance(preferred_index, int):
                    preferred_has_match = int(ordered_idle[preferred_index].get("has_bucket_signature_match") or 0) > 0
                if isinstance(preferred_index, int) and preferred_index > 0 and (preferred_has_match or not any_bucket_match):
                    preferred_node = ordered_idle.pop(preferred_index)
                    ordered_idle.insert(0, preferred_node)

            return ordered_idle

    def _dispatch_reservation_window_seconds(self) -> float:
        heartbeat_window = max(10, int(config.cluster_heartbeat_interval_seconds) * 2)
        dispatch_window = max(10, int(config.cluster_master_dispatch_timeout_seconds) + 5)
        return float(max(heartbeat_window, dispatch_window))

    def _completed_routed_session_window_seconds(self) -> float:
        configured_ttl = max(120, int(getattr(config, "session_ttl_seconds", 1200) or 1200))
        return float(max(600, min(configured_ttl * 2, 21600)))

    @staticmethod
    def _normalize_routed_session_id(routed_session_id: str) -> str:
        return str(routed_session_id or "").strip()

    def _prune_completed_routed_sessions_locked(self, now_ts: Optional[float] = None):
        if not self._completed_routed_sessions:
            return

        now_ts = float(now_ts if now_ts is not None else time.time())
        expire_before = now_ts - self._completed_routed_session_window_seconds()
        stale_ids = [
            session_id
            for session_id, finished_ts in self._completed_routed_sessions.items()
            if float(finished_ts) < expire_before
        ]
        for session_id in stale_ids:
            self._completed_routed_sessions.pop(session_id, None)

    async def _mark_dispatch_session_started(self, routed_session_id: str, node_id: int) -> bool:
        normalized = self._normalize_routed_session_id(routed_session_id)
        if not normalized or node_id <= 0:
            return False

        now_ts = time.time()
        async with self._routed_sessions_lock:
            self._prune_completed_routed_sessions_locked(now_ts)
            self._active_routed_sessions[normalized] = node_id
            self._completed_routed_sessions.pop(normalized, None)
        return True

    async def _mark_dispatch_session_finished(
        self,
        routed_session_id: str,
        *,
        fallback_node_id: int = 0,
    ) -> int:
        normalized = self._normalize_routed_session_id(routed_session_id)
        if not normalized:
            return int(fallback_node_id or 0)

        now_ts = time.time()
        async with self._routed_sessions_lock:
            self._prune_completed_routed_sessions_locked(now_ts)
            if normalized in self._completed_routed_sessions:
                return 0

            tracked_node_id = int(self._active_routed_sessions.pop(normalized, 0) or 0)
            self._completed_routed_sessions[normalized] = now_ts

        if tracked_node_id > 0:
            return tracked_node_id
        return int(fallback_node_id or 0)

    def _prune_dispatch_reservations_locked(self):
        if not self._dispatch_reservations:
            return

        expire_before = time.time() - self._dispatch_reservation_window_seconds()
        stale_node_ids: List[int] = []
        for node_id, slots in self._dispatch_reservations.items():
            valid_slots = [ts for ts in slots if ts >= expire_before]
            if valid_slots:
                self._dispatch_reservations[node_id] = valid_slots
            else:
                stale_node_ids.append(node_id)

        for node_id in stale_node_ids:
            self._dispatch_reservations.pop(node_id, None)

    async def _try_reserve_dispatch_slot(self, node: Dict[str, Any]) -> bool:
        node_id = int(node.get("id") or 0)
        if node_id <= 0:
            return False

        total_capacity = max(1, int(node.get("thread_total") or node.get("max_concurrency") or 1))
        reported_active = max(0, int(node.get("reported_active_sessions") or node.get("active_sessions") or 0))

        async with self._dispatch_lock:
            self._prune_dispatch_reservations_locked()
            reserved = len(self._dispatch_reservations.get(node_id, []))
            if reported_active + reserved >= total_capacity:
                return False

            self._dispatch_reservations.setdefault(node_id, []).append(time.time())
            return True

    async def _release_dispatch_slot(self, node_id: int):
        if node_id <= 0:
            return

        async with self._dispatch_lock:
            self._prune_dispatch_reservations_locked()
            slots = self._dispatch_reservations.get(node_id)
            if not slots:
                return

            slots.pop()
            if slots:
                self._dispatch_reservations[node_id] = slots
            else:
                self._dispatch_reservations.pop(node_id, None)

    @staticmethod
    def _as_positive_int(raw: Any, fallback: int = 1) -> int:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = int(fallback)
        return max(1, value)

    @staticmethod
    def _parse_db_timestamp(raw: Any) -> Optional[datetime]:
        text = str(raw or "").strip()
        if not text:
            return None
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
            return parsed.replace(tzinfo=None)
        except ValueError:
            return None

    @staticmethod
    def _extract_http_status(error_text: str) -> Optional[int]:
        match = _HTTP_STATUS_RE.search(str(error_text or ""))
        if not match:
            return None
        try:
            return int(match.group(1))
        except (TypeError, ValueError):
            return None

    @classmethod
    def _summarize_last_error(cls, last_error: str) -> Optional[tuple[str, str]]:
        error_text = str(last_error or "").strip()
        if not error_text:
            return None

        lowered_error = error_text.lower()
        status_code = cls._extract_http_status(error_text)
        if status_code is not None:
            if status_code in {401, 403}:
                return "auth_failed", f"HTTP {status_code}"
            return "report_failed", f"HTTP {status_code}"

        if "cluster key" in lowered_error or "api key" in lowered_error or "unauthorized" in lowered_error or "forbidden" in lowered_error or "认证" in lowered_error or "无效" in lowered_error:
            return "auth_failed", "认证失败"

        if "connection refused" in lowered_error or "failed to establish a new connection" in lowered_error:
            return "report_failed", "连接失败"

        if "node_not_registered" in lowered_error:
            return "report_failed", "节点未注册"

        if "timed out" in lowered_error or "timeout" in lowered_error:
            return "timeout", "请求超时"

        if "heartbeat failed" in lowered_error or "register failed" in lowered_error or "上报" in lowered_error:
            return "report_failed", "上报失败"

        return None

    @staticmethod
    def _classify_health_reason(
        *,
        enabled: bool,
        healthy: bool,
        heartbeat_age_seconds: Optional[int],
        stale_seconds: int,
        last_error: str,
    ) -> tuple[str, str]:
        if not enabled:
            return "disabled", "已禁用"

        summarized_error = ClusterManager._summarize_last_error(last_error)
        if summarized_error is not None:
            return summarized_error

        if not healthy:
            return "report_failed", "上报失败"

        if heartbeat_age_seconds is None or heartbeat_age_seconds > stale_seconds:
            return "timeout", "心跳超时"

        return "ok", "正常"

    @classmethod
    def decorate_node_capacity(cls, node: Dict[str, Any], extra_active: int = 0) -> Dict[str, Any]:
        effective_capacity = cls._as_positive_int(node.get("max_concurrency"), 1)
        reported_browser_count = cls._as_positive_int(node.get("reported_browser_count"), effective_capacity)
        reported_node_max = cls._as_positive_int(node.get("reported_node_max_concurrency"), effective_capacity)

        reported_active = max(0, int(node.get("active_sessions") or 0))
        active = reported_active + max(0, int(extra_active or 0))
        idle = max(effective_capacity - active, 0)

        stale_seconds = max(10, int(config.cluster_master_node_stale_seconds))
        heartbeat_dt = cls._parse_db_timestamp(node.get("last_heartbeat_at"))
        heartbeat_age_seconds: Optional[int] = None
        if heartbeat_dt is not None:
            heartbeat_age_seconds = max(0, int((datetime.utcnow() - heartbeat_dt).total_seconds()))

        reason_code, reason_text = cls._classify_health_reason(
            enabled=bool(node.get("enabled", 1)),
            healthy=bool(node.get("healthy", 1)),
            heartbeat_age_seconds=heartbeat_age_seconds,
            stale_seconds=stale_seconds,
            last_error=str(node.get("last_error") or ""),
        )

        decorated = dict(node)
        decorated["browser_count"] = reported_browser_count
        decorated["node_max_concurrency"] = reported_node_max
        decorated["effective_capacity"] = effective_capacity
        decorated["thread_total"] = effective_capacity
        decorated["thread_active"] = active
        decorated["thread_idle"] = idle
        decorated["reported_active_sessions"] = reported_active
        decorated["standby_token_count"] = max(0, int(node.get("standby_token_count") or 0))
        decorated["dispatch_reserved"] = max(0, int(extra_active or 0))
        decorated["heartbeat_age_seconds"] = heartbeat_age_seconds
        decorated["health_reason_code"] = reason_code
        decorated["health_reason"] = reason_text
        decorated["is_healthy"] = bool(node.get("enabled", 1)) and reason_code == "ok"
        return decorated

    def decorate_nodes_capacity(self, nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        return [self.decorate_node_capacity(node) for node in (nodes or [])]

    async def _post_to_node(
        self,
        node: Dict[str, Any],
        path: str,
        json_payload: Dict[str, Any],
        timeout: int,
    ) -> Dict[str, Any]:
        base_url = str(node.get("base_url") or "").rstrip("/")
        api_key = str(node.get("node_api_key") or "").strip()
        if not base_url or not api_key:
            raise RuntimeError("节点配置缺少 base_url 或 node_api_key")

        url = f"{base_url}{path}"
        headers = {"Authorization": f"Bearer {api_key}"}
        status_code, payload, response_text = await asyncio.to_thread(
            self._sync_json_http_request,
            "POST",
            url,
            headers,
            json_payload,
            timeout,
        )

        if status_code >= 400:
            detail = payload.get("detail") if isinstance(payload, dict) else None
            if not detail:
                detail = (response_text or "").strip()[:300]
            raise RuntimeError(f"HTTP {status_code}: {detail or payload}")

        if isinstance(payload, dict):
            return payload
        raise RuntimeError("子节点响应不是 JSON 对象")

    @staticmethod
    def _sync_json_http_request(
        method: str,
        url: str,
        headers: Dict[str, str],
        payload: Optional[Dict[str, Any]],
        timeout: int,
    ) -> tuple[int, Optional[Any], str]:
        req_headers = dict(headers or {})
        req_headers.setdefault("Accept", "application/json")

        data = None
        if payload is not None:
            data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            req_headers["Content-Type"] = "application/json; charset=utf-8"

        request = urllib.request.Request(
            url=url,
            data=data,
            headers=req_headers,
            method=(method or "GET").upper(),
        )

        def execute_request(*, insecure_tls: bool = False) -> tuple[int, bytes]:
            open_kwargs: Dict[str, Any] = {"timeout": timeout}
            if insecure_tls:
                open_kwargs["context"] = ssl._create_unverified_context()
            with urllib.request.urlopen(request, **open_kwargs) as response:
                return int(response.getcode() or 0), response.read()

        try:
            status_code, raw_body = execute_request()
        except urllib.error.HTTPError as e:
            status_code = int(getattr(e, "code", 500))
            raw_body = e.read() if hasattr(e, "read") else b""
        except Exception as e:
            if not ClusterManager._should_retry_without_tls_verify(url, e):
                raise RuntimeError(f"HTTP 请求失败: {e}") from e
            debug_logger.log_warning(
                f"[ClusterManager] HTTPS 证书校验失败，改用非校验模式重试内部节点通信: {url}"
            )
            try:
                status_code, raw_body = execute_request(insecure_tls=True)
            except urllib.error.HTTPError as retry_error:
                status_code = int(getattr(retry_error, "code", 500))
                raw_body = retry_error.read() if hasattr(retry_error, "read") else b""
            except Exception as retry_error:
                raise RuntimeError(f"HTTP 请求失败: {retry_error}") from retry_error

        text = raw_body.decode("utf-8", errors="replace") if raw_body else ""
        parsed: Optional[Any] = None
        if text:
            try:
                parsed = json.loads(text)
            except Exception:
                parsed = None

        return status_code, parsed, text

    async def _heartbeat_loop(self):
        debug_logger.log_info("[ClusterManager] subnode heartbeat loop started")
        while True:
            try:
                await self._send_subnode_heartbeat()
            except asyncio.CancelledError:
                return
            except Exception as e:
                debug_logger.log_warning(f"[ClusterManager] heartbeat error {diag_label(e)}: {e}")

            await asyncio.sleep(config.cluster_heartbeat_interval_seconds)

    async def _send_subnode_heartbeat(self):
        master_base = config.cluster_master_base_url
        cluster_key = config.cluster_master_cluster_key
        node_api_key = config.node_api_key

        if not master_base or not cluster_key or not node_api_key:
            debug_logger.log_warning(
                "[ClusterManager] subnode mode 缺少 master_base_url/master_cluster_key/node_api_key，跳过心跳"
            )
            return

        public_base_url = config.cluster_node_public_base_url
        if not public_base_url:
            debug_logger.log_warning(
                "[ClusterManager] subnode mode 缺少 node_public_base_url，跳过心跳。"
                "请填写主节点可以访问到的子节点地址，例如 http://subnode:8060 或 http://公网IP:8061"
            )
            return

        parsed_public = urllib.parse.urlparse(public_base_url)
        public_host = (parsed_public.hostname or "").strip().lower()
        if parsed_public.scheme not in {"http", "https"} or not public_host:
            debug_logger.log_warning(
                f"[ClusterManager] node_public_base_url 无效: {public_base_url}"
            )
            return

        if public_host in {"0.0.0.0", "127.0.0.1", "localhost", "::1", "::"}:
            debug_logger.log_warning(
                "[ClusterManager] node_public_base_url 不能是 0.0.0.0 / 127.0.0.1 / localhost。"
                f"当前值: {public_base_url}"
            )
            return

        runtime_stats = await self.runtime.get_stats()
        active_sessions = int(runtime_stats.get("active_sessions") or 0)
        cached_sessions = int(runtime_stats.get("cached_sessions") or 0)
        browser_stats = runtime_stats.get("browser") if isinstance(runtime_stats, dict) else {}
        configured_browser_count = 0
        standby_token_count = 0
        standby_bucket_signatures: List[str] = []
        if isinstance(browser_stats, dict):
            configured_browser_count = max(0, int(browser_stats.get("configured_browser_count") or 0))
            standby_token_count = max(0, int(browser_stats.get("standby_token_count") or 0))
            standby_bucket_signatures = list(browser_stats.get("standby_bucket_signatures") or [])
        configured_browser_count = max(1, configured_browser_count or int(config.browser_count))
        configured_dispatch_limit = max(1, int(config.cluster_node_max_concurrency))
        effective_capacity = max(1, min(configured_browser_count, configured_dispatch_limit))
        if configured_dispatch_limit < configured_browser_count:
            debug_logger.log_warning(
                f"[ClusterManager] dispatch capacity limited by cluster.node_max_concurrency: "
                f"browser_count={configured_browser_count}, node_max_concurrency={configured_dispatch_limit}, "
                f"effective_capacity={effective_capacity}"
            )

        register_payload = {
            "node_name": config.node_name,
            "base_url": public_base_url,
            "node_api_key": node_api_key,
            "weight": config.cluster_node_weight,
            "max_concurrency": effective_capacity,
            "browser_count": configured_browser_count,
            "node_max_concurrency": configured_dispatch_limit,
            "effective_capacity": effective_capacity,
            "active_sessions": active_sessions,
            "cached_sessions": cached_sessions,
            "standby_token_count": standby_token_count,
            "standby_bucket_signatures": standby_bucket_signatures,
            "healthy": True,
        }
        heartbeat_payload = {
            "node_name": config.node_name,
            "base_url": public_base_url,
            "max_concurrency": effective_capacity,
            "browser_count": configured_browser_count,
            "node_max_concurrency": configured_dispatch_limit,
            "effective_capacity": effective_capacity,
            "active_sessions": active_sessions,
            "cached_sessions": cached_sessions,
            "standby_token_count": standby_token_count,
            "standby_bucket_signatures": standby_bucket_signatures,
            "healthy": True,
        }

        headers = {"X-Cluster-Key": cluster_key}
        register_url = f"{master_base}/api/cluster/register"
        hb_url = f"{master_base}/api/cluster/heartbeat"

        if not self._subnode_registered:
            register_status, _, register_text = await asyncio.to_thread(
                self._sync_json_http_request,
                "POST",
                register_url,
                headers,
                register_payload,
                20,
            )
            if register_status >= 400:
                raise RuntimeError(f"register failed: {register_status}, {(register_text or '')[:200]}")
            self._subnode_registered = True
            debug_logger.log_info(
                f"[ClusterManager] subnode register success node={config.node_name} base_url={public_base_url}"
            )
            return

        hb_status, _, hb_text = await asyncio.to_thread(
            self._sync_json_http_request,
            "POST",
            hb_url,
            headers,
            heartbeat_payload,
            20,
        )
        if hb_status == 404 and "node_not_registered" in str(hb_text or ""):
            self._subnode_registered = False
            debug_logger.log_warning(
                f"[ClusterManager] heartbeat reported node_not_registered, will re-register node={config.node_name}"
            )
            register_status, _, register_text = await asyncio.to_thread(
                self._sync_json_http_request,
                "POST",
                register_url,
                headers,
                register_payload,
                20,
            )
            if register_status >= 400:
                raise RuntimeError(f"register failed: {register_status}, {(register_text or '')[:200]}")
            self._subnode_registered = True
            debug_logger.log_info(
                f"[ClusterManager] subnode re-register success node={config.node_name} base_url={public_base_url}"
            )
            return

        if hb_status >= 400:
            raise RuntimeError(f"heartbeat failed: {hb_status}, {(hb_text or '')[:200]}")

    async def get_cluster_runtime_summary(self) -> Dict[str, Any]:
        nodes = self.decorate_nodes_capacity(await self.db.list_cluster_nodes())
        total_thread_capacity = sum(max(0, int(node.get("thread_total") or 0)) for node in nodes)
        total_idle_capacity = sum(max(0, int(node.get("thread_idle") or 0)) for node in nodes)
        total_active_capacity = sum(max(0, int(node.get("thread_active") or 0)) for node in nodes)
        total_standby_token_count = sum(max(0, int(node.get("standby_token_count") or 0)) for node in nodes)
        healthy_node_count = sum(1 for node in nodes if bool(node.get("healthy")) and bool(node.get("enabled")))
        return {
            "role": config.cluster_role,
            "node_name": config.node_name,
            "node_count": len(nodes),
            "healthy_node_count": healthy_node_count,
            "total_thread_capacity": total_thread_capacity,
            "total_idle_capacity": total_idle_capacity,
            "total_active_capacity": total_active_capacity,
            "total_standby_token_count": total_standby_token_count,
            "nodes": nodes,
        }
