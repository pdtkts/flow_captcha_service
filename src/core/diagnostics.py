from __future__ import annotations

import re
from typing import Any


_HTTP_STATUS_RE = re.compile(r"\bhttp\s*([1-5]\d{2})\b", re.IGNORECASE)


def _extract_http_status(text: str) -> int | None:
    match = _HTTP_STATUS_RE.search(text or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def classify_issue(error: Any) -> str:
    raw_text = str(error or "").strip()
    if not raw_text:
        return "unknown"
    text = raw_text.lower()

    if "database is locked" in text or "sqlite" in text and "locked" in text:
        return "db_locked"

    status_code = _extract_http_status(raw_text)
    if status_code is not None:
        if status_code in {401, 403, 404}:
            return f"http_{status_code}"
        if 400 <= status_code < 500:
            return "http_4xx"
        if 500 <= status_code < 600:
            return "http_5xx"

    if "node_not_registered" in text or "session_not_found" in text:
        return "not_found"

    if "cluster key" in text or "api key" in text or "unauthorized" in text or "forbidden" in text or "认证" in text:
        return "auth"

    if "certificate verify failed" in text or "[ssl:" in text or " tls:" in text or "hostname mismatch" in text:
        return "network_tls"

    if "timed out" in text or "timeout" in text:
        if "session_timeout" in text or "finish:timeout" in text:
            return "session_timeout"
        return "network_timeout"

    if "connection refused" in text or "name or service not known" in text or "failed to establish a new connection" in text:
        return "network_connection"

    if "quota" in text:
        return "quota"

    return "unknown"


def diag_label(error: Any) -> str:
    return f"diag={classify_issue(error)}"
