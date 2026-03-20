from __future__ import annotations

import time
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from ..core.auth import verify_service_api_key
from ..core.config import config
from ..core.database import Database
from ..core.diagnostics import diag_label
from ..core.logger import debug_logger
from ..core.models import CustomScoreRequest, ErrorRequest, FinishRequest, SolveRequest, SolveResponse
from ..services.captcha_runtime import CaptchaRuntime
from ..services.cluster_manager import ClusterManager

router = APIRouter(prefix="/api/v1", tags=["captcha-service"])

_db: Optional[Database] = None
_runtime: Optional[CaptchaRuntime] = None
_cluster: Optional[ClusterManager] = None


def set_dependencies(db: Database, runtime: CaptchaRuntime, cluster_manager: ClusterManager):
    global _db, _runtime, _cluster
    _db = db
    _runtime = runtime
    _cluster = cluster_manager


def _resolve_service_request_owner(api_key: dict) -> tuple[Optional[int], Optional[int], Optional[int]]:
    if bool(api_key.get("is_internal")):
        return None, None, None

    portal_user_id = int(api_key.get("portal_user_id") or 0)
    portal_api_key_id = int(api_key.get("portal_api_key_id") or 0)
    if portal_user_id > 0:
        return None, portal_user_id, portal_api_key_id or None

    service_api_key_id = int(api_key.get("id", 0) or 0)
    return (service_api_key_id if service_api_key_id > 0 else None), None, None


async def _safe_create_job_log(**kwargs):
    if _db is None:
        return
    try:
        await _db.create_job_log(**kwargs)
    except Exception as e:
        debug_logger.log_warning(
            "[service_api] create_job_log failed "
            f"status={kwargs.get('status')} session_id={kwargs.get('session_id')}: {e}"
        )


@router.get("/health")
async def health_check():
    return {
        "success": True,
        "status": "ok",
        "node_name": config.node_name,
        "role": config.cluster_role,
    }


@router.post("/solve", response_model=SolveResponse)
async def solve_captcha(
    request: SolveRequest,
    api_key: dict = Depends(verify_service_api_key),
):
    if _db is None or _runtime is None:
        raise HTTPException(status_code=500, detail="服务未初始化")

    service_api_key_id, portal_user_id, portal_api_key_id = _resolve_service_request_owner(api_key)
    if portal_user_id:
        available, message = await _db.ensure_portal_user_available(portal_user_id)
    else:
        available, message = await _db.ensure_api_key_available(service_api_key_id or 0)
    if not available:
        raise HTTPException(status_code=403, detail=message)

    started = time.perf_counter()
    result: Optional[dict] = None

    try:
        if config.cluster_role == "master":
            if _cluster is None:
                raise RuntimeError("cluster manager 未初始化")
            result = await _cluster.dispatch_solve(request.model_dump())
            log_status = "success_master_dispatch"
        else:
            result = await _runtime.solve(
                project_id=request.project_id,
                action=request.action,
                token_id=request.token_id,
                api_key_id=service_api_key_id or 0,
            )
            log_status = "success"

        if portal_user_id:
            consumed, consume_message = await _db.consume_portal_user_quota(
                portal_user_id,
                source_type="api_solve_success",
                source_ref=str(result.get("session_id") or "") if result else None,
                note=str(api_key.get("name") or "?? API Key"),
                portal_api_key_id=portal_api_key_id or None,
            )
        else:
            consumed, consume_message = await _db.consume_api_key_quota(
                service_api_key_id or 0,
                session_id=str(result.get("session_id") or "") if result else None,
            )

        if not consumed:
            if result and config.cluster_role == "master" and _cluster is not None:
                try:
                    await _cluster.dispatch_error(str(result.get("session_id")), "quota_conflict")
                except Exception:
                    pass
            if result and config.cluster_role != "master":
                try:
                    await _runtime.mark_error(str(result.get("session_id")), "quota_conflict")
                except Exception:
                    pass
            raise HTTPException(status_code=403, detail=consume_message)

        elapsed = int((time.perf_counter() - started) * 1000)
        await _safe_create_job_log(
            session_id=result.get("session_id") if result else None,
            api_key_id=service_api_key_id,
            project_id=request.project_id,
            action=request.action,
            status=log_status,
            error_reason=None,
            duration_ms=elapsed,
            portal_user_id=portal_user_id,
            portal_api_key_id=portal_api_key_id,
        )

        return SolveResponse(**(result or {}))
    except HTTPException:
        raise
    except Exception as e:
        elapsed = int((time.perf_counter() - started) * 1000)
        await _safe_create_job_log(
            session_id=result.get("session_id") if result else None,
            api_key_id=service_api_key_id,
            project_id=request.project_id,
            action=request.action,
            status="failed",
            error_reason=str(e),
            duration_ms=elapsed,
            portal_user_id=portal_user_id,
            portal_api_key_id=portal_api_key_id,
        )
        raise HTTPException(status_code=500, detail=f"打码失败: {e}")


@router.post("/sessions/{session_id}/finish")
async def finish_session(
    session_id: str,
    request: FinishRequest,
    api_key: dict = Depends(verify_service_api_key),
):
    if _db is None or _runtime is None:
        raise HTTPException(status_code=500, detail="服务未初始化")

    service_api_key_id, portal_user_id, portal_api_key_id = _resolve_service_request_owner(api_key)
    entry = None
    if config.cluster_role == "master" and ":" in session_id:
        if _cluster is None:
            raise HTTPException(status_code=500, detail="cluster manager 未初始化")
        try:
            await _cluster.dispatch_finish(session_id, request.status)
            message = "ok"
        except Exception as e:
            debug_logger.log_warning(
                "[service_api] finish dispatch failed "
                f"session_id={session_id} status={request.status} {diag_label(e)}: {e}"
            )
            raise HTTPException(status_code=500, detail=f"finish 转发失败: {e}")
    else:
        try:
            ok, message, entry = await _runtime.finish(session_id)
        except Exception as e:
            debug_logger.log_warning(
                "[service_api] local finish failed "
                f"session_id={session_id} {diag_label(e)}: {e}"
            )
            raise HTTPException(status_code=500, detail=f"finish 本地处理失败: {e}")
        if not ok:
            raise HTTPException(status_code=404, detail=message)

    normalized_status = str(request.status or "").strip().lower()
    refund_reason = f"finish:{normalized_status}" if normalized_status and normalized_status != "success" else None
    try:
        await _db.finalize_service_session(
            session_id=session_id,
            api_key_id=service_api_key_id,
            project_id=entry.project_id if entry else None,
            action=entry.action if entry else None,
            status=f"finish:{request.status}",
            error_reason=None,
            portal_user_id=portal_user_id,
            portal_api_key_id=portal_api_key_id,
            refund_reason=refund_reason,
        )
    except Exception as e:
        debug_logger.log_warning(
            "[service_api] finalize finish failed "
            f"status={request.status} session_id={session_id} {diag_label(e)}: {e}"
        )
    return {"success": True, "message": message}


@router.post("/sessions/{session_id}/error")
async def report_session_error(
    session_id: str,
    request: ErrorRequest,
    api_key: dict = Depends(verify_service_api_key),
):
    if _db is None or _runtime is None:
        raise HTTPException(status_code=500, detail="服务未初始化")

    service_api_key_id, portal_user_id, portal_api_key_id = _resolve_service_request_owner(api_key)
    entry = None
    if config.cluster_role == "master" and ":" in session_id:
        if _cluster is None:
            raise HTTPException(status_code=500, detail="cluster manager 未初始化")
        try:
            await _cluster.dispatch_error(session_id, request.error_reason)
            message = "ok"
        except Exception as e:
            debug_logger.log_warning(
                "[service_api] error dispatch failed "
                f"session_id={session_id} error_reason={request.error_reason} {diag_label(e)}: {e}"
            )
            raise HTTPException(status_code=500, detail=f"error 转发失败: {e}")
    else:
        try:
            ok, message, entry = await _runtime.mark_error(session_id, request.error_reason)
        except Exception as e:
            debug_logger.log_warning(
                "[service_api] local error failed "
                f"session_id={session_id} error_reason={request.error_reason} {diag_label(e)}: {e}"
            )
            raise HTTPException(status_code=500, detail=f"error 本地处理失败: {e}")
        if not ok:
            raise HTTPException(status_code=404, detail=message)

    try:
        await _db.finalize_service_session(
            session_id=session_id,
            api_key_id=service_api_key_id,
            project_id=entry.project_id if entry else None,
            action=entry.action if entry else None,
            status="error_reported",
            error_reason=request.error_reason,
            portal_user_id=portal_user_id,
            portal_api_key_id=portal_api_key_id,
            refund_reason=request.error_reason or "error_reported",
        )
    except Exception as e:
        debug_logger.log_warning(
            "[service_api] finalize error failed "
            f"session_id={session_id} error_reason={request.error_reason} {diag_label(e)}: {e}"
        )
    return {"success": True, "message": message}


@router.post("/custom-score")
async def custom_score(
    request: CustomScoreRequest,
    api_key: dict = Depends(verify_service_api_key),
):
    if _db is None or _runtime is None:
        raise HTTPException(status_code=500, detail="服务未初始化")

    try:
        if config.cluster_role == "master":
            if _cluster is None:
                raise RuntimeError("cluster manager 未初始化")
            payload = await _cluster.dispatch_custom_score(request.model_dump())
        else:
            payload = await _runtime.custom_score(
                website_url=request.website_url,
                website_key=request.website_key,
                verify_url=request.verify_url,
                action=request.action,
                enterprise=request.enterprise,
            )

        verify_result = payload.get("verify_result") or payload
        token_value = payload.get("token")
        if not token_value and isinstance(verify_result, dict):
            token_value = (
                verify_result.get("token")
                or verify_result.get("gRecaptchaResponse")
            )

        return {
            "success": bool(payload.get("success", True)),
            "captcha_method": "remote_browser",
            "node_name": payload.get("node_name", config.node_name),
            "token": token_value,
            "token_elapsed_ms": payload.get("token_elapsed_ms"),
            "verify_elapsed_ms": payload.get("verify_elapsed_ms"),
            "verify_http_status": payload.get("verify_http_status"),
            "verify_mode": payload.get("verify_mode"),
            "fingerprint": payload.get("fingerprint"),
            "verify_result": verify_result,
            "message": payload.get("message", "ok"),
            "raw": payload,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"custom-score 失败: {e}")
