import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI

from src.api import service
from src.core.auth import set_database
from src.core.database import Database
from src.services.captcha_runtime import CaptchaRuntime


class FakeClusterManager:
    async def dispatch_solve(self, request_payload):
        raise AssertionError("standalone test should not dispatch_solve")


class ServiceLocalCaptchaModeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.env_patcher = patch.dict(os.environ, {"FCS_CLUSTER_ROLE": "standalone"}, clear=False)
        self.env_patcher.start()

        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp_dir.name) / "test.sqlite3")
        await self.db.init_db()
        set_database(self.db)

        self.runtime = CaptchaRuntime(self.db)
        self.cluster = FakeClusterManager()
        service.set_dependencies(self.db, self.runtime, self.cluster)

        self.app = FastAPI()
        self.app.include_router(service.router)
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://testserver",
        )

        self.raw_key, self.api_key = await self.db.create_api_key("svc-local-mode", 5)

    async def asyncTearDown(self):
        try:
            await self.client.aclose()
            await self.runtime.close()
            await self.db.close()
            for attempt in range(5):
                try:
                    self.temp_dir.cleanup()
                    break
                except (PermissionError, NotADirectoryError):
                    if attempt >= 4:
                        raise
                    await asyncio.sleep(0.05)
        finally:
            self.env_patcher.stop()

    async def _run_concurrent_solve_requests(self, mode: str):
        first_live_started = asyncio.Event()
        release_live = asyncio.Event()
        state = {"call_count": 0, "inflight": 0, "max_inflight": 0}

        class FakeSerializedService:
            def __init__(self):
                self._lock = asyncio.Lock()
                self.close = AsyncMock()

            async def get_token(self, project_id: str, action: str, token_id: int | None = None):
                _ = token_id
                async with self._lock:
                    state["call_count"] += 1
                    current_call = state["call_count"]
                    state["inflight"] += 1
                    state["max_inflight"] = max(state["max_inflight"], state["inflight"])
                    first_live_started.set()
                    try:
                        await release_live.wait()
                        browser_ref = 17 if mode == "browser" else f"personal:{project_id}"
                        return SimpleNamespace(
                            token=f"{mode}-token-{current_call}",
                            browser_ref=browser_ref,
                            browser_id=browser_ref,
                            fingerprint={"userAgent": f"{mode}-agent-{current_call}"},
                        )
                    finally:
                        state["inflight"] -= 1

        fake_service = FakeSerializedService()
        self.runtime._browser_service = fake_service
        self.runtime._service_mode = mode

        with patch("src.api.service.config", SimpleNamespace(cluster_role="standalone")):
            with patch(
                "src.services.captcha_runtime.config",
                SimpleNamespace(
                    cluster_role="standalone",
                    captcha_method=mode,
                    node_name="node-a",
                    session_ttl_seconds=1200,
                ),
            ):
                first_task = asyncio.create_task(
                    self.client.post(
                        "/api/v1/solve",
                        headers={"Authorization": f"Bearer {self.raw_key}"},
                        json={"project_id": "demo-project", "action": "IMAGE_GENERATION"},
                    )
                )
                await first_live_started.wait()
                second_task = asyncio.create_task(
                    self.client.post(
                        "/api/v1/solve",
                        headers={"Authorization": f"Bearer {self.raw_key}"},
                        json={"project_id": "demo-project", "action": "IMAGE_GENERATION"},
                    )
                )
                await asyncio.sleep(0.01)
                self.assertFalse(first_task.done())
                self.assertFalse(second_task.done())

                release_live.set()
                first_response, second_response = await asyncio.gather(first_task, second_task)

        return fake_service, state, first_response, second_response

    async def _run_concurrent_custom_token_route(self, *, mode: str):
        first_started = asyncio.Event()
        release = asyncio.Event()
        state = {"call_count": 0, "inflight": 0, "max_inflight": 0}

        async def fake_get_custom_token(*args, **kwargs):
            state["call_count"] += 1
            index = state["call_count"]
            state["inflight"] += 1
            state["max_inflight"] = max(state["max_inflight"], state["inflight"])
            first_started.set()
            try:
                await release.wait()
                browser_ref = index if mode == "browser" else f"personal-custom:{index}"
                return SimpleNamespace(
                    token=f"{mode}-token-{index}",
                    browser_ref=browser_ref,
                    browser_id=browser_ref,
                    fingerprint={"userAgent": f"{mode}-agent-{index}"},
                )
            finally:
                state["inflight"] -= 1

        fake_service = SimpleNamespace(
            get_custom_token=AsyncMock(side_effect=fake_get_custom_token),
            close=AsyncMock(),
        )
        self.runtime._browser_service = fake_service
        self.runtime._service_mode = mode

        request_payload = {
            "website_url": "https://example.com/login",
            "website_key": "site-key",
            "action": "login",
            "enterprise": False,
        }

        with patch("src.api.service.config", SimpleNamespace(cluster_role="standalone", captcha_method=mode, node_name="node-a")):
            with patch(
                "src.services.captcha_runtime.config",
                SimpleNamespace(
                    cluster_role="standalone",
                    captcha_method=mode,
                    node_name="node-a",
                    session_ttl_seconds=1200,
                ),
            ):
                first_task = asyncio.create_task(
                    self.client.post(
                        "/api/v1/custom-token",
                        headers={"Authorization": f"Bearer {self.raw_key}"},
                        json=request_payload,
                    )
                )
                await first_started.wait()
                second_task = asyncio.create_task(
                    self.client.post(
                        "/api/v1/custom-token",
                        headers={"Authorization": f"Bearer {self.raw_key}"},
                        json=request_payload,
                    )
                )
                await asyncio.sleep(0.01)
                self.assertFalse(first_task.done())
                self.assertFalse(second_task.done())
                release.set()
                first_response, second_response = await asyncio.gather(first_task, second_task)

        first_payload = first_response.json()
        second_payload = second_response.json()
        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(first_payload["captcha_method"], mode)
        self.assertEqual(second_payload["captcha_method"], mode)
        self.assertTrue(str(first_payload["token"] or "").startswith(f"{mode}-token-"))
        self.assertTrue(str(second_payload["token"] or "").startswith(f"{mode}-token-"))
        self.assertGreaterEqual(state["max_inflight"], 1)
        self.assertEqual(fake_service.get_custom_token.await_count, 2)

    async def test_solve_route_returns_token_in_browser_mode(self):
        fake_service = SimpleNamespace(
            get_token=AsyncMock(
                return_value=SimpleNamespace(
                    token="browser-token",
                    browser_ref=17,
                    browser_id=17,
                    fingerprint={"userAgent": "browser-agent"},
                )
            ),
            close=AsyncMock(),
        )
        self.runtime._browser_service = fake_service
        self.runtime._service_mode = "browser"

        with patch("src.api.service.config", SimpleNamespace(cluster_role="standalone")):
            with patch(
                "src.services.captcha_runtime.config",
                SimpleNamespace(
                    cluster_role="standalone",
                    captcha_method="browser",
                    node_name="node-a",
                    session_ttl_seconds=1200,
                ),
            ):
                response = await self.client.post(
                    "/api/v1/solve",
                    headers={"Authorization": f"Bearer {self.raw_key}"},
                    json={"project_id": "demo-project", "action": "IMAGE_GENERATION", "token_id": 11},
                )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["token"], "browser-token")
        self.assertEqual(payload["fingerprint"], {"userAgent": "browser-agent"})
        self.assertEqual(payload["node_name"], "node-a")
        fake_service.get_token.assert_awaited_once_with("demo-project", "IMAGE_GENERATION", token_id=11)

    async def test_solve_route_returns_token_in_personal_mode(self):
        fake_service = SimpleNamespace(
            get_token=AsyncMock(
                return_value=SimpleNamespace(
                    token="personal-token",
                    browser_ref="personal:demo-project",
                    browser_id="personal:demo-project",
                    fingerprint={"userAgent": "personal-agent"},
                )
            ),
            close=AsyncMock(),
        )
        self.runtime._browser_service = fake_service
        self.runtime._service_mode = "personal"

        with patch("src.api.service.config", SimpleNamespace(cluster_role="standalone")):
            with patch(
                "src.services.captcha_runtime.config",
                SimpleNamespace(
                    cluster_role="standalone",
                    captcha_method="personal",
                    node_name="node-a",
                    session_ttl_seconds=1200,
                ),
            ):
                response = await self.client.post(
                    "/api/v1/solve",
                    headers={"Authorization": f"Bearer {self.raw_key}"},
                    json={"project_id": "demo-project", "action": "IMAGE_GENERATION"},
                )

        payload = response.json()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["token"], "personal-token")
        self.assertEqual(payload["fingerprint"], {"userAgent": "personal-agent"})
        self.assertEqual(payload["node_name"], "node-a")
        fake_service.get_token.assert_awaited_once_with("demo-project", "IMAGE_GENERATION")

    async def test_concurrent_solve_route_returns_tokens_in_browser_mode(self):
        fake_service, state, first_response, second_response = await self._run_concurrent_solve_requests("browser")

        first_payload = first_response.json()
        second_payload = second_response.json()

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(state["max_inflight"], 1)
        self.assertEqual(state["call_count"], 2)
        self.assertNotEqual(first_payload["session_id"], second_payload["session_id"])
        self.assertNotEqual(first_payload["token"], second_payload["token"])
        self.assertEqual(first_payload["node_name"], "node-a")
        self.assertEqual(second_payload["node_name"], "node-a")
        self.assertEqual(await self.runtime.registry.active_count(), 2)

    async def test_concurrent_solve_route_returns_tokens_in_personal_mode(self):
        fake_service, state, first_response, second_response = await self._run_concurrent_solve_requests("personal")

        first_payload = first_response.json()
        second_payload = second_response.json()

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(state["max_inflight"], 1)
        self.assertEqual(state["call_count"], 2)
        self.assertNotEqual(first_payload["session_id"], second_payload["session_id"])
        self.assertNotEqual(first_payload["token"], second_payload["token"])
        self.assertEqual(first_payload["node_name"], "node-a")
        self.assertEqual(second_payload["node_name"], "node-a")
        self.assertEqual(await self.runtime.registry.active_count(), 2)

    async def test_custom_token_route_supports_concurrent_requests_in_browser_mode(self):
        await self._run_concurrent_custom_token_route(mode="browser")

    async def test_custom_token_route_supports_concurrent_requests_in_personal_mode(self):
        await self._run_concurrent_custom_token_route(mode="personal")


if __name__ == "__main__":
    unittest.main()
