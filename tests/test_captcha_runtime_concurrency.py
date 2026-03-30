import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.services.captcha_runtime import CaptchaRuntime


class CaptchaRuntimeConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def _run_concurrent_custom_token(self, *, mode: str):
        runtime = CaptchaRuntime(db=object())
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
        runtime._browser_service = fake_service
        runtime._service_mode = mode

        with patch(
            "src.services.captcha_runtime.config",
            SimpleNamespace(
                cluster_role="standalone",
                captcha_method=mode,
                node_name="node-a",
            ),
        ):
            first_task = asyncio.create_task(
                runtime.custom_token(
                    website_url="https://example.com/login",
                    website_key="site-key",
                    action="login",
                    enterprise=False,
                )
            )
            await first_started.wait()
            second_task = asyncio.create_task(
                runtime.custom_token(
                    website_url="https://example.com/login",
                    website_key="site-key",
                    action="login",
                    enterprise=False,
                )
            )
            await asyncio.sleep(0.01)
            self.assertFalse(first_task.done())
            self.assertFalse(second_task.done())
            release.set()
            first_payload, second_payload = await asyncio.gather(first_task, second_task)

        self.assertEqual(state["max_inflight"], 2)
        self.assertEqual(fake_service.get_custom_token.await_count, 2)
        self.assertEqual(first_payload["node_name"], "node-a")
        self.assertEqual(second_payload["node_name"], "node-a")
        self.assertTrue(str(first_payload["token"] or "").startswith(f"{mode}-token-"))
        self.assertTrue(str(second_payload["token"] or "").startswith(f"{mode}-token-"))

    async def test_custom_token_runs_concurrently_in_browser_mode(self):
        await self._run_concurrent_custom_token(mode="browser")

    async def test_custom_token_runs_concurrently_in_personal_mode(self):
        await self._run_concurrent_custom_token(mode="personal")


if __name__ == "__main__":
    unittest.main()
