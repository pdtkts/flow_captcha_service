import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.services.browser_captcha_personal import BrowserCaptchaService, ResidentTabInfo


class _FakeTab:
    async def sleep(self, _seconds: float):
        return None


class BrowserCaptchaPersonalConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def test_same_resident_slot_concurrent_get_token_serializes_execution(self):
        service = BrowserCaptchaService()
        resident_info = ResidentTabInfo(tab=object(), slot_id="slot-1", project_id="project-a")
        resident_info.recaptcha_ready = True

        first_started = asyncio.Event()
        release_live = asyncio.Event()
        state = {"inflight": 0, "max_inflight": 0, "call_count": 0}

        async def fake_execute(_tab, _action):
            state["call_count"] += 1
            state["inflight"] += 1
            state["max_inflight"] = max(state["max_inflight"], state["inflight"])
            first_started.set()
            try:
                await release_live.wait()
                return f"personal-token-{state['call_count']}"
            finally:
                state["inflight"] -= 1

        with patch.object(service, "initialize", AsyncMock()):
            with patch.object(
                service,
                "_ensure_resident_tab",
                AsyncMock(return_value=("slot-1", resident_info)),
            ):
                with patch.object(service, "_execute_recaptcha_on_tab", side_effect=fake_execute):
                    with patch.object(
                        service,
                        "_extract_tab_fingerprint",
                        AsyncMock(return_value={"user_agent": "ua-personal"}),
                    ):
                        first_task = asyncio.create_task(
                            service.get_token(project_id="project-a", action="IMAGE_GENERATION")
                        )
                        await first_started.wait()
                        second_task = asyncio.create_task(
                            service.get_token(project_id="project-a", action="IMAGE_GENERATION")
                        )
                        await asyncio.sleep(0.01)
                        self.assertFalse(first_task.done())
                        self.assertFalse(second_task.done())

                        release_live.set()
                        first_result, second_result = await asyncio.gather(first_task, second_task)

        self.assertEqual(state["max_inflight"], 1)
        self.assertTrue(bool(first_result.token))
        self.assertTrue(bool(second_result.token))
        self.assertEqual(first_result.browser_ref, "personal:project-a")
        self.assertEqual(second_result.browser_ref, "personal:project-a")
        self.assertEqual(first_result.fingerprint, {"user_agent": "ua-personal"})
        self.assertEqual(second_result.fingerprint, {"user_agent": "ua-personal"})

    async def test_same_custom_tab_concurrent_get_custom_token_serializes_execution(self):
        service = BrowserCaptchaService()
        website_url = "https://example.com/login"
        website_key = "site-key"
        cache_key = f"{website_url}|{website_key}|0"
        service._custom_tabs[cache_key] = {
            "tab": _FakeTab(),
            "recaptcha_ready": True,
            "warmed_up": True,
            "created_at": 0.0,
        }

        first_started = asyncio.Event()
        release_live = asyncio.Event()
        state = {"inflight": 0, "max_inflight": 0, "call_count": 0}

        async def fake_execute(*, tab, website_key, action, enterprise):
            _ = tab
            _ = website_key
            _ = action
            _ = enterprise
            state["call_count"] += 1
            state["inflight"] += 1
            state["max_inflight"] = max(state["max_inflight"], state["inflight"])
            first_started.set()
            try:
                await release_live.wait()
                return f"custom-token-{state['call_count']}"
            finally:
                state["inflight"] -= 1

        with patch.object(service, "initialize", AsyncMock()):
            with patch("src.services.browser_captcha_personal.config", SimpleNamespace(
                browser_score_test_warmup_seconds=0,
                browser_score_test_settle_seconds=0,
            )):
                async def fake_tab_evaluate(_tab, _script, label, timeout_seconds=None):
                    _ = timeout_seconds
                    if label == "custom_document_ready":
                        return "complete"
                    return None

                with patch.object(service, "_tab_evaluate", AsyncMock(side_effect=fake_tab_evaluate)):
                    with patch.object(
                        service,
                        "_execute_custom_recaptcha_on_tab",
                        side_effect=fake_execute,
                    ):
                        with patch.object(
                            service,
                            "_extract_tab_fingerprint",
                            AsyncMock(return_value={"user_agent": "ua-custom"}),
                        ):
                            first_task = asyncio.create_task(
                                service.get_custom_token(
                                    website_url=website_url,
                                    website_key=website_key,
                                    action="login",
                                    enterprise=False,
                                )
                            )
                            await first_started.wait()
                            second_task = asyncio.create_task(
                                service.get_custom_token(
                                    website_url=website_url,
                                    website_key=website_key,
                                    action="login",
                                    enterprise=False,
                                )
                            )
                            await asyncio.sleep(0.01)
                            self.assertFalse(first_task.done())
                            self.assertFalse(second_task.done())

                            release_live.set()
                            first_result, second_result = await asyncio.gather(first_task, second_task)

        self.assertEqual(state["max_inflight"], 1)
        self.assertTrue(bool(first_result.token))
        self.assertTrue(bool(second_result.token))
        self.assertEqual(first_result.browser_ref, second_result.browser_ref)
        self.assertEqual(first_result.fingerprint, {"user_agent": "ua-custom"})
        self.assertEqual(second_result.fingerprint, {"user_agent": "ua-custom"})


if __name__ == "__main__":
    unittest.main()
