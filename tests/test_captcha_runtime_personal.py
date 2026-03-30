import sys
import types
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.services.captcha_runtime import CaptchaRuntime


class CaptchaRuntimePersonalTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_browser_service_switches_from_browser_to_personal(self):
        runtime = CaptchaRuntime(db=object())
        old_service = SimpleNamespace(close=AsyncMock())
        new_service = SimpleNamespace(close=AsyncMock())
        runtime._browser_service = old_service
        runtime._service_mode = "browser"

        service_cls = type(
            "FakePersonalService",
            (),
            {
                "get_instance": classmethod(lambda cls, db=None: AsyncMock(return_value=new_service)()),
            },
        )
        fake_module = types.ModuleType("src.services.browser_captcha_personal")
        fake_module.BrowserCaptchaService = service_cls

        with patch("src.services.captcha_runtime.config", SimpleNamespace(cluster_role="subnode", captcha_method="personal")):
            with patch.dict(sys.modules, {"src.services.browser_captcha_personal": fake_module}):
                service = await runtime._get_browser_service()

        self.assertIs(service, new_service)
        self.assertEqual(runtime._service_mode, "personal")
        old_service.close.assert_awaited_once()

    async def test_solve_uses_personal_token_result_shape_and_returns_fingerprint(self):
        runtime = CaptchaRuntime(db=object())
        fake_service = SimpleNamespace(
            get_token=AsyncMock(
                return_value=SimpleNamespace(
                    token="personal-token",
                    browser_ref="personal:project-a",
                    fingerprint={"user_agent": "ua-personal"},
                )
            ),
            get_last_fingerprint=lambda: {"user_agent": "ua-personal"},
            close=AsyncMock(),
        )

        service_cls = type(
            "FakePersonalService",
            (),
            {
                "get_instance": classmethod(lambda cls, db=None: AsyncMock(return_value=fake_service)()),
            },
        )
        fake_module = types.ModuleType("src.services.browser_captcha_personal")
        fake_module.BrowserCaptchaService = service_cls

        with patch("src.services.captcha_runtime.config", SimpleNamespace(
            cluster_role="subnode",
            captcha_method="personal",
            personal_max_resident_tabs=5,
            browser_auto_warm_project_id="",
            node_name="node-a",
            session_ttl_seconds=1200,
        )):
            with patch.dict(sys.modules, {"src.services.browser_captcha_personal": fake_module}):
                payload = await runtime.solve(
                    project_id="project-a",
                    action="IMAGE_GENERATION",
                    token_id=None,
                    api_key_id=9,
                )

        self.assertEqual(payload["token"], "personal-token")
        self.assertEqual(payload["fingerprint"], {"user_agent": "ua-personal"})
        self.assertEqual(payload["node_name"], "node-a")
        self.assertTrue(payload["session_id"])
        fake_service.get_token.assert_awaited_once_with("project-a", "IMAGE_GENERATION")

    async def test_custom_token_uses_personal_token_result_shape(self):
        runtime = CaptchaRuntime(db=object())
        runtime._browser_service = SimpleNamespace(
            get_custom_token=AsyncMock(
                return_value=SimpleNamespace(
                    token="custom-token",
                    browser_ref="personal-custom:abc123",
                    fingerprint={"user_agent": "ua-custom"},
                )
            ),
            close=AsyncMock(),
        )
        runtime._service_mode = "personal"

        with patch("src.services.captcha_runtime.config", SimpleNamespace(cluster_role="subnode", captcha_method="personal", node_name="node-a")):
            payload = await runtime.custom_token(
                website_url="https://example.com",
                website_key="site-key",
                action="homepage",
                enterprise=False,
            )

        self.assertEqual(payload["token"], "custom-token")
        self.assertEqual(payload["browser_id"], "personal-custom:abc123")
        self.assertEqual(payload["fingerprint"], {"user_agent": "ua-custom"})
        self.assertEqual(payload["node_name"], "node-a")

    async def test_custom_score_accepts_personal_tuple_shape(self):
        runtime = CaptchaRuntime(db=object())
        runtime._browser_service = SimpleNamespace(
            get_custom_score=AsyncMock(
                return_value=(
                    {"token": "score-token", "verify_result": {"success": True}},
                    "personal-custom:def456",
                )
            ),
            close=AsyncMock(),
        )
        runtime._service_mode = "personal"

        with patch("src.services.captcha_runtime.config", SimpleNamespace(cluster_role="subnode", captcha_method="personal", node_name="node-a")):
            payload = await runtime.custom_score(
                website_url="https://example.com",
                website_key="site-key",
                verify_url="https://example.com/verify",
                action="homepage",
                enterprise=False,
            )

        self.assertEqual(payload["browser_id"], "personal-custom:def456")
        self.assertEqual(payload["verify_result"], {"success": True})
        self.assertEqual(payload["node_name"], "node-a")

    async def test_mark_error_routes_to_personal_flow_error(self):
        runtime = CaptchaRuntime(db=object())
        fake_service = SimpleNamespace(
            report_flow_error=AsyncMock(),
            close=AsyncMock(),
        )
        runtime._browser_service = fake_service
        runtime._service_mode = "personal"

        await runtime.registry.create(
            session_id="sess-1",
            browser_id="personal:project-a",
            api_key_id=1,
            project_id="project-a",
            action="IMAGE_GENERATION",
        )

        with patch("src.services.captcha_runtime.config", SimpleNamespace(cluster_role="subnode", captcha_method="personal")):
            ok, message, entry = await runtime.mark_error("sess-1", "upstream_failed")

        self.assertTrue(ok)
        self.assertEqual(message, "ok")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.status, "error")
        fake_service.report_flow_error.assert_awaited_once_with(
            "project-a",
            error_reason="upstream_failed",
            error_message="sess-1",
        )


if __name__ == "__main__":
    unittest.main()
