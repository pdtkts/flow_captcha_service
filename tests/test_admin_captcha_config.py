import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.api import admin as admin_api
from src.core.database import Database
from src.core.models import CaptchaConfig, UpdateCaptchaConfigRequest


class DatabaseCaptchaConfigTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.temp_dir.name) / "captcha-config.sqlite3")
        await self.db.init_db()

    async def asyncTearDown(self):
        await self.db.close()
        self.temp_dir.cleanup()

    async def test_update_and_get_captcha_config_persists_mode_and_personal_fields(self):
        await self.db.update_captcha_config(
            captcha_method="personal",
            browser_proxy_enabled=True,
            browser_proxy_url="http://127.0.0.1:8080",
            browser_count=3,
            personal_project_pool_size=7,
            personal_max_resident_tabs=9,
            personal_idle_tab_ttl_seconds=1800,
        )

        cfg = await self.db.get_captcha_config()

        self.assertEqual(cfg.captcha_method, "personal")
        self.assertTrue(cfg.browser_proxy_enabled)
        self.assertEqual(cfg.browser_proxy_url, "http://127.0.0.1:8080")
        self.assertEqual(cfg.browser_count, 3)
        self.assertEqual(cfg.personal_project_pool_size, 7)
        self.assertEqual(cfg.personal_max_resident_tabs, 9)
        self.assertEqual(cfg.personal_idle_tab_ttl_seconds, 1800)


class AdminCaptchaConfigRouteTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_captcha_config_exposes_mode_and_personal_fields(self):
        fake_db = SimpleNamespace(
            get_captcha_config=AsyncMock(
                return_value=CaptchaConfig(
                    browser_proxy_enabled=True,
                    browser_proxy_url="http://127.0.0.1:8080",
                    browser_count=2,
                )
            )
        )

        with patch.object(
            admin_api,
            "config",
            SimpleNamespace(
                cluster_role="standalone",
                captcha_method="personal",
                personal_project_pool_size=6,
                personal_max_resident_tabs=8,
                personal_idle_tab_ttl_seconds=1200,
            ),
        ):
            with patch.object(admin_api, "_db", fake_db):
                payload = await admin_api.get_captcha_config(token="test-token")

        self.assertTrue(payload["success"])
        self.assertEqual(payload["captcha_method"], "personal")
        self.assertEqual(payload["browser_count"], 2)
        self.assertEqual(payload["personal_project_pool_size"], 6)
        self.assertEqual(payload["personal_max_resident_tabs"], 8)
        self.assertEqual(payload["personal_idle_tab_ttl_seconds"], 1200)

    async def test_update_captcha_config_syncs_config_db_and_runtime(self):
        fake_db = SimpleNamespace(
            update_captcha_config=AsyncMock(),
            get_captcha_config=AsyncMock(
                side_effect=[
                    CaptchaConfig(
                        browser_proxy_enabled=False,
                        browser_proxy_url=None,
                        browser_count=1,
                    ),
                    CaptchaConfig(
                        captcha_method="personal",
                        browser_proxy_enabled=False,
                        browser_proxy_url=None,
                        browser_count=4,
                        personal_project_pool_size=7,
                        personal_max_resident_tabs=9,
                        personal_idle_tab_ttl_seconds=1800,
                    ),
                ]
            ),
        )
        fake_runtime = SimpleNamespace(
            reload_browser_count=AsyncMock(),
            refresh_browser_warmup_settings=AsyncMock(),
        )
        fake_config = SimpleNamespace(
            cluster_role="standalone",
            captcha_method="personal",
            personal_project_pool_size=7,
            personal_max_resident_tabs=9,
            personal_idle_tab_ttl_seconds=1800,
            update_config_sections=MagicMock(),
        )

        request = UpdateCaptchaConfigRequest(
            captcha_method="personal",
            browser_proxy_enabled=False,
            browser_proxy_url="",
            browser_count=4,
            personal_project_pool_size=7,
            personal_max_resident_tabs=9,
            personal_idle_tab_ttl_seconds=1800,
        )

        with patch.object(admin_api, "config", fake_config):
            with patch.object(admin_api, "_db", fake_db):
                with patch.object(admin_api, "_runtime", fake_runtime):
                    payload = await admin_api.update_captcha_config(request, token="test-token")

        fake_config.update_config_sections.assert_called_once_with(
            {
                "captcha": {
                    "captcha_method": "personal",
                    "personal_project_pool_size": 7,
                    "personal_max_resident_tabs": 9,
                    "personal_idle_tab_ttl_seconds": 1800,
                }
            }
        )
        fake_db.update_captcha_config.assert_awaited_once_with(
            captcha_method="personal",
            browser_proxy_enabled=False,
            browser_proxy_url=None,
            browser_count=4,
            personal_project_pool_size=7,
            personal_max_resident_tabs=9,
            personal_idle_tab_ttl_seconds=1800,
        )
        fake_runtime.reload_browser_count.assert_awaited_once()
        fake_runtime.refresh_browser_warmup_settings.assert_awaited_once()
        self.assertEqual(payload["captcha_method"], "personal")
        self.assertEqual(payload["personal_project_pool_size"], 7)
        self.assertEqual(payload["personal_max_resident_tabs"], 9)
        self.assertEqual(payload["personal_idle_tab_ttl_seconds"], 1800)


if __name__ == "__main__":
    unittest.main()
