import os
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

from src.core.config import Config


class BrowserConfigSettingsTests(unittest.TestCase):
    @contextmanager
    def _config_context(self, env_overrides: dict[str, str]):
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "setting.toml"
            config_path.write_text("[captcha]\n", encoding="utf-8")
            with patch.dict(
                os.environ,
                {
                    "FCS_CONFIG_FILE": str(config_path),
                    **env_overrides,
                },
                clear=False,
            ):
                yield Config()

    def test_execute_timeout_env_zero_keeps_auto_mode(self):
        with self._config_context({"FCS_BROWSER_EXECUTE_TIMEOUT_SECONDS": "0"}) as config:
            self.assertEqual(config.browser_execute_timeout_seconds, 0.0)

    def test_reload_and_clr_wait_env_zero_disable_wait(self):
        with self._config_context(
            {
                "FCS_BROWSER_RELOAD_WAIT_TIMEOUT_SECONDS": "0",
                "FCS_BROWSER_CLR_WAIT_TIMEOUT_SECONDS": "0",
            }
        ) as config:
            self.assertEqual(config.browser_reload_wait_timeout_seconds, 0.0)
            self.assertEqual(config.browser_clr_wait_timeout_seconds, 0.0)

    def test_standby_bucket_idle_ttl_env_zero_keeps_auto_mode(self):
        with self._config_context({"FCS_BROWSER_STANDBY_BUCKET_IDLE_TTL_SECONDS": "0"}) as config:
            self.assertEqual(config.browser_standby_bucket_idle_ttl_seconds, 0.0)

    def test_flow_website_key_and_native_warmup_env_override(self):
        with self._config_context(
            {
                "FCS_BROWSER_FLOW_WEBSITE_KEY": "site-key-from-env",
                "FCS_BROWSER_AUTO_WARM_PROJECT_ID": "project-from-env",
                "FCS_BROWSER_AUTO_WARMUP_ACTION": "video_generation",
            }
        ) as config:
            self.assertEqual(config.browser_flow_website_key, "site-key-from-env")
            self.assertEqual(config.browser_auto_warm_project_id, "project-from-env")
            self.assertEqual(config.browser_auto_warmup_action, "VIDEO_GENERATION")

    def test_custom_warm_target_defaults_and_env_override(self):
        with self._config_context({}) as config:
            self.assertEqual(config.browser_auto_warm_action, "homepage")

        with self._config_context(
            {
                "FCS_BROWSER_AUTO_WARM_WEBSITE_URL": "https://example.com/login",
                "FCS_BROWSER_AUTO_WARM_WEBSITE_KEY": "site-key-custom",
                "FCS_BROWSER_AUTO_WARM_ACTION": "login",
            }
        ) as config:
            self.assertEqual(config.browser_auto_warm_website_url, "https://example.com/login")
            self.assertEqual(config.browser_auto_warm_website_key, "site-key-custom")
            self.assertEqual(config.browser_auto_warm_action, "login")

    def test_personal_mode_defaults_and_env_override(self):
        with self._config_context({}) as config:
            self.assertEqual(config.captcha_method, "browser")
            self.assertEqual(config.personal_project_pool_size, 4)
            self.assertEqual(config.personal_max_resident_tabs, 5)
            self.assertEqual(config.personal_idle_tab_ttl_seconds, 600)
            self.assertEqual(config.browser_score_test_settle_seconds, 2.5)
            self.assertEqual(config.browser_personal_recreate_threshold, 2)
            self.assertEqual(config.browser_personal_restart_threshold, 3)

        with self._config_context(
            {
                "FCS_CAPTCHA_METHOD": "personal",
                "FCS_PERSONAL_PROJECT_POOL_SIZE": "7",
                "FCS_PERSONAL_MAX_RESIDENT_TABS": "9",
                "FCS_PERSONAL_IDLE_TAB_TTL_SECONDS": "1800",
                "FCS_BROWSER_SCORE_TEST_SETTLE_SECONDS": "4.5",
                "FCS_BROWSER_PERSONAL_RECREATE_THRESHOLD": "4",
                "FCS_BROWSER_PERSONAL_RESTART_THRESHOLD": "6",
            }
        ) as config:
            self.assertEqual(config.captcha_method, "personal")
            self.assertEqual(config.personal_project_pool_size, 7)
            self.assertEqual(config.personal_max_resident_tabs, 9)
            self.assertEqual(config.personal_idle_tab_ttl_seconds, 1800)
            self.assertEqual(config.browser_score_test_settle_seconds, 4.5)
            self.assertEqual(config.browser_personal_recreate_threshold, 4)
            self.assertEqual(config.browser_personal_restart_threshold, 6)


if __name__ == "__main__":
    unittest.main()
