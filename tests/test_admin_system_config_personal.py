import unittest

from fastapi import HTTPException

from src.api.admin import _sanitize_system_config_updates


class AdminSystemConfigPersonalTests(unittest.TestCase):
    def test_accepts_personal_mode_and_extended_personal_fields(self):
        updates, changed_keys = _sanitize_system_config_updates(
            {
                "captcha": {
                    "captcha_method": "personal",
                    "personal_project_pool_size": 7,
                    "personal_max_resident_tabs": 9,
                    "personal_idle_tab_ttl_seconds": 1800,
                    "browser_score_test_settle_seconds": 4.5,
                    "browser_personal_recreate_threshold": 4,
                    "browser_personal_restart_threshold": 6,
                }
            }
        )

        self.assertEqual(
            updates["captcha"],
            {
                "captcha_method": "personal",
                "personal_project_pool_size": 7,
                "personal_max_resident_tabs": 9,
                "personal_idle_tab_ttl_seconds": 1800,
                "browser_score_test_settle_seconds": 4.5,
                "browser_personal_recreate_threshold": 4,
                "browser_personal_restart_threshold": 6,
            },
        )
        self.assertIn("captcha.captcha_method", changed_keys)
        self.assertIn("captcha.browser_score_test_settle_seconds", changed_keys)
        self.assertIn("captcha.browser_personal_restart_threshold", changed_keys)

    def test_rejects_invalid_personal_restart_threshold(self):
        with self.assertRaises(HTTPException) as ctx:
            _sanitize_system_config_updates(
                {"captcha": {"browser_personal_restart_threshold": 1}}
            )

        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("captcha.browser_personal_restart_threshold", str(ctx.exception.detail))


if __name__ == "__main__":
    unittest.main()
