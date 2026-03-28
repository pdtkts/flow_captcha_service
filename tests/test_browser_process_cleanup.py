import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.services.browser_captcha import TokenBrowser


class DummyProc:
    def __init__(self, pid=None):
        self.pid = pid
        self.returncode = None
        self.terminate_calls = 0
        self.kill_calls = 0
        self.wait_calls = 0

    def terminate(self):
        self.terminate_calls += 1
        self.returncode = 0

    def kill(self):
        self.kill_calls += 1
        self.returncode = -9

    async def wait(self):
        self.wait_calls += 1
        return self.returncode


class BrowserProcessCleanupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.browser = TokenBrowser(7, "tmp/test-browser-cleanup")

    def test_extract_driver_proc_and_pid(self):
        proc = SimpleNamespace(pid=123, returncode=None)
        browser_obj = SimpleNamespace(
            _impl_obj=SimpleNamespace(
                _connection=SimpleNamespace(
                    _transport=SimpleNamespace(_proc=proc)
                )
            )
        )

        self.assertIs(self.browser._extract_driver_proc(browser=browser_obj), proc)
        self.assertEqual(self.browser._extract_driver_pid(browser=browser_obj), 123)

    def test_pid_looks_like_playwright_driver(self):
        with patch.object(
            self.browser,
            "_get_pid_command_line",
            return_value="node playwright-cli.js run-driver",
        ):
            self.assertTrue(self.browser._pid_looks_like_playwright_driver(100))

    async def test_terminate_driver_proc_graceful(self):
        proc = DummyProc()
        with patch.object(self.browser, "_reap_pid_if_direct_child", return_value=False):
            await self.browser._terminate_driver_proc(proc, reason="unit_test", timeout_seconds=0.01)
        self.assertEqual(proc.terminate_calls, 1)
        self.assertEqual(proc.kill_calls, 0)
        self.assertGreaterEqual(proc.wait_calls, 1)

    async def test_terminate_driver_proc_kills_after_timeout(self):
        proc = DummyProc()
        with patch.object(self.browser, "_wait_process_exit", AsyncMock(side_effect=[False, True])):
            with patch.object(self.browser, "_reap_pid_if_direct_child", return_value=False):
                await self.browser._terminate_driver_proc(proc, reason="unit_test_timeout", timeout_seconds=0.01)
        self.assertEqual(proc.terminate_calls, 1)
        self.assertEqual(proc.kill_calls, 1)

    async def test_cleanup_stale_slot_process_merges_marker_and_pid_file(self):
        terminated = []
        with patch.object(self.browser, "_list_slot_process_pids", return_value=[11]):
            with patch.object(self.browser, "_read_pid_file", return_value=22):
                with patch.object(self.browser, "_is_pid_running", return_value=True):
                    with patch.object(self.browser, "_pid_matches_slot", return_value=False):
                        with patch.object(self.browser, "_pid_looks_like_playwright_driver", return_value=True):
                            with patch.object(
                                self.browser,
                                "_terminate_pid",
                                AsyncMock(side_effect=lambda pid, reason: terminated.append((pid, reason))),
                            ):
                                with patch.object(self.browser, "_write_pid_file") as write_pid_file:
                                    await self.browser._cleanup_stale_slot_process()

        self.assertEqual(terminated, [(11, "stale_slot_process"), (22, "stale_slot_process")])
        write_pid_file.assert_called_once_with(None)

    async def test_custom_token_uses_fresh_browser_path(self):
        context = object()
        with patch.object(
            self.browser,
            "_open_fresh_browser_context",
            AsyncMock(return_value=(None, None, context)),
        ) as open_fresh_browser:
            with patch.object(
                self.browser,
                "_execute_custom_captcha",
                AsyncMock(return_value="custom-token"),
            ) as execute_custom_captcha:
                with patch.object(
                    self.browser,
                    "_close_fresh_browser_context",
                    AsyncMock(),
                ) as close_fresh_browser:
                    token = await self.browser.get_custom_token(
                        website_url="https://example.com",
                        website_key="site-key",
                        action="homepage",
                    )

        self.assertEqual(token, "custom-token")
        open_fresh_browser.assert_awaited_once()
        close_fresh_browser.assert_awaited_once()
        execute_custom_captcha.assert_awaited_once()
        self.assertFalse(execute_custom_captcha.await_args.kwargs["reuse_ready_page"])

    async def test_custom_token_failures_retry_with_fresh_browser(self):
        context = object()
        with patch.object(
            self.browser,
            "_open_fresh_browser_context",
            AsyncMock(return_value=(None, None, context)),
        ):
            with patch.object(
                self.browser,
                "_execute_custom_captcha",
                AsyncMock(side_effect=[None, None, None]),
            ):
                with patch.object(
                    self.browser,
                    "_close_fresh_browser_context",
                    AsyncMock(),
                ) as close_fresh_browser:
                    token = await self.browser.get_custom_token(
                        website_url="https://example.com",
                        website_key="site-key",
                        action="homepage",
                    )

        self.assertIsNone(token)
        self.assertGreaterEqual(close_fresh_browser.await_count, 3)


if __name__ == "__main__":
    unittest.main()
