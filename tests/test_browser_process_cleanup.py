import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.services.browser_captcha import TokenBrowser
from src.services.browser_captcha_personal import BrowserCaptchaService


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


class DummyTransport:
    def __init__(self):
        self.close_calls = 0

    def close(self):
        self.close_calls += 1


class DummyWriter:
    def __init__(self):
        self.close_calls = 0
        self._transport = DummyTransport()

    def close(self):
        self.close_calls += 1


class DummyReader:
    def __init__(self):
        self._transport = DummyTransport()


class DummyPipeProto:
    def __init__(self):
        self.pipe = DummyTransport()


class DummySubprocessTransport:
    def __init__(self):
        self.close_calls = 0
        self._pipes = {
            0: DummyPipeProto(),
            1: DummyPipeProto(),
        }
        self._proc = object()

    def close(self):
        self.close_calls += 1


class DummyProcWithPipes(DummyProc):
    def __init__(self, pid=None):
        super().__init__(pid=pid)
        self.stdin = DummyWriter()
        self.stdout = DummyReader()
        self.stderr = DummyReader()
        self._transport = DummySubprocessTransport()


class DummyPersonalBrowser:
    def __init__(self, proc, connection):
        self._process = proc
        self.connection = connection
        self.stop_calls = 0

    def stop(self):
        self.stop_calls += 1


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

    async def test_terminate_driver_proc_detaches_asyncio_pipe_refs_after_wait(self):
        proc = DummyProcWithPipes()
        subprocess_transport = proc._transport
        with patch.object(self.browser, "_reap_pid_if_direct_child", return_value=False):
            await self.browser._terminate_driver_proc(proc, reason="unit_test_detach", timeout_seconds=0.01)

        self.assertEqual(proc.stdin, None)
        self.assertEqual(proc.stdout, None)
        self.assertEqual(proc.stderr, None)
        self.assertEqual(proc._transport, None)
        self.assertEqual(subprocess_transport.close_calls, 1)
        self.assertEqual(subprocess_transport._pipes, {})
        self.assertIsNone(subprocess_transport._proc)

    def test_detach_playwright_driver_refs_clears_bound_proc(self):
        proc = DummyProcWithPipes()
        transport = SimpleNamespace(_proc=proc)
        playwright = SimpleNamespace(
            _impl_obj=SimpleNamespace(
                _connection=SimpleNamespace(_transport=transport)
            )
        )
        browser_obj = SimpleNamespace(
            _impl_obj=SimpleNamespace(
                _connection=SimpleNamespace(_transport=transport)
            )
        )

        self.browser._detach_playwright_driver_refs(playwright=playwright, browser=browser_obj, proc=proc)

        self.assertIsNone(transport._proc)

    def test_detach_playwright_connection_resources_clears_output_and_proc_refs(self):
        proc = DummyProcWithPipes()
        subprocess_transport = proc._transport
        output_writer = DummyWriter()
        output_transport = output_writer._transport
        transport = SimpleNamespace(_output=output_writer, _proc=proc)
        playwright = SimpleNamespace(
            _impl_obj=SimpleNamespace(
                _connection=SimpleNamespace(_transport=transport)
            )
        )

        self.browser._detach_playwright_connection_resources(playwright=playwright)

        self.assertIsNone(transport._output)
        self.assertIsNone(transport._proc)
        self.assertEqual(output_writer.close_calls, 1)
        self.assertEqual(output_transport.close_calls, 1)
        self.assertIsNone(proc.stdin)
        self.assertIsNone(proc.stdout)
        self.assertIsNone(proc.stderr)
        self.assertIsNone(proc._transport)
        self.assertEqual(subprocess_transport._pipes, {})
        self.assertIsNone(subprocess_transport._proc)

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

    async def test_close_fresh_browser_context_waits_before_close(self):
        with patch.object(self.browser, "_drop_shared_ready_page", AsyncMock()):
            with patch.object(self.browser, "_close_browser", AsyncMock()) as close_browser_mock:
                with patch("src.services.browser_captcha.asyncio.sleep", AsyncMock()) as sleep_mock:
                    await self.browser._close_fresh_browser_context(
                        playwright=object(),
                        browser=object(),
                        context=object(),
                        close_wait_seconds=3.0,
                    )

        sleep_mock.assert_awaited_once_with(3.0)
        close_browser_mock.assert_awaited_once()

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
                with patch.object(self.browser, "_schedule_background_close") as schedule_close:
                    token = await self.browser.get_custom_token(
                        website_url="https://example.com",
                        website_key="site-key",
                        action="homepage",
                    )

        self.assertEqual(token, "custom-token")
        open_fresh_browser.assert_awaited_once()
        schedule_close.assert_called_once()
        self.assertEqual(schedule_close.call_args.kwargs["close_wait_seconds"], 3.0)
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


class BrowserPersonalProcessCleanupTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.service = BrowserCaptchaService()

    async def test_stop_browser_process_disconnects_and_waits_before_detach(self):
        proc = DummyProcWithPipes()
        subprocess_transport = proc._transport
        connection = SimpleNamespace(
            disconnect=AsyncMock(),
            _websocket=object(),
        )
        browser = DummyPersonalBrowser(proc=proc, connection=connection)

        async def passthrough(awaitable, timeout_seconds: float, label: str):
            _ = timeout_seconds
            _ = label
            return await awaitable

        with patch.object(self.service, "_run_with_timeout", AsyncMock(side_effect=passthrough)):
            await self.service._stop_browser_process(browser)

        connection.disconnect.assert_awaited_once()
        self.assertEqual(browser.stop_calls, 1)
        self.assertEqual(proc.wait_calls, 1)
        self.assertIsNone(browser._process)
        self.assertIsNone(connection._websocket)
        self.assertIsNone(proc.stdin)
        self.assertIsNone(proc.stdout)
        self.assertIsNone(proc.stderr)
        self.assertIsNone(proc._transport)
        self.assertEqual(subprocess_transport.close_calls, 1)
        self.assertEqual(subprocess_transport._pipes, {})
        self.assertIsNone(subprocess_transport._proc)

    async def test_wait_browser_process_exit_kills_stuck_process(self):
        proc = DummyProcWithPipes()

        async def hang_wait():
            proc.wait_calls += 1
            if proc.wait_calls == 1:
                await asyncio.sleep(1)
            proc.returncode = 0
            return 0

        proc.wait = hang_wait

        real_wait_for = asyncio.wait_for

        async def fake_wait_for(awaitable, timeout=None):
            if proc.wait_calls == 0:
                proc.wait_calls += 1
                try:
                    awaitable.close()
                except Exception:
                    pass
                raise asyncio.TimeoutError()
            return await real_wait_for(awaitable, timeout=timeout)

        with patch("src.services.browser_captcha_personal.asyncio.wait_for", side_effect=fake_wait_for):
            await self.service._wait_browser_process_exit(proc, timeout_seconds=0.01)

        self.assertEqual(proc.kill_calls, 1)
        self.assertGreaterEqual(proc.wait_calls, 2)


if __name__ == "__main__":
    unittest.main()
