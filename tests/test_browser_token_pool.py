import asyncio
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from src.services.browser_captcha import (
    BrowserCaptchaService,
    BrowserProfile,
    StandbyTokenEntry,
    TokenAcquireResult,
    TokenBrowser,
    _build_user_agent_pool,
)


class BrowserTokenPoolTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.service = BrowserCaptchaService()
        self.bucket_key = "project-a|IMAGE_GENERATION|-"

    async def test_expired_entry_does_not_hit(self):
        now_value = time.monotonic()
        self.service._standby_tokens[self.bucket_key] = [
            StandbyTokenEntry(
                token="expired-token",
                browser_id=1,
                fingerprint={"user_agent": "ua-expired"},
                browser_epoch=3,
                project_id="project-a",
                action="IMAGE_GENERATION",
                proxy_signature="-",
                created_monotonic=now_value - 10,
                expires_monotonic=now_value - 1,
            )
        ]

        with patch.object(self.service, "_get_browser_epoch_for_standby", return_value=3):
            result = await self.service._take_standby_token(self.bucket_key)

        self.assertIsNone(result)
        self.assertNotIn(self.bucket_key, self.service._standby_tokens)

    async def test_hit_pops_entry_from_pool(self):
        now_value = time.monotonic()
        self.service._standby_tokens[self.bucket_key] = [
            StandbyTokenEntry(
                token="warm-token",
                browser_id=2,
                fingerprint={"user_agent": "ua-live"},
                browser_epoch=5,
                project_id="project-a",
                action="IMAGE_GENERATION",
                proxy_signature="-",
                created_monotonic=now_value,
                expires_monotonic=now_value + 30,
            )
        ]

        with patch.object(self.service, "_get_browser_epoch_for_standby", return_value=5):
            result = await self.service._take_standby_token(self.bucket_key)

        self.assertIsNotNone(result)
        self.assertEqual(result.token, "warm-token")
        self.assertEqual(result.browser_ref, 2)
        self.assertEqual(result.browser_epoch, 5)
        self.assertEqual(result.fingerprint, {"user_agent": "ua-live"})
        self.assertNotIn(self.bucket_key, self.service._standby_tokens)

    async def test_epoch_mismatch_keeps_fresh_entry_usable(self):
        now_value = time.monotonic()
        self.service._standby_tokens[self.bucket_key] = [
            StandbyTokenEntry(
                token="stale-token",
                browser_id=4,
                fingerprint={"user_agent": "ua-stale"},
                browser_epoch=7,
                project_id="project-a",
                action="IMAGE_GENERATION",
                proxy_signature="-",
                created_monotonic=now_value,
                expires_monotonic=now_value + 30,
            )
        ]

        with patch.object(self.service, "_get_browser_epoch_for_standby", return_value=8):
            result = await self.service._take_standby_token(self.bucket_key)

        self.assertIsNotNone(result)
        self.assertEqual(result.token, "stale-token")
        self.assertNotIn(self.bucket_key, self.service._standby_tokens)

    async def test_missing_browser_epoch_keeps_entry_usable(self):
        now_value = time.monotonic()
        self.service._standby_tokens[self.bucket_key] = [
            StandbyTokenEntry(
                token="warm-token",
                browser_id=5,
                fingerprint={"user_agent": "ua-live"},
                browser_epoch=9,
                project_id="project-a",
                action="IMAGE_GENERATION",
                proxy_signature="-",
                created_monotonic=now_value,
                expires_monotonic=now_value + 30,
            )
        ]

        with patch.object(self.service, "_get_browser_epoch_for_standby", return_value=None):
            result = await self.service._take_standby_token(self.bucket_key)

        self.assertIsNotNone(result)
        self.assertEqual(result.token, "warm-token")
        self.assertEqual(result.browser_ref, 5)
        self.assertNotIn(self.bucket_key, self.service._standby_tokens)

    async def test_custom_token_uses_fresh_browser_path(self):
        browser = TokenBrowser(3, "tmp/test-custom-token-shared")
        fake_context = object()

        with patch.object(
            browser,
            "_open_fresh_browser_context",
            AsyncMock(return_value=(object(), object(), fake_context)),
        ) as open_fresh_browser:
            with patch.object(browser, "_schedule_background_close") as schedule_close:
                with patch.object(
                    browser,
                    "_execute_custom_captcha",
                    AsyncMock(return_value="shared-custom-token"),
                ) as execute_mock:
                    token = await browser.get_custom_token(
                        website_url="https://example.com/login",
                        website_key="site-key",
                        action="login",
                    )

        self.assertEqual(token, "shared-custom-token")
        open_fresh_browser.assert_awaited_once()
        schedule_close.assert_called_once()
        self.assertEqual(schedule_close.call_args.kwargs["close_wait_seconds"], 3.0)
        execute_mock.assert_awaited_once()
        self.assertFalse(bool(execute_mock.await_args.kwargs["reuse_ready_page"]))

    async def test_get_token_success_reuses_shared_browser_when_defer_requested(self):
        browser = TokenBrowser(31, "tmp/test-native-close-delay")
        fake_context = object()
        fake_result = TokenAcquireResult(
            token="native-token",
            browser_ref=None,
            browser_id=None,
            fingerprint={"user_agent": "ua-native"},
            source="live",
            elapsed_ms=12,
            browser_epoch=1,
            timings={},
        )

        with patch.object(
            browser,
            "_get_or_create_shared_browser",
            AsyncMock(return_value=(object(), object(), fake_context)),
        ) as get_shared_browser:
            with patch.object(browser, "_execute_captcha", AsyncMock(return_value=fake_result)):
                with patch.object(browser, "_defer_browser_close_until_request_done", AsyncMock()) as defer_close:
                    result = await browser.get_token(
                        project_id="project-a",
                        website_key="site-key",
                        action="IMAGE_GENERATION",
                        defer_close_until_request_done=True,
                    )

        self.assertEqual(result.token, "native-token")
        self.assertEqual(result.browser_ref, 31)
        self.assertEqual(result.browser_id, 31)
        get_shared_browser.assert_awaited_once()
        defer_close.assert_not_awaited()

    async def test_report_request_finished_notifies_pending_request_ref(self):
        browser = SimpleNamespace(
            notify_generation_request_finished=AsyncMock(),
            has_shared_browser=lambda: False,
        )
        self.service._browsers[9] = browser

        await self.service.report_request_finished("9:request-xyz")

        browser.notify_generation_request_finished.assert_awaited_once_with(request_ref="request-xyz")

    async def test_custom_page_cache_hits_same_site(self):
        browser = TokenBrowser(4, "tmp/test-custom-page-cache")
        website_url = "https://example.com/login"
        website_key = "site-key"
        custom_key = browser._build_custom_page_key(
            website_url=website_url,
            website_key=website_key,
            captcha_type="recaptcha_v3",
            enterprise=False,
        )

        class FakePage:
            def is_closed(self):
                return False

            async def evaluate(self, _expression):
                return True

        fake_page = FakePage()
        browser._shared_custom_pages[custom_key] = fake_page
        browser._shared_custom_page_last_used[custom_key] = time.monotonic()

        class FakeContext:
            async def new_page(self):
                raise AssertionError("cache hit should not create a new page")

        page, resolved_key, runtime, ready_hit = await browser._get_or_create_custom_page(
            FakeContext(),
            website_url=website_url,
            website_key=website_key,
            captcha_type="recaptcha_v3",
            enterprise=False,
        )

        self.assertIs(page, fake_page)
        self.assertEqual(resolved_key, custom_key)
        self.assertEqual(runtime["normalized_type"], "recaptcha_v3")
        self.assertTrue(ready_hit)

    async def test_custom_page_cache_evicts_stale_entries(self):
        browser = TokenBrowser(5, "tmp/test-custom-page-cache-stale")

        class FakePage:
            def __init__(self):
                self.closed = False

            def is_closed(self):
                return self.closed

            async def close(self):
                self.closed = True

        stale_page = FakePage()
        hot_page = FakePage()
        browser._shared_custom_pages = {"stale": stale_page, "hot": hot_page}
        browser._shared_custom_page_last_used = {"stale": 1.0, "hot": time.monotonic()}

        with patch.object(browser, "_custom_page_idle_ttl_seconds", return_value=0.01):
            await browser._trim_shared_custom_pages(keep_key="hot", max_pages=2)

        self.assertNotIn("stale", browser._shared_custom_pages)
        self.assertTrue(stale_page.closed)
        self.assertIn("hot", browser._shared_custom_pages)

    async def test_execute_custom_captcha_waits_for_recaptcha_network_ready(self):
        browser = TokenBrowser(54, "tmp/test-custom-network-wait")

        class FakePage:
            def __init__(self):
                self.closed = False

            async def add_init_script(self, *args, **kwargs):
                return None

            async def evaluate(self, expression, *args):
                if "resolve(t)" in str(expression):
                    return "custom-token"
                return None

            def on(self, *_args, **_kwargs):
                return None

            async def close(self):
                self.closed = True

        class FakeContext:
            async def new_page(self):
                return fake_page

        fake_page = FakePage()
        fake_context = FakeContext()
        reload_event = asyncio.Event()
        clr_event = asyncio.Event()

        with patch.object(browser, "_install_custom_page_hook", AsyncMock()) as install_hook_mock:
            with patch.object(browser, "_prepare_custom_page", AsyncMock()) as prepare_mock:
                with patch.object(browser, "_capture_page_fingerprint", AsyncMock()) as capture_mock:
                    with patch.object(browser, "_attach_recaptcha_network_waiters", return_value=(reload_event, clr_event)) as attach_mock:
                        with patch.object(browser, "_wait_recaptcha_network_ready", AsyncMock(return_value={"reload_wait_ms": 1, "clr_wait_ms": 1})) as wait_mock:
                            with patch.object(browser, "_recaptcha_settle_seconds", return_value=0.0):
                                token = await browser._execute_custom_captcha(
                                    context=fake_context,
                                    website_url="https://example.com/login",
                                    website_key="site-key",
                                    action="login",
                                    enterprise=False,
                                    captcha_type="recaptcha_v3",
                                    is_invisible=True,
                                    reuse_ready_page=False,
                                )

        self.assertEqual(token, "custom-token")
        install_hook_mock.assert_awaited_once()
        prepare_mock.assert_awaited_once()
        capture_mock.assert_awaited_once()
        attach_mock.assert_called_once_with(fake_page, "site-key")
        wait_mock.assert_awaited_once_with(
            reload_ok_event=reload_event,
            clr_ok_event=clr_event,
            log_prefix=f"[BrowserCaptcha] Token-{browser.token_id} 自定义打码",
        )
        self.assertTrue(fake_page.closed)

    async def test_inject_custom_page_scripts_passes_single_argument_object(self):
        browser = TokenBrowser(52, "tmp/test-custom-page-inject")
        evaluate_calls = []

        class FakePage:
            async def evaluate(self, expression, arg=None):
                evaluate_calls.append((expression, arg))

        runtime = {
            "is_turnstile": False,
            "primary_host": "https://www.google.com",
            "secondary_host": "https://www.recaptcha.net",
            "script_path": "/recaptcha/api.js",
            "render_value": "site-key",
        }

        await browser._inject_custom_page_scripts(FakePage(), runtime)

        self.assertEqual(len(evaluate_calls), 1)
        _, payload = evaluate_calls[0]
        self.assertEqual(
            payload,
            {
                "primaryUrl": "https://www.google.com//recaptcha/api.js?render=site-key",
                "secondaryUrl": "https://www.recaptcha.net//recaptcha/api.js?render=site-key",
            },
        )

    async def test_install_custom_page_hook_passes_single_script(self):
        browser = TokenBrowser(53, "tmp/test-custom-page-hook")
        add_init_calls = []

        class FakePage:
            async def add_init_script(self, script=None, path=None):
                add_init_calls.append((script, path))

        runtime = {
            "is_turnstile": False,
            "primary_host": "https://www.google.com",
            "secondary_host": "https://www.recaptcha.net",
            "script_path": "/recaptcha/api.js",
            "render_value": "site-key",
        }

        await browser._install_custom_page_hook(FakePage(), runtime)

        self.assertEqual(len(add_init_calls), 1)
        script, path = add_init_calls[0]
        self.assertIsNone(path)
        self.assertIn("https://www.google.com//recaptcha/api.js?render=site-key", script)
        self.assertIn("https://www.recaptcha.net//recaptcha/api.js?render=site-key", script)

    async def test_user_agent_pool_expanded_by_one_hundred(self):
        expected_total = len(TokenBrowser._BASE_UA_LIST) + TokenBrowser.UA_POOL_EXTRA_COUNT
        expected_compatible_total = sum(
            1 for user_agent in TokenBrowser.UA_LIST if TokenBrowser._is_windows_chromium_user_agent(user_agent)
        )
        self.assertEqual(len(TokenBrowser.UA_LIST), expected_total)
        self.assertEqual(len(TokenBrowser.UA_LIST), len(set(TokenBrowser.UA_LIST)))
        browser = TokenBrowser(51, "tmp/test-default-profile-pool")
        self.assertEqual(len(browser._profile_pool), expected_compatible_total)

    async def test_profile_pool_honors_configured_extra_count(self):
        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(browser_fingerprint_pool_extra_count=5),
        ):
            browser = TokenBrowser(6, "tmp/test-profile-pool-extra")

        compatible_total = sum(
            1
            for user_agent in _build_user_agent_pool(TokenBrowser._BASE_UA_LIST, extra_count=5)
            if TokenBrowser._is_windows_chromium_user_agent(user_agent)
        )
        self.assertEqual(len(browser._profile_pool), compatible_total)

    async def test_default_profile_pool_is_reused_across_browsers(self):
        browser_a = TokenBrowser(61, "tmp/test-profile-pool-reuse-a")
        browser_b = TokenBrowser(62, "tmp/test-profile-pool-reuse-b")

        self.assertIs(browser_a._profile_pool, browser_b._profile_pool)
        self.assertIs(browser_a._profile_pool, TokenBrowser.DEFAULT_PROFILE_POOL)

    async def test_profile_pool_cache_is_reused_for_same_extra_count(self):
        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(browser_fingerprint_pool_extra_count=5),
        ):
            browser_a = TokenBrowser(63, "tmp/test-profile-pool-cache-a")
            browser_b = TokenBrowser(64, "tmp/test-profile-pool-cache-b")

        self.assertIs(browser_a._profile_pool, browser_b._profile_pool)

    async def test_profile_pool_allows_zero_extra_count(self):
        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(browser_fingerprint_pool_extra_count=0),
        ):
            browser = TokenBrowser(6, "tmp/test-profile-pool-zero-extra")

        compatible_total = sum(
            1
            for user_agent in _build_user_agent_pool(TokenBrowser._BASE_UA_LIST, extra_count=0)
            if TokenBrowser._is_windows_chromium_user_agent(user_agent)
        )
        self.assertEqual(len(browser._profile_pool), compatible_total)

    async def test_refresh_browser_profile_keeps_mobile_profile_shape(self):
        browser = TokenBrowser(7, "tmp/test-mobile-profile")
        browser._profile_pool = [
            BrowserProfile(
                user_agent="Mozilla/5.0 (iPhone; CPU iPhone OS 18_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.2 Mobile/15E148 Safari/604.1",
                viewport={"width": 430, "height": 932},
                locale="en-US",
                timezone_id="America/Los_Angeles",
                accept_language="en-US,en;q=0.9",
                device_scale_factor=3.0,
                is_mobile=True,
                has_touch=True,
                profile_family="mobile",
            )
        ]

        browser._refresh_browser_profile()

        self.assertTrue(browser._profile_is_mobile)
        self.assertTrue(browser._profile_has_touch)
        self.assertEqual(browser._profile_viewport, {"width": 430, "height": 932})
        self.assertEqual(browser._profile_timezone_id, "America/Los_Angeles")

    async def test_custom_page_cache_uses_configured_limits(self):
        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(
                browser_custom_page_cache_max_pages=7,
                browser_custom_page_idle_ttl_seconds=90,
            ),
        ):
            browser = TokenBrowser(8, "tmp/test-custom-page-config")
            self.assertEqual(browser._custom_page_cache_max_pages(), 7)
            self.assertEqual(browser._custom_page_idle_ttl_seconds(), 90.0)

    async def test_service_custom_token_uses_site_affinity_slot_selection(self):
        service = BrowserCaptchaService()

        class FakeBrowser:
            def __init__(self):
                self.get_custom_token = AsyncMock(return_value="service-token")

            def get_last_fingerprint(self):
                return {"user_agent": "fake-agent"}

            def get_browser_epoch(self):
                return 12

        fake_browser = FakeBrowser()

        with patch.object(service, "_check_available"):
            with patch.object(service, "_resolve_global_proxy_url", AsyncMock(return_value=None)):
                with patch.object(service, "_schedule_custom_standby_refill", AsyncMock()):
                    with patch.object(service, "_select_browser_id", AsyncMock(return_value=2)) as select_mock:
                        with patch.object(service, "_get_next_browser_id", side_effect=AssertionError("should not use round robin")):
                            with patch.object(service, "_get_or_create_browser", AsyncMock(return_value=fake_browser)):
                                result = await service.get_custom_token(
                                    website_url="https://example.com/login",
                                    website_key="site-key",
                                    action="login",
                                    captcha_type="recaptcha_v3",
                                )

        self.assertEqual(result.token, "service-token")
        self.assertEqual(result.browser_id, 2)
        self.assertEqual(result.fingerprint, {"user_agent": "fake-agent"})
        select_mock.assert_awaited_once()

    async def test_same_bucket_concurrent_get_token_serializes_live_acquire(self):
        service = BrowserCaptchaService()
        first_live_started = asyncio.Event()
        release_live = asyncio.Event()
        state = {"inflight": 0, "max_inflight": 0, "call_count": 0}

        async def fake_acquire_live_token(*args, **kwargs):
            state["call_count"] += 1
            state["inflight"] += 1
            state["max_inflight"] = max(state["max_inflight"], state["inflight"])
            first_live_started.set()
            try:
                await release_live.wait()
                return SimpleNamespace(
                    token=f"live-token-{state['call_count']}",
                    browser_id=1,
                    browser_ref=1,
                    fingerprint={"user_agent": "ua-live"},
                    source="live",
                    elapsed_ms=0,
                    browser_epoch=3,
                )
            finally:
                state["inflight"] -= 1

        with patch.object(service, "_check_available"):
            with patch.object(service, "_resolve_effective_proxy_url", AsyncMock(return_value=None)):
                with patch.object(service, "_schedule_standby_refill", AsyncMock()):
                    with patch.object(
                        service,
                        "_acquire_live_token",
                        AsyncMock(side_effect=fake_acquire_live_token),
                    ) as acquire_mock:
                        first_task = asyncio.create_task(
                            service.get_token(project_id="project-a", action="IMAGE_GENERATION")
                        )
                        await first_live_started.wait()
                        second_task = asyncio.create_task(
                            service.get_token(project_id="project-a", action="IMAGE_GENERATION")
                        )
                        await asyncio.sleep(0.01)
                        self.assertFalse(first_task.done())
                        self.assertFalse(second_task.done())

                        release_live.set()
                        first_result, second_result = await asyncio.gather(first_task, second_task)

        self.assertEqual(state["max_inflight"], 1)
        self.assertGreaterEqual(acquire_mock.await_count, 1)
        self.assertTrue(bool(first_result.token))
        self.assertTrue(bool(second_result.token))
        self.assertEqual(service._standby_live_tasks, {})

    async def test_get_token_hits_matching_project_warm_bucket(self):
        service = BrowserCaptchaService()
        bucket_key = service._build_standby_bucket_key("project-a", "IMAGE_GENERATION", None)

        with patch.object(service, "_check_available"):
            with patch.object(service, "_resolve_effective_proxy_url", AsyncMock(return_value=None)):
                with patch.object(service, "_schedule_standby_refill", AsyncMock()) as refill_mock:
                    with patch.object(
                        service,
                        "_take_standby_token",
                        AsyncMock(return_value=SimpleNamespace(
                            token="global-warm-token",
                            browser_id=4,
                            browser_ref=4,
                            fingerprint={"user_agent": "ua-global"},
                            source="standby",
                            elapsed_ms=0,
                            browser_epoch=2,
                        )),
                    ):
                        result = await service.get_token(project_id="project-a", action="IMAGE_GENERATION")

        self.assertEqual(result.token, "global-warm-token")
        self.assertEqual(result.fingerprint, {"user_agent": "ua-global"})
        refill_kwargs = refill_mock.await_args.kwargs
        self.assertEqual(refill_kwargs["bucket_key"], bucket_key)
        self.assertEqual(refill_kwargs["project_id"], "project-a")

    async def test_select_browser_id_reserves_slots_until_released(self):
        service = BrowserCaptchaService()
        service._browser_count = 2

        first = await service._select_browser_id("project-a")
        second = await service._select_browser_id("project-a")

        self.assertEqual({first, second}, {0, 1})
        self.assertEqual(service._slot_reservations[first], 1)
        self.assertEqual(service._slot_reservations[second], 1)

        await service._release_slot_reservation(first)
        await service._release_slot_reservation(second)

        self.assertEqual(service._slot_reservations, {})

    async def test_live_select_browser_id_prefers_warmed_idle_slot(self):
        service = BrowserCaptchaService()
        service._browser_count = 3
        service._round_robin_index = 0

        class FakeBrowser:
            def __init__(self, busy=False, warmed=False):
                self._busy = busy
                self._warmed = warmed

            def is_busy(self):
                return self._busy

            def has_shared_browser(self):
                return self._warmed

        service._browsers = {
            0: FakeBrowser(busy=False, warmed=False),
            1: FakeBrowser(busy=False, warmed=True),
            2: FakeBrowser(busy=True, warmed=True),
        }

        selected = await service._select_browser_id(
            "project-a",
            prefer_warmed_shared=True,
            use_project_affinity=False,
        )

        self.assertEqual(selected, 1)
        self.assertEqual(service._slot_reservations[selected], 1)

    async def test_warmup_browser_slots_starts_native_keep_warm_refill_with_real_project(self):
        service = BrowserCaptchaService()
        service._browser_count = 2

        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(
                browser_flow_website_key="native-key",
                browser_auto_warm_project_id="project-auto",
                browser_auto_warmup_action="IMAGE_GENERATION",
                browser_auto_warm_website_url="",
                browser_auto_warm_website_key="",
                browser_auto_warm_action="homepage",
                browser_standby_token_pool_enabled=True,
                browser_standby_token_pool_depth=2,
                browser_standby_token_ttl_seconds=60,
            ),
        ):
            with patch.object(service, "_get_or_create_browser", AsyncMock(side_effect=[object(), object()])):
                with patch.object(service, "_ensure_auto_warmup_loop", AsyncMock()) as ensure_auto_warmup_loop:
                    with patch.object(service, "_resolve_global_proxy_url", AsyncMock(return_value=None)):
                        with patch.object(service, "_schedule_standby_refill", AsyncMock()) as refill_mock:
                            with patch.object(service, "_schedule_custom_standby_refill", AsyncMock()) as custom_refill_mock:
                                await service.warmup_browser_slots()

        ensure_auto_warmup_loop.assert_awaited_once()
        self.assertEqual(refill_mock.await_count, 2)
        called_actions = [call.kwargs["action"] for call in refill_mock.await_args_list]
        self.assertEqual(called_actions, ["IMAGE_GENERATION", "VIDEO_GENERATION"])
        self.assertTrue(all(call.kwargs["project_id"] == "project-auto" for call in refill_mock.await_args_list))
        self.assertTrue(all(call.kwargs["keep_warm"] is True for call in refill_mock.await_args_list))
        custom_refill_mock.assert_not_awaited()

    async def test_warmup_browser_slots_skips_native_keep_warm_without_project_id(self):
        service = BrowserCaptchaService()

        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(
                browser_flow_website_key="native-key",
                browser_auto_warm_project_id="",
                browser_auto_warmup_action="IMAGE_GENERATION",
                browser_auto_warm_website_url="",
                browser_auto_warm_website_key="",
                browser_auto_warm_action="homepage",
                browser_standby_token_pool_enabled=True,
                browser_standby_token_pool_depth=2,
                browser_standby_token_ttl_seconds=60,
            ),
        ):
            with patch.object(service, "_get_or_create_browser", AsyncMock(return_value=object())):
                with patch.object(service, "_ensure_auto_warmup_loop", AsyncMock()):
                    with patch.object(service, "_resolve_global_proxy_url", AsyncMock(return_value=None)):
                        with patch.object(service, "_schedule_standby_refill", AsyncMock()) as refill_mock:
                            with patch.object(service, "_schedule_custom_standby_refill", AsyncMock()):
                                await service.warmup_browser_slots()

        refill_mock.assert_not_awaited()

    async def test_warmup_browser_slots_schedules_configured_custom_target(self):
        service = BrowserCaptchaService()

        with patch("src.services.browser_captcha.config", SimpleNamespace(
            browser_flow_website_key="native-key",
            browser_auto_warm_project_id="project-auto",
            browser_auto_warmup_action="IMAGE_GENERATION",
            browser_auto_warm_website_url="https://example.com/login",
            browser_auto_warm_website_key="site-key",
            browser_auto_warm_action="login",
            browser_standby_token_pool_enabled=True,
            browser_standby_token_pool_depth=2,
            browser_standby_token_ttl_seconds=60,
        )):
            with patch.object(service, "_get_or_create_browser", AsyncMock(return_value=object())):
                with patch.object(service, "_ensure_auto_warmup_loop", AsyncMock()):
                    with patch.object(service, "_resolve_global_proxy_url", AsyncMock(return_value=None)):
                        with patch.object(service, "_schedule_standby_refill", AsyncMock()):
                            with patch.object(service, "_schedule_custom_standby_refill", AsyncMock()) as custom_refill_mock:
                                await service.warmup_browser_slots()

        custom_refill_mock.assert_awaited_once()
        self.assertEqual(custom_refill_mock.await_args.kwargs["website_url"], "https://example.com/login")
        self.assertEqual(custom_refill_mock.await_args.kwargs["website_key"], "site-key")
        self.assertEqual(custom_refill_mock.await_args.kwargs["action"], "login")

    async def test_refresh_warmup_settings_updates_website_key_and_clears_pool_when_changed(self):
        service = BrowserCaptchaService()
        service.website_key = "old-site-key"
        service._warmup_settings_signature = "old-signature"

        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(
                browser_flow_website_key="new-site-key",
                browser_auto_warm_project_id="project-next",
                browser_auto_warmup_action="VIDEO_GENERATION",
                browser_auto_warm_website_url="https://example.com/login",
                browser_auto_warm_website_key="site-key",
                browser_auto_warm_action="login",
            ),
        ):
            with patch.object(service, "_clear_all_standby_tokens", AsyncMock()) as clear_pool_mock:
                with patch.object(service, "warmup_browser_slots", AsyncMock()) as warmup_mock:
                    await service.refresh_warmup_settings()

        self.assertEqual(service.website_key, "new-site-key")
        clear_pool_mock.assert_awaited_once()
        warmup_mock.assert_awaited_once()

    async def test_get_stats_exposes_bucket_signatures(self):
        service = BrowserCaptchaService()
        now_value = time.monotonic()
        service._standby_tokens["bucket-a"] = [
            StandbyTokenEntry(
                token="warm-token",
                browser_id=1,
                fingerprint={"user_agent": "ua-live"},
                browser_epoch=1,
                project_id="project-a",
                action="IMAGE_GENERATION",
                proxy_signature="-",
                created_monotonic=now_value,
                expires_monotonic=now_value + 30,
                match_signature="custom|bucket-a",
            )
        ]

        stats = service.get_stats()

        self.assertEqual(stats["standby_bucket_signatures"], ["custom|bucket-a"])

    async def test_same_bucket_concurrent_get_custom_token_serializes_live_acquire(self):
        service = BrowserCaptchaService()
        first_live_started = asyncio.Event()
        release_live = asyncio.Event()
        state = {"inflight": 0, "max_inflight": 0, "call_count": 0}

        async def fake_acquire_live_custom_token(*args, **kwargs):
            state["call_count"] += 1
            state["inflight"] += 1
            state["max_inflight"] = max(state["max_inflight"], state["inflight"])
            first_live_started.set()
            try:
                await release_live.wait()
                return SimpleNamespace(
                    token=f"custom-token-{state['call_count']}",
                    browser_id=2,
                    browser_ref=2,
                    fingerprint={"user_agent": "ua-custom"},
                    source="live",
                    elapsed_ms=0,
                    browser_epoch=5,
                )
            finally:
                state["inflight"] -= 1

        with patch.object(service, "_check_available"):
            with patch.object(service, "_resolve_global_proxy_url", AsyncMock(return_value=None)):
                with patch.object(service, "_schedule_custom_standby_refill", AsyncMock()):
                    with patch.object(
                        service,
                        "_acquire_live_custom_token",
                        AsyncMock(side_effect=fake_acquire_live_custom_token),
                    ) as acquire_mock:
                        first_task = asyncio.create_task(
                            service.get_custom_token(
                                website_url="https://example.com/login",
                                website_key="site-key",
                                action="login",
                                captcha_type="recaptcha_v3",
                            )
                        )
                        await first_live_started.wait()
                        second_task = asyncio.create_task(
                            service.get_custom_token(
                                website_url="https://example.com/login",
                                website_key="site-key",
                                action="login",
                                captcha_type="recaptcha_v3",
                            )
                        )
                        await asyncio.sleep(0.01)
                        self.assertFalse(first_task.done())
                        self.assertFalse(second_task.done())

                        release_live.set()
                        first_result, second_result = await asyncio.gather(first_task, second_task)

        self.assertEqual(state["max_inflight"], 1)
        self.assertGreaterEqual(acquire_mock.await_count, 1)
        self.assertTrue(bool(first_result.token))
        self.assertTrue(bool(second_result.token))
        self.assertEqual(service._standby_live_tasks, {})

    async def test_refill_prefers_other_idle_browser_when_preferred_busy(self):
        service = BrowserCaptchaService()
        service._browser_count = 3

        class FakeBrowser:
            def __init__(self, busy):
                self._busy = busy

            def is_busy(self):
                return self._busy

        service._browsers = {
            0: FakeBrowser(True),
            1: FakeBrowser(False),
        }

        selected = await service._select_idle_browser_id_for_refill(
            project_id="project-a",
            preferred_browser_id=0,
        )

        self.assertEqual(selected, 1)

    async def test_project_affinity_trim_evicts_old_keys(self):
        service = BrowserCaptchaService()
        service._project_slot_affinity = {
            "old-1": [0],
            "old-2": [1],
            "keep": [0],
        }
        service._project_slot_last_used = {
            "old-1": 1.0,
            "old-2": 2.0,
            "keep": time.monotonic(),
        }

        with patch.object(service, "_project_affinity_max_keys", return_value=1):
            async with service._project_slot_lock:
                service._trim_project_affinity_locked()

        self.assertEqual(service._project_slot_affinity, {"keep": [0]})

    async def test_standby_bucket_trim_evicts_old_buckets(self):
        service = BrowserCaptchaService()
        now_value = time.monotonic()
        service._standby_tokens = {
            "old": [
                StandbyTokenEntry(
                    token="token-old",
                    browser_id=1,
                    fingerprint={"user_agent": "ua-old"},
                    browser_epoch=1,
                    project_id="p-old",
                    action="IMAGE_GENERATION",
                    proxy_signature="-",
                    created_monotonic=now_value,
                    expires_monotonic=now_value + 30,
                )
            ],
            "new": [
                StandbyTokenEntry(
                    token="token-new",
                    browser_id=2,
                    fingerprint={"user_agent": "ua-new", "extra": "drop-me"},
                    browser_epoch=1,
                    project_id="p-new",
                    action="IMAGE_GENERATION",
                    proxy_signature="-",
                    created_monotonic=now_value,
                    expires_monotonic=now_value + 30,
                )
            ],
        }
        service._standby_bucket_last_used = {"old": 1.0, "new": now_value}

        with patch.object(service, "_standby_bucket_max_count", return_value=1):
            with patch.object(service, "_is_standby_entry_valid", return_value=True):
                async with service._standby_lock:
                    cancelled = service._trim_standby_buckets_locked(now_value=now_value)

        self.assertEqual(cancelled, [])
        self.assertNotIn("old", service._standby_tokens)
        self.assertIn("new", service._standby_tokens)

    async def test_service_limits_use_configured_values(self):
        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(
                browser_project_affinity_max_keys=5,
                browser_project_affinity_ttl_seconds=120,
                browser_standby_bucket_max_count=9,
                browser_standby_bucket_idle_ttl_seconds=150,
                browser_idle_reaper_interval_seconds=4,
            ),
        ):
            service = BrowserCaptchaService()
            self.assertEqual(service._project_affinity_max_keys(), 5)
            self.assertEqual(service._project_affinity_ttl_seconds(), 120.0)
            self.assertEqual(service._standby_bucket_max_count(), 9)
            self.assertEqual(service._standby_bucket_idle_ttl_seconds(), 150.0)
            self.assertEqual(service._idle_reaper_interval_seconds(), 4.0)

    async def test_service_limits_keep_auto_fallback_when_zero_configured(self):
        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(
                browser_project_affinity_max_keys=0,
                browser_project_affinity_ttl_seconds=120,
                browser_standby_bucket_max_count=0,
                browser_standby_bucket_idle_ttl_seconds=0,
                browser_standby_token_ttl_seconds=150,
                browser_idle_reaper_interval_seconds=4,
            ),
        ):
            service = BrowserCaptchaService()
            service._browser_count = 4
            self.assertEqual(service._project_affinity_max_keys(), 64)
            self.assertEqual(service._project_affinity_ttl_seconds(), 120.0)
            self.assertEqual(service._standby_bucket_max_count(), 48)
            self.assertEqual(service._standby_token_ttl_seconds(), 60.0)
            self.assertEqual(service._standby_bucket_idle_ttl_seconds(), 180.0)
            self.assertEqual(service._idle_reaper_interval_seconds(), 4.0)

    async def test_browser_request_finish_and_execute_timeout_support_auto_mode(self):
        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(
                browser_execute_timeout_seconds=0,
                browser_request_finish_image_wait_seconds=0,
                browser_request_finish_non_image_wait_seconds=0,
                browser_retry_backoff_seconds=0,
            ),
        ):
            browser = TokenBrowser(9, "tmp/test-auto-timeouts")
            self.assertEqual(browser._execute_timeout_seconds(fallback=30.0), 30.0)
            self.assertEqual(browser._execute_timeout_seconds(fallback=45.0), 45.0)
            self.assertEqual(browser._retry_backoff_seconds(), 0.0)
            self.assertEqual(
                browser._request_finish_image_wait_seconds(flow_timeout=600, upsample_timeout=800),
                980,
            )
            self.assertEqual(
                browser._request_finish_non_image_wait_seconds(flow_timeout=600),
                1800,
            )

    async def test_browser_recaptcha_settle_seconds_allows_zero(self):
        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(browser_recaptcha_settle_seconds=0),
        ):
            browser = TokenBrowser(10, "tmp/test-settle-zero")
            self.assertEqual(browser._recaptcha_settle_seconds(), 0.0)

    async def test_store_standby_token_compacts_fingerprint(self):
        service = BrowserCaptchaService()
        result = type("Result", (), {})()
        result.token = "standby-token"
        result.browser_id = 7
        result.browser_epoch = 3
        result.fingerprint = {
            "user_agent": "ua-live",
            "accept_language": "zh-CN",
            "big_blob": "x" * 128,
        }

        await service._store_standby_token(
            "bucket-a",
            result,
            project_id="project-a",
            action="IMAGE_GENERATION",
        )

        stored = service._standby_tokens["bucket-a"][0]
        self.assertEqual(stored.fingerprint, {"user_agent": "ua-live", "accept_language": "zh-CN"})

    async def test_store_standby_token_keeps_compact_profile_fields(self):
        service = BrowserCaptchaService()
        result = type("Result", (), {})()
        result.token = "standby-profile"
        result.browser_id = 9
        result.browser_epoch = 4
        result.fingerprint = {
            "user_agent": "ua-live",
            "locale": "en-US",
            "timezone_id": "America/Los_Angeles",
            "device_scale_factor": 3.0,
            "is_mobile": True,
            "has_touch": True,
            "viewport": {"width": 430, "height": 932},
            "big_blob": "drop-me",
        }

        await service._store_standby_token(
            "bucket-profile",
            result,
            project_id="project-a",
            action="IMAGE_GENERATION",
        )

        stored = service._standby_tokens["bucket-profile"][0]
        self.assertEqual(
            stored.fingerprint,
            {
                "user_agent": "ua-live",
                "locale": "en-US",
                "timezone_id": "America/Los_Angeles",
                "device_scale_factor": 3.0,
                "is_mobile": True,
                "has_touch": True,
                "viewport": {"width": 430, "height": 932},
            },
        )

    async def test_store_standby_token_does_not_refresh_bucket_last_used(self):
        service = BrowserCaptchaService()
        existing_last_used = 123.0
        service._standby_bucket_last_used["bucket-sticky"] = existing_last_used

        result = type("Result", (), {})()
        result.token = "standby-token"
        result.browser_id = 7
        result.browser_epoch = 3
        result.fingerprint = {"user_agent": "ua-live"}

        await service._store_standby_token(
            "bucket-sticky",
            result,
            project_id="project-a",
            action="IMAGE_GENERATION",
        )

        self.assertEqual(service._standby_bucket_last_used["bucket-sticky"], existing_last_used)

    async def test_refill_retries_until_idle_browser_is_available(self):
        service = BrowserCaptchaService()
        service._browser_count = 2
        service._standby_bucket_last_used[self.bucket_key] = time.monotonic()

        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(
                browser_standby_refill_idle_seconds=0.01,
                browser_standby_token_pool_enabled=True,
                browser_standby_token_pool_depth=1,
                browser_standby_token_ttl_seconds=45,
            ),
        ):
            with patch.object(
                service,
                "_select_idle_browser_id_for_refill",
                AsyncMock(side_effect=[None, 1]),
            ) as select_mock:
                with patch.object(
                    service,
                    "_acquire_live_token",
                    AsyncMock(
                        return_value=SimpleNamespace(
                            token="filled-token",
                            browser_id=1,
                            browser_ref=1,
                            fingerprint={"user_agent": "ua-fill"},
                            source="live",
                            elapsed_ms=0,
                            browser_epoch=3,
                        )
                    ),
                ) as acquire_mock:
                    await service._refill_standby_token(
                        bucket_key=self.bucket_key,
                        project_id="project-a",
                        action="IMAGE_GENERATION",
                        token_proxy_url=None,
                        preferred_browser_id=0,
                    )

        self.assertGreaterEqual(select_mock.await_count, 2)
        acquire_mock.assert_awaited_once()
        self.assertIn(self.bucket_key, service._standby_tokens)
        self.assertEqual(service._standby_tokens[self.bucket_key][0].token, "filled-token")

    async def test_schedule_standby_refill_spawns_parallel_tasks_for_missing_depth(self):
        service = BrowserCaptchaService()
        service._browser_count = 4
        now_value = time.monotonic()
        service._standby_tokens[self.bucket_key] = [
            StandbyTokenEntry(
                token="warm-token",
                browser_id=0,
                fingerprint={"user_agent": "ua-live"},
                browser_epoch=1,
                project_id="project-a",
                action="IMAGE_GENERATION",
                proxy_signature="-",
                created_monotonic=now_value,
                expires_monotonic=now_value + 30,
            )
        ]

        gate = asyncio.Event()
        started = 0

        async def fake_refill(*args, **kwargs):
            nonlocal started
            started += 1
            await gate.wait()

        with patch(
            "src.services.browser_captcha.config",
            SimpleNamespace(
                browser_standby_refill_idle_seconds=0.01,
                browser_standby_token_pool_enabled=True,
                browser_standby_token_pool_depth=4,
                browser_standby_token_ttl_seconds=45,
            ),
        ):
            with patch.object(service, "_trim_standby_buckets_locked", return_value=[]):
                with patch.object(service, "_refill_standby_token", AsyncMock(side_effect=fake_refill)):
                    await service._schedule_standby_refill(
                        bucket_key=self.bucket_key,
                        project_id="project-a",
                        action="IMAGE_GENERATION",
                        token_proxy_url=None,
                        preferred_browser_id=None,
                    )
                    await asyncio.sleep(0)

                    async with service._standby_lock:
                        refill_tasks = set(service._standby_refill_tasks.get(self.bucket_key, set()))

                    self.assertEqual(len(refill_tasks), 3)
                    self.assertEqual(started, 3)

                    gate.set()
                    await asyncio.gather(*refill_tasks)

                    async with service._standby_lock:
                        self.assertEqual(service._get_active_refill_tasks_locked(self.bucket_key), set())
                        self.assertNotIn(self.bucket_key, service._standby_refill_tasks)

    async def test_claim_idle_browser_id_for_refill_skips_reserved_slots(self):
        service = BrowserCaptchaService()
        service._browser_count = 2

        first = await service._claim_idle_browser_id_for_refill("project-a", None)
        second = await service._claim_idle_browser_id_for_refill("project-a", None)

        self.assertEqual({first, second}, {0, 1})
        self.assertEqual(service._slot_reservations[first], 1)
        self.assertEqual(service._slot_reservations[second], 1)

        await service._release_refill_browser_claim(first)
        await service._release_refill_browser_claim(second)

        async with service._standby_lock:
            self.assertEqual(service._standby_refill_browser_claims, set())
        self.assertEqual(service._slot_reservations, {})


if __name__ == "__main__":
    unittest.main()
