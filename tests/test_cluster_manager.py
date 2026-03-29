import asyncio
import unittest
from unittest.mock import AsyncMock, patch

from src.services.cluster_manager import ClusterManager


class FakeDb:
    def __init__(self):
        self.mark_cluster_node_error = AsyncMock()


class ClusterManagerDispatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_dispatch_custom_token_tls_certificate_error_raises_without_retry_loop(self):
        db = FakeDb()
        manager = ClusterManager(db, runtime=object())
        node = {
            "id": 7,
            "node_name": "subnode-a",
            "base_url": "https://subnode-a.example.com",
            "node_api_key": "node-secret",
            "thread_total": 1,
            "thread_idle": 1,
            "thread_active": 0,
            "max_concurrency": 1,
            "active_sessions": 0,
        }
        request_payload = {
            "website_url": "https://example.com/login",
            "website_key": "site-key",
            "action": "login",
            "captcha_type": "recaptcha_v3",
            "enterprise": False,
            "is_invisible": True,
        }
        bucket_key = manager._build_custom_dispatch_bucket_key(request_payload, prefix="custom")
        tls_error = RuntimeError(
            "HTTP 请求失败: [SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed: self signed certificate"
        )

        with patch.object(manager, "_select_candidate_nodes", AsyncMock(return_value=[node])) as select_nodes_mock:
            with patch.object(manager, "_try_reserve_dispatch_slot", AsyncMock(return_value=True)) as reserve_mock:
                with patch.object(manager, "_release_dispatch_slot", AsyncMock()) as release_mock:
                    with patch.object(manager, "_post_to_node", AsyncMock(side_effect=tls_error)) as post_mock:
                        with patch.object(manager, "_clear_bucket_affinity", AsyncMock()) as clear_affinity_mock:
                            with patch("src.services.cluster_manager.asyncio.sleep", AsyncMock()) as sleep_mock:
                                with self.assertRaisesRegex(RuntimeError, "子节点调度失败"):
                                    await asyncio.wait_for(
                                        manager.dispatch_custom_token(request_payload),
                                        timeout=0.2,
                                    )

        select_nodes_mock.assert_awaited_once_with(bucket_key=bucket_key)
        reserve_mock.assert_awaited_once_with(node)
        release_mock.assert_awaited_once_with(7)
        post_mock.assert_awaited_once()
        clear_affinity_mock.assert_awaited_once_with(bucket_key, expected_node_id=7)
        db.mark_cluster_node_error.assert_awaited_once()
        mark_error_args = db.mark_cluster_node_error.await_args
        self.assertEqual(mark_error_args.args[0], 7)
        self.assertIn("certificate verify failed", mark_error_args.args[1].lower())
        self.assertEqual(mark_error_args.kwargs["error_type"], "dispatch")
        sleep_mock.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
