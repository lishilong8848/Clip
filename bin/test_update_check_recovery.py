"""Offline update-check recovery; writes and credentials never leave the fixture."""
import logging
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_distribution import FeishuRuntimeMirror, MirrorRequestError, PATCH_TABLE
from upload_event_module.services.http_client import FeishuHTTPError, FeishuHttpClient
from upload_event_module.services.patch_mirror import fetch_manifest, _releases
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater, update_check_diagnostic


class UpdateCheckRecoveryTests(unittest.TestCase):
    def mirror(self, client):
        mirror = FeishuRuntimeMirror(client=client, headers={'Authorization': 'fixture'}, table=PATCH_TABLE)
        self.addCleanup(mirror.close)
        return mirror

    def test_search_refreshes_expired_token_once(self):
        client = Mock()
        client.request_json.side_effect = [
            {'code': 99991663}, {'code': 0, 'data': {'items': [], 'has_more': False}},
        ]
        mirror = self.mirror(client)
        mirror._managed_auth = True
        mirror._auth_headers = Mock(return_value={'Authorization': 'refreshed-fixture'})
        self.assertEqual(mirror.request('POST', '/records/search')['items'], [])
        self.assertEqual(client.request_json.call_count, 2)
        self.assertEqual(sum(c.kwargs.get('force_refresh', False) for c in mirror._auth_headers.call_args_list), 1)

    def test_search_expiry_does_not_loop_or_replay_writes(self):
        for method, suffix, calls in (
            ('POST', '/records/search', 2), ('POST', '/records', 1),
            ('PUT', '/records/recFixture', 1), ('DELETE', '/records/recFixture', 1),
            ('POST', '/records/search/anything', 1),
        ):
            with self.subTest(method=method, suffix=suffix):
                client = Mock()
                client.request_json.return_value = {'code': 99991663}
                mirror = self.mirror(client)
                mirror._managed_auth = True
                mirror._auth_headers = Mock(return_value={})
                with self.assertRaises(MirrorRequestError):
                    mirror.request(method, suffix)
                self.assertEqual(client.request_json.call_count, calls)
                self.assertEqual(client.request_json.call_args.kwargs['retries'], 2 if calls == 2 else 0)

    def test_transient_network_and_rate_limit_recover_with_bounded_retry(self):
        for failure in ('timeout', 'rate_limit'):
            with self.subTest(failure=failure):
                requests = []
                def handler(request):
                    requests.append(request)
                    if len(requests) < 3:
                        if failure == 'timeout':
                            raise httpx.ReadTimeout('fixture', request=request)
                        return httpx.Response(429, json={'code': 99991400})
                    return httpx.Response(200, json={'code': 0, 'data': {'items': []}})
                client = FeishuHttpClient(transport=httpx.MockTransport(handler))
                mirror = self.mirror(client)
                with patch('upload_event_module.services.http_client.time.sleep'):
                    self.assertEqual(mirror.request('POST', '/records/search')['items'], [])
                self.assertEqual(len(requests), 3)

    def test_exhausted_read_and_uncertain_write_have_distinct_attempt_counts(self):
        for suffix, expected in (('/records/search', 3), ('/records', 1)):
            requests = []
            def handler(request):
                requests.append(request)
                raise httpx.ReadTimeout('fixture', request=request)
            mirror = self.mirror(FeishuHttpClient(transport=httpx.MockTransport(handler)))
            with patch('upload_event_module.services.http_client.time.sleep'), self.assertRaises(FeishuHTTPError):
                mirror.request('POST', suffix, retries=7)
            self.assertEqual(len(requests), expected)

    def test_update_timeout_reaches_transport_and_download_default_is_unchanged(self):
        requests = []
        def handler(request):
            requests.append(request)
            return httpx.Response(200, json={'code': 0, 'data': {'items': [], 'has_more': False}})
        def factory(**kwargs):
            return FeishuRuntimeMirror(client=FeishuHttpClient(transport=httpx.MockTransport(handler)),
                                       headers={}, **kwargs)
        original = fetch_manifest
        with tempfile.TemporaryDirectory() as folder:
            updater = RemotePatchUpdater(Path(folder), Path(folder) / 'cache', '')
            with patch('upload_event_module.services.patch_mirror.fetch_manifest',
                       side_effect=lambda **kwargs: original(mirror_factory=factory, **kwargs)):
                self.assertEqual(updater.fetch_manifest((3, 21)), {})
        self.assertEqual(requests[0].extensions['timeout'], {'connect': 3, 'read': 21, 'write': 21, 'pool': 3})
        self.assertIsNone(self.mirror(Mock()).timeout)

    def test_diagnostics_keep_safe_code_and_never_echo_transport_secrets(self):
        exc = MirrorRequestError(1254302, 'request-fixture')
        result = update_check_diagnostic(exc)
        self.assertEqual(result['category'], 'permission')
        self.assertIn('1254302', result['detail'])
        self.assertIn('request-fixture', result['detail'])
        result = update_check_diagnostic(FeishuHTTPError('https://host/?token=do-not-echo'))
        self.assertNotIn('do-not-echo', str(result))
        self.assertEqual(result['category'], 'network')
        self.assertEqual(update_check_diagnostic(ValueError('同一补丁号存在不同发布清单'))['category'], 'conflict')
        self.assertEqual(update_check_diagnostic(ValueError('broken JSON'))['category'], 'manifest')

    def test_distinct_pagination_tokens_still_have_a_finite_bound(self):
        mirror = Mock()
        mirror.request.side_effect = (
            {'items': [], 'has_more': True, 'page_token': 'page' + str(n)}
            for n in range(1000)
        )
        with self.assertRaisesRegex(ValueError, '分页上限'):
            _releases(mirror)
        self.assertEqual(mirror.request.call_count, 100)


if __name__ == '__main__':
    unittest.main()
