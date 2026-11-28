"""No-network coverage of live inspection requests and per-user learned times."""
import datetime as dt
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

import httpx
import requests

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.inspection_collection import InspectionCollection, NS, CN, minimum_source_time
from lan_bitable_template_portal.risk_inspection import RiskError
from lan_bitable_template_portal.state_store import LanPortalStateStore


class InspectionCollectionTests(unittest.TestCase):
    def setUp(self):
        folder = self.enterContext(tempfile.TemporaryDirectory())
        self.store = LanPortalStateStore(Path(folder) / 'state.sqlite3')
        self.addCleanup(self.store.shutdown_write_worker, timeout=2)
        self.now = dt.datetime(2026, 11, 18, 10, 0, tzinfo=CN).timestamp()
        self.transport = Mock(side_effect=self.reply)
        self.service = InspectionCollection(self.store, transport=self.transport, clock=lambda: self.now)
        self.store.put_document(NS, 'settings', {'revision': 1, 'enabled': True, 'url': 'http://192.168.1.5:18765', 'token_cipher': 'isolated'})
        self.duty = {'scope': 'D', 'date': '2026-11-18', 'shift': '白'}

    @staticmethod
    def reply(method, path, payload):
        return {'ok': True, 'request_id': payload['request_id'] if payload else path[1:],
                'building': 'D楼', 'state': 'running'}

    def test_first_start_is_durable_and_restart_polls_same_building_request(self):
        first = self.service.ensure(self.duty, 1)
        self.now += 3
        replacement = InspectionCollection(self.store, transport=self.transport, clock=lambda: self.now)
        next_state = replacement.ensure(self.duty, 1)
        self.assertEqual(first['request_id'], next_state['request_id'])
        self.assertEqual([call.args[0] for call in self.transport.call_args_list], ['POST', 'GET'])
        self.assertFalse(self.store.list_documents(NS, key_prefix='capture:E:'))

    def test_fresh_batch_needs_no_new_collection(self):
        result = self.service.ensure(self.duty, 1, source_time=int((self.now - 120) * 1000))
        self.assertEqual(result['state'], 'ready')
        self.transport.assert_not_called()

    def test_lost_receipt_retries_original_id_and_keeps_other_building_independent(self):
        self.transport.side_effect = requests.Timeout('isolated network failure')
        first = self.service.ensure(self.duty, 1)
        self.now += 6
        self.transport.side_effect = self.reply
        second = self.service.ensure(self.duty, 1)
        self.assertEqual(first['request_id'], second['request_id'])
        self.assertEqual([call.args[0] for call in self.transport.call_args_list], ['POST', 'POST'])
        self.transport.side_effect = lambda method, path, payload: {
            'ok': True, 'request_id': payload['request_id'], 'building': 'E楼', 'state': 'pending'}
        other = self.service.ensure({**self.duty, 'scope': 'E'}, 1)
        self.assertNotEqual(other['request_id'], first['request_id'])

    def test_upload_receipt_requires_sample_time_not_upload_time(self):
        first = self.service.ensure(self.duty, 1)
        self.now += 3
        for building, sampled in [('E楼', int(self.now * 1000)), ('D楼', first['not_before'] - 1)]:
            self.transport.side_effect = lambda *args: {'ok': True, 'request_id': first['request_id'],
                'building': building, 'state': 'uploaded', 'source_time': sampled, 'uploaded_at': int(self.now * 1000)}
            result = self.service.ensure(self.duty, 1)
            self.assertNotEqual(result['state'], 'ready')
            self.now += 6
        self.transport.side_effect = lambda *args: {'ok': True, 'request_id': first['request_id'],
            'building': 'D楼', 'state': 'uploaded', 'source_time': int((self.now - 30)*1000), 'uploaded_at': int(self.now * 1000)}
        self.assertEqual(self.service.ensure(self.duty, 1)['state'], 'ready')
        self.now += 3
        calls = self.transport.call_count
        self.assertEqual(self.service.ensure(self.duty, 1)['state'], 'ready')
        self.assertEqual(self.transport.call_count, calls)

    def test_resaving_same_endpoint_during_capture_retains_original_request(self):
        original = self.service.ensure(self.duty, 1)
        config = self.service.config()
        config['revision'] += 1
        self.store.put_document(NS, 'settings', config)
        self.now += 3
        again = self.service.ensure(self.duty, 1)
        self.assertEqual(original['request_id'], again['request_id'])
        self.assertEqual(self.transport.call_args.args[0], 'GET')

    def test_repeat_start_does_not_train_twice_and_habits_are_personal(self):
        for day in ('2026-11-12', '2026-11-14', '2026-11-16'):
            at = dt.datetime.fromisoformat(day).replace(hour=10, tzinfo=CN).timestamp()
            for _ in range(2):
                self.service.remember_start('person-1', {**self.duty, 'date': day}, 1, day, started_at=at)
        rows = self.store.list_documents(NS, key_prefix='habit:D:')
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]['payload']['samples']), 3)
        predicted = self.service.prediction(rows[0]['payload'], '2026-11-18')
        self.assertEqual(predicted.hour, 10)
        self.service.remember_start('person-2', self.duty, 1, 'another')
        self.assertEqual(len(self.store.list_documents(NS, key_prefix='habit:D:')), 2)
        self.assertFalse(self.store.list_documents(NS, key_prefix='habit:E:'))

    def test_prefetch_requires_stable_history_and_current_inspection_permission(self):
        for day in ('2026-11-12', '2026-11-14', '2026-11-16'):
            at = dt.datetime.fromisoformat(day).replace(hour=10, tzinfo=CN).timestamp()
            self.service.remember_start('person-1', {**self.duty, 'date': day}, 1, day, started_at=at)
        self.now -= 4 * 60
        denied = Mock(return_value=False)
        self.service.prefetch('D', denied)
        self.transport.assert_not_called()
        self.service.prefetch('D', lambda owner, duty: owner == 'person-1' and duty == self.duty)
        self.transport.assert_called_once()
        self.assertEqual(self.transport.call_args.args[2]['building'], 'D楼')

    def test_missing_source_sample_is_not_fresh_and_manual_marks_are_exempt(self):
        rows = [{'fields': {'记录类型': '测点明细', '测点ID': 'source', '源文件时间': 1000}},
                {'fields': {'记录类型': '测点明细', '测点ID': 'local_' + 'a'*32, '测点来源': 'manual_status'}}]
        self.assertEqual(minimum_source_time({'rows': rows}), 1000)
        rows.append({'fields': {'记录类型': '设备巡检'}})
        self.assertEqual(minimum_source_time({'rows': rows}), 0)

    def test_settings_keep_lan_address_without_key_and_validate_revision(self):
        result = self.service.save_config({'revision': 1, 'enabled': True, 'url': 'http://192.168.1.5:18765'})
        self.assertTrue(result['enabled'])
        self.assertNotIn('token_cipher', result)
        self.assertNotIn('token_cipher', self.service.config())
        with self.assertRaises(RiskError):
            self.service.save_config({'revision': 1, 'enabled': False, 'url': ''})
        for url in ('http://[broken', 'http://', 'https://example.com', 'http://8.8.8.8', 'http://0.0.0.0'):
            with self.subTest(url=url), self.assertRaises(RiskError):
                self.service.save_config({'revision': 2, 'enabled': True, 'url': url})

    def test_lan_request_needs_no_token_and_rejects_an_old_public_url(self):
        self.service.transport = None
        with patch('requests.Session.request', return_value=Mock(status_code=200, json=lambda: {})) as request:
            self.service._request(self.service.config(), 'GET', '/fixture')
            self.assertNotIn('headers', request.call_args.kwargs)
            self.assertFalse(request.call_args.kwargs['allow_redirects'])
            with self.assertRaises(RiskError):
                self.service._request({'url': 'https://example.com'}, 'GET', '/fixture')
            request.assert_called_once()

    def test_start_waits_for_complete_bitable_batch_with_the_receipt_sample(self):
        from lan_bitable_template_portal import daily_inspection as daily
        inspection = daily.DailyInspection(self.store, None, scheduled=True)
        current = int(self.now * 1000)
        sample = current - 60_000
        receipt = {'state': 'ready', 'source_time': sample, 'uploaded_at': current,
                   'not_before': current - 600_000}

        def batch(stamp):
            return {'D': {'batch': stamp + 1000, 'rows': [{'fields': {'源文件时间': stamp}}]}}

        clock = dt.datetime

        class FixedClock(clock):
            @classmethod
            def now(cls, tz=None):
                return clock.fromtimestamp(current / 1000, tz)

        with patch.object(daily.dt, 'datetime', FixedClock), patch.object(
                inspection.collection, 'ensure', return_value=receipt), patch.object(
                inspection, 'capture', side_effect=[RiskError('源表分页未读完', 503),
                                                    httpx.ReadTimeout('隔离模拟超时'),
                                                    batch(sample - 60_000), batch(sample)]) as capture:
            state = {'active': None, 'history': []}
            for _ in range(3):
                self.assertEqual(inspection.prepare_start_collection(state, self.duty)['state'], 'waiting_bitable')
                self.assertIsNone(state['active'])
            self.assertEqual(inspection.prepare_start_collection(state, self.duty), receipt)
            self.assertEqual(capture.call_count, 4)
            capture.assert_called_with(['D'], force_probe=True)

    def test_learned_prefetch_before_midnight_and_later_bucket_belong_to_current_round(self):
        from lan_bitable_template_portal import daily_inspection as daily
        inspection = daily.DailyInspection(self.store, None, scheduled=True)
        midnight = dt.datetime(2026, 11, 19, 0, 0, tzinfo=CN)
        sample = int((midnight - dt.timedelta(minutes=4)).timestamp() * 1000)
        batch = {'batch': sample + 1000, 'rows': [{'fields': {'记录类型': '设备巡检', '源文件时间': sample}}]}
        night = {**self.duty, 'shift': '夜'}
        self.assertTrue(inspection.matches_scheduled_batch(batch, night, 2, midnight))
        real_datetime = dt.datetime
        class MidnightClock(real_datetime):
            @classmethod
            def now(cls, tz=None):
                return midnight
        saved_pressures = {'fixture-pump': {'inlet': '0.3'}}
        with patch.object(daily.dt, 'datetime', MidnightClock), patch.object(
                daily, 'original_templates', return_value=[{'scope': 'D', 'profession': '暖通', 'name': '暖通.xlsx'}]), patch.object(
                daily, 'collect_operating_marks', return_value={'C40': '√'}) as collect, patch.object(
                daily, 'apply_handover_readings', return_value=False), patch.object(
                daily, 'compressor_running_marks', side_effect=lambda *args: args[-1]), patch.object(
                daily, 'unit_pump_marks', side_effect=lambda marks: marks), patch.object(daily, 'apply_operating_marks') as apply:
            self.assertTrue(inspection.fill_handover_points('D', {'duty': night, 'round': 2}, batch, batch, saved_pressures))
            self.assertLessEqual(collect.call_args.args[3], sample)
            self.assertEqual(apply.call_args.kwargs['saved_pressures'], saved_pressures)
        later = dt.datetime(2026, 11, 18, 11, 0, tzinfo=CN)
        sample = int((later - dt.timedelta(minutes=1)).timestamp() * 1000)
        batch = {'batch': sample + 1000, 'rows': [{'fields': {'记录类型': '设备巡检', '源文件时间': sample}}]}
        self.assertTrue(inspection.matches_scheduled_batch(batch, self.duty, 1, later))
        class Clock(real_datetime):
            @classmethod
            def now(cls, tz=None):
                return later
        with patch.object(daily.dt, 'datetime', Clock):
            self.assertTrue(inspection.live_batch_belongs_to_round({'duty': self.duty, 'round': 1}, batch))
        cutoff = inspection.round_cutoff(self.duty, 1)
        self.assertFalse(inspection.matches_scheduled_batch(
            {**batch, 'batch': int(cutoff.timestamp()*1000)}, self.duty, 1, cutoff))


if __name__ == '__main__':
    unittest.main()
