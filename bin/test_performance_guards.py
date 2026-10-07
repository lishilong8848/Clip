from __future__ import annotations

import ast
import asyncio
import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from pathlib import Path
from unittest.mock import Mock, patch
from types import SimpleNamespace

import httpx

BIN = Path(__file__).resolve().parent
ROOT = BIN.parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from upload_event_module.services.http_client import FeishuHttpClient
from upload_event_module.services.remote_patch_updater import RemotePatchUpdater
from upload_event_module.ui.active_notice_index import ActiveNoticeIndex
from lan_bitable_template_portal.state_store import LanPortalStateStore
import package_portable


class _Item:
    valid = True

    def __init__(self, record_id: str):
        self.payload = {"record_id": record_id, "active_item_id": record_id, "text": record_id}

    def data(self, _role):
        return dict(self.payload)


class PerformanceGuardTests(unittest.TestCase):
    def test_heavy_maintenance_is_separate_from_time_sensitive_jobs(self):
        from clipflow_backend.main import FastAPIPortalController
        from apscheduler.schedulers.background import BackgroundScheduler
        from upload_event_module.services.process_lifetime import lower_current_thread_priority
        controller = object.__new__(FastAPIPortalController)
        controller._scheduler = None
        controller._write_runtime_heartbeat = Mock()
        with patch.object(BackgroundScheduler, 'start'):
            controller._start_scheduler()
        scheduler = controller._scheduler
        pool = scheduler._executors['maintenance']._pool
        try:
            self.assertEqual(pool._max_workers, 1)
            self.assertIs(pool._initializer, lower_current_thread_priority)
            jobs = {job.id: job for job in scheduler.get_jobs()}
            for name in ('job_cleanup', 'job_cleanup_startup', 'sqlite_maintenance', 'repair_maintenance', 'water_consumption_refresh', 'daily_report_recipient_refresh'):
                self.assertEqual(jobs[name].executor, 'maintenance')
                self.assertEqual(jobs[name].misfire_grace_time, 3600)
            for name in ('notice_robot', 'polling_delay_reminders', 'repair_summary_sync', 'daily_work_report'):
                self.assertEqual(jobs[name].executor, 'default')
        finally:
            pool.shutdown(wait=False)

    def test_learning_ignores_inactive_notifications_before_reading_settings(self):
        from lan_bitable_template_portal.learning import LearningService, now
        with tempfile.TemporaryDirectory() as temp:
            sender = Mock()
            service = LearningService(root=temp, cloud=Mock(), send_message=sender)
            with service.transaction() as conn:
                for i in range(40):
                    service._put('notification', str(i), {'id': str(i), 'status': 'sent'}, conn, False)
                service._put('notification', 'future', {'id': 'future', 'status': 'pending',
                             'retry_at': time.time() + 3600}, conn, False)
            with patch.object(service, 'settings', return_value={'enabled': True, 'reminder_enabled': False}) as settings:
                service.send_notifications(now())
                settings.assert_called_once_with()
            sender.assert_not_called()

    def test_learning_list_reads_all_payloads_in_one_query(self):
        from lan_bitable_template_portal.learning import LearningService
        with tempfile.TemporaryDirectory() as temp:
            service = LearningService(root=temp, cloud=Mock())
            with service.transaction() as conn:
                for key in ('c', 'b', 'a'):
                    service._put('fixture', key, {'id': key, 'value': 1}, conn)
                service._put('fixture', 'a', {'id': 'a', 'value': 2}, conn, False)
                service._put('other', 'd', {'id': 'd'}, conn)
            expected = [service._get('fixture', key) for key in ('a', 'b', 'c')]
            with closing(service._connect()) as conn:
                queries = []
                conn.set_trace_callback(queries.append)
                self.assertEqual(service._all('fixture', conn), expected)
                self.assertEqual(len([q for q in queries if q.startswith('SELECT')]), 1)
                self.assertEqual(conn.execute('SELECT 1').fetchone()[0], 1)
            self.assertEqual(service._all('fixture'), expected)
            self.assertEqual(service._all('missing'), [])

    def test_failed_repair_backfill_does_not_wake_the_worker_with_a_terminal_id(self):
        from lan_bitable_template_portal.server import PortalRuntime
        with tempfile.TemporaryDirectory() as temp:
            store = LanPortalStateStore(Path(temp) / 'state.sqlite3')
            channel = PortalRuntime.event_repair_queue_channel
            old = {'event_record_id': 'rec-old-failed', 'scope': 'A'}
            identity = store.enqueue_outbox_event(channel, {'idempotency_key': 'event_repair:rec-old-failed', **old})
            with patch('lan_bitable_template_portal.state_store.time.time', return_value=time.time() - 901):
                store.mark_outbox_event(identity, 'failed', error='fixture terminal failure')
            candidates = [old]
            service = SimpleNamespace(list_unlinked_transferred_events_for_repair=lambda **_: candidates)
            with patch.object(PortalRuntime, 'state_store', store), patch.object(PortalRuntime, 'service', service), \
                    patch.object(store, 'enqueue_outbox_event', wraps=store.enqueue_outbox_event) as enqueue:
                self.assertEqual(PortalRuntime._enqueue_unlinked_event_repair_project(), 0)
                enqueue.assert_not_called()
                candidates.append({'event_record_id': 'rec-new-transfer', 'scope': 'A'})
                new_id = PortalRuntime._enqueue_unlinked_event_repair_project()
                self.assertGreater(new_id, identity)
                self.assertEqual(PortalRuntime._enqueue_unlinked_event_repair_project(), 0)
                self.assertEqual(enqueue.call_count, 1)
            self.assertEqual(store.count_outbox_events(channel), {'failed': 1, 'pending': 1})

    def test_daily_backup_finishes_on_one_snapshot_while_writes_continue(self):
        with tempfile.TemporaryDirectory() as temp:
            store = LanPortalStateStore(Path(temp) / 'state.sqlite3')
            store.put_document('fixture', 'record', {'value': 'preserved'})
            real_connect = sqlite3.connect
            writer = real_connect(store.db_path)
            try:
                writer.execute('CREATE TABLE backup_fixture (id INTEGER PRIMARY KEY, value BLOB)')
                writer.executemany('INSERT INTO backup_fixture VALUES (?, zeroblob(4096))', ((i,) for i in range(128)))
                writer.execute('CREATE TABLE backup_counter (value INTEGER)')
                writer.execute('INSERT INTO backup_counter VALUES (0)')
                writer.commit()
                steps = []

                class ConcurrentConnection(sqlite3.Connection):
                    def backup(self, target, **kwargs):
                        def progress(status, remaining, total):
                            steps.append(remaining)
                            if len(steps) > 64:
                                raise RuntimeError('backup repeatedly restarted by concurrent writes')
                            writer.execute('UPDATE backup_counter SET value = value + 1')
                            writer.commit()
                        kwargs.update(pages=8, progress=progress)
                        return super().backup(target, **kwargs)

                def connect(*args, **kwargs):
                    return real_connect(*args, factory=ConcurrentConnection, **kwargs)

                with patch('lan_bitable_template_portal.state_store.sqlite3.connect', side_effect=connect):
                    result = store.backup_database()
                self.assertTrue(result['created'])
                self.assertGreater(len(steps), 1)
                self.assertEqual(steps[-1], 0)
                self.assertEqual(sorted(steps, reverse=True), steps)
                with closing(real_connect(result['backup_path'])) as backup:
                    self.assertEqual(backup.execute('SELECT value FROM backup_counter').fetchone()[0], 0)
                    self.assertEqual(backup.execute('PRAGMA quick_check').fetchone()[0], 'ok')
                self.assertEqual(writer.execute('SELECT value FROM backup_counter').fetchone()[0], len(steps))
                self.assertEqual(store.get_document('fixture', 'record'), {'value': 'preserved'})
            finally:
                writer.close()

    def test_unchanged_backup_is_not_rescanned_but_restart_and_changes_are_checked(self):
        with tempfile.TemporaryDirectory() as temp:
            store = LanPortalStateStore(Path(temp) / 'state.sqlite3')
            store.put_document('fixture', 'record', {'value': 'preserved'})
            created = store.backup_database()
            backup = Path(created['backup_path'])
            real_connect = sqlite3.connect
            with patch('lan_bitable_template_portal.state_store.sqlite3.connect', wraps=real_connect) as connect:
                self.assertEqual(store.backup_database()['reason'], 'already_exists')
                connect.assert_not_called()
                restarted = LanPortalStateStore(store.db_path)
                self.assertEqual(restarted.backup_database()['reason'], 'already_exists')
                self.assertEqual(connect.call_count, 1)
                info = backup.stat()
                import os
                os.utime(backup, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000))
                self.assertEqual(restarted.backup_database()['reason'], 'already_exists')
                self.assertEqual(connect.call_count, 2)
                with closing(real_connect(backup)) as editing:
                    editing.execute('PRAGMA journal_mode = WAL')
                    restarted.backup_database()
                    checked = connect.call_count
                    editing.execute('CREATE TABLE backup_edit_fixture (value INTEGER)')
                    editing.commit()
                    restarted.backup_database()
                    self.assertEqual(connect.call_count, checked + 1)
                backup.write_bytes(b'corrupt backup')
                self.assertTrue(restarted.backup_database()['created'])
                self.assertGreater(connect.call_count, 2)
            with closing(real_connect(backup)) as saved:
                self.assertEqual(saved.execute('PRAGMA quick_check').fetchone()[0], 'ok')
            self.assertEqual(store.get_document('fixture', 'record'), {'value': 'preserved'})

    def test_assistant_catalog_copies_only_requested_page_without_sharing_mutable_schema(self):
        import copy
        from openclaw_service.assistant.lighthouse_api import PortalAPICatalog
        catalog = object.__new__(PortalAPICatalog)
        catalog._order = [f'GET /api/fixture/{index}' for index in range(500)]
        catalog._descriptors = {key: {'id': key, 'group': '业务接口', 'name': 'fixture',
            'schema': {'body': {'properties': {'name': {'type': 'string'}}}}} for key in catalog._order}
        with patch('openclaw_service.assistant.lighthouse_api.deepcopy', wraps=copy.deepcopy) as copied:
            result = catalog.discover(keyword='fixture', page=2, page_size=8)
            copied.assert_called_once()
            self.assertEqual(len(copied.call_args.args[0]), 8)
        self.assertEqual(result['total'], 500)
        self.assertEqual([item['id'] for item in result['items']], catalog._order[8:16])
        result['items'][0]['schema']['body']['properties']['name']['type'] = 'changed'
        self.assertEqual(catalog.get(catalog._order[8])['schema']['body']['properties']['name']['type'], 'string')
        self.assertEqual(catalog.discover(keyword='missing')['items'], [])

    def test_queue_stats_reads_details_once(self):
        from clipflow_backend.main import PortalRuntime, _queue_stats
        details = {'message': {'queued_due': 2, 'queued_future': 1}, 'qt_action': {'queued_due': 4}}
        store = Mock()
        store.count_outbox_events.return_value = {}
        store.runtime_queue_details.return_value = details
        store.runtime_queue_counts.return_value = {'message': 3}
        store.get_write_worker_stats.return_value = {}
        with patch.object(PortalRuntime, 'state_store', store), patch.object(PortalRuntime, 'runtime_limits', return_value={}), \
                patch.object(PortalRuntime, 'runtime_pressure', return_value={}):
            result = _queue_stats()
        store.runtime_queue_details.assert_called_once_with()
        self.assertEqual(result['runtime_queue_details'], details)
        self.assertEqual(result['message_queue_size'], 3)
        self.assertEqual(result['qt_queue_size'], 4)

    def test_sse_heartbeat_reads_database_outside_event_loop(self):
        from clipflow_backend.main import FastAPIPortalController
        main_thread = threading.get_ident()
        thread_ids = []
        def stats():
            thread_ids.append(threading.get_ident())
            return {'fixture': True}
        async def run():
            controller = SimpleNamespace(_stopping_event=threading.Event())
            async def connected(): return False
            stream = FastAPIPortalController._heartbeat_stream(controller, SimpleNamespace(is_disconnected=connected), event_name='fixture')
            try:
                self.assertIn(b'"fixture": true', await stream.__anext__())
            finally:
                await stream.aclose()
        with patch('clipflow_backend.main._queue_stats', side_effect=stats):
            asyncio.run(run())
        self.assertEqual(len(thread_ids), 1)
        self.assertNotEqual(thread_ids[0], main_thread)

    def test_qt_drag_uses_native_movement_without_manual_move_events(self):
        from PyQt6.QtCore import QPointF, Qt
        from upload_event_module.ui.main_window_ui import MainWindowUiMixin
        window = SimpleNamespace(drag_position='old', windowHandle=Mock(return_value=SimpleNamespace(startSystemMove=Mock(return_value=True))),
                                 frameGeometry=Mock(), move=Mock())
        event = SimpleNamespace(button=lambda: Qt.MouseButton.LeftButton, buttons=lambda: Qt.MouseButton.LeftButton,
                                globalPosition=lambda: QPointF(120, 180), accept=Mock())
        MainWindowUiMixin.mousePressEvent(window, event)
        MainWindowUiMixin.mouseMoveEvent(window, event)
        window.windowHandle.return_value.startSystemMove.assert_called_once()
        window.frameGeometry.assert_not_called()
        window.move.assert_not_called()
        self.assertIsNone(window.drag_position)

    def test_qt_drag_fallback_stops_after_release(self):
        from PyQt6.QtCore import QPoint, QPointF, Qt
        from upload_event_module.ui.main_window_ui import MainWindowUiMixin
        window = SimpleNamespace(drag_position=None, windowHandle=Mock(return_value=SimpleNamespace(startSystemMove=Mock(return_value=False))),
                                 frameGeometry=Mock(return_value=SimpleNamespace(topLeft=lambda: QPoint(20, 30))), move=Mock())
        event = SimpleNamespace(button=lambda: Qt.MouseButton.LeftButton, buttons=lambda: Qt.MouseButton.LeftButton,
                                globalPosition=Mock(return_value=QPointF(120, 180)), accept=Mock())
        MainWindowUiMixin.mousePressEvent(window, event)
        event.globalPosition.return_value = QPointF(140, 200)
        MainWindowUiMixin.mouseMoveEvent(window, event)
        window.move.assert_called_once_with(QPoint(40, 50))
        MainWindowUiMixin.mouseReleaseEvent(window, event)
        MainWindowUiMixin.mouseMoveEvent(window, event)
        self.assertEqual(window.move.call_count, 1)
        self.assertIsNone(window.drag_position)

    def test_active_index_reuses_snapshot_until_invalidated(self):
        calls = 0
        item = _Item("one")

        def items():
            nonlocal calls
            calls += 1
            return [(object(), item)]

        index = ActiveNoticeIndex(lambda candidate: candidate.valid)
        self.assertEqual(index.data_snapshot(items)[0]["record_id"], "one")
        self.assertEqual(index.data_snapshot(items)[0]["record_id"], "one")
        self.assertEqual(calls, 1)
        index.invalidate()
        index.data_snapshot(items)
        self.assertEqual(calls, 2)

    def test_empty_runtime_queue_does_not_begin_immediate(self):
        with tempfile.TemporaryDirectory() as temp:
            statements = []

            class Store(LanPortalStateStore):
                def _connect(self):
                    conn = super()._connect()
                    conn.set_trace_callback(statements.append)
                    return conn

            store = Store(Path(temp) / "state.sqlite3")
            store.get_settings()
            statements.clear()
            self.assertEqual(store.lease_runtime_queue_items("message"), [])
            self.assertFalse(any("BEGIN IMMEDIATE" in sql.upper() for sql in statements))

    def test_outbox_payload_is_idempotent(self):
        with tempfile.TemporaryDirectory() as temp:
            store = LanPortalStateStore(Path(temp) / "state.sqlite3")
            first = store.enqueue_outbox_event("qt_action", {"idempotency_key": "delete:r1", "kind": "active_delete", "payload": {"record_id": "r1"}})
            second = store.enqueue_outbox_event("qt_action", {"idempotency_key": "delete:r1", "payload": {"record_id": "r1"}, "kind": "active_delete"})
            third = store.enqueue_outbox_event("qt_action", {"idempotency_key": "delete:r2", "kind": "active_delete", "payload": {"record_id": "r2"}})
            self.assertEqual(first, second)
            self.assertNotEqual(first, third)

    def test_empty_outbox_poll_is_read_only_and_recovers_expired_leases(self):
        with tempfile.TemporaryDirectory() as temp:
            statements = []

            class Store(LanPortalStateStore):
                def _connect(self):
                    conn = super()._connect()
                    conn.set_trace_callback(statements.append)
                    return conn

            store = Store(Path(temp) / 'state.sqlite3')
            store.get_settings()
            statements.clear()
            self.assertEqual(store.lease_outbox_events('qt_action'), [])
            self.assertFalse(any(sql.lstrip().upper().startswith(('UPDATE ', 'BEGIN ')) for sql in statements))
            identity = store.enqueue_outbox_event('qt_action', {'kind': 'fixture'})
            leased = store.lease_outbox_events('qt_action')
            self.assertEqual([row['id'] for row in leased], [identity])
            statements.clear()
            self.assertEqual(store.lease_outbox_events('qt_action'), [])
            self.assertFalse(any(sql.lstrip().upper().startswith(('UPDATE ', 'BEGIN ')) for sql in statements))
            with patch('lan_bitable_template_portal.state_store.time.time', return_value=time.time() + 31):
                recovered = store.lease_outbox_events('qt_action')
            self.assertEqual([row['id'] for row in recovered], [identity])

    def test_outbox_count_is_read_only_without_expired_leases(self):
        with tempfile.TemporaryDirectory() as temp:
            statements = []
            class Store(LanPortalStateStore):
                def _connect(self):
                    conn = super()._connect()
                    conn.set_trace_callback(statements.append)
                    return conn
            store = Store(Path(temp) / 'state.sqlite3')
            store.get_settings()
            statements.clear()
            self.assertEqual(store.count_outbox_events('qt_action'), {})
            self.assertFalse(any(sql.lstrip().upper().startswith('UPDATE ') for sql in statements))
            store.enqueue_outbox_event('qt_action', {'kind': 'fixture'})
            store.lease_outbox_events('qt_action')
            statements.clear()
            self.assertEqual(store.count_outbox_events('qt_action'), {'leased': 1})
            self.assertFalse(any(sql.lstrip().upper().startswith('UPDATE ') for sql in statements))
            with patch('lan_bitable_template_portal.state_store.time.time', return_value=time.time() + 31):
                self.assertEqual(store.count_outbox_events('qt_action', stale_lease_seconds=30), {'pending': 1})

    def test_empty_outbox_avoids_writable_connections_and_observes_other_writers(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'state.sqlite3'
            reader, writer = LanPortalStateStore(path), LanPortalStateStore(path)
            reader.put_settings({'fixture': True})
            with patch.object(reader, '_connect', wraps=reader._connect) as writable:
                for _ in range(3):
                    self.assertEqual(reader.lease_outbox_events('qt_action'), [])
                writable.assert_not_called()
                identity = writer.enqueue_outbox_event('qt_action', {'kind': 'fixture'})
                self.assertEqual([row['id'] for row in reader.lease_outbox_events('qt_action')], [identity])
                self.assertEqual(writable.call_count, 1)

    def test_relay_worker_polls_idle_less_often_without_slowing_active_work(self):
        from bin.test_transport_safety import isolated_class
        waits, stop = [], threading.Event()
        def wait(delay):
            waits.append(delay)
            if len(waits) == 3:
                stop.set()
        stop.wait = wait
        worker = Mock(is_alive=Mock(return_value=False))
        namespace = {'threading': SimpleNamespace(Event=lambda: stop, Thread=Mock(return_value=worker)),
                     'lower_current_thread_priority': lambda: None, 'log_warning': lambda _: None}
        cls = isolated_class(ROOT / 'bin/clipflow_backend/main.py', 'FastAPIPortalController',
                             {'_start_polling_relay_worker'}, namespace)
        controller = cls()
        controller._polling_relay_thread = None
        controller._run_scheduled_polling_relay = Mock(side_effect=[False, True, RuntimeError('offline')])
        controller._start_polling_relay_worker()
        namespace['threading'].Thread.call_args.kwargs['target']()
        self.assertEqual(waits, [10.0, 2.0, 4.0])

    def test_http_client_allows_parallel_requests_and_honors_retry_after(self):
        lock = threading.Lock()
        active = maximum = calls = 0

        def handler(request):
            nonlocal active, maximum, calls
            with lock:
                calls += 1
                call = calls
                active += 1
                maximum = max(maximum, active)
            threading.Event().wait(0.04)
            with lock:
                active -= 1
            if call == 3:
                return httpx.Response(429, headers={"Retry-After": "0.01"}, json={"code": 429}, request=request)
            return httpx.Response(200, json={"code": 0}, request=request)

        client = FeishuHttpClient(transport=httpx.MockTransport(handler), retries=1)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(lambda _: client.request_json("GET", "https://open.feishu.cn/test"), range(2)))
            with patch("upload_event_module.services.http_client.time.sleep") as sleep:
                client.request_json("GET", "https://open.feishu.cn/retry")
                sleep.assert_called_once_with(0.01)
        finally:
            client.close()
        self.assertEqual(maximum, 2)

    def test_http_transport_error_does_not_close_shared_client(self):
        calls = 0

        def handler(request):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise httpx.ConnectError("temporary", request=request)
            return httpx.Response(200, json={"code": 0}, request=request)

        client = FeishuHttpClient(transport=httpx.MockTransport(handler), retries=0)
        try:
            with patch.object(client, "close", wraps=client.close) as close:
                with self.assertRaisesRegex(Exception, "temporary"):
                    client.request_json("GET", "https://open.feishu.cn/first")
                close.assert_not_called()
                self.assertEqual(client.request_json("GET", "https://open.feishu.cn/second")["code"], 0)
        finally:
            client.close()

    def test_patch_zip_rejects_parent_paths_and_meta_has_file_hashes(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            archive = root / "bad.zip"
            with zipfile.ZipFile(archive, "w") as stream:
                stream.writestr("../outside.txt", "bad")
            updater = RemotePatchUpdater(root / "app", root / "data", "")
            with self.assertRaisesRegex(RuntimeError, "unsafe path"):
                updater._extract_patch_dir(archive)
            self.assertFalse((root / "outside.txt").exists())

            patch_dir = root / "patch"
            source = patch_dir / "bin" / "sample.py"
            source.parent.mkdir(parents=True)
            source.write_text("VALUE = 1\n", encoding="utf-8")
            package_portable.write_patch_meta(patch_dir, target_build_id="test")
            meta = json.loads((patch_dir / "bin" / "patch_meta.json").read_text(encoding="utf-8"))
            self.assertIn("bin/sample.py", meta["file_sha256"])

    def test_cabinet_editor_filters_all_blank_operation_groups(self):
        source = (BIN / "lan_bitable_template_portal" / "frontend" / "src" / "components" / "CabinetPowerPage.vue").read_text(encoding="utf-8")
        self.assertIn("groupHasBusinessData(group) || group?._editing", source)
        self.assertNotIn("form.groups = form.groups.filter(groupHasBusinessData)", source)

    def test_extra_screenshot_callback_does_not_submit_notice(self):
        source = (BIN / "upload_event_module" / "ui" / "dialogs.py").read_text(encoding="utf-8")
        methods = {
            node.name: ast.unparse(node)
            for node in ast.walk(ast.parse(source))
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        self.assertIn("self.upload_confirmed.emit", methods["skip_screenshot"])
        self.assertNotIn("self.upload_confirmed.emit", methods["_on_extra_screenshot_encoded"])
        self.assertNotIn("self._capture_generation += 1", methods["_on_extra_screenshot_captured"])


if __name__ == "__main__":
    unittest.main()
