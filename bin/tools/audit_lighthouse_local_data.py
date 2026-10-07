"""Read a consistent private copy of real local data; never query or write cloud APIs."""
import asyncio
import json
import socket
import sqlite3
import sys
import tempfile
import threading
import time
from contextlib import closing
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def run():
    from openclaw_service.protocol import protect_state_directory
    source_path = Path(__file__).resolve().parents[1] / 'data/lan_portal_state.sqlite3'
    if not source_path.is_file():
        raise FileNotFoundError('Local business database is unavailable')
    with tempfile.TemporaryDirectory(prefix='lighthouse-local-audit-') as directory:
        root = Path(directory).resolve()
        protect_state_directory(root)
        copied_path = root / 'snapshot.sqlite3'
        assert copied_path.resolve().is_relative_to(root)
        started = time.monotonic()
        def progress(*_):
            if time.monotonic() - started > 30:
                raise TimeoutError('Local snapshot copy exceeded its budget')
        with closing(sqlite3.connect(source_path.resolve().as_uri() + '?mode=ro', uri=True)) as source:
            source.execute('PRAGMA query_only=ON')
            assert source.execute('PRAGMA query_only').fetchone()[0] == 1
            with closing(sqlite3.connect(copied_path)) as target:
                source.backup(target, pages=2048, progress=progress)
        with asyncio.Runner() as runner, \
                patch.object(socket.socket, 'connect', side_effect=AssertionError('Offline audit attempted networking')), \
                patch('socket.getaddrinfo', side_effect=AssertionError('Offline audit attempted DNS')):
            from lan_bitable_template_portal.state_store import LanPortalStateStore
            from lan_bitable_template_portal.portal_service import MaintenancePortalService, DEFAULT_APP_TOKEN
            from openclaw_service.assistant.lighthouse_pending import cached_items, collect_pending
            from openclaw_service.assistant.lighthouse_sources import SCOPES, record_codes
            store = LanPortalStateStore(copied_path)
            service = MaintenancePortalService.__new__(MaintenancePortalService)
            service.app_token = DEFAULT_APP_TOKEN
            service._state_store = store
            runtime = SimpleNamespace(state_store=store, service=service)
            report = {'source_readonly': True, 'copy_seconds': round(time.monotonic() - started, 3),
                      'snapshots': [], 'projections': []}
            try:
                with closing(sqlite3.connect(copied_path)) as connection:
                    for row in connection.execute('SELECT source_key,record_count FROM repair_snapshot_sources ORDER BY source_key'):
                        count = connection.execute('SELECT count(*) FROM repair_snapshot_records WHERE source_key=?', (row[0],)).fetchone()[0]
                        assert count == row[1], 'Snapshot header count disagrees with records'
                        report['snapshots'].append({'source': row[0], 'records': count})
                for kind in ('events', 'orders', 'mops'):
                    for scopes in ([scope] for scope in sorted(SCOPES)):
                        begin = time.monotonic()
                        loaded = cached_items(kind, scopes, runtime, allowed=SCOPES)
                        items, warnings = loaded[:2]
                        assert len({item['id'] for item in items}) == len(items), 'Duplicate projected identity'
                        assert all(item['scopes'] and set(item['scopes']) <= set(scopes) for item in items), 'Cross-scope projection'
                        metadata = loaded[2] if len(loaded) == 3 else {}
                        report['projections'].append({'kind': kind, 'scope': scopes[0], 'count': len(items),
                            'partial': bool(warnings), 'seconds': round(time.monotonic() - begin, 3),
                            'source_freshness': metadata.get('source_freshness', [])})

                # Rebuild only the private copy, then compare indexed lists with
                # native detail projection from independently grouped followups.
                begin = time.monotonic()
                service._repair_snapshots_enabled = True
                service._repair_project_status_index_lock = threading.RLock()
                rebuilt = service._rebuild_repair_project_status_index()
                _, metas, projects = service._repair_snapshot_from_local(store.get_repair_snapshot('repair_projects'))
                followups = store.get_repair_snapshot('repair_followups')['records']
                project_ids = [item['record_id'] for item in projects]
                grouped = service._repair_followups_by_summary(followups, project_ids)
                sql_grouped = store.repair_snapshot_records_by_parents('repair_followups', project_ids)
                for identity in project_ids:
                    assert {row['record_id'] for row in grouped.get(identity, [])} == {
                        row['record_id'] for row in sql_grouped.get(identity, [])}, 'Followup SQL grouping differs from detail grouping'
                targets = service._repair_relation_snapshot_records('repair_notices', {
                    service._repair_target_record_id(item) for item in projects})
                details = {item['record_id']: service._repair_management_record_payload(item, meta_by_name=metas,
                    authoritative_followups=grouped.get(item['record_id'], []),
                    authoritative_target_record=targets.get(service._repair_target_record_id(item)),
                    summary_only=True) for item in projects}
                assert rebuilt['record_count'] == len(details)
                unassigned = [row for row in details.values() if not row['is_completed'] and not row['building_codes']]
                report['repairs'] = {'projects': len(details), 'followup_rows': len(followups), 'pages': [],
                    'unassigned_active': len(unassigned),
                    'unassigned_with_assistant_scope': sum(bool(record_codes(row)) for row in unassigned)}
                for scope in ('ALL', *sorted(SCOPES)):
                    for state in ('active', 'completed'):
                        expected = {identity for identity, row in details.items()
                            if service._scope_matches_buildings(scope, row['building_codes'])
                            and row['is_completed'] == (state == 'completed')}
                        seen, offset = set(), 0
                        while True:
                            page = store.query_repair_project_status_page('repair_projects', scope=scope,
                                state=state, limit=200, offset=offset)
                            assert page['total'] == len(expected), 'Repair list count differs from detail projection'
                            for row in page['records']:
                                identity = row['record_id']
                                assert identity not in seen, 'Repair pagination repeats an identity'
                                seen.add(identity)
                                indexed, detail = row['_repair_status_index'], details[identity]
                                for field in ('workflow', 'followup_count', 'progress_percent'):
                                    assert indexed[field] == detail[field], 'Repair index/detail state differs: ' + field
                            offset += len(page['records'])
                            if not page['has_more']:
                                break
                            assert page['records'], 'Repair pagination did not advance'
                        assert seen == expected, 'Repair pagination omitted records'
                        report['repairs']['pages'].append({'scope': scope, 'state': state, 'count': len(seen)})
                report['repairs']['seconds'] = round(time.monotonic() - begin, 3)
                async def invoke(operation):
                    assert operation['api_id'] == 'GET /api/repair-management/records'
                    params = operation['params']
                    limit = int(params['limit'])
                    offset = int(params['offset'])
                    page = store.query_repair_project_status_page('repair_projects', scope=params['scope'],
                        state=params['state'], limit=limit, offset=offset)
                    return {'ok': True, '_raw': {**page,
                        'records': [details[row['record_id']] for row in page['records']]}}

                begin = time.monotonic()
                pending = runner.run(collect_pending({'id': 'private-audit', 'scopes': sorted(SCOPES)},
                    '现在未完成维修项目', invoke, None, groups_only={'repairs'}))
                group = pending['groups'][0]
                expected = {row['record_id'] for row in details.values()
                    if not row['is_completed'] and record_codes(row)}
                assert group['known_count'] == len(expected), 'Assistant repair count differs from scoped native projection'
                if unassigned:
                    assert not group['available'] and group['count'] is None
                    assert any('楼栋' in warning for warning in group['warnings'])
                else:
                    assert group['available'] and group['count'] == len(expected)
                report['repairs']['assistant'] = {'available': group['available'],
                    'count': group['count'], 'known_count': group['known_count'],
                    'warns_unassigned': any('楼栋' in warning for warning in group['warnings']),
                    'seconds': round(time.monotonic() - begin, 3)}
            finally:
                store.shutdown_write_worker()
            print(json.dumps(report, ensure_ascii=False))


if __name__ == '__main__':
    run()
