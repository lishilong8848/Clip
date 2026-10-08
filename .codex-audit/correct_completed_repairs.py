"""User-authorized status correction. Dry-run unless --apply is supplied."""
import argparse
import datetime as dt
import json
from pathlib import Path
import sqlite3
import sys
import threading

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'bin'))
from lan_bitable_template_portal.portal_service import MaintenancePortalService as Service
from lan_bitable_template_portal.state_store import LanPortalStateStore

parser = argparse.ArgumentParser()
parser.add_argument('--apply', action='store_true')
args = parser.parse_args()
db_path = ROOT / 'bin/data/lan_portal_state.sqlite3'
with sqlite3.connect(db_path.as_uri() + '?mode=ro', uri=True) as db:
    settings = {k: json.loads(v) for k, v in db.execute(
        "SELECT key,value_json FROM settings WHERE key IN ('feishu_app_id','feishu_app_secret')")}
    automatic = {r: op.rsplit(':', 1)[-1] for op, r in db.execute(
        "SELECT operation_id,record_id FROM repair_management_operations WHERE operation_type='project_create' "
        "AND operation_id LIKE 'event-end-transfer:tblschT48zXwigUG:%' AND status IN ('completed','sync_pending')")}

audit_root = ROOT / '.codex-audit'
schemas = {name: json.loads((audit_root / f'repair-{name}-schema.json').read_text('utf-8'))
           for name in ('projects', 'reference')}
assert schemas['projects']['table'] == 'tblschT48zXwigUG'
assert schemas['reference']['table'] == 'tblj9XJLq5QzTAqX'
snapshots = {name: json.loads((audit_root / f'repair-{name}-rows.json').read_text('utf-8'))
             for name in schemas}
option_maps = {row['field_name']: Service._extract_option_map(row)[0] for row in schemas['reference']['fields']}
plain = Service._repair_management_plain_text

def normalized(fields):
    return {key: '、'.join(option_maps[key].get(part, part) for part in plain(value).split('、'))
            if option_maps.get(key) else value for key, value in fields.items()}

events = {row['record_id']: normalized(row['fields']) for row in snapshots['reference']}
candidates = {row['record_id']: row for row in snapshots['projects']
              if automatic.get(row['record_id']) == plain(row['fields'].get('关联事件单-L'))
              and Service._event_repair_is_completed(events.get(automatic[row['record_id']], {}))}
print('DRY_RUN', json.dumps({'candidates': len(candidates), 'unstarted': len(snapshots['projects']),
    'source_completed_and_auto_creation_verified': len(candidates)}), flush=True)
if not args.apply:
    raise SystemExit(0)
assert candidates, 'No confirmed candidates'

stamp = dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).strftime('%Y%m%d_%H%M%S')
audit_file = ROOT / 'bin/data/audit' / f'repair_completed_correction_{stamp}.json'
audit_file.parent.mkdir(parents=True, exist_ok=True)
audit = {'target_table': schemas['projects']['table'], 'source_table': schemas['reference']['table'],
         'created_at': stamp, 'requested_ids': sorted(candidates), 'batches': [], 'verified': {}, 'skipped': {}}

def persist():
    temporary = audit_file.with_suffix('.tmp')
    temporary.write_text(json.dumps(audit, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(audit_file)

persist()
with httpx.Client(timeout=30, trust_env=False) as client:
    auth = client.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
        json={'app_id': settings['feishu_app_id'], 'app_secret': settings['feishu_app_secret']}).json()
    assert auth.get('code') == 0, 'Credential validation failed'
    client.headers['Authorization'] = 'Bearer ' + auth['tenant_access_token']

    def request(name, action, body):
        target = schemas[name]
        if action == 'batch_update':
            assert name == 'projects'
            for row in body['records']:
                assert row['record_id'] in candidates
                assert set(row['fields']) <= {'流程-L', '当前维修进度-L'}
                assert row['fields']['流程-L'] == '维修完成'
        else:
            assert action == 'batch_get'
        value = client.post(f"https://open.feishu.cn/open-apis/bitable/v1/apps/{target['app']}/tables/{target['table']}/records/{action}",
                            json=body).json()
        assert value.get('code') == 0, (value.get('code'), value.get('msg'))
        return {row['record_id']: row for row in value['data']['records']}

    ids = sorted(candidates)
    for offset in range(0, len(ids), 40):
        current_ids = ids[offset:offset + 40]
        current = request('projects', 'batch_get', {'record_ids': current_ids})
        source_ids = sorted({automatic[rid] for rid in current_ids})
        source = request('reference', 'batch_get', {'record_ids': source_ids})
        updates, before = [], {}
        for rid in current_ids:
            fields = (current.get(rid) or {}).get('fields') or {}
            original = candidates[rid]['fields']
            event_id = automatic[rid]
            evidence = normalized((source.get(event_id) or {}).get('fields') or {})
            if (plain(fields.get('流程-L')) != '未开始'
                    or plain(fields.get('关联事件单-L')) != event_id
                    or plain(fields.get('维修跟进记录-L'))
                    or plain(fields.get('当前维修进度-L')) != plain(original.get('当前维修进度-L'))
                    or not Service._event_repair_is_completed(evidence)):
                audit['skipped'][rid] = 'Current state, links or evidence changed; not overwritten'
                continue
            desired = {'流程-L': '维修完成'}
            progress = Service._repair_followup_progress_percent(evidence.get('检修进展'))
            if progress is not None and progress >= 100:
                desired['当前维修进度-L'] = 1
            before[rid] = {'fields': {key: fields.get(key) for key in desired}, 'event_id': event_id,
                           'source': {key: evidence.get(key) for key in ('最终状态','检修进度','检修进展')}}
            updates.append({'record_id': rid, 'fields': desired})
        batch = {'before': before, 'updates': updates, 'status': 'prepared'}
        audit['batches'].append(batch)
        persist()
        if not updates:
            continue
        try:
            request('projects', 'batch_update', {'records': updates})
            batch['status'] = 'response_received'
        except Exception as exc:
            batch['status'] = 'response_unconfirmed'
            batch['error_type'] = type(exc).__name__
        persist()
        # Never resend a write after a missing response: verify its original IDs.
        verified = request('projects', 'batch_get', {'record_ids': [row['record_id'] for row in updates]})
        for row in updates:
            actual = (verified.get(row['record_id']) or {}).get('fields') or {}
            assert all(plain(actual.get(key)) == plain(value) for key, value in row['fields'].items()), \
                'Write not confirmed; audit retained, no automatic resend'
            audit['verified'][row['record_id']] = row['fields']
        batch['status'] = 'verified'
        persist()
        print('VERIFIED', len(audit['verified']), 'SKIPPED', len(audit['skipped']), flush=True)

# Refresh only the confirmed local snapshots and their existing status index.
store = LanPortalStateStore(db_path)
service = Service.__new__(Service)
service._state_store = store
service._field_meta_by_name = {}
service._repair_snapshots_enabled = True
service._repair_project_status_index_lock = threading.RLock()
try:
    snapshot = store.get_repair_snapshot('repair_projects', record_ids=list(audit['verified']))
    for record in snapshot['records']:
        rid = record['record_id']
        fields = Service._repair_logical_record_fields('tblschT48zXwigUG', audit['verified'][rid])
        for name in ('raw_fields', 'display_fields'):
            record.setdefault(name, {}).update(fields)
        envelope = Service._repair_snapshot_record_payload(record)
        store.upsert_repair_snapshot_record('repair_projects', rid, record,
            parent_record_id=envelope.get('parent_record_id',''), scope_codes=envelope['scope_codes'],
            title=envelope['title'], status=envelope['status'], search_text=envelope['search_text'], sort_time=envelope['sort_time'])
    service._rebuild_repair_project_status_index(list(audit['verified']))
    audit['local_refreshed'] = True
    persist()
finally:
    store.shutdown_write_worker()
print('DONE', len(audit['verified']), str(audit_file), flush=True)
