"""Keep personnel summaries in Bitable and verified historical backups in SQLite."""
import hashlib
import json
import sqlite3
import time
from contextlib import closing

from . import learning as core
from . import learning_cloud as cloud
from . import learning_shift_storage as shifts

PROGRESS_KEY = 'personnel_table_v1'
HISTORY_KINDS = {'paper', 'record', 'sharing_attempt', 'daily_progress'}


def _archive_schema(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS learning_cloud_archive(
        record_id TEXT NOT NULL, sha256 TEXT NOT NULL, document TEXT NOT NULL,
        files_ready INTEGER NOT NULL DEFAULT 0, removed INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY(record_id,sha256))''')
    conn.execute('''CREATE TABLE IF NOT EXISTS learning_cloud_files(
        file_token TEXT PRIMARY KEY, sha256 TEXT NOT NULL, content BLOB NOT NULL)''')
    conn.execute('''CREATE INDEX IF NOT EXISTS learning_cloud_archive_identity
        ON learning_cloud_archive(json_extract(document,'$.kind'),json_extract(document,'$.key'))''')


def _archive(service, records):
    with service.transaction() as conn:
        _archive_schema(conn)
        for row in records:
            document = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
            checksum = hashlib.sha256(document.encode('utf-8')).hexdigest()
            conn.execute('INSERT OR IGNORE INTO learning_cloud_archive(record_id,sha256,document) VALUES(?,?,?)',
                         (row['record_id'], checksum, document))
            stored = conn.execute('SELECT document FROM learning_cloud_archive WHERE record_id=? AND sha256=?',
                                  (row['record_id'], checksum)).fetchone()
            if not stored or hashlib.sha256(stored[0].encode('utf-8')).hexdigest() != checksum:
                raise core.LearningError('历史学练本地备份核验失败，未清理云端记录。', 503)


def _backup_database(service):
    directory = service.root / 'backups'
    directory.mkdir(exist_ok=True)
    target = directory / 'learning-before-personnel-only.sqlite3'
    temporary = target.with_suffix('.tmp')
    with closing(service._connect()) as source, closing(sqlite3.connect(temporary)) as backup:
        source.backup(backup)
        if backup.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
            raise core.LearningError('学练本地备份完整性检查未通过，未清理云端记录。', 503)
    temporary.replace(target)


def prepare(service):
    progress = service._get('local', PROGRESS_KEY) or {}
    if progress.get('prepared'):
        return
    service.cloud.ensure_personnel_schema()
    records = service.cloud.list_entity_records(include_fields=True)
    _archive(service, records)
    # Restore the exact captured graph before replacing any existing bundled document.
    service.restore(entities=service.cloud.load_entities(records=records))
    _backup_database(service)
    state = shifts.Snapshot(shifts.snapshot(service), include_legacy=True)
    groups, _ = state.groups()
    with service.transaction() as conn:
        for owner in groups:
            service._put('local', 'shift_summary_pending:' + shifts.key(owner), {'pending': True}, conn, False)
        service._put('local', PROGRESS_KEY, {'prepared': True, 'complete': False}, conn, False)


def _tokens(value):
    if isinstance(value, dict):
        for name, item in value.items():
            if name == 'file_token' and isinstance(item, str) and item:
                yield item
            else:
                yield from _tokens(item)
    elif isinstance(value, list):
        for item in value:
            yield from _tokens(item)


def archived_attachment(service, token, max_bytes):
    with closing(service._connect()) as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='learning_cloud_files'").fetchone():
            return None
        stored = conn.execute('SELECT sha256,content FROM learning_cloud_files WHERE file_token=?', (token,)).fetchone()
    if stored is None:
        return None
    if len(stored[1]) > max_bytes or hashlib.sha256(stored[1]).hexdigest() != stored[0]:
        raise core.LearningError('历史附件本地备份损坏或超过限额，未使用。', 503)
    return stored[1]


def _verify_archive(service, row, app_id):
    document = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)
    checksum = hashlib.sha256(document.encode('utf-8')).hexdigest()
    with closing(service._connect()) as conn:
        stored = conn.execute('SELECT document FROM learning_cloud_archive WHERE record_id=? AND sha256=?',
                              (row['record_id'], checksum)).fetchone()
    if not stored or hashlib.sha256(stored[0].encode('utf-8')).hexdigest() != checksum:
        raise core.LearningError('原记录尚无匹配的完整本地备份，未清理。', 503)
    for token in sorted(set(_tokens(row))):
        if service.cloud.current_app_id() != app_id:
            raise core.LearningError('飞书应用已变化，暂停历史清理。', 503)
        if archived_attachment(service, token, cloud.MAX_DOCUMENT_BYTES) is not None:
            continue
        content = service.cloud.download_attachment(token, cloud.MAX_DOCUMENT_BYTES, table_id=service.cloud._find_entity_table())
        if not isinstance(content, bytes) or not content:
            raise core.LearningError('历史附件尚未完整备份，未清理。', 503)
        if service.cloud.current_app_id() != app_id:
            raise core.LearningError('飞书应用已变化，暂停历史清理。', 503)
        with service.transaction() as conn:
            conn.execute('INSERT INTO learning_cloud_files(file_token,sha256,content) VALUES(?,?,?)',
                         (token, hashlib.sha256(content).hexdigest(), content))
        if archived_attachment(service, token, cloud.MAX_DOCUMENT_BYTES) != content:
            raise core.LearningError('历史附件本地回读核验失败，未清理。', 503)
    with service.transaction() as conn:
        conn.execute('UPDATE learning_cloud_archive SET files_ready=1 WHERE record_id=? AND sha256=?',
                     (row['record_id'], checksum))
    return checksum


def verify_bundle_archive(service, identity, app_id):
    with closing(service._connect()) as conn:
        archived = conn.execute('''SELECT document,files_ready FROM learning_cloud_archive
            WHERE json_extract(document,'$.kind')='shift_person' AND json_extract(document,'$.key')=?''',
            (identity,)).fetchall()
    expected = None
    for stored in archived:
        row = json.loads(stored[0])
        if row['payload'].get('entities'):
            if not stored[1]:
                _verify_archive(service, row, app_id)
            expected = row['payload_sha256']
    return expected


def cleanup(service, limit=8):
    progress = service._get('local', PROGRESS_KEY) or {}
    if not progress.get('prepared') or progress.get('complete') or progress.get('retry_at', 0) > time.time():
        return
    app_id = service.cloud.current_app_id()
    records = service.cloud.list_entity_records(include_fields=True)
    candidates = [row for row in records if row['kind'] != 'shift_person' and row['kind'] not in HISTORY_KINDS]
    history = [row for row in records if row['kind'] in HISTORY_KINDS
               and (service._get('local', 'personnel_history:' + row['record_id']) or {}).get('sha256') != row['payload_sha256']]
    state = shifts.Snapshot(shifts.snapshot(service), include_legacy=True)
    directory, directory_ready, _ = shifts.current_directory(service)
    groups, errors = state.groups()
    owners = {}
    for owner, members in groups.items():
        for pair in {pair for pair, _ in members} | state.dependencies[owner]:
            owners.setdefault(pair, set()).add(owner)
    ordered = sorted([*candidates, *history], key=lambda row: row['record_id'])
    cursor = progress.get('cursor', '')
    ordered = [r for r in ordered if r['record_id'] > cursor] + [r for r in ordered if r['record_id'] <= cursor]
    failures, removed, enriched = [], 0, 0
    for row in ordered[:limit]:
        if service._stop.is_set():
            break
        cursor = row['record_id']
        try:
            pair = (row['kind'], row['key'])
            if pair in errors:
                raise core.LearningError(errors[pair], 503)
            if row['kind'] in HISTORY_KINDS:
                if not directory_ready:
                    raise core.LearningError('人员目录尚未就绪，保留历史答题记录等待补齐。', 503)
                fields = shifts.history_fields(state, *pair, directory)
                oid, reason, _ = shifts.resolve_history_identity(shifts.history_identity(state, row), directory)
                if reason:
                    raise core.LearningError(reason, 503)
                fields['人员'] = [{'id': oid}] if oid else []
                service.cloud.enrich_entity_record(row['record_id'], *pair, fields,
                    expected_payload_sha256=row['payload_sha256'], expected_app_id=app_id,
                    expected_display_name=row.get('display_name', ''))
                with service.transaction() as conn:
                    service._put('local', 'personnel_history:' + row['record_id'], {'sha256': row['payload_sha256']}, conn, False)
                    conn.execute("DELETE FROM documents WHERE kind='local' AND key=?", ('shift_history_issue:' + row['record_id'],))
                enriched += 1
                continue
            if row['kind'] in shifts.KINDS:
                identity = shifts.history_identity(state, row)
                if identity['required'] and not owners.get(pair):
                    raise core.LearningError('历史人员班次归属待核对，原记录保留。', 503)
                for owner in owners.get(pair, ()):
                    ack = state.get('local', 'shift_cloud:' + shifts.key(owner))
                    pending = state.get('local', 'shift_summary_pending:' + shifts.key(owner)).get('pending')
                    if not ack.get('personnel_only') or pending or any(member['dirty'] for _, member in groups[owner]):
                        raise core.LearningError('对应人员班次汇总尚未确认成功，原记录保留。', 503)
            checksum = _verify_archive(service, row, app_id)
            service.cloud.delete_archived_entity(row, expected_app_id=app_id)
            with service.transaction() as conn:
                conn.execute('UPDATE learning_cloud_archive SET removed=1 WHERE record_id=? AND sha256=?',
                             (row['record_id'], checksum))
                conn.execute("DELETE FROM documents WHERE kind='local' AND key=?", ('shift_history_issue:' + row['record_id'],))
            removed += 1
        except Exception as exc:
            reason = str(exc)[:180]
            failures.append(reason)
            with service.transaction() as conn:
                service._put('local', 'shift_history_issue:' + row['record_id'], {
                    'record_id': row['record_id'], 'name': row.get('display_name', ''), 'reason': reason,
                }, conn, False)
    complete = (not candidates and len(history) == enriched
                and not any(row['payload'].get('entities') for row in records if row['kind'] == 'shift_person'))
    with service.transaction() as conn:
        service._put('local', PROGRESS_KEY, {'prepared': True, 'complete': complete,
                     'retry_at': time.time() + 60 if failures else 0,
                     'remaining': len(candidates) - removed + len(history) - enriched, 'cursor': cursor,
                     'error': '；'.join(failures[:3])}, conn, False)
    if failures:
        service._last_error = '历史学练清理待重试，原数据及本地备份保留：' + '；'.join(failures[:3])
