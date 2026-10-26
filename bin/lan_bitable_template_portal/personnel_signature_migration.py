"""One-way, non-destructive signature copy into existing personnel rows."""
from collections import Counter, defaultdict
import hashlib
import json
import time

from . import personnel_source as source

OLD_BASE = 'HU38bc1vnamMK9sCeOgclUvXnFc'
OLD_TABLE = 'tbluozblhRAjbljX'
NAMESPACE = 'personnel_signature_migration'
FIELDS = {'手写签名': 17, '密钥': 1}


def ensure_fields(service):
    _, existing = service._load_table_fields(app_token=source.APP_TOKEN, table_id=source.TABLE_ID)
    for name, kind in FIELDS.items():
        if name not in existing:
            try:
                reply = service._request_payload('POST',
                    f'https://open.feishu.cn/open-apis/bitable/v1/apps/{source.APP_TOKEN}/tables/{source.TABLE_ID}/fields',
                    context='员工信息池签名字段创建', headers=service._auth_headers(), retries=0,
                    json_payload={'field_name': name, 'type': kind})
                if reply.get('code') != 0:
                    raise ValueError('签名字段创建未确认')
            except Exception:
                pass
            # Re-read even after an uncertain response; never blindly create again.
            _, existing = service._load_table_fields(app_token=source.APP_TOKEN, table_id=source.TABLE_ID)
        field = existing.get(name)
        if field is None or field.field_type != kind or field.has_formula:
            raise ValueError('员工信息池字段类型需核对：' + name)


def _identity(row):
    fields = row.get('fields') or {}
    users = fields.get('员工姓名') or []
    oid = (users[0].get('id') or users[0].get('open_id') or '') if isinstance(users, list) and len(users) == 1 and isinstance(users[0], dict) else ''
    return source.text(fields.get('姓名')), source.text(fields.get('员工工号')), oid


def _same(key, current, expected):
    if key == '手写签名':
        return sorted(a['file_token'] for a in (current or [])) == sorted(a['file_token'] for a in (expected or []))
    return source.text(current) == source.text(expected)


def _verify_files(service, target, state):
    expected = {a['file_token']: a['sha256'] for a in state.get('files', {}).values()}
    actual = service._extract_signature_attachments(target['fields'])
    if {a['file_token'] for a in actual} != set(expected):
        raise ValueError('签名附件回读不一致')
    for attachment in actual:
        context = service._attachment_with_cache_context(attachment, category='signature',
            app_token=source.APP_TOKEN, table_id=source.TABLE_ID, record_id=target['record_id'], latest_publish_time='')
        content, _ = service._download_mop_attachment(context, force_download=True)
        if hashlib.sha256(content).hexdigest() != expected[attachment['file_token']]:
            raise ValueError('签名附件复制校验失败')


def match_people(old_rows, new_rows):
    indexes = [defaultdict(list) for _ in range(3)]
    for row in new_rows:
        for index, value in zip(indexes, _identity(row)):
            if value:
                index[value].append(row)
    matched, unresolved = [], []
    for old in old_rows:
        name, number, oid = _identity(old)
        choices = indexes[1][number] if number else indexes[2][oid] if oid else indexes[0][name]
        if len(choices) > 1:
            choices = [item for item in choices if _identity(item)[0] == name
                       and (not oid or _identity(item)[2] == oid)]
        reason = '工号、身份或姓名未唯一匹配'
        if len(choices) == 1:
            target = choices[0]
            n, no, user = _identity(target)
            if name == n and (not number or number == no) and (not oid or not user or oid == user):
                matched.append((old, target))
                continue
            reason = '姓名或身份对应关系冲突'
        unresolved.append({'record_id': old['record_id'], 'name': name, 'employee_no': number, 'reason': reason})
    counts = Counter(row['record_id'] for _, row in matched)
    result = []
    for old, target in matched:
        if counts[target['record_id']] == 1:
            result.append((old, target))
        else:
            unresolved.append({'record_id': old['record_id'], 'name': _identity(old)[0], 'reason': '多个旧记录对应同一员工，未自动合并'})
    return result, unresolved


def migrate(service, *, force=False):
    previous = service._state_store.get_document(NAMESPACE, 'summary') or {}
    if previous.get('complete') and not force:
        return previous
    ensure_fields(service)
    old_rows = source.read_records(service, base=OLD_BASE, table=OLD_TABLE,
        fields=['姓名', '员工姓名', '员工工号', '手写签名', '密钥'])
    new_rows = source.read_records(service, fields=['姓名', '员工姓名', '员工工号', *FIELDS])
    pairs, unresolved = match_people(old_rows, new_rows)
    identities = {row['record_id']: _identity(row) for _, row in pairs}
    pending, errors, copied, verified = [], [], 0, 0
    states = {}
    for old, target in pairs:
        old_id, target_id = old['record_id'], target['record_id']
        target_fields, old_fields = target['fields'], old['fields']
        state = service._state_store.get_document(NAMESPACE, old_id) or {}
        fields = {}
        if target_fields.get('手写签名'):
            if state.get('files') and {a['file_token'] for a in target_fields['手写签名']} == {a['file_token'] for a in state['files'].values()}:
                try:
                    _verify_files(service, target, state)
                    verified += 1
                except Exception as exc:
                    errors.append({'record_id': old_id, 'reason': type(exc).__name__ + '：签名回读待重试'})
            continue
        elif old_fields.get('手写签名'):
            try:
                attachments = service._extract_signature_attachments(old_fields)
                stamp = hashlib.sha256(json.dumps([[a['file_token'] for a in attachments], old_fields.get('密钥')], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
                if state.get('stamp') != stamp or state.get('target_id') != target_id:
                    state = {'stamp': stamp, 'target_id': target_id, 'files': {}}
                files = []
                for attachment in attachments:
                    old_token = attachment['file_token']
                    saved = state['files'].get(old_token)
                    if not saved:
                        original = service._attachment_with_cache_context(attachment, category='signature',
                            app_token=OLD_BASE, table_id=OLD_TABLE, record_id=old_id, latest_publish_time='')
                        content, _ = service._download_mop_attachment(original)
                        token = service._upload_encrypted_signature_file(encrypted_bytes=content,
                            file_name=str(attachment.get('name') or attachment.get('file_name') or 'signature.sigenc'), app_token=source.APP_TOKEN)
                        saved = {'file_token': token, 'sha256': hashlib.sha256(content).hexdigest()}
                        state['files'][old_token] = saved
                        service._state_store.put_document(NAMESPACE, old_id, state)
                    files.append({'file_token': saved['file_token']})
                states[target_id] = state
                fields['手写签名'] = files
                fields['密钥'] = source.text(old_fields.get('密钥'))
                copied += 1
            except Exception as exc:
                errors.append({'record_id': old_id, 'name': _identity(old)[0], 'reason': type(exc).__name__ + '：签名复制待重试'})
                continue
        elif source.text(old_fields.get('密钥')) and not source.text(target_fields.get('密钥')):
            fields['密钥'] = source.text(old_fields['密钥'])
        if any(not _same(k, target_fields.get(k), v) for k, v in fields.items()):
            pending.append({'record_id': target_id, 'fields': fields})
    for start in range(0, len(pending), 50):
        batch = pending[start:start + 50]
        fresh = {row['record_id']: row for row in source.read_records(service, fields=['姓名', '员工姓名', '员工工号', *FIELDS])}
        writes = []
        for row in batch:
            current = fresh.get(row['record_id'])
            if not current:
                errors.append({'record_id': row['record_id'], 'reason': '目标员工记录不存在'})
                continue
            if _identity(current) != identities[row['record_id']]:
                errors.append({'record_id': row['record_id'], 'reason': '员工身份或签名对应关系已变化，未覆盖'})
                continue
            if current['fields'].get('手写签名'):
                continue
            if source.text(current['fields'].get('密钥')) not in ('', row['fields'].get('密钥', '')):
                errors.append({'record_id': row['record_id'], 'reason': '目标密钥已变化，未覆盖'})
                continue
            writes.append(row)
        if not writes:
            continue
        try:
            reply = service._request_payload('POST',
                f'https://open.feishu.cn/open-apis/bitable/v1/apps/{source.APP_TOKEN}/tables/{source.TABLE_ID}/records/batch_update',
                context='员工签名批量迁移', headers=service._auth_headers(), retries=0, json_payload={'records': writes})
        except Exception:
            reply = {}
        readback = {row['record_id']: row for row in source.read_records(service, fields=['姓名', *FIELDS])}
        for row in writes:
            actual = readback.get(row['record_id'], {})
            valid = all(_same(key, actual.get('fields', {}).get(key), value) for key, value in row['fields'].items())
            if valid and row['fields'].get('手写签名'):
                try:
                    _verify_files(service, actual, states[row['record_id']])
                except Exception:
                    valid = False
            if valid:
                verified += 1
            else:
                errors.append({'record_id': row['record_id'], 'reason': '迁移回读未确认，保留源记录'})
        time.sleep(.2)
    service.signature_management.invalidate()
    summary = {'complete': not errors and not unresolved, 'matched': len(pairs), 'copied': copied, 'verified_updates': verified,
               'unresolved': unresolved, 'errors': errors, 'updated_at': time.time()}
    service._state_store.put_document(NAMESPACE, 'summary', summary)
    return summary
