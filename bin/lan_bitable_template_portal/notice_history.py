"""Notice record history and recoverable, message-free field corrections."""
from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import math
import re
import threading
import time
import uuid
from contextlib import closing
from urllib.parse import urlencode, urlsplit

from openclaw_service.assistant.lighthouse_ai import AssistantError
from upload_event_module.config import config, get_field_config
from upload_event_module.services.handlers.base import notice_field_text
from .document_library_remote import DocumentLibraryRemote
from .module_access import allowed_modules, require_module
from .operation_audit import begin_business_audit, finish_business_audit
from .workbench_lite import NOTICE_TYPE_BY_WORK_TYPE, WORK_TYPE_LABELS

ZONE = dt.timezone(dt.timedelta(hours=8))
SCOPES = ('110', 'A', 'B', 'C', 'D', 'E', 'H')
BUILDING_FIELDS = ('楼栋', '机楼', '所属楼栋', '楼栋-L', '机楼-L',
    '所属数据中心/楼栋-使用', '所属数据中心/楼栋-使用-L', '涉及楼栋', '涉及楼栋-L')
NAMESPACE = 'notice_history_corrections'
WRITABLE = {1, 2, 3, 4, 5, 7, 15}


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def field_value(field, value):
    kind = field.get('type')
    if kind == 1:
        return notice_field_text(value) if value is not None else ''
    if kind in {3, 4}:
        names = {item.get('id'): item.get('name') for item in (field.get('property') or {}).get('options', [])}
        if kind == 4:
            return [names.get(item, item) for item in value] if isinstance(value, list) else []
        return names.get(value, value) if isinstance(value, str) else None
    if kind == 15:
        return {'link': value.get('link', ''), 'text': value.get('text', '')} if isinstance(value, dict) else {'link': '', 'text': ''}
    if kind == 7:
        return bool(value)
    return value


def comparable(field, value):
    if field.get('type') in {17, 11}:
        return sorted(str(item.get('file_token') or item.get('id') or '') for item in value
                      if isinstance(item, dict)) if isinstance(value, list) else []
    value = field_value(field, value)
    if field.get('type') == 4:
        return sorted(value)
    return value


def normalize_edit(field, value):
    kind, name = field.get('type'), field.get('field_name', '')
    if kind not in WRITABLE:
        raise AssistantError(f'“{name}”为只读字段。')
    if value is None:
        return None
    if kind == 1 and isinstance(value, str) and len(value) <= 20000:
        return value
    if kind == 2 and type(value) in (float, int) and math.isfinite(value):
        return value
    if kind in {3, 4}:
        options = {item.get('name') for item in (field.get('property') or {}).get('options', [])}
        if kind == 3 and (value == '' or isinstance(value, str) and value in options):
            return value or None
        if kind == 4 and isinstance(value, list) and len(value) <= 100 and all(isinstance(item, str) and item in options for item in value):
            return list(dict.fromkeys(value))
    if kind == 5 and type(value) is int and 0 <= value <= 4133980800000:
        return value
    if kind == 7 and type(value) is bool:
        return value
    if kind == 15 and isinstance(value, dict) and not set(value) - {'link', 'text'}:
        link, text = value.get('link') or '', value.get('text') or ''
        if (isinstance(link, str) and isinstance(text, str) and len(link) <= 2048 and len(text) <= 500
                and (not link or urlsplit(link).scheme in {'http', 'https'}) and not re.search(r'[\x00-\x20]', link)):
            return {'link': link, 'text': text or link} if link else None
    raise AssistantError(f'“{name}”的值与字段类型或选项不符，请重新核对。')


class NoticeHistory:
    def __init__(self, controller, runtime):
        self.controller, self.runtime = controller, runtime
        self.service, self.store, self.auth = runtime.service, runtime.state_store, runtime.auth_manager
        self.locks = [threading.RLock() for _ in range(32)]
        self.local = threading.local()

    def _user(self, session, work='all'):
        user = session.get('user') or {} if isinstance(session, dict) else {}
        if not user.get('open_id') or session.get('is_guest') or session.get('temporary'):
            raise AssistantError('请使用正式账号登录。', 401)
        modules = set(allowed_modules(self.auth, session))
        types = [key for key in WORK_TYPE_LABELS if key in modules]
        if work != 'all':
            if work not in WORK_TYPE_LABELS:
                raise AssistantError('通告类型无效。')
            require_module(self.auth, session, work)
            types = [work]
        if not types:
            raise AssistantError('当前账号没有通告查看权限。', 403)
        return str(user['open_id']), types, {scope for scope in SCOPES if self.auth.scope_allowed(session, scope)}

    def _config_identity(self):
        from upload_event_module.services.feishu_token_manager import token_manager
        app, secret = token_manager._credentials()
        return digest([app, secret, config.app_token, self.service.app_token, self.service.table_id,
            {key: config.get_table_id(value) for key, value in NOTICE_TYPE_BY_WORK_TYPE.items()}])

    def _identity_revision(self, work, record_id):
        identity = self.store.resolve_notice_identity(work_type=work, target_record_id=record_id) or {}
        payload = identity.get('payload') or {}
        return digest([{key: identity.get(key) for key in ('identity_id', 'work_type', 'source_app_token',
            'source_table_id', 'source_record_id', 'target_app_token', 'target_table_id', 'target_record_id', 'deleted_at')},
            {key: payload.get(key) for key in ('paired_maintenance_target_record_id', 'repair_management_record_id', '_source_period')}])

    def _target(self, work):
        notice = NOTICE_TYPE_BY_WORK_TYPE[work]
        base, table = str(config.app_token or ''), str(config.get_table_id(notice) or '')
        if not base or not table:
            raise AssistantError('该通告类型尚未配置目标数据表。', 503)
        return {'key': 'target', 'label': WORK_TYPE_LABELS[work] + '通告字段', 'base_token': base,
                'table_id': table, 'notice_type': notice, 'work_type': work}

    def _remote(self, group):
        return closing(DocumentLibraryRemote(group))

    def _codes(self, fields, work):
        codes = set(self.service._target_record_building_codes(fields, get_field_config(NOTICE_TYPE_BY_WORK_TYPE[work])))
        for key in BUILDING_FIELDS:
            codes.update(self.service._building_codes_from_value(fields.get(key)))
        return codes & set(SCOPES)

    def _authorize_record(self, session, work, fields, *, inherited=None):
        _, _, allowed = self._user(session, work)
        codes = self._codes(fields, work)
        if not codes and any(fields.get(key) not in (None, '', [], {}) for key in BUILDING_FIELDS):
            raise AssistantError('关联记录已有楼栋信息但尚未准确识别，未沿用其它记录的权限。', 403)
        codes = codes or set(inherited or [])
        if codes - allowed or not codes and not self.auth.is_admin(session):
            raise AssistantError('该记录楼栋范围不在当前账号权限内。', 403)
        return codes

    def _normalized(self, group, raw, fields):
        metas = self.service._parse_field_metas(fields)
        return self.service._normalize_record(raw, meta_by_name={meta.field_name: meta for meta in metas},
            work_type=group['work_type'], notice_type=group['notice_type'],
            source_app_token=group['base_token'], source_table_id=group['table_id'])

    def _summary(self, work, record):
        notice = NOTICE_TYPE_BY_WORK_TYPE[work]
        form = self.service._target_record_form_fields(work_type=work, notice_type=notice, target_record=record)
        life = self.service._target_record_lifecycle(work_type=work, notice_type=notice, target_record=record)
        return {'record_id': record['record_id'], 'work_type': work, 'notice_type': notice,
                'title': form.get('title') or '未填写名称', 'building': form.get('building') or '',
                'start_time': life.get('started_at') or '', 'end_time': life.get('ended_at') or '',
                'status': life.get('status') or ('已结束' if life.get('finished') else '进行中' if life.get('active') else '待核对')}

    def list(self, session, *, month='', scope='ALL', work_type='all', q='', page=1, page_size=20):
        _, types, allowed = self._user(session, work_type)
        month = month or dt.datetime.now(ZONE).strftime('%Y-%m')
        if not re.fullmatch(r'20\d{2}-(?:0[1-9]|1[0-2])', month):
            raise AssistantError('请选择有效的统计月份。')
        if scope not in {*SCOPES, 'ALL', 'CAMPUS'} or scope in SCOPES and scope not in allowed:
            raise AssistantError('无权查看所选楼栋。', 403)
        selected = allowed if scope == 'ALL' else allowed & set('ABCDE') if scope == 'CAMPUS' else {scope}
        page, page_size = max(1, int(page)), min(100, max(1, int(page_size)))
        rows, warnings, successful, missing_dates = [], [], 0, 0
        for work in types:
            try:
                self._target(work)
                records = self.service._target_records_for_notice_type(NOTICE_TYPE_BY_WORK_TYPE[work], work)
                snapshot = self.store.get_repair_snapshot_meta(self.service._notice_target_snapshot_source_key(work)) or {}
                if snapshot.get('status') == 'failed':
                    warnings.append(WORK_TYPE_LABELS[work] + '最新同步未完成，当前保留上次成功数据。')
                successful += 1
            except Exception:
                warnings.append(WORK_TYPE_LABELS[work] + '历史通告读取未完成，请稍后重新读取。')
                continue
            for record in records:
                codes = self._codes(record.get('raw_fields') or record.get('display_fields') or {}, work)
                if codes - allowed or codes and not codes & selected or not codes and (not self.auth.is_admin(session) or scope != 'ALL'):
                    continue
                summary = self._summary(work, record)
                stamp = self.service._repair_management_datetime_ms(summary['start_time'])
                if stamp is None:
                    missing_dates += 1
                    continue
                if dt.datetime.fromtimestamp(stamp / 1000, ZONE).strftime('%Y-%m') != month:
                    continue
                if q and str(q).casefold() not in (summary['title'] + ' ' + summary['building']).casefold():
                    continue
                rows.append(summary)
        if not successful:
            raise AssistantError('历史通告暂未读取成功，不能判断为没有记录。', 503)
        if missing_dates:
            warnings.append(f'{missing_dates}条记录缺少可核验的实际开始时间，未混入所选月份。')
        rows.sort(key=lambda row: (row['start_time'], row['work_type'], row['record_id']), reverse=True)
        return {'items': rows[(page - 1) * page_size:page * page_size], 'total': len(rows), 'month': month,
                'page': page, 'page_size': page_size, 'warnings': warnings,
                'scope_options': [{'value': 'ALL', 'label': '全部有权楼栋'},
                    *({'value': code, 'label': self.auth.scope_label(code)} for code in SCOPES if code in allowed)],
                'types': [{'value': 'all', 'label': '全部类型'},
                    *({'value': key, 'label': WORK_TYPE_LABELS[key]} for key in self._user(session)[1])]}

    def _editable(self, field, work):
        mapping = get_field_config(NOTICE_TYPE_BY_WORK_TYPE[work])
        protected = {*BUILDING_FIELDS, *(mapping.get(key) for key in ('status', 'building', 'building_codes'))}
        name = field['field_name']
        if name in protected or re.search(r'记录ID|记录编号|重要操作H楼确认|H楼确认', name, re.I):
            return False, '办理状态、归属和关联标识沿用原办理流程。'
        if field.get('type') not in WRITABLE or field.get('is_computed'):
            return False, '系统计算、人员、附件或关联字段只读；关联计算值由源字段自动更新。'
        return True, ''

    def _snapshot(self, session, work, record_id):
        self._user(session, work)
        if not re.fullmatch(r'rec[A-Za-z0-9]+', str(record_id)):
            raise AssistantError('通告记录标识无效。')
        group = {**self._target(work), 'record_id': record_id}
        with self._remote(group) as remote:
            fields, raw = remote.fields(), remote.get(record_id)
        if raw is None:
            raise AssistantError('该通告已被删除，请重新读取列表。', 404)
        codes = self._authorize_record(session, work, raw['fields'])
        normalized = self._normalized(group, raw, fields)
        group.update(raw=raw, schema=fields)
        mapping = get_field_config(NOTICE_TYPE_BY_WORK_TYPE[work])
        protected = {*BUILDING_FIELDS, *(mapping.get(key) for key in ('status', 'building', 'building_codes'))}
        group['constraints'] = [{'field': name, 'value': copy.deepcopy(raw['fields'].get(name))}
            for name in protected if name]
        identity = self.store.resolve_notice_identity(work_type=work, target_record_id=record_id) or {}
        from .notice_history_relations import related_records
        descriptors, warnings = related_records(self.service, self.store, work, raw, fields, identity)
        groups = [group]
        seen = {(group['base_token'], group['table_id'], record_id): group}
        for descriptor in descriptors:
            group['constraints'].extend(copy.deepcopy(descriptor.get('target_constraints', [])))
            address = (descriptor['base_token'], descriptor['table_id'], descriptor['record_id'])
            if address in seen:
                existing = seen[address]
                for source, destinations in descriptor.get('mappings', {}).items():
                    prior = existing.setdefault('mappings', {}).get(source, [])
                    existing['mappings'][source] = list(dict.fromkeys(
                        (prior if isinstance(prior, list) else [prior]) +
                        (destinations if isinstance(destinations, list) else [destinations])))
                existing.setdefault('constraints', []).extend(copy.deepcopy(descriptor.get('constraints', [])))
                existing.setdefault('blocked_mappings', {}).update(descriptor.get('blocked_mappings', {}))
                continue
            related = {**descriptor, 'work_type': work, 'notice_type': group['notice_type']}
            with self._remote(related) as remote:
                schema, item = remote.fields(), remote.get(related['record_id'])
            if item is None:
                raise AssistantError('关联记录已不存在，未准备历史修改。', 409)
            self._authorize_record(session, work, item['fields'], inherited=codes)
            for condition in related.get('constraints', []):
                if item['fields'].get(condition['field']) != condition['value']:
                    raise AssistantError('原通告关联关系已变化，请重新核对。', 409)
            related.update(raw=item, schema=schema)
            seen[address] = related
            groups.append(related)
        revision = digest([{'base': item['base_token'], 'table': item['table_id'], 'id': item['record_id'],
            'schema': item['schema'],
            'fields': {field['field_id']: comparable(field, item['raw']['fields'].get(field['field_name']))
                       for field in item['schema'] if field.get('type') not in {19, 20, 1001, 1002}}} for item in groups])
        return groups, normalized, revision, warnings, codes

    def _public(self, session, work, groups, normalized, revision, warnings):
        public = []
        blocked_fields = {name: reason for group in groups for name, reason in group.get('blocked_mappings', {}).items()}
        mapping = get_field_config(NOTICE_TYPE_BY_WORK_TYPE[work])
        actual_end = mapping.get('actual_end') or mapping.get('end_time')
        for group in groups:
            values = group['raw']['fields']
            fields = []
            for field in group['schema']:
                name, kind = field['field_name'], field['type']
                editable, reason = self._editable(field, work)
                if name in blocked_fields:
                    editable, reason = False, blocked_fields[name]
                if group['key'] == 'target' and name == actual_end and not values.get(name):
                    editable, reason = False, '尚未结束，请通过原办理流程发送结束后再更正实际结束时间。'
                if group['key'] != 'target':
                    editable, reason = False, '随本条通告的对应字段同步更新。'
                value = values.get(name)
                display = notice_field_text(value)
                if kind in {5, 1001, 1002} and type(value) in (int, float):
                    display = dt.datetime.fromtimestamp(value / 1000, ZONE).strftime('%Y-%m-%d %H:%M')
                fields.append({'id': field['field_id'], 'name': name, 'type': kind,
                    'value': field_value(field, value), 'display': display, 'editable': editable, 'reason': reason,
                    'options': [{'value': item['name'], 'label': item['name']} for item in (field.get('property') or {}).get('options', []) if item.get('name')],
                    'attachments': [{'name': item.get('name') or '附件', 'url': '/api/notice-history/attachment?' + urlencode({
                        'work_type': work, 'record_id': normalized['record_id'], 'group': group['key'],
                        'field_id': field['field_id'], 'file_token': item['file_token']})}
                        for item in (value if isinstance(value, list) and kind == 17 else [])
                        if isinstance(item, dict) and item.get('file_token')]})
            public.append({'key': group['key'], 'label': group['label'], 'record_id': group['record_id'], 'fields': fields})
        pointer = self.store.get_document(NAMESPACE, 'record:' + work + ':' + normalized['record_id']) or {}
        operation = self.store.get_document(NAMESPACE, 'operation:' + pointer.get('operation_id', '')) or {}
        owner = self._user(session, work)[0]
        pending = self._result(operation) if operation.get('acting') == self._acting_identity(owner) and operation.get('status') not in {'completed', 'abandoned'} else None
        blocked = bool(operation and operation.get('status') not in {'completed', 'abandoned'} and not pending)
        if blocked:
            warnings = [*warnings, '此记录还有其它办理身份的保存任务待核验，请由原办理人继续或停止剩余同步。']
        return {'record': self._summary(work, normalized), 'revision': revision, 'groups': public,
                'editable': not blocked and (not warnings or bool(pending)), 'warnings': warnings, 'pending_operation': pending}

    def detail(self, session, *, work_type, record_id, scope='ALL'):
        return self._public(session, work_type, *self._snapshot(session, work_type, record_id)[:4])

    @staticmethod
    def _result(operation):
        if not operation:
            return None
        status = operation.get('status', 'pending')
        return {'operation_id': operation['operation_id'], 'status': status,
            'message': '通告和关联记录已保存，未重新发送通告消息。' if status == 'completed' else
                '已停止剩余同步；此前已保存的字段保留，请重新读取后核对。' if status == 'abandoned' else
                operation.get('error') or '关联保存尚未全部核验，请继续原任务。',
            'steps': [{'label': step['label'], 'status': step.get('status', 'pending'), 'error': step.get('error', '')}
                      for step in operation.get('steps', [])]}

    @staticmethod
    def _acting_identity(owner):
        from .impersonation_context import CURRENT_IMPERSONATION
        grant = CURRENT_IMPERSONATION.get() or {}
        return {'owner': owner, 'impersonation_id': grant.get('id', ''),
                'operator': grant.get('operator_open_id') or owner}

    def _validate_times(self, work, raw, patch):
        mapping = get_field_config(NOTICE_TYPE_BY_WORK_TYPE[work])
        start = mapping.get('actual_start') or mapping.get('start_time')
        end = mapping.get('actual_end') or mapping.get('end_time')
        title = mapping.get('title') or mapping.get('name')
        if title in patch and not notice_field_text(patch[title]).strip():
            raise AssistantError('通告名称不能为空。')
        fields = {**raw, **patch}
        if start in patch and not patch[start]:
            raise AssistantError('实际开始时间不能为空。')
        if end in patch and bool(raw.get(end)) != bool(patch[end]):
            raise AssistantError('历史更正不能代替通告结束或回退；只能修正已有的实际结束时间。')
        start_ms = self.service._repair_management_datetime_ms(fields.get(start))
        end_ms = self.service._repair_management_datetime_ms(fields.get(end))
        if start_ms is not None and end_ms is not None and end_ms < start_ms:
            raise AssistantError('实际结束时间不能早于实际开始时间。')

    def save(self, session, payload):
        if not isinstance(payload, dict) or set(payload) - {'scope', 'work_type', 'record_id', 'operation_id', 'expected_revision', 'changes', 'reason'}:
            raise AssistantError('历史修改参数无效。')
        work, record_id, operation_id = payload.get('work_type'), payload.get('record_id'), payload.get('operation_id')
        owner = self._user(session, work)[0]
        if not isinstance(operation_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{16,100}', operation_id):
            raise AssistantError('缺少有效的保存操作标识。')
        lock = self.locks[int(digest([work, record_id])[:8], 16) % len(self.locks)]
        with lock:
            original = self.store.get_document(NAMESPACE, 'operation:' + operation_id)
            fingerprint = digest(payload)
            acting = self._acting_identity(owner)
            if original:
                if original.get('fingerprint') != fingerprint or original.get('acting') != acting:
                    raise AssistantError('该操作标识已对应其它内容或办理身份，未重复保存。', 409)
                return self._execute(session, original)
            pointer = self.store.get_document(NAMESPACE, 'record:' + str(work) + ':' + str(record_id)) or {}
            pending = self.store.get_document(NAMESPACE, 'operation:' + pointer.get('operation_id', '')) or {}
            if pending and pending.get('status') not in {'completed', 'abandoned'}:
                raise AssistantError('该通告还有保存结果待核验，请先由原办理人继续原保存任务。', 409)
            config_identity = self._config_identity()
            groups, normalized, revision, warnings, codes = self._snapshot(session, work, record_id)
            if payload.get('expected_revision') != revision:
                raise AssistantError('通告或关联字段已变化，请保留当前填写并重新核对，未覆盖其它人的修改。', 409)
            if warnings:
                raise AssistantError('关联关系尚未核验，未修改任何记录：' + '；'.join(warnings), 409)
            changes = payload.get('changes')
            if not isinstance(changes, dict) or set(changes) != {'target'} or not isinstance(changes['target'], dict):
                raise AssistantError('请修改通告自身字段；关联记录由原关系自动同步。')
            by_id = {field['field_id']: field for field in groups[0]['schema']}
            patch = {}
            for identity, value in changes['target'].items():
                field = by_id.get(identity)
                if not field or not self._editable(field, work)[0]:
                    raise AssistantError('提交包含不存在或只读的字段。')
                value = normalize_edit(field, value)
                if comparable(field, value) != comparable(field, groups[0]['raw']['fields'].get(field['field_name'])):
                    patch[field['field_name']] = value
            if not patch:
                return {'operation_id': operation_id, 'status': 'completed', 'message': '没有需要保存的字段变化。', 'steps': []}
            self._validate_times(work, groups[0]['raw']['fields'], patch)
            steps = []
            for group in groups:
                schema = {field['field_name']: field for field in group['schema']}
                for source, explanation in group.get('blocked_mappings', {}).items():
                    if source in patch:
                        raise AssistantError(f'“{source}”的关联字段尚不可同步：{explanation}', 409)
                desired = patch if group['key'] == 'target' else {
                    destination: patch[source] for source, destinations in group.get('mappings', {}).items() if source in patch
                    for destination in (destinations if isinstance(destinations, list) else [destinations])}
                normalized_fields = {}
                for name, value in desired.items():
                    field = schema.get(name)
                    if not field or field.get('is_computed') or field.get('type') not in WRITABLE:
                        raise AssistantError(f'关联字段“{name}”不可直接更新，未修改任何记录。', 409)
                    normalized_fields[name] = normalize_edit(field, value)
                steps.append({key: copy.deepcopy(group[key]) for key in ('key', 'label', 'base_token', 'table_id', 'record_id')}
                    | {'fields': normalized_fields, 'before': {name: group['raw']['fields'].get(name) for name in normalized_fields},
                       'schema': {name: schema[name] for name in normalized_fields}, 'constraints': group.get('constraints', []),
                       'snapshot_key': group.get('snapshot_key', ''), 'snapshot_kind': group.get('snapshot_kind', 'repair'),
                       'status': 'pending'})
            if self._config_identity() != config_identity:
                raise AssistantError('飞书应用或目标表配置已变化，请重新读取后保存。', 409)
            operation = {'operation_id': operation_id, 'owner': owner, 'acting': acting, 'fingerprint': fingerprint,
                'work_type': work, 'record_id': record_id, 'scope': payload.get('scope') or 'ALL',
                'scopes': sorted(codes), 'configuration': config_identity, 'steps': steps, 'status': 'pending',
                'identity_revision': self._identity_revision(work, record_id),
                'source_bindings': [{'work_type': work, 'source_record_id': group['record_id'], 'target_record_id': record_id}
                    for group in groups if group['key'] == 'source_plan'],
                'created_at': time.time(), 'reason': str(payload.get('reason') or '')[:500]}
            self.store.put_documents(NAMESPACE, {'operation:' + operation_id: operation,
                'record:' + work + ':' + record_id: {'operation_id': operation_id}})
            return self._execute(session, operation)

    def retry(self, session, operation_id):
        if not isinstance(operation_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{16,100}', operation_id):
            raise AssistantError('保存操作标识无效。')
        operation = self.store.get_document(NAMESPACE, 'operation:' + operation_id)
        if not operation:
            raise AssistantError('未找到原保存任务。', 404)
        owner = self._user(session, operation['work_type'])[0]
        if operation.get('acting') != self._acting_identity(owner):
            raise AssistantError('该保存任务属于另一办理身份。', 403)
        lock = self.locks[int(digest([operation['work_type'], operation['record_id']])[:8], 16) % len(self.locks)]
        with lock:
            return self._execute(session, self.store.get_document(NAMESPACE, 'operation:' + operation_id))

    def _check_operation(self, session, operation):
        for key, owner in getattr(self.local, 'leases', []):
            if not self.store.renew_notice_operation_lock(key, owner):
                raise AssistantError('原通告办理锁已失效，请继续原保存任务重新核验。', 409)
        owner = self._user(session, operation['work_type'])[0]
        if operation.get('acting') != self._acting_identity(owner):
            raise AssistantError('保存任务办理身份已变化。', 403)
        if self._config_identity() != operation['configuration']:
            raise AssistantError('飞书应用或数据表配置已变化，已暂停原保存任务。', 409)
        if self._identity_revision(operation['work_type'], operation['record_id']) != operation['identity_revision']:
            raise AssistantError('通告办理身份或关联已变化，已暂停原保存任务。', 409)
        for binding in operation.get('source_bindings', []):
            current = self.store.resolve_notice_identity(work_type=binding['work_type'], source_record_id=binding['source_record_id']) or {}
            if current.get('target_record_id') != binding['target_record_id']:
                raise AssistantError('源计划已绑定其它目标，未覆盖新周期的数据。', 409)

    def _check_record(self, session, operation, step, record):
        self._check_operation(session, operation)
        if not record or record.get('record_id') != step['record_id']:
            raise AssistantError('待保存的原记录已被删除，未新增替代记录。', 409)
        self._authorize_record(session, operation['work_type'], record['fields'], inherited=operation['scopes'])
        for condition in step.get('constraints', []):
            name = condition['field']
            actual = record['fields'].get(name)
            if actual != condition['value'] and (condition.get('allow_desired') is False or name not in step['fields'] or
                comparable(step['schema'][name], actual) != comparable(step['schema'][name], step['fields'][name])):
                raise AssistantError('关联关系已变化，已暂停原保存任务。', 409)

    def _check_step(self, session, operation, step, remote):
        record = remote.get(step['record_id'])
        self._check_record(session, operation, step, record)
        current_schema = {field['field_name']: field for field in remote.fields()}
        for name, field in step['schema'].items():
            if digest(current_schema.get(name)) != digest(field):
                raise AssistantError('字段结构或选项已变化，请核对原保存任务。', 409)
            value = comparable(field, record['fields'].get(name))
            if value not in (comparable(field, step['before'].get(name)), comparable(field, step['fields'][name])):
                raise AssistantError(f'“{name}”已被其它操作修改，未覆盖新内容。', 409)
        return record

    def _execute(self, session, operation):
        if operation['status'] in {'completed', 'abandoned'}:
            return self._result(operation)
        audit = begin_business_audit(self.store, domain='notice_history', action='correct_fields',
            operation_id=operation['operation_id'], scope=operation['scope'], actor_open_id=operation['acting']['operator'],
            actor_name=str((session.get('user') or {}).get('name') or ''), target_record_id=operation['record_id'],
            metadata={'work_type': operation['work_type']})
        leases = self.local.leases = []
        try:
            keys = {operation['work_type'] + ':target:' + operation['record_id']}
            keys.update('maintenance:target:' + step['record_id'] for step in operation['steps'] if step['key'] == 'paired_maintenance')
            for key in sorted(keys):
                acquired, owner = self.store.acquire_notice_operation_lock(key, owner=uuid.uuid4().hex,
                    action='历史字段更正', ttl_seconds=180)
                if not acquired:
                    raise AssistantError('该通告正在执行其它办理操作，请稍后继续本次保存。', 503)
                leases.append((key, owner))
            self._check_operation(session, operation)
            # Check every recorded destination before the first write. A retry
            # never rediscovers newer bindings or creates replacement records.
            for step in operation['steps']:
                with self._remote(step) as remote:
                    self._check_step(session, operation, step, remote)
            for step in operation['steps']:
                # Recheck the upstream edges immediately before a dependent
                # write, without repeatedly reading all sibling follow-ups.
                for parent in operation['steps']:
                    if parent['key'] in {'target', 'repair_project'} and parent is not step:
                        with self._remote(parent) as parent_remote:
                            self._check_record(session, operation, parent, parent_remote.get(parent['record_id']))
                with self._remote(step) as remote:
                    current = self._check_step(session, operation, step, remote)
                    needed = {name: value for name, value in step['fields'].items()
                              if comparable(step['schema'][name], current['fields'].get(name)) != comparable(step['schema'][name], value)}
                    if step['status'] == 'completed' and needed:
                        raise AssistantError('已核验的记录后来又发生变化，未重做已完成修改。', 409)
                    if needed:
                        step['status'] = 'writing'
                        self.store.put_document(NAMESPACE, 'operation:' + operation['operation_id'], operation)
                        remote.update(step['record_id'], needed)
                    checked = remote.get(step['record_id'])
                    self._check_record(session, operation, step, checked)
                    if not checked or any(comparable(step['schema'][name], checked['fields'].get(name)) != comparable(step['schema'][name], value)
                                          for name, value in step['fields'].items()):
                        raise AssistantError('字段写入尚未准确读回，已保留原保存任务等待核验。', 503)
                    step['status'], step['error'] = 'completed', ''
                    self.store.put_document(NAMESPACE, 'operation:' + operation['operation_id'], operation)
                    self._refresh_local(operation, step, checked['fields'])
            for step in operation['steps']:
                with self._remote(step) as remote:
                    checked = self._check_step(session, operation, step, remote)
                    if any(comparable(step['schema'][name], checked['fields'].get(name)) != comparable(step['schema'][name], value)
                           for name, value in step['fields'].items()):
                        raise AssistantError('保存期间字段又被其它操作修改，请核对原保存任务。', 409)
            operation['status'], operation['error'] = 'completed', ''
            baseline_key = 'baseline:' + operation['work_type'] + ':' + operation['record_id']
            baseline = self.store.get_document(NAMESPACE, baseline_key) or {}
            if baseline.get('configuration') != operation['configuration']:
                baseline = {'configuration': operation['configuration'], 'targets': {}}
            for step in operation['steps']:
                address = '/'.join(step[key] for key in ('base_token', 'table_id', 'record_id'))
                previous = baseline.setdefault('targets', {}).setdefault(address, {'fields': {}, 'before': {}})
                previous['fields'].update(copy.deepcopy(step['fields']))
                for name, value in step['before'].items():
                    previous['before'].setdefault(name, copy.deepcopy(value))
            self.store.put_documents(NAMESPACE, {'operation:' + operation['operation_id']: operation, baseline_key: baseline})
            finish_business_audit(self.store, audit, success=True, result={'message': '仅更正历史字段及关联表'},
                                  remote_written=True, message_sent=False)
        except Exception as exc:
            operation['status'] = 'conflict' if getattr(exc, 'status', None) in {403, 409} else 'pending'
            operation['error'] = str(exc)[:1200]
            self.store.put_document(NAMESPACE, 'operation:' + operation['operation_id'], operation)
            finish_business_audit(self.store, audit, success=False, error=operation['error'],
                error_stage='field_sync_pending', remote_written=any(step['status'] in {'writing', 'completed'} for step in operation['steps']), message_sent=False)
        finally:
            self.local.leases = []
            for key, owner in reversed(leases):
                self.store.release_notice_operation_lock(key, owner)
        return self._result(operation)

    def abandon(self, session, operation_id):
        operation = self.store.get_document(NAMESPACE, 'operation:' + str(operation_id))
        if not operation:
            raise AssistantError('未找到原保存任务。', 404)
        owner = self._user(session, operation['work_type'])[0]
        if operation.get('acting') != self._acting_identity(owner):
            raise AssistantError('只有原办理身份可以停止剩余同步。', 403)
        lock = self.locks[int(digest([operation['work_type'], operation['record_id']])[:8], 16) % len(self.locks)]
        with lock:
            operation = self.store.get_document(NAMESPACE, 'operation:' + operation_id)
            if operation['status'] not in {'completed', 'abandoned'}:
                operation['status'], operation['stopped_at'] = 'abandoned', time.time()
                self.store.put_document(NAMESPACE, 'operation:' + operation_id, operation)
                audit = begin_business_audit(self.store, domain='notice_history', action='stop_remaining_sync',
                    operation_id=operation_id, actor_open_id=operation['acting']['operator'], target_record_id=operation['record_id'])
                finish_business_audit(self.store, audit, success=True, result={'message': '停止未完成同步，已写入字段不回退'}, message_sent=False)
            return self._result(operation)

    def _refresh_local(self, operation, step, fields):
        work = operation['work_type']
        source_key = step.get('snapshot_key') or (self.service._notice_target_snapshot_source_key(work) if step['key'] == 'target' else '')
        if source_key and step.get('snapshot_kind') == 'source':
            self.store.patch_active_source_record_fields(source_record_id=step['record_id'], work_type=source_key, fields=fields)
            with self.service._refresh_lock:
                records = getattr(self.service, '_change_records' if source_key == 'change' else '_records', [])
                for row in records:
                    if row.get('record_id') == step['record_id']:
                        row.setdefault('raw_fields', {}).update(copy.deepcopy(fields))
                        row.setdefault('display_fields', {}).update({key: notice_field_text(value) for key, value in fields.items()})
        elif source_key:
            self.service._upsert_repair_snapshot_fields(source_key=source_key, record_id=step['record_id'], fields=fields)
        self.service.clear_target_record_cache()
        self.service.clear_engineer_mop_cache()
        self.service._touch_state_cache_version()

    def attachment(self, session, *, work_type, record_id, group, field_id, file_token):
        groups, _, _, _, _ = self._snapshot(session, work_type, record_id)
        selected = next((item for item in groups if item['key'] == group), None)
        field = next((item for item in (selected or {}).get('schema', []) if item['field_id'] == field_id and item['type'] == 17), None)
        attachment = next((item for item in (selected['raw']['fields'].get(field['field_name']) or [])
            if isinstance(item, dict) and item.get('file_token') == file_token), None) if field else None
        if not attachment:
            raise AssistantError('附件不属于当前通告或关联记录。', 403)
        with self._remote(selected) as remote:
            content = remote.download(file_token, record_id=selected['record_id'], field_id=field_id)
        fresh, _, _, _, _ = self._snapshot(session, work_type, record_id)
        matched = next((item for item in fresh if item['key'] == group and
            (item['base_token'], item['table_id'], item['record_id']) ==
            (selected['base_token'], selected['table_id'], selected['record_id'])), None)
        if not matched or not any(isinstance(item, dict) and item.get('file_token') == file_token
            for item in (matched['raw']['fields'].get(field['field_name']) or [])):
            raise AssistantError('附件或关联关系已变化，请重新读取后下载。', 409)
        return content, str(attachment.get('name') or '通告附件')
