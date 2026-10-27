"""Bounded roster reads and idempotent checklist uploads using the shared HTTP pool."""
import datetime as dt
import asyncio
import json
import io
import re
import uuid
from collections import defaultdict
from pathlib import Path

from .pre_shift_checks import CheckError, CheckDeliveryUncertain, SCOPES, TZ

APP_TOKEN = 'MliKbC3fXa8PXrsndKscmxjdn1g'
TABLE_ID = 'tbloxEhpBIIW3Joq'
SCHEMA = {'文本': 1, '检查标识': 1, '楼栋': 1, '检查日期': 5, '班次': 1, '提交人员': 1,
          '提交人员OpenID': 1, '提交时间': 5, '检查项数': 2, '无异常项数': 2,
          '异常项数': 2, '检查明细': 1, '异常备注': 1, '清单版本': 1,
          '检查附件': 17, '交接班照片': 17, '检查清单': 1, '检查状态': 1,
          '检查结果': 1, '正常检查项': 1, '异常检查项': 1,
          '排班人员': 1, '计划检查时间': 5, '检查截止时间': 5}


def read_roster(service, day, *, scopes=SCOPES):
    from .learning_reminders import resolve_roster
    if not scopes:
        return {'people': [], 'warnings': {}}
    from .personnel_source import daily_roster
    snapshot = daily_roster(service, day)
    people = [p for p in snapshot['people'] if p['scope'] in scopes and p.get('duty_kind') != '长白']
    issues = snapshot.get('issues', [])
    directory = service.get_permission_directory_people()
    if not isinstance(directory, dict) or not isinstance(directory.get('items'), list):
        raise CheckError('人员目录尚未就绪，请稍后重试。')
    directory = {row['record_id']: {'name': row.get('name') or '', 'scopes': row.get('scopes') or [],
        'selectable': row.get('selectable'), 'inactive': row.get('departed', False),
        'login_ids': [row['open_id']] if row.get('open_id') else []} for row in directory['items']}
    result, warnings = [], {}
    for scope in scopes:
        group = [person for person in people if person['scope'] == scope]
        resolved, missing = resolve_roster(group, directory, allowed_scopes=SCOPES)
        result.extend(resolved)
        unresolved = [item for item in issues if item.get('scope') == scope
                      and item.get('duty_kind') != '长白' and item.get('group') != '长白']
        missing.extend((str(item.get('name') or '未命名人员') + '：' + str(item.get('reason') or '排班资料待核对'))
                       for item in unresolved)
        if missing:
            reasons = list(dict.fromkeys(missing))
            detail = '；'.join(reasons[:3]) + (f'；另有{len(reasons)-3}项待核对' if len(reasons)>3 else '')
            warnings[scope] = f'已匹配{len(resolved)}/{len(group)+len(unresolved)}人：' + detail + '；已匹配人员正常推送，其余5分钟后重试。'
    return {'people': result, 'warnings': warnings}


def fixed_recipient_options(runtime, scope):
    from .pre_shift_checks import scopes_for
    from .portal_auth import BUILDING_OPEN_ID_MAP
    auth = runtime.auth_manager
    provider = getattr(auth, '_account_directory_provider', None)
    snapshot = provider() if provider else {}
    if not isinstance(snapshot, dict) or not snapshot.get('loaded_at') or not isinstance(snapshot.get('people'), dict):
        raise CheckError('人员目录暂未就绪，请稍后重试。', 503)
    options = {}
    for person in snapshot.get('people', {}).values():
        ids = person.get('login_ids') or []
        if person.get('inactive') or not person.get('selectable') or len(ids) != 1:
            continue
        oid = ids[0]
        if not re.fullmatch(r'ou_[A-Za-z0-9]+', str(oid)) or auth._open_id_explicitly_disabled(oid):
            continue
        profile = auth.account_profile(oid)
        if profile.get('modules') and scope in scopes_for(profile):
            options[oid] = {'open_id': oid, 'name': person['name'], 'employee_no': person.get('employee_no', '')}
    for building, oid in BUILDING_OPEN_ID_MAP.items():
        profile = auth.account_profile(oid)
        if profile.get('modules') and scope in scopes_for(profile) and not auth._open_id_explicitly_disabled(oid):
            options[oid] = {'open_id': oid, 'name': ('110站' if building == '110' else building + '楼') + '值班账号', 'employee_no': ''}
    return sorted(options.values(), key=lambda p: (p['name'], p['open_id']))


class CheckSender:
    """Use the running agent's transport and its verified app-scoped identity map."""
    def __init__(self, runtime):
        self.runtime, self.loop = runtime, None

    @staticmethod
    def _diagnostic(app_id, person, recipient, receive_id, response=None, value=None):
        def masked(identifier):
            if isinstance(identifier, str) and re.fullmatch(r'(?:ou|on)_[A-Za-z0-9]+', identifier):
                return identifier[:3] + '***' + (identifier[-4:] if len(identifier) > 7 else '')
            return '***'

        app_id = app_id if isinstance(app_id, str) and re.fullmatch(r'cli_[A-Za-z0-9]{1,64}', app_id) else '未知'
        name = person.get('name') or '接收账号'
        name = name if isinstance(name, str) and re.fullmatch(r'[\w\u4e00-\u9fff .·-]{1,40}', name) and not name.startswith(('ou_', 'on_')) else '接收账号'
        parts = [f'应用ID={app_id}', f'接收人={name}', f'本账号={masked(recipient)}', f'投递账号={masked(receive_id)}']
        if response is not None:
            parts.append(f'HTTP {response.status_code}')
            value = value if isinstance(value, dict) else {}
            code = value.get('code')
            valid_code = type(code) is int and -999999999 <= code <= 999999999
            valid_code = valid_code or isinstance(code, str) and re.fullmatch(r'[0-9]{1,9}', code)
            parts.append('code=' + (str(code) if valid_code else '未知'))
            headers = getattr(response, 'headers', {})
            error = value.get('error')
            log_ids = [v for k, v in headers.items() if k.lower() in {'x-tt-logid', 'x-request-id'}]
            if isinstance(error, dict):
                log_ids.append(error.get('log_id'))
            log_id = next((v for v in log_ids if isinstance(v, str) and re.fullmatch(r'[A-Za-z0-9_-]{1,128}', v)), '')
            if log_id:
                parts.append(f'request-logid={log_id}')
        return '（' + '；'.join(parts) + '）'

    def __call__(self, recipient, card, identity):
        if self.loop is None or self.loop.is_closed():
            return False, '飞书智能体尚未就绪，提醒等待重试。', []
        future = asyncio.run_coroutine_threadsafe(self.send(recipient, card, identity), self.loop)
        try:
            return future.result(timeout=45)
        except CheckError:
            raise
        except Exception:
            future.cancel()
            raise CheckDeliveryUncertain('飞书智能体提醒暂未确认送达，等待原任务重试。') from None

    def photo(self, recipient, photo, identity, checkpoint):
        if self.loop is None or self.loop.is_closed():
            raise CheckError('照片已保存，飞书智能体尚未就绪，后台稍后重试。')
        future = asyncio.run_coroutine_threadsafe(self.send(recipient, None, identity, photo=photo, checkpoint=checkpoint), self.loop)
        try:
            return future.result(timeout=45)
        except CheckError:
            raise
        except Exception:
            future.cancel()
            raise CheckError('照片已保存，飞书图片发送尚未确认，后台将核验原发送。') from None

    async def send(self, recipient, card, identity, *, photo=None, checkpoint=None):
        from .portal_service import external_real_write_guard
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        guard = external_real_write_guard()
        if guard.get('mock_external'):
            return 'om_mockexternal' if photo else (False, '隔离模式未发送真实消息。', [])
        if not guard.get('real_write_allowed'):
            raise CheckError('当前未允许真实飞书消息发送。')
        bot = getattr(self.runtime, 'feishu_assistant_channel', None)
        if not bot or not getattr(bot, 'messenger', None) or not getattr(bot, 'config', {}).get('app_id'):
            raise CheckError('飞书智能体尚未就绪，班前检查提醒等待重试。')
        try:
            person = await asyncio.to_thread(self.runtime.auth_manager.directory_person, recipient) or {}
        except Exception:
            raise CheckError('接收人目录暂未就绪，班前检查提醒等待重试。') from None
        from .portal_auth import BUILDING_OPEN_ID_MAP, REQUIRED_ADMIN_USERS
        fixed_account = recipient in BUILDING_OPEN_ID_MAP.values() or recipient in REQUIRED_ADMIN_USERS
        if photo is not None or not person and fixed_account:
            try:
                profile = await asyncio.to_thread(self.runtime.auth_manager.account_profile, recipient)
            except Exception:
                raise CheckError('接收人权限暂未就绪，等待重试。') from None
            if photo is not None and not profile.get('modules'):
                raise CheckError('照片已保存，上传账号权限已变化，暂未发送。')
            if not person and fixed_account and profile.get('modules'):
                person = {'id': recipient, 'selectable': True}
        if not person.get('id') or not person.get('selectable') or person.get('inactive'):
            raise CheckError('班前检查接收人身份已变化，请核对人员目录。')
        receive_id, directory_error = None, None
        try:
            if re.fullmatch(r'rec[A-Za-z0-9]+', person['id']):
                try:
                    receive_id = await bot.messenger.recipient_open_id(person['id'])
                except AssistantError as exc:
                    if exc.category != 'feishu_sender_directory_unavailable':
                        raise
                    directory_error = exc
            if receive_id:
                receive_type = 'open_id'
            else:
                # Keep verified legacy bindings and fixed duty accounts usable.
                receive_type, receive_id = await asyncio.to_thread(bot.handover_cards.recipient_id,
                    {'id': person['id'], 'open_id': recipient})
        except AssistantError as exc:
            raise CheckError(str(directory_error or exc)) from None
        except Exception:
            # Nothing was sent yet; do not exhaust the uncertain-delivery dedup window.
            raise CheckError(str(directory_error) if directory_error else
                '未能取得接收人的飞书智能体身份，请核对人员表的员工姓名关联；提醒等待重试。') from None
        try:
            client = await bot.messenger.client()
        except Exception:
            raise CheckError('飞书智能体消息连接暂未就绪，提醒等待重试。') from None
        content, kind = card, 'interactive'
        if photo is not None:
            if photo.get('sender_app_id') and photo['sender_app_id'] != bot.config['app_id']:
                raise CheckError('照片原发送应用已变化，请管理员核对原消息；未改用其他应用重复发送。')
            key = photo.get('image_key') or ''
            if not key:
                path = Path(photo['path'])
                from .pre_shift_photo import prepare_delivery_image
                try:
                    image = await asyncio.to_thread(prepare_delivery_image, path, photo['name'])
                except (ValueError, OSError) as exc:
                    raise CheckError('照片已保存，发送副本生成失败：' + str(exc)) from None
                with io.BytesIO(image['content']) if image['content'] is not None else path.open('rb') as fp:
                    uploaded = await client.post('https://open.feishu.cn/open-apis/im/v1/images',
                        headers=await bot.messenger.headers(), data={'image_type': 'message'},
                        files={'image': (image['name'], fp, image['mime'])}, timeout=30)
                data = uploaded.json()
                key = (data.get('data') or {}).get('image_key')
                if uploaded.status_code >= 400 or data.get('code') != 0 or not isinstance(key, str) or not re.fullmatch(r'img_[A-Za-z0-9_-]{1,500}', key):
                    raise CheckError('照片已保存，飞书智能体图片上传未确认，请核对应用图片权限，后台稍后重试。')
            if not isinstance(key, str) or not re.fullmatch(r'img_[A-Za-z0-9_-]{1,500}', key):
                raise CheckError('照片发送进度中的图片标识无效，请管理员核对。')
            await asyncio.to_thread(checkpoint, key, bot.config['app_id'])
            content, kind = {'image_key': key}, 'image'
        try:
            headers = await bot.messenger.headers()
        except Exception:
            raise CheckError('飞书智能体消息凭据暂未就绪，提醒等待重试。') from None
        try:
            response = await client.post('https://open.feishu.cn/open-apis/im/v1/messages',
                params={'receive_id_type': receive_type}, headers=headers,
                json={'receive_id': receive_id, 'msg_type': kind, 'content': json.dumps(content, ensure_ascii=False),
                      'uuid': str(uuid.uuid5(uuid.NAMESPACE_URL, identity))})
        except Exception:
            raise CheckDeliveryUncertain('飞书发送回执未确认，等待原任务重试。' +
                self._diagnostic(bot.config['app_id'], person, recipient, receive_id)) from None
        try:
            value = response.json()
        except Exception:
            value = None
        diagnostic = self._diagnostic(bot.config['app_id'], person, recipient, receive_id, response, value)
        if response.status_code >= 500:
            raise CheckDeliveryUncertain('飞书服务暂未确认发送结果，等待原任务重试。' + diagnostic)
        code = value.get('code') if isinstance(value, dict) else None
        if isinstance(code, str) and re.fullmatch(r'[0-9]{1,9}', code):
            code = int(code)
        if response.status_code >= 400 or (type(code) is int and code != 0):
            if code == 230013:
                raise CheckError('飞书拒绝提醒：接收人不在当前应用机器人的可用范围内（或被禁用、已离职）。'
                    '请管理员核对该应用对该接收人的可用范围，使授权生效后将自动重试。' + diagnostic)
            raise CheckError('飞书拒绝提醒，请管理员根据错误码及日志号核对；提醒保留并稍后重试。' + diagnostic)
        if type(code) is not int or code != 0:
            raise CheckDeliveryUncertain('飞书发送回执格式未确认，等待原任务重试。' + diagnostic)
        data = value.get('data')
        mid = data.get('message_id') if isinstance(data, dict) else None
        if not isinstance(mid, str) or not re.fullmatch(r'om_[A-Za-z0-9]+', mid):
            raise CheckDeliveryUncertain('飞书智能体未返回有效发送回执，提醒等待原任务重试。' + diagnostic)
        if photo is not None:
            return mid
        return True, 'ok', [{'open_id': recipient, 'sender_app_id': bot.config['app_id'], 'message_id': mid}]


class CheckCloud:
    def __init__(self, service):
        self.service = service
        self.schema_ready = False
        self.on_file_token = None  # callable(file_id, token) to persist locally

    def request(self, method, path, body=None, params=None):
        from .portal_service import TOKEN_ERROR_CODES, external_real_write_guard, refresh_feishu_token
        from upload_event_module.services.feishu_token_manager import token_manager
        read = method == 'GET' or path == 'records/search'
        if not read:
            guard = external_real_write_guard()
            if not guard.get('real_write_allowed'):
                raise CheckError(guard.get('reason') or '未允许飞书写入。')
        url = f'https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{TABLE_ID}/{path}'
        try:
            credentials = token_manager._credentials()
        except Exception:
            raise CheckError('Qt 飞书鉴权暂不可用，未发送班前检查请求。') from None

        def check_credentials():
            try:
                matching = token_manager._credentials() == credentials
            except Exception:
                matching = False
            if not matching:
                raise CheckError('Qt 飞书鉴权已变化，未跨应用重试班前检查请求。') from None

        for attempt in range(2):
            check_credentials()
            headers = self.service._auth_headers()
            check_credentials()
            payload = self.service._http_client.request_json(method, url, headers=headers,
                params=params or {}, json_payload=body, retries=1 if read else 0)
            if not isinstance(payload, dict) or type(payload.get('code')) is not int:
                raise CheckError('飞书返回不完整，已保留本地结果。')
            if payload['code'] in TOKEN_ERROR_CODES and not attempt:
                check_credentials()
                refresh_feishu_token()
                continue
            if payload['code'] != 0 or not isinstance(payload.get('data'), dict):
                raise CheckError(f"班前检查飞书请求未成功：code={payload['code']}")
            return payload['data']
        raise CheckError('飞书授权暂不可用。')

    def _guard_real_write(self):
        try:
            from .portal_service import external_real_write_guard
            guard = external_real_write_guard()
            if not guard.get('real_write_allowed'):
                raise CheckError(guard.get('reason') or '未允许飞书写入。')
        except ImportError:
            raise CheckError('未允许飞书写入。') from None

    def all(self, path, body=None):
        items, cursor, seen = [], '', set()
        for _ in range(100):
            data = self.request('GET' if body is None else 'POST', path, body,
                                {'page_size': 100, **({'page_token': cursor} if cursor else {})})
            if not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
                raise CheckError('飞书分页不完整，未再次新增。')
            items.extend(data['items'])
            if not data['has_more']:
                return items
            cursor = data.get('page_token')
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                break
            seen.add(cursor)
        raise CheckError('飞书分页未完成。')

    def ensure_schema(self):
        fields = {row['field_name']: row['type'] for row in self.all('fields')}
        for name, kind in SCHEMA.items():
            if name in fields and fields[name] != kind:
                raise CheckError(f'班前检查表字段类型不匹配：{name}')
            if name not in fields:
                self.request('POST', 'fields', {'field_name': name, 'type': kind})
        self.schema_ready = True

    def _upload_files(self, tasks):
        """Upload locally-staged attachments to bitable, reuse persisted tokens."""
        tokens, seen, todo = {}, set(), []
        for task in tasks:
            for ref in task.get('_files', []):
                file_id = ref['file_id']
                if file_id in seen:
                    continue
                seen.add(file_id)
                if ref.get('token'):
                    tokens[file_id] = ref['token']
                else:
                    todo.append(ref)
        if not todo:
            return tokens
        self._guard_real_write()
        for ref in todo:
            token = self.service._upload_bitable_file(
                file_path=ref['path'], file_name=ref['name'], app_token=APP_TOKEN)
            if not isinstance(token, str) or not token:
                raise CheckError('附件上传未返回有效file_token。')
            if self.on_file_token:
                self.on_file_token(ref['file_id'], token)
            tokens[ref['file_id']] = token
        return tokens

    def _file_ref(self, token, name, is_image):
        # Bitable attachment write contract accepts only the file_token; name/type
        # are read-only metadata (MIME types are not represented here). File names
        # still appear in the 检查明细 text built by the caller.
        return {'file_token': token}

    def upsert(self, key, tasks, record_id=''):
        if not self.schema_ready:
            self.ensure_schema()
        first = tasks[0]
        tokens = self._upload_files(tasks)
        checksum_files, handover_files, seen_handover = [], [], set()
        lines, normal, abnormal, count = [], [], [], 0
        for task in tasks:
            files_by_item = {}
            for ref in task.get('_files', []):
                token = tokens.get(ref['file_id'])
                if token is None:
                    continue
                if ref['kind'] == 'handover':
                    shift_key = ref.get('shift') or ''
                    if shift_key not in seen_handover:
                        seen_handover.add(shift_key)
                        handover_files.append(self._file_ref(token, ref['name'], ref['is_image']))
                else:
                    checksum_files.append(self._file_ref(token, ref['name'], ref['is_image']))
                    files_by_item.setdefault(ref['item_id'], []).append(ref['name'])
            lines.append(task['shift'] + '班 · ' + task['submitted'])
            for index, answer in enumerate(json.loads(task['answers']), 1):
                count += 1
                label = '异常' if answer['status'] == 'abnormal' else '正常（无异常）'
                line = f"{index}. {answer['text']}：{label}" + (f"；{answer['note']}" if answer['note'] else '')
                attached = files_by_item.get(answer['id'])
                if attached:
                    line += '（附件：' + '、'.join(attached) + '）'
                lines.append(line)
                if answer['status'] == 'abnormal':
                    abnormal.append(task['shift'] + '班 · ' + line)
                else:
                    normal.append(task['shift'] + '班 · ' + line)
        fields = {'文本': f"{first['day']} {first['scope']}楼 {first['actor_name']} 班前检查", '检查标识': key, '楼栋': first['scope'],
                  '检查日期': int(dt.datetime.combine(dt.date.fromisoformat(first['day']), dt.time(), TZ).timestamp() * 1000),
                  '班次': '、'.join(dict.fromkeys(t['shift'] for t in tasks)), '提交人员': first['actor_name'], '提交人员OpenID': first['actor_id'],
                  '提交时间': max(int(dt.datetime.fromisoformat(t['submitted']).timestamp() * 1000) for t in tasks),
                  '检查项数': count, '无异常项数': count - len(abnormal), '异常项数': len(abnormal),
                  '检查明细': '\n'.join(lines), '异常备注': '\n'.join(abnormal),
                  '检查结果': '存在异常' if abnormal else '全部正常',
                  '正常检查项': '\n'.join(normal), '异常检查项': '\n'.join(abnormal),
                  '清单版本': '、'.join(f"{t['shift']}班 v{t['definition_version']}" for t in tasks),
                  '检查清单': '\n'.join(lines),
                  '检查状态': '已提交',
                  '排班人员': '、'.join(dict.fromkeys(p['name'] for t in tasks for p in json.loads(t.get('people') or '[]') if p.get('name'))),
                  **({'计划检查时间': int(min(t['due'] for t in tasks) * 1000)} if all('due' in t for t in tasks) else {}),
                  **({'检查截止时间': int(max(t['deadline'] for t in tasks) * 1000)} if all('deadline' in t for t in tasks) else {}),
                  '检查附件': checksum_files, '交接班照片': handover_files}
        if not record_id:
            records = self.all('records/search', {'filter': {'conjunction': 'and', 'conditions': [
                {'field_name': '检查标识', 'operator': 'is', 'value': [key]}]}})
            if len(records) > 1:
                raise CheckError('发现重复检查记录，需要管理员核对；未覆盖。')
            if records:
                record_id = records[0].get('record_id', '')
                if not record_id:
                    raise CheckError('飞书返回缺少记录ID。')
        if record_id:
            self.request('PUT', 'records/' + record_id, {'fields': fields})
            return record_id
        from upload_event_module.services.feishu_service import _stable_uuid4_client_token
        data = self.request('POST', 'records', {'fields': fields}, {'client_token': _stable_uuid4_client_token(key)})
        record_id = data.get('record', {}).get('record_id')
        if not isinstance(record_id, str) or not re.fullmatch(r'rec[A-Za-z0-9]+', record_id):
            raise CheckError('飞书写入结果尚未确认，稍后按检查标识核验。')
        return record_id
