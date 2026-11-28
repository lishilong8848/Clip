"""Edit one EOP scenario in the designated Bitable view."""

import asyncio
import base64
import hashlib
import hmac
import io
import json
import logging
import os
import re
import secrets
import threading
import time
import uuid
from collections import Counter
from functools import lru_cache
from pathlib import Path
from urllib.parse import parse_qs, quote, urlsplit

from fastapi import Request
from fastapi.responses import JSONResponse, Response

from .module_access import allowed_modules
from .risk_inspection import RiskError, digest, text
from .risk_inspection_cloud import API, RiskCloud

BASE = 'MliKbC3fXa8PXrsndKscmxjdn1g'
TABLE = 'tblKXuy3V1CWK4Q8'
VIEW = 'vewOjTVH4H'
MAIN = ('场景名称', '专业', '系统/设备', '原优先级')
LONG = ('处置正文（原文）', '原通告模板', '原演练场景', '原EOP精炼（参考）')
SCENARIO_CODE = '场景编号'
DETAIL_FIELDS = (*MAIN, SCENARIO_CODE, *LONG)
STEPS, IMAGES, LINK = '原通告重点项', '图片', '步骤图片关联'
SELECTS = MAIN[1:]
MAX_UPLOAD = 18 * 1024 * 1024
MAX_DOWNLOAD = 36 * 1024 * 1024
SCHEMA_TTL = 120
PREVIEW_WINDOW = 900
MIME = {'PNG': 'image/png', 'JPEG': 'image/jpeg', 'GIF': 'image/gif', 'WEBP': 'image/webp'}
NUMBER = re.compile(r'^\s*(?:第\s*\d+\s*步[：:]?|\d+[、.．）)])\s*')


def plain(value):
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return ''.join(map(plain, value))
    if isinstance(value, dict):
        return plain(value.get('text', value.get('value')))
    return ''


def split_steps(value):
    lines = value.replace('\r\n', '\n').replace('\r', '\n').split('\n')
    if not any(NUMBER.match(line) for line in lines):
        return [line.strip() for line in lines if line.strip()]
    steps = []
    for line in lines:
        match = NUMBER.match(line)
        if match or not steps:
            if line.strip():
                steps.append(line[match.end():] if match else line.strip())
        else:
            steps[-1] += '\n' + line
    return [step.strip() for step in steps if step.strip()]


def step_metadata(fields, raw):
    try:
        saved = json.loads(plain(fields.get(LINK)) or '{}')
        if isinstance(saved, dict) and saved.get('text_hash') == digest(raw):
            return saved
    except ValueError:
        pass
    return {}


def record_steps(fields, raw):
    # Explicit boundaries preserve numbered substeps without changing the readable Bitable text.
    counts = step_metadata(fields, raw).get('step_lines')
    if isinstance(counts, list) and counts and all(type(count) is int and count > 0 for count in counts):
        lines = raw.split('\n')
        if sum(counts) == len(lines):
            steps, start = [], 0
            for index, count in enumerate(counts, 1):
                prefix = f'{index}、'
                if not lines[start].startswith(prefix):
                    break
                steps.append('\n'.join([lines[start][len(prefix):], *lines[start+1:start+count]]))
                start += count
            if start == len(lines) and all(step.strip() for step in steps):
                return steps
    return split_steps(raw)


def attachment_rows(fields):
    photos = fields.get(IMAGES) or []
    if not isinstance(photos, list) or any(not isinstance(item, dict) or not isinstance(item.get('file_token'), str)
                                          or not re.fullmatch(r'[A-Za-z0-9_-]{1,180}', item['file_token']) for item in photos):
        raise RiskError('图片字段返回不完整，请刷新记录。', 503)
    return {item['file_token']: item for item in photos}


def linked_groups(fields, raw, count):
    groups = step_metadata(fields, raw).get('groups')
    if (not isinstance(groups, list) or len(groups) != count
            or any(not isinstance(group, list) or any(not isinstance(token, str) for token in group) for group in groups)):
        return [[] for _ in range(count)]
    return groups


def disposition_cards(fields):
    steps = record_steps(fields, plain(fields.get(STEPS)))
    if not steps:
        raise RiskError('请先保存至少一条处置步骤。')
    title = plain(fields.get(MAIN[0])).strip()
    context = ' · '.join(filter(None, (plain(fields.get(name)).strip() for name in (SCENARIO_CODE, MAIN[1], MAIN[2]))))
    summary = f'{title}\n{context}' if context else title
    introduction = {'tag': 'div', 'text': {'tag': 'plain_text', 'content': summary}}

    def card(panels):
        return {'schema': '2.0', 'config': {'enable_forward': False},
                'header': {'template': 'blue', 'title': {'tag': 'plain_text', 'content': 'EOP 处置步骤'}},
                'body': {'elements': [introduction, *panels]}}

    cards, panels = [], []
    for index, step in enumerate(steps, 1):
        first_line = step.partition('\n')[0].strip()
        preview = first_line[:36] + ('…' if len(first_line) > 36 else '')
        # Feishu caps a message at 30 KB and a card at 200 components.
        for offset in range(0, len(step), 3200):
            heading = f'第 {index} 步' + ('（续）' if offset else f' · {preview}')
            panel = {'tag': 'collapsible_panel', 'expanded': index == 1 and offset == 0,
                     'header': {'title': {'tag': 'plain_text', 'content': heading}},
                     'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': step[offset:offset+3200]}}]}
            candidate = json.dumps({'content': json.dumps(card([*panels, panel]), ensure_ascii=False)}, ensure_ascii=False)
            if panels and (len(panels) >= 45 or len(candidate.encode('utf-8')) > 26 * 1024):
                cards.append(card(panels))
                panels = []
            panels.append(panel)
    cards.append(card(panels))
    if len(cards) > 1:
        for index, item in enumerate(cards, 1):
            item['header']['title']['content'] += f'（{index}/{len(cards)}）'
    return cards


async def send_disposition_cards(runtime, cards, user, rid, revision, delivery_id):
    from .feishu_delivery import FeishuDeliveryError, send_agent_message
    from .portal_service import BUILDING_OPEN_ID_MAP, LI_SHILONG_OPEN_ID

    async def deliver(name, recipient):
        for index, card in enumerate(cards, 1):
            message_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL,
                'lighthouse:eop:' + digest([user, rid, revision, delivery_id, recipient, index])))
            try:
                await send_agent_message(runtime, recipient, card, message_uuid=message_uuid)
            except FeishuDeliveryError as exc:
                prefix = f'第 {index}/{len(cards)} 张：' if len(cards) > 1 else ''
                return {'name': name, 'sent': False, 'error': prefix + str(exc)}
        return {'name': name, 'sent': True}

    return await asyncio.gather(
        deliver('H楼值班账号', BUILDING_OPEN_ID_MAP['H']),
        deliver('李世龙', LI_SHILONG_OPEN_ID),
    )


class EopEditor:
    def __init__(self, cloud, store=None):
        self.cloud = cloud
        self.store = store
        self.path = cloud.table_path(BASE, TABLE)
        self.key = secrets.token_bytes(32)
        self.lock = threading.RLock()
        self.schema_lock = threading.Lock()
        self.detail_schema = None
        self.download_slots = threading.BoundedSemaphore(3)
        self.cache_lock = threading.RLock()
        self.cached = None
        self.cache_generation = 0
        self.image_dir = Path(store.db_path).parent / 'eop_scenario_images' if store else None

    def list(self):
        cursor, seen, rows = '', set(), []
        with self.schema_lock:
            schema = self.schema()
            self.detail_schema = (getattr(self.cloud, 'config_hash', None), time.monotonic() + SCHEMA_TTL, schema)
        selected_fields = [name for name in (*DETAIL_FIELDS, STEPS, IMAGES, LINK) if name in schema]
        for _ in range(50):
            params = {'page_size': 250} | ({'page_token': cursor} if cursor else {})
            data = self.cloud.request('POST', self.path + '/records/search', params=params,
                                      json={'field_names': selected_fields})
            if not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
                raise RiskError('场景列表未完整返回。', 503)
            for row in data['items']:
                rid, fields = row.get('record_id'), row.get('fields')
                if not isinstance(rid, str) or not isinstance(fields, dict):
                    raise RiskError('场景列表存在不完整记录。', 503)
                rows.append({'id': rid, 'fields': fields})
            if not data['has_more']:
                return rows
            cursor = data.get('page_token')
            if not cursor or cursor in seen:
                break
            seen.add(cursor)
        raise RiskError('场景列表分页未完整返回。', 503)

    def refresh_cache(self):
        with self.cache_lock:
            generation = self.cache_generation
        rows = self.list()
        with self.cache_lock:
            if generation != self.cache_generation:
                return self.cache()
            value = {'version':2, 'updated_at': time.time(), 'records': {row['id']: row['fields'] for row in rows},
                     'schema': self.detail_schema[2] if self.detail_schema else {}}
            if len(value['records']) != len(rows):
                raise RiskError('场景列表存在重复记录。', 503)
            if self.store:
                self.store.put_document('eop_scenario_cache', 'snapshot', value)
            self.cached = value
            self.cache_generation += 1
            return value

    def cache(self):
        with self.cache_lock:
            if self.cached is None and self.store:
                saved = self.store.get_document('eop_scenario_cache', 'snapshot')
                if isinstance(saved, dict) and saved.get('version') == 2 and isinstance(saved.get('records'), dict):
                    self.cached = saved
            value = self.cached
        return value or self.refresh_cache()

    def cached_list(self, priority_scope='p0'):
        if priority_scope not in ('p0','other'):
            raise RiskError('场景范围无效。',400)
        value = self.cache()
        return {'items': [{'id': rid, 'fields': {name: plain(fields.get(name)) for name in (*MAIN, SCENARIO_CODE)},
                           'has_steps': bool(split_steps(plain(fields.get(STEPS)))),
                           'has_template': bool(plain(fields.get('原通告模板')).strip()),
                           'has_images': bool(attachment_rows(fields))}
                          for rid, fields in value['records'].items()
                          if (plain(fields.get('原优先级')).strip() == 'P0-高') == (priority_scope == 'p0')], 'updated_at': value.get('updated_at')}

    def cached_detail(self, rid, user, editable):
        with self.cache_lock:
            snapshot = self.cache()
            fields = snapshot['records'].get(rid)
            if not fields:
                raise RiskError('该场景不在当前缓存中，请刷新后查看。', 404)
            with self.schema_lock:
                identity = getattr(self.cloud, 'config_hash', None)
                cached = self.detail_schema
                if cached and cached[0] == identity and time.monotonic() < cached[1]:
                    schema = cached[2]
                elif isinstance(snapshot.get('schema'), dict) and all(
                        name in snapshot['schema'] for name in (*SELECTS, IMAGES)):
                    schema = snapshot['schema']
                else:
                    schema = self.schema()
                    self.detail_schema = (identity, time.monotonic() + SCHEMA_TTL, schema)
            return self.record_detail(rid, user, editable, fields, schema)

    def cache_saved(self, rid, fields):
        if not self.store:
            return
        value = self.cache()
        records = dict(value['records'])
        records[rid] = fields
        value = {'version':2, 'updated_at': value.get('updated_at'), 'records': records, 'schema': value.get('schema', {})}
        try:
            self.store.put_document('eop_scenario_cache', 'snapshot', value)
        except Exception:
            # The cloud write has already been read back. A local cache error
            # must not report that the completed cloud save failed.
            logging.exception('EOP 保存已确认，但本地缓存未能持久化')
        self.cached = value
        self.cache_generation += 1

    def read(self, rid):
        if not re.fullmatch(r'rec[A-Za-z0-9]{1,100}', rid):
            raise RiskError('场景编号无效。', 404)
        record = self.cloud.request('GET', self.path + '/records/' + rid).get('record') or {}
        if record.get('record_id') != rid or not isinstance(record.get('fields'), dict):
            raise RiskError('场景记录未完整返回。', 503)
        return record['fields']

    def schema(self):
        fields = {row['field_name']: row for row in self.cloud.all(self.path + '/fields')}
        expected = dict.fromkeys((*LONG, MAIN[0], STEPS), 1) | dict.fromkeys(SELECTS, 3) | {IMAGES: 17}
        if any((fields.get(name) or {}).get('type') != kind for name, kind in expected.items()):
            raise RiskError('飞书场景表字段已变化，请核对字段名称和类型。', 503)
        return fields

    def ticket(self, rid, user, token, purpose, extra=''):
        now = int(time.time())
        expiry = now + 7200
        if purpose == 'preview':
            expiry -= now % PREVIEW_WINDOW
        value = base64.urlsafe_b64encode(json.dumps([rid, user, token, purpose, expiry, extra]).encode()).decode()
        return value + '.' + hmac.new(self.key, value.encode(), 'sha256').hexdigest()

    def check_ticket(self, ticket, rid, user, purpose):
        try:
            value, signature = ticket.split('.', 1)
            if not hmac.compare_digest(signature, hmac.new(self.key, value.encode(), 'sha256').hexdigest()):
                raise ValueError
            rec, owner, token, use, expiry, extra = json.loads(base64.urlsafe_b64decode(value))
            if (rec, owner, use) != (rid, user, purpose) or expiry < time.time():
                raise ValueError
            return token, extra
        except (AttributeError, TypeError, ValueError):
            raise RiskError('图片凭据已失效，请重新打开记录或选择图片。', 403) from None

    @staticmethod
    def revision(fields):
        values = {name: plain(fields.get(name)) for name in (*MAIN, *LONG, STEPS, LINK)}
        if code := plain(fields.get(SCENARIO_CODE)):
            values[SCENARIO_CODE] = code
        return digest([values, list(attachment_rows(fields))])

    def detail(self, rid, user, editable):
        fields = self.read(rid)
        with self.schema_lock:
            identity = getattr(self.cloud, 'config_hash', None)
            cached = self.detail_schema
            if cached and cached[0] == identity and time.monotonic() < cached[1]:
                schema = cached[2]
            else:
                schema = self.schema()
                self.detail_schema = (identity, time.monotonic() + SCHEMA_TTL, schema)
        return self.record_detail(rid, user, editable, fields, schema)

    def record_detail(self, rid, user, editable, fields, schema):
        raw = plain(fields.get(STEPS))
        steps = record_steps(fields, raw)
        photos = attachment_rows(fields)
        try:
            names = json.loads(plain(fields.get(LINK)) or '{}').get('names', {})
            if not isinstance(names, dict): names = {}
        except (ValueError, AttributeError, TypeError):
            names = {}
        assigned = set()
        result = []
        for step, group in zip(steps, linked_groups(fields, raw, len(steps)), strict=True):
            current = [token for token in dict.fromkeys(group) if token in photos and token not in assigned]
            assigned.update(current)
            result.append({'text': step, 'images': current})
        return {'id': rid, 'revision': self.revision(fields), 'editable': editable,
                'fields': {name: plain(fields.get(name)) for name in DETAIL_FIELDS}, 'steps': result,
                'readonly_fields': [SCENARIO_CODE] if schema.get(SCENARIO_CODE, {}).get('type', 1) != 1 else [],
                'options': {name: [item['name'] for item in (schema[name].get('property') or {}).get('options', [])]
                            for name in SELECTS},
                'images': [{'token': token, 'name': names.get(token) or item.get('name') or '场景图片',
                            'url': '/api/eop-scenarios/' + rid + '/image/' + self.ticket(
                                rid, user, token, 'preview', self.attachment_extra(rid, schema[IMAGES]['field_id'], token, item))}
                           for token, item in photos.items()]}

    @staticmethod
    def attachment_extra(rid, field, token, item):
        reported = parse_qs(urlsplit(item.get('url') or '').query).get('extra', [''])[0]
        return reported or json.dumps({'bitablePerm': {'tableId': TABLE, 'attachments': {field: {rid: [token]}}}},
                                      separators=(',', ':'))

    def save(self, rid, user, body):
        with self.cache_lock, self.lock:
            current = self.read(rid)
            if body.get('revision') != self.revision(current):
                raise RiskError('该记录已变化。请保留当前草稿，重新打开并核对飞书记录。', 409)
            schema = self.schema()
            values, steps, images = body.get('fields'), body.get('steps'), body.get('images')
            if not isinstance(values, dict) or not set((*MAIN, *LONG)) <= values.keys() <= set(DETAIL_FIELDS):
                raise RiskError('场景字段不完整。')
            if not isinstance(steps, list) or len(steps) > 200 or not isinstance(images, list) or len(images) > 100:
                raise RiskError('每条记录最多编辑 200 个步骤及 100 张图片。')
            original = attachment_rows(current)
            try:
                previous_names = json.loads(plain(current.get(LINK)) or '{}').get('names', {})
                if not isinstance(previous_names, dict): previous_names = {}
            except (ValueError, AttributeError, TypeError):
                previous_names = {}
            files, used, groups, labels, names = [], set(), [], [], {}
            for item in images:
                if not isinstance(item, dict) or not isinstance(item.get('token'), str) or item['token'] in files:
                    raise RiskError('图片列表无效或存在重复项。')
                token = item['token']
                if token not in original and token != self.check_ticket(item.get('ticket'), rid, user, 'upload')[0]:
                    raise RiskError('此图片未上传到当前场景。', 403)
                files.append(token)
                name = item.get('name', previous_names.get(token, ''))
                if not isinstance(name, str) or len(name) > 180 or '\x00' in name:
                    raise RiskError('图片名称无效或过长。')
                if name.strip(): names[token] = name.strip()
            removed = body.get('removed_images', [])
            if not isinstance(removed, list) or any(not isinstance(token, str) for token in removed):
                raise RiskError('移除图片的清单无效。')
            if set(original).difference(files) != set(removed):
                raise RiskError('图片清单与主动移除的图片不一致，请刷新核对。', 409)
            for index, step in enumerate(steps, 1):
                if not isinstance(step, dict) or not isinstance(step.get('images'), list):
                    raise RiskError('步骤内容不完整。')
                value = step.get('text')
                if isinstance(value, str):
                    value = value.replace('\r\n', '\n').replace('\r', '\n')
                    if len(value) > 16000:
                        raise RiskError(f'第 {index} 步超过 16000 字，请缩短文字后保存。')
                labels.append(text(value, 16000, True))
                group = []
                for token in step['images']:
                    if not isinstance(token, str) or token not in files or token in used:
                        raise RiskError('每张图片只能关联到一个步骤。')
                    used.add(token)
                    group.append(token)
                groups.append(group)
            changes = {}
            for name, value in values.items():
                if not isinstance(value, str) or len(value) > (100000 if name in LONG else 200) or '\x00' in value:
                    raise RiskError(f'「{name}」内容无效或过长。')
                if name == MAIN[0] and not value.strip():
                    raise RiskError('请填写场景名称。')
                if value == plain(current.get(name)):
                    continue
                if name == SCENARIO_CODE and schema.get(name, {}).get('type', 1) != 1:
                    raise RiskError('飞书场景编号字段不是可编辑的文本字段。')
                if name in SELECTS and value and value not in {
                        option['name'] for option in (schema[name].get('property') or {}).get('options', [])}:
                    raise RiskError(f'「{name}」请从原表选项中选择。')
                changes[name] = value or (None if name in SELECTS else '')
            original_text = raw = plain(current.get(STEPS))
            original_steps = record_steps(current, raw)
            if labels != original_steps:
                raw = '\n'.join(f'{index}、{label}' for index, label in enumerate(labels, 1))
                if len(raw) > 100000:
                    raise RiskError('处置步骤合计超过 100000 字，请缩短文字后保存。')
                changes[STEPS] = raw
            original_order = [item['file_token'] for item in current.get(IMAGES) or []]
            removed_tokens = set(removed)
            expected_files = [token for token in original_order if token not in removed_tokens]
            expected_files.extend(token for token in files if token not in original)
            if expected_files != original_order:
                changes[IMAGES] = [{'file_token': token} for token in expected_files]
            if groups != linked_groups(current, original_text, len(original_steps)) or STEPS in changes or names != previous_names:
                counts = [label.count('\n') + 1 for label in labels] if split_steps(raw) != labels else []
                if used or names or counts:
                    if LINK not in schema:
                        self.cloud.request('POST', self.path + '/fields', json={'field_name': LINK, 'type': 1})
                    elif schema[LINK]['type'] != 1:
                        raise RiskError('「步骤图片关联」应为文本字段。', 503)
                    metadata = {'text_hash': digest(raw), 'groups': groups, 'names': names}
                    if counts:
                        metadata['step_lines'] = counts
                    changes[LINK] = json.dumps(metadata, ensure_ascii=False)
                elif LINK in schema and plain(current.get(LINK)):
                    changes[LINK] = ''
            observed = current
            if changes:
                if SCENARIO_CODE in changes and SCENARIO_CODE not in schema:
                    self.cloud.request('POST', self.path + '/fields', json={'field_name': SCENARIO_CODE, 'type': 1})
                    schema = self.schema()
                    if schema.get(SCENARIO_CODE, {}).get('type') != 1:
                        raise RiskError('飞书未确认场景编号文本字段，请刷新后重试。', 503)
                data = self.cloud.request('PUT', self.path + '/records/' + rid, json={'fields': changes})
                if (data.get('record') or {}).get('record_id') != rid:
                    raise RiskError('保存结果未确认，请重新打开核对。', 503)
                observed = self.read(rid)
                attachment_rows(observed)
                observed_files = [item['file_token'] for item in observed.get(IMAGES) or []]
                expected_link = changes.get(LINK, plain(current.get(LINK)))
                if (Counter(observed_files) != Counter(expected_files)
                        or plain(observed.get(STEPS)) != raw or plain(observed.get(LINK)) != expected_link):
                    raise RiskError('飞书回读的步骤或图片与提交内容不一致，请重新打开核对。', 503)
                if any(plain(observed.get(name)) != plain(value) for name, value in changes.items() if name in values):
                    raise RiskError('飞书回读的场景字段与提交内容不一致，请重新打开核对。', 503)
            self.cache_saved(rid, observed)
            return {'saved': True, 'record': self.record_detail(rid, user, True, observed, schema)}

    def upload(self, rid, user, name, content):
        self.read(rid)
        name = re.sub(r'[\\/:*?"<>|]', '_', text(name, 180, True))
        mime, _ = self.check_image(content)
        result = self.cloud.request('POST', 'drive/v1/medias/upload_all',
            data={'file_name': name, 'parent_type': 'bitable_file', 'parent_node': BASE, 'size': str(len(content))},
            files={'file': (name, content, mime)})
        token = result.get('file_token', '')
        if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,180}', token):
            raise RiskError('飞书未确认图片上传。', 503)
        return {'token': token, 'ticket': self.ticket(rid, user, token, 'upload'), 'name': name}

    @staticmethod
    def check_image(content, thumb=False):
        from PIL import Image, ImageOps, UnidentifiedImageError
        try:
            with Image.open(io.BytesIO(content)) as photo:
                if photo.format not in MIME or photo.width * photo.height > 45_000_000:
                    raise RiskError('仅支持 PNG、JPG、GIF、WebP 图片，且单张不超过 4500 万像素。', 422)
                if thumb:
                    photo.thumbnail((520, 520))
                    output = io.BytesIO()
                    ImageOps.exif_transpose(photo).convert('RGB').save(output, 'JPEG', quality=82)
                    return 'image/jpeg', output.getvalue()
                mime = MIME[photo.format]
                photo.verify()
                return mime, content
        except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError):
            raise RiskError('图片内容无效或无法预览。', 422) from None

    def media(self, token, extra, thumb):
        with self.download_slots:
            path = self.image_dir / (hashlib.sha256((token + extra + str(bool(thumb))).encode()).hexdigest() + '.bin') if self.image_dir else None
            if path and path.is_file():
                try:
                    return self.check_image(path.read_bytes(), False)
                except (OSError, RiskError):
                    pass
            with self.cloud.lock:
                headers = self.cloud.headers()
            content, _ = self.cloud.http.request_bytes('GET', API + 'drive/v1/medias/' + quote(token, safe='') + '/download',
                                                        headers=headers, params={'extra': extra}, max_bytes=MAX_DOWNLOAD)
            mime, content = self.check_image(content, thumb)
            if path:
                try:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    tmp = path.with_name(path.name + '.' + secrets.token_hex(6) + '.tmp')
                    tmp.write_bytes(content)
                    os.replace(tmp, path)
                except OSError:
                    logging.warning('EOP 图片本地缓存写入失败')
            return mime, content

    @lru_cache(maxsize=48)
    def thumbnail(self, token, extra):
        return self.media(token, extra, True)


def install_eop_scenario_routes(app, controller, runtime):
    service = None
    lock = threading.Lock()
    stopped = threading.Event()

    def manager():
        nonlocal service
        with lock:
            if service is None:
                service = EopEditor(RiskCloud(None, runtime.state_store.db_path.parent / 'feishu_assistant.json', None, None), runtime.state_store)
            return service

    def refresh_loop():
        while not stopped.is_set():
            try:
                manager().refresh_cache()
            except Exception:
                logging.exception('EOP 场景定时同步失败，保留上次完整缓存')
            stopped.wait(300)

    def startup():
        threading.Thread(target=refresh_loop, name='eop-scenario-refresh', daemon=True).start()

    def shutdown():
        stopped.set()

    app.add_event_handler('startup', startup)
    app.add_event_handler('shutdown', shutdown)

    async def endpoint(request: Request):
        from .module_access import professional_record_visible, profession_query

        try:
            session = await asyncio.to_thread(controller._current_session, request)
            if not session:
                return controller._auth_required_response()
            user = (session.get('user') or {}).get('open_id') or session.get('open_id')
            granted = await asyncio.to_thread(allowed_modules, runtime.auth_manager, session)
            if not user or 'eop_scenarios' not in granted:
                raise RiskError('需要 EOP 场景维护权限。', 403)
            editor = await asyncio.to_thread(runtime.auth_manager.is_admin, session)
            svc = await asyncio.to_thread(manager)
            rid, ticket = (request.path_params.get(key, '') for key in ('rid', 'ticket'))
            if request.method == 'GET' and ticket:
                token, extra = svc.check_ticket(ticket, rid, user, 'preview')
                if request.query_params.get('original') == '1':
                    mime, content = await asyncio.to_thread(svc.media, token, extra, False)
                else:
                    mime, content = await asyncio.to_thread(svc.thumbnail, token, extra)
                return Response(content, media_type=mime, headers={'Cache-Control': 'private, max-age=180',
                    'X-Content-Type-Options': 'nosniff', 'Referrer-Policy': 'no-referrer'})
            view = request.query_params.get('profession_view') if request.method == 'GET' else None
            professional_scope = await asyncio.to_thread(profession_query, runtime.auth_manager, session, view=view)
            professions = professional_scope.get('professions')
            if rid and professions is not None:
                cache = await asyncio.to_thread(svc.cache)
                fields = cache['records'].get(rid)
                if not fields or not professional_record_visible(professions, fields):
                    raise RiskError('该场景不存在或不在当前专业范围。', 404)
            if request.method == 'GET':
                if not rid and request.query_params.get('refresh') == '1':
                    await asyncio.to_thread(svc.refresh_cache)
                data = await asyncio.to_thread(svc.cached_detail, rid, user, editor) if rid else await asyncio.to_thread(svc.cached_list, request.query_params.get('priority_scope','p0'))
                if not rid and professions is not None:
                    data = data | {'items': [item for item in data['items'] if professional_record_visible(professions, item)]}
            else:
                source = urlsplit(request.headers.get('origin') or request.headers.get('referer') or '')
                expected = urlsplit(controller._request_base_url(request))
                if not editor or (source.scheme, source.netloc) != (expected.scheme, expected.netloc) or request.headers.get('sec-fetch-site') == 'cross-site':
                    raise RiskError('仅管理员可在当前站点修改场景。', 403)
                if request.method == 'POST' and request.url.path.endswith('/send-card'):
                    if session.get('impersonation') or session.get('source') == 'admin_impersonation':
                        raise RiskError('请退出代登录后发送处置步骤卡片。', 403)
                    payload = await controller._read_json_request(request, max_bytes=2048)
                    delivery_id = payload.get('delivery_id') if isinstance(payload, dict) else None
                    if not isinstance(delivery_id, str) or not re.fullmatch(r'[0-9a-f]{32}', delivery_id):
                        raise RiskError('发送请求无效，请重新打开场景。')
                    fields = await asyncio.to_thread(svc.read, rid)
                    if professions is not None and not professional_record_visible(professions, fields):
                        raise RiskError('该场景不存在或不在当前专业范围。', 404)
                    revision = svc.revision(fields)
                    if payload.get('revision') != revision:
                        raise RiskError('场景已更新，请刷新并确认最新步骤后再发送。', 409)
                    cards = disposition_cards(fields)
                    data = {'recipients': await send_disposition_cards(runtime, cards, user, rid, revision, delivery_id)}
                elif request.method == 'POST':
                    content = bytearray()
                    async for part in request.stream():
                        if len(content) + len(part) > MAX_UPLOAD:
                            raise RiskError('单张图片最大 18 MB。', 413)
                        content.extend(part)
                    data = await asyncio.to_thread(svc.upload, rid, user, request.query_params.get('name', ''), bytes(content))
                else:
                    body = await controller._read_json_request(request, max_bytes=2 * 1024 * 1024)
                    if not isinstance(body, dict):
                        raise RiskError('提交格式无效。')
                    data = await asyncio.to_thread(svc.save, rid, user, body)
            return JSONResponse({'ok': True, 'data': data}, headers={'Cache-Control': 'private, no-store'})
        except RiskError as exc:
            return JSONResponse({'ok': False, 'error': str(exc)}, status_code=exc.status)
        except Exception:
            logging.exception('EOP scenario editor operation failed')
            return JSONResponse({'ok': False, 'error': '飞书请求未确认，请刷新核对原记录。'}, status_code=503)

    for suffix, methods in (('', ['GET']), ('/{rid}', ['GET', 'PUT']), ('/{rid}/send-card', ['POST']), ('/{rid}/image', ['POST']),
                            ('/{rid}/image/{ticket}', ['GET'])):
        app.add_api_route('/api/eop-scenarios' + suffix, endpoint, methods=methods)
