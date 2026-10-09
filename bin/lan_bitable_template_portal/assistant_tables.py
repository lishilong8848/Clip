"""On-demand Bitable reads, restricted to the administrator's navigation directory."""
import hashlib
import json
import re
import threading
import time
from urllib.parse import parse_qs, urlsplit

from openclaw_service.assistant.lighthouse_ai import AssistantError, SECRET, SENSITIVE, safe_data, safe_text
from openclaw_service.assistant.lighthouse_sources import SCOPES, query_terms
from .link_directory import LinkDirectory, LinkRemote, plain

NS = 'assistant_table_cache'
PERSONNEL_TABLE = 'tbluozblhRAjbljX'
PERSONNEL_BASE = 'HU38bc1vnamMK9sCeOgclUvXnFc'
PERSONNEL_URL = f'https://vnet.feishu.cn/base/{PERSONNEL_BASE}?table={PERSONNEL_TABLE}'
SCOPE_NAMES = {'楼栋', '所属楼栋', '机楼', '机楼/专业', '建筑名称', '楼宇', '所属机楼'}


def field_allowed(name):
    return not (SECRET.search(name) or SENSITIVE.search(name) or '地址' in name or 'address' in name.lower())


class AssistantTables:
    def __init__(self, service, store):
        self.service, self.store = service, store
        self.directory = LinkDirectory(store, LinkRemote(service))
        self.locks = [threading.Lock() for _ in range(16)]

    def cached(self, key, ttl, load):
        # Fixed lock stripes bound memory and coalesce identical concurrent reads.
        with self.locks[int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % len(self.locks)]:
            old = self.store.get_document(NS, key)
            if old and time.time() - old['observed_at'] < ttl:
                return old
            value = {'value': load(), 'observed_at': time.time()}
            self.store.put_document(NS, key, value)
            return value

    def request(self, method, path, *, params=None, body=None):
        # This service cannot write records, even when called by an administrator.
        if not (method == 'GET' or method == 'POST' and re.fullmatch(
                r'bitable/v1/apps/[A-Za-z0-9]+/tables/tbl[A-Za-z0-9]+/records/search', path)):
            raise AssistantError('多维表直连仅支持只读查询。', 403)
        data = self.service._request_payload(method, 'https://open.feishu.cn/open-apis/' + path,
            context='助手多维表只读查询', headers=self.service._auth_headers(), params=params or {},
            **({'json_payload': body} if body is not None else {}))
        if not isinstance(data, dict) or data.get('code') != 0 or not isinstance(data.get('data'), dict):
            raise AssistantError('飞书未返回有效查询结果，请检查应用的表格读取权限；数量未知。', 503)
        return data['data']

    def directory_rows(self):
        snapshot = self.store.get_document('link_directory', 'snapshot')
        if not snapshot:
            snapshot = self.directory.read()
        elif time.time() - snapshot['updated_at'] > 300:
            self.directory._snapshot = snapshot
            snapshot = self.directory.read()
        result = []
        for item in snapshot['items']:
            url = urlsplit(item['url'])
            if url.hostname != 'vnet.feishu.cn' or not re.fullmatch(r'/(base|wiki)/[A-Za-z0-9]+', url.path):
                continue
            table = parse_qs(url.query).get('table', [''])[0]
            if re.fullmatch(r'tbl[A-Za-z0-9]+', table):
                result.append({**item, 'table_id': table})
        return result

    def catalog(self, keyword='', page=1):
        if not isinstance(keyword, str) or len(keyword) > 160 or type(page) is not int or page < 1:
            raise AssistantError('表格检索参数无效。')
        rows = self.directory_rows()
        terms = query_terms(keyword)
        if keyword.strip():
            def score(row):
                text = ' '.join(row[k] for k in ('name', 'category', 'purpose')).casefold()
                return (100 if keyword.casefold() in text else 0) + sum(term.casefold() in text for term in terms)
            rows = sorted((row for row in rows if score(row)), key=lambda row: (-score(row), row['sort']))
        return {'items': [{k: row[k] for k in ('name', 'table_id', 'category', 'purpose', 'url')}
                          for row in rows[(page - 1) * 20:page * 20]], 'total': len(rows), 'page': page}

    def table(self, table_id):
        matches = [row for row in self.directory_rows() if row['table_id'] == table_id]
        if len(matches) != 1:
            raise AssistantError('该表未在导航目录中登记，不能直接查询。', 403)
        return matches[0]

    def base(self, item):
        parts = urlsplit(item['url']).path.split('/')
        if parts[1] == 'base':
            return parts[2]
        def load():
            node = self.request('GET', 'wiki/v2/spaces/get_node', params={'token': parts[2]})['node']
            if node.get('obj_type') != 'bitable' or not re.fullmatch(r'[A-Za-z0-9]+', str(node.get('obj_token') or '')):
                raise AssistantError('该知识库链接未指向多维表格。')
            return node['obj_token']
        return self.cached('base:' + parts[2], 86400, load)['value']

    def schema(self, item):
        base = self.base(item)
        def load():
            fields, cursor, seen = [], '', set()
            for _ in range(20):
                data = self.request('GET', f"bitable/v1/apps/{base}/tables/{item['table_id']}/fields",
                    params={'page_size': 100, **({'page_token': cursor} if cursor else {})})
                self.valid_page(data)
                fields.extend({'name': f['field_name'], 'type': f['type']} for f in data['items'])
                if not data['has_more']:
                    return fields
                cursor = data['page_token']
                if cursor in seen:
                    break
                seen.add(cursor)
            raise AssistantError('字段分页不完整，未继续读取记录。', 503)
        return base, self.cached('schema:' + item['table_id'], 3600, load)['value']

    @staticmethod
    def valid_page(data):
        if (not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool
                or data['has_more'] and not isinstance(data.get('page_token'), str)
                or data['has_more'] and not data['page_token']):
            raise AssistantError('多维表分页不完整，不能判断为零条。', 503)

    def read(self, actor, query):
        allowed_keys = {'table_id', 'fields', 'filters', 'cursor', 'limit', 'metadata_only'}
        if not isinstance(query, dict) or set(query) - allowed_keys:
            raise AssistantError('只读表格查询参数无效。')
        item = self.table(query.get('table_id'))
        if re.search(r'密钥|凭证|密码|令牌|模型配置|应用配置|账号配置|token|secret', item['name'] + item['category'], re.I):
            raise AssistantError('凭证及安全配置表不向助手开放直接读取，请使用原安全设置页面。', 403)
        base, schema = self.schema(item)
        safe_fields = [f['name'] for f in schema if field_allowed(f['name']) and f['type'] not in {17, 18}]
        # Questions/answers retain the learning module's original visibility rules.
        if not actor.get('is_admin') and re.search(r'题库|学练|权限|账户|账号|密钥|配置|程序日志', item['name'] + item['category']):
            raise AssistantError('该表请使用原业务模块查询，不能通过直连扩大原权限。', 403)
        if query.get('metadata_only'):
            return {'name': item['name'], 'table_id': item['table_id'], 'fields': safe_fields,
                'source': item['url'], 'read_only': True}
        fields = query.get('fields') or safe_fields[:40]
        if not isinstance(fields, list) or len(fields) > 60 or any(x not in safe_fields for x in fields):
            raise AssistantError('字段不存在或属于禁止提供的敏感字段。', 403)
        filters = query.get('filters') or []
        if not isinstance(filters, list) or len(filters) > 8:
            raise AssistantError('最多使用8个筛选条件。')
        for condition in filters:
            if (not isinstance(condition, dict) or set(condition) != {'field_name', 'operator', 'value'}
                    or condition['field_name'] not in safe_fields
                    or condition['operator'] not in {'is', 'isNot', 'contains', 'doesNotContain', 'isEmpty', 'isNotEmpty', 'isGreater', 'isLess'}
                    or not isinstance(condition['value'], list) or len(condition['value']) > 10
                    or any(not isinstance(x, str) or len(x) > 200 for x in condition['value'])):
                raise AssistantError('筛选条件无效或涉及禁止查询字段。')
        scoped = not actor.get('is_admin') or set(actor['scopes']) != set(SCOPES)
        scope_fields = [f['name'] for f in schema if f['name'] in SCOPE_NAMES]
        if scoped and not scope_fields:
            raise AssistantError('该表缺少可核验的楼栋字段，请通过原业务入口查询；未扩大当前账号权限。', 403)
        selected = list(dict.fromkeys(fields + scope_fields))
        limit, cursor = query.get('limit', 40), query.get('cursor', '')
        if type(limit) is not int or not 1 <= limit <= 40 or not isinstance(cursor, str) or len(cursor) > 2048:
            raise AssistantError('分页参数无效，每页最多40条。')
        body = {'field_names': selected, 'automatic_fields': False}
        view = parse_qs(urlsplit(item['url']).query).get('view', [''])[0]
        if view:
            body['view_id'] = view
        if filters:
            body['filter'] = {'conjunction': 'and', 'conditions': filters}
        params = {'page_size': limit, **({'page_token': cursor} if cursor else {})}
        key = hashlib.sha256(json.dumps([item['url'], body, params], sort_keys=True).encode()).hexdigest()
        def load():
            data = self.request('POST', f"bitable/v1/apps/{base}/tables/{item['table_id']}/records/search", params=params, body=body)
            self.valid_page(data)
            # Cache only the permitted field projection, never hidden attachments or secrets.
            return {**{k: data[k] for k in ('has_more', 'page_token', 'total') if k in data},
                'items': [{'record_id': r['record_id'], 'fields': safe_data(
                    {k: v for k, v in (r.get('fields') or {}).items() if k in selected}, list_limit=100)} for r in data['items']]}
        saved = self.cached('page:' + key, 120, load)
        data = saved['value']
        rows = data['items']
        if scoped:
            from openclaw_service.assistant.lighthouse_people_stats import building_codes
            rows = [r for r in rows if (scopes := set().union(*(building_codes(plain(r['fields'].get(k))) for k in scope_fields)))
                    and scopes <= set(actor['scopes'])]
        return {'name': item['name'], 'source': item['url'], 'observed_at': saved['observed_at'],
            'items': rows, 'has_more': data['has_more'], 'cursor': data.get('page_token', ''),
            'total': None if scoped else data.get('total'), 'visible_page_count': len(rows),
            'note': '记录按当前权限过滤。分页未读完或total为空时，不得把本页条数当作全表数量。', 'read_only': True}

    def staff_count(self, actor):
        from openclaw_service.assistant.lighthouse_people_stats import summarize_staff
        item = self.table(PERSONNEL_TABLE)
        base, schema = self.schema(item)
        known = {f['name'] for f in schema}
        if '离职/异动情况' not in known:
            raise AssistantError('人员表缺少离职/异动情况字段，无法可靠统计。', 503)
        fields = [x for x in ('员工姓名', '员工工号', '工号', '楼栋', '机楼/专业', 'openid', '离职/异动情况') if x in known]
        def load():
            rows, cursor, seen = [], '', set()
            deadline = time.monotonic() + 25
            for _ in range(20):
                if time.monotonic() > deadline:
                    break
                data = self.request('POST', f'bitable/v1/apps/{base}/tables/{PERSONNEL_TABLE}/records/search',
                    params={'page_size': 500, **({'page_token': cursor} if cursor else {})}, body={'field_names': fields})
                self.valid_page(data)
                for record in data['items']:
                    values = record.get('fields') or {}
                    inactive = values.get('离职/异动情况', False)
                    rows.append({'record_id': record['record_id'], 'name': plain(values.get('员工姓名')),
                        'employee_no': plain(values.get('员工工号') or values.get('工号')),
                        'building': plain(values.get('楼栋') or values.get('机楼/专业')),
                        'open_id': hashlib.sha256(plain(values['openid']).encode()).hexdigest() if plain(values.get('openid')) else '',
                        'inactive': inactive if type(inactive) is bool else None})
                if not data['has_more']:
                    if type(data.get('total')) is int and len({r['record_id'] for r in rows}) != data['total']:
                        raise AssistantError('人员表返回数量不完整，本次未确认人数。', 503)
                    return rows
                cursor = data['page_token']
                if cursor in seen:
                    break
                seen.add(cursor)
            raise AssistantError('人员表未完整读取，未返回不可靠的人数。', 503)
        saved = self.cached('staff-count', 300, load)
        return summarize_staff(saved['value'], actor, observed_at=saved['observed_at'], source_url=item['url'])
