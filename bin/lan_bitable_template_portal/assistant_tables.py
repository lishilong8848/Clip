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
from .personnel_source import TABLE_ID as PERSONNEL_TABLE, APP_TOKEN as PERSONNEL_BASE
PERSONNEL_URL = f'https://vnet.feishu.cn/base/{PERSONNEL_BASE}?table={PERSONNEL_TABLE}'
SCOPE_NAMES = {'楼栋', '所属楼栋', '机楼', '机楼/专业', '建筑名称', '楼宇', '所属机楼'}


def field_allowed(name):
    return not (SECRET.search(name) or SENSITIVE.search(name) or '地址' in name or 'address' in name.lower())


class AssistantTables:
    def __init__(self, service, store):
        self.service, self.store = service, store
        self.directory = LinkDirectory(store, LinkRemote(service))
        self.locks = [threading.Lock() for _ in range(16)]

    def cached(self, key, ttl, load, *, source_identity=None):
        # Fixed lock stripes bound memory and coalesce identical concurrent reads.
        with self.locks[int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % len(self.locks)]:
            version = getattr(self.service, 'state_cache_version', None)
            revision = version() if callable(version) and not key.startswith('base:') else None
            old = self.store.get_document(NS, key)
            hit = bool(old and old.get('source_revision') == revision
                and old.get('source_identity') == source_identity and 0 <= time.time() - old['observed_at'] < ttl)
            value = old if hit else {'value': load(), 'observed_at': time.time(),
                'source_revision': revision, 'source_identity': source_identity}
            if not hit:
                self.store.put_document(NS, key, value)
            return {**value, 'cache_hit': hit, 'expires_at': value['observed_at'] + ttl,
                'age_seconds': max(0, time.time() - value['observed_at'])}

    @staticmethod
    def source_metadata(item, schema=None):
        return {'kind': 'structured_table', 'source': item['url'], 'source_version': None,
            'fields': None if schema is None else [dict(field) for field in schema
                if field_allowed(field['name']) and field['type'] not in {17, 18}],
            'field_lookup': {'table_id': item['table_id'], 'metadata_only': True},
            'history': {'coverage': 'unknown', 'start': None, 'end': None},
            'refresh': {'mode': 'on_demand', 'schema_ttl_seconds': 3600, 'records_ttl_seconds': 120},
            'pagination': {'cursor': True, 'page_size_max': 40, 'max_pages': 20, 'timeout_seconds': 25},
            'read_coverage': {'state': 'not_read', 'complete': False},
            'statistics': {'tool': 'query_statistics', 'basis': 'authorized_structured_records',
                'full_total_requires_complete': True}, 'content_trust': 'untrusted_data'}

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
            if table in {'tbluozblhRAjbljX', 'tblRV9KeWFh9xCkm'}:
                continue
            if re.fullmatch(r'tbl[A-Za-z0-9]+', table):
                result.append({**item, 'table_id': table})
        if not any(row['table_id'] == PERSONNEL_TABLE for row in result):
            result.append({'table_id': PERSONNEL_TABLE, 'url': PERSONNEL_URL, 'name': '员工信息池',
                           'category': '人员', 'purpose': '人员信息与当天班组班次', 'sort': 0})
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
        return {'items': [{**{k: row[k] for k in ('name', 'table_id', 'category', 'purpose', 'url')},
                          'source_metadata': self.source_metadata(row)}
                          for row in rows[(page - 1) * 20:page * 20]], 'total': len(rows), 'page': page,
                'page_size': 20, 'has_more': page * 20 < len(rows), 'count_basis': 'directory_entries'}

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

    def schema(self, item, *, ttl=3600, receipt=False):
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
        saved = self.cached('schema:' + item['table_id'], ttl, load, source_identity=[base, item['url']])
        return base, saved if receipt else saved['value']

    @staticmethod
    def valid_page(data):
        if (not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool
                or data['has_more'] and not isinstance(data.get('page_token'), str)
                or data['has_more'] and not data['page_token']):
            raise AssistantError('多维表分页不完整，不能判断为零条。', 503)

    def read(self, actor, query):
        allowed_keys = {'table_id', 'fields', 'filters', 'cursor', 'limit', 'metadata_only', 'complete'}
        if not isinstance(query, dict) or set(query) - allowed_keys:
            raise AssistantError('只读表格查询参数无效。')
        if type(query.get('complete', False)) is not bool:
            raise AssistantError('完整查询参数无效。')
        if query.get('complete') and not query.get('metadata_only'):
            current = {key: value for key, value in query.items() if key != 'complete'}
            rows, cursors, deadline = {}, set(), time.monotonic() + 25
            observed, expiries, cache_hits, revisions, totals = [], [], [], set(), set()
            for pages in range(1, 21):
                page = self.read(actor, current)
                observed.append(page['observed_at'])
                expiries.append(page['freshness']['expires_at'])
                cache_hits.append(page['freshness']['cache_hit'])
                revisions.add(page['freshness']['local_cache_revision'])
                if type(page['total']) is int:
                    totals.add(page['total'])
                for row in page['items']:
                    rid = row['record_id']
                    if rid in rows and rows[rid] != row:
                        raise AssistantError('分页期间记录已变化，请重新核对；未返回不一致的汇总。', 409)
                    rows[rid] = row
                cursor = page['cursor']
                if not page['has_more'] or time.monotonic() >= deadline:
                    break
                if not cursor or cursor in cursors:
                    raise AssistantError('多维表分页游标未前进，未将部分数据当作完整结果。', 503)
                cursors.add(cursor)
                current['cursor'] = cursor
            complete = (not query.get('cursor') and not page['has_more'] and len(revisions) == 1
                and len(totals) <= 1 and (not totals or totals == {len(rows)})
                and (pages > 1 or page['complete']))
            return {**page, 'items': list(rows.values()), 'visible_page_count': len(rows),
                    'complete': complete, 'observed_at': min(observed),
                    'freshness': {**page['freshness'], 'oldest_observed_at': min(observed),
                        'newest_observed_at': max(observed), 'cache_hit': any(cache_hits),
                        'all_cache_hits': all(cache_hits), 'expires_at': min(expiries),
                        'age_seconds': max(0, time.time() - min(observed))},
                    'coverage': {**page['coverage'], 'complete': complete, 'pages_read': pages,
                        'records_read': len(rows), 'started_from_first_page': not bool(query.get('cursor'))},
                    'note': '仅统计已授权、实际读取的记录；complete=false表示分页、起始范围或来源一致性未核验完整，不能作为完整总数。'}
        item = self.table(query.get('table_id'))
        if re.search(r'密钥|凭证|密码|令牌|模型配置|应用配置|账号配置|token|secret', item['name'] + item['category'], re.I):
            raise AssistantError('凭证及安全配置表不向助手开放直接读取，请使用原安全设置页面。', 403)
        base, schema_saved = self.schema(item, receipt=True)
        schema = schema_saved['value']
        requested = query.get('fields') or []
        filters = query.get('filters') or []
        names = ([name for name in requested if isinstance(name, str)] if isinstance(requested, list) else [])
        if isinstance(filters, list):
            names += [row['field_name'] for row in filters if isinstance(row, dict) and isinstance(row.get('field_name'), str)]
        if any(field_allowed(name) and name not in {field['name'] for field in schema} for name in names):
            # Missing columns may have been added remotely; coalesce a bounded schema re-read.
            base, schema_saved = self.schema(item, ttl=10, receipt=True)
            schema = schema_saved['value']
        safe_fields = [f['name'] for f in schema if field_allowed(f['name']) and f['type'] not in {17, 18}]
        # Questions/answers retain the learning module's original visibility rules.
        if not actor.get('is_admin') and re.search(r'题库|学练|权限|账户|账号|密钥|配置|程序日志', item['name'] + item['category']):
            raise AssistantError('该表请使用原业务模块查询，不能通过直连扩大原权限。', 403)
        if query.get('metadata_only'):
            return {'name': item['name'], 'table_id': item['table_id'], 'fields': safe_fields,
                'source': item['url'], 'read_only': True, 'complete': False,
                'source_metadata': self.source_metadata(item, schema),
                'schema_observed_at': schema_saved['observed_at'],
                'schema_freshness': {key: schema_saved[key] for key in ('cache_hit', 'age_seconds', 'expires_at')}}
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
        saved = self.cached('page:' + key, 120, load, source_identity=base)
        data = saved['value']
        rows = data['items']
        if scoped:
            from openclaw_service.assistant.lighthouse_people_stats import building_codes
            rows = [r for r in rows if (scopes := set().union(*(building_codes(plain(r['fields'].get(k))) for k in scope_fields)))
                    and scopes <= set(actor['scopes'])]
        complete = (not cursor and not data['has_more'] and
            (type(data.get('total')) is not int or data['total'] == len({r['record_id'] for r in data['items']})))
        return {'name': item['name'], 'source': item['url'], 'observed_at': saved['observed_at'],
            'items': rows, 'has_more': data['has_more'], 'cursor': data.get('page_token', ''),
            'total': None if scoped else data.get('total'), 'visible_page_count': len(rows),
            'complete': complete,
            'freshness': {key: saved[key] for key in ('cache_hit', 'age_seconds', 'expires_at')}
                | {'local_cache_revision': saved['source_revision'], 'source_version': None},
            'coverage': {'complete': complete, 'scope': 'authorized_filtered_records', 'pages_read': 1,
                'records_read': len(rows), 'started_from_first_page': not bool(cursor),
                'history': 'unknown', 'atomic_snapshot': False},
            'content_trust': 'untrusted_data',
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
                        'building': plain(values.get('机楼/专业')),
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

    def roster_count(self, actor, day, shift=''):
        import datetime as dt
        from .personnel_source import daily_roster, APP_TOKEN, TABLE_ID
        from openclaw_service.assistant.lighthouse_people_stats import summarize_roster
        try:
            if not isinstance(day, str) or dt.date.fromisoformat(day).isoformat() != day or shift not in {'', '白', '夜'}:
                raise ValueError
        except (TypeError, ValueError):
            raise AssistantError('排班统计日期或班次无效。') from None
        scopes = sorted(actor.get('scopes') or [])
        if not scopes or not set(scopes) <= set(SCOPES):
            raise AssistantError('当前账号未配置可查询的楼栋范围。', 403)
        def load():
            try:
                return daily_roster(self.service, day)
            except ValueError:
                raise AssistantError('员工信息池排班未完整读取，不能确认人数；请稍后重试。', 503) from None
        saved = self.cached('roster-count:' + day + ':' + ','.join(scopes), 60, load)
        result = summarize_roster([p for p in saved['value']['people'] if p['scope'] in scopes], actor, day=day, shift=shift, observed_at=saved['observed_at'],
            source_url=f'https://vnet.feishu.cn/base/{APP_TOKEN}?table={TABLE_ID}', complete_source=True)
        issues = [i for i in saved['value'].get('issues', []) if not i.get('scope') or i['scope'] in scopes]
        if issues:
            result['complete'] = False
            result['missing'] = [item['reason'] for item in issues]
            result['basis'] += '\n存在班组、班次或楼栋待核对的记录，上述人数仅为已核实部分。'
        return result
