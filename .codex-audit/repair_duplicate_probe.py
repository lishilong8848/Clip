"""Read-only verification of the reported repair duplicates; no business writes."""
import json
import sqlite3
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
with sqlite3.connect((ROOT / 'bin/data/lan_portal_state.sqlite3').as_uri() + '?mode=ro', uri=True) as db:
    settings = {k: json.loads(v) for k, v in db.execute('SELECT key,value_json FROM settings')}
    sources = {key: (app, table) for key, app, table in db.execute(
        "SELECT source_key,app_token,table_id FROM repair_snapshot_sources WHERE source_key IN ('repair_projects','repair_events','repair_notices')")}

keep = {'关联事件单', '关联事件单-L', '设备检修关联', '设备检修关联-L', '维修汇总记录ID-L',
        '维修跟进记录', '维修跟进记录-L', '检修通告名称', '名称', '故障维修原因',
        '故障发生现象描述', '故障发生时间', '流程', '当前维修进度', '创建时间',
        '是否转检修', '最终状态', '检修进度', '告警描述', '事件发生时间', '事件编号'}
with httpx.Client(timeout=20, trust_env=False) as client:
    auth = client.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
        json={'app_id': settings['feishu_app_id'], 'app_secret': settings['feishu_app_secret']}).json()
    print('AUTH', auth.get('code'), flush=True)
    if auth.get('code') != 0:
        raise SystemExit('Credential validation failed; no record requests made')
    client.headers['Authorization'] = 'Bearer ' + auth['tenant_access_token']
    resolved = {}

    def read(path, params=None):
        try:
            payload = client.get('https://open.feishu.cn/open-apis/' + path, params=params).json()
        except httpx.TimeoutException:
            print('READ_TIMEOUT', path.rsplit('/', 1)[-1], flush=True)
            return {}
        if payload.get('code') != 0:
            print('READ_ERROR', payload.get('code'), payload.get('msg'), flush=True)
            return {}
        return payload.get('data') or {}

    def base(source):
        app, table = sources[source]
        if app not in resolved:
            node = read('wiki/v2/spaces/get_node', {'token': app}).get('node') or {}
            resolved[app] = node.get('obj_token') or app
        return f'bitable/v1/apps/{resolved[app]}/tables/{table}'

    def show(source, record):
        fields = record.get('fields') or {}
        print(json.dumps({'source': source, 'record_id': record.get('record_id'),
            'fields': {k: v for k, v in fields.items() if k in keep or k.removesuffix('-L') in keep}}, ensure_ascii=False), flush=True)

    project_base = base('repair_projects')
    terms = ['A-445-TRB-202-3', 'A-301-RPP-DC-003', 'A-345-HVDC-212-1#', 'A-316-开关柜-112-电池组1-49#']
    payload = client.post('https://open.feishu.cn/open-apis/' + project_base + '/records/search',
        params={'page_size': 100}, json={'field_names': ['关联事件单', '关联事件单-L', '设备检修关联-L', '维修跟进记录-L',
            '检修通告名称-L', '流程-L', '当前维修进度-L', '故障发生时间-L', '创建时间'],
            'filter': {'conjunction': 'or', 'conditions': [
                {'field_name': '检修通告名称-L', 'operator': 'contains', 'value': [term]} for term in terms]}}).json()
    print('SEARCH', payload.get('code'), flush=True)
    projects = payload.get('data') or {}
    assert not projects.get('has_more'), 'Scoped probe requires another page'
    for record in projects.get('items') or []:
        show('repair_projects', record)

    legacy_base = project_base.rsplit('/', 1)[0] + '/tblPXxIxz4TTmSAU'
    for record_id in ['recvwIMUkcH5m6', 'recvtorKO7NJa5']:
        result = read(legacy_base + '/records/' + record_id)
        if result.get('record'):
            show('legacy_event', result['record'])
            print('LEGACY_FIELD_NAMES', list(result['record']['fields']), flush=True)
