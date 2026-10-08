"""Read the two user-named tables only; never modify Feishu records."""
import json
import sqlite3
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
TABLES = [('projects', 'AnEBwJlvGiJfDdkOB32cUPuknzg', 'tblschT48zXwigUG'),
          ('reference', 'D01TwFPyXiJBY6kCBDZcMCGLnSe', 'tblj9XJLq5QzTAqX')]
selected = {
    'projects': ['关联事件单-L', '设备检修关联-L', '维修跟进记录-L', '检修通告名称-L',
                 '流程-L', '当前维修进度-L', '故障发生时间-L', '创建时间'],
    'reference': ['事件编号', '告警描述', '机楼', '事件发生时间', '是否转检修',
                  '检修进展', '检修完成时间', '检修进度', '最终状态'],
}
with sqlite3.connect((ROOT / 'bin/data/lan_portal_state.sqlite3').as_uri() + '?mode=ro', uri=True) as db:
    settings = {k: json.loads(v) for k, v in db.execute(
        "SELECT key,value_json FROM settings WHERE key IN ('feishu_app_id','feishu_app_secret')")}
with httpx.Client(timeout=30, trust_env=False) as client:
    auth = client.post('https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal',
        json={'app_id': settings['feishu_app_id'], 'app_secret': settings['feishu_app_secret']}).json()
    assert auth.get('code') == 0, 'Credential validation failed'
    client.headers['Authorization'] = 'Bearer ' + auth['tenant_access_token']

    def read(path, params=None):
        value = client.get('https://open.feishu.cn/open-apis/' + path, params=params).json()
        assert value.get('code') == 0, (value.get('code'), value.get('msg'))
        return value['data']

    for name, wiki, table in TABLES:
        app = read('wiki/v2/spaces/get_node', {'token': wiki})['node']['obj_token']
        base = f'bitable/v1/apps/{app}/tables/{table}'
        fields, page, seen = [], '', set()
        while True:
            response = read(base + '/fields', {'page_size': 100, **({'page_token': page} if page else {})})
            fields.extend(response['items'])
            if not response.get('has_more'):
                break
            page = response.get('page_token')
            assert page and page not in seen
            seen.add(page)
        (ROOT / '.codex-audit' / ('repair-' + name + '-schema.json')).write_text(
            json.dumps({'app': app, 'table': table, 'fields': fields}, ensure_ascii=False), encoding='utf-8')
        rows, page, seen = [], '', set()
        body = {'field_names': selected[name]}
        if name == 'projects':
            body['filter'] = {'conjunction': 'and', 'conditions': [
                {'field_name': '流程-L', 'operator': 'is', 'value': ['未开始']}]}
        while True:
            value = client.post('https://open.feishu.cn/open-apis/' + base + '/records/search',
                params={'page_size': 500, **({'page_token': page} if page else {})}, json=body).json()
            assert value.get('code') == 0, (value.get('code'), value.get('msg'))
            response = value['data']
            rows.extend(response.get('items') or [])
            if not response.get('has_more'):
                break
            page = response.get('page_token')
            assert page and page not in seen
            seen.add(page)
        (ROOT / '.codex-audit' / ('repair-' + name + '-rows.json')).write_text(
            json.dumps(rows, ensure_ascii=False), encoding='utf-8')
        print(name, 'complete_count', len(rows), flush=True)
