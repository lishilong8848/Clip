"""Durable, private Feishu delivery of tool review decisions."""

import json
import re
import time
import uuid

from .risk_inspection import RiskError
from .tool_review_cards import plain


def init_tables(db):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS tool_result_messages(
            id TEXT PRIMARY KEY, recipient TEXT NOT NULL, card TEXT NOT NULL,
            next_try REAL NOT NULL DEFAULT 0, sent INTEGER NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '');
        CREATE TABLE IF NOT EXISTS tool_result_targets(
            id INTEGER PRIMARY KEY, batch TEXT NOT NULL, order_id TEXT NOT NULL, revision INTEGER NOT NULL,
            event TEXT NOT NULL, review_round TEXT NOT NULL, role TEXT NOT NULL, name TEXT NOT NULL,
            person_id TEXT NOT NULL, open_id TEXT NOT NULL DEFAULT '', payload TEXT NOT NULL,
            archive_revision INTEGER NOT NULL, next_try REAL NOT NULL DEFAULT 0, error TEXT NOT NULL DEFAULT '',
            message_id TEXT NOT NULL DEFAULT '', UNIQUE(order_id,revision,event,role,name));
        CREATE INDEX IF NOT EXISTS tool_result_pending ON tool_result_targets(message_id,next_try,batch);
        CREATE INDEX IF NOT EXISTS tool_result_recipient ON tool_result_targets(batch,order_id,open_id);
        CREATE INDEX IF NOT EXISTS tool_result_delivery ON tool_result_messages(sent,next_try);
    """)


def queue_result(db, row, event, actor, at, note='', batch=None):
    review = db.execute('SELECT * FROM tool_work_reviews WHERE order_id=?', (row['id'],)).fetchone()
    draft = json.loads(review['payload']) if review else {}
    linked = db.execute('SELECT session_id FROM tool_workspace_items WHERE order_id=?', (row['id'],)).fetchone()
    asset = json.loads(row['asset'])
    records = json.loads(row['records'])
    if linked:
        inspectors = [draft.get('inspector_name', '')]
    else:
        inspectors = list(dict.fromkeys(r.get('inspector', '') for r in records))
    review_round = review['submitted_at'] if review else ''
    targets = []
    for name in inspectors or ['']:
        prior = db.execute("""SELECT person_id FROM tool_result_targets WHERE order_id=? AND review_round=?
            AND role='检查人' AND name=? AND open_id<>'' ORDER BY id LIMIT 1""",
            (row['id'], review_round, name)).fetchone()
        targets.append(('检查人', name, prior[0] if prior else ''))
    if event.startswith('manager_'):
        reviewer_name = next((r.get('reviewer') for r in records if r.get('reviewer')), '')
        targets.append(('审核人', reviewer_name, review['reviewer'] if review else ''))
    archived_revision = 0
    if event == 'normal':
        archived_revision = db.execute("""SELECT revision FROM tool_work_sync
            WHERE order_id=? AND dest='maintenance'""", (row['id'],)).fetchone()[0]
    payload = json.dumps(dict(scope=row['scope'], asset=row['asset_id'], session=linked[0] if linked else '',
        tool=asset['name'], model=asset.get('model') or '', quantity=row['scrap_qty'],
        reason=row['scrap_reason'], by=actor['name'], at=at, note=note), ensure_ascii=False)
    batch = batch or uuid.uuid4().hex
    db.executemany("""INSERT OR IGNORE INTO tool_result_targets
        (batch,order_id,revision,event,review_round,role,name,person_id,payload,archive_revision)
        VALUES(?,?,?,?,?,?,?,?,?,?)""", [(batch, row['id'], row['revision']+1, event, review_round,
                                          role, name, person_id, payload, archived_revision)
                                         for role, name, person_id in targets])


def current_participant(db, order_id, actor, *, refill=False):
    # A prior rejected round must not authorize changes after resubmission.
    return db.execute("""SELECT 1 FROM tool_work_reviews r WHERE r.order_id=? AND
        ((?=0 AND r.submitted_by=? AND EXISTS (SELECT 1 FROM tool_result_targets t
          WHERE t.order_id=r.order_id AND t.person_id=r.submitted_by AND t.open_id=? AND t.open_id<>'')) OR
        EXISTS (SELECT 1 FROM tool_result_targets t WHERE
        t.order_id=r.order_id AND t.review_round=r.submitted_at AND t.person_id=? AND t.open_id=?
        AND t.open_id<>'' AND (?=0 OR (r.status='rejected' AND t.role='检查人' AND t.event='rejected'))))""",
        (order_id, int(refill), actor['id'], actor.get('open_id', ''), actor['id'],
         actor.get('open_id', ''), int(refill))).fetchone() is not None


def pending_labels(db, order_ids):
    if not order_ids:
        return {}
    slots = ','.join('?' for _ in order_ids)
    rows = db.execute(f"""SELECT t.order_id,t.role,t.name,t.error,m.error delivery_error
        FROM tool_result_targets t LEFT JOIN tool_result_messages m ON m.id=t.message_id
        WHERE t.order_id IN ({slots}) AND COALESCE(m.sent,0)=0 ORDER BY t.id""", tuple(order_ids))
    result = {}
    for row in rows:
        errors = result.setdefault(row['order_id'], [])
        error = row['error'] or row['delivery_error']
        if error:
            message = f"{row['role']}{'「'+row['name']+'」' if row['name'] else ''}：{error}"
            if message not in errors:
                errors.append(message)
    return {key: '结果通知待发送' if not errors else '结果通知 · ' + '；'.join(errors)
            for key, errors in result.items()}


def resolve_targets(service):
    from .tool_management import can_scope

    now = time.time()
    with service.connect(True) as db:
        seed = db.execute("""SELECT batch FROM tool_result_targets
            WHERE open_id='' AND next_try<=? ORDER BY next_try,id LIMIT 1""", (now,)).fetchone()
        if not seed:
            return False
        rows = db.execute("""SELECT * FROM tool_result_targets WHERE batch=? AND open_id='' AND next_try<=?""",
                          (seed['batch'], now)).fetchall()
        db.executemany('UPDATE tool_result_targets SET next_try=? WHERE id=?', [(now+120, r['id']) for r in rows])
    directory = None
    resolved = {}
    for row in rows:
        identity, name = row['person_id'], row['name']
        try:
            if not identity:
                if not name:
                    raise RiskError('记录缺少对应人员，请核对人员资料。')
                if directory is None:
                    directory = service.workspace_directory() if service.workspace_directory else []
                matches = {p['record_id'] for p in directory if p.get('record_id') and p.get('name') == name
                           and p.get('selectable') and not p.get('departed')}
                if len(matches) != 1:
                    raise RiskError('人员目录中有同名或缺少有效匹配，请核对。')
                identity, = matches
            if identity not in resolved:
                resolved[identity] = service.person_for(identity)
            person = resolved[identity]
            open_id = person.get('open_id', '')
            if (person.get('id') != identity or not re.fullmatch(r'ou_[A-Za-z0-9]+', open_id)
                    or person.get('inactive') or person.get('explicit_disabled')
                    or not row['person_id'] and person.get('name') != name):
                raise RiskError('飞书身份无法确认，请核对人员资料。')
            if not service.available_to(person) or not can_scope(person, json.loads(row['payload'])['scope']):
                raise RiskError('收件人当前没有该楼工具记录的查看权限，请核对。')
            error = ''
        except Exception as exc:
            open_id = ''
            error = str(exc) if isinstance(exc, RiskError) else '人员目录暂不可用，将自动重试。'
        with service.connect(True) as db:
            db.execute("""UPDATE tool_result_targets SET open_id=?,person_id=?,error=?,next_try=?
                WHERE id=? AND open_id=''""", (open_id, identity if open_id else row['person_id'], error,
                                               0 if open_id else now+120, row['id']))
    return True


RESULTS = {
    'rejected': '自检记录已被驳回，请打开原记录重新填写并提交。',
    'normal': '工具检验已通过审核，检查记录已归档。',
    'repair': '工具检验已审核，请进行后续检修。',
    'scrap': '工具检验已审核，报废申请已移交园区经理审批。',
    'manager_approve': '园区经理已通过报废申请，工具已批准报废。',
    'manager_reject': '园区经理已驳回报废申请。',
}


def make_card(rows, review_link, order_link):
    elements = []
    title = '工具报废审批结果' if rows[0]['event'].startswith('manager_') else '工具自检审核结果'
    card = {'schema': '2.0', 'header': {'title': plain(title), 'template': 'blue'}, 'body': {'elements': elements}}
    size = len(json.dumps(card, ensure_ascii=False).encode())
    members = []
    for row in rows:
        data = json.loads(row['payload'])
        event = row['event']
        url = (review_link(data['scope'], data['session'], data['asset']) if data['session']
               else order_link(data['scope'], row['order_id']))
        if not url:
            raise RiskError('灯塔记录链接暂不可用，将自动重试。')
        lines = [f"{data['scope']}楼 · {data['tool']}" + (' · '+data['model'] if data['model'] else ''), RESULTS[event]]
        if event.startswith('manager_'):
            lines.extend([f"报废单号：{row['order_id']}　数量：{data['quantity']}", '报废原因：'+data['reason']])
        lines.extend([('审批人：' if event.startswith('manager_') else '审核人：') + data['by'],
                      ('审批时间：' if event.startswith('manager_') else '审核时间：') + data['at'][:19].replace('T', ' ') + '（北京时间）'])
        if data['note']:
            lines.append('意见：'+data['note'])
        item = [{'tag': 'div', 'text': plain('\n'.join(lines))},
                {'tag': 'button', 'type': 'primary', 'text': plain('打开原记录重新填写' if event == 'rejected' else '查看记录'),
                 'behaviors': [{'type': 'open_url', 'default_url': url}]}]
        size += len(json.dumps(item, ensure_ascii=False).encode())
        if members and size > 28000:
            break
        elements.extend(item)
        members.append(row)
    return card, members


def prepare_message(service, review_link, order_link):
    now = time.time()
    with service.connect(True) as db:
        # The second role may resolve after the first role's card was delivered.
        db.execute("""UPDATE tool_result_targets AS t SET message_id=(SELECT s.message_id FROM tool_result_targets s
            WHERE s.batch=t.batch AND s.order_id=t.order_id AND s.open_id=t.open_id AND s.message_id<>'' LIMIT 1)
            WHERE t.message_id='' AND t.open_id<>'' AND EXISTS (SELECT 1 FROM tool_result_targets s
            WHERE s.batch=t.batch AND s.order_id=t.order_id AND s.open_id=t.open_id AND s.message_id<>'')""")
        ready = """FROM tool_result_targets t WHERE message_id='' AND open_id<>'' AND next_try<=?
            AND (archive_revision=0 OR (EXISTS (SELECT 1 FROM tool_work_sync s WHERE s.order_id=t.order_id
                AND s.dest='maintenance' AND s.synced>=t.archive_revision AND s.remote_id<>'')
                AND NOT EXISTS (SELECT 1 FROM tool_result_targets other JOIN tool_work_sync s
                    ON s.order_id=other.order_id AND s.dest='maintenance'
                    WHERE other.batch=t.batch AND other.open_id=t.open_id AND other.message_id=''
                    AND other.archive_revision>s.synced AND s.error='')))"""
        seed = db.execute(f'SELECT batch,open_id {ready} ORDER BY id LIMIT 1', (now,)).fetchone()
        if not seed:
            return False
        rows = db.execute(f"""SELECT t.* {ready} AND batch=? AND open_id=? ORDER BY id""",
                          (now, seed['batch'], seed['open_id'])).fetchall()
    distinct = {}
    for row in rows:
        distinct.setdefault(row['order_id'], row)
    try:
        card, group = make_card(list(distinct.values()), review_link, order_link)
    except Exception as exc:
        error = str(exc) if isinstance(exc, RiskError) else '暂时无法生成记录链接，将自动重试。'
        with service.connect(True) as db:
            db.executemany("UPDATE tool_result_targets SET error=?,next_try=? WHERE id=? AND message_id=''",
                           [(error, now+60, r['id']) for r in rows])
        return True
    with service.connect(True) as db:
        ids = [r['id'] for r in group]
        slots = ','.join('?' for _ in ids)
        if db.execute(f"SELECT 1 FROM tool_result_targets WHERE id IN ({slots}) AND message_id<>'' LIMIT 1", ids).fetchone():
            return True
        identifier = str(uuid.uuid4())
        db.execute('INSERT INTO tool_result_messages(id,recipient,card) VALUES(?,?,?)',
                   (identifier, seed['open_id'], json.dumps(card, ensure_ascii=False)))
        db.executemany("""UPDATE tool_result_targets SET message_id=?,error=''
            WHERE batch=? AND open_id=? AND order_id=? AND message_id=''""",
            [(identifier, seed['batch'], seed['open_id'], r['order_id']) for r in group])
    return True


def send_result_card(recipient, card, identifier):
    from upload_event_module.services.robot_webhook import send_interactive_to_open_ids

    ok, _, receipts = send_interactive_to_open_ids(card, [recipient], message_uuid=identifier)
    if ok:
        return True
    detail = receipts[0] if receipts else {}
    if detail.get('failure_kind') == 'uncertain' and '核验原消息' in detail.get('message', ''):
        raise RiskError('送达结果待核验，请核验原消息。系统会保留原发送编号，避免重复推送。')
    return False


def result_notice_one(service, deliver, review_link, order_link):
    resolved = resolve_targets(service)
    prepared = prepare_message(service, review_link, order_link)
    now = time.time()
    with service.connect(True) as db:
        message = db.execute("""SELECT * FROM tool_result_messages WHERE sent=0 AND next_try<=?
            ORDER BY next_try,rowid LIMIT 1""", (now,)).fetchone()
        if not message:
            return resolved or prepared
        db.execute('UPDATE tool_result_messages SET next_try=? WHERE id=?', (now+600, message['id']))
    try:
        sent = deliver(message['recipient'], json.loads(message['card']), message['id']) is True
        error = '' if sent else '飞书未确认送达，将自动重试。'
    except RiskError as exc:
        sent, error = False, str(exc)
    except Exception:
        sent, error = False, '飞书未确认送达，将自动重试。'
    with service.connect(True) as db:
        db.execute("""UPDATE tool_result_messages SET sent=?,error=?,next_try=? WHERE id=? AND sent=0""",
                   (sent, error, now+60, message['id']))
    return True
