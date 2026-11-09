"""Tool inventory, periodic work orders and durable Feishu ledger synchronization."""

import calendar
import datetime as dt
import hashlib
import io
import json
import os
import re
from tempfile import NamedTemporaryFile
import time
import uuid
from zipfile import BadZipFile, ZIP_DEFLATED, ZipFile

from .risk_inspection import RiskError, RiskInspections, can_scope as risk_can_scope, digest, text
from .tool_self_check import BUILDINGS, TZ, ToolSelfChecks, dumps, fill_template, month_key


CYCLES = {'每月': 1, '季度': 3, '半年': 6, '一年': 12, '两年': 24, '三年': 36, '四年': 48, '五年': 60}
KINDS = {'self': '自检', 'external': '送检'}
OUTCOMES = {'normal': '正常', 'repair': '检修中', 'repaired': '检修完成',
            'scrap': '待报废审批', 'scrapped': '已批准报废', 'rejected': '报废未通过'}
DESTINATIONS = {'maintenance': '正常维护台账', 'inspection': '送检存档表', 'repair': '检修跟踪表',
                'scrap': '工具报废表', 'change': '异动台账'}
MAX_REPORT = 12 * 1024 * 1024
MAX_ORDER_DELETIONS = 200


def full_access(actor):
    return actor.get('_tool_management_full_access') is True and not (
        actor.get('inactive') or actor.get('explicit_disabled') or actor.get('role') in {'guest','temporary'})


def trusted_full_access(auth, open_id, person):
    """Resolve the two designated people from the authenticated employee directory only."""
    from .portal_service import LI_SHILONG_OPEN_ID
    if person.get('inactive') or person and not person.get('selectable'):return False
    if open_id==LI_SHILONG_OPEN_ID:return True
    if not person.get('id') or person.get('name') not in {'李世龙','刘小杏'} or not person.get('selectable') or person.get('inactive'):
        return False
    if open_id not in person.get('login_ids',[]) and open_id!='person:'+person['id']:
        return False
    read=auth._account_read_state()
    provider=getattr(auth,'_account_directory_provider',None)
    snapshot=read['snapshot'] if read is not None else provider() if provider else {}
    app_id=read['app_id'] if read is not None else auth.current_feishu_app_id()
    if snapshot.get('schema')!=4 or snapshot.get('app_id')!=app_id:return False
    matching=[p for p in snapshot.get('people',{}).values() if p.get('name')==person['name'] and not p.get('inactive')]
    return len(matching)==1 and matching[0].get('id')==person['id']


def can_scope(actor, scope):
    return scope in BUILDINGS and full_access(actor) or risk_can_scope(actor,scope)


def clock():
    return dt.datetime.now(TZ)


def month_offset(day, months):
    year, index = divmod(day.year * 12 + day.month - 1 + months, 12)
    return dt.date(year, index + 1, min(day.day, calendar.monthrange(year, index + 1)[1]))


def date_value(value, *, required=False):
    if not value and not required:
        return ''
    try:
        parsed = dt.date.fromisoformat(text(value, 10, True))
        if parsed.isoformat() != value:
            raise ValueError
    except ValueError:
        raise RiskError('请输入有效日期。') from None
    return value


def report_file(name, data):
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', text(name, 180, True)).strip('. ')
    if not data or len(data) > MAX_REPORT:
        raise RiskError('请选择 12 MB 以内的非空报告。')
    extension = name.rpartition('.')[2].lower()
    prefixes = {'pdf': b'%PDF-', 'png': b'\x89PNG\r\n\x1a\n', 'jpg': b'\xff\xd8\xff', 'jpeg': b'\xff\xd8\xff'}
    if extension in {'docx', 'xlsx'}:
        try:
            with ZipFile(io.BytesIO(data)) as package:
                items = set(package.namelist())
                valid = '[Content_Types].xml' in items and (
                    'word/document.xml' if extension == 'docx' else 'xl/workbook.xml') in items
                valid = valid and not any(item.endswith('vbaProject.bin') for item in items)
        except (BadZipFile, OSError, ValueError):
            valid = False
    else:
        valid = extension in prefixes and data.startswith(prefixes[extension])
    if not valid:
        raise RiskError('请选择内容有效的 PDF、Word、Excel 或 JPG／PNG 报告。')
    return name


class ToolManagement(ToolSelfChecks):
    @staticmethod
    def access(actor):
        RiskInspections.check(actor)
        if not full_access(actor):ToolSelfChecks.access(actor)

    @staticmethod
    def administrator(actor):
        return full_access(actor) or ToolSelfChecks.administrator(actor)

    def __init__(self, inspection, cloud):
        super().__init__(inspection)
        self.cloud = cloud
        self.person_for = inspection.person_for
        self.archive_dir = inspection.db_path.parent / 'tool_management_archives'
        with self.connect(True) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS tool_inventory(
                    id TEXT PRIMARY KEY, source TEXT NOT NULL, active INTEGER NOT NULL,
                    config TEXT NOT NULL DEFAULT '{}', version INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE IF NOT EXISTS tool_management_state(key TEXT PRIMARY KEY, value TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS tool_work_orders(
                    id TEXT PRIMARY KEY, month TEXT NOT NULL, asset_id TEXT NOT NULL, scope TEXT NOT NULL,
                    kind TEXT NOT NULL, due TEXT NOT NULL, asset TEXT NOT NULL, supervisor TEXT NOT NULL DEFAULT '',
                    owner TEXT NOT NULL DEFAULT '', owner_name TEXT NOT NULL DEFAULT '', revision INTEGER NOT NULL DEFAULT 0,
                    records TEXT NOT NULL DEFAULT '[]', draft TEXT NOT NULL DEFAULT '{}',
                    submit_key TEXT NOT NULL DEFAULT '', report_name TEXT NOT NULL DEFAULT '', report BLOB,
                    actual TEXT NOT NULL DEFAULT '', outcome TEXT NOT NULL DEFAULT '',
                    submitted_at TEXT, manager TEXT NOT NULL DEFAULT '', scrap_qty INTEGER NOT NULL DEFAULT 0,
                    reason TEXT NOT NULL DEFAULT '', resolution TEXT NOT NULL DEFAULT '',
                    scrap_reason TEXT NOT NULL DEFAULT '', scrap_status TEXT NOT NULL DEFAULT '', resolved_at TEXT NOT NULL DEFAULT '',
                    UNIQUE(month,asset_id,kind));
                CREATE INDEX IF NOT EXISTS tool_work_orders_month ON tool_work_orders(month,scope);
                CREATE INDEX IF NOT EXISTS tool_work_orders_asset ON tool_work_orders(asset_id,kind);
                CREATE TABLE IF NOT EXISTS tool_work_audit(
                    id INTEGER PRIMARY KEY, order_id TEXT NOT NULL, action TEXT NOT NULL,
                    actor TEXT NOT NULL, name TEXT NOT NULL, at TEXT NOT NULL, detail TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS tool_work_sync(
                    order_id TEXT NOT NULL, dest TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1,
                    synced INTEGER NOT NULL DEFAULT 0, remote_id TEXT NOT NULL DEFAULT '',
                    file_token TEXT NOT NULL DEFAULT '', error TEXT NOT NULL DEFAULT '', next_try INTEGER NOT NULL DEFAULT 0,
                    lease TEXT NOT NULL DEFAULT '', lease_until INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY(order_id,dest));
                CREATE TABLE IF NOT EXISTS tool_work_archives(
                    month TEXT PRIMARY KEY, created_at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS tool_work_reviews(
                    order_id TEXT PRIMARY KEY, status TEXT NOT NULL, payload TEXT NOT NULL,
                    submitted_by TEXT NOT NULL, submitted_at TEXT NOT NULL,
                    reviewer TEXT NOT NULL DEFAULT '', reviewed_at TEXT NOT NULL DEFAULT '', note TEXT NOT NULL DEFAULT '');
                CREATE TABLE IF NOT EXISTS tool_inventory_creation(
                    operation TEXT PRIMARY KEY, actor TEXT NOT NULL, fingerprint TEXT NOT NULL,
                    status TEXT NOT NULL, record_id TEXT NOT NULL DEFAULT '');
            """)

    @staticmethod
    def state(db, key):
        row = db.execute('SELECT value FROM tool_management_state WHERE key=?', (key,)).fetchone()
        return row[0] if row else ''

    @staticmethod
    def set_state(db, key, value):
        db.execute('INSERT INTO tool_management_state VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                   (key, value))

    def refresh(self):
        assets, leaders = self.cloud.inventory()
        with self.connect(True) as db:
            db.execute('UPDATE tool_inventory SET active=0')
            db.executemany('''INSERT INTO tool_inventory(id,source,active) VALUES(?,?,1)
                ON CONFLICT(id) DO UPDATE SET source=excluded.source,active=1''',
                           [(asset['id'], dumps(asset)) for asset in assets])
            self.set_state(db, 'inventory_at', clock().isoformat(timespec='seconds'))
            self.set_state(db, 'inventory_error', '')
            self.set_state(db, 'leaders', dumps(leaders))

    @staticmethod
    def effective(row):
        return json.loads(row['source']) | json.loads(row['config'])

    def inventory(self, actor):
        self.access(actor)
        if not self.administrator(actor):
            raise RiskError('仅管理员可配置工具排期。', 403)
        with self.connect() as db:
            assets = []
            for row in db.execute('SELECT * FROM tool_inventory WHERE active=1'):
                source, config = json.loads(row['source']), json.loads(row['config'])
                assets.append(source | config | {'revision': row['version'],
                    **{f'source_{kind}_cycle': source.get(f'{kind}_cycle', '') for kind in KINDS},
                    **{f'override_{kind}_cycle': config.get(f'{kind}_cycle', '') for kind in KINDS}})
            all_scopes = all(can_scope(actor, s) for s in BUILDINGS)
            return {'items': [item for item in assets if can_scope(actor, item['scope'])],
                    'can_set_manager': all_scopes, 'manager_id': self.state(db, 'manager_id') if all_scopes else ''}

    @classmethod
    def supervisor(cls, actor, scope):
        return can_scope(actor, scope) and (actor.get('category') == 'supervisor' or '主管' in (actor.get('position') or ''))

    def available_to(self, actor):
        scopes = [scope for scope in BUILDINGS if can_scope(actor, scope)]
        if not scopes:
            return False
        if self.administrator(actor) or actor.get('category') == 'manager' or any(self.supervisor(actor, s) for s in scopes):
            return True
        with self.connect() as db:
            return any(self.is_building_leader(db, actor, scope) for scope in scopes)

    def configure(self, actor, identity, body):
        self.access(actor)
        if not self.administrator(actor):
            raise RiskError('仅管理员可配置工具排期。', 403)
        if identity == 'park-manager':
            if not all(can_scope(actor, s) for s in BUILDINGS):
                raise RiskError('仅全楼管理员可配置园区经理。', 403)
            person = self.person_for(text(body.get('manager_id'), 120, True))
            if person.get('category') != 'manager' or not all(can_scope(person, s) for s in BUILDINGS):
                raise RiskError('请选择在岗且有六栋楼权限的园区经理。')
            with self.connect(True) as db:
                self.set_state(db, 'manager_id', person['id'])
                db.execute("UPDATE tool_work_orders SET manager=?,revision=revision+1 WHERE outcome='scrap' AND manager<>?",
                           (person['id'], person['id']))
                db.execute("""UPDATE tool_work_sync SET revision=revision+1,next_try=0
                    WHERE dest='scrap' AND order_id IN(SELECT id FROM tool_work_orders WHERE outcome='scrap')""")
            return {'saved': True}
        with self.connect(True) as db:
            row = db.execute('SELECT * FROM tool_inventory WHERE id=? AND active=1', (identity,)).fetchone()
            if not row or not can_scope(actor, self.effective(row)['scope']):
                raise RiskError('没有权限配置该工具。', 403)
            scope = self.effective(row)['scope']
            if body.get('revision') != row['version']:
                raise RiskError('配置已更新，请刷新后重试。', 409)
        config = {}
        for kind in KINDS:
            cycle = text(body.get(kind + '_cycle', ''), 10)
            if cycle and cycle not in CYCLES and cycle != '停用':
                raise RiskError('请选择有效周期。')
            if cycle:
                config[kind + '_cycle'] = cycle
            first = date_value(body.get(kind + '_first', ''))
            if first:
                config[kind + '_first'] = first
        supervisor = text(body.get('supervisor', ''), 120)
        if supervisor:
            if not self.supervisor(self.person_for(supervisor), scope):
                raise RiskError('请选择对该楼栋有权限的在岗专业主管。')
            config['supervisor'] = supervisor
        with self.connect(True) as db:
            row = db.execute('SELECT * FROM tool_inventory WHERE id=? AND active=1', (identity,)).fetchone()
            if not row or row['version'] != body['revision'] or self.effective(row)['scope'] != scope:
                raise RiskError('配置或工具归属已更新，请刷新后重试。', 409)
            db.execute('UPDATE tool_inventory SET config=?,version=version+1 WHERE id=?', (dumps(config), identity))
            db.execute("""UPDATE tool_work_orders SET supervisor=?,revision=revision+1
                WHERE asset_id=? AND scope=? AND kind='external' AND submitted_at IS NULL
                AND outcome='' AND supervisor<>?""", (supervisor, identity, scope, supervisor))
        return {'saved': True}

    def order_candidates(self, actor):
        self.access(actor)
        with self.connect() as db:
            items=[]
            for row in db.execute('SELECT * FROM tool_inventory WHERE active=1'):
                asset=self.effective(row)
                if (can_scope(actor,asset['scope']) and not asset.get('retired') and
                        (self.is_building_leader(db,actor,asset['scope']) or self.administrator(actor))):
                    items.append({key:asset.get(key) for key in ('id','scope','name','model','location')})
            return {'items':items}

    def new_inventory_fields(self, actor):
        self.access(actor)
        if not self.available_to(actor):raise RiskError('当前账号没有工具管理权限。',403)
        fields=self.cloud.inventory_fields()
        return {'fields':fields,'scopes':[scope for scope in BUILDINGS if can_scope(actor,scope)]}

    def create_inventory(self, actor, body):
        self.access(actor)
        operation=text(body.get('operation_id'),32,True)
        if not re.fullmatch(r'[a-f0-9]{32}',operation):raise RiskError('新增操作编号无效。')
        values=body.get('fields')
        if not isinstance(values,dict):raise RiskError('工具资料格式无效。')
        schema={item['name']:item for item in self.cloud.inventory_fields()}
        if set(values)-set(schema):raise RiskError('包含不支持编辑的工具字段。')
        fields={}
        for key,value in values.items():
            field=schema[key];kind=field['type']
            if value in ('',None):continue
            if kind in (1,3):
                value=text(value,500,True)
                if kind==3 and field['options'] and value not in field['options']:raise RiskError(key+'选项已变化，请刷新。')
            elif kind==2:
                if type(value) not in (float,int) or value<0 or value>1000000:raise RiskError(key+'必须为有效数字。')
            elif kind==7:
                if type(value) is not bool:raise RiskError(key+'必须选择是或否。')
            elif kind==5:
                value=int(dt.datetime.combine(dt.date.fromisoformat(date_value(value,required=True)),dt.time.min,TZ).timestamp()*1000)
            fields[key]=value
        building=re.fullmatch(r'(?:南通)?([ABCDEH])楼',str(fields.get('楼栋') or ''))
        if not building or not fields.get('工具名称') or fields.get('一级分类')!='B工具类' or not fields.get('区域对应工具数量'):
            raise RiskError('请填写楼栋、工具名称、B工具类和工具数量。')
        scope=building[1]
        with self.connect(True) as db:
            if not can_scope(actor,scope) or not (self.is_building_leader(db,actor,scope) or self.administrator(actor)):
                raise RiskError('仅本楼楼长或有权限的管理员可新增工具。',403)
            old=db.execute('SELECT * FROM tool_inventory_creation WHERE operation=?',(operation,)).fetchone()
            fingerprint=digest(fields)
            if old:
                if old['actor']!=actor['id'] or old['fingerprint']!=fingerprint:raise RiskError('新增操作编号不匹配。',409)
                if old['status']=='done':return {'record_id':old['record_id'],'saved':True}
                raise RiskError('上次新增结果待核对，请先检查工具底表，勿重复新增。',409)
            db.execute("INSERT INTO tool_inventory_creation(operation,actor,fingerprint,status) VALUES(?,?,?,'posting')",(operation,actor['id'],fingerprint))
        try:
            record=self.cloud.create_inventory(fields,operation)
        except Exception:
            with self.connect(True) as db:db.execute("UPDATE tool_inventory_creation SET status='uncertain' WHERE operation=?",(operation,))
            raise RiskError('工具新增结果暂未确认，请先核对多维表，勿重复新增。',503) from None
        with self.connect(True) as db:
            db.execute("UPDATE tool_inventory_creation SET status='done',record_id=? WHERE operation=?",(record,operation))
        return {'record_id':record,'saved':True}

    def create_order(self, actor, body):
        self.access(actor)
        identity=text(body.get('asset_id'),120,True)
        kind=body.get('kind')
        if kind not in KINDS:raise RiskError('请选择自检或送检工单。')
        current=clock(); month=current.strftime('%Y-%m')
        with self.connect(True) as db:
            row=db.execute('SELECT * FROM tool_inventory WHERE id=? AND active=1',(identity,)).fetchone()
            if not row:raise RiskError('工具不存在或已停用。',404)
            asset=self.effective(row);scope=asset['scope']
            if not can_scope(actor,scope) or not (self.is_building_leader(db,actor,scope) or self.administrator(actor)):
                raise RiskError('仅本楼楼长或有权限的管理员可新增工单。',403)
            if asset.get('retired') or asset.get('quantity',0)<=0:raise RiskError('工具已报废或数量无效。')
            if db.execute('SELECT 1 FROM tool_work_archives WHERE month=?',(month,)).fetchone():
                raise RiskError('本月已归档，不能新增工单。',409)
            if db.execute('SELECT 1 FROM tool_work_orders WHERE month=? AND asset_id=? AND kind=?',(month,identity,kind)).fetchone():
                raise RiskError('本月该工具已有此类工单，请在列表中查看。',409)
            order_id=uuid.uuid4().hex
            db.execute('INSERT INTO tool_work_orders(id,month,asset_id,scope,kind,due,asset,supervisor) VALUES(?,?,?,?,?,?,?,?)',
                (order_id,month,identity,scope,kind,current.date().isoformat(),dumps(asset),asset.get('supervisor','') if kind=='external' else ''))
            self.audit(db,order_id,actor,'手动新增工单')
            return self.work_info(db,actor,self._work(db,actor,order_id))

    def check_order_deletion(self, db, actor, identity, revision, month):
        row = self._work(db, actor, identity)
        if not (self.is_building_leader(db, actor, row['scope']) or self.administrator(actor)):
            raise RiskError('仅本楼楼长或有权限的管理员可删除工单。', 403)
        self.check_revision(row, revision)
        if row['month'] != month or row['submitted_at'] or row['outcome']:
            raise RiskError('仅本月未提交工单可删除。', 409)
        if db.execute('SELECT 1 FROM tool_work_archives WHERE month=?', (month,)).fetchone():
            raise RiskError('本月已归档，不能删除工单。', 409)
        if db.execute("SELECT 1 FROM tool_work_reviews WHERE order_id=? AND status='pending'", (identity,)).fetchone():
            raise RiskError('待审核工单不能删除，请先驳回。', 409)
        if db.execute('SELECT 1 FROM tool_work_sync WHERE order_id=?', (identity,)).fetchone():
            raise RiskError('已有台账同步记录，不能删除工单。', 409)

    def mark_order_deleted(self, db, actor, identity):
        db.execute("""UPDATE tool_work_orders SET outcome='cancelled',resolution='手动删除工单',revision=revision+1
            WHERE id=?""", (identity,))
        self.audit(db, identity, actor, '删除工单', '保留历史及草稿')

    def cancel_order(self, actor, identity, body):
        with self.connect(True) as db:
            self.check_order_deletion(db, actor, identity, body.get('revision'), clock().strftime('%Y-%m'))
            self.mark_order_deleted(db, actor, identity)
            return self.work_info(db, actor, self._work(db, actor, identity))

    def cancel_orders(self, actor, body):
        entries = body.get('orders')
        if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_ORDER_DELETIONS:
            raise RiskError(f'请选择 1 至 {MAX_ORDER_DELETIONS} 张工单。')
        revisions = {}
        for entry in entries:
            if not isinstance(entry, dict) or type(entry.get('revision')) is not int or entry['revision'] < 0:
                raise RiskError('批量删除工单或版本格式无效。')
            identity = text(entry.get('id'), 120, True)
            if identity in revisions:
                raise RiskError('所选工单重复，请重新选择。')
            revisions[identity] = entry['revision']
        with self.connect(True) as db:
            month = clock().strftime('%Y-%m')
            if body.get('month') != month:
                raise RiskError('仅支持批量删除本月工单，请刷新后重新选择。', 409)
            for identity, revision in revisions.items():
                self.check_order_deletion(db, actor, identity, revision, month)
            for identity in revisions:
                self.mark_order_deleted(db, actor, identity)
        return {'deleted_ids': list(revisions)}

    def schedule(self, current=None):
        today = (current or clock()).astimezone(TZ).date()
        month = today.strftime('%Y-%m')
        end = month_offset(today.replace(day=1), 1)
        with self.connect(True) as db:
            if self.state(db, 'inventory_at')[:10] != today.isoformat() or db.execute(
                    'SELECT 1 FROM tool_work_archives WHERE month=?', (month,)).fetchone():
                return
            rows = db.execute('SELECT * FROM tool_inventory WHERE active=1').fetchall()
            for row in rows:
                asset = self.effective(row)
                history = db.execute("""SELECT kind,actual,outcome,submitted_at,scrap_qty FROM tool_work_orders
                    WHERE asset_id=?""", (row['id'],)).fetchall()
                scrapped = sum(entry['scrap_qty'] for entry in history if entry['outcome'] == 'scrapped')
                if asset['retired'] or scrapped >= asset['quantity'] or any(
                        entry['outcome'] in {'repair', 'scrap'} for entry in history):
                    continue
                for kind in KINDS:
                    cycle = CYCLES.get(asset.get(kind + '_cycle'))
                    if not cycle or any(entry['kind'] == kind and not entry['submitted_at']
                                        and entry['outcome'] != 'cancelled' for entry in history):
                        continue
                    latest = max([asset.get(kind + '_last', '')] +
                                 [entry['actual'] for entry in history if entry['kind'] == kind and entry['submitted_at']])
                    due = month_offset(dt.date.fromisoformat(latest), cycle) if latest else dt.date.fromisoformat(
                        asset.get(kind + '_first') or today.replace(day=1).isoformat())
                    if due >= end:
                        continue
                    identity = hashlib.sha256(f'{month}:{row["id"]}:{kind}'.encode()).hexdigest()[:32]
                    db.execute("""INSERT OR IGNORE INTO tool_work_orders(id,month,asset_id,scope,kind,due,asset,supervisor)
                        VALUES(?,?,?,?,?,?,?,?)""", (identity, month, row['id'], asset['scope'], kind,
                                                     due.isoformat(), dumps(asset), asset.get('supervisor', '') if kind == 'external' else ''))

    def is_building_leader(self, db, actor, scope):
        leaders = json.loads(self.state(db, 'leaders') or '{}')
        return can_scope(actor, scope) and actor['open_id'] in leaders.get(scope, ())

    def eligible(self, db, actor, row):
        return full_access(actor) or self.is_building_leader(db, actor, row['scope']) or (
            row['kind'] == 'external' and actor['id'] == row['supervisor'] and self.supervisor(actor, row['scope']))

    def visible(self, db, actor, row):
        return can_scope(actor, row['scope']) and (self.administrator(actor) or self.eligible(db, actor, row)
                 or actor['id'] == row['manager'] and actor.get('category') == 'manager')

    def _work(self, db, actor, identity):
        self.access(actor)
        row = db.execute('SELECT * FROM tool_work_orders WHERE id=?', (identity,)).fetchone()
        if not row or not self.visible(db, actor, row):
            raise RiskError('工单不存在或当前账号无权查看。', 404)
        return row

    def work_info(self, db, actor, row):
        eligible = self.eligible(db, actor, row)
        open_order = not row['submitted_at'] and row['outcome'] != 'cancelled'
        review = db.execute('SELECT * FROM tool_work_reviews WHERE order_id=?', (row['id'],)).fetchone()
        awaiting_review = bool(review and review['status'] == 'pending')
        mine = eligible and (row['owner'] == actor['id'] or full_access(actor) and bool(row['owner']))
        result = {key: row[key] for key in ('id', 'month', 'scope', 'kind', 'due', 'revision', 'owner_name', 'submitted_at',
                                            'outcome', 'report_name', 'actual', 'reason', 'resolution', 'scrap_qty')}
        sync = [{'label': DESTINATIONS[job['dest']], 'state': 'done' if job['synced'] == job['revision'] else 'pending',
                 'error': job['error']} for job in db.execute(
                    'SELECT dest,revision,synced,error FROM tool_work_sync WHERE order_id=?', (row['id'],))]
        asset = json.loads(row['asset'])
        return result | {'tool_name': asset['name'], 'location': asset['location'], 'model': asset['model'],
                         'quantity': asset['quantity'], 'supervisor_assigned': bool(row['supervisor']),
                         'assigned_to_me': row['supervisor'] == actor['id'] and eligible,
                         'status': 'cancelled' if row['outcome'] == 'cancelled' else 'review_pending' if awaiting_review else 'pending' if open_order else 'submitted',
                         'review_note': review['note'] if review else '',
                         'can_review': awaiting_review and (self.is_building_leader(db, actor, row['scope']) or full_access(actor)),
                         'can_delete': (open_order and not row['outcome'] and not awaiting_review and not sync
                                        and row['month'] == clock().strftime('%Y-%m')
                                        and (self.is_building_leader(db, actor, row['scope']) or self.administrator(actor))),
                         'can_claim': open_order and not awaiting_review and not row['owner'] and eligible,
                         'can_edit': open_order and not awaiting_review and mine,
                         'can_release': open_order and not awaiting_review and bool(row['owner']) and self.administrator(actor),
                         'can_repair': row['outcome'] == 'repair' and (self.is_building_leader(db, actor, row['scope']) or self.administrator(actor)),
                         'can_approve': row['outcome'] == 'scrap' and (full_access(actor) or row['manager'] == actor['id']
                             and actor.get('category') == 'manager'), 'sync': sync}

    def list(self, actor, month=''):
        self.access(actor)
        if not self.available_to(actor):
            raise RiskError('当前账号没有工具管理权限。', 403)
        current = clock().strftime('%Y-%m')
        month = month_key(month or current)
        with self.connect() as db:
            rows = db.execute("""SELECT id,month,scope,kind,due,asset,supervisor,owner,owner_name,revision,actual,outcome,
                submitted_at,manager,scrap_qty,reason,resolution,report_name FROM tool_work_orders
                WHERE month=? OR outcome IN('repair','scrap') ORDER BY month DESC,scope,due,kind""", (month,)).fetchall()
            visible = [self.work_info(db, actor, row) for row in rows if self.visible(db, actor, row)]
            admin = self.administrator(actor)
            all_scopes = admin and all(can_scope(actor, scope) for scope in BUILDINGS)
            archive = db.execute('SELECT created_at FROM tool_work_archives WHERE month=?', (month,)).fetchone()
            templates = [self.template_info(row) for row in db.execute("""SELECT id,version,name,active,schema
                FROM tool_check_templates t WHERE version=(SELECT MAX(version) FROM tool_check_templates WHERE id=t.id)
                ORDER BY name""") if admin or row['active']]
            return {'month': month, 'orders': [row for row in visible if row['month'] == month],
                    'followups': [row for row in visible if row['outcome'] in {'repair', 'scrap'}],
                    'templates': templates, 'can_manage_templates': admin, 'can_download_month': all_scopes,
                    'archived_at': archive[0] if archive and all_scopes else None,
                    'inventory_at': self.state(db, 'inventory_at'), 'inventory_error': self.state(db, 'inventory_error'),
                    'actor_name': actor['name']}

    def detail(self, actor, identity):
        with self.connect() as db:
            row = self._work(db, actor, identity)
            records = [{**record, 'template': self.template_info(self._template(db, record['template_id'], record['version']))}
                       for record in json.loads(row['records'])]
            history = [dict(entry) for entry in db.execute(
                'SELECT action,name,at,detail FROM tool_work_audit WHERE order_id=? ORDER BY id', (identity,))]
            review = db.execute('SELECT * FROM tool_work_reviews WHERE order_id=?', (identity,)).fetchone()
            draft = json.loads(review['payload']) if review and review['status']=='pending' else json.loads(row['draft'])
            return self.work_info(db, actor, row) | {'records': records, 'actor_name': actor['name'],
                                                   'history': history, 'draft': draft}

    @staticmethod
    def audit(db, identity, actor, action, detail=''):
        db.execute('INSERT INTO tool_work_audit(order_id,action,actor,name,at,detail) VALUES(?,?,?,?,?,?)',
                   (identity, action, actor['id'], actor['name'], clock().isoformat(timespec='seconds'), detail))

    @staticmethod
    def enqueue(db, identity, *destinations):
        db.executemany("""INSERT INTO tool_work_sync(order_id,dest) VALUES(?,?)
            ON CONFLICT(order_id,dest) DO UPDATE SET revision=revision+1,error='',next_try=0""",
                       [(identity, dest) for dest in destinations])

    def attach(self, actor, identity, revision, name, data):
        name = report_file(name, data)
        with self.connect(True) as db:
            row = self._work(db, actor, identity)
            info = self.work_info(db, actor, row)
            if row['kind'] != 'external' or not info['can_edit']:
                raise RiskError('仅已领取的送检工单可上传报告。', 403)
            if row['report_name'] == name and row['report'] == data:
                return info
            self.check_revision(row, revision)
            db.execute('UPDATE tool_work_orders SET report_name=?,report=?,revision=revision+1 WHERE id=?',
                       (name, data, identity))
            self.audit(db, identity, actor, '上传送检报告', name)
            return self.work_info(db, actor, self._work(db, actor, identity))

    @staticmethod
    def check_revision(row, revision):
        if type(revision) is not int or revision != row['revision']:
            raise RiskError('工单已变化，请重新打开核对后再操作。', 409)

    def action(self, actor, identity, action, body):
        with self.connect(True) as db:
            row = self._work(db, actor, identity)
            info = self.work_info(db, actor, row)
            submit_key = digest({k: v for k, v in body.items() if k != 'revision'}) if action == 'submit' else ''
            pending = db.execute('SELECT * FROM tool_work_reviews WHERE order_id=?', (identity,)).fetchone()
            if action in {'review-approve','review-reject'} and pending and pending['reviewer']==actor['id']:
                expected = 'approved' if action=='review-approve' else 'rejected'
                if pending['status']==expected and pending['note']==text(body.get('reason',''),1000):
                    return info
            if action == 'submit' and pending and pending['status'] == 'pending' and row['owner'] == actor['id'] and row['submit_key'] == submit_key:
                return info
            if action == 'claim' and info['can_edit'] or (action == 'submit' and row['submitted_at']
                    and row['owner'] == actor['id'] and row['submit_key'] == submit_key):
                return info
            self.check_revision(row, body.get('revision'))
            now = clock().isoformat(timespec='seconds')
            updates, dests, note = {}, (), ''
            reviewing = action == 'review-approve'
            if action in {'review-approve', 'review-reject'}:
                if not info['can_review']:
                    raise RiskError('仅本楼楼长可审核待审核的自检工单。', 403)
                note = text(body.get('reason', ''), 1000, action == 'review-reject')
                if action == 'review-reject':
                    db.execute("UPDATE tool_work_reviews SET status='rejected',reviewer=?,reviewed_at=?,note=? WHERE order_id=?", (actor['id'],now,note,identity))
                    saved=json.loads(pending['payload'])
                    draft={key:saved.get(key) for key in ('outcome','reason','quantity')}
                    db.execute('UPDATE tool_work_orders SET draft=?,revision=revision+1 WHERE id=?', (dumps(draft),identity))
                    self.audit(db, identity, actor, '驳回自检审核', note)
                    return self.work_info(db, actor, self._work(db, actor, identity))
                body = json.loads(pending['payload'])
                action = 'submit'
                submit_key = row['submit_key']
            if action == 'submit' and row['kind'] == 'self' and not reviewing and info['can_edit']:
                records = self._records(db, body.get('records'), json.loads(row['records']), submit=True)
                for entry in records:
                    entry.update(reviewer='', reviewed_at='')
                body = {**body, 'records': records}
                outcome = body.get('outcome')
                if outcome not in ('normal','repair','scrap'):
                    raise RiskError('请选择工具状态。')
                if outcome == 'normal' and any('否' in item['checks'] for item in records):
                    raise RiskError('检查结果含未通过项，请核对工具状态。')
                if outcome == 'scrap': self.scrap_request(db, row, body)
                if outcome == 'repair': text(body.get('reason'), 1000, True)
                if any(item['checked_at'][:10] > now[:10] for item in records):
                    raise RiskError('实际检查日期不能晚于今天。')
                db.execute("""INSERT INTO tool_work_reviews(order_id,status,payload,submitted_by,submitted_at)
                    VALUES(?,'pending',?,?,?) ON CONFLICT(order_id) DO UPDATE SET status='pending',payload=excluded.payload,
                    submitted_by=excluded.submitted_by,submitted_at=excluded.submitted_at,reviewer='',reviewed_at='',note=''""",
                    (identity,dumps(body),actor['id'],now))
                db.execute("UPDATE tool_work_orders SET records=?,submit_key=?,revision=revision+1 WHERE id=?", (dumps(records),submit_key,identity))
                self.audit(db, identity, actor, '提交自检待审核')
                return self.work_info(db, actor, self._work(db, actor, identity))
            if action == 'claim' and info['can_claim']:
                updates = {'owner': actor['id'], 'owner_name': actor['name']}
                label = '领取工单'
            elif action == 'release' and info['can_release']:
                updates = {'owner': '', 'owner_name': ''}
                label = '释放领取'
            elif action in {'draft', 'submit'} and (info['can_edit'] or reviewing):
                if row['kind'] == 'self':
                    records = self._records(db, body.get('records'), json.loads(row['records']), submit=action == 'submit')
                    if reviewing:
                        for entry in records:
                            entry.update(reviewer=actor['name'],reviewed_at=now[:16])
                    updates['records'] = dumps(records)
                if action == 'draft' and row['kind'] == 'self':
                    label = '保存草稿'
                    draft_outcome, draft_qty = body.get('outcome'), body.get('quantity')
                    updates['draft'] = dumps({'outcome': draft_outcome if draft_outcome in ('normal', 'repair', 'scrap') else '',
                        'quantity': draft_qty if type(draft_qty) is int and draft_qty > 0 else 1,
                        'reason': text(body.get('reason', ''), 1000)})
                elif action == 'submit':
                    updates.update(submitted_at=now, submit_key=submit_key, draft='{}')
                    if full_access(actor) and not reviewing:updates.update(owner=actor['id'],owner_name=actor['name'])
                    label = '提交工单'
                    if row['kind'] == 'external':
                        if not row['report']:
                            raise RiskError('请先上传送检报告。')
                        updates['actual'] = date_value(body.get('actual'), required=True)
                        dests = ('inspection',)
                    else:
                        outcome = body.get('outcome')
                        if outcome not in ('normal', 'repair', 'scrap'):
                            raise RiskError('请选择工具状态。')
                        if outcome == 'normal' and any('否' in item['checks'] for item in records):
                            raise RiskError('检查结果含未通过项，请核对工具状态。')
                        updates.update(outcome=outcome, actual=max(item['checked_at'][:10] for item in records))
                        if outcome == 'scrap':
                            updates |= self.scrap_request(db, row, body)
                            dests = ('scrap', 'change')
                        elif outcome == 'repair':
                            updates['reason'] = text(body.get('reason'), 1000, True)
                            dests = ('repair', 'change')
                        else:
                            dests = ('maintenance',)
                else:
                    raise RiskError('送检报告已单独保存，请直接提交。')
            elif action in {'repaired', 'repair-scrap'} and info['can_repair']:
                note = text(body.get('reason'), 1000, True)
                updates = {'resolution': note, 'outcome': 'repaired' if action == 'repaired' else 'scrap'}
                label = '检修完成' if action == 'repaired' else '检修转报废'
                dests = ('repair', 'change')
                if action == 'repair-scrap':
                    updates |= self.scrap_request(db, row, body)
                    dests += ('scrap',)
            elif action in {'approve', 'reject'} and info['can_approve']:
                note = text(body.get('reason', ''), 1000, action == 'reject')
                label = '通过报废审批' if action == 'approve' else '驳回报废审批'
                repair = db.execute("SELECT 1 FROM tool_work_sync WHERE order_id=? AND dest='repair'", (identity,)).fetchone()
                outcome = 'scrapped' if action == 'approve' else 'repair' if repair else 'rejected'
                updates = {'outcome': outcome, 'resolved_at': now,
                           'scrap_status': 'approved' if action == 'approve' else 'rejected',
                           'resolution': '\n'.join(filter(None, (row['resolution'], note)))}
                dests = ('scrap', 'change', 'repair') if repair else ('scrap', 'change')
                if outcome == 'scrapped':
                    prior = db.execute("SELECT COALESCE(SUM(scrap_qty),0) FROM tool_work_orders WHERE asset_id=? AND outcome='scrapped'",
                                       (row['asset_id'],)).fetchone()[0]
                    if prior + row['scrap_qty'] >= json.loads(row['asset'])['quantity']:
                        cancelled = db.execute("""SELECT id FROM tool_work_orders WHERE asset_id=?
                            AND submitted_at IS NULL AND outcome=''""", (row['asset_id'],)).fetchall()
                        for sibling in cancelled:
                            db.execute("""UPDATE tool_work_orders SET outcome='cancelled',resolution='工具已全部报废',
                                revision=revision+1 WHERE id=?""", (sibling['id'],))
                            self.audit(db, sibling['id'], actor, '取消工单', '工具已全部报废')
            else:
                raise RiskError('当前工单状态或账号权限不允许此操作。', 409)
            if updates.get('actual', '') > now[:10]:
                raise RiskError('实际检查日期不能晚于今天。')
            sql = ','.join(f'{field}=?' for field in updates)
            db.execute(f'UPDATE tool_work_orders SET {sql},revision=revision+1 WHERE id=?',
                       (*updates.values(), identity))
            self.audit(db, identity, actor, label, note or updates.get('reason') or updates.get('scrap_reason', ''))
            if reviewing:
                db.execute("UPDATE tool_work_reviews SET status='approved',reviewer=?,reviewed_at=?,note=? WHERE order_id=?", (actor['id'],now,note,identity))
                self.audit(db, identity, actor, '通过自检审核', note)
            self.enqueue(db, identity, *dests)
            return self.work_info(db, actor, self._work(db, actor, identity))

    def scrap_request(self, db, row, body):
        manager_id = self.state(db, 'manager_id')
        if not manager_id:
            raise RiskError('请管理员先配置园区经理，再提交报废。', 409)
        prior = db.execute("""SELECT COALESCE(SUM(scrap_qty),0) FROM tool_work_orders
            WHERE asset_id=? AND outcome='scrapped'""", (row['asset_id'],)).fetchone()[0]
        quantity = body.get('quantity')
        if type(quantity) is not int or not 1 <= quantity <= json.loads(row['asset'])['quantity'] - prior:
            raise RiskError('报废数量不能超过该工具的剩余数量。')
        reason = text(body.get('reason'), 1000, True)
        return {'manager': manager_id, 'scrap_qty': quantity, 'scrap_reason': reason,
                'scrap_status': 'pending', 'resolved_at': ''}

    def sync_one(self):
        now = int(time.time())
        lease = uuid.uuid4().hex
        with self.connect(True) as db:
            task = db.execute("""SELECT * FROM tool_work_sync WHERE synced<revision AND next_try<=? AND lease_until<=?
                ORDER BY next_try,order_id,dest LIMIT 1""", (now, now)).fetchone()
            if not task:
                return False
            job = dict(task)
            identity, dest = job['order_id'], job['dest']
            db.execute('UPDATE tool_work_sync SET lease=?,lease_until=? WHERE order_id=? AND dest=?',
                       (lease, now + 600, identity, dest))
            row = db.execute('SELECT * FROM tool_work_orders WHERE id=?', (identity,)).fetchone()
            job['from_repair'] = bool(db.execute(
                "SELECT 1 FROM tool_work_sync WHERE order_id=? AND dest='repair'", (identity,)).fetchone())
            if row and row['kind']=='self' and dest in {'maintenance','repair','scrap'}:
                records=json.loads(row['records'])
                job['record_entries']=[(entry,dict(self._template(db,entry['template_id'],entry['version']))) for entry in records]

        def save_file(token):
            with self.connect(True) as db:
                db.execute('UPDATE tool_work_sync SET file_token=? WHERE order_id=? AND dest=? AND lease=?',
                           (token, identity, dest, lease))

        try:
            entries=job.get('record_entries') or []
            if entries:
                if len(entries)==1:
                    entry,template=entries[0]
                    job['record_name']='自检记录单.xlsx'
                    job['record_content']=fill_template(template['content'],json.loads(template['schema']),entry,row['scope'])
                else:
                    output=io.BytesIO()
                    with ZipFile(output,'w',ZIP_DEFLATED) as archive:
                        for index,(entry,template) in enumerate(entries,1):
                            archive.writestr(f'{index:02d}-自检记录单.xlsx',fill_template(template['content'],json.loads(template['schema']),entry,row['scope']))
                    job['record_name']='自检记录单.zip';job['record_content']=output.getvalue()
            remote_id = self.cloud.write(dest, row, json.loads(row['asset']), job, self.person_for, save_file)
        except Exception as exc:
            error = str(exc) if isinstance(exc, RiskError) else '飞书同步结果未确认，将重试原工单。'
            with self.connect(True) as db:
                db.execute("""UPDATE tool_work_sync SET lease='',lease_until=0,
                    error=CASE WHEN revision=? THEN ? ELSE error END,
                    next_try=CASE WHEN revision=? THEN ? ELSE next_try END
                    WHERE order_id=? AND dest=? AND lease=?""",
                           (job['revision'], error, job['revision'], int(time.time()) + 60, identity, dest, lease))
        else:
            with self.connect(True) as db:
                db.execute("""UPDATE tool_work_sync SET remote_id=?,synced=?,error='',next_try=0,lease='',lease_until=0
                    WHERE order_id=? AND dest=? AND lease=?""", (remote_id, job['revision'], identity, dest, lease))
        return True

    def package_work(self, db, month, rows, output):
        manifest = []
        with ZipFile(output, 'w', ZIP_DEFLATED) as archive:
            for row in rows:
                folder = f'{row["scope"]}楼/{KINDS[row["kind"]]}/{row["id"]}'
                files = []
                for index, entry in enumerate(json.loads(row['records']), 1):
                    if not row['submitted_at']:
                        break
                    template = self._template(db, entry['template_id'], entry['version'])
                    safe = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', template['name'])
                    name = f'{folder}/{index:02d}-{safe}.xlsx'
                    archive.writestr(name, fill_template(template['content'], json.loads(template['schema']), entry, row['scope']))
                    files.append(name)
                if row['submitted_at'] and row['report']:
                    name = f'{folder}/{row["report_name"]}'
                    archive.writestr(name, row['report'])
                    files.append(name)
                manifest.append({'id': row['id'], 'tool': json.loads(row['asset'])['name'], 'scope': row['scope'],
                    'type': KINDS[row['kind']], 'due': row['due'], 'actual': row['actual'],
                    'outcome': OUTCOMES.get(row['outcome'], '已提交' if row['submitted_at'] else '已取消'),
                    'submitted_at': row['submitted_at'], 'submitted_by': row['owner_name'], 'files': files})
            archive.writestr('归档清单.json', dumps({'month': month, 'orders': manifest}).encode())

    def archive_ready(self):
        self.archive_dir.mkdir(parents=True, exist_ok=True)
        with self.connect() as db:
            ready = db.execute("""SELECT month FROM tool_work_orders WHERE month NOT IN(SELECT month FROM tool_work_archives)
                GROUP BY month HAVING SUM(submitted_at IS NULL AND outcome<>'cancelled')=0 ORDER BY month LIMIT 1""").fetchone()
            if not ready:
                return
            month = ready[0]
        signature_sql = 'SELECT id,revision FROM tool_work_orders WHERE month=? ORDER BY scope,id'
        with NamedTemporaryFile(dir=self.archive_dir, suffix='.zip.tmp', delete=False) as temporary:
            path = self.archive_dir / os.path.basename(temporary.name)
        try:
            with self.connect() as db:
                signature = db.execute(signature_sql, (month,)).fetchall()
                rows = db.execute('SELECT * FROM tool_work_orders WHERE month=? ORDER BY scope,id', (month,))
                self.package_work(db, month, rows, path)
            with self.connect(True) as db:
                actual = db.execute(signature_sql, (month,)).fetchall()
                pending = db.execute("""SELECT 1 FROM tool_work_orders WHERE month=?
                    AND submitted_at IS NULL AND outcome<>'cancelled' LIMIT 1""", (month,)).fetchone()
                if actual == signature and not pending and not db.execute(
                        'SELECT 1 FROM tool_work_archives WHERE month=?', (month,)).fetchone():
                    os.replace(path, self.archive_dir / f'{month}.zip')
                    db.execute('INSERT INTO tool_work_archives VALUES(?,?)', (month, clock().isoformat(timespec='seconds')))
        finally:
            path.unlink(missing_ok=True)

    def download(self, actor, month='', identity=''):
        self.access(actor)
        with self.connect() as db:
            if identity:
                row = self._work(db, actor, identity)
                if not row['submitted_at']:
                    raise RiskError('工单尚未提交。', 409)
                output = io.BytesIO()
                self.package_work(db, row['month'], [row], output)
                return output.getvalue()
            if not self.administrator(actor) or not all(can_scope(actor, scope) for scope in BUILDINGS):
                raise RiskError('当前账号无权下载跨楼栋归档。', 403)
            row = db.execute('SELECT 1 FROM tool_work_archives WHERE month=?', (month_key(month),)).fetchone()
            if not row:
                raise RiskError('当月归档尚未生成。', 409)
            archive = self.archive_dir / f'{month}.zip'
            if not archive.is_file():
                raise RiskError('本地归档文件缺失，请联系管理员。', 503)
            return archive
