"""Monthly building tool checks and immutable local archive packages."""

import datetime as dt
import hashlib
import io
import json
import re
from pathlib import Path
from zipfile import BadZipFile, ZIP_DEFLATED, ZipFile

from openpyxl import load_workbook
from openpyxl.cell.cell import MergedCell
from openpyxl.utils.exceptions import InvalidFileException

from .risk_inspection import MODULE, RiskError, RiskInspections, can_scope, text


BUILDINGS = tuple('ABCDEH')
TZ = dt.timezone(dt.timedelta(hours=8))
TEMPLATES = Path(__file__).resolve().parents[1] / 'resources' / 'tool_self_checks'
MAX_FILE = 2 * 1024 * 1024
MAX_RECORDS = 20
RESULTS = ('是', '否')


def dumps(data):
    return json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def month_key(value):
    if not isinstance(value, str) or not re.fullmatch(r'20\d\d-(?:0[1-9]|1[0-2])', value):
        raise RiskError('请选择有效的工单月份。')
    return value


def timestamp(value):
    value = text(value, 16)
    if value:
        try:
            if dt.datetime.fromisoformat(value).isoformat(timespec='minutes') != value:
                raise ValueError
        except ValueError:
            raise RiskError('检查和审核时间应填写到分钟。') from None
    return value


def parse_template(data):
    """Accept the supplied record layout with a variable number of check rows."""
    if not data or len(data) > MAX_FILE:
        raise RiskError('请选择 2 MB 以内的 XLSX 自检记录单。')
    try:
        with ZipFile(io.BytesIO(data)) as package:
            parts = package.infolist()
            if len(parts) > 200 or sum(part.file_size for part in parts) > 16 * 1024 * 1024:
                raise RiskError('模板解压后的内容过大。')
            if any(part.filename.startswith(('xl/externalLinks/', 'xl/embeddings/'))
                   or part.filename.endswith('vbaProject.bin') for part in parts):
                raise RiskError('模板不能包含外部工作簿、嵌入对象或宏。')
        book = load_workbook(io.BytesIO(data), keep_links=False)
        try:
            sheet = book.active
            if len(book.worksheets) != 1 or not 15 <= sheet.max_row <= 113 or not 9 <= sheet.max_column <= 24:
                raise RiskError('请选择与现有自检记录单布局一致的单工作表。')
            cells = list(sheet.iter_rows())
            if any(cell.data_type == 'f' for row in cells for cell in row):
                raise RiskError('自检模板的填写区不能包含公式。')

            def value(row, col):
                return str(cells[row - 1][col - 1].value or '').strip()

            headers = [value(10, col) for col in (3, 5, 7, 9)]
            if headers != ['自检项目', '操作方法', '合格标准', '完成情况']:
                raise RiskError('模板第 10 行缺少自检项目、操作方法、合格标准或完成情况。')
            checks = []
            for row in range(11, sheet.max_row + 1):
                if value(row, 2) == '结果审核':
                    break
                name, method, standard = (value(row, col) for col in (3, 5, 7))
                if not all((name, method, standard)) or not re.fullmatch(r'是\s*\[\s*\]\s*否\s*\[\s*\]', value(row, 9)):
                    raise RiskError(f'模板第 {row} 行的检查项目或是／否栏不完整。')
                checks.append({'row': row, 'name': text(name, 300, True), 'method': text(method, 1500, True),
                               'standard': text(standard, 1500, True)})
            if (not checks or row + 2 > sheet.max_row or value(row, 2) != '结果审核'
                    or value(row + 1, 2) != '完成情况' or not value(row + 2, 2).startswith('审核人')):
                raise RiskError('模板缺少检查事项或结果审核区。')
            inputs = ['C3', 'G4', 'C6', 'D8', 'G8', f'E{row + 1}', f'E{row + 2}', f'G{row + 2}']
            if any(isinstance(sheet[address], MergedCell) for address in inputs):
                raise RiskError('模板的合并单元格覆盖了填写位置。')
            return {'description': value(7, 3), 'specialty': value(3, 7), 'cycle': value(4, 3),
                    'applicable': value(5, 3), 'precautions': value(6, 3),
                    'checks': checks, 'result_row': row + 1, 'review_row': row + 2}
        finally:
            book.close()
    except RiskError:
        raise
    except (BadZipFile, InvalidFileException, ValueError, TypeError, KeyError, OSError, AttributeError, SyntaxError):
        raise RiskError('XLSX 模板无法读取，请核对文件后重试。') from None


def fill_template(content, schema, record, scope):
    book = load_workbook(io.BytesIO(content), keep_links=False)
    try:
        sheet = book.active
        review = schema['review_row']
        fields = {'C3': scope + '楼', 'G4': record['inspector'], 'D8': record['location'],
                  'G8': record['checked_at'].replace('T', ' '),
                  f'E{schema["result_row"]}': '已完成' if all(check == '是' for check in record['checks']) else '未完成'}
        if record['notes']:
            fields['C6'] = '\n'.join(filter(None, (schema['precautions'], record['notes'])))
        if record['reviewer']:
            fields[f'E{review}'] = record['reviewer']
        if record['reviewed_at']:
            fields[f'G{review}'] = record['reviewed_at'].replace('T', ' ')
        for item, answer in zip(schema['checks'], record['checks'], strict=True):
            fields[f'I{item["row"]}'] = '是[√]  否[ ]' if answer == '是' else '是[ ]  否[√]'
        for address, value in fields.items():
            sheet[address] = value
            sheet[address].data_type = 's'
        output = io.BytesIO()
        book.save(output)
        return output.getvalue()
    finally:
        book.close()


class ToolSelfChecks:
    def __init__(self, inspection, templates=TEMPLATES):
        self.connect = inspection.connect
        with self.connect(True) as db:
            db.executescript('''
                CREATE TABLE IF NOT EXISTS tool_check_templates(
                    id TEXT NOT NULL, version INTEGER NOT NULL, name TEXT NOT NULL, active INTEGER NOT NULL,
                    content BLOB NOT NULL, schema TEXT NOT NULL, PRIMARY KEY(id,version));
                CREATE TABLE IF NOT EXISTS tool_check_orders(
                    month TEXT NOT NULL, scope TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0,
                    owner TEXT NOT NULL DEFAULT '', owner_name TEXT NOT NULL DEFAULT '', records TEXT NOT NULL DEFAULT '[]',
                    submitted_at TEXT, PRIMARY KEY(month,scope));
                CREATE TABLE IF NOT EXISTS tool_check_audit(
                    id INTEGER PRIMARY KEY, month TEXT NOT NULL, scope TEXT NOT NULL,
                    action TEXT NOT NULL, actor TEXT NOT NULL, at TEXT NOT NULL);
                CREATE TABLE IF NOT EXISTS tool_check_archives(
                    month TEXT PRIMARY KEY, created_at TEXT NOT NULL, content BLOB NOT NULL);
            ''')
            if not db.execute('SELECT 1 FROM tool_check_templates LIMIT 1').fetchone():
                seeds = [(path, path.read_bytes()) for path in sorted(templates.glob('*.xlsx'))]
                if not seeds:
                    raise RiskError('工具自检模板资源未安装。', 503)
                db.executemany('INSERT OR IGNORE INTO tool_check_templates VALUES(?,1,?,1,?,?)', [
                    (hashlib.sha256(path.name.encode()).hexdigest()[:24], path.stem, content, dumps(parse_template(content)))
                    for path, content in seeds])

    @staticmethod
    def access(actor):
        RiskInspections.check(actor)
        if MODULE not in actor.get('modules', ()):
            raise RiskError('当前账号没有工具自检权限。', 403)

    @staticmethod
    def administrator(actor):
        return actor.get('level') in {'L0', 'L1'}

    @staticmethod
    def leader(actor, scope):
        return (actor.get('home_scope') == scope and can_scope(actor, scope)
                and (actor.get('category') == 'supervisor' or actor.get('level') == 'L3'
                     or actor.get('category') == 'other' and '楼长' in (actor.get('position') or '')))

    @classmethod
    def available_to(cls, actor):
        return any(can_scope(actor, scope) and (cls.administrator(actor) or cls.leader(actor, scope))
                   for scope in BUILDINGS)

    def allowed_order(self, actor, scope):
        self.access(actor)
        if scope not in BUILDINGS or not can_scope(actor, scope) or not (self.administrator(actor) or self.leader(actor, scope)):
            raise RiskError('仅本楼楼长和有权限的管理员可查看工单。', 403)

    def issue(self, current=None):
        current = (current or dt.datetime.now(TZ)).astimezone(TZ)
        month = current.strftime('%Y-%m')
        with self.connect() as db:
            latest = db.execute('SELECT MAX(month) FROM tool_check_orders').fetchone()[0]
        if latest and latest >= month:
            return month
        period = latest or month
        rows = []
        while period <= month:
            rows.extend((period, scope) for scope in BUILDINGS)
            year, number = map(int, period.split('-'))
            period = f'{year + (number == 12):04d}-{number % 12 + 1:02d}'
        with self.connect(True) as db:
            db.executemany('INSERT OR IGNORE INTO tool_check_orders(month,scope) VALUES(?,?)', rows)
        return month

    @staticmethod
    def _template(db, identity, version=None):
        sql = 'SELECT * FROM tool_check_templates WHERE id=?'
        row = (db.execute(sql + ' ORDER BY version DESC LIMIT 1', (identity,)).fetchone() if version is None
               else db.execute(sql + ' AND version=?', (identity, version)).fetchone())
        if not row:
            raise RiskError('模板版本不存在，请刷新后重试。', 404)
        return row

    @staticmethod
    def template_info(row):
        return {'id': row['id'], 'version': row['version'], 'name': row['name'], 'active': bool(row['active']),
                'schema': json.loads(row['schema'])}

    def save_template(self, actor, name, content, identity='', version=None):
        self.access(actor)
        if not self.administrator(actor):
            raise RiskError('仅管理员可维护工具自检模板。', 403)
        name = text(name, 100, True)
        schema = parse_template(content)
        with self.connect(True) as db:
            if identity:
                old = self._template(db, identity)
                if old['version'] != version:
                    raise RiskError('模板已更新，请刷新后重试。', 409)
                next_version = version + 1
            else:
                identity = hashlib.sha256(name.encode() + b'\x00' + content).hexdigest()[:24]
                existing = db.execute('SELECT 1 FROM tool_check_templates WHERE id=?', (identity,)).fetchone()
                if existing:
                    raise RiskError('相同模板已导入。', 409)
                next_version = 1
            db.execute('INSERT INTO tool_check_templates VALUES(?,?,?,1,?,?)',
                       (identity, next_version, name, content, dumps(schema)))
            return self.template_info(self._template(db, identity, next_version))

    def availability(self, actor, identity, body):
        self.access(actor)
        if not self.administrator(actor):
            raise RiskError('仅管理员可维护工具自检模板。', 403)
        if type(body.get('active')) is not bool:
            raise RiskError('模板启用状态无效。')
        with self.connect(True) as db:
            row = self._template(db, identity)
            if row['version'] != body.get('version'):
                raise RiskError('模板已更新，请刷新后重试。', 409)
            db.execute('UPDATE tool_check_templates SET active=? WHERE id=? AND version=?',
                       (body['active'], identity, row['version']))

    @staticmethod
    def _order(db, month, scope):
        row = db.execute('SELECT * FROM tool_check_orders WHERE month=? AND scope=?', (month, scope)).fetchone()
        if not row:
            raise RiskError('该月份暂无此楼工单。', 404)
        return row

    def order_info(self, actor, row):
        submitted = bool(row['submitted_at'])
        lead = self.leader(actor, row['scope'])
        return {key: row[key] for key in ('month', 'scope', 'revision', 'owner_name', 'submitted_at')} | {
            'status': 'submitted' if submitted else 'pending',
            'can_claim': not submitted and not row['owner'] and lead,
            'can_edit': not submitted and row['owner'] == actor['id'] and lead,
            'can_release': not submitted and bool(row['owner']) and self.administrator(actor),
        }

    def list(self, actor, month=''):
        self.access(actor)
        if not self.available_to(actor):
            raise RiskError('仅楼长和有权限的管理员可使用工具自检。', 403)
        current = self.issue()
        month = month_key(month or current)
        admin = self.administrator(actor)
        scopes = [scope for scope in BUILDINGS if can_scope(actor, scope) and (admin or self.leader(actor, scope))]
        with self.connect() as db:
            orders = [self.order_info(actor, row) for row in db.execute(
                'SELECT * FROM tool_check_orders WHERE month=? ORDER BY scope', (month,)) if row['scope'] in scopes]
            templates = [self.template_info(row) for row in db.execute('''SELECT t.id,t.version,t.name,t.active,t.schema
                FROM tool_check_templates t WHERE version=(SELECT MAX(version) FROM tool_check_templates WHERE id=t.id)
                ORDER BY name''') if admin or row['active']]
            archive = db.execute('SELECT created_at FROM tool_check_archives WHERE month=?', (month,)).fetchone()
        return {'month': month, 'current_month': current, 'orders': orders, 'templates': templates,
                'can_manage_templates': admin, 'can_download_month': admin and set(scopes) == set(BUILDINGS),
                'archived_at': archive[0] if archive and admin and set(scopes) == set(BUILDINGS) else None,
                'actor_name': actor['name']}

    def detail(self, actor, month, scope):
        self.allowed_order(actor, scope)
        with self.connect() as db:
            row = self._order(db, month_key(month), scope)
            records = json.loads(row['records'])
            return {**self.order_info(actor, row), 'records': [
                {**record, 'template': self.template_info(self._template(db, record['template_id'], record['version']))}
                for record in records], 'actor_name': actor['name']}

    def _records(self, db, payload, saved, *, submit):
        if not isinstance(payload, list) or len(payload) > MAX_RECORDS or submit and not payload:
            raise RiskError('请选用至少一份模板，单张工单最多 20 份自检记录。')
        previous = {record['entry_id']: record for record in saved}
        records, seen = [], set()
        for entry in payload:
            if not isinstance(entry, dict):
                raise RiskError('自检记录格式无效。')
            entry_id = entry.get('entry_id')
            if not isinstance(entry_id, str) or not re.fullmatch(r'[0-9a-f]{32}', entry_id) or entry_id in seen:
                raise RiskError('自检记录编号无效或重复。')
            seen.add(entry_id)
            identity, version = entry.get('template_id'), entry.get('version')
            if not isinstance(identity, str) or type(version) is not int:
                raise RiskError('请选择有效的模板版本。')
            old = previous.get(entry_id, {})
            pinned = (old.get('template_id'), old.get('version')) == (identity, version)
            template = self._template(db, identity, version if pinned else None)
            if not pinned and (not template['active'] or template['version'] != version):
                raise RiskError('新选模板已更新或停用，请刷新模板后重新选用。', 409)
            checks = entry.get('checks')
            if (not isinstance(checks, list) or len(checks) != len(json.loads(template['schema'])['checks'])
                    or any(answer not in (*RESULTS, '') for answer in checks) or submit and '' in checks):
                raise RiskError(f'请逐项填写「{template["name"]}」的完成情况。')
            record = {'entry_id': entry_id, 'template_id': identity, 'version': version, 'checks': checks,
                      'location': text(entry.get('location', ''), 240, submit),
                      'inspector': text(entry.get('inspector', ''), 80, submit),
                      'notes': text(entry.get('notes', ''), 1500), 'reviewer': text(entry.get('reviewer', ''), 80),
                      'checked_at': timestamp(entry.get('checked_at', '')),
                      'reviewed_at': timestamp(entry.get('reviewed_at', ''))}
            if submit and not record['checked_at']:
                raise RiskError(f'请填写「{template["name"]}」的检查开始时间。')
            records.append(record)
        return records

    def action(self, actor, month, scope, action, body):
        self.allowed_order(actor, scope)
        month_key(month)
        if action not in {'claim', 'draft', 'submit', 'release'}:
            raise RiskError('未知的工单操作。', 404)
        if action == 'release' and not self.administrator(actor) or action != 'release' and not self.leader(actor, scope):
            raise RiskError('仅本楼楼长可填写提交，管理员可释放未提交的工单。', 403)
        with self.connect(True) as db:
            row = self._order(db, month, scope)
            owner = row['owner']
            if action in {'draft', 'submit'} and owner != actor['id']:
                raise RiskError('请先由本人领取这张工单。', 409)
            same_claim = action == 'claim' and owner == actor['id'] and not row['submitted_at']
            if same_claim:
                return self.order_info(actor, row)
            saved = json.loads(row['records'])
            records = self._records(db, body.get('records'), saved, submit=action == 'submit') if action in {'draft', 'submit'} else saved
            if row['submitted_at']:
                if action == 'submit' and records == saved:
                    return self.order_info(actor, row)
                raise RiskError('工单已经提交。', 409)
            if action == 'draft' and records == saved:
                return self.order_info(actor, row)
            if type(body.get('revision')) is not int or body['revision'] != row['revision']:
                raise RiskError('工单已变化，请重新打开核对后再操作。', 409)
            if action == 'claim' and owner:
                raise RiskError('工单已由本楼其他楼长领取。', 409)
            when = dt.datetime.now(TZ).isoformat(timespec='seconds')
            owner, name = {'claim': (actor['id'], actor['name']), 'release': ('', '')}.get(action, (owner, row['owner_name']))
            db.execute('''UPDATE tool_check_orders SET owner=?,owner_name=?,records=?,submitted_at=?,revision=revision+1
                          WHERE month=? AND scope=?''', (owner, name, dumps(records), when if action == 'submit' else None, month, scope))
            db.execute('INSERT INTO tool_check_audit(month,scope,action,actor,at) VALUES(?,?,?,?,?)',
                       (month, scope, action, actor['name'], when))
            return self.order_info(actor, self._order(db, month, scope))

    def package(self, month, orders):
        output = io.BytesIO()
        manifest = []
        with ZipFile(output, 'w', ZIP_DEFLATED) as archive, self.connect() as db:
            for order in orders:
                scope = order['scope']
                files = []
                for index, record in enumerate(json.loads(order['records']), 1):
                    template = self._template(db, record['template_id'], record['version'])
                    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', '_', template['name']).strip('. ') or '工具自检'
                    file = f'{scope}楼/{index:02d}-{name}-V{template["version"]}.xlsx'
                    archive.writestr(file, fill_template(template['content'], json.loads(template['schema']), record, scope))
                    files.append({'file': file, 'template_id': template['id'], 'version': template['version'],
                                  'template_sha256': hashlib.sha256(template['content']).hexdigest()})
                manifest.append({'scope': scope, 'submitted_by': order['owner_name'], 'submitted_at': order['submitted_at'], 'files': files})
            archive.writestr('归档清单.json', dumps({'month': month, 'orders': manifest}).encode('utf-8'))
        return output.getvalue()

    def tick(self, current=None):
        self.issue(current)
        with self.connect() as db:
            ready = db.execute('''SELECT month FROM tool_check_orders WHERE month NOT IN (SELECT month FROM tool_check_archives)
                GROUP BY month HAVING COUNT(*)=? AND COUNT(submitted_at)=COUNT(*) ORDER BY month LIMIT 1''', (len(BUILDINGS),)).fetchone()
            if not ready:
                return
            month = ready[0]
            orders = db.execute('SELECT * FROM tool_check_orders WHERE month=? ORDER BY scope', (month,)).fetchall()
        content = self.package(month, orders)
        with self.connect(True) as db:
            db.execute('INSERT OR IGNORE INTO tool_check_archives VALUES(?,?,?)',
                       (month, dt.datetime.now(TZ).isoformat(timespec='seconds'), content))

    def download(self, actor, month, scope=''):
        month_key(month)
        if scope:
            self.allowed_order(actor, scope)
            with self.connect() as db:
                row = self._order(db, month, scope)
            if not row['submitted_at']:
                raise RiskError('工单尚未提交。', 409)
            return self.package(month, [row])
        self.access(actor)
        if not self.administrator(actor) or not all(can_scope(actor, building) for building in BUILDINGS):
            raise RiskError('当前账号无权下载跨楼栋归档。', 403)
        with self.connect() as db:
            row = db.execute('SELECT content FROM tool_check_archives WHERE month=?', (month,)).fetchone()
            if not row:
                raise RiskError('当月工单尚未全部提交或归档正在处理。', 409)
            return row[0]
