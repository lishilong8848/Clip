# -*- coding: utf-8 -*-
"""
收敛规则集模块
- 规则集(rule_set) 与 规则集项(rule_set_item) 的增删改查
- 匹配逻辑：智航「计划收敛」记录 与 规则集 比对，判断规则集是否覆盖记录
- 设备范围匹配使用本地 zh_device；逐设备测点详情另存 points.sqlite3。
"""
import re
import sqlite3
import hashlib
import json
from contextlib import closing, contextmanager
from contextvars import ContextVar
from pathlib import Path
from upload_event_module.utils import get_data_file_path

DATA_DIR = Path(get_data_file_path('plan_convergence'))
RULE_DB = DATA_DIR / 'rule_sets.sqlite3'
CATALOG_DB = DATA_DIR / 'catalog.sqlite3'
_lookups = ContextVar('plan_convergence_lookups', default=None)


class RuleConflictError(ValueError):
    pass

# 实例名 / 规则名可能用逗号、顿号、分号、换行分隔多个值
_SEP = re.compile(r'[,，、;；\r\n]+')

# 「全部规则」的各种写法
_ALL_RULES = ('全部', '全部规则', '全部告警规则', '全部设备', 'all')

# position 层级段（与 points_db 保持一致）
_POS_SEG_ZONE = 2
_POS_SEG_BUILDING = 3
_POS_SEG_FLOOR = 4
_POS_SEG_ROOM = 5


def _conn():
    RULE_DB.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(RULE_DB, timeout=5, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA foreign_keys=ON')
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS rule_set (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL UNIQUE,
            remark TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS rule_set_item (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            set_id INTEGER NOT NULL REFERENCES rule_set(id) ON DELETE CASCADE,
            scope_type TEXT, obj_name TEXT, zone TEXT, building TEXT,
            floor TEXT, room TEXT, inst_name TEXT, point_name TEXT,
            rule_name TEXT, alarm_config_id TEXT, rule_group_no INTEGER,
            rule_type TEXT, rule_label TEXT,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    ''')
    if 'ins_id' not in {row[1] for row in conn.execute('PRAGMA table_info(rule_set_item)')}:
        try:
            conn.execute('ALTER TABLE rule_set_item ADD COLUMN ins_id TEXT')
        except sqlite3.OperationalError:
            if 'ins_id' not in {row[1] for row in conn.execute('PRAGMA table_info(rule_set_item)')}:
                conn.close()
                raise
    if CATALOG_DB.is_file():
        conn.execute('ATTACH DATABASE ? AS catalog', (CATALOG_DB.resolve().as_uri() + '?mode=ro',))
    return conn


def _exec(sql, args=None, fetch=True):
    lookup = _lookups.get()
    if lookup is not None and fetch:
        conn, cache = lookup
        key = (sql, tuple(args or ()))
        if key not in cache:
            cache[key] = [dict(row) for row in conn.execute(sql.replace('%s', '?'), args or ())]
        return cache[key]
    conn = _conn()
    try:
        cursor = conn.execute(sql.replace('%s', '?'), args or ())
        if fetch:
            return [dict(row) for row in cursor.fetchall()]
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


@contextmanager
def _lookup_session():
    if _lookups.get() is not None:
        yield
        return
    with closing(_conn()) as conn:
        conn.execute('BEGIN')
        token = _lookups.set((conn, {}))
        try:
            yield
        finally:
            _lookups.reset(token)


def _version(name, remark, items):
    keys = ('scope_type', 'obj_name', 'zone', 'building', 'floor', 'room', 'inst_name',
            'point_name', 'rule_name', 'alarm_config_id', 'rule_group_no', 'rule_type', 'rule_label', 'ins_id')
    defaults = {'scope_type': 'device', 'rule_group_no': 1, 'rule_type': 'normal'}
    ordered = sorted(items, key=lambda item: str(item.get('rule_group_no', 1)))
    content = (name, remark, [tuple(item.get(key, defaults.get(key)) for key in keys) for item in ordered])
    return hashlib.sha256(json.dumps(content, ensure_ascii=False, sort_keys=True).encode()).hexdigest()


def _read_set(conn, set_id):
    row = conn.execute('SELECT * FROM rule_set WHERE id=?', (set_id,)).fetchone()
    if not row:
        return None
    value = dict(row)
    value['items'] = [dict(item) for item in conn.execute(
        'SELECT id,scope_type,obj_name,zone,building,floor,room,inst_name,point_name,rule_name,'
        'alarm_config_id,rule_group_no,rule_type,rule_label,ins_id FROM rule_set_item '
        'WHERE set_id=? ORDER BY rule_group_no,id', (set_id,))]
    value['version'] = _version(value['name'], value['remark'], value['items'])
    return value

# ==================== 规则集 CRUD ====================

def list_sets():
    """列出所有规则集，附带每套的项数和覆盖统计。"""
    rows = _exec("""
        SELECT s.id, s.name, s.remark, s.created_at, s.updated_at,
               (SELECT COUNT(*) FROM rule_set_item i WHERE i.set_id=s.id) AS item_count
        FROM rule_set s
        ORDER BY s.id DESC
    """)
    return rows


def get_set(set_id):
    """获取规则集详情（含所有项）。"""
    with closing(_conn()) as conn:
        conn.execute('BEGIN')
        return _read_set(conn, set_id)


def create_set(name, remark=''):
    if not isinstance(name, str) or not isinstance(remark, str) or len(name) > 120 or len(remark) > 500:
        raise ValueError('规则集名称或备注无效')
    name = (name or '').strip()
    if not name:
        raise ValueError('规则集名称不能为空')
    exists = _exec("SELECT id FROM rule_set WHERE name=%s", (name,))
    if exists:
        raise ValueError(f'规则集「{name}」已存在')
    return _exec("INSERT INTO rule_set (name, remark) VALUES (%s, %s)", (name, remark), fetch=False)


def update_set(set_id, name=None, remark=None):
    fields, args = [], []
    if name is not None:
        name = name.strip()
        if not name:
            raise ValueError('规则集名称不能为空')
        fields.append('name=%s')
        args.append(name)
    if remark is not None:
        fields.append('remark=%s')
        args.append(remark)
    if not fields:
        return
    args.append(set_id)
    _exec(f"UPDATE rule_set SET {', '.join(fields)} WHERE id=%s", tuple(args), fetch=False)


def delete_set(set_id):
    _exec("DELETE FROM rule_set WHERE id=%s", (set_id,), fetch=False)


# ==================== 规则集项 ====================

def add_items(set_id, items):
    """批量添加规则集项。items: [{scope_type, obj_name, zone, building, floor, room, inst_name, point_name, rule_name, alarm_config_id, rule_group_no, rule_type, rule_label}]"""
    rows = _item_rows(set_id, items)
    with closing(_conn()) as conn, conn:
        _insert_items(conn, rows)
    return len(rows)


def _item_rows(set_id, items):
    if not isinstance(items, list) or len(items) > 500 or any(not isinstance(it, dict) for it in items):
        raise ValueError('规则条目必须为不超过 500 项的对象列表')
    rows = []
    for it in items:
        for key in ('scope_type', 'obj_name', 'zone', 'building', 'floor', 'room', 'inst_name',
                    'point_name', 'rule_name', 'alarm_config_id', 'rule_type', 'rule_label', 'ins_id'):
            if it.get(key) is not None and not isinstance(it[key], str):
                raise ValueError('规则项字段须为文本：' + key)
        if it.get('scope_type', 'device') not in {
            'all', 'zone', 'building', 'floor', 'room', 'objtype', 'objtype_room',
            'device', 'point', 'exclude_device', 'exclude_point',
        }:
            raise ValueError('规则范围无效')
        if it.get('rule_type', 'normal') not in {'common', 'normal'}:
            raise ValueError('规则类型无效')
        group = it.get('rule_group_no', 1)
        if not isinstance(group, int) or isinstance(group, bool) or group < 1 or group > 500:
            raise ValueError('规则组号必须为 1-500')
        if any(isinstance(value, str) and len(value) > 1000 for value in it.values()):
            raise ValueError('规则项内容过长')
        rows.append((
            set_id,
            it.get('scope_type', 'device'),
            it.get('obj_name'),
            it.get('zone'),
            it.get('building'), it.get('floor'), it.get('room'),
            it.get('inst_name'), it.get('point_name'),
            it.get('rule_name'), it.get('alarm_config_id'),
            it.get('rule_group_no', 1),          # 规则组号（同组=同一次添加的组合）
            it.get('rule_type', 'normal'),        # common=公共规则 / normal=普通规则
            it.get('rule_label'),                 # 组/规则显示名（前端 draft.label）
            it.get('ins_id'),
        ))
    return rows


def _insert_items(conn, rows):
    conn.executemany(
        """INSERT INTO rule_set_item
           (set_id, scope_type, obj_name, zone, building, floor, room,
            inst_name, point_name, rule_name, alarm_config_id,
            rule_group_no, rule_type, rule_label, ins_id)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", rows)


def clear_items(set_id):
    _exec("DELETE FROM rule_set_item WHERE set_id=%s", (set_id,), fetch=False)


def replace_items(set_id, items):
    """替换规则集的全部项（先清空再批量插入）。"""
    rows = _item_rows(set_id, items)
    with closing(_conn()) as conn, conn:
        conn.execute("DELETE FROM rule_set_item WHERE set_id=?", (set_id,))
        _insert_items(conn, rows)
    return len(rows)


def save_set(set_id, name, remark, items, *, expected_version=None):
    """Update metadata and items in one transaction."""
    if not isinstance(name, str):
        raise ValueError('规则集名称无效')
    name = name.strip()
    if not name or len(name) > 120:
        raise ValueError('规则集名称须为 1-120 字')
    if not isinstance(remark, str) or len(remark) > 500:
        raise ValueError('规则集备注过长')
    rows = _item_rows(set_id, items)
    with closing(_conn()) as conn, conn:
        conn.execute('BEGIN IMMEDIATE')
        current = _read_set(conn, set_id)
        if not current:
            raise ValueError('规则集不存在')
        if expected_version is not None and expected_version != current['version']:
            if current['version'] == _version(name, remark, items):
                return current
            raise RuleConflictError('规则集已被其他人修改，当前填写已保留，请重新读取后核对再保存。')
        conn.execute('UPDATE rule_set SET name=?, remark=?, updated_at=CURRENT_TIMESTAMP WHERE id=?',
                     (name, remark, set_id))
        conn.execute('DELETE FROM rule_set_item WHERE set_id=?', (set_id,))
        _insert_items(conn, rows)
        return _read_set(conn, set_id)


# ==================== 匹配逻辑（新底表：设备实例级） ====================

_BUILDING_ALIAS = {
    'A': '南通数据中心A', 'B': '南通数据中心B', 'C': '南通数据中心C',
    'D': '南通基地数据中心_D', 'E': '南通基地数据中心_E',
}


def _norm_building(name):
    """旧底表楼栋短名（A/B/C/D/E）→ 新底表 position 中的楼栋全名。"""
    if not name:
        return name
    return _BUILDING_ALIAS.get(name.strip(), name)


def _pos_like(zone='', building='', floor='', room=''):
    """按 区/楼/层/房 逐级构造 position 匹配条件（元组返回）。
    position 形如 全国/华东一区/HD8/南通数据中心B/B-F2/[房间[/子级]]：
      段0=国家 段1=大区 段2=园区 段3=楼栋 段4=楼层 段5+=房间(+子级)
    楼栋短名 A/B/C/D/E 自动映射为全名（兼容旧规则集条目）。
    返回 (SQL条件片段, 参数列表)：
      - 段链为空 → ('1=1', [])
      - 只到楼层(无房间) → position LIKE '%/<链>/%'（楼层后必有房间）
      - 含房间 → position = '%/<链>' OR position LIKE '%/<链>/%'（房间可能是最后一段，也可能带子级）
    """
    building = _norm_building(building)
    segs = [s for s in (zone, building, floor, room) if s]
    if not segs:
        return '1=1', []
    chain = '/'.join(segs)
    if not room:
        return "`position` LIKE %s", (f'%/{chain}/%',)
    # 房间可能是最后一段（position 尾部正好是该房间），也可能带子级
    return "(`position` LIKE %s OR `position` LIKE %s)", (f'%/{chain}', f'%/{chain}/%')


def _device_by_name(inst_name, ins_id=None):
    """Prefer stable identity; a name must resolve to one device."""
    if ins_id:
        rows = _exec('SELECT inst_name,ins_id,ins_standard_id,obj_name,position FROM zh_device WHERE ins_id=%s LIMIT 1', (ins_id,))
        return rows[0] if rows else None
    rows = _exec("""
        SELECT inst_name, ins_id, ins_standard_id, obj_name, `position`
        FROM zh_device WHERE inst_name = %s LIMIT 2
    """, (inst_name,))
    if not rows:
        rows = _exec("""
            SELECT inst_name, ins_id, ins_standard_id, obj_name, `position`
            FROM zh_device WHERE inst_name LIKE %s ESCAPE '\\' LIMIT 2
        """, ('%' + str(inst_name).replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%',))
    if len(rows) > 1:
        raise ValueError(f'设备「{inst_name}」匹配到多条目录记录，请重新选择具体设备。')
    return rows[0] if rows else None


def _expand_item(it):
    included, excludes = _expand_item_devices(it)
    if not included and not excludes:
        label = it.get('inst_name') or it.get('room') or it.get('building') or it.get('obj_name') or it.get('scope_type')
        raise ValueError(f'规则条目「{label}」未找到设备，请核对目录和配置后重新核对。')
    return included, excludes


def _expand_item_devices(it):
    """
    把一条规则集项展开为设备实例集合。
    scope_type 分支与 expand_set_items 原文案一致：
      zone/building/floor/room/objtype/objtype_room/device/point/all/exclude_device/exclude_point
    返回 (included:set, excludes:set)：
      - exclude_device/exclude_point 只产出 excludes（组内排除，不跨组生效）
      - 其余类型只产出 included
    """
    included, excludes = set(), set()
    st = it['scope_type']
    required = {'zone': 'zone', 'building': 'building', 'floor': 'floor', 'room': 'room', 'objtype': 'obj_name', 'objtype_room': 'obj_name'}.get(st)
    if required and not str(it.get(required) or '').strip():
        raise ValueError('规则范围缺少必要条件：' + required)
    if st == 'objtype_room' and not any(it.get(key) for key in ('zone', 'building', 'floor', 'room')):
        raise ValueError('设备类型与空间规则缺少空间条件')
    if st == 'exclude_device' or st == 'exclude_point':
        if it.get('inst_name') or it.get('ins_id'):
            d = _device_by_name(it.get('inst_name'), it.get('ins_id'))
            if d and d['ins_id']:
                excludes.add(d['ins_id'])
        return included, excludes
    if st == 'all':
        rows = _exec("SELECT ins_id FROM zh_device WHERE ins_id<>''")
        included.update(r['ins_id'] for r in rows)
        return included, excludes
    if st == 'objtype_room':
        cond_sql, cond_args = _pos_like(
            it.get('zone') or '', it.get('building') or '',
            it.get('floor') or '', it.get('room') or '')
        rows = _exec(f"""
            SELECT ins_id FROM zh_device
            WHERE obj_name = %s AND ({cond_sql})
        """, (it.get('obj_name'),) + tuple(cond_args))
        included.update(r['ins_id'] for r in rows)
        return included, excludes
    if st == 'objtype':
        rows = _exec("SELECT ins_id FROM zh_device WHERE obj_name=%s AND ins_id<>''",
                     (it.get('obj_name'),))
        included.update(r['ins_id'] for r in rows)
        return included, excludes
    if st == 'zone':
        cond_sql, cond_args = _pos_like(it.get('zone') or '')
        rows = _exec(f"SELECT ins_id FROM zh_device WHERE {cond_sql} AND ins_id<>''",
                     tuple(cond_args))
        included.update(r['ins_id'] for r in rows)
        return included, excludes
    if st == 'building':
        cond_sql, cond_args = _pos_like('', it.get('building') or '')
        rows = _exec(f"SELECT ins_id FROM zh_device WHERE {cond_sql} AND ins_id<>''",
                     tuple(cond_args))
        included.update(r['ins_id'] for r in rows)
        return included, excludes
    if st == 'floor':
        cond_sql, cond_args = _pos_like('', it.get('building') or '', it.get('floor') or '')
        rows = _exec(f"SELECT ins_id FROM zh_device WHERE {cond_sql} AND ins_id<>''",
                     tuple(cond_args))
        included.update(r['ins_id'] for r in rows)
        return included, excludes
    if st == 'room':
        cond_sql, cond_args = _pos_like('', it.get('building') or '', it.get('floor') or '',
                                        it.get('room') or '')
        rows = _exec(f"SELECT ins_id FROM zh_device WHERE {cond_sql} AND ins_id<>''",
                     tuple(cond_args))
        included.update(r['ins_id'] for r in rows)
        return included, excludes
    if st in ('device', 'point'):
        if it.get('inst_name') or it.get('ins_id'):
            d = _device_by_name(it.get('inst_name'), it.get('ins_id'))
            if d and d['ins_id']:
                included.add(d['ins_id'])
            return included, excludes
        # point 无设备名：退化为类型/空间展开（规则名仅作标注，不参与设备集合匹配）
        if st == 'point':
            if it.get('obj_name'):
                rows = _exec("SELECT ins_id FROM zh_device WHERE obj_name=%s AND ins_id<>''",
                             (it.get('obj_name'),))
                included.update(r['ins_id'] for r in rows)
            elif any(it.get(k) for k in ('zone', 'building', 'floor', 'room')):
                cond_sql, cond_args = _pos_like(
                    it.get('zone') or '', it.get('building') or '',
                    it.get('floor') or '', it.get('room') or '')
                rows = _exec(f"SELECT ins_id FROM zh_device WHERE {cond_sql} AND ins_id<>''",
                             tuple(cond_args))
                included.update(r['ins_id'] for r in rows)
        return included, excludes
    return included, excludes


def expand_set_items(set_id):
    with _lookup_session():
        return _expand_set_items(set_id)


def _expand_set_items(set_id):
    """
    把规则集项展开为「设备实例集合」（新底表，按 ins_id 去重）。
    每项 scope_type：
      zone       -> 该园区（区）下所有设备
      building   -> 该楼栋下所有设备
      floor      -> 该楼层下所有设备
      room       -> 该房间下所有设备
      objtype    -> 该设备类型下所有设备
      objtype_room -> 设备类型 × 空间（zone/building/floor/room）下的设备
      device     -> 单台设备
      point      -> 单台设备（兼容：按设备名取该设备）
      all        -> 全部设备
      exclude_device -> 排除设备（跨组统一减除）
      exclude_point  -> 排除设备（兼容旧数据）
    返回 {ins_id, ...}
    """
    items = _exec("SELECT * FROM rule_set_item WHERE set_id=%s", (set_id,))
    included = set()
    excludes = set()
    for it in items:
        inc, exc = _expand_item(it)
        included |= inc
        excludes |= exc
    # 应用排除（全集展开：排除跨所有组生效，与旧版行为一致）
    return included - excludes


def _expand_space_devices(space_name, obj_name=''):
    """按智航 spaceModelName 展开设备集合（新底表）。
    space_name 可能是：房间(A-124.EA118) / 楼层(A-F1) / 楼栋(南通数据中心A) / 园区(HD8)。
    space_name 作为 position 的「独立段」匹配（避免 A-124.EA118X 误命中）：
      段在中间 → LIKE '%/<name>/%'；段在结尾 → LIKE '%/<name>'。
    返回 ins_id 集合。"""
    space_name = (space_name or '').strip()
    if not space_name:
        return set()
    space_name = _norm_building(space_name)
    args = [f'%/{space_name}/%', f'%/{space_name}']
    sql = ("SELECT ins_id FROM zh_device WHERE "
           "(`position` LIKE %s OR `position` LIKE %s) AND ins_id<>''")
    if obj_name:
        sql += " AND obj_name=%s"
        args.append(obj_name)
    rows = _exec(sql, tuple(args))
    return {r['ins_id'] for r in rows}


def expand_record(details):
    """
    把智航「计划收敛」记录明细展开为「设备实例集合」（新底表，按 ins_id）。
    details: [{instances(设备名), instanceIds(设备ID), relateConfig(规则名),
              classifyModel/classifyModelId(设备类型), spaceModelName(空间)}]
    规则名不影响设备集合——设备是匹配主体。
    返回 (设备集合, 结构化明细, 本地库中未找到的设备名列表)。
    """
    result = set()
    structured = []
    not_found = set()
    for d in details:
        inst_text = (d.get('instances') or '').strip()
        id_text = (d.get('instanceIds') or '').strip()
        rule_text = (d.get('relateConfig') or '').strip()
        if not inst_text and not id_text:
            continue
        # 「全部设备/规则」占位记录：instanceIds=all / instances=全部 / classifyModelId=all。
        # 不等于「忽略」——它通常绑定 spaceModelName（如 A-124.EA118 房间），表示该空间下全部设备。
        # 用 spaceModelName 展开空间设备（可配 classifyModel 类型限定）。
        is_all_placeholder = (
            id_text.lower() == 'all'
            or inst_text in ('全部', 'all')
            or (d.get('classifyModelId') or '').strip().lower() == 'all'
        )
        if is_all_placeholder:
            space_name = (d.get('spaceModelName') or '').strip() or (d.get('spaceModel') or '').strip()
            obj_name = (d.get('classifyModel') or '').strip()
            obj_name = '' if obj_name in ('全部', 'all') else obj_name
            found = _expand_space_devices(space_name, obj_name) if space_name else set()
            if not found:
                not_found.add(space_name or inst_text or id_text)
            result.update(found)
            structured.append({
                'device': f'{space_name or "全部"}（全部设备）' if found else (inst_text or id_text),
                'rule': rule_text or '全部规则',
                'device_count': len(found),
                'found_in_db': bool(found),
                'devices_found': sorted(found)[:20],
                'devices_not_found': [],
                'placeholder': True,
            })
            continue
        # 实例名与实例ID合并解析
        names = [s.strip() for s in _SEP.split(inst_text) if s.strip()]
        ids = [s.strip() for s in _SEP.split(id_text) if s.strip() and s.strip().lower() != 'all']
        found = set()
        for iid in ids:
            # ID 可能带 INSTANCE- 前缀，也可能纯数字（兼容）
            iid_clean = iid.removeprefix('INSTANCE-6-').removeprefix('INSTANCE-')
            rows = _exec("SELECT ins_id FROM zh_device WHERE ins_id=%s LIMIT 1", (iid,))
            if not rows and iid_clean != iid:
                rows = _exec("SELECT ins_id FROM zh_device WHERE ins_id=%s LIMIT 1", (iid_clean,))
            if rows:
                found.add(rows[0]['ins_id'])
            else:
                not_found.add(iid)
        for inst in names if not ids else []:
            d_dev = _device_by_name(inst)
            if d_dev and d_dev['ins_id']:
                found.add(d_dev['ins_id'])
            else:
                not_found.add(inst)
        result.update(found)
        structured.append({
            'device': inst_text or id_text,
            'rule': rule_text or '全部规则',
            'device_count': len(found),
            'found_in_db': bool(found),
            'devices_found': sorted(found)[:20],
            'devices_not_found': [i for i in (ids or names) if i in not_found],
        })
    return result, structured, sorted(not_found)


def _dev_info(ins_ids):
    """ins_id 集合 → 设备信息列表 [{ins_id, inst_name, obj_name, position}]（按名字排序）。"""
    ins_ids = set(ins_ids or ())
    if not ins_ids:
        return []
    ids = sorted(ins_ids)
    rows = []
    for start in range(0, len(ids), 900):
        batch = ids[start:start + 900]
        ph = ','.join(['%s'] * len(batch))
        rows.extend(_exec(f"SELECT ins_id, inst_name, obj_name, `position` FROM zh_device WHERE ins_id IN ({ph})", tuple(batch)))
    rows.sort(key=lambda item: str(item['inst_name']))
    return [{'ins_id': m['ins_id'], 'inst_name': m['inst_name'],
             'obj_name': m['obj_name'], 'position': m.get('position') or ''}
            for m in rows]


def match_record_to_set(set_id, details):
    with _lookup_session():
        return _match_record_to_set(set_id, details)


def _match_record_to_set(set_id, details):
    """
    匹配：规则集(配置)是否覆盖智航记录（设备实例级）。
    分组语义（公共规则 + 多条规则）：
      - 公共规则组(rule_type='common')：记录详情必须包含每一组的全部设备（缺一不可）
      - 普通规则组(rule_type='normal')：记录详情至少包含其中一组即可
      - 无公共组：至少一个普通组被包含即通过（n 组普通 = 旧逻辑的 n 选 1）
      - 无普通组：公共组全部被包含即通过（旧规则集迁移后即单组公共 → 与旧「全集包含」等价）
    返回 {passed, set_device_count, record_device_count, missing_count, extra_count,
          missing:[{inst_name, ins_id}], not_found_devices:[...], structured,
          group_count, common:{passed, groups:[...]}, rules:{passed, matched_group, groups:[...]}}
    """
    record_devices, structured, not_found_devices = expand_record(details)
    items = _exec("SELECT * FROM rule_set_item WHERE set_id=%s ORDER BY rule_group_no, id", (set_id,))

    # 1) 按规则组聚合（rule_group_no 相同 = 同一次添加的组合）
    groups = {}  # group_no -> {type, label, included:set, excludes:set}
    for it in items:
        g = groups.setdefault(it['rule_group_no'], {
            'type': it['rule_type'] or 'common',
            'label': it['rule_label'] or '',
            'included': set(), 'excludes': set(),
        })
        inc, exc = _expand_item(it)
        g['included'] |= inc
        g['excludes'] |= exc

    common_groups = []  # [{label, group_no, devices}]
    normal_groups = []
    set_devices = set()
    for no in sorted(groups):
        g = groups[no]
        devices = g['included'] - g['excludes']      # 组内排除（不跨组生效）
        set_devices |= devices
        entry = {'label': g['label'], 'group_no': no, 'devices': devices}
        (common_groups if g['type'] == 'common' else normal_groups).append(entry)

    # 2) 逐组判定（组间设备重叠不全局去重：每组独立求差，缺失即在本组列出）
    def _group_detail(g):
        missing = g['devices'] - record_devices
        return {
            'label': g['label'] or f'规则组{g["group_no"]}',
            'group_no': g['group_no'],
            'matched': bool(g['devices']) and not missing,
            'device_count': len(g['devices']),
            'missing_count': len(missing),
            'missing': _dev_info(missing),
        }

    common_results = [_group_detail(g) for g in common_groups]
    normal_results = [_group_detail(g) for g in normal_groups]
    common_passed = all(g['matched'] for g in common_results) if common_results else True
    normal_passed = any(g['matched'] for g in normal_results) if normal_results else True
    matched_group = next((g['group_no'] for g in normal_results if g['matched']), None)

    # 3) 总判定（记录详情须包含公共规则全部 + 至少一条普通规则；无普通组时只看公共组）
    if not groups:
        passed = False
    else:
        passed = common_passed and (normal_passed if normal_groups else True) and not not_found_devices

    # 兼容旧字段：全集中缺的台数与清单（供旧前端/缓存兜底）
    missing_all = set_devices - record_devices
    extra = record_devices - set_devices
    placeholder_count = sum(1 for s in structured if s.get('placeholder'))
    return {
        'passed': passed,
        'set_device_count': len(set_devices),
        'record_device_count': len(record_devices),
        'placeholder_count': placeholder_count,
        'missing_count': len(missing_all),
        'extra_count': len(extra),
        'missing': _dev_info(missing_all),
        'not_found_devices': not_found_devices,
        'structured': structured,
        # ---- 新增：分组明细 ----
        'group_count': len(groups),
        'common': {'passed': common_passed, 'groups': common_results},
        'rules': {'passed': normal_passed, 'matched_group': matched_group, 'groups': normal_results},
    }
