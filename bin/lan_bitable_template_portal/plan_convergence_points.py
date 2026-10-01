# -*- coding: utf-8 -*-
"""Read-only SQLite port of the legacy ``points_db`` lookup functions.

The original implementation (C:\\Users\\l1773\\WorkBuddy\\拉取智航收敛规则\\points_db.py)
read from a local MySQL database. This module ports the POINT/DEVICE lookup
functions used by the plan-convergence portal against two local read-only
SQLite files placed under :func:`get_data_file_path('plan_convergence')`:

* ``catalog.sqlite3``  — tables ``zh_device``, ``zh_rules``
* ``points.sqlite3``   — table ``point_detail``

All queries are parameterized, results are returned as ``list[dict]`` using the
same JSON shapes as the source, connections are opened read-only, and missing
files raise a clear :class:`FileNotFoundError`.
"""

import sqlite3
from contextlib import closing
from pathlib import Path

from upload_event_module.utils import get_data_file_path

# ---------------------------------------------------------------------------
# Data-file locations (module-level so isolated tests can patch them).
# ---------------------------------------------------------------------------
DATA_DIR = Path(get_data_file_path('plan_convergence'))
CATALOG_PATH = DATA_DIR / 'catalog.sqlite3'
POINTS_PATH = DATA_DIR / 'points.sqlite3'

# position 格式: 全国/华东一区/HD8/南通数据中心B/B-F2/设备间-H楼2F
# 段0=国家 段1=大区 段2=园区(区) 段3=楼栋 段4=楼层 段5+=房间
_POS_SEG_ZONE = 2
_POS_SEG_BUILDING = 3
_POS_SEG_FLOOR = 4
_POS_SEG_ROOM = 5

_BUILDING_ALIAS = {
    'A': '南通数据中心A', 'B': '南通数据中心B', 'C': '南通数据中心C',
    'D': '南通基地数据中心_D', 'E': '南通基地数据中心_E',
}


def _catalog_conn():
    """Read-only connection to the catalog (zh_device / zh_rules)."""
    if not CATALOG_PATH.is_file():
        raise FileNotFoundError("本机尚未放置计划收敛审查目录库 (catalog.sqlite3)")
    conn = sqlite3.connect(CATALOG_PATH.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _points_conn():
    """Read-only connection to the per-device point detail database."""
    if not POINTS_PATH.is_file():
        raise FileNotFoundError("本机尚未放置逐设备测点详情文件 (points.sqlite3)")
    conn = sqlite3.connect(POINTS_PATH.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


def _catalog_points_conn():
    """Read-only catalog connection with the points DB attached as ``points``."""
    if not CATALOG_PATH.is_file():
        raise FileNotFoundError("本机尚未放置计划收敛审查目录库 (catalog.sqlite3)")
    if not POINTS_PATH.is_file():
        raise FileNotFoundError("本机尚未放置逐设备测点详情文件 (points.sqlite3)")
    conn = sqlite3.connect(CATALOG_PATH.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("ATTACH DATABASE ? AS points", (POINTS_PATH.resolve().as_uri() + "?mode=ro",))
    return conn


def _exec_catalog(sql, args=()):
    with closing(_catalog_conn()) as conn:
        return [dict(row) for row in conn.execute(sql, args).fetchall()]


def _exec_points(sql, args=()):
    with closing(_points_conn()) as conn:
        return [dict(row) for row in conn.execute(sql, args).fetchall()]


def _exec_catalog_points(sql, args=()):
    conn = _catalog_points_conn()
    try:
        return [dict(row) for row in conn.execute(sql, args).fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# position helpers (replicate the MySQL SUBSTRING_INDEX-based segmentation).
# ---------------------------------------------------------------------------
def _mysql_seg(position, n):
    """Replicate ``SUBSTRING_INDEX(SUBSTRING_INDEX(position,'/',n),'/',-1)``.

    ``n`` is the 1-based segment number used by the original SQL.
    Returns '' for empty / null positions.
    """
    if not position:
        return ''
    parts = (position or '').split('/')
    if n <= 0:
        return (position or '').strip()
    sub = parts[:n] if len(parts) >= n else parts
    return sub[-1].strip() if sub else ''


def _norm_building(name):
    """旧底表楼栋短名（A/B/C/D/E）→ 新底表 position 中的楼栋全名。"""
    if not name:
        return name
    return _BUILDING_ALIAS.get(name.strip(), name)


def _pos_match_sql(col, zone='', building='', floor='', room=''):
    """按 区/楼/层/房 逐级构造 position 精确匹配条件。

    返回 ``(SQL条件片段, 参数列表)``：
      - 只到楼层（无房间）→ ``col`` LIKE '%/<链>/%'
      - 含房间 → ``col`` = '%/<链>' OR ``col`` LIKE '%/<链>/%'
    """
    building = _norm_building(building)
    segs = [s for s in (zone, building, floor, room) if s]
    if not segs:
        return '1=1', []
    chain = '/'.join(segs)
    if not room:
        return f"`{col}` LIKE ?", (f'%/{chain}/%',)
    return f"(`{col}` LIKE ? OR `{col}` LIKE ?)", (f'%/{chain}', f'%/{chain}/%')


def _aggregate_positions(seg_n, like=None):
    """Aggregate zh_device by the ``seg_n`` (1-based) position segment.

    Returns ``{segment: {'devices': int, 'objs': set}}`` where ``devices`` is
    the sum of device counts and ``objs`` gathers distinct ``obj_name``.
    """
    sql = ("SELECT `position`, obj_name, COUNT(*) AS c FROM zh_device "
           "WHERE `position`<>''")
    args = []
    if like is not None:
        sql += " AND `position` LIKE ?"
        args.append(like)
    sql += " GROUP BY `position`, obj_name"
    rows = _exec_catalog(sql, tuple(args))
    agg = {}
    for r in rows:
        seg = _mysql_seg(r['position'], seg_n)
        item = agg.setdefault(seg, {'devices': 0, 'objs': set()})
        item['devices'] += r['c']
        item['objs'].add(r['obj_name'])
    return agg


def _rules_dedup_sql(where_sql, args, kw, limit):
    """构造「类型+告警名」去重后的规则列表查询（rule_desc 取该组合首条）。"""
    like = f'%{kw}%'
    sql = f"""
        SELECT MIN(alarm_config_id) AS alarm_config_id,
               alarm_name, MIN(classify_model_id) AS classify_model_id,
               classify_model, MIN(rule_desc) AS rule_desc
        FROM zh_rules
        {where_sql}
          AND (? = '' OR alarm_name LIKE ? OR rule_desc LIKE ?)
        GROUP BY classify_model, alarm_name
        ORDER BY classify_model, alarm_name
        LIMIT ?
    """
    return _exec_catalog(sql, tuple(args) + (kw, like, like, limit))


# ===========================================================================
# 点 / 设备 / 规则查询
# ===========================================================================

def points_by_inst_name(name, limit=500):
    """按智航实例名称（模糊匹配）查该设备的所有测点，返回实例列表 + 各自测点统计。"""
    like = f'%{name}%'
    base_sql = (
        "SELECT inst_id, inst_name, location, building, floor, room, "
        "       COUNT(*) AS points, SUM(has_alarm='是') AS alarms, "
        "       COUNT(DISTINCT point_name) AS type_count "
        "FROM point_detail "
    )
    # 先精确匹配实例名，再模糊兜底
    rows = _exec_points(
        base_sql + " WHERE inst_name = ? "
        "GROUP BY inst_id, inst_name, location, building, floor, room "
        "ORDER BY inst_name",
        (name,),
    )
    if not rows:
        rows = _exec_points(
            base_sql + " WHERE inst_name LIKE ? "
            "GROUP BY inst_id, inst_name, location, building, floor, room "
            "ORDER BY inst_name LIMIT ?",
            (like, limit),
        )
    return rows


def list_points(inst_id):
    """返回某实例（inst_id）下的测点行。

    底层 ``point_detail`` 只落地了部分列；源模块返回的
    ``point_ext / dev_id / dev_name / dpoint_id / dpoint_name`` 在本地无对应列，
    因此以空串补齐，保持与源相同的 JSON 键形状。
    """
    rows = _exec_points(
        "SELECT point_id, point_name, has_alarm "
        "FROM point_detail WHERE inst_id = ? "
        "ORDER BY point_name, point_id",
        (inst_id,),
    )
    out = []
    for r in rows:
        out.append({
            'point_id': r['point_id'],
            'point_name': r['point_name'],
            'point_ext': '',
            'has_alarm': r['has_alarm'],
            'dev_id': '',
            'dev_name': '',
            'dpoint_id': '',
            'dpoint_name': '',
        })
    return out


def zh_zone_list():
    """新列2 - 园区（区）列表：从 zh_device.position 段2 聚合。"""
    agg = _aggregate_positions(_POS_SEG_ZONE + 1)
    return [{'zone': k, 'devices': v['devices']} for k, v in sorted(agg.items())]


def zh_building_list(zone=None):
    """新列2 - 楼栋列表：段3（可按园区过滤）。"""
    like = f'%/{zone}/%' if zone else None
    agg = _aggregate_positions(_POS_SEG_BUILDING + 1, like)
    return [{'building': k, 'devices': v['devices'], 'types': len(v['objs'])}
            for k, v in sorted(agg.items())]


def zh_floor_list(zone, building):
    """新列2 - 楼层列表：段4（按园区+楼栋过滤）。"""
    like = f'%/{zone}/{building}/%'
    agg = _aggregate_positions(_POS_SEG_FLOOR + 1, like)
    return [{'floor': k, 'devices': v['devices'], 'types': len(v['objs'])}
            for k, v in sorted(agg.items())]


def zh_room_list(zone, building, floor):
    """新列2 - 房间列表：段5（按园区+楼栋+楼层过滤）。"""
    like = f'%/{zone}/{building}/{floor}/%'
    agg = _aggregate_positions(_POS_SEG_ROOM + 1, like)
    return [{'room': k, 'devices': v['devices'], 'types': len(v['objs'])}
            for k, v in sorted(agg.items())]


def zh_objtypes_new():
    """新列1 - 设备类型列表：从 zh_device.obj_name 聚合。"""
    return _exec_catalog(
        "SELECT zd.obj_name AS obj_name, "
        "       COUNT(*) AS devices, "
        "       COUNT(DISTINCT zd.ins_id) AS inst_count "
        "FROM zh_device zd "
        "GROUP BY zd.obj_name "
        "ORDER BY devices DESC, zd.obj_name"
    )


def zh_rules_for_obj(obj_name, kw='', limit=3000):
    """新列4 - 某设备类型下的告警规则列表（按 (类型,告警名) 去重）。"""
    return _rules_dedup_sql("WHERE classify_model = ?", (obj_name,), kw, limit)


def zh_rules_all(kw='', limit=3000):
    """新列4 - 全部规则（未选类型时）。按 (类型,告警名) 去重。"""
    return _rules_dedup_sql("WHERE 1=1", (), kw, limit)


def zh_rules_for_device(inst_name, kw='', limit=3000):
    """新列4 - 某设备实例对应的规则（经 obj_name 桥接 zh_rules）。"""
    like = f'%{kw}%'
    return _exec_catalog(
        "SELECT MIN(zr.alarm_config_id) AS alarm_config_id, "
        "       zr.alarm_name, MIN(zr.classify_model_id) AS classify_model_id, "
        "       zr.classify_model, MIN(zr.rule_desc) AS rule_desc "
        "FROM zh_device zd "
        "JOIN zh_rules zr ON zr.classify_model = zd.obj_name "
        "WHERE zd.inst_name = ? "
        "  AND (? = '' OR zr.alarm_name LIKE ? OR zr.rule_desc LIKE ?) "
        "GROUP BY zr.classify_model, zr.alarm_name "
        "ORDER BY zr.alarm_name "
        "LIMIT ?",
        (inst_name, kw, like, like, limit),
    )


def zh_rules_all_by_rooms(rooms=None, kw='', limit=5000):
    """新列4 - 规则列表（按空间过滤版）。rooms=None → 全部规则。"""
    obj_names = []
    if rooms:
        pos_ors, or_args = [], []
        for zone, building, floor, room in rooms:
            segs = [s for s in (zone, building, floor, room) if s]
            if segs:
                cond_sql, cond_args = _pos_match_sql('position', zone, building, floor, room)
                pos_ors.append(cond_sql)
                or_args += list(cond_args)
        if pos_ors:
            rows = _exec_catalog(
                f"SELECT DISTINCT obj_name FROM zh_device "
                f"WHERE `position`<>'' AND ({' OR '.join(pos_ors)})",
                tuple(or_args),
            )
            obj_names = [r['obj_name'] for r in rows if r.get('obj_name')]
    if not obj_names:
        return zh_rules_all(kw, limit)
    ph = ','.join(['?'] * len(obj_names))
    return _rules_dedup_sql(f"WHERE classify_model IN ({ph})", tuple(obj_names), kw, limit)


def zh_devices_new(obj_names=None, zone='', building='', floor='', room='', kw='', limit=3000):
    """新列3 - 设备列表：zh_device（可按类型/空间过滤）。"""
    like = f'%{kw}%'
    conds, args = [], []
    if obj_names:
        ph = ','.join(['?'] * len(obj_names))
        conds.append(f'zd.obj_name IN ({ph})')
        args += list(obj_names)
    if zone:
        cond_sql, cond_args = _pos_match_sql('position', zone, building, floor, room)
        conds.append(cond_sql)
        args += list(cond_args)
    where = ('WHERE ' + ' AND '.join(conds) + ' AND ') if conds else 'WHERE '
    return _exec_catalog(
        f"SELECT zd.inst_name, zd.ins_id, zd.ins_standard_id, zd.obj_name, "
        f"       zd.classification_name, zd.`position` "
        f"FROM zh_device zd "
        f"{where}(? = '' OR zd.inst_name LIKE ? OR zd.ins_id LIKE ? OR zd.obj_name LIKE ?) "
        f"GROUP BY zd.inst_name, zd.ins_id, zd.ins_standard_id, zd.obj_name, "
        f"         zd.classification_name, zd.`position` "
        f"ORDER BY zd.inst_name "
        f"LIMIT ?",
        tuple(args) + (kw, like, like, like, limit),
    )


def zh_devices_by_rooms(obj_names=None, rooms=None, kw='', limit=3000):
    """新列3 - 设备列表（多空间组合版）。rooms 为空/None → 不限空间。"""
    like = f'%{kw}%'
    conds, args = [], []
    if obj_names:
        ph = ','.join(['?'] * len(obj_names))
        conds.append(f'zd.obj_name IN ({ph})')
        args += list(obj_names)
    if rooms:
        pos_ors, or_args = [], []
        for zone, building, floor, room in rooms:
            segs = [s for s in (zone, building, floor, room) if s]
            if segs:
                cond_sql, cond_args = _pos_match_sql('position', zone, building, floor, room)
                pos_ors.append(cond_sql)
                or_args += list(cond_args)
        if pos_ors:
            conds.append('(' + ' OR '.join(pos_ors) + ')')
            args += or_args
    where = ('WHERE ' + ' AND '.join(conds) + ' AND ') if conds else 'WHERE '
    return _exec_catalog(
        f"SELECT zd.inst_name, zd.ins_id, zd.ins_standard_id, zd.obj_name, "
        f"       zd.classification_name, zd.`position` "
        f"FROM zh_device zd "
        f"{where}(? = '' OR zd.inst_name LIKE ? OR zd.ins_id LIKE ? OR zd.obj_name LIKE ?) "
        f"GROUP BY zd.inst_name, zd.ins_id, zd.ins_standard_id, zd.obj_name, "
        f"         zd.classification_name, zd.`position` "
        f"ORDER BY zd.inst_name "
        f"LIMIT ?",
        tuple(args) + (kw, like, like, like, limit),
    )


def zh_device_full(kw='', limit=200):
    """zh_device 全字段列表。kw 可模糊匹配 ins_id / inst_name / obj_name / position。"""
    like = f'%{kw}%'
    return _exec_catalog(
        "SELECT zd.inst_name, zd.ins_standard_id, zd.ins_id, zd.obj_name, "
        "       zd.obj_id, zd.obj_standard_id, zd.classification_id, "
        "       zd.classification_name, zd.`position` "
        "FROM zh_device zd "
        "WHERE (? = '' OR zd.ins_id LIKE ? OR zd.inst_name LIKE ? "
        "       OR zd.obj_name LIKE ? OR zd.`position` LIKE ?) "
        "ORDER BY zd.inst_name "
        "LIMIT ?",
        (kw, like, like, like, like, limit),
    )


def zh_devices_by_obj(obj_name, kw='', limit=2000):
    """按设备分类列出设备（可加名称过滤），并带本地测点数。"""
    like = f'%{kw}%'
    if kw:
        return _exec_catalog_points(
            "SELECT zd.inst_name, zd.ins_standard_id, zd.obj_name, "
            "       COUNT(pd.point_id) AS points, SUM(pd.has_alarm='是') AS alarms "
            "FROM zh_device zd "
            "JOIN points.point_detail pd ON pd.inst_name = zd.inst_name "
            "WHERE zd.obj_name = ? AND (zd.inst_name LIKE ? OR pd.inst_id LIKE ?) "
            "GROUP BY zd.inst_name, zd.ins_standard_id, zd.obj_name "
            "ORDER BY points DESC, zd.inst_name "
            "LIMIT ?",
            (obj_name, like, like, limit),
        )
    return _exec_catalog_points(
        "SELECT zd.inst_name, zd.ins_standard_id, zd.obj_name, "
        "       COUNT(pd.point_id) AS points, SUM(pd.has_alarm='是') AS alarms "
        "FROM zh_device zd "
        "JOIN points.point_detail pd ON pd.inst_name = zd.inst_name "
        "WHERE zd.obj_name = ? "
        "GROUP BY zd.inst_name, zd.ins_standard_id, zd.obj_name "
        "ORDER BY points DESC, zd.inst_name "
        "LIMIT ?",
        (obj_name, limit),
    )


def zh_objtype_rooms(obj_name, kw='', limit=5000):
    """某设备类型下的空间组合（楼栋/楼层/房间），并带该组合内设备台数。"""
    like = f'%{kw}%'
    return _exec_catalog_points(
        "SELECT COALESCE(pd.building,'') AS building, "
        "       COALESCE(pd.floor,'')    AS floor, "
        "       COALESCE(pd.room,'')     AS room, "
        "       COUNT(DISTINCT zd.inst_name) AS devices "
        "FROM zh_device zd "
        "JOIN points.point_detail pd ON pd.inst_name = zd.inst_name "
        "WHERE zd.obj_name = ? "
        "GROUP BY pd.building, pd.floor, pd.room "
        "HAVING COALESCE(pd.building,'') LIKE ? "
        "    OR COALESCE(pd.floor,'')    LIKE ? "
        "    OR COALESCE(pd.room,'')     LIKE ? "
        "ORDER BY building, floor, room "
        "LIMIT ?",
        (obj_name, like, like, like, limit),
    )


def zh_objtype_room_devices(obj_name, building, floor, room, kw='', limit=2000):
    """某设备类型 × 空间组合（楼栋/楼层/房间）下的设备列表（带本地测点数）。"""
    b = (building or '').strip()
    f = (floor or '').strip()
    r = (room or '').strip()
    like = f'%{kw}%'
    return _exec_catalog_points(
        "SELECT zd.inst_name, zd.ins_standard_id, zd.obj_name, "
        "       COUNT(pd.point_id) AS points, SUM(pd.has_alarm='是') AS alarms "
        "FROM zh_device zd "
        "JOIN points.point_detail pd ON pd.inst_name = zd.inst_name "
        "WHERE zd.obj_name = ? "
        "  AND COALESCE(pd.building,'') = ? "
        "  AND COALESCE(pd.floor,'')    = ? "
        "  AND COALESCE(pd.room,'')     = ? "
        "  AND (? = '' OR zd.inst_name LIKE ? OR pd.inst_id LIKE ?) "
        "GROUP BY zd.inst_name, zd.ins_standard_id, zd.obj_name "
        "ORDER BY points DESC, zd.inst_name "
        "LIMIT ?",
        (obj_name, b, f, r, kw, like, like, limit),
    )