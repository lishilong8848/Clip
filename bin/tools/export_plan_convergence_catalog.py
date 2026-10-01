"""Offline exporter: MySQL device/rule catalog to a distributable SQLite file."""

import os
import sqlite3
from pathlib import Path


TABLES = {
    "zh_device": ("id", "inst_name", "ins_standard_id", "ins_id", "obj_name", "obj_id", "obj_standard_id", "classification_id", "classification_name", "position", "synced_at"),
    "zh_rules": ("id", "alarm_config_id", "alarm_name", "classify_model_id", "classify_model", "rule_desc", "domain_code", "synced_at"),
}

POINT_COLUMNS = ("id", "inst_id", "inst_name", "location", "building", "floor", "room", "point_id", "point_name", "has_alarm")


def export_catalog(source, destination):
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    pending = destination.with_suffix(destination.suffix + ".pending")
    if pending.exists():
        raise FileExistsError(f"未完成的目录库文件仍存在：{pending}")
    conn = sqlite3.connect(pending)
    try:
        for table, columns in TABLES.items():
            conn.execute(f"CREATE TABLE {table} (" + ",".join(f"{name} {'INTEGER' if name == 'id' else 'TEXT'}" for name in columns) + ")")
            cursor = source.cursor()
            cursor.execute("SELECT " + ",".join("`" + name + "`" for name in columns) + f" FROM {table}")
            query = f"INSERT INTO {table} VALUES (" + ",".join("?" for _ in columns) + ")"
            while batch := cursor.fetchmany(1000):
                conn.executemany(query, [tuple(str(row[name]) if row[name] is not None and name != "id" else row[name] for name in columns)
                                         if isinstance(row, dict) else tuple(str(value) if value is not None and name != "id" else value for name, value in zip(columns, row))
                                         for row in batch])
            cursor.close()
        for table, columns in (("zh_device", ("inst_name", "ins_id", "obj_name")), ("zh_rules", ("alarm_name", "classify_model", "alarm_config_id"))):
            for column in columns:
                conn.execute(f"CREATE INDEX idx_{table}_{column} ON {table}({column})")
        conn.commit()
        if conn.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("生成的目录库完整性校验失败")
    except Exception:
        conn.close()
        pending.unlink(missing_ok=True)
        raise
    conn.close()
    os.replace(pending, destination)
    return destination


def import_rule_sets(source, destination):
    """Copy original rule groups once; never replace local edits."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(destination)
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("CREATE TABLE IF NOT EXISTS rule_set (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, remark TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, updated_at TEXT NOT NULL)")
        conn.execute("CREATE TABLE IF NOT EXISTS rule_set_item (id INTEGER PRIMARY KEY, set_id INTEGER NOT NULL REFERENCES rule_set(id) ON DELETE CASCADE, scope_type TEXT, obj_name TEXT, zone TEXT, building TEXT, floor TEXT, room TEXT, inst_name TEXT, point_name TEXT, rule_name TEXT, alarm_config_id TEXT, rule_group_no INTEGER, rule_type TEXT, rule_label TEXT, created_at TEXT NOT NULL)")
        if conn.execute("SELECT COUNT(*) FROM rule_set").fetchone()[0]:
            raise ValueError("目标规则库已有数据，拒绝覆盖")
        for table, columns in (
            ("rule_set", ("id", "name", "remark", "created_at", "updated_at")),
            ("rule_set_item", ("id", "set_id", "scope_type", "obj_name", "zone", "building", "floor", "room", "inst_name", "point_name", "rule_name", "alarm_config_id", "rule_group_no", "rule_type", "rule_label", "created_at")),
        ):
            cursor = source.cursor()
            cursor.execute("SELECT " + ",".join("`" + column + "`" for column in columns) + f" FROM {table}")
            rows = cursor.fetchall()
            cursor.close()
            placeholders = ",".join("?" for _ in columns)
            conn.executemany(f"INSERT INTO {table} VALUES ({placeholders})", [
                tuple(str(row[column]) if row[column] is not None and column in {"created_at", "updated_at"} else row[column] for column in columns)
                if isinstance(row, dict) else tuple(row) for row in rows
            ])
        conn.commit()
        return destination
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def export_points(source, destination):
    """Export only the columns used by the review's per-device point drilldown."""
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    pending = destination.with_suffix(destination.suffix + ".pending")
    if pending.exists():
        raise FileExistsError(f"未完成的测点文件仍存在：{pending}")
    conn = sqlite3.connect(pending)
    try:
        conn.execute("PRAGMA journal_mode=OFF")
        conn.execute("PRAGMA synchronous=OFF")
        conn.execute("CREATE TABLE point_detail (id INTEGER PRIMARY KEY," +
                     ",".join(f"{name} TEXT" for name in POINT_COLUMNS[1:]) + ")")
        cursor = source.cursor()
        last_id = 0
        query = "SELECT " + ",".join("`" + name + "`" for name in POINT_COLUMNS) + " FROM point_detail WHERE id > %s ORDER BY id LIMIT 10000"
        insert = "INSERT INTO point_detail VALUES (" + ",".join("?" for _ in POINT_COLUMNS) + ")"
        while True:
            cursor.execute(query, (last_id,))
            batch = cursor.fetchall()
            if not batch:
                break
            conn.executemany(insert, [tuple(row[name] for name in POINT_COLUMNS) if isinstance(row, dict) else tuple(row) for row in batch])
            last_id = int(batch[-1]["id"] if isinstance(batch[-1], dict) else batch[-1][0])
        cursor.close()
        conn.execute("CREATE INDEX idx_point_detail_inst_name ON point_detail(inst_name)")
        conn.execute("CREATE INDEX idx_point_detail_inst_id ON point_detail(inst_id)")
        conn.commit()
        if conn.execute("PRAGMA quick_check").fetchone()[0] != "ok":
            raise ValueError("测点文件完整性校验失败")
    except Exception:
        conn.close()
        pending.unlink(missing_ok=True)
        raise
    conn.close()
    os.replace(pending, destination)
    return destination


if __name__ == "__main__":
    import argparse
    import pymysql
    from upload_event_module.utils import get_data_file_path

    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=str(Path(get_data_file_path("plan_convergence")) / "catalog.sqlite3"))
    parser.add_argument("--with-points", action="store_true", help="另生成核对台逐设备测点详情文件")
    args = parser.parse_args()
    password = os.environ.get("PLAN_CONVERGENCE_MYSQL_PASSWORD")
    if password is None:
        parser.error("请先设置 PLAN_CONVERGENCE_MYSQL_PASSWORD 环境变量")
    source = pymysql.connect(host=os.environ.get("PLAN_CONVERGENCE_MYSQL_HOST", "127.0.0.1"),
                             port=int(os.environ.get("PLAN_CONVERGENCE_MYSQL_PORT", "3306")),
                             user=os.environ.get("PLAN_CONVERGENCE_MYSQL_USER", "root"),
                             password=password, database="zhihang_points", charset="utf8mb4")
    try:
        result = export_catalog(source, args.output)
        print(f"目录库已生成：{result}")
        if args.with_points:
            points_path = export_points(source, Path(args.output).with_name("points.sqlite3"))
            print(f"测点详情文件已生成：{points_path}")
    finally:
        source.close()
