# -*- coding: utf-8 -*-
"""Isolated tests for the read-only SQLite port of the legacy points_db lookups.

Builds representative zh_device / zh_rules / point_detail tables in temporary
SQLite files, then exercises the cascade (zone -> building -> floor -> room)
and point lookups against the ported ``plan_convergence_legacy.points_db``.
"""

import importlib.util
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path
from unittest.mock import patch

_BIN = Path(__file__).resolve().parent
sys.path.insert(0, str(_BIN))

_MODULE_PATH = _BIN / "lan_bitable_template_portal" / "plan_convergence_points.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("plan_convergence_legacy_points_db", _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _make_db(path, tables):
    """Create a sqlite file with the given ``{table: [row tuples]}`` data."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as conn:
        for table, (cols_sql, rows) in tables.items():
            conn.execute(f"CREATE TABLE {table} ({cols_sql})")
            if rows:
                conn.executemany(
                    f"INSERT INTO {table} VALUES ({','.join('?' * len(rows[0]))})", rows
                )
        conn.commit()


CATALOG_COLS = (
    "id INTEGER, inst_name TEXT, ins_standard_id TEXT, ins_id TEXT, obj_name TEXT, "
    "obj_id TEXT, obj_standard_id TEXT, classification_id TEXT, "
    "classification_name TEXT, position TEXT, synced_at TEXT"
)
RULES_COLS = (
    "id INTEGER, alarm_config_id TEXT, alarm_name TEXT, classify_model_id TEXT, "
    "classify_model TEXT, rule_desc TEXT, domain_code TEXT, synced_at TEXT"
)
POINTS_COLS = (
    "id INTEGER, inst_id TEXT, inst_name TEXT, location TEXT, building TEXT, "
    "floor TEXT, room TEXT, point_id TEXT, point_name TEXT, has_alarm TEXT"
)

POSITION_A = "全国/华东一区/HD8/南通数据中心B/B-F2/设备间-H楼2F"
POSITION_B = "全国/华东一区/HD8/南通数据中心B/B-F2/另一房间"


def _build_databases(directory):
    catalog = Path(directory) / "plan_convergence" / "catalog.sqlite3"
    points = Path(directory) / "plan_convergence" / "points.sqlite3"

    _make_db(catalog, {
        "zh_device": (CATALOG_COLS, [
            (1, "冷机一号", "STD-1", "INS-1", "冷机", "OBJ-1", "OBJ-STD-1",
             "CLS-1", "冷机类", POSITION_A, "2026-01-01"),
            (2, "冷机二号", "STD-2", "INS-2", "冷机", "OBJ-1", "OBJ-STD-1",
             "CLS-1", "冷机类", POSITION_B, "2026-01-01"),
            (3, "水泵三号", "STD-3", "INS-3", "水泵", "OBJ-2", "OBJ-STD-2",
             "CLS-2", "水泵类", POSITION_A, "2026-01-01"),
        ]),
        "zh_rules": (RULES_COLS, [
            (1, "ALM-1", "高温", "MODEL-1", "冷机", "冷机温度过高", "d1", "2026-01-01"),
            (2, "ALM-2", "低压", "MODEL-1", "冷机", "冷机压力过低", "d1", "2026-01-01"),
        ]),
    })
    _make_db(points, {
        "point_detail": (POINTS_COLS, [
            (1, "INS-1", "冷机一号", "A-101", "南通数据中心B", "B-F2", "设备间-H楼2F",
             "P-1", "温度", "是"),
            (2, "INS-1", "冷机一号", "A-101", "南通数据中心B", "B-F2", "设备间-H楼2F",
             "P-2", "压力", "否"),
            (3, "INS-2", "冷机二号", "A-102", "南通数据中心B", "B-F2", "另一房间",
             "P-3", "流量", "是"),
        ]),
    })
    return catalog, points


class PointsAdapterTests(unittest.TestCase):
    def setUp(self):
        self.mod = _load_module()
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        catalog, points = _build_databases(Path(self._tmp.name))
        self.catalog = catalog
        self.points = points
        # Point the module at the isolated temp databases.
        patcher_catalog = patch.object(self.mod, "CATALOG_PATH", catalog)
        patcher_points = patch.object(self.mod, "POINTS_PATH", points)
        patcher_catalog.start()
        patcher_points.start()
        self.addCleanup(patcher_catalog.stop)
        self.addCleanup(patcher_points.stop)

    def test_cascade_zone_building_floor_room(self):
        zones = self.mod.zh_zone_list()
        self.assertEqual([r["zone"] for r in zones], ["HD8"])
        self.assertEqual(zones[0]["devices"], 3)

        buildings = self.mod.zh_building_list("HD8")
        self.assertEqual([b["building"] for b in buildings], ["南通数据中心B"])
        self.assertEqual(buildings[0]["devices"], 3)
        self.assertEqual(buildings[0]["types"], 2)  # 冷机 + 水泵

        floors = self.mod.zh_floor_list("HD8", "南通数据中心B")
        self.assertEqual([f["floor"] for f in floors], ["B-F2"])
        self.assertEqual(floors[0]["devices"], 3)

        rooms = self.mod.zh_room_list("HD8", "南通数据中心B", "B-F2")
        self.assertEqual({r["room"] for r in rooms}, {"设备间-H楼2F", "另一房间"})
        by_room = {r["room"]: r["devices"] for r in rooms}
        self.assertEqual(by_room["设备间-H楼2F"], 2)  # 冷机一号 + 水泵三号
        self.assertEqual(by_room["另一房间"], 1)

    def test_devices_new_and_by_rooms(self):
        devs = self.mod.zh_devices_new(
            ["冷机"], "HD8", "南通数据中心B", "B-F2", "设备间-H楼2F", "", limit=3000)
        self.assertEqual([d["inst_name"] for d in devs], ["冷机一号"])
        self.assertEqual(devs[0]["ins_id"], "INS-1")
        self.assertEqual(devs[0]["obj_name"], "冷机")

        by_rooms = self.mod.zh_devices_by_rooms(
            ["冷机"], [("HD8", "南通数据中心B", "B-F2", "设备间-H楼2F")], "", limit=3000)
        self.assertEqual([d["inst_name"] for d in by_rooms], ["冷机一号"])

        all_devs = self.mod.zh_devices_new(None, "HD8", "南通数据中心B", "B-F2", "", "", limit=3000)
        self.assertEqual({d["inst_name"] for d in all_devs},
                         {"冷机一号", "冷机二号", "水泵三号"})

    def test_device_full_and_objtypes(self):
        full = self.mod.zh_device_full("冷机一号", limit=200)
        self.assertEqual(full[0]["ins_id"], "INS-1")
        self.assertEqual(full[0]["position"], POSITION_A)
        self.assertIn("obj_id", full[0])

        objtypes = self.mod.zh_objtypes_new()
        by_obj = {r["obj_name"]: r for r in objtypes}
        self.assertEqual(by_obj["冷机"]["devices"], 2)
        self.assertEqual(by_obj["冷机"]["inst_count"], 2)

    def test_rules_lookups(self):
        rules_obj = self.mod.zh_rules_for_obj("冷机", "")
        self.assertEqual({r["alarm_name"] for r in rules_obj}, {"高温", "低压"})
        self.assertEqual(rules_obj[0]["classify_model"], "冷机")
        self.assertIn("rule_desc", rules_obj[0])

        rules_all = self.mod.zh_rules_all("")
        self.assertEqual(len(rules_all), 2)

        rules_dev = self.mod.zh_rules_for_device("冷机一号", "")
        self.assertEqual({r["alarm_name"] for r in rules_dev}, {"高温", "低压"})

        rules_by_room = self.mod.zh_rules_all_by_rooms(
            [("HD8", "南通数据中心B", "B-F2", "")], "")
        self.assertEqual({r["alarm_name"] for r in rules_by_room}, {"高温", "低压"})

    def test_point_lookups(self):
        by_name = self.mod.points_by_inst_name("冷机一号")
        self.assertEqual(len(by_name), 1)
        self.assertEqual(by_name[0]["inst_name"], "冷机一号")
        self.assertEqual(by_name[0]["points"], 2)
        self.assertEqual(by_name[0]["alarms"], 1)  # 只有「温度」是告警
        self.assertIn("type_count", by_name[0])

        # 精确匹配无果 → 模糊兜底
        fuzzy = self.mod.points_by_inst_name("冷机")
        self.assertEqual({r["inst_name"] for r in fuzzy}, {"冷机一号", "冷机二号"})

        points = self.mod.list_points("INS-1")
        self.assertEqual(len(points), 2)
        self.assertEqual(points[0]["point_name"], "压力")  # 按 point_name 排序
        for key in ("point_id", "point_name", "point_ext", "has_alarm",
                    "dev_id", "dev_name", "dpoint_id", "dpoint_name"):
            self.assertIn(key, points[0])

    def test_join_devices_obj_rooms(self):
        by_obj = self.mod.zh_devices_by_obj("冷机", "")
        by_inst = {r["inst_name"]: r for r in by_obj}
        self.assertEqual(by_inst["冷机一号"]["points"], 2)
        self.assertEqual(by_inst["冷机一号"]["alarms"], 1)

        obj_rooms = self.mod.zh_objtype_rooms("冷机", "")
        by_room = {(r["building"], r["floor"], r["room"]): r["devices"] for r in obj_rooms}
        self.assertEqual(by_room[("南通数据中心B", "B-F2", "设备间-H楼2F")], 1)
        self.assertEqual(by_room[("南通数据中心B", "B-F2", "另一房间")], 1)

        room_devs = self.mod.zh_objtype_room_devices(
            "冷机", "南通数据中心B", "B-F2", "设备间-H楼2F", "")
        self.assertEqual([d["inst_name"] for d in room_devs], ["冷机一号"])
        self.assertEqual(room_devs[0]["points"], 2)

    def test_missing_files_raise(self):
        with tempfile.TemporaryDirectory() as empty_dir:
            with patch.object(self.mod, "CATALOG_PATH", Path(empty_dir) / "nope.sqlite3"):
                with patch.object(self.mod, "POINTS_PATH", Path(empty_dir) / "nope_points.sqlite3"):
                    with self.assertRaises(FileNotFoundError):
                        self.mod.zh_devices_new(["冷机"])
                    with self.assertRaises(FileNotFoundError):
                        self.mod.points_by_inst_name("冷机")

    def test_read_only_connections(self):
        # Queries must not modify the files - verify data is unchanged.
        before = self.mod.zh_objtypes_new()
        self.mod.zh_zone_list()
        self.mod.zh_devices_by_obj("冷机", "")
        after = self.mod.zh_objtypes_new()
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
