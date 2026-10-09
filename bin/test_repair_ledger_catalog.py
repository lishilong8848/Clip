# -*- coding: utf-8 -*-
"""Focused temporary-file tests for ``LedgerCatalog`` (repair device ledger cache).

These tests exercise the durable SQLite equipment cache in isolation using
:class:`tempfile.TemporaryDirectory`, so no real cloud requests or product writes
happen.  This module stays focused on the ``repair_ledger_catalog.LedgerCatalog``
public contract.
"""
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.repair_ledger_catalog import (  # noqa: E402
    LedgerCatalog,
)


def _record(record_id, *, building="A", system="安防", big_type="消防", device="主机",
            device_num="D001", other="参数X", brand="品牌1", install="1层",
            model="M-100", type_id="T-1", capacity="10", scopes=None):
    return {
        "record_id": record_id,
        "scope_codes": list(scopes or []),
        "设备编号": device_num,
        "机楼": building,
        "系统名称": system,
        "大设备类型": big_type,
        "设备名称": device,
        "产品其它参数": other,
        "品牌": brand,
        "安装位置": install,
        "型号": model,
        "设备类型标识": type_id,
        "容量": capacity,
    }


class LedgerCatalogBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.db = Path(self.tmp.name) / "repair_catalog.sqlite3"
        self.cat = LedgerCatalog(self.db)

    def _list_records(self, result):
        return [r["record_id"] for r in result["records"]]

    def _rebuild(self):
        self.cat = LedgerCatalog(self.db)

    def _direct_conn(self):
        """Raw sqlite3 connection for inspecting internals without cache helpers."""
        import sqlite3

        return sqlite3.connect(str(self.db))


class BoundedReconcileTests(LedgerCatalogBase):
    """Bounded reconcile keeps unchanged rows untouched while only reconciling
    the added/changed/deleted source records."""

    def _install_write_audit(self):
        """Create BEFORE-row triggers on ``records`` storing every attempted live-row
        mutation. Any live-row write (insert/update/delete) is recorded."""
        conn = self._direct_conn()
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS write_audit(mut TEXT NOT NULL)")
            conn.execute("DROP TRIGGER IF EXISTS trg_ledger_insert")
            conn.execute("DROP TRIGGER IF EXISTS trg_ledger_update")
            conn.execute("DROP TRIGGER IF EXISTS trg_ledger_delete")
            conn.execute(
                "CREATE TRIGGER trg_ledger_insert BEFORE INSERT ON records "
                "BEGIN INSERT INTO write_audit VALUES('insert'); END;"
            )
            conn.execute(
                "CREATE TRIGGER trg_ledger_update BEFORE UPDATE ON records "
                "BEGIN INSERT INTO write_audit VALUES('update'); END;"
            )
            conn.execute(
                "CREATE TRIGGER trg_ledger_delete BEFORE DELETE ON records "
                "BEGIN INSERT INTO write_audit VALUES('delete'); END;"
            )
            conn.execute("DELETE FROM write_audit")
            conn.commit()
        finally:
            conn.close()

    def _audit_count(self):
        conn = self._direct_conn()
        try:
            return conn.execute("SELECT COUNT(*) FROM write_audit").fetchone()[0]
        finally:
            conn.close()

    def test_unchanged_snapshot_does_not_write_live_records(self):
        self.cat.replace([_record("r1", building="A", device="主机", scopes=["A"])])
        self._install_write_audit()

        self.cat.replace([_record("r1", building="A", device="主机", scopes=["A"])])

        self.assertEqual(self._audit_count(), 0)
        status = self.cat.status()
        self.assertTrue(status["ready"])
        self.assertEqual(status["record_count"], 1)
        result = self.cat.query()
        self.assertEqual(result["records"][0]["设备名称"], "主机")

    def test_unchanged_rowid_preserved_across_change_plus_add_del(self):
        self.cat.replace([
            _record("keep", building="A", device="主机", scopes=["A"]),
            _record("change", building="B", device="旧名", scopes=["B"]),
            _record("gone", building="C", device="删除", scopes=["C"]),
        ])

        def _rowid(record_id):
            conn = self._direct_conn()
            try:
                return conn.execute(
                    "SELECT rowid FROM records WHERE record_id=?", (record_id,)
                ).fetchone()[0]
            finally:
                conn.close()

        keep_rowid = _rowid("keep")
        change_rowid = _rowid("change")

        self.cat.replace([
            _record("keep", building="A", device="主机", scopes=["A"]),
            _record("change", building="B", device="新名", scopes=["B"]),
            _record("brand_new", building="D", device="插入", scopes=["D"]),
        ])

        result = self.cat.query()
        by_id = {r["record_id"]: r for r in result["records"]}
        self.assertEqual(set(by_id), {"keep", "change", "brand_new"})
        self.assertEqual(by_id["change"]["设备名称"], "新名")
        self.assertEqual(by_id["brand_new"]["设备名称"], "插入")
        # Unchanged and changed rows keep their original rowids (no delete+reinsert).
        self.assertEqual(_rowid("keep"), keep_rowid)
        self.assertEqual(_rowid("change"), change_rowid)

    def test_reordered_incoming_keys_do_not_trigger_update(self):
        first = {
            "record_id": "r1",
            "scope_codes": ["A"],
            "设备编号": "D001",
            "机楼": "A",
            "系统名称": "安防",
            "大设备类型": "消防",
            "设备名称": "主机",
            "产品其它参数": "参数X",
            "品牌": "品牌1",
            "安装位置": "1层",
            "型号": "M-100",
            "设备类型标识": "T-1",
            "容量": "10",
        }
        reordered = {k: v for k, v in list(first.items())[::-1]}
        self.cat.replace([dict(first)])
        self._install_write_audit()

        self.cat.replace([reordered])

        self.assertEqual(self._audit_count(), 0)
        result = self.cat.query()
        self.assertEqual(len(result["records"]), 1)
        self.assertEqual(result["records"][0]["设备名称"], "主机")

    def test_rollback_keeps_previous_when_change_mixed_with_bad_tail(self):
        self.cat.replace([_record("keep", building="A", device="原", scopes=["A"])])
        self._install_write_audit()

        bad = [
            _record("touched", building="B", device="本应回滚", scopes=["B"]),
            {"机楼": "C"},  # malformed: no record_id -> failure after staging first
        ]
        with self.assertRaises(ValueError):
            self.cat.replace(bad)

        status = self.cat.status()
        self.assertEqual(status["record_count"], 1)
        result = self.cat.query()
        self.assertEqual(self._list_records(result), ["keep"])
        self.assertEqual(result["records"][0]["设备名称"], "原")


class VariableLimitReconcileTests(LedgerCatalogBase):
    """Prove reconciliation never builds an unbounded SQL ``IN (...)`` list.

    Lowering ``SQLITE_LIMIT_VARIABLE_NUMBER`` to 64 on the catalog's connections
    makes any parameter-heavy statement (e.g. ``WHERE record_id IN (?, ? , ...)``
    with >64 ids) fail. The set-based reconcile against ``ledger_stage`` must
    still handle a large initial load plus many adds/deletes.
    """

    def _low_limit_cat(self):
        import sqlite3

        cat = LedgerCatalog(self.db)
        original_connect = LedgerCatalog._connect

        def _low_limit_connect(catalog):
            conn = original_connect(catalog)
            conn.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 64)
            return conn

        import types

        cat._connect = types.MethodType(_low_limit_connect, cat)
        return cat

    def test_large_initial_sync_and_many_adds_deletes_under_low_variable_limit(self):
        cat = self._low_limit_cat()

        # Initial load larger than the reduced variable limit (64).
        first = [_record(f"r{i:04d}", scopes=["A"]) for i in range(100)]
        status = cat.replace(first)
        self.assertTrue(status["ready"])
        self.assertEqual(status["record_count"], 100)

        # Replace with a disjoint larger set (delete all 100, add 130) which would
        # overflow a >64 item ``IN (... )`` list if such a list were emitted.
        second = [_record(f"n{i:04d}", scopes=["B"]) for i in range(130)]
        status = cat.replace(second)
        self.assertTrue(status["ready"])
        self.assertEqual(status["record_count"], 130)

        result = cat.query(page_size=100)
        self.assertEqual(result["total"], 130)
        second_ids = {f"n{i:04d}" for i in range(130)}
        # First page holds up to the clamped page size, all from the new set.
        self.assertTrue({r["record_id"] for r in result["records"]} <= second_ids)


class EmptySuccessAndStatusTests(LedgerCatalogBase):
    def test_empty_successful_source_is_valid_ready_cache(self):
        status = self.cat.replace([])
        self.assertTrue(status["ready"])
        self.assertEqual(status["record_count"], 0)
        self.assertIsNotNone(status["refreshed_at"])
        self.assertIsNone(status["error"])

        result = self.cat.query()
        self.assertEqual(result["records"], [])
        self.assertEqual(result["total"], 0)


class PersistenceRestartTests(LedgerCatalogBase):
    def test_restart_persists_records(self):
        self.cat.replace([
            _record("r1", building="A", device="主机", scopes=["A"]),
            _record("r2", building="B", device="探头", scopes=["B"]),
        ])
        self._rebuild()
        status = self.cat.status()
        self.assertTrue(status["ready"])
        self.assertEqual(status["record_count"], 2)
        self.assertIsNotNone(status["refreshed_at"])

        found = self.cat.get_records(["r1", "r2"])
        self.assertEqual([r["record_id"] for r in found], ["r1", "r2"])
        self.assertEqual(found[0]["机楼"], "A")
        self.assertEqual(found[0]["设备名称"], "主机")
        self.assertEqual(found[0]["scope_codes"], ["A"])

    def test_refresh_replace_replaces_snapshot_atomically(self):
        self.cat.replace([_record("r1", building="A", device="旧", scopes=["A"])])
        self.cat.replace([_record("r2", building="B", device="新", scopes=["B"])])
        result = self.cat.query()
        self.assertEqual(self._list_records(result), ["r2"])
        self.assertEqual(result["records"][0]["device_name"] if False else result["records"][0]["设备名称"], "新")


class CombinedFiltersTests(LedgerCatalogBase):
    def test_combined_filters_are_and(self):
        self.cat.replace([
            _record("a1", building="A", system="安防", big_type="消防", device="主机", scopes=["A"]),
            _record("a2", building="A", system="安防", big_type="电力", device="探头", scopes=["A"]),
            _record("b1", building="B", system="安防", big_type="消防", device="主机", scopes=["B"]),
        ])
        result = self.cat.query(filters={"机楼": "A", "系统名称": "安防", "大设备类型": "消防", "设备名称": "主机"})
        self.assertEqual(self._list_records(result), ["a1"])

    def test_unknown_filter_key_rejected(self):
        self.cat.replace([_record("a1", building="A", scopes=["A"])])
        with self.assertRaises(ValueError):
            self.cat.query(filters={"不存在字段": "x"})


class SearchFieldTests(LedgerCatalogBase):
    def _seed_fields(self):
        # Differentiate each searchable field with a unique marker value; tokens must
        # not be substrings of each other so a single-keyword search hits exactly one.
        markers = [
            ("设备编号", "Num9x"),
            ("机楼", "Bld8y"),
            ("系统名称", "Sys7z"),
            ("大设备类型", "Big6q"),
            ("设备名称", "Dev5r"),
            ("产品其它参数", "Oth4p"),
            ("品牌", "Brd3s"),
            ("安装位置", "Ins2t"),
            ("型号", "Mod1u"),
            ("设备类型标识", "Typ0v"),
            ("容量", "Cap9w"),
        ]
        records = []
        for idx, (field, marker) in enumerate(markers):
            rec = _record(f"m{idx}", scopes=["A"])
            rec[field] = marker
            records.append(rec)
        # Also add a record with nothing matching.
        records.append(_record("m-none", scopes=["A"]))
        self.cat.replace(records)

    def test_every_searchable_field_matches_keyword(self):
        self._seed_fields()
        for field, marker in [
            ("设备编号", "Num9x"),
            ("机楼", "Bld8y"),
            ("系统名称", "Sys7z"),
            ("大设备类型", "Big6q"),
            ("设备名称", "Dev5r"),
            ("产品其它参数", "Oth4p"),
            ("品牌", "Brd3s"),
            ("安装位置", "Ins2t"),
            ("型号", "Mod1u"),
            ("设备类型标识", "Typ0v"),
            ("容量", "Cap9w"),
        ]:
            with self.subTest(field=field):
                result = self.cat.query(query=marker)
                self.assertEqual(result["total"], 1, msg=f"field={field}")
                self.assertEqual(result["records"][0][field], marker)

    def test_multi_keyword_search_is_and(self):
        self.cat.replace([
            _record("k1", building="A", system="安防", device="主机", scopes=["A"]),
            _record("k2", building="A", system="消防", device="主机", scopes=["A"]),
            _record("k3", building="A", system="安防", device="探头", scopes=["A"]),
        ])
        result = self.cat.query(query="安防 主机")
        self.assertEqual(self._list_records(result), ["k1"])
        self.assertEqual(result["total"], 1)

    def test_search_normalizes_nfkc_and_casefold(self):
        # NFKC: full-width A -> A; casefold: mixed case matches.
        self.cat.replace([
            _record("n1", device="ＦＩＲＥ　ＡＬＡＲＭ", scopes=["A"]),
            _record("n2", device="fire alarm", scopes=["A"]),
            _record("n3", device="完全无关", scopes=["A"]),
        ])
        result = self.cat.query(query="fire ＡＬＡＲＭ")
        matched = {r["record_id"] for r in result["records"]}
        self.assertEqual(matched, {"n1", "n2"})


class WildcardLiteralTests(LedgerCatalogBase):
    def test_percent_underscore_bracket_are_literal(self):
        self.cat.replace([
            _record("w1", model="A%B", scopes=["A"]),
            _record("w2", model="A_C", scopes=["A"]),
            _record("w3", model="A[xy]B", scopes=["A"]),
            _record("w4", model="AXB", scopes=["A"]),
            _record("w5", model="A123", scopes=["A"]),
        ])
        result = self.cat.query(query="A%B")
        self.assertEqual(self._list_records(result), ["w1"])
        result = self.cat.query(query="A_C")
        self.assertEqual(self._list_records(result), ["w2"])
        result = self.cat.query(query="A[xy]B")
        self.assertEqual(self._list_records(result), ["w3"])


class ScopePermissionTests(LedgerCatalogBase):
    def _seed(self):
        self.cat.replace([
            _record("a1", building="A", device="主机", scopes=["A"]),
            _record("b1", building="B", device="探头", scopes=["B"]),
            _record("a-c", building="A", device="公共A", scopes=["A", "C"]),
            _record("public", building="", device="无楼栋", scopes=[]),
            _record("unknown-x", building="X", device="未知楼", scopes=["X"]),
        ])

    def test_all_scopes_none_returns_everything(self):
        self._seed()
        result = self.cat.query()
        self.assertEqual(set(self._list_records(result)), {"a1", "b1", "a-c", "public", "unknown-x"})

    def test_empty_allowed_scopes_returns_nothing_and_empty_options(self):
        self._seed()
        result = self.cat.query(allowed_scopes=[])
        self.assertEqual(result["records"], [])
        self.assertEqual(result["total"], 0)
        for values in result["options"].values():
            self.assertEqual(values, [])

    def test_scoped_query_includes_owned_and_truly_unscoped(self):
        self._seed()
        result = self.cat.query(allowed_scopes=["A"])
        self.assertEqual(set(self._list_records(result)), {"a1", "a-c", "public"})
        self.assertNotIn("b1", result["records"])
        # unknown building X is not public without a matching scope
        self.assertNotIn("unknown-x", result["records"])

    def test_scope_in_list_matches(self):
        self._seed()
        result = self.cat.query(allowed_scopes=["C"])
        self.assertEqual(set(self._list_records(result)), {"a-c", "public"})

    def test_options_scoped_by_permission_no_cross_building_leak(self):
        self._seed()
        result_a = self.cat.query(allowed_scopes=["A"])
        self.assertEqual(result_a["options"]["机楼"], ["A"])
        # B building must not leak into A's options
        self.assertNotIn("B", result_a["options"]["机楼"])
        self.assertNotIn("X", result_a["options"]["机楼"])

        result_b = self.cat.query(allowed_scopes=["B"])
        self.assertEqual(result_b["options"]["机楼"], ["B"])
        self.assertNotIn("A", result_b["options"]["机楼"])

        # unscoped public record (blank building) stays visible but its blank
        # building value is excluded from the building option list
        self.assertEqual(result_a["options"]["机楼"], ["A"])
        self.assertEqual(set(result_a["options"]["设备名称"]), {"公共A", "主机", "无楼栋"})


class AtomicReplaceTests(LedgerCatalogBase):
    def test_duplicate_id_failure_keeps_previous_cache(self):
        self.cat.replace([_record("old", building="A", device="原", scopes=["A"])])
        # two records in the same incoming batch share a record_id -> must fail
        bad = [_record("dup", building="B"), _record("dup", building="C")]
        with self.assertRaises(ValueError):
            self.cat.replace(bad)
        status = self.cat.status()
        self.assertEqual(status["record_count"], 1)
        self.assertEqual(status["error"], None)
        result = self.cat.query()
        self.assertEqual(self._list_records(result), ["old"])
        self.assertEqual(result["records"][0]["设备名称"], "原")

    def test_generator_failure_rolls_back_previous_cache(self):
        self.cat.replace([_record("keep", building="A", device="保留", scopes=["A"])])

        def broken():
            yield _record("new1", building="B")
            raise RuntimeError("上游读取失败")

        with self.assertRaises(RuntimeError):
            self.cat.replace(broken())
        status = self.cat.status()
        self.assertEqual(status["record_count"], 1)
        result = self.cat.query()
        self.assertEqual(self._list_records(result), ["keep"])
        self.assertEqual(result["records"][0]["设备名称"], "保留")

    def test_partial_bad_id_failure_keeps_previous_cache(self):
        self.cat.replace([_record("good", building="A", scopes=["A"])])
        bad = [_record("ok1", building="B"), {"机楼": "C"}]
        with self.assertRaises(ValueError):
            self.cat.replace(bad)
        self.assertEqual(self.cat.status()["record_count"], 1)


class StaleCacheOnFailureTests(LedgerCatalogBase):
    def test_failed_replace_keeps_previous_ready_snapshot(self):
        self.cat.replace([_record("old", building="A", device="旧", scopes=["A"])])
        with self.assertRaises(RuntimeError):
            self.cat.replace(_raise_gen())
        status = self.cat.status()
        self.assertTrue(status["ready"])
        self.assertEqual(status["record_count"], 1)
        self.assertEqual(self._list_records(self.cat.query()), ["old"])

    def test_mark_error_keeps_old_good_cache_readable(self):
        self.cat.replace([_record("r1", building="A", scopes=["A"])])
        self.cat.mark_error("刷新失败：上游超时")
        status = self.cat.status()
        self.assertTrue(status["ready"])
        self.assertEqual(status["record_count"], 1)
        self.assertIsNotNone(status["error"])
        # old cache still fully readable
        result = self.cat.query(allowed_scopes=["A"])
        self.assertEqual(self._list_records(result), ["r1"])
        # successful replace clears the error
        self.cat.replace([_record("r2", building="B", scopes=["B"])])
        self.assertIsNone(self.cat.status()["error"])


def _raise_gen():
    yield _record("new", building="B")
    raise RuntimeError("上游读取失败")


class GetRecordsTests(LedgerCatalogBase):
    def test_selected_ids_respect_permission(self):
        self.cat.replace([
            _record("a1", building="A", scopes=["A"]),
            _record("b1", building="B", scopes=["B"]),
            _record("pub", building="", scopes=[]),
        ])
        found = self.cat.get_records(["a1", "b1", "pub", "missing"])
        self.assertEqual([r["record_id"] for r in found], ["a1", "b1", "pub"])
        # caller handles missing IDs -> absent
        self.assertNotIn("missing", [r["record_id"] for r in found])

        scoped = self.cat.get_records(["a1", "b1", "pub"], allowed_scopes=["A"])
        self.assertEqual([r["record_id"] for r in scoped], ["a1", "pub"])
        self.assertNotIn("b1", [r["record_id"] for r in scoped])

    def test_empty_request_returns_empty(self):
        self.cat.replace([_record("a1", building="A", scopes=["A"])])
        self.assertEqual(self.cat.get_records([]), [])


class QueryPaginationTests(LedgerCatalogBase):
    def test_pagination_clamps_and_orders(self):
        records = [
            _record(f"r{i}", building="A", device_num=f"{1000+i:04d}", scopes=["A"])
            for i in range(5)
        ]
        self.cat.replace(records)
        result = self.cat.query(page_size=3, page=1, allowed_scopes=["A"])
        self.assertEqual(result["page_size"], 3)
        self.assertEqual(result["total"], 5)
        self.assertEqual(len(result["records"]), 3)
        result2 = self.cat.query(page_size=1000, page=0, allowed_scopes=["A"])
        self.assertEqual(result2["page_size"], 100)
        self.assertEqual(result2["page"], 1)
        # deterministic order by 设备编号 then record_id
        got = [r["设备编号"] for r in result2["records"]]
        self.assertEqual(got, sorted(got))


class ParentDirectoryTests(unittest.TestCase):
    def test_nested_parent_directory_created_on_demand(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "nested" / "deeper" / "catalog.sqlite3"
            cat = LedgerCatalog(db)
            self.assertTrue(db.parent.exists())
            cat.replace([_record("r1", building="A", scopes=["A"])])
            self.assertTrue(db.exists())
            self.assertEqual(cat.status()["record_count"], 1)


class ValidationTests(LedgerCatalogBase):
    def test_record_id_is_trimmed(self):
        self.cat.replace([_record("  r1  ", building="A", scopes=["A"])])
        result = self.cat.query()
        self.assertEqual(result["records"][0]["record_id"], "r1")

    def test_whitespace_only_or_empty_record_id_rejected(self):
        for bad in ("   ", ""):
            with self.assertRaises(ValueError):
                self.cat.replace([_record(bad, building="A", scopes=["A"])])

    def test_scope_codes_must_be_list_like_not_string(self):
        rec = _record("r1", building="A", scopes=["A"])
        rec["scope_codes"] = "ABC"
        with self.assertRaises(ValueError):
            self.cat.replace([rec])


class ConcurrentAtomicReadTests(LedgerCatalogBase):
    def test_reader_sees_old_or_new_never_partial_during_slow_replace(self):
        self.cat.replace(
            [_record(f"o{i}", building="OLD", device="old", scopes=["A"]) for i in range(30)]
        )
        staging_done = threading.Event()
        release = threading.Event()
        stop = []
        seen = []

        def reader_loop():
            while not stop:
                result = self.cat.query(allowed_scopes=["A"])
                seen.append({r["record_id"] for r in result["records"]})
                time.sleep(0.001)

        def slow_replace():
            def gen():
                yield _record("n0", building="NEW", scopes=["A"])
                staging_done.set()
                for i in range(1, 40):
                    yield _record(f"n{i}", building="NEW", scopes=["A"])
                release.wait()

            self.cat.replace(gen())

        reader = threading.Thread(target=reader_loop)
        writer = threading.Thread(target=slow_replace)
        reader.start()
        writer.start()
        self.assertTrue(staging_done.wait(timeout=5))
        time.sleep(0.02)
        release.set()
        writer.join(timeout=10)
        self.assertFalse(writer.is_alive())
        stop.append(1)
        reader.join(timeout=10)
        self.assertFalse(reader.is_alive())

        old_set = {f"o{i}" for i in range(30)}
        new_set = {f"n{i}" for i in range(40)}
        for snapshot in seen:
            self.assertIn(snapshot, (old_set, new_set))
        result = self.cat.query(allowed_scopes=["A"])
        self.assertEqual({r["record_id"] for r in result["records"]}, new_set)


if __name__ == "__main__":
    unittest.main(verbosity=2)