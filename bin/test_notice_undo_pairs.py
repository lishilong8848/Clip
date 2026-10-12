# -*- coding: utf-8 -*-
"""Isolated regression tests for paired (dual-write) notice undo.

This module only touches the paired-notice undo surface:
  * MaintenancePortalService.paired_notice_undo
  * LanPortalStateStore.list_notice_undo_actions(job_id=...) / mark_notice_undo_action(related_actions=...)
  * PortalRuntime.execute_notice_undo

All external IO (Feishu/bitable) is mocked and the real-write guard is forced
to "real write allowed" so we can prove that a paired undo converges to the
main change notice and restores both target tables.  Zero real network/server
activity is performed.  Temporary SQLite data lives under CLIPFLOW_DATA_DIR and
no real business data or credentials are read.
"""
import copy
import gc
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    NOTICE_TYPE_CHANGE,
    NOTICE_TYPE_MAINTENANCE,
    PortalError,
    WORK_TYPE_CHANGE,
    WORK_TYPE_MAINTENANCE,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402

PAIR_SUFFIX = ":paired-maintenance"
REAL_GUARD = {
    "mock_external": False,
    # confirmed stays 0: the guard is mocked so the "real write" code path is
    # exercised only against the also-mocked external functions below, never
    # against a genuinely confirmed external write.
    "require_confirm": True,
    "confirmed": False,
    "real_write_allowed": True,
    "reason": "",
}


def _service(tmpdir: str) -> MaintenancePortalService:
    service = MaintenancePortalService()
    service._state_store = LanPortalStateStore(Path(tmpdir) / "state.sqlite3")
    service._summary_dir = Path(tmpdir) / "summary"
    service._summary_dir.mkdir(parents=True, exist_ok=True)
    service._work_status_dir = Path(tmpdir) / "work_status"
    service._work_status_dir.mkdir(parents=True, exist_ok=True)
    service._hidden_ongoing_path = Path(tmpdir) / "hidden.json"
    return service


def _main_context(*, scope="A", source="src-change", target="rec-change", mirror_target="rec-mirror"):
    return {
        "scope": scope,
        "source_record_id": source,
        "target_record_id": target,
        "record_id": target,
        "active_item_id": "aid-change",
        "work_type": WORK_TYPE_CHANGE,
        "notice_type": NOTICE_TYPE_CHANGE,
        "sync_maintenance_target": True,
        "paired_maintenance_original_title": "A楼双发变更",
        "paired_maintenance_actual_start_time": "2026-10-12 09:00",
        "paired_maintenance_target_record_id": mirror_target,
        "paired_maintenance_upload": {
            "job_id": "jobX" + PAIR_SUFFIX,
            "source_record_id": source,
            "target_record_id": mirror_target,
        },
    }


def _mirror_context(*, scope="A", source="src-change", target="rec-mirror"):
    return {
        "scope": scope,
        "source_record_id": source,
        "target_record_id": target,
        "record_id": target,
        "active_item_id": "aid-mirror",
        "work_type": WORK_TYPE_MAINTENANCE,
        "notice_type": NOTICE_TYPE_MAINTENANCE,
        "paired_target_role": "maintenance_mirror",
    }


def _make_undo(
    store,
    *,
    action="update",
    identity_key,
    job_id,
    scope,
    work_type,
    notice_type,
    active_item_id,
    source_record_id,
    target_record_id,
    title,
    context,
    remote_fields,
    local=None,
    remote_missing=False,
    source_remote=None,
):
    return store.create_notice_undo_action(
        {
            "identity_key": identity_key,
            "identity_keys": [identity_key],
            "action_type": action,
            "job_id": job_id,
            "scope": scope,
            "work_type": work_type,
            "notice_type": notice_type,
            "active_item_id": active_item_id,
            "source_record_id": source_record_id,
            "target_record_id": target_record_id,
            "title": title,
            "context": copy.deepcopy(context),
            "remote": {"missing": remote_missing, "fields": dict(remote_fields)},
            "remote_missing": remote_missing,
            "source_remote": copy.deepcopy(source_remote or {}),
            "local": copy.deepcopy(local or {}),
            "created_at": time.time(),
            "expires_at": time.time() + 7 * 24 * 60 * 60,
        }
    )


def _make_pair(
    store,
    *,
    action="update",
    scope="A",
    source="src-change",
    main_target="rec-change",
    mirror_target="rec-mirror",
    job_id="jobX",
):
    main_context = _main_context(
        scope=scope, source=source, target=main_target, mirror_target=mirror_target
    )
    mirror_context = _mirror_context(scope=scope, source=source, target=mirror_target)
    main_id = _make_undo(
        store,
        action=action,
        identity_key=f"change:target:{main_target}",
        job_id=job_id,
        scope=scope,
        work_type=WORK_TYPE_CHANGE,
        notice_type=NOTICE_TYPE_CHANGE,
        active_item_id="aid-change",
        source_record_id=source,
        target_record_id=main_target,
        title="A楼双发变更",
        context=main_context,
        remote_fields={"名称": "A楼双发变更", "变更状态": "进行中"},
    )
    mirror_id = _make_undo(
        store,
        action=action,
        identity_key=f"maintenance:target:{mirror_target}",
        job_id=job_id + PAIR_SUFFIX,
        scope=scope,
        work_type=WORK_TYPE_MAINTENANCE,
        notice_type=NOTICE_TYPE_MAINTENANCE,
        active_item_id="aid-mirror",
        source_record_id=source,
        target_record_id=mirror_target,
        title="A楼双发变更",
        context=mirror_context,
        remote_fields={"名称": "A楼双发维保", "维保状态": "进行中"},
    )
    return main_id, mirror_id


def _make_end_pair_with_source_remote(
    store,
    *,
    scope="A",
    source="src-change",
    main_target="rec-change",
    mirror_target="rec-mirror",
    job_id="jobE",
):
    main_context = _main_context(
        scope=scope, source=source, target=main_target, mirror_target=mirror_target
    )
    main_context["source_progress"] = "结束回退中"
    mirror_context = _mirror_context(scope=scope, source=source, target=mirror_target)
    main_id = _make_undo(
        store,
        action="end",
        identity_key=f"change:target:{main_target}",
        job_id=job_id,
        scope=scope,
        work_type=WORK_TYPE_CHANGE,
        notice_type=NOTICE_TYPE_CHANGE,
        active_item_id="aid-change",
        source_record_id=source,
        target_record_id=main_target,
        title="A楼双发变更结束",
        context=main_context,
        remote_fields={"名称": "A楼双发变更", "变更状态": "已结束", "结束时间": "2026-10-12 10:00"},
        source_remote={
            "record_id": source,
            "work_type": WORK_TYPE_MAINTENANCE,
            "app_token": "app-src",
            "table_id": "tbl-src",
            "fields": {"维护实施状态": "进行中"},
        },
    )
    mirror_id = _make_undo(
        store,
        action="end",
        identity_key=f"maintenance:target:{mirror_target}",
        job_id=job_id + PAIR_SUFFIX,
        scope=scope,
        work_type=WORK_TYPE_MAINTENANCE,
        notice_type=NOTICE_TYPE_MAINTENANCE,
        active_item_id="aid-mirror",
        source_record_id=source,
        target_record_id=mirror_target,
        title="A楼双发维保结束",
        context=mirror_context,
        remote_fields={"名称": "A楼双发维保", "维保状态": "已结束"},
    )
    return main_id, mirror_id


class PairedNoticeUndoBase(unittest.TestCase):
    def setUp(self):
        self._tmpdir = tempfile.TemporaryDirectory()
        self.service = _service(self._tmpdir.name)
        self.store = self.service._state_store
        self._previous = (PortalRuntime.service, PortalRuntime.state_store)
        PortalRuntime.service = self.service
        PortalRuntime.state_store = self.store

    def tearDown(self):
        PortalRuntime.service, PortalRuntime.state_store = self._previous
        self.store.shutdown_write_worker(timeout=1.0)
        del self.service
        gc.collect()
        self._tmpdir.cleanup()

    def _guard_patch(self):
        return patch(
            "lan_bitable_template_portal.server.external_real_write_guard",
            return_value=dict(REAL_GUARD),
        )


class PairedNoticeUndoTests(PairedNoticeUndoBase):
    def test_paired_undo_converges_to_main_from_either_side_and_restores_both_tables(self):
        # A fresh pair is created for each side so the previously-undone
        # checkpoint is never reused.
        for click_side in ("main", "mirror"):
            with self.subTest(click_side=click_side):
                main_id, mirror_id = _make_pair(self.store, action="update")
                with self._guard_patch(), patch(
                    "lan_bitable_template_portal.server.query_record_by_id",
                    return_value=(False, "not found"),
                ) as query, patch(
                    "lan_bitable_template_portal.server.create_bitable_record_fields",
                    return_value=(True, "rec-change"),
                ) as create, patch(
                    "lan_bitable_template_portal.server.update_bitable_record_fields",
                    return_value=(True, "rec-change"),
                ) as upd:
                    clicked = main_id if click_side == "main" else mirror_id
                    result = PortalRuntime.execute_notice_undo(clicked)
                self.assertTrue(result["ok"])
                self.assertEqual(result["undo_id"], main_id)
                self.assertEqual(upd.call_count, 2)
                create.assert_not_called()
                query.assert_called()
                # Both checkpoints are marked undone together.
                self.assertEqual(
                    self.store.get_notice_undo_action(main_id)["status"], "undone"
                )
                self.assertEqual(
                    self.store.get_notice_undo_action(mirror_id)["status"], "undone"
                )
                # The mirror is not re-projected as its own active item here.
                self.assertEqual(len(self.store.list_qt_active_items()), 1)

    def test_available_undo_list_contains_only_main_not_mirror(self):
        main_id, mirror_id = _make_pair(self.store, action="update")
        available = self.service.list_available_notice_undos(scope="A")
        ids = [item["undo_id"] for item in available]
        self.assertIn(main_id, ids)
        self.assertNotIn(mirror_id, ids)
        self.assertEqual(len(available), 1)

    def test_single_table_not_paired(self):
        # Change with no sync flag never pairs.
        lone = _make_undo(
            self.store,
            action="update",
            identity_key="change:target:rec-lone",
            job_id="jobLone",
            scope="A",
            work_type=WORK_TYPE_CHANGE,
            notice_type=NOTICE_TYPE_CHANGE,
            active_item_id="aid-lone",
            source_record_id="src-lone",
            target_record_id="rec-lone",
            title="A楼普通变更",
            context={
                "scope": "A",
                "work_type": WORK_TYPE_CHANGE,
                "notice_type": NOTICE_TYPE_CHANGE,
            },
            remote_fields={"名称": "A楼普通变更"},
        )
        undo, paired = self.service.paired_notice_undo(
            self.store.get_notice_undo_action(lone)
        )
        self.assertEqual(undo["undo_id"], lone)
        self.assertIsNone(paired)

    def test_paired_undo_missing_mirror_fails_before_remote_write(self):
        main_id = _make_undo(
            self.store,
            action="update",
            identity_key="change:target:rec-change",
            job_id="jobX",
            scope="A",
            work_type=WORK_TYPE_CHANGE,
            notice_type=NOTICE_TYPE_CHANGE,
            active_item_id="aid-change",
            source_record_id="src-change",
            target_record_id="rec-change",
            title="A楼双发变更",
            context=_main_context(),
            remote_fields={"名称": "A楼双发变更"},
        )
        with self._guard_patch(), patch(
            "lan_bitable_template_portal.server.update_bitable_record_fields"
        ) as upd, patch(
            "lan_bitable_template_portal.server.delete_bitable_record"
        ) as deleter:
            with self.assertRaises(PortalError):
                PortalRuntime.execute_notice_undo(main_id)
        upd.assert_not_called()
        deleter.assert_not_called()
        self.assertEqual(self.store.get_notice_undo_action(main_id)["status"], "available")

    def test_paired_undo_same_title_or_source_different_job_never_cross_pairs(self):
        # Pair A and Pair B share title, source and scope but use different jobs.
        main_a, mirror_a = _make_pair(
            self.store, action="update", job_id="jobA", main_target="rec-change-a",
            mirror_target="rec-mirror-a",
        )
        main_b, mirror_b = _make_pair(
            self.store, action="update", job_id="jobB", main_target="rec-change-b",
            mirror_target="rec-mirror-b",
        )
        # paired_notice_undo over job A resolves to A's own snapshot halves.
        undo_a = self.store.get_notice_undo_action(main_a)
        main, paired = self.service.paired_notice_undo(undo_a)
        self.assertEqual(main["undo_id"], main_a)
        self.assertEqual(paired["undo_id"], mirror_a)
        # Executing A only touches A's two target tables.
        with self._guard_patch(), patch(
            "lan_bitable_template_portal.server.query_record_by_id",
            return_value=(False, "not found"),
        ), patch(
            "lan_bitable_template_portal.server.update_bitable_record_fields",
            return_value=(True, "rec-change-a"),
        ) as upd:
            result = PortalRuntime.execute_notice_undo(main_a)
        self.assertTrue(result["ok"])
        self.assertEqual(upd.call_count, 2)
        main_targets = [call.args[0] for call in upd.call_args_list]
        self.assertEqual(main_targets, ["rec-change-a", "rec-mirror-a"])
        self.assertEqual(self.store.get_notice_undo_action(main_b)["status"], "available")
        self.assertEqual(self.store.get_notice_undo_action(mirror_b)["status"], "available")

    def test_paired_undo_scope_or_source_conflict_fails_before_remote_write(self):
        for broken in ("scope", "source"):
            main_id = _make_undo(
                self.store,
                action="update",
                identity_key=f"change:target:rec-change-{broken}",
                job_id=f"jobX-{broken}",
                scope="A",
                work_type=WORK_TYPE_CHANGE,
                notice_type=NOTICE_TYPE_CHANGE,
                active_item_id="aid-change",
                source_record_id="src-change",
                target_record_id=f"rec-change-{broken}",
                title="A楼双发变更",
                context=_main_context(target=f"rec-change-{broken}", mirror_target="rec-mirror"),
                remote_fields={"名称": "A楼双发变更"},
            )
            mirror_scope = "B" if broken == "scope" else "A"
            mirror_source = "src-change" if broken == "scope" else "src-other"
            _make_undo(
                self.store,
                action="update",
                identity_key=f"maintenance:target:rec-mirror-{broken}",
                job_id=f"jobX-{broken}" + PAIR_SUFFIX,
                scope=mirror_scope,
                work_type=WORK_TYPE_MAINTENANCE,
                notice_type=NOTICE_TYPE_MAINTENANCE,
                active_item_id="aid-mirror",
                source_record_id=mirror_source,
                target_record_id="rec-mirror",
                title="A楼双发维保",
                context=_mirror_context(scope=mirror_scope, source=mirror_source, target="rec-mirror"),
                remote_fields={"名称": "A楼双发维保"},
            )
            with self._guard_patch(), patch(
                "lan_bitable_template_portal.server.update_bitable_record_fields"
            ) as upd, patch(
                "lan_bitable_template_portal.server.delete_bitable_record"
            ) as deleter:
                with self.assertRaises(PortalError):
                    PortalRuntime.execute_notice_undo(main_id)
            upd.assert_not_called()
            deleter.assert_not_called()

    def test_paired_undo_missing_target_snapshot_fails_before_remote_write(self):
        # A paired update whose main target snapshot is missing must abort.
        main_id = _make_undo(
            self.store,
            action="update",
            identity_key="change:target:rec-change",
            job_id="jobX",
            scope="A",
            work_type=WORK_TYPE_CHANGE,
            notice_type=NOTICE_TYPE_CHANGE,
            active_item_id="aid-change",
            source_record_id="src-change",
            target_record_id="rec-change",
            title="A楼双发变更",
            context=_main_context(),
            remote_fields={"名称": "A楼双发变更"},
            remote_missing=True,
        )
        _make_undo(
            self.store,
            action="update",
            identity_key="maintenance:target:rec-mirror",
            job_id="jobX" + PAIR_SUFFIX,
            scope="A",
            work_type=WORK_TYPE_MAINTENANCE,
            notice_type=NOTICE_TYPE_MAINTENANCE,
            active_item_id="aid-mirror",
            source_record_id="src-change",
            target_record_id="rec-mirror",
            title="A楼双发维保",
            context=_mirror_context(),
            remote_fields={"名称": "A楼双发维保"},
        )
        with self._guard_patch(), patch(
            "lan_bitable_template_portal.server.update_bitable_record_fields"
        ) as upd, patch(
            "lan_bitable_template_portal.server.delete_bitable_record"
        ) as deleter:
            with self.assertRaises(PortalError):
                PortalRuntime.execute_notice_undo(main_id)
        upd.assert_not_called()
        deleter.assert_not_called()

    def test_second_table_write_failure_keeps_both_available_and_updates_nothing(self):
        main_id, mirror_id = _make_pair(self.store, action="update")
        with self._guard_patch(), patch(
            "lan_bitable_template_portal.server.query_record_by_id",
            return_value=(False, "not found"),
        ), patch(
            "lan_bitable_template_portal.server.update_bitable_record_fields",
            side_effect=[(True, "rec-change"), (False, "维保目标表回退失败")],
        ) as upd, patch.object(
            self.service, "restore_notice_undo_local", wraps=self.service.restore_notice_undo_local
        ) as restore:
            with self.assertRaises(PortalError):
                PortalRuntime.execute_notice_undo(main_id)
        self.assertEqual(upd.call_count, 2)
        self.assertEqual(restore.call_count, 0)
        self.assertEqual(self.store.get_notice_undo_action(main_id)["status"], "available")
        self.assertEqual(self.store.get_notice_undo_action(mirror_id)["status"], "available")
        self.assertEqual(len(self.store.list_qt_active_items()), 0)

    def test_retry_same_snapshot_no_create_both_undone_only_main_projected(self):
        main_id, mirror_id = _make_pair(self.store, action="update")
        with self._guard_patch(), patch(
            "lan_bitable_template_portal.server.query_record_by_id",
            return_value=(False, "not found"),
        ), patch(
            "lan_bitable_template_portal.server.create_bitable_record_fields",
            return_value=(True, "rec-change"),
        ) as create, patch(
            "lan_bitable_template_portal.server.update_bitable_record_fields",
            # First run: main write succeeds, SECOND table (mirror) fails.
            # Second run (retry): both writes succeed.
            side_effect=[
                (True, "rec-change"),
                (False, "维保目标表回退失败"),
                (True, "rec-change"),
                (True, "rec-mirror"),
            ],
        ) as upd, patch.object(
            self.service, "restore_notice_undo_local",
            wraps=self.service.restore_notice_undo_local,
        ) as restore:
            with self.assertRaises(PortalError):
                PortalRuntime.execute_notice_undo(main_id)
            # First run PUT the main target and then the mirror target.
            self.assertEqual(upd.call_count, 2)
            self.assertEqual(
                [call.args[0] for call in upd.call_args_list],
                ["rec-change", "rec-mirror"],
            )
            # On the revoked run neither checkpoint is committed and the
            # source local state was not restored (restore runs only after
            # every remote write succeeds).
            self.assertEqual(restore.call_count, 0)
            self.assertEqual(
                self.store.get_notice_undo_action(main_id)["status"], "available"
            )
            self.assertEqual(
                self.store.get_notice_undo_action(mirror_id)["status"], "available"
            )
            self.assertEqual(len(self.store.list_qt_active_items()), 0)
            # Retry the exact same available snapshot.
            result = PortalRuntime.execute_notice_undo(main_id)
        self.assertTrue(result["ok"])
        create.assert_not_called()
        # Retry re-PUTs the same two original targets, never creating records.
        self.assertEqual(upd.call_count, 4)
        self.assertEqual(
            [call.args[0] for call in upd.call_args_list],
            ["rec-change", "rec-mirror", "rec-change", "rec-mirror"],
        )
        self.assertEqual(self.store.get_notice_undo_action(main_id)["status"], "undone")
        self.assertEqual(self.store.get_notice_undo_action(mirror_id)["status"], "undone")
        self.assertEqual(restore.call_count, 1)
        # Only the main change is projected as an ongoing Qt item, not a mirror.
        actives = self.store.list_qt_active_items()
        self.assertEqual(len(actives), 1)
        self.assertEqual(actives[0]["active_item_id"], "aid-change")

    def test_start_undo_deletes_both_tables_and_does_not_copy_mirror(self):
        main_id, mirror_id = _make_pair(self.store, action="start")
        with self._guard_patch(), patch(
            "lan_bitable_template_portal.server.query_record_by_id",
            side_effect=[(True, {"fields": {}}), (True, {"fields": {}})],
        ), patch(
            "lan_bitable_template_portal.server.delete_bitable_record",
            return_value=(True, "deleted"),
        ) as deleter, patch(
            "lan_bitable_template_portal.server.create_bitable_record_fields",
            return_value=(True, "rec-change"),
        ) as create:
            result = PortalRuntime.execute_notice_undo(main_id)
        self.assertTrue(result["ok"])
        # Both the change and the maintenance mirror rows are deleted.
        deleted_targets = [call.args[0] for call in deleter.call_args_list]
        self.assertEqual(deleted_targets, ["rec-change", "rec-mirror"])
        create.assert_not_called()
        self.assertEqual(self.store.get_notice_undo_action(main_id)["status"], "undone")
        self.assertEqual(self.store.get_notice_undo_action(mirror_id)["status"], "undone")
        # No mirror notice is copied back into ongoing Qt state.
        self.assertEqual(len(self.store.list_qt_active_items()), 0)

    def test_group_mark_unknown_related_returns_false_and_leaves_main_untouched(self):
        main_id, mirror_id = _make_pair(self.store, action="update")
        result = self.store.mark_notice_undo_action(
            main_id,
            "undone",
            payload_patch={"restored_target_record_id": "rec-change"},
            related_actions={"no-such-undo": {"restored_target_record_id": "x"}},
        )
        self.assertFalse(result)
        self.assertEqual(self.store.get_notice_undo_action(main_id)["status"], "available")
        self.assertEqual(self.store.get_notice_undo_action(mirror_id)["status"], "available")

    def test_group_mark_transaction_exception_does_not_partially_commit(self):
        main_id, mirror_id = _make_pair(self.store, action="update")
        # Prove a genuine MID-TRANSACTION failure (not a pre-executemany
        # serialization error): a BEFORE UPDATE trigger makes the SECOND batch
        # UPDATE raise sqlite3.IntegrityError. The first row (mirror) has
        # already been updated by executemany and must also be rolled back.
        trigger_name = "trg_undo_fail_second"
        with sqlite3.connect(str(self.store.db_path)) as conn:
            conn.executescript(f"DROP TRIGGER IF EXISTS {trigger_name}")
            conn.executescript(
                f"""
                CREATE TRIGGER {trigger_name}
                BEFORE UPDATE ON notice_undo_actions
                WHEN OLD.undo_id = '{main_id}'
                BEGIN
                    SELECT RAISE(ABORT, 'forced mid-transaction failure');
                END
                """
            )
        try:
            with self.assertRaises(sqlite3.IntegrityError):
                self.store.mark_notice_undo_action(
                    main_id,
                    "undone",
                    payload_patch={"restored_target_record_id": "rec-change"},
                    related_actions={
                        mirror_id: {"restored_target_record_id": "rec-mirror"}
                    },
                )
            # Both checkpoints remain 'available': the earlier mirror UPDATE was
            # rolled back together with the failed main UPDATE.
            self.assertEqual(
                self.store.get_notice_undo_action(main_id)["status"], "available"
            )
            self.assertEqual(
                self.store.get_notice_undo_action(mirror_id)["status"], "available"
            )
        finally:
            with sqlite3.connect(str(self.store.db_path)) as conn:
                conn.executescript(f"DROP TRIGGER IF EXISTS {trigger_name}")

    def test_paired_undo_missing_main_target_fails_before_remote_write(self):
        # RED-TEST intent: an empty MAIN target id must abort before any remote
        # write, exactly as the missing *mirror* target already does.  The
        # mirror snapshot here is complete, so only a missing main id remains.
        main_id = _make_undo(
            self.store,
            action="update",
            identity_key="change:target:rec-change",
            job_id="jobMissingMain",
            scope="A",
            work_type=WORK_TYPE_CHANGE,
            notice_type=NOTICE_TYPE_CHANGE,
            active_item_id="aid-change",
            source_record_id="src-change",
            target_record_id="",
            title="A楼双发变更",
            context=_main_context(target="", mirror_target="rec-mirror"),
            remote_fields={"名称": "A楼双发变更", "变更状态": "进行中"},
        )
        mirror_id = _make_undo(
            self.store,
            action="update",
            identity_key="maintenance:target:rec-mirror",
            job_id="jobMissingMain" + PAIR_SUFFIX,
            scope="A",
            work_type=WORK_TYPE_MAINTENANCE,
            notice_type=NOTICE_TYPE_MAINTENANCE,
            active_item_id="aid-mirror",
            source_record_id="src-change",
            target_record_id="rec-mirror",
            title="A楼双发维保",
            context=_mirror_context(),
            remote_fields={"名称": "A楼双发维保", "维保状态": "进行中"},
        )
        with self._guard_patch(), patch(
            "lan_bitable_template_portal.server.update_bitable_record_fields",
            return_value=(True, "rec-change"),
        ) as upd, patch(
            "lan_bitable_template_portal.server.create_bitable_record_fields",
            return_value=(True, "rec-change"),
        ) as create, patch(
            "lan_bitable_template_portal.server.delete_bitable_record",
            return_value=(True, "deleted"),
        ) as deleter:
            with self.assertRaises(PortalError):
                PortalRuntime.execute_notice_undo(main_id)
        # Production guards the empty MAIN target id before any remote write
        # (see the "not target_record_id" branch in execute_notice_undo), so a
        # PortalError is expected and no write occurs.
        upd.assert_not_called()
        create.assert_not_called()
        deleter.assert_not_called()
        self.assertEqual(self.store.get_notice_undo_action(main_id)["status"], "available")
        self.assertEqual(self.store.get_notice_undo_action(mirror_id)["status"], "available")

    def test_paired_end_with_source_remote_restores_source_once_and_projects_only_main(self):
        main_id, mirror_id = _make_end_pair_with_source_remote(self.store)
        self.service._records = []
        with self._guard_patch(), patch(
            "lan_bitable_template_portal.server.update_bitable_record_fields",
            side_effect=[(True, "rec-change"), (True, "rec-mirror")],
        ) as upd, patch.object(
            self.service,
            "_patch_record_fields",
            return_value={"code": 0, "msg": "success"},
        ) as patch_record, patch.object(
            self.service,
            "restore_notice_undo_related_remote",
            wraps=self.service.restore_notice_undo_related_remote,
        ) as related:
            result = PortalRuntime.execute_notice_undo(main_id)
        self.assertTrue(result["ok"])
        # Both target-table writes succeeded.
        self.assertEqual(upd.call_count, 2)
        self.assertEqual(
            [call.args[0] for call in upd.call_args_list],
            ["rec-change", "rec-mirror"],
        )
        # The end source_remote snapshot restores the source exactly once.
        self.assertEqual(related.call_count, 1)
        self.assertTrue(result["related_remote"]["restored"])
        self.assertEqual(patch_record.call_count, 1)
        # Only the main change is projected; the mirror is not copied into Qt.
        actives = self.store.list_qt_active_items()
        self.assertEqual(len(actives), 1)
        self.assertEqual(actives[0]["active_item_id"], "aid-change")
        self.assertEqual(self.store.get_notice_undo_action(main_id)["status"], "undone")
        self.assertEqual(self.store.get_notice_undo_action(mirror_id)["status"], "undone")

    def test_paired_end_with_source_remote_two_table_failure_does_not_restore_source(self):
        main_id, mirror_id = _make_end_pair_with_source_remote(self.store)
        self.service._records = []
        with self._guard_patch(), patch(
            "lan_bitable_template_portal.server.update_bitable_record_fields",
            side_effect=[(True, "rec-change"), (False, "维保目标表回退失败")],
        ) as upd, patch.object(
            self.service, "_patch_record_fields"
        ) as patch_record, patch.object(
            self.service,
            "restore_notice_undo_related_remote",
            wraps=self.service.restore_notice_undo_related_remote,
        ) as related:
            with self.assertRaises(PortalError):
                PortalRuntime.execute_notice_undo(main_id)
        # Main write occurred, second (mirror) write failed -> source must NOT
        # be restored and neither checkpoint may be committed.
        self.assertEqual(upd.call_count, 2)
        related.assert_not_called()
        patch_record.assert_not_called()
        self.assertEqual(self.store.get_notice_undo_action(main_id)["status"], "available")
        self.assertEqual(self.store.get_notice_undo_action(mirror_id)["status"], "available")
        self.assertEqual(len(self.store.list_qt_active_items()), 0)


if __name__ == "__main__":
    unittest.main()
