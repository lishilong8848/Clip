"""Isolated regression tests for paired maintenance finalization.

Focus areas (matching the Codex-side fix that is still in progress):

1. ``_execute_backend_prepared_upload`` for ``end`` actions: when the request /
   local projection has no site photo (the mirror-maintenance row does not show
   up in the local list), the backend must re-read the *real* target via
   ``query_record_by_id`` and validate its ``site_images`` before allowing the
   write.  A real target that has images must proceed, while a real target
   without images, or a failed read-back, must still block.  In mock_external
   mode the missing-photo rule must still be honoured.

2. Paired-maintenance upload finalization: automatic upload is retried at most
   3 times.  Below the cap the job stays ``remote_written`` and is requeued with
   ``paired_upload_pending=True``.  Once the cap is exceeded the job goes to
   ``phase=failed`` with ``remote_written=True``, ``paired_upload_pending=True``,
   ``error_retryable=True``, keeping its ``prepared`` payload and its original
   remote-operation id.  A later transient success (including via a manual
   ``retry_action_job``) must let the job finish without re-writing the main
   change.

These tests use the real service / state-store (temporary SQLite) plus
targeted mocks, and do not touch network, GUI or OpenClaw.
"""

import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

import lan_bitable_template_portal.server as portal_server_module  # noqa: E402
from lan_bitable_template_portal.server import PortalRuntime  # noqa: E402
from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402

MAINTENANCE_NOTICE_TYPE = "维保通告"
SITE_PHOTO_FIELD = "过程现场图片"


class _TestMaintenancePortalService(MaintenancePortalService):
    """Local no-op service used to isolate runtime tests without touching the
    reference work-status test module (which pulls in extra third-party deps)."""

    def refresh(self) -> None:
        return

    def ensure_loaded(self) -> None:
        return

    def _current_repair_event_fields(self, record_id):
        return {"是否转检修": True}


class EndPhotoGateTests(unittest.TestCase):
    def _prepare_end(self, *, target_record_id="rec-mirror-main"):
        return {
            "notice_type": MAINTENANCE_NOTICE_TYPE,
            "action": "end",
            "target_record_id": target_record_id,
            "job_id": "job-end-photo",
            "work_type": "maintenance",
        }

    def test_end_no_local_photo_rereads_target_site_images_and_proceeds(self):
        """When the local projection has no photo but the real target has
        site_images, the real (non-mock) backend re-reads the target and
        performs exactly one bitable write."""
        prepared = self._prepare_end()
        target = prepared["target_record_id"]
        with (
            # The real backend path (real_write_allowed) is what performs the
            # read-back; mock_external never reaches it.
            patch.object(
                portal_server_module,
                "external_real_write_guard",
                return_value={
                    "mock_external": False,
                    "real_write_allowed": True,
                    "reason": "",
                },
            ),
            patch.object(
                PortalRuntime,
                "_has_cumulative_site_photo_for_notice",
                return_value=False,
            ),
            # Stub work-order guard and attachment paths like the existing
            # work-status tests so no external/local side effects are needed.
            patch.object(PortalRuntime, "_work_order_end_error", return_value=""),
            patch.object(
                PortalRuntime, "_create_backend_undo_checkpoint", return_value=""
            ),
            patch.object(PortalRuntime, "_mark_local_notice_images_target_written"),
            patch.object(
                PortalRuntime,
                "_upload_extra_images_for_notice",
                return_value=(True, "", [], []),
            ),
            patch.object(
                PortalRuntime,
                "_upload_change_confirmation_images",
                return_value=(True, "", [], []),
            ),
            patch.object(PortalRuntime, "_stage_notice_robot_delivery"),
            patch.object(
                portal_server_module,
                "query_record_by_id",
                return_value=(
                    True,
                    {"fields": {SITE_PHOTO_FIELD: [{"file_token": "site-tok-1"}]}},
                ),
            ) as query_record,
            patch.object(
                portal_server_module,
                "update_bitable_record_by_payload",
                return_value=(True, "rec-mirror-main"),
            ) as update,
            patch.object(
                portal_server_module, "update_bitable_record_fields"
            ) as update_fields,
            patch.object(portal_server_module, "create_bitable_record_by_payload"),
            patch.object(portal_server_module, "upload_media_to_feishu"),
        ):
            ok, message, record_id = PortalRuntime._execute_backend_prepared_upload(
                prepared
            )

        self.assertTrue(ok, message)
        self.assertEqual(record_id, "rec-mirror-main")
        query_record.assert_called_once_with(target, MAINTENANCE_NOTICE_TYPE)
        # The remote-photo path performs exactly one real bitable update.
        update.assert_called_once()
        update_fields.assert_not_called()

    def test_end_no_local_photo_blocks_when_target_has_no_site_images(self):
        """A real target without site_images must keep blocking the end upload
        on the real (non-mock) backend; no bitable write is performed."""
        prepared = self._prepare_end()
        with (
            patch.object(
                portal_server_module,
                "external_real_write_guard",
                return_value={
                    "mock_external": False,
                    "real_write_allowed": True,
                    "reason": "",
                },
            ),
            patch.object(
                PortalRuntime,
                "_has_cumulative_site_photo_for_notice",
                return_value=False,
            ),
            patch.object(PortalRuntime, "_work_order_end_error", return_value=""),
            patch.object(
                PortalRuntime, "_create_backend_undo_checkpoint", return_value=""
            ),
            patch.object(PortalRuntime, "_mark_local_notice_images_target_written"),
            patch.object(
                PortalRuntime,
                "_upload_extra_images_for_notice",
                return_value=(True, "", [], []),
            ),
            patch.object(
                PortalRuntime,
                "_upload_change_confirmation_images",
                return_value=(True, "", [], []),
            ),
            patch.object(PortalRuntime, "_stage_notice_robot_delivery"),
            patch.object(
                portal_server_module,
                "query_record_by_id",
                return_value=(True, {"fields": {SITE_PHOTO_FIELD: []}}),
            ) as query_record,
            patch.object(
                portal_server_module, "update_bitable_record_by_payload"
            ) as update,
            patch.object(
                portal_server_module, "update_bitable_record_fields"
            ) as update_fields,
            patch.object(portal_server_module, "create_bitable_record_by_payload"),
            patch.object(portal_server_module, "upload_media_to_feishu"),
        ):
            ok, message, record_id = PortalRuntime._execute_backend_prepared_upload(
                prepared
            )

        self.assertFalse(ok)
        self.assertIn("现场照片", message or "")
        query_record.assert_called_once()
        update.assert_not_called()
        update_fields.assert_not_called()

    def test_end_no_local_photo_blocks_when_target_read_fails(self):
        """If the read-back of the actual target fails, the end upload must be
        blocked on the real (non-mock) backend (never assume the write can
        proceed)."""
        prepared = self._prepare_end()
        with (
            patch.object(
                portal_server_module,
                "external_real_write_guard",
                return_value={
                    "mock_external": False,
                    "real_write_allowed": True,
                    "reason": "",
                },
            ),
            patch.object(
                PortalRuntime,
                "_has_cumulative_site_photo_for_notice",
                return_value=False,
            ),
            patch.object(PortalRuntime, "_work_order_end_error", return_value=""),
            patch.object(
                PortalRuntime, "_create_backend_undo_checkpoint", return_value=""
            ),
            patch.object(PortalRuntime, "_mark_local_notice_images_target_written"),
            patch.object(
                PortalRuntime,
                "_upload_extra_images_for_notice",
                return_value=(True, "", [], []),
            ),
            patch.object(
                PortalRuntime,
                "_upload_change_confirmation_images",
                return_value=(True, "", [], []),
            ),
            patch.object(PortalRuntime, "_stage_notice_robot_delivery"),
            patch.object(
                portal_server_module,
                "query_record_by_id",
                return_value=(False, "读取失败：目标记录不可用"),
            ) as query_record,
            patch.object(
                portal_server_module, "update_bitable_record_by_payload"
            ) as update,
            patch.object(
                portal_server_module, "update_bitable_record_fields"
            ) as update_fields,
            patch.object(portal_server_module, "create_bitable_record_by_payload"),
            patch.object(portal_server_module, "upload_media_to_feishu"),
        ):
            ok, message, record_id = PortalRuntime._execute_backend_prepared_upload(
                prepared
            )

        self.assertFalse(ok)
        query_record.assert_called_once()
        update.assert_not_called()
        update_fields.assert_not_called()

    def test_end_mock_external_still_rejects_missing_site_images(self):
        """mock_external short-circuits before any read-back or write: the
        missing-photo rule is still honoured without touching the network."""
        prepared = self._prepare_end()
        with (
            patch.object(
                portal_server_module,
                "external_real_write_guard",
                return_value={"mock_external": True, "real_write_allowed": False},
            ),
            patch.object(
                PortalRuntime,
                "_has_cumulative_site_photo_for_notice",
                return_value=False,
            ),
            patch.object(
                portal_server_module,
                "query_record_by_id",
                return_value=(True, {"fields": {SITE_PHOTO_FIELD: []}}),
            ) as query_record,
            patch.object(
                portal_server_module, "update_bitable_record_by_payload"
            ) as update,
            patch.object(
                portal_server_module, "update_bitable_record_fields"
            ) as update_fields,
            patch.object(portal_server_module, "create_bitable_record_by_payload"),
        ):
            ok, message, _record_id = PortalRuntime._execute_backend_prepared_upload(
                prepared
            )

        self.assertFalse(ok)
        self.assertIn("现场照片", message or "")
        # mock_external intentionally NEVER gets the target or writes it back.
        query_record.assert_not_called()
        update.assert_not_called()
        update_fields.assert_not_called()


class PairedFinalizationTests(unittest.TestCase):
    def _make_remote_written_end_job(
        self,
        service,
        *,
        job_suffix,
        paired_retry_count=0,
    ):
        target = "rec-mirror-main"
        job_id, _should_start = service.create_action_job(
            {
                "action": "end",
                "scope": "A",
                "record_id": "source-main",
                "source_record_id": "source-main",
                "target_record_id": target,
                "active_item_id": "active-main",
                "work_type": "maintenance",
                "notice_type": MAINTENANCE_NOTICE_TYPE,
                "operation_id": f"op-paired-{job_suffix}",
            }
        )
        prepared = {
            "_remote_operation_id": f"notice_action:{job_id}",
            "action": "end",
            "work_type": "maintenance",
            "notice_type": MAINTENANCE_NOTICE_TYPE,
            "scope": "A",
            "source_record_id": "source-main",
            "active_item_id": "active-main",
            "target_record_id": target,
            "status": "结束",
            "progress": "已结束",
            "title": "A楼维保结束",
            "text": "【维保通告】状态：结束\n【名称】A楼维保结束",
            "sync_maintenance_target": True,
            "paired_maintenance_upload": {
                "action": "end",
                "work_type": "maintenance",
                "notice_type": MAINTENANCE_NOTICE_TYPE,
                "scope": "A",
                "source_record_id": "source-main",
                "target_record_id": target,
                "title": "A楼维保结束镜像",
                "text": "【维保通告】状态：结束",
            },
            "skip_personal_message": True,
            "message_sent": True,
        }
        service.mark_job(
            job_id,
            phase="remote_written",
            remote_written=True,
            remote_record_id=target,
            target_record_id=target,
            remote_result_message="主变更已写入",
            restart_recovered=True,
            prepared=prepared,
        )
        if paired_retry_count:
            with service._jobs_lock:
                service._jobs[job_id]["paired_upload_retry_count"] = paired_retry_count
                service._persist_action_job_locked(service._jobs[job_id])
        return job_id, target

    def _query_patcher(self):
        return patch.object(
            portal_server_module,
            "query_record_by_id",
            return_value=(
                True,
                {
                    "fields": {
                        "名称": "A楼维保结束",
                        "维保状态": "结束",
                    },
                    "record_version": "v1",
                },
            ),
        )

    def test_paired_second_retry_below_cap_stays_remote_written_and_pending(self):
        """The automatic cap is exactly 3: a retry that would reach count 2 is
        still below the cap, so the job stays remote_written and is requeued."""
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            store = service._state_store
            job_id, _target = self._make_remote_written_end_job(
                service, job_suffix="below", paired_retry_count=1
            )
            try:
                with (
                    patch.object(PortalRuntime, "service", service),
                    patch.object(PortalRuntime, "state_store", store),
                    self._query_patcher(),
                    patch.object(
                        PortalRuntime,
                        "_execute_paired_maintenance_upload",
                        return_value=(False, "paired outage", ""),
                    ),
                    patch.object(
                        store, "requeue_runtime_queue_item", return_value=True
                    ),
                ):
                    PortalRuntime._process_maintenance_action_job(job_id)

                job = service.get_job(job_id)
                self.assertEqual(job["phase"], "remote_written")
                self.assertTrue(job["remote_written"])
                self.assertTrue(job["paired_upload_pending"])
                self.assertEqual(job["paired_upload_retry_count"], 2)
            finally:
                store.shutdown_write_worker(timeout=2.0)

    def test_paired_failure_exceeding_cap_marks_failed_with_expected_flags(self):
        """After the max automatic attempts the job becomes phase=failed while
        keeping remote_written / pending / retryable and its prepared payload.
        The 2->3 transition is the exact cap boundary."""
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            store = service._state_store
            job_id, _target = self._make_remote_written_end_job(
                service, job_suffix="cap", paired_retry_count=2
            )
            try:
                with (
                    patch.object(PortalRuntime, "service", service),
                    patch.object(PortalRuntime, "state_store", store),
                    self._query_patcher(),
                    patch.object(
                        PortalRuntime,
                        "_execute_paired_maintenance_upload",
                        return_value=(False, "paired outage", ""),
                    ),
                    # Even if the queue would accept another requeue, the cap must
                    # win and stop automatic retries.
                    patch.object(
                        store, "requeue_runtime_queue_item", return_value=True
                    ) as requeue,
                ):
                    PortalRuntime._process_maintenance_action_job(job_id)

                job = service.get_job(job_id)
                self.assertEqual(job["phase"], "failed")
                self.assertTrue(job["remote_written"])
                self.assertTrue(job["paired_upload_pending"])
                self.assertTrue(job["error_retryable"])
                # The exact cap was reached: 2 -> 3.
                self.assertEqual(job["paired_upload_retry_count"], 3)
                # The cap prevents another automatic requeue.
                requeue.assert_not_called()
                # Prepared payload is preserved for a later manual retry.
                self.assertTrue(job["prepared"].get("sync_maintenance_target"))
                self.assertTrue(job["prepared"].get("paired_maintenance_upload"))
                # Original operation id is kept in the preserved prepared payload.
                self.assertEqual(
                    job["prepared"].get("_remote_operation_id"),
                    f"notice_action:{job_id}",
                )
            finally:
                store.shutdown_write_worker(timeout=2.0)

    def test_paired_transient_failure_then_success_does_not_rewrite_main(self):
        """A short-lived paired failure is requeued, and when the mirror upload
        later succeeds the whole job finishes without re-writing the main
        change (``_execute_backend_prepared_upload`` is never called)."""
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            store = service._state_store
            job_id, _target = self._make_remote_written_end_job(
                service, job_suffix="transient", paired_retry_count=0
            )
            try:
                with (
                    patch.object(PortalRuntime, "service", service),
                    patch.object(PortalRuntime, "state_store", store),
                    self._query_patcher(),
                    patch.object(
                        PortalRuntime, "_execute_backend_prepared_upload"
                    ) as remote_upload,
                ):
                    with (
                        patch.object(
                            PortalRuntime,
                            "_execute_paired_maintenance_upload",
                            return_value=(False, "paired outage", ""),
                        ),
                        patch.object(
                            store, "requeue_runtime_queue_item", return_value=True
                        ),
                    ):
                        PortalRuntime._process_maintenance_action_job(job_id)

                    first = service.get_job(job_id)
                    self.assertEqual(first["phase"], "remote_written")
                    self.assertTrue(first["paired_upload_pending"])
                    self.assertEqual(first["paired_upload_retry_count"], 1)

                    with patch.object(
                        PortalRuntime,
                        "_execute_paired_maintenance_upload",
                        return_value=(True, "paired ok", "rec-paired"),
                    ):
                        PortalRuntime._process_maintenance_action_job(job_id)

                    final = service.get_job(job_id)
                    self.assertEqual(final["phase"], "success")
                    remote_upload.assert_not_called()
            finally:
                store.shutdown_write_worker(timeout=2.0)

    def test_paired_failed_job_manual_retry_continues_without_rewriting_main(self):
        """After the cap marks the job failed (error_retryable), a manual
        retry re-accepts the job and, once the mirror upload succeeds, the job
        finishes without re-writing the main change."""
        with tempfile.TemporaryDirectory() as tmp:
            service = self._new_temp_service(Path(tmp))
            store = service._state_store
            job_id, _target = self._make_remote_written_end_job(
                service, job_suffix="retry", paired_retry_count=3
            )
            try:
                with (
                    patch.object(PortalRuntime, "service", service),
                    patch.object(PortalRuntime, "state_store", store),
                    self._query_patcher(),
                    patch.object(
                        PortalRuntime, "_execute_backend_prepared_upload"
                    ) as remote_upload,
                ):
                    # Force cap-exceeded failure.
                    with (
                        patch.object(
                            PortalRuntime,
                            "_execute_paired_maintenance_upload",
                            return_value=(False, "paired outage", ""),
                        ),
                        patch.object(
                            store, "requeue_runtime_queue_item", return_value=True
                        ),
                    ):
                        PortalRuntime._process_maintenance_action_job(job_id)
                    failed = service.get_job(job_id)
                    self.assertEqual(failed["phase"], "failed")
                    self.assertTrue(failed["error_retryable"])

                    # Manual retry re-accepts the job and resets the cap counter.
                    service.retry_action_job(job_id)
                    retried = service.get_job(job_id)
                    self.assertEqual(retried["phase"], "accepted")
                    self.assertEqual(retried["paired_upload_retry_count"], 0)

                    # Mirror upload now succeeds; main change must not be re-run.
                    with patch.object(
                        PortalRuntime,
                        "_execute_paired_maintenance_upload",
                        return_value=(True, "paired ok", "rec-paired"),
                    ):
                        PortalRuntime._process_maintenance_action_job(job_id)
                    final = service.get_job(job_id)
                    self.assertEqual(final["phase"], "success")
                    remote_upload.assert_not_called()
            finally:
                store.shutdown_write_worker(timeout=2.0)

    def _new_temp_service(self, root, service_cls=_TestMaintenancePortalService):
        def fake_data_path(name):
            return str(root / name)

        patcher = patch(
            "lan_bitable_template_portal.portal_service.get_data_file_path",
            side_effect=fake_data_path,
        )
        store_patcher = patch(
            "lan_bitable_template_portal.state_store.get_data_file_path",
            side_effect=fake_data_path,
        )
        patcher.start()
        store_patcher.start()
        self.addCleanup(patcher.stop)
        self.addCleanup(store_patcher.stop)
        return service_cls()


if __name__ == "__main__":
    unittest.main(verbosity=2)