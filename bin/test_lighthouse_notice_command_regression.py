"""Isolated regression tests for the manual notice_command start path.

Drives the REAL ``PortalAgent.prepare -> amend -> confirm`` lifecycle against
in-process native FastAPI fake endpoints and the REAL
``MaintenancePortalService.expand_workbench_action_command`` +
``MaintenancePortalService.create_action_job``.  The fake workbench endpoint is
typed with the real ``WorkbenchActionRequest`` model and, on each POST, runs the
real expand and the real ``create_action_job`` (persist/temp-store stubbed).
No network, no real business data, no credentials, no Qt worker.
"""
import asyncio
import copy
import json
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request

from clipflow_backend.api_models import WorkbenchActionRequest

from lan_bitable_template_portal.lighthouse_agent import PLAN_NAMESPACE, PortalAgent, _result_refs
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog, _notice_frontend_fields
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles
from lan_bitable_template_portal.portal_service import MaintenancePortalService, PortalError
from lan_bitable_template_portal.workbench_lite import _form_fields

from test_lighthouse_agent_workflows import ACTOR, Store

OPERATION_ID = "notice_command_regression_00000001"

BASE_DRAFT = {
    "title": "A楼设备调整",
    "start_time": "2026-09-30 09:00",
    "end_time": "2026-09-30 11:00",
    "location": "A楼机房",
    "content": "调整机组参数",
    "reason": "设备更新",
    "impact": "短时停机",
    "specialty": "电气",
    "building_codes": ["A"],
}

VALID_STARTS = {
    "maintenance": {**BASE_DRAFT, "maintenance_cycle": "每月", "execution_party": "厂维", "progress": "已完成60%"},
    "change": {**BASE_DRAFT, "level": "低", "execution_party": "厂维", "progress": "已完成60%"},
    "repair": {**BASE_DRAFT, "level": "中", "repair_device": "空调1号机", "repair_fault": "故障A",
               "fault_type": "硬件", "repair_mode": "更换", "discovery": "巡检", "symptom": "异响",
               "solution": "更换备件", "progress": "已完成60%"},
    "power": {**BASE_DRAFT, "notice_type": "上电通告", "cabinet": "A-01", "quantity": "2", "progress": "已完成60%"},
    "polling": {**BASE_DRAFT, "device": "水泵1号", "progress": "已完成60%"},
    "adjust": dict(BASE_DRAFT),
}

# Polling-specific internal fields that must appear exactly once in the UI model.
POLLING_INTERNAL_FIELDS = ("device", "content", "impact")


def _as_payload(body: WorkbenchActionRequest) -> dict:
    """Reconstruct the full request dict including allowed extra keys."""
    return {**(body.model_dump()), **(body.model_extra or {})}


def make_request():
    return Request({
        "type": "http",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("127.0.0.1", 123),
        "path": "/api/assistant/plans/x",
        "root_path": "",
        "query_string": b"",
        "headers": [(b"origin", b"http://testserver"), (b"cookie", b"fixture=a")],
    })


class _IsolatedService:
    """Real MaintenancePortalService runnable via object.__new__ with isolated deps.

    ``validate_manual_source_binding`` and ``repair_management_notice_prefill``
    are stubbed to only record; ``_jobs_lock``/``_jobs`` are initialised
    in-memory and ``_persist_action_job_locked`` is a no-op so the real
    ``create_action_job`` dedup logic runs without touching SQLite.
    """

    def __init__(self):
        self.service = object.__new__(MaintenancePortalService)
        self.binding_calls = []
        self.prefill_calls = []
        self._summary_lock = threading.RLock()
        self._jobs_lock = threading.RLock()
        self._jobs = {}
        # Real command expansion may consult the active store for update/end only.
        self.service._state_store = SimpleNamespace(
            list_qt_active_items=lambda include_deleted=False: [],
            resolve_notice_identity=lambda **_kwargs: None,
            put_documents=lambda _namespace, _documents: None,
        )
        self.service._jobs_lock = self._jobs_lock
        self.service._jobs = self._jobs
        self.service._persist_action_job_locked = lambda _job: None

        def _record_binding(**kwargs):
            self.binding_calls.append(copy.deepcopy(kwargs))
            return {"source_record_id": kwargs.get("source_record_id", ""),
                    "work_type": kwargs.get("work_type", ""),
                    "title": kwargs.get("source_record_id", "")}

        def _fake_prefill(record_id, *, scope="ALL"):
            self.prefill_calls.append({"record_id": record_id, "scope": scope})
            return copy.deepcopy({
                "source_record_id": record_id,
                "repair_management_record_id": record_id,
                "target_record_id": "",
                "source_record": {"record_id": record_id, "scope": scope, "work_type": "repair"},
                "draft": {
                    "work_type": "repair",
                    "notice_type": "检修通告",
                    "title": BASE_DRAFT["title"],
                    "location": BASE_DRAFT["location"],
                    "content": BASE_DRAFT["content"],
                    "start_time": BASE_DRAFT["start_time"],
                    "end_time": BASE_DRAFT["end_time"],
                },
            })

        self.service.validate_manual_source_binding = _record_binding
        self.service.repair_management_notice_prefill = _fake_prefill

    def expand(self, payload, scope="A"):
        return self.service.expand_workbench_action_command(
            copy.deepcopy(payload), scope=scope, ongoing_items=[]
        )

    def create_job(self, payload):
        return self.service.create_action_job(copy.deepcopy(payload))


class NoticeCommandRegressionBase(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")

        model = Mock()
        model.settings.return_value = {
            "configured": True,
            "enabled": True,
            "active_model_id": "default",
            "models": [{"id": "default", "name": "测试模型", "model": "fixture-model", "configured": True}],
        }
        model.profile.return_value = {"id": "default", "name": "测试模型", "model": "fixture-model"}
        search = Mock(side_effect=AssertionError("agent must use fake native APIs, not local/DOM scraping"))
        self.assistant = LighthouseAssistant(self.store, search, model=model)
        self.files = LighthouseFiles(self.store)

        self.notice_writes = []
        self.expanded_writes = []
        self.job_ids = []
        self.sources = [
            {"source_record_id": "src-1", "repair_management_record_id": "src-1", "record_id": "src-1",
             "title": "A楼调整", "status": "未开始", "scope": "A", "work_type": "maintenance"},
        ]

        app = self.app = FastAPI()
        # The sending endpoint is real logic: it runs real expand and real
        # create_action_job on every POST (persist/jobs stubbed in _IsolatedService).
        self.isolated = _IsolatedService()

        @app.post("/api/workbench-actions")
        async def workbench_actions(body: WorkbenchActionRequest):
            payload = _as_payload(body)
            self.notice_writes.append(copy.deepcopy(payload))
            scope = str(payload.get("scope") or "A")
            expanded = self.isolated.expand(payload, scope=scope)
            self.expanded_writes.append(copy.deepcopy(expanded))
            job_id, _should_start = self.isolated.create_job(payload)
            self.job_ids.append(job_id)
            # No job_id in the reply: the confirm flow completes without a Qt
            # worker / job poll, while the real create_action_job has already
            # run and deduplicated on this operation_id.
            return {"ok": True, "data": {"accepted": True}}

        @app.get("/api/workbench/source-options")
        async def source_options():
            return {"ok": True, "data": {"items": self.sources}}

        self.catalog = PortalAPICatalog(app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)
        self.request = make_request()

    # ---------- helpers ----------
    def _start_decision(self, work_type="repair", *, body_extra=None, patch_extra=None):
        body = {
            "command_format": "notice_command",
            "scope": "A",
            "work_type": work_type,
            "action": "start",
            "manual": True,
            "polling_work_order_exempt": True,
            "patch": {**copy.deepcopy(VALID_STARTS[work_type]), **(patch_extra or {})},
        }
        body.update(body_extra or {})
        return {"operations": [{"api_id": "POST /api/workbench-actions", "body": body}]}

    def _prepare_start(self, work_type, *, body_extra=None, patch_extra=None):
        return self.agent.prepare(
            ACTOR,
            self._start_decision(work_type, body_extra=body_extra, patch_extra=patch_extra),
            OPERATION_ID + "_" + work_type,
            [],
        )

    def _stored(self, plan_id):
        return self.store.get_document(PLAN_NAMESPACE, plan_id)

    def _field(self, plan, name, path=None):
        for f in plan["fields"]:
            if f["name"] == name and (path is None or f.get("path") == path):
                return f
        self.fail("missing field %r (path=%r); fields: %r"
                  % (name, path, [(f.get("name"), f.get("path")) for f in plan["fields"]]))

    def _notice_field(self, plan, index=0):
        return self._field(plan, "step%d.patch" % index, path="patch")

    def _notice_values(self, plan, **overrides):
        values = copy.deepcopy(self._notice_field(plan)["_initial_form"])
        values.update(overrides)
        return values

    async def _load_sources(self, plan):
        source = self._field(plan, "step0.source_record_id", path="source_record_id")
        loaded = await self.agent.field_options(ACTOR, plan["id"], source["name"], self.request)
        self.current_version = loaded["version"]
        return loaded

    def _body_after_submit(self, plan_id):
        stored = self._stored(plan_id)
        return _result_refs(stored["operations"][0]["body"], [], stored["_references"], stored["_queries"])

    async def _amend_unbound(self, plan):
        notice = self._notice_field(plan)
        amended = self.agent.amend(ACTOR, plan["id"], {
            "version": plan["version"],
            "values": {
                "step0.manual_binding_choice": "unbound",
                "step0.source_record_id": "",
                notice["name"]: self._notice_values(plan, progress="已完成80%"),
            },
        })
        self.assertEqual(amended["status"], "awaiting_confirmation", amended)
        return self._body_after_submit(plan["id"])

    async def _amend_bind(self, plan, *, source="src-1"):
        loaded = await self._load_sources(plan)
        notice = self._notice_field(plan)
        amended = self.agent.amend(ACTOR, plan["id"], {
            "version": loaded["version"],
            "values": {
                "step0.manual_binding_choice": "bind",
                "step0.source_record_id": source,
                notice["name"]: self._notice_values(plan, progress="已完成80%"),
            },
        })
        self.assertEqual(amended["status"], "awaiting_confirmation", amended)
        return self._body_after_submit(plan["id"])

    async def _confirm_and_complete(self, plan_id):
        stored = self._stored(plan_id)
        review = await self.agent.confirm(
            ACTOR, plan_id, {"version": stored["version"], "stage": "review"}, self.request)
        self.assertEqual(review["status"], "awaiting_second_confirmation", review)
        stored2 = self._stored(plan_id)
        await self.agent.confirm(
            ACTOR, plan_id, {"version": stored2["version"], "stage": "execute"}, self.request)
        if self.agent.tasks:
            await asyncio.gather(*tuple(self.agent.tasks))
        return self._stored(plan_id)


class UnboundStartManualIdRegressionTests(NoticeCommandRegressionBase):
    """Expand-level: unbound start keeps manual_id and clears ids for all types."""

    async def test_unbound_start_preserves_manual_id_and_clears_ids_for_all_types(self):
        for work_type in VALID_STARTS:
            with self.subTest(work_type=work_type):
                self.isolated = _IsolatedService()
                plan = self._prepare_start(work_type)
                self.assertEqual(plan["status"], "needs_input")
                binding = self._field(plan, "step0.manual_binding_choice", path="manual_binding_choice")
                self.assertTrue(binding.get("native_notice_binding"))
                body = await self._amend_unbound(plan)
                self.assertTrue(str(body.get("manual_id") or "").startswith("manual_"),
                                "agent must generate a stable manual_id")
                manual_id = body["manual_id"]
                self.assertTrue(body.get("manual"), "unbound start must stay manual")
                # Real backend expansion keeps manual_id and does not forge ids.
                expanded = self.isolated.expand(body, scope="A")
                self.assertEqual(expanded.get("manual_id"), manual_id,
                                 "expand_workbench_action_command dropped manual_id (work_type=%s)" % work_type)
                self.assertNotIn("source_record_id", expanded)
                self.assertNotIn("target_record_id", expanded)
                self.assertTrue(expanded.get("manual"))
                # prepare/amend alone must not trigger a backend write.
                self.assertEqual(self.notice_writes, [])

    async def test_double_expansion_is_safe(self):
        self.isolated = _IsolatedService()
        plan = self._prepare_start("maintenance")
        body = await self._amend_unbound(plan)
        manual_id = body["manual_id"]
        first = self.isolated.expand(body, scope="A")
        second = self.isolated.expand(first, scope="A")
        self.assertEqual(second.get("manual_id"), manual_id)
        self.assertEqual(second.get("manual"), True)
        self.assertNotIn("source_record_id", second)
        self.assertNotIn("target_record_id", second)
        self.assertEqual(second.get("title"), first.get("title"))
        self.assertEqual(second.get("work_type"), "maintenance")
        # Double expansion must not fabricate a target identity.
        self.assertNotIn("src-1", json.dumps(second))


class RealSendRegressionTests(NoticeCommandRegressionBase):
    """Real prepare -> amend -> confirm drives the fake typed endpoint which runs
    the real expand and the real create_action_job."""

    async def test_confirm_sends_once_and_creates_one_job(self):
        self.isolated = _IsolatedService()
        plan = self._prepare_start("adjust")
        body = await self._amend_unbound(plan)
        before = len(self.isolated._jobs)
        completed = await self._confirm_and_complete(plan["id"])
        self.assertEqual(completed["status"], "completed")
        # The endpoint was invoked exactly once by the confirm flow.
        self.assertEqual(len(self.notice_writes), 1)
        self.assertEqual(len(self.expanded_writes), 1)
        # The real create_action_job created exactly one job.
        self.assertEqual(len(self.isolated._jobs), before + 1)
        self.assertEqual(len(self.job_ids), 1)
        # The real expand kept manual_id and the unbound identity.
        expanded = self.expanded_writes[0]
        self.assertEqual(expanded.get("manual_id"), body["manual_id"])
        self.assertNotIn("source_record_id", expanded)
        self.assertNotIn("target_record_id", expanded)
        self.assertTrue(expanded.get("manual"))
        # The real guard (inside create_action_job) accepted the manual_id; no
        # '开始通告缺少计划记录ID' error was raised.
        self.assertEqual(self.notice_writes[0].get("manual_id"), body["manual_id"])

    async def test_same_operation_repeated_does_not_create_second_job(self):
        self.isolated = _IsolatedService()
        plan = self._prepare_start("adjust")
        body = await self._amend_unbound(plan)
        await self._confirm_and_complete(plan["id"])
        self.assertEqual(len(self.isolated._jobs), 1)
        first_job_id = self.job_ids[0]
        # Re-run the real create_action_job with the same resolved body.
        job_id, should_start = self.isolated.service.create_action_job(self._body_after_submit(plan["id"]))
        self.assertEqual(job_id, first_job_id)
        self.assertFalse(should_start, "same operation must not start a second job")
        self.assertEqual(len(self.isolated._jobs), 1)

    async def test_missing_manual_id_raises_portal_error(self):
        self.isolated = _IsolatedService()
        payload = {
            "command_format": "notice_command",
            "scope": "A",
            "work_type": "adjust",
            "action": "start",
            "manual": True,
            "manual_binding_choice": "unbound",
            "patch": copy.deepcopy(VALID_STARTS["adjust"]),
        }
        with self.assertRaises(PortalError) as ctx:
            self.isolated.create_job(payload)
        self.assertIn("开始通告缺少计划记录ID", str(ctx.exception))


class BoundStartSourceRegressionTests(NoticeCommandRegressionBase):
    """Bind keeps the source field and uses only recorded/stubbed deps."""

    async def test_selected_bind_keeps_source_field(self):
        self.isolated = _IsolatedService()
        plan = self._prepare_start("maintenance")
        body = await self._amend_bind(plan, source="src-1")
        self.assertEqual(body.get("source_record_id"), "src-1")
        expanded = self.isolated.expand(body, scope="A")
        self.assertEqual(expanded.get("source_record_id"), "src-1",
                         "bind must preserve the source_record_id")
        self.assertNotIn("target_record_id", expanded)
        self.assertTrue(self.isolated.binding_calls, "validate_manual_source_binding must be invoked")
        # Stub only recorded; it never performed a cloud write.
        self.assertEqual(self.notice_writes, [])

    async def test_validate_manual_source_binding_only_records_no_cloud_write(self):
        self.isolated = _IsolatedService()
        plan = self._prepare_start("maintenance")
        body = await self._amend_bind(plan, source="src-1")
        expanded = self.isolated.expand(body, scope="A")
        self.assertEqual(len(self.isolated.binding_calls), 1)
        call = self.isolated.binding_calls[0]
        self.assertEqual(call.get("source_record_id"), "src-1")
        self.assertEqual(call.get("work_type"), "maintenance")
        self.assertEqual(call.get("scope"), "A")
        # The stub returned synchronously; no remote/cloud write happened.
        self.assertEqual(expanded.get("source_record_id"), "src-1")

    async def test_repair_prefill_uses_isolated_fake(self):
        self.isolated = _IsolatedService()
        plan = self._prepare_start("repair")
        body = await self._amend_bind(plan, source="src-1")
        expanded = self.isolated.expand(body, scope="A")
        self.assertEqual(len(self.isolated.prefill_calls), 1,
                         "repair bind must use repair_management_notice_prefill")
        self.assertEqual(self.isolated.prefill_calls[0]["record_id"], "src-1")
        # source_record_id / repair_management_record_id / source-record identity
        # are all consistent (src-1), never a mismatched src-repair.
        self.assertEqual(expanded.get("source_record_id"), "src-1")
        self.assertEqual(expanded.get("repair_management_record_id"), "src-1")
        self.assertNotIn("target_record_id", expanded)


class NoticeTemplateMetadataTests(unittest.TestCase):
    """native_notice metadata: no duplicate UI child paths and the native HTML agrees."""

    def test_no_duplicate_ui_field_paths_for_any_work_type(self):
        with_location = {"maintenance", "change", "repair", "adjust"}
        for work_type in ("maintenance", "change", "repair", "power", "polling", "adjust"):
            with self.subTest(work_type=work_type):
                desc = _notice_frontend_fields(work_type, ["A"])
                paths = [child["path"] for child in desc["children"]]
                self.assertEqual(len(paths), len(set(paths)),
                                 f"{work_type} duplicate child paths: {paths}")
                self.assertEqual(paths.count("building_codes"), 1, work_type)
                self.assertEqual(paths.count("location"),
                                 1 if work_type in with_location else 0, work_type)
                self.assertEqual(paths.count("notice_type"),
                                 1 if work_type == "power" else 0, work_type)
                # The unified building_codes input must not also surface a raw
                # 'building' child (duplicate declaration guard).
                self.assertEqual(paths.count("building"), 0, work_type)
                for field in POLLING_INTERNAL_FIELDS:
                    expected = 1 if field in paths else 0
                    self.assertEqual(paths.count(field), expected,
                                     f"{work_type}/{field} duplicate")

    def test_native_html_has_no_duplicate_field_names(self):
        for work_type in ("maintenance", "change", "repair", "power", "polling", "adjust"):
            with self.subTest(work_type=work_type):
                html = _form_fields(work_type, {}, scope="A")
                self.assertLessEqual(html.count('name="location"'), 1, work_type)
                self.assertLessEqual(html.count('name="title"'), 1, work_type)
                self.assertLessEqual(html.count('name="content"'), 1, work_type)


class PrepareRetainsNoticeNativeFieldsTests(NoticeCommandRegressionBase):
    """The LLM-provided extra fields must survive real prepare into the public
    native notice field (building/location/notice_type mapped onto native
    children), and polling extras carried on the body must not be dropped."""

    async def test_llm_extra_fields_retained_in_public_native_field(self):
        self.isolated = _IsolatedService()
        body_extra = {
            "polling_work_order_exempt": True,
            "polling_run_count": 3,
            "polling_runs": [
                {"run_index": "1", "run_label": "第1轮"},
                {"run_index": "2", "run_label": "第2轮"},
            ],
        }
        # building/location are native children for adjust; notice_type is a
        # native child only for power, so cover both mappings here.
        scenarios = [
            ("adjust", {"building": "A楼", "location": "A楼机房"},
             {"building_codes": ["A"], "location": "A楼机房"}),
            ("power", {"building": "A楼", "notice_type": "上电通告"},
             {"building_codes": ["A"], "notice_type": "上电通告"}),
        ]
        for work_type, patch_extra, expected in scenarios:
            with self.subTest(work_type=work_type):
                plan = self._prepare_start(work_type, body_extra=body_extra, patch_extra=patch_extra)
                self.assertEqual(plan["status"], "needs_input")
                notice = self._notice_field(plan)
                self.assertTrue(notice.get("native_notice"))
                initial = self._notice_field(plan)["_initial_form"]
                for key, value in expected.items():
                    self.assertEqual(initial[key], value,
                                     f"{work_type}/{key} lost from native field")
                # Public plan only exposes these business values through the
                # single native_notice control; no duplicate raw 'building' child.
                native_fields = [f for f in plan["fields"] if f.get("native_notice")]
                self.assertEqual(len(native_fields), 1)
                self.assertNotIn("building", initial)
                # Non-native extras (polling_run_count / polling_runs) are still
                # retained on the stored operation body so the later real
                # create_action_job keeps them for the send.
                stored = self._stored(plan["id"])
                op_body = stored["operations"][0]["body"]
                self.assertEqual(op_body.get("polling_run_count"), 3)
                self.assertEqual(op_body.get("polling_runs")[0]["run_index"], "1")
                body = self._body_after_submit(plan["id"])
                if "patch" in body:
                    self.assertEqual(body["patch"].get("building"), "A楼")


if __name__ == "__main__":
    unittest.main(verbosity=2)
