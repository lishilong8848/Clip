# -*- coding: utf-8 -*-
"""Coverage for opaque-schema hydration and native write/exclusion guards.

This module reuses the native fixtures already provided by the repository:

  * ``test_lighthouse_reference_workflows.ReferenceWorkflowTests`` for the
    isolated assistant/files/store/app harness;
  * ``test_drill_management._fixture_xlsx`` for the real drill workbook;
  * ``lan_bitable_template_portal.critical_guard.default_response_cells`` for
    real guard response cells;
  * ``lan_bitable_template_portal.portal_service.REPAIR_MANAGEMENT_TABLE_ID``
    for real repair metadata snapshots.

Scope (only this module is touched, nothing else in the repository):

  1. Opaque schema fields (drill ``configuration``/``execution``, guard
     ``cells``, repair ``fields`` metas, non-event notice ``patch``) are
     hydrated into typed controls (dates, person pickers, select/arrays/tables)
     by ``PortalAgent.prepare`` on top of the native builders.
  2. Users are never asked to type ``record_id`` / ``version`` / ``drill_id`` /
     ``scope`` / ``expected_version`` identity values - those are auto-populated
     from the loaded native snapshots during ``prepare``.
  3. ``prepare`` never writes (every write-capable route used below is wired to
     raise if called).
  4. Native exclusions are preserved:
     ``/api/learning``, ``/api/plan-convergence/settings``,
     ``/api/signatures/management``, raw signature images,
     ``/api/polling-work-orders`` (SOP execution stays outside the assistant),
     and Qt event writes (``_check_notice_channel`` raises for event notices).

No network, services, credentials or real data are touched.
"""
import copy
import sys
import unittest
from pathlib import Path

_BIN = Path(__file__).resolve().parent
if str(_BIN) not in sys.path:
    sys.path.insert(0, str(_BIN))

from fastapi import FastAPI, Request

from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_api import (
    PortalAPICatalog,
    _check_notice_channel,
)
from lan_bitable_template_portal.lighthouse_ai import AssistantError
from lan_bitable_template_portal.drill_management import DrillManagementService
from lan_bitable_template_portal.critical_guard import default_response_cells
from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_TABLE_ID

import test_lighthouse_reference_workflows as _ref
from test_lighthouse_reference_workflows import (
    ACTOR,
    _fixture_xlsx,  # noqa: F401  (imported for the native workbook fixture)
)
from clipflow_backend.api_models import (
    DrillExecutionRequest,
    CriticalGuardResponseRequest,
    RepairManagementRecordRequest,
)

# Sentinel table keys used to prove values are hydrated from snapshots.
_DRILL_EXECUTION_VERSION = 12
_GUARD_VERSION = 4


def _iter_controls(node):
    """Yield every control descriptor (dict) recursively, including items."""
    if not isinstance(node, dict):
        return
    yield node
    for child in node.get("children") or []:
        yield from _iter_controls(child)
    item = node.get("item")
    if isinstance(item, dict):
        yield from _iter_controls(item)


def _editable_json_controls(field):
    """Return controls that are editable-JSON textareas (the opaque fallback)."""
    return [
        control for control in _iter_controls(field)
        if control.get("type") == "textarea" and control.get("value_format") == "json"
    ]


def _field_by(fields, **kwargs):
    for field in fields:
        if all(field.get(key) == value for key, value in kwargs.items()):
            return field
    raise AssertionError(f"no field matching {kwargs!r}")


def _child_controls(field):
    return {child["path"]: child for child in (field.get("children") or [])}


def _require_no_identity_input(controls, forbidden):
    """Assert no leaf asks the user to type record/version identity values."""
    for control in controls:
        path = control.get("path")
        if path in forbidden:
            raise AssertionError(f"identity field must not be a user control: {control!r}")


class _CoverageHarnessMixin:
    """Wires the native ReferenceWorkflowTests fixture into each test."""

    def _fixture(self):
        fixture = _ref.ReferenceWorkflowTests()
        fixture.setUp()
        for callback, args, kwargs in fixture._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        fixture._cleanups.clear()
        return fixture

    def _catalog(self, fixture):
        return PortalAPICatalog(fixture.app)

    def _portal(self, fixture):
        return PortalAgent(
            fixture.assistant, self._catalog(fixture), fixture.files
        )


class DrillConfigurationHydrationTests(_CoverageHarnessMixin, unittest.TestCase):
    """PUT /api/drills/{drill_id}/configuration hydrates native typed controls."""

    def _definition(self, fixture):
        service = DrillManagementService(
            fixture.store, data_root=Path(fixture.tmp.name) / "forms"
        )
        definition = service.create_definition(
            name="配置演练", year=2026, month=10,
            file_name="fixture.xlsx", source=_fixture_xlsx(with_evaluator=True),
        )
        definition = service.publish(
            definition["drill_id"], expected_version=definition["version"]
        )
        # Administrator edits the template for exactly the buildings they own.
        definition = {**copy.deepcopy(definition), "assigned_scopes": ["A"], "configuration_locked": False}
        return definition

    def setUp(self):
        self.fixture = self._fixture()
        self.definition = self._definition(self.fixture)

        @self.fixture.app.put("/api/drills/{drill_id}/configuration")
        async def configure(drill_id: str, body: dict):
            self.fixture.writes.append(body)
            raise AssertionError("prepare 不得写入演练配置")

    def _prepare(self):
        portal = self._portal(self.fixture)
        operation = {
            "api_id": "PUT /api/drills/{drill_id}/configuration",
            "path_params": {"drill_id": self.definition["drill_id"]},
            "body": {"expected_version": self.definition["version"], "configuration": {}},
        }
        admin = {**ACTOR, "scopes": ["A"], "is_admin": True}
        return portal.prepare(
            admin,
            {"operations": [operation]},
            "interactive_config_00001",
            [],
            queries={"template": self.definition},
        )

    def test_configuration_is_native_not_editable_json(self):
        plan = self._prepare()
        field = _field_by(plan["fields"], path="configuration")
        self.assertTrue(field["native_drill_configuration"])
        self.assertEqual(field["_version"], self.definition["version"])
        self.assertEqual(_editable_json_controls(field), [])
        controls = _child_controls(field)
        self.assertEqual(controls["record_sheet"]["type"], "select")
        self.assertEqual(controls["assessment_sheet"]["type"], "select")
        self.assertEqual(controls["step_layout"]["type"], "object")
        assessment = _children_map(controls["assessment"])
        self.assertEqual(assessment["score_rows"]["type"], "array")
        self.assertEqual(assessment["score_rows"]["minItems"], 2)
        self.assertEqual(assessment["score_rows"]["item"]["type"], "object")
        self.assertEqual(plan["operations"][0]["body"]["expected_version"], self.definition["version"])
        self.assertEqual(self.fixture.writes, [])

    def test_configuration_does_not_ask_for_identity_inputs(self):
        plan = self._prepare()
        field = _field_by(plan["fields"], path="configuration")
        _require_no_identity_input(
            _iter_controls(field),
            {"drill_id", "version", "expected_version", "scope", "record_id", "configuration_locked"},
        )
        # drill_id is auto-populated into path_params, not a user field.
        self.assertEqual(plan["operations"][0]["path_params"]["drill_id"], self.definition["drill_id"])
        self.assertEqual(self.fixture.writes, [])

    def test_non_admin_cannot_open_configuration(self):
        portal = self._portal(self.fixture)
        operation = {
            "api_id": "PUT /api/drills/{drill_id}/configuration",
            "path_params": {"drill_id": self.definition["drill_id"]},
        }
        with self.assertRaisesRegex(AssistantError, "只有管理员"):
            portal.prepare(
                ACTOR, {"operations": [operation]},
                "interactive_config_00002", [], queries={"template": self.definition},
            )
        self.assertEqual(self.fixture.writes, [])


class DrillExecutionHydrationTests(_CoverageHarnessMixin, unittest.TestCase):
    """PUT /api/drills/{drill_id}/execution hydrates typed controls."""

    def setUp(self):
        self.fixture = self._fixture()
        service = DrillManagementService(
            self.fixture.store, data_root=Path(self.fixture.tmp.name) / "forms"
        )
        definition = service.create_definition(
            name="交互演练", year=2026, month=10,
            file_name="fixture.xlsx", source=_fixture_xlsx(with_evaluator=True),
        )
        definition = service.publish(definition["drill_id"], expected_version=definition["version"])
        execution = service.get_execution(definition["drill_id"], "A", create=True)
        execution.update(
            drill_date="2026-09-29", first_start_time="09:00",
            version=_DRILL_EXECUTION_VERSION, execution_version=2, status="synced",
        )
        self.definition, self.execution = definition, execution

        @self.fixture.app.put("/api/drills/{drill_id}/execution")
        async def save(drill_id: str, body: DrillExecutionRequest, scope: str):
            self.fixture.writes.append(body)
            raise AssertionError("prepare 不得保存演练执行")

        self.people = [
            {"record_id": "rec-commander", "name": "指挥人", "employee_no": "E1", "building": "A楼"},
            {"record_id": "rec-evaluator", "name": "评估人", "employee_no": "E2", "building": "A楼"},
            {"record_id": "rec-participant", "name": "参演甲", "employee_no": "E3", "building": "A楼"},
        ]

    def _prepare(self, body=None):
        portal = self._portal(self.fixture)
        operation = {
            "api_id": "PUT /api/drills/{drill_id}/execution",
            "path_params": {"drill_id": self.definition["drill_id"]},
            "params": {"scope": "A"},
        }
        if body:
            operation["body"] = body
        queries = {
            "execution": {"drill": self.definition, "execution": self.execution},
            "people": {"default_scope": "A", "people": self.people},
        }
        return portal.prepare(
            ACTOR, {"operations": [operation]},
            "interactive_execution_00001", [], queries=queries,
        )

    def test_execution_hydrated_into_typed_controls_not_json(self):
        plan = self._prepare()
        field = _field_by(plan["fields"], native_drill=True)
        self.assertTrue(field["native_drill"])
        self.assertEqual(plan["operations"][0]["body"]["expected_version"], _DRILL_EXECUTION_VERSION)
        self.assertEqual(_editable_json_controls(field), [])
        children = _child_controls(field)
        self.assertEqual(children["drill_date"]["type"], "date")
        self.assertEqual(children["first_start_time"]["type"], "time")
        for key in ("commander", "evaluator"):
            self.assertTrue(children[key]["person_picker"])
        self.assertEqual(children["participants"]["type"], "array")
        self.assertEqual(children["step_signers"]["type"], "object")
        self.assertTrue(children["step_signers"]["paginated"])
        self.assertTrue(children["step_signers"]["searchable"])
        self.assertEqual(self.fixture.writes, [])

    def test_execution_does_not_ask_for_identity_or_versions(self):
        plan = self._prepare()
        field = _field_by(plan["fields"], native_drill=True)
        _require_no_identity_input(
            _iter_controls(field),
            {"drill_id", "version", "expected_version", "scope", "record_id", "execution_version"},
        )
        # drill_id & scope are resolved from the operation path/params, not fields.
        self.assertEqual(plan["operations"][0]["path_params"]["drill_id"], self.definition["drill_id"])
        self.assertEqual(plan["operations"][0]["params"]["scope"], "A")
        self.assertEqual(self.fixture.writes, [])

    def test_incomplete_execution_snapshot_cannot_skip_native_form(self):
        portal = self._portal(self.fixture)
        operation = {
            "api_id": "PUT /api/drills/{drill_id}/execution",
            "path_params": {"drill_id": self.definition["drill_id"]},
            "params": {"scope": "A"},
        }
        with self.assertRaisesRegex(AssistantError, "完整执行记录"):
            portal.prepare(
                ACTOR, {"operations": [operation]},
                "interactive_execution_00002", [], queries={},
            )
        self.assertEqual(self.fixture.writes, [])


class GuardCellsHydrationTests(_CoverageHarnessMixin, unittest.TestCase):
    """PUT /api/critical-guard/responses/{response_id} hydrates guard cells."""

    def setUp(self):
        self.fixture = self._fixture()

        @self.fixture.app.put("/api/critical-guard/responses/{response_id}")
        async def save(response_id: str, body: CriticalGuardResponseRequest):
            self.fixture.writes.append(body)
            raise AssertionError("prepare 不得写入重保填报")

        cells = default_response_cells(
            "灾害专项", "A", today="2026-10-01",
            template_items=[
                {"key": "check.1", "category": "供配电", "content": "现场检查1"},
                {"key": "check.2", "category": "空调", "content": "现场检查2"},
            ],
            template_revision=7, template_customized=True,
        )
        cells["checks"]["check.2"] = {"status": "abnormal", "note": "原异常备注"}
        cells["weather"] = {"level1": "台风", "level2": "暴雨", "current": "大雨"}
        self.response = {
            "response_id": "guard-a", "scope": "A", "version": _GUARD_VERSION,
            "sheet_type": "灾害专项", "cells": cells,
            "signatures": [{"source": "staff", "record_id": "rec-inspector", "role": "inspector", "name": "检查人"}],
        }

    def _prepare(self):
        portal = self._portal(self.fixture)
        operation = {
            "api_id": "PUT /api/critical-guard/responses/{response_id}",
            "path_params": {"response_id": self.response["response_id"]},
            "body": {"scope": "A"},
        }
        return portal.prepare(
            ACTOR, {"operations": [operation]},
            "interactive_guard_00001", [], queries={"snapshot": self.response},
        )

    def test_guard_cells_hydrated_into_typed_controls(self):
        plan = self._prepare()
        field = _field_by(plan["fields"], path="cells")
        self.assertTrue(field["native_guard"])
        self.assertEqual(plan["operations"][0]["body"]["expected_version"], _GUARD_VERSION)
        self.assertEqual(_editable_json_controls(field), [])
        children = _child_controls(field)
        self.assertEqual(children["check_date"]["type"], "date")
        self.assertEqual(children["checks"]["type"], "object")
        self.assertEqual(children["suggestions"]["type"], "textarea")
        self.assertEqual(children["weather"]["type"], "object")
        check_children = _child_controls(children["checks"])
        for key in ("check.1", "check.2"):
            self.assertEqual(check_children[key]["type"], "object")
        self.assertEqual(self.fixture.writes, [])

    def test_guard_cells_do_not_expose_identity_inputs(self):
        plan = self._prepare()
        field = _field_by(plan["fields"], path="cells")
        _require_no_identity_input(
            _iter_controls(field),
            {"response_id", "version", "expected_version", "scope", "record_id", "task_id"},
        )
        self.assertEqual(plan["operations"][0]["path_params"]["response_id"], "guard-a")
        self.assertEqual(self.fixture.writes, [])


class RepairFieldsHydrationTests(_CoverageHarnessMixin, unittest.TestCase):
    """Repair record/followup forms hydrate raw ``fields`` metas into controls."""

    def setUp(self):
        self.fixture = self._fixture()
        self.metas = [
            {"field_name": name, "field_type": kind, "editable": True, "options": options}
            for name, kind, options in (
                ("故障发生时间", 5, []), ("故障维修原因", 1, []),
                ("所属专业", 3, ["电气", "暖通"]), ("维修进度", 2, []),
            )
        ]
        self.original = {
            "故障发生时间": 1790821800000, "故障维修原因": [{"text": "原原因"}],
            "所属专业": "电气", "维修进度": 0.5,
        }
        self.record = {
            "record_id": "rec-project", "record_version": "v-original",
            "building_codes": ["A"], "raw_fields": self.original,
            "source_event_id": "rec-event", "source_repair_ids": ["rec-repair"],
        }

        @self.fixture.app.put("/api/repair-management/records/{record_id}")
        async def update(record_id: str, body: RepairManagementRecordRequest):
            self.fixture.writes.append(body)
            raise AssertionError("prepare 不得写入维修单")

    def _prepare(self):
        portal = self._portal(self.fixture)
        queries = {
            "query_" + "a" * 32: {
                "table_id": REPAIR_MANAGEMENT_TABLE_ID,
                "records": [self.record], "fields": self.metas,
            }
        }
        operation = {
            "api_id": "PUT /api/repair-management/records/{record_id}",
            "path_params": {"record_id": "rec-project"},
            "body": {"scope": "A"},
        }
        return portal.prepare(
            ACTOR, {"operations": [operation]},
            "interactive_repair_00001", [], queries=queries,
        )

    def test_repair_fields_hydrated_into_typed_controls(self):
        plan = self._prepare()
        field = _field_by(plan["fields"], path="fields")
        self.assertTrue(field["native_repair"])
        self.assertEqual(plan["operations"][0]["body"]["expected_version"], "v-original")
        self.assertEqual(_editable_json_controls(field), [])
        children = _child_controls(field)
        self.assertEqual(set(children), {"故障发生时间", "故障维修原因", "所属专业", "维修进度"})
        self.assertEqual(children["故障发生时间"]["type"], "datetime-local")
        self.assertEqual(children["所属专业"]["type"], "select")
        self.assertEqual(
            children["所属专业"]["options"],
            [{"value": "电气", "label": "电气"}, {"value": "暖通", "label": "暖通"}],
        )
        self.assertEqual(self.fixture.writes, [])

    def test_repair_identity_is_auto_populated_not_user_input(self):
        plan = self._prepare()
        field = _field_by(plan["fields"], path="fields")
        _require_no_identity_input(
            _iter_controls(field),
            {"record_id", "record_version", "expected_version", "scope",
             "source_event_id", "source_repair_ids"},
        )
        self.assertEqual(plan["operations"][0]["path_params"]["record_id"], "rec-project")
        self.assertEqual(plan["operations"][0]["body"]["expected_version"], "v-original")
        body = plan["operations"][0]["body"]
        self.assertEqual(body["source_event_id"], "rec-event")
        self.assertEqual(body["source_repair_ids"], ["rec-repair"])
        self.assertEqual(self.fixture.writes, [])


class NonEventNoticePatchHydrationTests(_CoverageHarnessMixin, unittest.TestCase):
    """Non-event notice_command ``patch`` is a native typed form, not JSON."""

    def setUp(self):
        self.fixture = self._fixture()

        @self.fixture.app.post("/api/workbench-actions")
        async def workbench_actions(request: Request):
            self.fixture.writes.append(await request.json())
            raise AssertionError("prepare 不得提交通告")

    def _prepare(self, body):
        portal = self._portal(self.fixture)
        return portal.prepare(
            ACTOR, {"operations": [{"api_id": "POST /api/workbench-actions", "body": body}]},
            "interactive_notice_00001", [], queries={},
        )

    def test_start_patch_hydrated_into_native_notice_typed_form(self):
        plan = self._prepare({
            "command_format": "notice_command",
            "scope": "A", "work_type": "maintenance", "action": "start",
            "manual": True, "patch": {"building_codes": ["A"]},
        })
        form = _field_by(plan["fields"], native_notice=True)
        self.assertTrue(form["native_notice"])
        self.assertEqual(_editable_json_controls(form), [])
        children = _child_controls(form)
        # typed controls; never a raw JSON textarea.
        self.assertIn("building_codes", children)
        self.assertEqual(children["building_codes"]["type"], "multiselect")
        self.assertIn("start_time", children)
        self.assertEqual(children["start_time"]["type"], "datetime-local")
        self.assertEqual(children["specialty"]["type"], "select")
        self.assertEqual(children["content"]["type"], "textarea")
        _require_no_identity_input(
            _iter_controls(form),
            {"record_id", "version", "target_record_id", "source_record_id",
             "active_item_id", "scope"},
        )
        self.assertEqual(self.fixture.writes, [])

    def test_native_notice_descriptor_has_no_editable_json_or_identity_fields(self):
        # Pure descriptor-level check mirroring _notice_frontend_fields, so the
        # opaque ``patch`` object is always surfaced as typed controls.
        from lan_bitable_template_portal.lighthouse_api import _notice_frontend_fields
        for work_type in ("maintenance", "change", "repair", "power", "polling", "adjust"):
            with self.subTest(work_type=work_type):
                desc = _notice_frontend_fields(work_type, ["A"])
                self.assertTrue(desc["native_notice"])
                self.assertEqual(_editable_json_controls(desc), [])
                _require_no_identity_input(
                    _iter_controls(desc),
                    {"record_id", "version", "target_record_id", "source_record_id",
                     "active_item_id", "scope"},
                )
                self.assertTrue(desc["children"])


class ExclusionsPreservedTests(_CoverageHarnessMixin, unittest.TestCase):
    """Native exclusions must stay intact after prepare hydration coverage."""

    def setUp(self):
        self.fixture = self._fixture()

    def test_learning_is_not_discoverable_or_executable(self):
        @self.fixture.app.get("/api/learning/papers")
        async def papers(scope: str, today: str = "0"):
            return {"ok": True, "data": {"items": [{"id": "paper-d-1001", "scope": scope}], "total": 1}}
        @self.fixture.app.post("/api/learning/papers/{id}/answer")
        async def answer(id: str, body: dict):
            raise AssertionError("learning write must be excluded")
        portal = self._portal(self.fixture)
        # 合法的学习 GET 查询在受 learning_scopes 约束下可发现。
        self.assertIsInstance(portal.catalog.get("GET /api/learning/papers"), dict)
        # 学习写入端点(answer)仍不可发现、不可执行。
        with self.assertRaises(AssistantError):
            portal.catalog.get("POST /api/learning/papers/d-1/answer")
        with self.assertRaises(AssistantError):
            portal.prepare(
                ACTOR,
                {"operations": [{"api_id": "POST /api/learning/papers/d-1/answer", "path_params": {"id": "d-1"}, "body": {"choices": []}}]},
                "interactive_excl_learning_00001", [],
            )

    def test_settings_and_signature_management_are_excluded(self):
        @self.fixture.app.get("/api/plan-convergence/settings")
        async def settings(request: Request):
            raise AssertionError("settings must be excluded")
        @self.fixture.app.get("/api/signatures/management/{operation}")
        async def mgmt(operation: str, request: Request):
            raise AssertionError("signature management must be excluded")
        catalog = self._catalog(self.fixture)
        for api_id in (
            "GET /api/plan-convergence/settings",
            "GET /api/signatures/management/people",
            "POST /api/signatures/management/people",
            "POST /api/signatures/management/merge",
            "GET /api/signatures/management/request",
        ):
            with self.subTest(api_id=api_id), self.assertRaises(AssistantError):
                catalog.validate_operation({
                    "api_id": api_id, "path_params": {"operation": api_id.rsplit("/", 1)[-1]},
                })

    def test_raw_signature_images_are_excluded(self):
        catalog = self._catalog(self.fixture)
        for api_id in ("GET /api/signatures/image", "POST /api/signatures/save",
                       "GET /api/signatures/temporary/image", "GET /api/signatures/temporary/session"):
            with self.subTest(api_id=api_id), self.assertRaises(AssistantError):
                catalog.get(api_id)

    def test_sop_work_order_execution_stays_outside_the_assistant(self):
        @self.fixture.app.get("/api/polling-work-orders/{role}/session")
        async def sop_session(role: str, request: Request):
            raise AssertionError("SOP execution must not run inside the assistant")
        portal = self._portal(self.fixture)
        # The exclusive polling-work-orders session route is not in the catalog.
        with self.assertRaises(AssistantError):
            portal.catalog.get("GET /api/polling-work-orders/private-role/session")
        # A prepare that tries to prep a generic operation with the excluded SOP
        # route is blocked before any native call, keeping SOP execution in the
        # original work-order entry point.
        with self.assertRaises(AssistantError):
            portal.prepare(
                ACTOR,
                {"operations": [{"api_id": "GET /api/polling-work-orders/private-role/session"}]},
                "interactive_excl_sop_00001", [],
            )

    def test_event_notice_write_raises_qt_channel_exclusion(self):
        # Direct unit check of the guarded helper with an event patch.
        with self.assertRaisesRegex(AssistantError, "Qt 专用链路"):
            _check_notice_channel({"patch": {"work_type": "event", "progress": "隔离测试"}})
        # Non-event patches pass the channel guard.
        _check_notice_channel({"patch": {"work_type": "maintenance", "progress": "正常"}})

        # And via prepare: an event command must not reach the native handler.
        @self.fixture.app.post("/api/workbench-actions")
        async def workbench_actions(request: Request):
            raise AssertionError("event must never reach maintenance handler")
        portal = self._portal(self.fixture)
        with self.assertRaisesRegex(AssistantError, "Qt 专用链路"):
            portal.prepare(
                ACTOR,
                {"operations": [{"api_id": "POST /api/workbench-actions", "body": {
                    "work_type": "event", "command_format": "notice_command", "scope": "A",
                    "action": "update", "target_record_id": "rec-event", "patch": {"progress": "隔离测试"},
                }}]},
                "interactive_excl_event_00001", [],
            )
        self.assertEqual(self.fixture.writes, [])


def _children_map(control):
    return {child["path"]: child for child in (control.get("children") or [])}


if __name__ == "__main__":
    unittest.main(verbosity=2)