"""Repair assistant native device-catalog fidelity.

Focused isolated tests for the followup repair form consuming real snapshot
``brand_model_options`` / ``device_brand_model_options`` into ``repair_catalog``.
Uses only Store/ACTOR (not any test class) from test_lighthouse_agent_workflows as
helpers, plus fake Pydantic-validated native repair endpoints.  No real
cloud/provider access and no product edits.

Each test asserts a declared fidelity contract and reports the real outcome
against the current build:
  1. descriptors/catalogs: 50+ brand-model options survive ``public_plan`` untruncated,
  2. fields delta preserves untouched raw data and accepts a custom model,
  3. absence of snapshots leaves the pre-existing controls unchanged.
"""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI

from clipflow_backend.api_models import (RepairFollowupRecordRequest,
                                         RepairManagementRecordRequest)

from lan_bitable_template_portal.lighthouse_agent import PortalAgent
from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles

from lan_bitable_template_portal.portal_service import REPAIR_MANAGEMENT_TABLE_ID

from test_lighthouse_agent_workflows import ACTOR, Store

OPERATION_ID = "repair_catalog_00000001"

BIG_BRAND = "品牌A"
BIG_MODELS = [f"modelA-{i:02d}" for i in range(60)]  # >= 50 model options
BRAND_B = "品牌B"
MODEL_PREFIX_B = "modelB-"


class RepairCatalogBase(unittest.IsolatedAsyncioTestCase):
    """Shared fixture: assistant/files/agent + fake native followup endpoint."""

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
        search = Mock(side_effect=AssertionError("agent must use backend APIs, not local/DOM scraping"))
        self.assistant = LighthouseAssistant(self.store, search, model=model)
        self.files = LighthouseFiles(self.store)

        self.app = FastAPI()
        self.followup_writes = []

        @self.app.put("/api/repair-management/followups/{record_id}")
        async def update_followup(record_id: str, body: RepairFollowupRecordRequest):
            self.followup_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": record_id}}

        @self.app.post("/api/repair-management/records")
        async def create_project(body: RepairManagementRecordRequest):
            return {"ok": True, "data": {"record_id": "rec-created-project"}}

        @self.app.put("/api/repair-management/records/{record_id}")
        async def update_project(record_id: str, body: RepairManagementRecordRequest):
            self.followup_writes.append(body.model_dump())
            return {"ok": True, "data": {"record_id": record_id}}

        self.catalog = PortalAPICatalog(self.app)
        self.agent = PortalAgent(self.assistant, self.catalog, self.files)

    # ---------- helpers ----------
    def _prepare(self, operation, queries=None):
        decision = {"operations": [operation]}
        return self.agent.prepare(ACTOR, decision, OPERATION_ID, [], queries=queries or {})

    def _followup_queries(self, original, metas, summary_record_id="rec-parent",
                          brand_options=None, device_options=None):
        payload = {
            "summary_record_id": summary_record_id,
            "relation_mode": "record_id",
            "fields": metas,
            "records": [original],
        }
        if brand_options is not None:
            payload["brand_model_options"] = brand_options
        if device_options is not None:
            payload["device_brand_model_options"] = device_options
        return {"query_" + ("b" * 32): payload}

    def _project_original(self):
        return {
            "record_id": "rec-project",
            "record_version": "pv1",
            "raw_fields": {
                "设备名称": "设备A",
                "设备品牌": BIG_BRAND,
                "设备型号": "modelA-00",
                "设备编号": "",
                "维修进度": 0.5,
            },
            "source_event_id": "rec-event",
            "source_repair_ids": ["rec-repair"],
        }

    def _project_queries(self, original, metas, table_id=None):
        return {"query_" + ("a" * 32): {
            "table_id": table_id or REPAIR_MANAGEMENT_TABLE_ID,
            "records": [original],
            "fields": metas,
        }}

    def _public_form(self, plan, idx=0):
        public = self.agent.public_plan(plan, ACTOR)
        return next(f for f in public["fields"]
                    if f.get("operation_index", 0) == idx and f.get("path") == "fields")

    def _field(self, plan, path, idx=0):
        return next(f for f in plan["fields"]
                    if f.get("operation_index", 0) == idx and f.get("path") == path)

    def _control_from_plan(self, plan, idx=0):
        return self._field(plan, "fields", idx)

    def _base_metas(self):
        return [
            {"field_name": "设备名称", "field_type": 1, "editable": True, "options": []},
            {"field_name": "设备品牌", "field_type": 1, "editable": True, "options": []},
            {"field_name": "设备型号", "field_type": 3, "editable": True, "options": ["modelA-00", "modelA-01"]},
            {"field_name": "设备编号", "field_type": 1, "editable": True, "options": []},
            {"field_name": "维修进度", "field_type": 2, "editable": True, "options": []},
        ]

    def _catalog(self):
        return {
            BIG_BRAND: BIG_MODELS,
            BRAND_B: [f"{MODEL_PREFIX_B}{i:02d}" for i in range(50)],
        }, {
            "设备A": {BIG_BRAND: BIG_MODELS},
            "设备B": {BRAND_B: [f"{MODEL_PREFIX_B}{i:02d}" for i in range(50)]},
        }


class RepairCatalogFidelityTests(RepairCatalogBase):
    """Contract-by-contract fidelity coverage for the followup device catalog."""

    async def test_descriptor_has_followup_and_catalog_key(self):
        """The catalog descriptor is valid and the followup endpoint is covered."""
        descriptor = self.catalog.get("PUT /api/repair-management/followups/{record_id}")
        body_schema = descriptor.get("schema", {}).get("body", {})
        self.assertIn("fields", body_schema.get("properties", {}))
        # Native repair controls expose the catalog under the fields control.
        brand_opts, device_opts = self._catalog()
        original = self._original()
        plan = self._prepare(
            {"api_id": "PUT /api/repair-management/followups/{record_id}",
             "path_params": {"record_id": "rec-followup"},
             "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": []}},
            queries=self._followup_queries(original, self._base_metas(),
                                           brand_options=brand_opts, device_options=device_opts),
        )
        control = self._control_from_plan(plan)
        self.assertIn("repair_catalog", control)
        self.assertEqual({"brands", "devices"}, set(control["repair_catalog"].keys()))

    def _original(self):
        return {
            "record_id": "rec-followup",
            "record_version": "fv1",
            "raw_fields": {
                "设备名称": "设备A",
                "设备品牌": BIG_BRAND,
                "设备型号": "modelA-00",
                "设备编号": "",
                "维修进度": 0.5,
                "维修进展描述": [{"text": "已排查"}],
            },
            "cmdb_record_ids": [],
        }

    async def test_public_plan_keeps_50_plus_model_options_untruncated(self):
        """50+ catalog model options survive public_plan with no list truncation."""
        brand_opts, device_opts = self._catalog()
        plan = self._prepare(
            {"api_id": "PUT /api/repair-management/followups/{record_id}",
             "path_params": {"record_id": "rec-followup"},
             "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": []}},
            queries=self._followup_queries(self._original(), self._base_metas(),
                                           brand_options=brand_opts, device_options=device_opts),
        )
        control = self._control_from_plan(plan)
        # Internal plan holds the full snapshot.
        self.assertEqual(len(control["repair_catalog"]["brands"][BIG_BRAND]), len(BIG_MODELS))
        self.assertEqual(len(control["repair_catalog"]["devices"]["设备B"][BRAND_B]), 50)

        public = self._public_form(plan)
        self.assertIn("repair_catalog", public)
        # public_plan must NOT truncate the catalog model lists.
        self.assertEqual(public["repair_catalog"]["brands"][BIG_BRAND], BIG_MODELS)
        self.assertEqual(public["repair_catalog"]["devices"]["设备B"][BRAND_B],
                         [f"{MODEL_PREFIX_B}{i:02d}" for i in range(50)])
        self.assertEqual(len(public["repair_catalog"]["brands"][BIG_BRAND]),
                         len(BIG_MODELS), ">=50 model options must remain untruncated")

    async def test_field_delta_preserves_untouched_and_accepts_custom_model(self):
        """Preparing an edit merges the baseline raw fields with the delta and accepts a custom model."""
        brand_opts, device_opts = self._catalog()
        plan = self._prepare(
            {"api_id": "PUT /api/repair-management/followups/{record_id}",
             "path_params": {"record_id": "rec-followup"},
             "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": [],
                      "fields": {"设备型号": "custom-model-xyz"}}},
            queries=self._followup_queries(self._original(), self._base_metas(),
                                           brand_options=brand_opts, device_options=device_opts),
        )
        body = plan["operations"][0]["body"]
        fields = body["fields"]
        # Untouched raw data preserved.
        self.assertEqual(fields["设备名称"], "设备A")
        self.assertEqual(fields["设备品牌"], BIG_BRAND)
        self.assertEqual(fields["设备编号"], "")
        self.assertEqual(fields["维修进度"], 0.5)
        self.assertEqual(fields["维修进展描述"], [{"text": "已排查"}])
        # Custom model (not in the catalog) accepted by the agent delta.
        self.assertEqual(fields["设备型号"], "custom-model-xyz")
        # Expected version propagated for conflict guarding.
        self.assertEqual(body["expected_version"], "fv1")

    async def test_absence_of_catalog_leaves_existing_controls_unchanged(self):
        """Without snapshots the followup keeps the pre-existing native controls."""
        original = self._original()
        metas = self._base_metas()
        # No brand_model_options / device_brand_model_options in the snapshot.
        plan = self._prepare(
            {"api_id": "PUT /api/repair-management/followups/{record_id}",
             "path_params": {"record_id": "rec-followup"},
             "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": []}},
            queries=self._followup_queries(original, metas),
        )
        control = self._control_from_plan(plan)
        self.assertNotIn("repair_catalog", control)
        children = {child["path"]: child for child in control["children"]}
        # 设备品牌 stays the plain text repair field (no searchable-select/allow-custom).
        brand = children["设备品牌"]
        self.assertEqual(brand["type"], "text")
        self.assertNotIn("allow_custom_select", brand)
        # 设备型号 stays a normal select with its original options.
        model = children["设备型号"]
        self.assertEqual(model["type"], "select")
        self.assertEqual([opt["value"] for opt in model.get("options", [])],
                         ["modelA-00", "modelA-01"])

    async def test_native_repair_followup_only_on_followup_forms(self):
        """native_repair_followup marks only followup forms, independent of catalog dictionaries."""
        # A followup without optional brand/device catalog dictionaries still gets
        # the marker: it is a property of the followup form, not the catalog data.
        followup_plan = self._prepare(
            {"api_id": "PUT /api/repair-management/followups/{record_id}",
             "path_params": {"record_id": "rec-followup"},
             "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": []}},
            queries=self._followup_queries(self._original(), self._base_metas()),
        )
        followup_control = self._control_from_plan(followup_plan)
        self.assertTrue(followup_control.get("native_repair_followup"))
        self.assertNotIn("repair_catalog", followup_control,
                         "marker must be independent of the optional catalog dictionaries")

        # A project (repair-management/records) form must never receive it.
        project_plan = self._prepare(
            {"api_id": "PUT /api/repair-management/records/{record_id}",
             "path_params": {"record_id": "rec-project"},
             "body": {"scope": "A"}},
            queries=self._project_queries(self._project_original(), self._base_metas()),
        )
        project_control = self._control_from_plan(project_plan)
        self.assertNotIn("native_repair_followup", project_control,
                         "project forms must not receive the followup marker")

    async def test_followup_option_endpoint_accepts_submit_without_real_write(self):
        """Native fake endpoint validates the Pydantic request but never writes real data."""
        plan = self._prepare(
            {"api_id": "PUT /api/repair-management/followups/{record_id}",
             "path_params": {"record_id": "rec-followup"},
             "body": {"scope": "A", "summary_record_id": "rec-parent", "cmdb_record_ids": [],
                      "fields": {"设备型号": "custom-model-xyz"}}},
            queries=self._followup_queries(self._original(), self._base_metas()),
        )
        # The agent prepares the plan; no real write happens (the fake endpoint is only
        # available to catalog validation, and the agent itself never calls it).
        self.assertIsNotNone(plan.get("id"))
        self.assertEqual(self.followup_writes, [], "no real write may be issued by prepare")


if __name__ == "__main__":
    unittest.main(verbosity=2)