"""Isolated regression for the notice-panel native ``notice_sop`` selection.

Covers: the lazy ``sop-options`` directory endpoint, in-memory legacy
``work_order_choice`` migration, strict typed shape validation, all three SOP
work types' payloads/versions, local-only directory privacy, same-person /
H 楼值班 reviewer rules and invalid ``runs``.  No Feishu calls, no business
writes, and no live data scripts.
"""
from __future__ import annotations

import copy
import datetime as dt
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from lan_bitable_template_portal import notice_panel as panel
from lan_bitable_template_portal import notice_panel_data as data
from lan_bitable_template_portal.notice_panel import NoticePanel
from lan_bitable_template_portal.notice_panel_routes import install_notice_panel_routes
from lan_bitable_template_portal.polling_work_orders import (
    POLLING_H_DUTY_RECORD_ID, PollingWorkOrderService,
)
from lan_bitable_template_portal.portal_service import PortalError
from lan_bitable_template_portal.state_store import LanPortalStateStore

NS = panel.NS
IDENTITY = panel.run_identity("ou-li", "A", "2026-10-09", "morning")


class Store:
    def __init__(self):
        self.docs = {}

    def get_document(self, ns, key):
        return copy.deepcopy(self.docs.get((ns, key)))

    def put_document(self, ns, key, value):
        self.docs[(ns, key)] = copy.deepcopy(value)

    def put_documents(self, ns, payloads):
        self.docs.update({(ns, key): copy.deepcopy(value) for key, value in payloads.items()})


class FakeAuth:
    def __init__(self, disabled=()):
        self.disabled = set(disabled)
        self.user_scopes = {}
        self.user_roles = {}

    def register(self, open_id, scopes, role="admin"):
        self.user_scopes[open_id] = list(scopes)
        self.user_roles[open_id] = role

    def scopes_for_open_id(self, oid):
        if oid in self.disabled:
            return []
        return list(self.user_scopes.get(str(oid or ""), ["A", "B", "C", "D", "E", "H", "110"]))

    def role_for_open_id(self, oid):
        if oid in self.disabled:
            return "building"
        return self.user_roles.get(str(oid or ""), "admin")

    def _open_id_explicitly_disabled(self, oid):
        return oid in self.disabled

    def scope_allowed(self, session, scope):
        return scope in set(session.get("allowed_scopes") or [])

    def scope_label(self, scope):
        return str(scope) + "栋"


class FakeController:
    def __init__(self, runtime, auth):
        self.runtime = runtime
        self.auth = auth
        self.session = None

    def _current_session(self, request):
        return self.session

    def _authorized_scope_or_error(self, session, scope):
        normalized = str(scope or "").upper()
        allowed = set(session.get("allowed_scopes") or [])
        if "ALL" not in allowed and normalized not in allowed:
            raise PortalError(f"当前飞书账号无权访问 {normalized}。")
        return normalized

    def _get_ongoing(self, scope):
        return []

    def _portal_error_response(self, exc, default_status=400):
        status = getattr(exc, "status_code", default_status) if isinstance(exc, PortalError) else 500
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=status)


class FakeSignatureManagement:
    def __init__(self, people):
        self.people = people

    def directory(self, refresh=False):
        return {"people": copy.deepcopy(self.people)}


class FakeService:
    """Minimal service facade for notice_panel_data/notice_panel submission paths."""

    def __init__(self, people):
        self.signature_management = FakeSignatureManagement(people)

    def _format_input_datetime(self, value):
        return str(value or "")

    def _validate_minimum_notice_duration(self, start, end):
        return None

    def _require_end_site_photo_cumulative(self, body, action, notice_type="", work_type=""):
        return None

    def _synchronize_prepared_notice_text(self, body):
        return {"text": json.dumps(body, ensure_ascii=False, sort_keys=True, default=str)}


class SopFixture:
    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.service = PollingWorkOrderService(LanPortalStateStore(self.root / "sops.sqlite3"))
        self.sops = {}
        for work, content in (("maintenance", "检查供水温度"),
                              ("polling", "{{from}}切换至{{to}}"),
                              ("adjust", "{{from}}切换运行模式")):
            sop = self.service.save_sop({"scope": "A", "work_type": work, "name": work + " SOP",
                                         "steps": [{"content": content, "operator_required": True,
                                                    "reviewer_required": True}]})
            self.sops[work] = self.service.add_sop_attachment(
                sop["sop_id"], file_name="fixture.txt", content=b"isolated",
                expected_version=sop["version"])
        self.people = [
            {"record_id": "person-one", "name": "张三", "employee_no": "E1001",
             "building": "A楼", "position": "暖通", "shift": "早班",
             "open_id": "o-private-one", "can_receive_message": True},
            {"record_id": "person-two", "name": "李四", "employee_no": "E2002",
             "building": "A楼", "position": "电气", "shift": "晚班",
             "open_id": "o-private-two", "can_receive_message": True},
            {"record_id": "person-other", "name": "王五", "employee_no": "E3003",
             "building": "B楼", "position": "给排水", "shift": "中班",
             "open_id": "o-private-three", "can_receive_message": True},
        ]
        self.app_service = FakeService(self.people)

    def close(self):
        self.tmp.cleanup()


class SopBase(unittest.TestCase):
    def setUp(self):
        self.fx = SopFixture()
        self.addCleanup(self.fx.close)
        self.store = Store()
        self.auth = FakeAuth()
        self.auth.register("ou-li", ["A", "B", "C", "D", "E", "H", "110"], "admin")
        self.auth.register("ou-other", ["A"], "admin")
        self.runtime = type("Runtime", (), {})()
        self.runtime.state_store = self.store
        self.runtime.auth_manager = self.auth
        self.runtime.service = self.fx.app_service
        self.runtime.polling_work_orders = lambda: self.fx.service
        self.controller = FakeController(self.runtime, self.auth)
        self.manager = NoticePanel(self.controller, self.runtime)
        self.addCleanup(self.manager.close)
        self._time_patcher = patch.object(panel, "now")
        self._mock_now = self._time_patcher.start()
        self.addCleanup(self._time_patcher.stop)
        self._mock_now.return_value = dt.datetime(2026, 10, 9, 10, 0, tzinfo=panel.ZONE)

    def sop_field(self):
        return {"key": "notice_sop", "label": "本次工单", "required": True,
                "readonly": False, "kind": "notice_sop", "options": []}

    def item(self, key="a", work_type="maintenance", action="start", notice_sop=None):
        d = {"title": "测试" + key, "content": "现场已核对", "progress": "完成",
             "start_time": "2026-10-09 10:00", "end_time": "2026-10-09 12:00",
             "notice_action": "更新", "specialty": "暖通", "scope": "A",
             "building_codes": ["A"], "work_type": work_type, "action": action,
             "source_record_id": "src-" + key}
        if work_type in data.SOP_WORK_TYPES and action == "start":
            d["notice_sop"] = notice_sop or data._default_notice_sop(exempt=False)
        fields = [
            {"key": "title", "label": "通告名称", "required": True, "readonly": True,
             "kind": "text", "options": []},
            {"key": "content", "label": "内容", "required": True, "readonly": False,
             "kind": "multiline", "options": []},
            {"key": "progress", "label": "本次进度", "required": True, "readonly": False,
             "kind": "multiline", "options": []},
            {"key": "specialty", "label": "专业", "required": True, "readonly": False,
             "kind": "select", "options": ["暖通", "电气"]},
            {"key": "start_time", "label": "开始时间", "required": True, "readonly": False,
             "kind": "text", "options": []},
            {"key": "end_time", "label": "结束时间", "required": True, "readonly": False,
             "kind": "text", "options": []},
        ]
        if work_type in data.SOP_WORK_TYPES and action == "start":
            fields.append(self.sop_field())
        else:
            fields.append({"key": "notice_action", "label": "本次操作", "required": False,
                           "readonly": False, "kind": "select", "options": ["更新", "结束"]})
        return {"key": key, "title": d["title"], "window": "本月", "status": "未开始",
                "action": action, "version": "v1", "blocked": "",
                "selected": False, "fields": fields, "draft": d}

    def seed_run(self, items, state="edit"):
        doc = {"id": IDENTITY, "owner": "ou-li", "scope": "A", "date": "2026-10-09",
               "slot": "morning", "state": state, "revision": 1, "generation": 1,
               "items": items, "error": "", "confirmed_session": None,
               "created_at": 1.0, "updated_at": 1.0}
        self.store.put_document(NS, IDENTITY, doc)
        return doc

    def make_session(self, open_id="ou-li", scopes=("A", "B", "C", "D", "E", "H", "110"),
                     role="admin"):
        return {"user": {"open_id": open_id, "name": "测试"}, "role": role,
                "allowed_scopes": list(scopes)}


class SopShapeTests(unittest.TestCase):
    def test_accepts_incomplete_blank_draft(self):
        result = data._normalize_notice_sop_shape({"exempt": False})
        self.assertEqual(result, data._default_notice_sop(exempt=False))

    def test_missing_exempt_rejected(self):
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape({"scope": "A"})

    def test_string_false_exempt_rejected(self):
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape({"exempt": "false"})

    def test_non_bool_exempt_rejected(self):
        for bad in (0, 1, "true", None, []):
            with self.subTest(bad=bad):
                with self.assertRaises(PortalError):
                    data._normalize_notice_sop_shape({"exempt": bad})

    def test_non_string_ids_scope_rejected(self):
        for key in ("scope", "sop_id", "operator_record_id", "reviewer_record_id"):
            with self.subTest(key=key):
                with self.assertRaises(PortalError):
                    data._normalize_notice_sop_shape({"exempt": False, key: 123})

    def test_bool_negative_nonint_version_rejected(self):
        for bad in (True, -1, 1.5, "2", None):
            value = {"exempt": False, "sop_version": bad}
            if bad is None:
                # None coerces to a fresh default version on purpose (incomplete draft).
                result = data._normalize_notice_sop_shape({**value})
                self.assertEqual(result["sop_version"], 0)
                continue
            with self.subTest(bad=bad):
                with self.assertRaises(PortalError):
                    data._normalize_notice_sop_shape({**value})

    def test_non_list_runs_rejected(self):
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape({"exempt": False, "runs": "x"})
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape({"exempt": False, "runs": {"a": 1}})

    def test_non_string_run_values_rejected(self):
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape(
                {"exempt": False, "runs": [{"from_unit": 1, "to_unit": "3#"}]})

    def test_extra_keys_rejected(self):
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape({"exempt": False, "extra": 1})
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape(
                {"exempt": False, "runs": [{"from_unit": "1#", "bogus": 2}]})

    def test_oversized_values_rejected(self):
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape({"exempt": False, "scope": "X" * 201})
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape(
                {"exempt": False, "runs": [{"from_unit": "1#", "label": "Y" * 201}]})

    def test_run_count_capped_at_two(self):
        runs = [{"from_unit": str(i) + "#", "to_unit": "3#"} for i in (1, 2)] + \
               [{"from_unit": "4#", "to_unit": "6#"}]
        with self.assertRaises(PortalError):
            data._normalize_notice_sop_shape({"exempt": False, "runs": runs})
        ok = data._normalize_notice_sop_shape({"exempt": False, "runs": runs[:2]})
        self.assertEqual(len(ok["runs"]), 2)


class SopMigrationTests(SopBase):
    def _seed_legacy_doc(self, state="edit"):
        legacy_field = {"key": "work_order_choice", "label": "本次工单", "required": True,
                        "readonly": False, "kind": "select",
                        "options": ["本次无需工单", "使用网页已配置工单"]}
        draft = {"title": "T", "content": "c", "scope": "A", "work_type": "maintenance",
                 "action": "start", "work_order_choice": "本次无需工单"}
        item = {"key": "a", "title": "T", "action": "start", "selected": True,
                "edit_all": True, "brief_fields": ["content", "work_order_choice"],
                "fields": [{"key": "content", "label": "内容", "required": True,
                            "readonly": False, "kind": "multiline", "options": []},
                           legacy_field],
                "draft": draft}
        doc = self.seed_run([item], state=state)
        return doc, item

    def test_legacy_edit_doc_migrated_in_memory(self):
        doc, item = self._seed_legacy_doc("edit")
        migrated = self.manager._get(IDENTITY)
        migrated_item = migrated["items"][0]
        self.assertNotIn("work_order_choice", migrated_item["draft"])
        self.assertIn("notice_sop", migrated_item["draft"])
        self.assertTrue(migrated_item["draft"]["notice_sop"]["exempt"])
        keys = {f["key"] for f in migrated_item["fields"]}
        self.assertIn("notice_sop", keys)
        self.assertNotIn("work_order_choice", keys)
        # Preserve unrelated state/revision.
        self.assertEqual(migrated_item["selected"], True)
        self.assertEqual(migrated_item["edit_all"], True)
        self.assertIn("notice_sop", migrated_item["brief_fields"])
        self.assertNotIn("work_order_choice", migrated_item["brief_fields"])
        self.assertIn("content", migrated_item["brief_fields"])

    def test_confirm_running_done_payloads_not_touched(self):
        for state in ("confirm", "running", "done"):
            with self.subTest(state=state):
                doc, item = self._seed_legacy_doc(state)
                item["payload"] = {"work_order_choice": "本次无需工单", "polling_work_order_exempt": True}
                self.store.put_document(NS, IDENTITY, doc)
                migrated = self.manager._get(IDENTITY)
                migrated_item = migrated["items"][0]
                self.assertEqual(migrated_item["draft"].get("work_order_choice"), "本次无需工单")
                self.assertNotIn("notice_sop", migrated_item["draft"])
                # _get mutates only an in-memory copy; the stored confirm/running/done
                # payload must remain byte-for-byte intact.
                stored = self.store.get_document(NS, IDENTITY)["items"][0]
                self.assertEqual(stored["payload"]["work_order_choice"], "本次无需工单")
                self.assertTrue(stored["payload"]["polling_work_order_exempt"])
                self.assertNotIn("polling_sop_id", stored["payload"])
                self.assertNotIn("notice_sop", stored["draft"])

    def test_apply_changes_sees_migrated_notice_sop(self):
        self._seed_legacy_doc("edit")
        run = self.manager.run_action(self.make_session(), IDENTITY, {
            "action": "save", "revision": 1,
            "changes": [{"key": "a", "selected": True,
                         "draft": {"notice_sop": {"exempt": True}}}]})
        stored = self.store.get_document(NS, IDENTITY)["items"][0]
        # save mutates draft; the migrated notice_sop remains present.
        self.assertNotIn("work_order_choice", stored["draft"])
        self.assertTrue(stored["draft"]["notice_sop"]["exempt"])
        self.assertFalse(run.get("error"))


class SopDirectoryTests(SopBase):
    def _valid_item(self, field_dict=None, **draft_kwargs):
        notice_sop = {"exempt": False, "scope": "A",
                      "sop_id": self.fx.sops["maintenance"]["sop_id"],
                      "sop_version": self.fx.sops["maintenance"]["version"],
                      "operator_record_id": "person-one", "reviewer_record_id": "person-two",
                      "runs": []}
        notice_sop.update(draft_kwargs or {})
        item = self.item("a", "maintenance", "start", notice_sop=notice_sop)
        if field_dict is not None:
            item["fields"] = field_dict
        return item

    def test_endpoint_returns_public_field_without_private_data(self):
        self.seed_run([self._valid_item()])
        client = TestClient(self._app())
        self.controller.session = self.make_session()
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "a"})
        self.assertEqual(res.status_code, 200)
        body = res.json()["data"]
        self.assertEqual(body["scope"], "A")
        self.assertEqual(body["work_type"], "maintenance")
        field = body["field"]
        self.assertEqual(field["directory_scope"], "A")
        self.assertTrue(field["sops"])
        names = {p["name"] for p in field["people"]}
        self.assertIn("张三", names)
        self.assertIn("李四", names)
        text = json.dumps(body)
        self.assertNotIn("o-private-one", text)
        self.assertNotIn("_people", text)
        self.assertNotIn("_sops", text)
        self.assertNotIn(str(self.fx.root), text)
        for person in field["people"]:
            self.assertEqual(set(person), {"record_id", "name", "label"})
        for sop in field["sops"]:
            self.assertNotIn("attachments", sop)
            self.assertNotIn("open_id", sop)

    def test_endpoint_filters_people_by_query(self):
        selected = {"exempt": False, "scope": "A",
                    "sop_id": self.fx.sops["maintenance"]["sop_id"],
                    "sop_version": self.fx.sops["maintenance"]["version"],
                    "operator_record_id": "person-one", "reviewer_record_id": "person-two"}
        self.seed_run([self._valid_item(**selected)])
        client = TestClient(self._app())
        self.controller.session = self.make_session()
        # q matches only 王五; persisted operator/reviewer are still shown first and
        # the reviewer-only H 楼值班账号 stays available.
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "a", "q": "王五"})
        ids = [p["record_id"] for p in res.json()["data"]["field"]["people"]]
        self.assertEqual(ids[:3], ["person-one", "person-two", POLLING_H_DUTY_RECORD_ID])
        self.assertIn("person-other", ids)
        # Querying by building/local name matches only local directory rows.
        res2 = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                          params={"item_key": "a", "q": "E2002"})
        ids2 = [p["record_id"] for p in res2.json()["data"]["field"]["people"]]
        self.assertEqual(ids2, ["person-one", "person-two", POLLING_H_DUTY_RECORD_ID])

    def test_endpoint_caps_people_but_retains_selected(self):
        # Build >200 staff so the cap (200) would normally drop the selected one.
        selected_id = "person-selected"
        people = [{"record_id": "p%03d" % i, "name": "甲", "employee_no": "E%04d" % i,
                   "building": "A楼", "position": "运维", "shift": "早班",
                   "open_id": "o-p%03d" % i, "can_receive_message": True}
                  for i in range(200)]
        people.append({"record_id": selected_id, "name": "甲", "employee_no": "E9999",
                       "building": "A楼", "position": "运维", "shift": "早班",
                       "open_id": "o-person-selected", "can_receive_message": True})
        self.fx.app_service = FakeService(people)
        self.runtime.service = self.fx.app_service
        selected = {"exempt": False, "scope": "A",
                    "sop_id": self.fx.sops["maintenance"]["sop_id"],
                    "sop_version": self.fx.sops["maintenance"]["version"],
                    "operator_record_id": selected_id, "reviewer_record_id": "person-two"}
        self.seed_run([self._valid_item(**selected)])
        if "person-two" not in {p["record_id"] for p in people}:
            people.append({"record_id": "person-two", "name": "李四", "employee_no": "E2002",
                           "building": "A楼", "position": "电气", "shift": "晚班",
                           "open_id": "o-private-two", "can_receive_message": True})
            self.fx.app_service = FakeService(people)
            self.runtime.service = self.fx.app_service
        client = TestClient(self._app())
        self.controller.session = self.make_session()
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "a"})
        field = res.json()["data"]["field"]
        ids = [p["record_id"] for p in field["people"]]
        # Cap is 200, but the selected operator is guaranteed to survive truncation.
        self.assertIn(selected_id, ids)
        self.assertIn("person-two", ids)
        self.assertLessEqual(len(ids), data.SOP_OPTIONS_PEOPLE_CAP + 2)

    def test_endpoint_retains_selected_even_outside_query_and_cap(self):
        # 200 staff all match ``q``; the persisted operator is beyond the cap and
        # does NOT match the query, yet must survive so the frontend can always
        # show the saved choice while the user searches.
        matching = [{"record_id": "p%03d" % i, "name": "甲", "employee_no": "E%04d" % i,
                     "building": "A楼", "position": "运维", "shift": "早班",
                     "open_id": "o-p%03d" % i, "can_receive_message": True}
                    for i in range(200)]
        operator_id = "person-kept"
        operator = {"record_id": operator_id, "name": "李特选", "employee_no": "E9999",
                    "building": "A楼", "position": "电气", "shift": "晚班",
                    "open_id": "o-kept", "can_receive_message": True}
        people = matching + [operator, {"record_id": "person-two", "name": "李四",
                                        "employee_no": "E2002", "building": "A楼",
                                        "position": "电气", "shift": "晚班",
                                        "open_id": "o-private-two",
                                        "can_receive_message": True}]
        self.fx.app_service = FakeService(people)
        self.runtime.service = self.fx.app_service
        selected = {"exempt": False, "scope": "A",
                    "sop_id": self.fx.sops["maintenance"]["sop_id"],
                    "sop_version": self.fx.sops["maintenance"]["version"],
                    "operator_record_id": operator_id, "reviewer_record_id": "person-two"}
        self.seed_run([self._valid_item(**selected)])
        client = TestClient(self._app())
        self.controller.session = self.make_session()
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "a", "q": "甲"})
        field = res.json()["data"]["field"]
        ids = [p["record_id"] for p in field["people"]]
        self.assertIn(operator_id, ids)
        self.assertIn("person-two", ids)
        # First two slots are the persisted selections (stable order), regardless of q.
        self.assertEqual(ids[:2], [operator_id, "person-two"])
        self.assertLessEqual(len(ids), data.SOP_OPTIONS_PEOPLE_CAP + 2)

    def test_guest_wrong_owner_wrong_scope_and_unsupported_type_errors(self):
        self.seed_run([self._valid_item()])
        client = TestClient(self._app())
        # Guest is rejected before any directory access.
        self.controller.session = {"user": {}, "role": "guest", "allowed_scopes": []}
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "a"})
        self.assertEqual(res.status_code, 400)
        # Wrong owner.
        self.controller.session = self.make_session("ou-other")
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "a"})
        self.assertIn("无权", res.json()["error"])
        # Owner with no authorized scope for A.
        auth = self.auth
        auth.register("ou-li", ["B"], "building")
        self.controller.session = self.make_session("ou-li", ["B"], "building")
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "a"})
        self.assertIn("无权", res.json()["error"])
        # Unsupported work type (repair item).
        self.seed_run([self.item("r", "repair", "start")])
        self.controller.session = self.make_session()
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "r"})
        self.assertIn("无需工单", res.json()["error"])

    def test_missing_item_and_oversize_query_rejected(self):
        self.seed_run([self._valid_item()])
        client = TestClient(self._app())
        self.controller.session = self.make_session()
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "nope"})
        self.assertEqual(res.status_code, 400)
        self.assertIn("不存在", res.json()["error"])
        res = client.get(f"/api/assistant/notice-panel/{IDENTITY}/sop-options",
                         params={"item_key": "a", "q": "x" * 121})
        self.assertEqual(res.status_code, 400)
        self.assertIn("过长", res.json()["error"])

    def _app(self):
        app = FastAPI()
        install_notice_panel_routes(app, self.controller, self.runtime)
        return app


class SopSubmissionTests(SopBase):
    def _ready_item(self, work_type="maintenance", **notice_patch):
        sop = self.fx.sops[work_type]
        notice_sop = {"exempt": False, "scope": "A", "sop_id": sop["sop_id"],
                      "sop_version": sop["version"],
                      "operator_record_id": "person-one",
                      "reviewer_record_id": "person-two", "runs": []}
        notice_sop.update(notice_patch)
        return self.item("a", work_type, "start", notice_sop=notice_sop)

    def _prepare(self, item):
        body = copy.deepcopy(item["draft"])
        labels = data._prepare_notice_sop(self.fx.app_service, item, body, self.runtime)
        return body, labels

    def test_all_three_work_types_native_payload_and_version(self):
        runs_by_work = {
            "maintenance": [],
            "polling": [{"from_unit": "1#", "to_unit": "3#"}, {"from_unit": "4#", "to_unit": "6#"}],
            "adjust": [{"from_unit": "2#", "to_unit": "4#", "other_unit": "6#"}],
        }
        for work_type in ("maintenance", "polling", "adjust"):
            with self.subTest(work=work_type):
                item = self._ready_item(work_type, runs=runs_by_work[work_type])
                body, labels = self._prepare(item)
                self.assertIsNone(body.get("notice_sop"))
                self.assertEqual(body["polling_sop_id"], self.fx.sops[work_type]["sop_id"])
                self.assertEqual(body["polling_sop_version"], self.fx.sops[work_type]["version"])
                self.assertFalse(body["polling_work_order_exempt"])
                self.assertEqual(body["polling_operator_record_id"], "person-one")
                self.assertEqual(body["polling_reviewer_record_id"], "person-two")
                self.assertIn("SOP", labels["sop"])

    def test_exempt_short_circuits_without_sop_payload(self):
        item = self._ready_item("maintenance", exempt=True)
        body, labels = self._prepare(item)
        self.assertTrue(body["polling_work_order_exempt"])
        self.assertNotIn("polling_sop_id", body)
        self.assertEqual(labels, {"sop": "本次不使用工单"})

    def test_exempt_preview_shows_no_work_order_summary(self):
        item = self._ready_item("maintenance", exempt=True)
        body, preview = data.submission(self.fx.app_service, item, self.runtime)
        self.assertTrue(body["polling_work_order_exempt"])
        self.assertNotIn("polling_sop_id", body)
        self.assertIn("本次不使用工单", preview)

    def test_same_person_rejected(self):
        item = self._ready_item("maintenance", operator_record_id="person-two",
                                reviewer_record_id="person-two")
        with self.assertRaises(PortalError):
            self._prepare(item)

    def test_h_duty_only_reviewer_accepted(self):
        # H 楼值班账号 as reviewer is allowed.
        item = self._ready_item("polling", reviewer_record_id=POLLING_H_DUTY_RECORD_ID,
                                runs=[{"from_unit": "1#", "to_unit": "3#"}])
        body, labels = self._prepare(item)
        self.assertEqual(body["polling_reviewer_record_id"], POLLING_H_DUTY_RECORD_ID)
        # H 楼值班账号 as operator must be rejected.
        bad = self._ready_item("polling", operator_record_id=POLLING_H_DUTY_RECORD_ID,
                               reviewer_record_id="person-two",
                               runs=[{"from_unit": "1#", "to_unit": "3#"}])
        with self.assertRaises(PortalError):
            self._prepare(bad)

    def test_invalid_runs_rejected(self):
        for work_type, runs, label in (
            ("polling", [{"from_unit": "1#", "to_unit": "1#"}], "same"),
            ("polling", [{"from_unit": "1#", "to_unit": "5#"}], "cross group"),
            ("adjust", [{"from_unit": "2#", "to_unit": "2#", "other_unit": "6#"}], "same mode"),
            ("adjust", [], "missing unit"),
        ):
            with self.subTest(work=work_type, case=label):
                item = self._ready_item(work_type, runs=runs)
                with self.assertRaises(PortalError):
                    self._prepare(item)

    def test_stale_sop_version_rejected(self):
        item = self._ready_item("maintenance", sop_version=9999)
        with self.assertRaises(PortalError):
            self._prepare(item)

    def test_preview_confirm_flow_via_runner(self):
        item = self._ready_item("maintenance")
        item["selected"] = True
        item["brief_fields"] = []
        self.seed_run([item], state="edit")
        with patch("lan_bitable_template_portal.notice_panel_data.build_items",
                   side_effect=lambda service, scope, work, ongoing, current=None, **k: [item]):
            previewed = self.manager.run_action(self.make_session(), IDENTITY, {
                "action": "preview", "revision": 1, "changes": []})
            self.assertEqual(previewed["state"], "confirm")
            self.assertIn("工单", previewed["items"][0]["preview"])
            # Re-running confirm uses the confirmed payload without needing a new preview.
            confirmed = self.manager.run_action(self.make_session(), IDENTITY, {
                "action": "confirm", "revision": previewed["revision"], "changes": []})
            self.assertEqual(confirmed["state"], "running")
            stored_payload = self.store.get_document(NS, IDENTITY)["items"][0]["payload"]
            self.assertEqual(stored_payload["polling_sop_id"],
                             self.fx.sops["maintenance"]["sop_id"])


if __name__ == "__main__":
    unittest.main()