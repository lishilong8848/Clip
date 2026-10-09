"""Isolated assistant notice-panel acceptance tests.

No Feishu card, no network, no cloud writes beyond the mocked native sender, and no
business service calls beyond the patched local ``build_items``/``submission`` and the
mocked ``submit_notice_action`` job submission.
"""
from __future__ import annotations

import copy
import datetime as dt
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import AsyncMock, patch

sys.path.insert(0, str(Path(__file__).parent))

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from lan_bitable_template_portal import notice_panel as panel
from lan_bitable_template_portal.notice_panel import NoticePanel
from lan_bitable_template_portal.notice_panel_routes import install_notice_panel_routes
from lan_bitable_template_portal.portal_service import PortalError

ALL_SCOPES = ("A", "B", "C", "D", "E", "H", "110")


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

    def register(self, open_id, scopes=None, role="admin"):
        self.user_scopes[open_id] = list(scopes if scopes is not None else ALL_SCOPES)
        self.user_roles[open_id] = role

    def scopes_for_open_id(self, oid):
        if oid in self.disabled:
            return []
        return list(self.user_scopes.get(str(oid or ""), ALL_SCOPES))

    def role_for_open_id(self, oid):
        if oid in self.disabled:
            return "building"
        return self.user_roles.get(str(oid or ""), "admin")

    def _open_id_explicitly_disabled(self, oid):
        return oid in self.disabled

    def scope_allowed(self, session, scope):
        if not isinstance(session, dict):
            return False
        normalized = str(scope or "").upper()
        allowed = set(session.get("allowed_scopes") or [])
        if "ALL" in allowed:
            return True
        return normalized in allowed

    def scope_label(self, scope):
        return str(scope) + "栋"


class FakeController:
    def __init__(self, runtime, auth):
        self.runtime = runtime
        self.auth = auth
        self.session = None
        self.build_calls = 0
        self.get_ongoing_calls = 0

    def _current_session(self, request):
        return self.session

    def _authorized_scope_or_error(self, session, scope):
        if not isinstance(session, dict):
            raise PortalError("未登录。")
        normalized = str(scope or "").upper()
        allowed = set(session.get("allowed_scopes") or [])
        if "ALL" not in allowed and normalized not in allowed:
            raise PortalError(f"当前飞书账号无权访问 {normalized}。")
        if normalized == "ALL":
            raise PortalError("通告待办必须使用具体楼栋。")
        return normalized

    def _get_ongoing(self, scope):
        self.get_ongoing_calls += 1
        return []

    def _portal_error_response(self, exc, default_status=400):
        status = getattr(exc, "status_code", default_status) if isinstance(exc, PortalError) else 500
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=status)


def make_session(open_id="ou-li", scopes=ALL_SCOPES, role="admin", name="测试"):
    return {"user": {"open_id": open_id, "name": name}, "role": role,
            "allowed_scopes": list(scopes)}


class PanelTestBase(unittest.TestCase):
    def setUp(self):
        self.store = Store()
        self.auth = FakeAuth()
        self.auth.register("ou-li", list(ALL_SCOPES), "admin")
        self.service = SimpleNamespaceForTest()
        self.runtime = SimpleNamespaceForTest(state_store=self.store, auth_manager=self.auth, service=self.service)
        self.controller = FakeController(self.runtime, self.auth)
        self.manager = NoticePanel(self.controller, self.runtime)

        # Synchronous worker execution keeps acceptance tests deterministic.
        def sync_queue(self_, key, fn, *args):
            fn(*args)
            return True

        self._queue_patcher = patch.object(NoticePanel, "_queue", sync_queue)
        self._queue_patcher.start()
        self.addCleanup(self._queue_patcher.stop)
        self.addCleanup(self.manager.close)

        # Fixed Beijing clock (default 2026-10-09 10:00 morning edition).
        self._time_patcher = patch.object(panel, "now")
        self._mock_now = self._time_patcher.start()
        self.addCleanup(self._time_patcher.stop)
        self.set_time(dt.datetime(2026, 10, 9, 10, 0, tzinfo=panel.ZONE))

        # Local snapshot builders are delegated to notice_panel_data; patch them.
        self.build_impl = self._default_build_items
        import lan_bitable_template_portal.notice_panel_data as nd
        self._build_patcher = patch.object(nd, "build_items", side_effect=self._build_items)
        self._build_patcher.start()
        self.addCleanup(self._build_patcher.stop)

        self._submission_patcher = patch.object(nd, "submission", side_effect=self._submission)
        self._submission_patcher.start()
        self.addCleanup(self._submission_patcher.stop)

        self.native = AsyncMock()
        self._native_patcher = patch("lan_bitable_template_portal.notice_actions.submit_notice_action", self.native)
        self._native_patcher.start()
        self.addCleanup(self._native_patcher.stop)
        self.service.get_job = lambda job_id: None

    def set_time(self, at):
        self._mock_now.return_value = at

    def _build_items(self, service, scope, work, ongoing, current=None):
        self.controller.build_calls += 1
        return copy.deepcopy(self.build_impl(service, scope, work, ongoing, current))

    def _default_build_items(self, service, scope, work, ongoing, current=None):
        return [self._item("a", scope), self._item("b", scope)]

    def _item(self, key, scope, blocked="", action="start", draft=None, work_type="maintenance"):
        fields = [
            {"key": "title", "label": "通告名称", "required": True, "readonly": True, "kind": "text", "options": []},
            {"key": "content", "label": "内容", "required": True, "readonly": False, "kind": "multiline", "options": []},
            {"key": "progress", "label": "本次进度", "required": True, "readonly": False, "kind": "multiline", "options": []},
            {"key": "notice_action", "label": "本次操作", "required": False, "readonly": False,
             "kind": "select", "options": ["更新", "结束"]},
        ]
        d = {"title": "测试" + key, "content": "初始内容", "progress": "",
             "notice_action": "更新", "scope": scope, "work_type": work_type,
             "action": action, "source_record_id": "src-" + key}
        d.update(draft or {})
        return {"key": key, "title": d["title"], "window": "本月", "status": "未开始",
                "action": action, "version": "v1", "blocked": blocked, "selected": False,
                "fields": fields, "draft": d}

    def _submission(self, service, item, runtime=None):
        draft = copy.deepcopy(item["draft"])
        missing = [f["label"] for f in item["fields"]
                   if f["required"] and not str(draft.get(f["key"]) or "").strip()]
        if missing:
            raise PortalError("请填写：" + "、".join(missing))
        body = {**draft, "scope": draft["scope"], "work_type": draft["work_type"],
                "action": draft["action"]}
        preview = "通告预览：" + str(draft.get("content") or "") + "/" + str(draft.get("progress") or "")
        return body, preview

    def open(self, scope="E", session=None, slot=None):
        session = session if session is not None else make_session()
        self.controller.session = session
        return self.manager.open_run(session, scope, slot)

    def item(self, run, key):
        return next(it for it in run["items"] if it["key"] == key)

    def action(self, identity, action, session=None, revision=None, changes=None):
        session = session if session is not None else make_session()
        self.controller.session = session
        doc = self.store.get_document(panel.NS, identity)
        payload = {"action": action, "revision": doc["revision"] if revision is None else revision}
        if changes is not None:
            payload["changes"] = changes
        return self.manager.run_action(session, identity, payload)

    def select_fill_save(self, identity, key, **draft_fields):
        change = {"key": key, "selected": True}
        if draft_fields:
            change["draft"] = draft_fields
        return self.action(identity, "save", changes=[change])

    def preview(self, identity):
        return self.action(identity, "preview", changes=[])

    def preview_confirm_flow(self, scope="E", key="a", **draft_fields):
        run = self.open(scope)
        self.select_fill_save(run["id"], key, **draft_fields)
        previewed = self.preview(run["id"])
        self.assertEqual(previewed["state"], "confirm")
        return run, previewed

    def client(self):
        app = FastAPI()
        install_notice_panel_routes(app, self.controller, self.runtime)
        return TestClient(app)


class SimpleNamespaceForTest:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class AuthPersonaTests(PanelTestBase):
    def test_guest_denied_everywhere(self):
        guest = {"user": {}, "role": "guest", "allowed_scopes": ["A", "B", "C", "D", "E"]}
        self.controller.session = guest
        with self.assertRaises(PortalError):
            self.manager.open_run(guest, "E")

    def test_no_open_id_denied(self):
        bad = {"user": {"name": "临时"}, "role": "building", "allowed_scopes": ["E"]}
        self.controller.session = bad
        with self.assertRaises(PortalError):
            self.manager.open_run(bad, "E")

    def test_disabled_account_denied(self):
        self.auth.disabled.add("ou-disabled")
        sess = make_session(open_id="ou-disabled")
        self.controller.session = sess
        with self.assertRaises(PortalError):
            self.manager.open_run(sess, "E")

    def test_scope_unauthorized_denied(self):
        sess = make_session(scopes=("B",))
        self.controller.session = sess
        with self.assertRaises(PortalError):
            self.manager.open_run(sess, "E")

    def test_owner_cannot_read_other_account_draft(self):
        run = self.open(scope="E")
        other = make_session(open_id="ou-other")
        self.controller.session = other
        with self.assertRaises(PortalError):
            self.manager.get_run(other, run["id"])


class OriginTests(PanelTestBase):
    def test_post_without_origin_denied(self):
        self.open(scope="E")
        client = self.client()
        resp = client.post("/api/assistant/notice-panel/open", json={"scope": "E"})
        self.assertEqual(resp.status_code, 400)
        self.assertIn("来源", resp.json()["error"])

    def test_post_with_matching_origin_ok(self):
        client = self.client()
        self.controller.session = make_session()
        resp = client.post("/api/assistant/notice-panel/open", json={"scope": "E"},
                           headers={"Origin": "http://testserver"})
        self.assertEqual(resp.status_code, 202)
        self.assertTrue(resp.json()["ok"])


class EditionTests(PanelTestBase):
    def test_before_8_no_edition(self):
        self.set_time(dt.datetime(2026, 10, 9, 6, 30, tzinfo=panel.ZONE))
        self.controller.session = make_session()
        client = self.client()
        resp = client.get("/api/assistant/notice-panel")
        data = resp.json()["data"]
        self.assertIsNone(data["edition"])
        self.assertGreater(data["next_at"], 0)
        with self.assertRaises(PortalError):
            self.manager.open_run(make_session(), "E")

    def test_morning_edition_metadata(self):
        self.set_time(dt.datetime(2026, 10, 9, 12, 0, tzinfo=panel.ZONE))
        self.controller.session = make_session()
        client = self.client()
        data = client.get("/api/assistant/notice-panel").json()["data"]
        self.assertEqual(data["edition"]["slot"], "morning")
        self.assertEqual(data["edition"]["label"], "08:00通告待办")
        self.assertEqual(data["edition"]["date"], "2026-10-09")

    def test_evening_edition_metadata(self):
        self.set_time(dt.datetime(2026, 10, 9, 18, 0, tzinfo=panel.ZONE))
        self.controller.session = make_session()
        client = self.client()
        data = client.get("/api/assistant/notice-panel").json()["data"]
        self.assertEqual(data["edition"]["slot"], "evening")
        self.assertEqual(data["edition"]["label"], "17:00通告待办")

    def test_cross_date_creates_new_run(self):
        morning = self.open(scope="E")
        self.assertEqual(morning["slot"], "morning")
        self.set_time(dt.datetime(2026, 10, 10, 10, 0, tzinfo=panel.ZONE))
        next_day = self.open(scope="E")
        self.assertNotEqual(morning["id"], next_day["id"])
        self.assertEqual(next_day["date"], "2026-10-10")


class SlotTests(PanelTestBase):
    def test_default_uses_current_morning_edition(self):
        run = self.open(scope="E")
        self.assertEqual(run["slot"], "morning")
        self.assertEqual(run["date"], "2026-10-09")

    def test_explicit_morning_ok(self):
        run = self.open(scope="E", slot="morning")
        self.assertEqual(run["slot"], "morning")

    def test_future_evening_rejected(self):
        with self.assertRaises(PortalError):
            self.open(scope="E", slot="evening")

    def test_evening_after_17_ok(self):
        self.set_time(dt.datetime(2026, 10, 9, 18, 30, tzinfo=panel.ZONE))
        run = self.open(scope="E", slot="evening")
        self.assertEqual(run["slot"], "evening")

    def test_invalid_slot_rejected(self):
        with self.assertRaises(PortalError):
            self.open(scope="E", slot="lunch")

    def test_open_route_accepts_slot(self):
        client = self.client()
        self.controller.session = make_session()
        resp = client.post("/api/assistant/notice-panel/open", json={"scope": "E", "slot": "morning"},
                           headers={"Origin": "http://testserver"})
        self.assertEqual(resp.status_code, 202)
        self.assertEqual(resp.json()["data"]["run"]["slot"], "morning")


class RunPersistenceTests(PanelTestBase):
    def test_repeat_open_reuses_same_run(self):
        first = self.open(scope="E")
        second = self.open(scope="E")
        self.assertEqual(first["id"], second["id"])

    def test_restart_keeps_same_run_and_drafts(self):
        run = self.open(scope="E")
        self.select_fill_save(run["id"], "a", content="保留内容")
        # Simulate restart: new manager on the same store.
        manager2 = NoticePanel(self.controller, self.runtime)
        self.addCleanup(manager2.close)
        replay = manager2.public_run(run["id"])
        self.assertEqual(replay["state"], "edit")
        self.assertTrue(replay["items"][0]["selected"])
        self.assertEqual(replay["items"][0]["draft"]["content"], "保留内容")

    def test_polling_does_not_rebuild_sources(self):
        run = self.open(scope="E")
        calls_after_open = self.controller.build_calls
        self.manager.get_run(make_session(), run["id"])
        self.assertEqual(self.controller.build_calls, calls_after_open)
        self.assertEqual(self.controller.get_ongoing_calls, 1)  # only the initial prepare


class SelectionDraftTests(PanelTestBase):
    def test_selection_and_mode_per_row(self):
        run = self.open(scope="E")
        self.action(run["id"], "save", changes=[{"key": "a", "selected": True}])
        after = self.action(run["id"], "save", changes=[{"key": "a", "edit_all": True}])
        self.assertTrue(self.item(after, "a")["selected"])
        self.assertTrue(self.item(after, "a")["edit_all"])
        self.assertFalse(self.item(after, "b")["selected"])
        self.assertFalse(self.item(after, "b")["edit_all"])

    def test_draft_allowed_without_selected(self):
        run = self.open(scope="E")
        after = self.action(run["id"], "save", changes=[{"key": "b", "draft": {"content": "未选中填词"}}])
        self.assertEqual(self.item(after, "b")["draft"]["content"], "未选中填词")
        self.assertFalse(self.item(after, "b")["selected"])

    def test_deselect_preserves_draft_and_reselect_restores(self):
        run = self.open(scope="E")
        self.select_fill_save(run["id"], "a", content="已编辑", progress="进展")
        deselected = self.action(run["id"], "save", changes=[{"key": "a", "selected": False}])
        self.assertFalse(self.item(deselected, "a")["selected"])
        self.assertEqual(self.item(deselected, "a")["draft"]["content"], "已编辑")
        reselected = self.action(run["id"], "save", changes=[{"key": "a", "selected": True}])
        self.assertTrue(self.item(reselected, "a")["selected"])
        self.assertEqual(self.item(reselected, "a")["draft"]["content"], "已编辑")

    def test_readonly_field_ignored(self):
        run = self.open(scope="E")
        after = self.select_fill_save(run["id"], "a", title="尝试改标题", progress="进展")
        self.assertEqual(self.item(after, "a")["draft"].get("title"), "测试a")
        self.assertEqual(self.item(after, "a")["draft"].get("progress"), "进展")

    def test_duplicate_keys_rejected(self):
        run = self.open(scope="E")
        with self.assertRaises(PortalError):
            self.action(run["id"], "save", changes=[{"key": "a", "selected": True},
                                                    {"key": "a", "draft": {"content": "重复"}}])

    def test_brief_fields_captured_before_filling(self):
        run = self.open(scope="E")
        after = self.select_fill_save(run["id"], "a", progress="已填")
        # brief_fields should be computed while progress was still blank.
        self.assertIn("progress", self.item(after, "a")["brief_fields"])
        self.assertIn("notice_action", self.item(after, "a")["brief_fields"])

    def test_missing_value_retains_draft_on_preview(self):
        run = self.open(scope="E")
        self.select_fill_save(run["id"], "a", content="填了内容")
        after = self.preview(run["id"])
        self.assertEqual(after["state"], "edit")
        self.assertTrue(after["error"])
        self.assertEqual(self.item(after, "a")["draft"].get("content"), "填了内容")


class StateGuardTests(PanelTestBase):
    def test_confirm_from_edit_fails_without_native(self):
        run = self.open(scope="E")
        self.select_fill_save(run["id"], "a", content="x", progress="y")
        with self.assertRaises(PortalError):
            self.action(run["id"], "confirm")
        self.assertEqual(self.native.await_count, 0)

    def test_save_while_running_fails(self):
        run = self.open(scope="E")
        doc = self.store.get_document(panel.NS, run["id"])
        doc["state"] = "running"
        self.store.put_document(panel.NS, run["id"], doc)
        with self.assertRaises(PortalError):
            self.action(run["id"], "save", changes=[{"key": "a", "selected": True}])

    def test_edit_only_from_confirm(self):
        run = self.open(scope="E")
        with self.assertRaises(PortalError):
            self.action(run["id"], "edit")

    def test_confirm_and_edit_accept_no_changes(self):
        run, _ = self.preview_confirm_flow(content="内容", progress="进展")
        with self.assertRaises(PortalError):
            self.action(run["id"], "confirm", changes=[{"key": "a", "selected": True}])
        with self.assertRaises(PortalError):
            self.action(run["id"], "edit", changes=[{"key": "a", "selected": True}])

    def test_retry_and_refresh_accept_no_changes(self):
        run = self.open(scope="E")
        with self.assertRaises(PortalError):
            self.action(run["id"], "refresh", changes=[{"key": "a", "selected": True}])
        with self.assertRaises(PortalError):
            self.action(run["id"], "retry", changes=[{"key": "a", "selected": True}])

    def test_zero_selected_preview_stays_edit(self):
        run = self.open(scope="E")
        after = self.preview(run["id"])
        self.assertEqual(after["state"], "edit")
        self.assertTrue(after["error"])

    def test_refresh_rejects_running(self):
        run = self.open(scope="E")
        doc = self.store.get_document(panel.NS, run["id"])
        doc["state"] = "running"
        self.store.put_document(panel.NS, run["id"], doc)
        with self.assertRaises(PortalError):
            self.action(run["id"], "refresh")

    def test_refresh_rejects_preparing(self):
        run = self.open(scope="E")
        doc = self.store.get_document(panel.NS, run["id"])
        doc["state"] = "preparing"
        self.store.put_document(panel.NS, run["id"], doc)
        with self.assertRaises(PortalError):
            self.action(run["id"], "refresh")


class PreviewConfirmTests(PanelTestBase):
    def test_full_preview_then_confirm_runs_native_sender(self):
        self.native.return_value = JSONResponse({"ok": True, "data": {"job_id": "job-1", "initial_phase": "accepted"}})
        self.service.get_job = lambda job_id: {"phase": "success"}
        run, _ = self.preview_confirm_flow(content="完整内容", progress="人员到位")
        confirmed = self.action(run["id"], "confirm")
        self.assertIn(confirmed["state"], ("running", "done"))
        self.assertEqual(confirmed["items"][0]["phase"], "success")
        self.assertTrue(self.native.await_count == 1)
        args = self.native.call_args
        submitted = args.args[3]
        self.assertEqual(submitted["content"], "完整内容")
        self.assertTrue(args.kwargs["card_submission"])
        self.assertTrue(set(("source_record_id", "scope", "work_type")).issubset(submitted))

    def test_mandatory_missing_blocks_preview_and_confirm(self):
        run = self.open(scope="E")
        self.select_fill_save(run["id"], "a", content="缺进度")
        previewed = self.preview(run["id"])
        self.assertEqual(previewed["state"], "edit")
        with self.assertRaises(PortalError):
            self.action(run["id"], "confirm")
        self.assertEqual(self.native.await_count, 0)

    def test_edit_back_from_confirm_preserves_data(self):
        run, _ = self.preview_confirm_flow(content="全", progress="全")
        edited = self.action(run["id"], "edit")
        self.assertEqual(edited["state"], "edit")
        self.assertEqual(self.item(edited, "a")["draft"]["content"], "全")

    def test_double_click_version_conflict_409(self):
        run = self.open(scope="E")
        self.action(run["id"], "save", changes=[{"key": "a", "selected": True}])
        stale_revision = self.store.get_document(panel.NS, run["id"])["revision"] - 1
        with self.assertRaises(PortalError) as ctx:
            self.action(run["id"], "save", revision=stale_revision,
                        changes=[{"key": "a", "draft": {"content": "迟到"}}])
        self.assertEqual(type(ctx.exception).__name__, "PanelVersionConflict")

    def test_past_date_draft_cannot_send(self):
        run, _ = self.preview_confirm_flow(content="x", progress="y")
        # A prior date's already-previewed draft cannot be confirmed later.
        self.set_time(dt.datetime(2026, 10, 10, 10, 0, tzinfo=panel.ZONE))
        with self.assertRaises(PortalError):
            self.action(run["id"], "confirm")

    def test_past_date_save_rejected(self):
        run = self.open(scope="E")
        # An unsubmitted draft from yesterday can neither be saved nor sent today.
        self.set_time(dt.datetime(2026, 10, 10, 10, 0, tzinfo=panel.ZONE))
        with self.assertRaises(PortalError):
            self.select_fill_save(run["id"], "a", content="昨天")

    def test_api_version_conflict_returns_409_with_current_run(self):
        run = self.open(scope="E")
        self.controller.session = make_session()
        client = self.client()
        stale = run["revision"] - 1
        resp = client.post(f"/api/assistant/notice-panel/{run['id']}/action",
                           json={"action": "save", "revision": stale,
                                 "changes": [{"key": "b", "selected": True}]},
                           headers={"Origin": "http://testserver"})
        self.assertEqual(resp.status_code, 409)
        body = resp.json()
        self.assertFalse(body["ok"])
        self.assertEqual(body["data"]["run"]["id"], run["id"])
        self.assertEqual(body["data"]["run"]["revision"], run["revision"])


class RefreshGenTests(PanelTestBase):
    def test_dirty_draft_refresh_allowed_and_generation_bumps(self):
        run = self.open(scope="E")
        gen_before = run["generation"]
        self.select_fill_save(run["id"], "a", content="未提交草稿")
        refreshed = self.action(run["id"], "refresh")
        self.assertEqual(refreshed["state"], "edit")
        self.assertEqual(refreshed["generation"], gen_before + 1)

    def test_done_refresh_allowed_and_clears_public_result(self):
        self.native.return_value = JSONResponse({"ok": True, "data": {"job_id": "job-d", "initial_phase": "accepted"}})
        self.service.get_job = lambda job_id: {"phase": "success"}
        run, _ = self.preview_confirm_flow(content="内容", progress="进展")
        done = self.action(run["id"], "confirm")
        self.assertEqual(done["state"], "done")
        self.assertEqual(done["items"][0]["phase"], "success")
        refreshed = self.action(run["id"], "refresh")
        self.assertEqual(refreshed["state"], "edit")
        self.assertEqual(refreshed["items"][0]["phase"], "")

    def test_operation_id_includes_generation(self):
        self.native.return_value = JSONResponse({"ok": True, "data": {"job_id": "job-g", "initial_phase": "accepted"}})
        self.service.get_job = lambda job_id: {"phase": "success"}
        run, _ = self.preview_confirm_flow(content="内容", progress="进展")
        confirmed = self.action(run["id"], "confirm")
        self.assertEqual(confirmed["state"], "done")
        op1 = self.store.get_document(panel.NS, run["id"])["items"][0]["operation_id"]
        self.assertIn(f":{run['generation']}:a", op1)
        # Refresh bumps generation so a later attempt for the same record is not deduped.
        refreshed = self.action(run["id"], "refresh")
        new_gen = refreshed["generation"]
        self.assertEqual(new_gen, run["generation"] + 1)
        self.select_fill_save(run["id"], "a", content="新内容", progress="新进展")
        previewed = self.preview(run["id"])
        self.assertEqual(previewed["state"], "confirm")
        confirmed2 = self.action(run["id"], "confirm")
        op2 = self.store.get_document(panel.NS, run["id"])["items"][0]["operation_id"]
        self.assertIn(f":{new_gen}:a", op2)
        self.assertNotEqual(op1, op2)


class RetryTests(PanelTestBase):
    def test_retry_only_failed_keeps_same_operation_id(self):
        self.native.side_effect = RuntimeError("网络失败")
        run, _ = self.preview_confirm_flow(content="c", progress="p")
        first = self.action(run["id"], "confirm")
        op1 = self.store.get_document(panel.NS, run["id"])["items"][0]["operation_id"]
        self.assertEqual(first["items"][0]["phase"], "failed")
        self.native.side_effect = None
        self.native.return_value = JSONResponse({"ok": True, "data": {"job_id": "job-r", "initial_phase": "accepted"}})
        self.service.get_job = lambda job_id: {"phase": "success"}
        retried = self.action(run["id"], "retry")
        self.assertEqual(retried["state"], "done")
        self.assertEqual(retried["items"][0]["phase"], "success")
        op2 = self.store.get_document(panel.NS, run["id"])["items"][0]["operation_id"]
        self.assertEqual(op1, op2)

    def test_retry_requires_done_with_failed_items(self):
        run = self.open(scope="E")
        with self.assertRaises(PortalError):
            self.action(run["id"], "retry")


class NativeDispatchTests(PanelTestBase):
    def test_error_response_becomes_failed_not_accepted(self):
        self.native.return_value = JSONResponse({"ok": False, "error": "权限不足"}, status_code=403)
        run, _ = self.preview_confirm_flow(content="x", progress="y")
        confirmed = self.action(run["id"], "confirm")
        item = confirmed["items"][0]
        self.assertEqual(item["phase"], "failed")
        self.assertIn("权限不足", item["result"])
        # No job id means no accepted-without-result state.
        self.assertTrue(confirmed["state"] == "done")

    def test_ok_but_no_job_id_is_failed(self):
        self.native.return_value = JSONResponse({"ok": True, "data": {}})
        run, _ = self.preview_confirm_flow(content="x", progress="y")
        confirmed = self.action(run["id"], "confirm")
        self.assertEqual(confirmed["items"][0]["phase"], "failed")

    def test_superseded_job_marked_unavailable_not_reinserted(self):
        self.native.return_value = JSONResponse({"ok": True, "data": {"job_id": "job-s", "initial_phase": "accepted"}})
        self.service.get_job = lambda job_id: {"phase": "accepted", "superseded_by_job_id": "job-later"}
        run, _ = self.preview_confirm_flow(content="x", progress="y")
        confirmed = self.action(run["id"], "confirm")
        self.assertEqual(confirmed["items"][0]["phase"], "unavailable")

    def test_response_loss_retains_operation_id(self):
        self.native.side_effect = RuntimeError("超时")
        run, _ = self.preview_confirm_flow(content="x", progress="y")
        self.action(run["id"], "confirm")
        op = self.store.get_document(panel.NS, run["id"])["items"][0]["operation_id"]
        self.assertTrue(op.startswith("notice-panel:"))


class AuthReevalTests(PanelTestBase):
    def _queue_undispatched_confirm(self, run):
        """Build a running doc with a queued item that hasn't been dispatched yet."""
        doc = self.store.get_document(panel.NS, run["id"])
        item = next(it for it in doc["items"] if it.get("selected"))
        doc["state"] = "running"
        doc["confirmed_session"] = self.manager._store_session(make_session())
        item["phase"] = "queued"
        item["operation_id"] = f"notice-panel:{run['id']}:{doc['generation']}:{item['key']}"
        item["result"] = ""
        item["payload"] = {"content": "x", "scope": doc["scope"], "work_type": "maintenance"}
        self.store.put_document(panel.NS, run["id"], doc)
        self.manager._track(run["id"])
        return doc["id"]

    def test_revoked_permission_before_dispatch_blocks_send(self):
        run = self.open(scope="E")
        self.select_fill_save(run["id"], "a", content="x", progress="y")
        self.preview(run["id"])
        identity = self._queue_undispatched_confirm(run)
        # Revoke the user's scope in the *current* auth config before dispatch.
        self.auth.user_scopes["ou-li"] = []
        manager2 = NoticePanel(self.controller, self.runtime)
        self.addCleanup(manager2.close)
        manager2.tick()
        after = manager2.public_run(identity)
        item = after["items"][0]
        self.assertEqual(item["phase"], "failed")
        self.assertIn("权限", item["result"])
        self.assertEqual(self.native.await_count, 0)

    def test_disabled_before_dispatch_blocks_send(self):
        run = self.open(scope="E")
        self.select_fill_save(run["id"], "a", content="x", progress="y")
        self.preview(run["id"])
        identity = self._queue_undispatched_confirm(run)
        self.auth.disabled.add("ou-li")
        manager2 = NoticePanel(self.controller, self.runtime)
        self.addCleanup(manager2.close)
        manager2.tick()
        after = manager2.public_run(identity)
        self.assertEqual(after["items"][0]["phase"], "failed")
        self.assertEqual(self.native.await_count, 0)


class QueueAndTickTests(PanelTestBase):
    def test_tick_resumes_running_run_after_restart(self):
        self.native.return_value = JSONResponse({"ok": True, "data": {"job_id": "job-tick", "initial_phase": "accepted"}})
        self.service.get_job = lambda job_id: {"phase": "accepted"}
        run, _ = self.preview_confirm_flow(content="c", progress="p")
        confirmed = self.action(run["id"], "confirm")
        self.assertEqual(confirmed["state"], "running")
        self.assertEqual(self.native.await_count, 1)

        # Restart: a brand-new manager on the same store must resume via tick().
        manager2 = NoticePanel(self.controller, self.runtime)
        self.addCleanup(manager2.close)
        self.service.get_job = lambda job_id: {"phase": "success"}
        manager2.tick()
        resumed = manager2.public_run(run["id"])
        self.assertEqual(resumed["state"], "done")
        self.assertEqual(resumed["items"][0]["phase"], "success")
        # No duplicate native submission on resume.
        self.assertEqual(self.native.await_count, 1)

    def test_prior_day_unsubmitted_prepare_expired_in_tick(self):
        # Force a preparing run for yesterday and leave it in the resume index.
        self.set_time(dt.datetime(2026, 10, 8, 10, 0, tzinfo=panel.ZONE))
        yesterday = self.open(scope="E")
        doc = self.store.get_document(panel.NS, yesterday["id"])
        doc["state"] = "preparing"
        self.store.put_document(panel.NS, yesterday["id"], doc)
        self.manager._track(yesterday["id"])
        # Today: the stale prepare must be expired, not dispatched.
        self.set_time(dt.datetime(2026, 10, 9, 10, 0, tzinfo=panel.ZONE))
        manager2 = NoticePanel(self.controller, self.runtime)
        self.addCleanup(manager2.close)
        manager2.tick()
        after = manager2.public_run(yesterday["id"])
        self.assertEqual(after["state"], "failed")
        self.assertTrue(after["error"])

    def test_idle_runs_removed_from_pending_index(self):
        run = self.open(scope="E")
        pending = (self.store.get_document(panel.NS, "pending") or {}).get("ids", [])
        self.assertNotIn(run["id"], pending)


class WorkTypeSortingTests(PanelTestBase):
    def test_empty_complete_source_is_valid_empty_edit(self):
        self.build_impl = lambda *a, **kw: []
        run = self.open(scope="E")
        self.assertEqual(run["state"], "edit")
        self.assertEqual(run["items"], [])

    def test_public_items_return_work_type_not_ids(self):
        run = self.open(scope="E")
        item = self.item(run, "a")
        self.assertEqual(item["work_type"], "maintenance")
        self.assertNotIn("source_record_id", item["draft"])
        self.assertNotIn("target_record_id", item["draft"])

    def test_sort_plans_first_ongoing_and_blocked_last(self):
        def sorted_build(service, scope, work, ongoing, current=None):
            return [
                self._item("blocked_update", "E", blocked="原因", action="update"),
                self._item("plan_b", "E", action="start"),
                self._item("ongoing_a", "E", action="update"),
                self._item("plan_a", "E", action="start"),
                self._item("blocked_plan", "E", blocked="原因", action="start"),
            ]
        self.build_impl = sorted_build
        run = self.open(scope="E")
        keys = [it["key"] for it in run["items"]]
        self.assertEqual(keys, ["plan_a", "plan_b", "ongoing_a", "blocked_plan", "blocked_update"])

    def test_all_seven_scopes_open(self):
        for scope in ("A", "B", "C", "D", "E", "H", "110"):
            run = self.open(scope=scope)
            self.assertEqual(run["scope"], scope)
            self.assertEqual(run["state"], "edit")

    def test_reopen_preserves_selection(self):
        run = self.open(scope="E")
        self.action(run["id"], "save", changes=[{"key": "a", "selected": True}])
        reopened = self.open(scope="E")
        self.assertEqual(reopened["id"], run["id"])
        self.assertTrue(self.item(reopened, "a")["selected"])


class PublicPayloadTests(PanelTestBase):
    def test_no_raw_ids_or_private_payload_leak(self):
        run, _ = self.preview_confirm_flow(content="x", progress="y")
        public = self.manager.public_run(run["id"])
        for item in public["items"]:
            self.assertNotIn("source_record_id", item["draft"])
            self.assertNotIn("target_record_id", item["draft"])
            self.assertNotIn("payload", item)
            self.assertNotIn("operation_id", item)
        self.assertNotIn("owner", public)

    def test_no_cloud_refresh_on_open_or_poll(self):
        run = self.open(scope="E")
        self.manager.get_run(make_session(), run["id"])
        self.assertEqual(self.controller.build_calls, 1)


class EveningSortingTests(PanelTestBase):
    def test_evening_prefers_update_over_start(self):
        def sorted_build(service, scope, work, ongoing, current=None):
            return [
                self._item("blocked_update", "E", blocked="原因", action="update"),
                self._item("plan_b", "E", action="start"),
                self._item("ongoing_a", "E", action="update"),
                self._item("plan_a", "E", action="start"),
                self._item("blocked_plan", "E", blocked="原因", action="start"),
            ]
        self.build_impl = sorted_build
        self.set_time(dt.datetime(2026, 10, 9, 18, 0, tzinfo=panel.ZONE))
        run = self.open(scope="E", slot="evening")
        keys = [it["key"] for it in run["items"]]
        # evening → ongoing (update) first, then starts, then blocked (update before start).
        self.assertEqual(keys, ["ongoing_a", "plan_a", "plan_b", "blocked_update", "blocked_plan"])

    def test_morning_still_prefers_start_over_update(self):
        def sorted_build(service, scope, work, ongoing, current=None):
            return [
                self._item("blocked_update", "E", blocked="原因", action="update"),
                self._item("plan_b", "E", action="start"),
                self._item("ongoing_a", "E", action="update"),
                self._item("plan_a", "E", action="start"),
                self._item("blocked_plan", "E", blocked="原因", action="start"),
            ]
        self.build_impl = sorted_build
        run = self.open(scope="E", slot="morning")
        keys = [it["key"] for it in run["items"]]
        # morning → starts first, then ongoing, then blocked (start before update).
        self.assertEqual(keys, ["plan_a", "plan_b", "ongoing_a", "blocked_plan", "blocked_update"])


class ApplyChangesFrontendPayloadTests(PanelTestBase):
    def test_frontend_payload_deselect_edit_all_true_with_draft(self):
        run = self.open(scope="E")
        self.select_fill_save(run["id"], "a", content="旧内容", progress="进展")
        after = self.action(run["id"], "save", changes=[
            {"key": "a", "selected": False, "edit_all": True, "draft": {"content": "新内容"}}
        ])
        item = self.item(after, "a")
        self.assertFalse(item["selected"])
        self.assertTrue(item["edit_all"])
        self.assertEqual(item["draft"]["content"], "新内容")

    def test_frontend_payload_edit_all_false_without_selection(self):
        run = self.open(scope="E")
        after = self.action(run["id"], "save", changes=[
            {"key": "b", "selected": False, "edit_all": False, "draft": {"content": "未选中填词"}}
        ])
        item = self.item(after, "b")
        self.assertFalse(item["selected"])
        self.assertFalse(item["edit_all"])
        self.assertEqual(item["draft"]["content"], "未选中填词")

    def test_edit_all_true_on_unselected_item_is_allowed(self):
        run = self.open(scope="E")
        after = self.action(run["id"], "save", changes=[
            {"key": "b", "selected": False, "edit_all": True, "draft": {}}
        ])
        self.assertTrue(self.item(after, "b")["edit_all"])
        self.assertFalse(self.item(after, "b")["selected"])

    def test_select_blocked_item_still_forbidden(self):
        self.build_impl = lambda *a, **kw: [self._item("blk", "E", blocked="理由", action="start")]
        run = self.open(scope="E")
        with self.assertRaises(PortalError):
            self.action(run["id"], "save", changes=[
                {"key": "blk", "selected": True, "edit_all": False, "draft": {}}
            ])

    def test_deselect_blocked_item_allowed(self):
        run = self.open(scope="E")
        doc = self.store.get_document(panel.NS, run["id"])
        item = next(it for it in doc["items"] if it["key"] == "b")
        item["blocked"] = "理由"
        item["selected"] = True
        self.store.put_document(panel.NS, run["id"], doc)
        after = self.action(run["id"], "save", changes=[
            {"key": "b", "selected": False, "edit_all": True, "draft": {}}
        ])
        self.assertFalse(self.item(after, "b")["selected"])


class GetRunLockingTests(PanelTestBase):
    def test_get_run_completes_while_another_thread_holds_run_lock(self):
        run = self.open(scope="E")
        identity = run["id"]
        lock = self.manager._lock_for(identity)
        held = threading.Event()
        release = threading.Event()
        def holder():
            lock.acquire()
            held.set()
            release.wait(timeout=5)
            lock.release()
        t = threading.Thread(target=holder)
        t.start()
        self.assertTrue(held.wait(timeout=2))
        try:
            started = time.monotonic()
            result = self.manager.get_run(make_session(), identity)
            elapsed = time.monotonic() - started
            self.assertEqual(result["id"], identity)
            self.assertLess(elapsed, 1.0)
        finally:
            release.set()
            t.join(timeout=5)


class PendingOrderingTests(PanelTestBase):
    def test_confirm_commits_running_document_and_pending_index_together(self):
        run = self.open(scope="E")
        self.select_fill_save(run["id"], "a", content="x", progress="y")
        self.preview(run["id"])
        identity = run["id"]
        events = []
        orig_put = self.store.put_documents
        def spy_put(ns, payloads):
            events.append(copy.deepcopy(payloads))
            return orig_put(ns, payloads)
        self.store.put_documents = spy_put
        try:
            self.action(identity, "confirm")
        finally:
            self.store.put_documents = orig_put
        self.assertEqual(len(events), 1)
        self.assertEqual(events[0][identity]['state'], 'running')
        self.assertIn(identity, events[0]['pending']['ids'])


class TickLockNonBlockingTests(PanelTestBase):
    def test_tick_skips_identity_when_run_lock_busy(self):
        run = self.open(scope="E")
        identity = run["id"]
        self.manager._track(identity)
        lock = self.manager._lock_for(identity)
        held = threading.Event()
        release = threading.Event()
        def holder():
            lock.acquire()
            held.set()
            release.wait(timeout=5)
            lock.release()
        t = threading.Thread(target=holder)
        t.start()
        self.assertTrue(held.wait(timeout=2))
        try:
            started = time.monotonic()
            self.manager.tick()
            elapsed = time.monotonic() - started
            self.assertLess(elapsed, 1.0)
            # Lock busy → tick skips the identity and must not untrack it.
            pending = (self.store.get_document(panel.NS, "pending") or {}).get("ids", [])
            self.assertIn(identity, pending)
        finally:
            release.set()
            t.join(timeout=5)


class AdvanceRetryPrecheckTests(PanelTestBase):
    def test_retry_does_not_resubmit_superseded_native_job(self):
        self.native.return_value = JSONResponse(
            {"ok": True, "data": {"job_id": "job-s", "initial_phase": "accepted"}})
        jobs = {"job-s": {"phase": "failed", "error": "发送失败"}}
        self.service.get_job = lambda job_id: jobs.get(job_id)
        run, _ = self.preview_confirm_flow(content="c", progress="p")
        failed = self.action(run["id"], "confirm")
        self.assertEqual(failed["items"][0]["phase"], "failed")
        self.assertEqual(self.native.await_count, 1)
        # The original native job is now superseded at cloud level.
        jobs["job-s"]["superseded_by_job_id"] = "job-later"
        retried = self.action(run["id"], "retry")
        self.assertEqual(retried["items"][0]["phase"], "unavailable")
        self.assertIn("已失效", retried["items"][0]["result"])
        self.assertEqual(self.native.await_count, 1)

    def test_retry_does_not_resubmit_when_native_job_missing(self):
        self.native.return_value = JSONResponse(
            {"ok": True, "data": {"job_id": "job-m", "initial_phase": "accepted"}})
        self.service.get_job = lambda job_id: {"phase": "failed", "error": "发送失败"}
        run, _ = self.preview_confirm_flow(content="c", progress="p")
        failed = self.action(run["id"], "confirm")
        self.assertEqual(failed["items"][0]["phase"], "failed")
        self.assertEqual(self.native.await_count, 1)
        # The job is deleted at cloud level before the retry is attempted.
        self.service.get_job = lambda job_id: None
        retried = self.action(run["id"], "retry")
        self.assertEqual(retried["items"][0]["phase"], "unavailable")
        self.assertEqual(self.native.await_count, 1)

    def test_initial_phase_failed_still_reads_job_error(self):
        self.native.return_value = JSONResponse(
            {"ok": True, "data": {"job_id": "job-f", "initial_phase": "failed"}})
        self.service.get_job = lambda job_id: {"phase": "failed", "error": "号码校验失败"}
        run, _ = self.preview_confirm_flow(content="c", progress="p")
        result = self.action(run["id"], "confirm")
        item = result["items"][0]
        self.assertEqual(item["phase"], "failed")
        self.assertIn("号码校验失败", item["result"])


class QueueRaceTests(PanelTestBase):
    def test_submit_race_with_closed_pool_releases_slot(self):
        run = self.open(scope="E")  # exercises the sync queue path first
        self._queue_patcher.stop()  # restore the real threaded _queue
        with patch.object(self.manager.pool, "submit",
                          side_effect=RuntimeError("pool closed")):
            result = self.manager._queue("race:key", lambda: None)
        self.assertFalse(result)
        with self.manager.lock:
            self.assertNotIn("race:key", self.manager.inflight)
        # The acquired slot was released; acquiring it without blocking succeeds.
        self.assertTrue(self.manager.slots.acquire(False))
        self.manager.slots.release()


class EditionMetadataSignatureTests(PanelTestBase):
    def test_edition_metadata_without_allowed_arg(self):
        edition, next_at = panel.edition_metadata()
        self.assertIsNotNone(edition)
        self.assertEqual(edition["slot"], "morning")
        self.assertGreater(next_at, 0)


if __name__ == "__main__":
    unittest.main()
