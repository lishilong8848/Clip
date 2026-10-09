"""Assistant-side native notice panel.

Persistent, per-real-user, per-scope notice drafts living in the assistant UI.
No Feishu transport/callbacks/card renderer.  The only business write path is
``notice_actions.submit_notice_action(..., card_submission=True)``, invoked only
after explicit user preview + confirmation.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import logging
import re
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from .cabinet_guest import is_cabinet_guest
from .portal_service import PortalError

ZONE = dt.timezone(dt.timedelta(hours=8))
NS = "assistant_notice_panels"
SCOPES = ("A", "B", "C", "D", "E", "H", "110")
MORNING_AT = 8
EVENING_AT = 17
FIELD_VALUE_MAX = 1000
TERMINAL_PHASES = {"success", "failed", "unavailable"}
ACTIONS = ("save", "preview", "confirm", "edit", "refresh", "retry", "confirm_unmatched")


def now():
    return dt.datetime.now(ZONE)


def _edition_now(at=None):
    """Return current Beijing edition descriptor or ``None`` before 08:00."""
    at = at or now()
    if at.hour < MORNING_AT:
        return None
    slot = "morning" if at.hour < EVENING_AT else "evening"
    return {"date": at.date().isoformat(), "slot": slot}


def _boundary(day, slot):
    return dt.datetime.combine(day, dt.time(MORNING_AT if slot == "morning" else EVENING_AT), tzinfo=ZONE)


def edition_identity(date_iso, slot):
    return uuid.uuid5(uuid.NAMESPACE_URL, f"assistant-notice-panel-edition:{date_iso}:{slot}").hex


def run_identity(owner_open_id, scope, date_iso, slot):
    return uuid.uuid5(uuid.NAMESPACE_URL,
                      f"assistant-notice-panel:{owner_open_id}:{scope}:{date_iso}:{slot}").hex


def brief_field_keys(item):
    """Editable fields still blank, plus the operation choice, frozen on selection."""
    draft = item.get("draft") or {}
    return [f["key"] for f in item.get("fields") or []
            if f["key"] == "notice_action" or (not f.get("readonly") and
               (draft.get(f["key"]) is None or not str(draft[f["key"]]).strip()))]


def edition_metadata(at=None):
    """Current edition metadata for the polling endpoint (no source loading)."""
    at = at or now()
    current = _edition_now(at)
    data = None
    if current:
        boundary = _boundary(at.date(), current["slot"])
        label = "08:00通告待办" if current["slot"] == "morning" else "17:00通告待办"
        data = {"id": edition_identity(current["date"], current["slot"]),
                "date": current["date"], "slot": current["slot"], "label": label,
                "published_at": int(boundary.timestamp())}
    if at.hour < MORNING_AT:
        next_boundary = _boundary(at.date(), "morning")
    elif current and current["slot"] == "morning":
        next_boundary = _boundary(at.date(), "evening")
    else:
        next_boundary = _boundary(at.date() + dt.timedelta(days=1), "morning")
    next_at = int(next_boundary.timestamp())
    if data is not None:
        data["next_at"] = next_at
    return data, next_at


def _fingerprint(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode()).hexdigest()


_NOTICE_SOP_FIELD = {"key": "notice_sop", "label": "本次工单", "required": True,
                     "readonly": False, "kind": "notice_sop", "options": []}


def _migrate_legacy_notice_sop(doc):
    """In-memory migration of legacy ``work_order_choice`` edit drafts to ``notice_sop``.

    Only edit/failed drafts are converted so confirm/running/done native payloads are
    never touched.  The migration reassigns the field descriptor, maps the old
    binary choice into a normalized ``notice_sop`` draft, rewrites brief-field keys,
    and otherwise preserves every other text/selection/revision detail.
    """
    if not isinstance(doc, dict) or str(doc.get("state") or "") not in ("edit", "failed"):
        return
    for item in doc.get("items") or []:
        fields = item.get("fields") or []
        fields_by_key = {f.get("key"): f for f in fields if isinstance(f, dict)}
        old_field = fields_by_key.get("work_order_choice")
        if not old_field or any(f.get("key") == "notice_sop" for f in fields):
            continue
        draft = item.setdefault("draft", {})
        work_type = str(draft.get("work_type") or "").strip()
        action = str(item.get("action") or draft.get("action") or "").strip()
        if work_type not in ("maintenance", "polling", "adjust") or action != "start":
            continue
        # Replace the descriptor in place, preserving order relative to other fields.
        for field in fields:
            if field is old_field or (isinstance(field, dict) and field.get("key") == "work_order_choice"):
                field.clear()
                field.update(_NOTICE_SOP_FIELD)
                break
        # Migrate the legacy binary choice through the native normalizer.
        from .notice_panel_data import _notice_sop_from_draft
        draft["notice_sop"] = _notice_sop_from_draft(draft, work_type, action)
        draft.pop("work_order_choice", None)
        if isinstance(item.get("brief_fields"), list):
            item["brief_fields"] = [
                "notice_sop" if str(key).strip() == "work_order_choice" else key
                for key in item["brief_fields"]
            ]


class PanelVersionConflict(PortalError):
    """Optimistic-revision rejection; carries the current persisted run for 409."""

    def __init__(self, current_run):
        super().__init__("数据已在其他窗口更新，请刷新后重试。")
        self.current_run = current_run


class NoticePanel:
    def __init__(self, controller, runtime):
        self.controller = controller
        self.runtime = runtime
        self.store = runtime.state_store
        self.lock = threading.RLock()
        self._run_locks = {}
        self.inflight = set()
        self.slots = threading.BoundedSemaphore(8)
        # Native assistant panel uses a maximum of two worker threads.
        self.pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="NoticePanel")
        self.closed = False

    # ---- locking/queue helpers ------------------------------------------
    def _lock_for(self, identity):
        with self.lock:
            return self._run_locks.setdefault(identity, threading.RLock())

    def _queue(self, key, fn, *args):
        with self.lock:
            if self.closed or key in self.inflight:
                return False
            if not self.slots.acquire(False):
                return False
            self.inflight.add(key)

        def run():
            try:
                fn(*args)
            except Exception:
                logging.exception("通告待办后台任务失败")
            finally:
                with self.lock:
                    self.inflight.discard(key)
                self.slots.release()

        try:
            self.pool.submit(run)
        except RuntimeError:
            # Pool closed while we raced to submit: release the slot/discard the
            # inflight key so neither leaks. The pending document stays tracked so
            # a restart can still resume it.
            with self.lock:
                self.inflight.discard(key)
            self.slots.release()
            return False
        return True

    def _track(self, identity, active=True):
        with self.lock:
            ids = (self.store.get_document(NS, "pending") or {}).get("ids", [])
            ids = list(dict.fromkeys([*ids, identity])) if active else [x for x in ids if x != identity]
            self.store.put_document(NS, "pending", {"ids": ids})

    def _persist_pending(self, doc):
        # The task and its restart index must survive a crash together.
        with self.lock:
            ids = (self.store.get_document(NS, "pending") or {}).get("ids", [])
            ids = list(dict.fromkeys([*ids, doc["id"]]))
            self.store.put_documents(NS, {doc["id"]: doc, "pending": {"ids": ids}})

    # ---- document/auth helpers ------------------------------------------
    def _get(self, identity):
        if not re.fullmatch(r"[a-f0-9]{32}", str(identity or "")):
            raise PortalError("通告待办标识无效。")
        doc = self.store.get_document(NS, identity)
        if not doc:
            raise PortalError("通告待办不存在或已清空。")
        _migrate_legacy_notice_sop(doc)
        return doc

    @staticmethod
    def _real_user(session):
        if not isinstance(session, dict) or is_cabinet_guest(session):
            raise PortalError("请使用正式飞书账号登录后使用通告待办。")
        user = session.get("user")
        if not isinstance(user, dict):
            raise PortalError("登录状态无效。")
        open_id = str(user.get("open_id") or "").strip()
        if not open_id:
            raise PortalError("当前登录账号缺少正式身份，无法使用通告待办。")
        return session, open_id

    def _require_user(self, session):
        session, open_id = self._real_user(session)
        auth = self.runtime.auth_manager
        if auth._open_id_explicitly_disabled(open_id):
            raise PortalError("账号已停用，无法使用通告待办。")
        return session, open_id

    def _check_owner(self, doc, open_id):
        if str(doc.get("owner") or "") != open_id:
            raise PortalError("无权查看或操作其他账号的通告待办。")

    def _authorize_run(self, doc, session):
        scope = self.controller._authorized_scope_or_error(session, doc["scope"])
        doc["scope"] = scope
        return session

    def _store_session(self, session):
        user = session.get("user") if isinstance(session.get("user"), dict) else {}
        return {
            "user": {key: user.get(key) for key in ("open_id", "name") if user.get(key)},
            "role": session.get("role", ""),
            "allowed_scopes": list(session.get("allowed_scopes") or []),
        }

    def _session_from_doc(self, doc):
        """Rebuild a dispatch session from *current* auth config, never stale data."""
        saved = doc.get("confirmed_session")
        if not isinstance(saved, dict):
            raise PortalError("缺少操作人凭据，请重新确认。")
        user = saved.get("user")
        if not isinstance(user, dict):
            raise PortalError("登录状态无效。")
        open_id = str(user.get("open_id") or "").strip()
        if not open_id:
            raise PortalError("当前登录账号缺少正式身份，无法操作。")
        auth = self.runtime.auth_manager
        if auth._open_id_explicitly_disabled(open_id):
            raise PortalError("账号已停用，无法继续发送。")
        allowed_scopes = list(auth.scopes_for_open_id(open_id) or [])
        role = str(auth.role_for_open_id(open_id) or "").strip()
        session = {
            "user": {"open_id": open_id, "name": str(user.get("name") or "")},
            "role": role,
            "allowed_scopes": allowed_scopes,
        }
        self.controller._authorized_scope_or_error(session, doc["scope"])
        return session

    def _assert_current_date(self, doc):
        current = _edition_now()
        if not current or current["date"] != doc.get("date"):
            raise PortalError("此待办已过期，请重新打开今日待办。")

    # ---- public payloads ------------------------------------------------
    def public_items(self, doc):
        from .notice_panel_data import _normalize_item_notice_sop
        result = []
        for item in doc.get("items") or []:
            _normalize_item_notice_sop(item)
            item_draft = item.get("draft") or {}
            public_draft = {}
            for field in item.get("fields") or []:
                key = field["key"]
                if key in item_draft:
                    public_draft[key] = item_draft[key]
            result.append({
                "key": item.get("key"),
                "title": item.get("title", ""),
                "window": item.get("window", ""),
                "status": item.get("status", ""),
                "action": item.get("action", ""),
                "work_type": str(item_draft.get("work_type") or ""),
                "blocked": item.get("blocked", ""),
                "selected": bool(item.get("selected")),
                "edit_all": bool(item.get("edit_all")),
                "brief_fields": list(item.get("brief_fields") or []),
                "fields": item.get("fields") or [],
                "draft": public_draft,
                "preview": item.get("preview", ""),
                "error": item.get("error", ""),
                "phase": item.get("phase", ""),
                "result": item.get("result", ""),
                "convergence_confirmation_required": bool(item.get('convergence_confirmation')),
            })
        return result

    def public_run(self, identity):
        doc = self._get(identity)
        return {"id": identity, "revision": doc["revision"], "scope": doc["scope"],
                "date": doc["date"], "slot": doc["slot"], "state": doc["state"],
                "generation": int(doc.get("generation") or 1),
                "items": self.public_items(doc), "error": str(doc.get("error") or ""),
                "updated_at": float(doc.get("updated_at") or 0)}

    # ---- open/prepare ----------------------------------------------------
    def _new_run(self, open_id, scope, date_iso, slot):
        return {"id": run_identity(open_id, scope, date_iso, slot), "owner": open_id,
                "scope": scope, "date": date_iso, "slot": slot,
                "state": "preparing", "revision": 1, "generation": 1,
                "items": [], "error": "",
                "created_at": time.time(), "updated_at": time.time()}

    def open_run(self, session, scope, slot=None):
        session, open_id = self._require_user(session)
        scope = self.controller._authorized_scope_or_error(session, scope)
        if scope not in SCOPES:
            raise PortalError("通告待办暂不支持该范围，请选择具体楼栋。")
        at_dt = now()
        if slot is None:
            current = _edition_now(at_dt)
            if not current:
                raise PortalError("当前时间不在通告待办时段内，请于每日08:00后再次查看。")
        else:
            slot = str(slot or "").strip()
            if slot not in ("morning", "evening"):
                raise PortalError("时段时间无效。")
            current = {"date": at_dt.date().isoformat(), "slot": slot}
            if at_dt < _boundary(at_dt.date(), slot):
                raise PortalError("该时段尚未开始，请稍后再试。")
        identity = run_identity(open_id, scope, current["date"], current["slot"])
        with self._lock_for(identity):
            doc = self.store.get_document(NS, identity)
            if doc is None:
                doc = self._new_run(open_id, scope, current["date"], current["slot"])
                self._persist_pending(doc)
            if doc["state"] == "preparing":
                self._queue("prepare:" + identity, self._prepare, identity)
            elif doc["state"] == "failed":
                doc["state"] = "preparing"
                doc.pop("error", None)
                doc["revision"] += 1
                doc["updated_at"] = time.time()
                self._persist_pending(doc)
                self._queue("prepare:" + identity, self._prepare, identity)
        return self.public_run(identity)

    def _prepare(self, identity):
        with self._lock_for(identity):
            doc = self._get(identity)
            if doc["state"] != "preparing":
                return
            try:
                doc["items"] = self._build_items(doc)
                doc["state"] = "edit"
                doc.pop("error", None)
            except Exception as exc:
                doc["state"] = "failed"
                doc["error"] = str(exc)
            doc["revision"] += 1
            doc["updated_at"] = time.time()
            self.store.put_document(NS, identity, doc)
            if doc["state"] in ("edit", "failed"):
                self._track(identity, False)

    def _build_items(self, doc):
        from .notice_panel_data import build_items
        rows = build_items(self.runtime.service, doc["scope"], "all",
                           self.controller._get_ongoing(doc["scope"]))
        preferred = "update" if doc.get("slot") == "evening" else "start"
        return sorted(rows, key=lambda item: (bool(item.get("blocked")),
                                              item.get("action") != preferred,
                                              item.get("title") or "",
                                              item.get("key") or ""))

    # ---- read -------------------------------------------------------------
    def get_run(self, session, identity):
        session, open_id = self._require_user(session)
        doc = self._get(identity)
        self._check_owner(doc, open_id)
        self._authorize_run(doc, session)
        return self.public_run(identity)

    def sop_options(self, session, identity, item_key, q=""):
        """Lazy owner-authorized directory endpoint for a single persisted item."""
        from .notice_panel_data import SOP_WORK_TYPES, build_sop_options_field
        session, open_id = self._require_user(session)
        doc = self._get(identity)
        self._check_owner(doc, open_id)
        self._authorize_run(doc, session)
        if not isinstance(item_key, str) or not item_key.strip():
            raise PortalError("通告标识格式无效。")
        query = str(q or "").strip()
        if len(query) > 120:
            raise PortalError("检索内容过长。")
        item = next((it for it in doc.get("items") or [] if it.get("key") == item_key.strip()), None)
        if not item:
            raise PortalError("变更对象不存在。")
        draft = item.get("draft") or {}
        work_type = str(draft.get("work_type") or "").strip()
        action = str(item.get("action") or draft.get("action") or "").strip()
        if work_type not in SOP_WORK_TYPES or action != "start":
            raise PortalError("该通告无需工单配置。")
        scope = str(doc.get("scope") or draft.get("scope") or "").strip().upper()
        notice_sop = draft.get("notice_sop") if isinstance(draft.get("notice_sop"), dict) else {}
        selected_ids = [notice_sop.get("operator_record_id"), notice_sop.get("reviewer_record_id")]
        field = build_sop_options_field(
            self.runtime, self.runtime.service, scope, work_type, [scope],
            selected_ids=selected_ids, q=query)
        return {"scope": scope, "work_type": work_type, "field": field}

    # ---- actions ----------------------------------------------------------
    def run_action(self, session, identity, payload):
        session, open_id = self._require_user(session)
        with self._lock_for(identity):
            doc = self._get(identity)
            self._check_owner(doc, open_id)
            session = self._authorize_run(doc, session)
            try:
                revision = int(payload.get("revision") or -1)
            except (TypeError, ValueError):
                raise PortalError("版本号格式无效。")
            if revision != doc["revision"]:
                raise PanelVersionConflict(self.public_run(identity))
            action = payload.get("action")
            if action not in ACTIONS:
                raise PortalError("不支持的操作。")
            state = doc["state"]
            if action in ("save", "preview") and state != "edit":
                raise PortalError("仅编辑态可预览或保存。")
            if action == "confirm" and state != "confirm":
                raise PortalError("请先预览核对后再确认发送。")
            if action == "edit" and state != "confirm":
                raise PortalError("仅确认态可返回编辑。")
            if action == "retry" and (state != "done" or not any(
                    it.get("phase") == "failed" for it in doc.get("items") or [])):
                raise PortalError("没有可重试的失败操作。")
            if action == 'confirm_unmatched' and (state != 'done' or not any(it.get('convergence_confirmation') for it in doc.get('items', []))):
                raise PortalError('当前没有等待再次确认的未匹配通告。')
            if action == "refresh" and state in ("preparing", "running"):
                raise PortalError("运行或准备中的待办不能刷新。")
            if action != "retry":
                self._assert_current_date(doc)
            changes = payload.get("changes") or []
            if action in ("confirm", "edit", "refresh", "retry", "confirm_unmatched") and changes:
                raise PortalError("该操作不接受变更。")
            if action in ("save", "preview"):
                self._apply_changes(doc, changes)
            if action == "save":
                doc["state"] = "edit"
                doc.pop("error", None)
            elif action == "preview":
                self._preview(doc)
            elif action == "edit":
                self._edit_from_confirm(doc)
            elif action == "confirm":
                self._confirm(doc, session)
            elif action == "refresh":
                self._refresh(doc)
            elif action == "retry":
                self._retry(doc)
            elif action == 'confirm_unmatched':
                for item in doc.get('items', []):
                    if item.get('selected') and item.get('phase') == 'failed' and item.get('convergence_confirmation'):
                        item['payload']['plan_convergence_confirmation'] = item.pop('convergence_confirmation')
                        item.update(phase='queued', result='')
                doc['state'] = 'running'
                doc.pop('error', None)
            doc["revision"] += 1
            doc["updated_at"] = time.time()
            if doc["state"] == "running":
                self._persist_pending(doc)
            else:
                self.store.put_document(NS, identity, doc)
        if doc["state"] == "running":
            self._queue("advance:" + identity, self._advance, identity)
        return self.public_run(identity)

    def _apply_changes(self, doc, changes):
        from .notice_panel_data import _normalize_notice_sop_shape
        if not isinstance(changes, list):
            raise PortalError("变更列表格式无效。")
        by_key = {item["key"]: item for item in doc.get("items") or []}
        parsed = []
        seen = set()
        for change in changes:
            if not isinstance(change, dict):
                raise PortalError("变更条目格式无效。")
            unknown = set(change) - {"key", "selected", "edit_all", "draft"}
            if unknown:
                raise PortalError("变更条目含未知字段。")
            key = change.get("key")
            if not isinstance(key, str):
                raise PortalError("通告标识格式无效。")
            item = by_key.get(key)
            if not item:
                raise PortalError("变更对象不存在。")
            if key in seen:
                raise PortalError("变更列表中同一通告只能出现一次。")
            seen.add(key)
            has_sel = "selected" in change
            selected = change.get("selected") if has_sel else None
            if has_sel and type(selected) is not bool:
                raise PortalError("选择值格式无效。")
            has_all = "edit_all" in change
            edit_all = change.get("edit_all") if has_all else None
            if has_all and type(edit_all) is not bool:
                raise PortalError("展开选项格式无效。")
            raw_draft = change.get("draft") if "draft" in change else None
            if raw_draft is not None and not isinstance(raw_draft, dict):
                raise PortalError("字段填写格式无效。")
            if has_sel and selected and item.get("blocked"):
                raise PortalError("此通告当前不可选择。")
            if raw_draft:
                for fkey, value in raw_draft.items():
                    field = next((f for f in item.get("fields") if f["key"] == fkey), None)
                    if field is None:
                        raise PortalError(f"未知字段：{fkey}")
                    if field.get("readonly"):
                        continue
                    if fkey == "notice_sop":
                        _normalize_notice_sop_shape(value)
                        continue
                    if not isinstance(value, str):
                        raise PortalError("字段填写必须是文本。")
                    if len(value) > FIELD_VALUE_MAX:
                        raise PortalError("填写内容过长：" + str(field.get("label") or fkey))
                    if field["options"] and value and value not in field["options"]:
                        raise PortalError("选项无效：" + str(field.get("label") or fkey))
            parsed.append((item, has_sel, selected, has_all, edit_all, raw_draft))
        # Apply atomically; brief_fields are captured before filling.
        for item, has_sel, selected, has_all, edit_all, raw_draft in parsed:
            if has_sel:
                item["selected"] = selected
                item["dirty"] = True
                if selected and not item.get("brief_fields"):
                    item["brief_fields"] = brief_field_keys(item)
            if has_all:
                item["edit_all"] = edit_all
                item["dirty"] = True
            if raw_draft:
                for fkey, value in raw_draft.items():
                    field = next((f for f in item.get("fields") if f["key"] == fkey), None)
                    if field.get("readonly"):
                        continue
                    if fkey == "notice_sop":
                        item.setdefault("draft", {})[fkey] = _normalize_notice_sop_shape(value)
                        item["dirty"] = True
                        continue
                    item.setdefault("draft", {})[fkey] = str(value).strip()
                    item["dirty"] = True

    def _preview(self, doc):
        selected = [item for item in doc.get("items") or [] if item.get("selected")]
        if not selected:
            doc["state"] = "edit"
            doc["error"] = "请至少选择一条通告。"
            return
        errors = False
        try:
            from .notice_panel_data import build_items, submission
            current_rows = {row["key"]: row for row in build_items(
                self.runtime.service, doc["scope"], "all", self.controller._get_ongoing(doc["scope"]))}
        except Exception as exc:
            doc["state"] = "edit"
            doc["error"] = str(exc)
            return
        for item in doc.get("items") or []:
            if not item.get("selected"):
                continue
            try:
                latest = current_rows.get(item["key"])
                if not latest or latest.get("blocked") or latest.get("version") != item.get("version"):
                    raise PortalError("计划或原通告已变化，请重新预览后再核对；原填写保留。")
                body, preview = submission(self.runtime.service, item, self.runtime)
                if not isinstance(body, dict) or not isinstance(preview, str):
                    raise PortalError("通告预览生成失败。")
                item["payload"] = body
                item["preview"] = preview
                item["error"] = ""
            except Exception as exc:
                item["error"] = str(exc)
                errors = True
        doc["state"] = "confirm" if not errors else "edit"
        doc["error"] = "" if not errors else "所选通告有待补充或需核对内容，本次未发送。"

    def _edit_from_confirm(self, doc):
        doc["state"] = "edit"
        doc.pop("error", None)

    def _confirm(self, doc, session):
        selected = [item for item in doc.get("items") or [] if item.get("selected")]
        if not selected:
            raise PortalError("请至少选择一条通告。")
        try:
            from .notice_panel_data import build_items, submission
            current_rows = {row["key"]: row for row in build_items(
                self.runtime.service, doc["scope"], "all", self.controller._get_ongoing(doc["scope"]))}
        except Exception as exc:
            doc["state"] = "edit"
            doc["error"] = str(exc)
            return
        errors = False
        generation = int(doc.get("generation") or 1)
        for item in selected:
            latest = current_rows.get(item["key"])
            if not latest or latest.get("blocked") or latest.get("version") != item.get("version"):
                item["error"] = "计划或原通告已变化，请刷新核对；原填写保留。"
                errors = True
                continue
            try:
                body, preview = submission(self.runtime.service, item, self.runtime)
                if body != item.get("payload") or preview != item.get("preview"):
                    item["error"] = "通告内容已变化，请重新预览后再确认。"
                    errors = True
                    continue
                item["payload"] = body
                item["preview"] = preview
                item["error"] = ""
                item["operation_id"] = f"notice-panel:{doc['id']}:{generation}:{item['key']}"
                item["phase"] = "queued"
                item["result"] = ""
            except Exception as exc:
                item["error"] = str(exc)
                errors = True
        if errors:
            doc["state"] = "edit"
            doc["error"] = "所选通告有待补充或需核对内容，本次未发送。"
            return
        doc["state"] = "running"
        doc.pop("error", None)
        doc["confirmed_session"] = self._store_session(session)

    def _refresh(self, doc):
        if doc["state"] in ("preparing", "running"):
            raise PortalError("运行或准备中的待办不能刷新。")
        doc["generation"] = int(doc.get("generation") or 1) + 1
        doc.pop("confirmed_session", None)
        try:
            doc["items"] = self._build_items(doc)
            doc["state"] = "edit"
            doc.pop("error", None)
        except Exception as exc:
            doc["state"] = "failed"
            doc["error"] = str(exc)
        self._track(doc["id"], False)

    def _retry(self, doc):
        found = False
        for item in doc.get("items") or []:
            if item.get("phase") == "failed" and not item.get('convergence_confirmation'):
                item["phase"] = "queued"
                item["result"] = ""
                found = True
        if not found:
            raise PortalError("没有可重试的失败操作。")
        doc["state"] = "running"
        doc.pop("error", None)

    # ---- native sending / polling ----------------------------------------
    def _advance(self, identity):
        from .notice_actions import submit_notice_action
        with self._lock_for(identity):
            doc = self._get(identity)
            if doc["state"] != "running":
                return
            before = _fingerprint(doc)
            try:
                session = self._session_from_doc(doc)
            except Exception:
                session = None
            for item in doc.get("items") or []:
                if not item.get("selected"):
                    continue
                if item.get("phase") == "queued":
                    # A retry may already carry a native job id. Never re-submit when
                    # that job was superseded or no longer exists.
                    if item.get("job_id"):
                        old_job = self.runtime.service.get_job(item["job_id"]) or {}
                        if not old_job or old_job.get("superseded_by_job_id"):
                            item.update(phase="unavailable",
                                        result="原任务已失效或被后续提交替代，请在网页核验；不会重复新增。")
                            continue
                    if session is None:
                        item.update(phase="failed", result="操作人权限已失效，请重新登录后重试。")
                        continue
                    try:
                        body = {**item.get("payload", {}), "operation_id": item["operation_id"]}
                        response = asyncio.run(submit_notice_action(
                            self.controller, self.runtime, session, body, card_submission=True))
                        status = int(getattr(response, "status_code", 200) or 200)
                        try:
                            resp_body = json.loads(response.body)
                        except Exception:
                            resp_body = {}
                        resp_ok = bool(resp_body.get("ok"))
                        data = resp_body.get("data") if isinstance(resp_body.get("data"), dict) else {}
                        job_id = str(data.get("job_id") or "").strip()
                        initial_phase = str(data.get("initial_phase") or "").strip() or "accepted"
                        if not (200 <= status < 300 and resp_ok and job_id):
                            msg = str(resp_body.get("error") or data.get("error") or "提交失败，请重试。")
                            item.update(phase="failed", result=msg)
                            continue
                        item.update(job_id=job_id, phase=initial_phase, result="已提交，正在处理")
                    except Exception as exc:
                        item.update(phase="failed", result=str(exc))
                        details = getattr(exc, 'details', {})
                        if isinstance(details, dict) and details.get('kind') == 'plan_convergence_unmatched':
                            item['convergence_confirmation'] = details['confirmation']
                # Read the native job for accurate status (even when the initial
                # phase was already "failed" — we still want its real error text).
                if item.get("job_id") and item.get("phase") != "queued":
                    job = self.runtime.service.get_job(item["job_id"]) or {}
                    if not job or job.get("superseded_by_job_id"):
                        item.update(phase="unavailable",
                                    result="原任务已失效或被后续提交替代，请在网页核验；不会重复新增。")
                        continue
                    phase = job.get("phase") or "accepted"
                    if phase == "success":
                        result = "发送成功"
                    elif phase == "failed":
                        result = str(job.get("error") or "发送失败")
                    else:
                        result = "后台处理中"
                    warnings = [str(job[key]) for key in ("message_warning", "memory_warning") if job.get(key)]
                    item.update(phase=phase, result="；".join([result, *warnings]))
            if all(not item.get("selected") or item.get("phase") in TERMINAL_PHASES
                   for item in doc.get("items") or []):
                doc["state"] = "done"
            if _fingerprint(doc) != before:
                doc["revision"] += 1
                doc["updated_at"] = time.time()
                self.store.put_document(NS, identity, doc)
            if doc["state"] == "done":
                self._track(identity, False)

    def tick(self):
        if self.closed:
            return
        current = _edition_now()
        for identity in list((self.store.get_document(NS, "pending") or {}).get("ids", [])):
            doc = self.store.get_document(NS, identity)
            if not doc:
                continue
            state = doc.get("state")
            if state == "running":
                self._queue("advance:" + identity, self._advance, identity)
            elif state == "preparing":
                if current and doc.get("date") == current["date"]:
                    self._queue("prepare:" + identity, self._prepare, identity)
                else:
                    # Prior-day unsubmitted preparation must not dispatch.
                    # Never block the tick thread on a busy run lock; skip instead.
                    run_lock = self._lock_for(identity)
                    if not run_lock.acquire(blocking=False):
                        continue
                    try:
                        expired = self._get(identity)
                        if expired["state"] == "preparing":
                            expired["state"] = "failed"
                            expired["error"] = "待办已过期，请重新打开今日待办。"
                            expired["revision"] += 1
                            expired["updated_at"] = time.time()
                            self.store.put_document(NS, identity, expired)
                            self._track(identity, False)
                    finally:
                        run_lock.release()
            else:
                # Idle edit/failed/done should not stay in the resume index.
                # Re-read under the run lock to avoid untracking a fresh running doc;
                # skip cleanly if another thread is actively holding the lock.
                run_lock = self._lock_for(identity)
                if not run_lock.acquire(blocking=False):
                    continue
                try:
                    fresh = self._get(identity)
                    if fresh["state"] in ("edit", "failed", "done"):
                        self._track(identity, False)
                finally:
                    run_lock.release()

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.pool.shutdown(wait=False, cancel_futures=True)
