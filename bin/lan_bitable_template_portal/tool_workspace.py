"""Persistent tool workspace sessions and per-tool archive receipts."""

import datetime as dt
import io
import json
import re
import threading
import time
import uuid
from zipfile import ZIP_DEFLATED, ZipFile

from .risk_inspection import RiskError, digest, text
from .tool_self_check import TZ, dumps, timestamp


MAX_ASSETS = 200
ARCHIVE_RETRY_SECONDS = 60
ARCHIVE_LEASE_SECONDS = 600
EDIT_FIELDS = ("values", "checks", "outcome", "reason", "quantity", "actual")
ORDER_STATUS_FIELDS = """id,month,scope,kind,due,revision,owner,owner_name,submitted_at,outcome,
    report_name,actual,reason,resolution,scrap_qty,asset,supervisor,manager"""


class ToolWorkspaceMixin:
    def _init_tool_workspace(self):
        self._workspace_open_lock = threading.Lock()
        self._workspace_refresh_lock = threading.Lock()
        with self.connect(True) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS tool_workspace_templates(
                    cache_key TEXT PRIMARY KEY, content BLOB NOT NULL, layout TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS tool_workspace_sessions(
                    id TEXT PRIMARY KEY, owner TEXT NOT NULL, owner_name TEXT NOT NULL,
                    operation TEXT NOT NULL, fingerprint TEXT NOT NULL, kind TEXT NOT NULL,
                    status TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL, submitted_at TEXT,
                    submitted_revision INTEGER, report_name TEXT NOT NULL DEFAULT '',
                    report BLOB, package BLOB, file_token TEXT NOT NULL DEFAULT '',
                    lease TEXT NOT NULL DEFAULT '',
                    lease_until INTEGER NOT NULL DEFAULT 0, UNIQUE(owner,operation)
                );
                CREATE TABLE IF NOT EXISTS tool_workspace_items(
                    session_id TEXT NOT NULL, asset_id TEXT NOT NULL, position INTEGER NOT NULL,
                    asset TEXT NOT NULL, template_id TEXT NOT NULL, template_version TEXT NOT NULL,
                    template_key TEXT NOT NULL, previous_template TEXT NOT NULL,
                    draft TEXT NOT NULL, remote_id TEXT NOT NULL DEFAULT '',
                    report_name TEXT NOT NULL DEFAULT '', report BLOB, file_token TEXT NOT NULL DEFAULT '',
                    attempted_at REAL NOT NULL DEFAULT 0, next_try INTEGER NOT NULL DEFAULT 0,
                    archive_error TEXT NOT NULL DEFAULT '', PRIMARY KEY(session_id,asset_id)
                );
                CREATE INDEX IF NOT EXISTS tool_workspace_due
                    ON tool_workspace_sessions(status,lease_until);
            """)
            db.execute("BEGIN IMMEDIATE")
            columns = {row["name"] for row in db.execute("PRAGMA table_info(tool_workspace_items)")}
            for name in ("order_id", "binding_error"):
                if name not in columns:
                    db.execute(f"ALTER TABLE tool_workspace_items ADD COLUMN {name} TEXT NOT NULL DEFAULT ''")
            db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS tool_workspace_order
                ON tool_workspace_items(order_id) WHERE order_id<>''""")

    @staticmethod
    def _workspace_for_order(db, identity):
        return db.execute("SELECT * FROM tool_workspace_items WHERE order_id=?", (identity,)).fetchone()

    def _workspace_access(self, actor):
        self.access(actor)
        if not self.available_to(actor):
            raise RiskError("当前账号没有工具管理权限。", 403)

    @staticmethod
    def _workspace_kind(asset):
        from .tool_management import configured_kind

        if asset.get("retired") or (asset.get("quantity") or 0) <= 0:
            return "", "工具已停用、报废或数量无效。"
        return configured_kind(asset)

    def _workspace_allowed(self, db, actor, asset, kind):
        from .tool_management import can_scope

        scope = asset.get("scope")
        return can_scope(actor, scope) and (
            self.administrator(actor) or self.eligible(db, actor, {
                "scope": scope, "kind": kind, "supervisor": asset.get("supervisor", ""),
            })
        )

    @staticmethod
    def _workspace_asset(asset):
        return {key: asset.get(key) or "" for key in (
            "id", "name", "scope", "model", "brand", "location", "template_id", "self_cycle", "external_cycle", "quantity",
        )}

    def _workspace_visible_items(self, db, actor, session, items):
        from .tool_management import can_scope

        if any(item["order_id"] for item in items):
            authorized = []
            for item in items:
                order = self._workspace_order(db, item, status_only=True)
                if order and self.visible(db, actor, order):
                    authorized.append(item)
            return authorized
        if (session["owner"] == actor["id"] or self.administrator(actor)) and all(
                can_scope(actor, json.loads(item["asset"])["scope"]) for item in items):
            return items
        return []

    @staticmethod
    def _workspace_order(db, item, *, status_only=False):
        fields = ORDER_STATUS_FIELDS if status_only else "*"
        return db.execute(f"SELECT {fields} FROM tool_work_orders WHERE id=?",
                          (item["order_id"],)).fetchone() if item["order_id"] else None

    @staticmethod
    def _workspace_rows(db, identity, *, status_only=False):
        fields = "asset_id,asset,template_key,order_id,binding_error,remote_id,attempted_at,archive_error" if status_only else "*"
        return db.execute(
            f"SELECT {fields} FROM tool_workspace_items WHERE session_id=? ORDER BY position", (identity,),
        ).fetchall()

    def _workspace_session(self, db, actor, identity, *, status_only=False):
        fields = "id,revision,kind,status,owner,owner_name,created_at,updated_at,submitted_at,report_name" if status_only else "*"
        row = db.execute(f"SELECT {fields} FROM tool_workspace_sessions WHERE id=?", (identity,)).fetchone()
        items = self._workspace_visible_items(db, actor, row, self._workspace_rows(db, identity, status_only=status_only)) if row else []
        if not items:
            raise RiskError("工具办理会话不存在或无权查看。", 404)
        return row, items

    @staticmethod
    def _workspace_summary(row, items):
        return {key: row[key] for key in (
            "id", "revision", "kind", "status", "owner_name", "created_at", "updated_at", "submitted_at", "report_name",
        )} | {"can_edit": row["status"] == "draft", "item_count": len(items),
              "archived_count": sum(bool(item["remote_id"]) for item in items),
              "archive_error": next((item["archive_error"] for item in items if item["archive_error"]), "")}

    @staticmethod
    def _workspace_legacy_overlaps(db, asset_id, kind, month):
        return db.execute("""SELECT session.id,session.status,session.submitted_at,item.remote_id,item.asset
            FROM tool_workspace_items item JOIN tool_workspace_sessions session ON session.id=item.session_id
            WHERE item.asset_id=? AND item.order_id='' AND session.kind=?
              AND (session.status='pending' OR
                   (session.status='archived' AND substr(session.submitted_at,1,7)=?))
            ORDER BY session.submitted_at,session.id""", (asset_id, kind, month)).fetchall()

    def _workspace_order_plan(self, db, actor, asset, kind):
        from .tool_management import clock, full_access

        today = clock().date()
        month = today.strftime('%Y-%m')
        order = db.execute("""SELECT * FROM tool_work_orders WHERE asset_id=? AND kind=?
            AND (month=? OR (month<? AND submitted_at IS NULL AND outcome=''))
            ORDER BY month=? DESC, month ASC, id LIMIT 1""", (asset["id"], kind, month, month, month)).fetchone()
        if order and not self.visible(db, actor, order):
            return {"order_id": "", "workspace_id": "", "mode": "blocked", "problem": "当前账号无权办理原工单。"}
        session = self._workspace_for_order(db, order["id"]) if order else None
        result = {"order_id": order["id"] if order else "", "order_month": order["month"] if order else month,
                  "workspace_id": session["session_id"] if session else "", "mode": "existing" if order else "temporary"}
        overlaps = self._workspace_legacy_overlaps(db, asset["id"], kind, order["month"] if order else month)
        if overlaps and not (order and order["submitted_at"]):
            result.update(mode="blocked", problem="有旧批次已提交或在同步，请先核对原台账回执。")
            if order and not order["submitted_at"] and self.administrator(actor):
                result["legacy_receipts"] = [{"session_id": entry["id"], "date": (entry["submitted_at"] or "")[:10],
                                               "remote_id": entry["remote_id"],
                                               "can_reconcile": len(overlaps) == 1 and entry["status"] == "archived"
                                               and bool(entry["remote_id"])} for entry in overlaps
                                              if json.loads(entry["asset"])["scope"] == asset["scope"]]
        elif order and order["scope"] != asset["scope"]:
            result.update(mode="blocked", problem="原工单楼栋与当前工具归属不同，请核对。")
        elif order and order["outcome"] == "cancelled":
            result.update(mode="blocked", problem="工单已取消，请通过月度清单恢复。")
        elif session:
            result.update(mode="resume", problem="已完成" if order["submitted_at"] else "")
        elif order and order["owner"] not in ("", actor["id"]) and not full_access(actor):
            result.update(mode="original", problem="已由其他人员领取")
        elif order and (order["submitted_at"]
                        or order["records"] != "[]" or order["report_name"]
                        or db.execute("SELECT 1 FROM tool_work_reviews WHERE order_id=? AND status='pending'", (order["id"],)).fetchone()):
            result.update(mode="original")
        elif not self.eligible(db, actor, order or {"scope": asset["scope"], "kind": kind,
                                                    "supervisor": asset.get("supervisor", "")}):
            result.update(mode="blocked", problem="仅本楼楼长或获授权人员可办理。")
        elif not self.tool_available(db, asset):
            result.update(mode="blocked", problem="工具在检修、报废审批中或已全部报废。")
        else:
            planned_due = self.planned_kinds(db, asset, today).get(kind) if not order else None
            if planned_due:
                result.update(mode="planned")
            result["due"] = order["due"] if order else planned_due or today.isoformat()
        return result

    def workspace_reconcile(self, actor, identity, body):
        self._workspace_access(actor)
        if not self.administrator(actor) or actor.get("_tool_impersonation"):
            raise RiskError("仅管理员本人可核验旧台账回执。", 403)
        asset_id = text(body.get("asset_id"), 120, True)
        order_id = text(body.get("order_id"), 120, True)

        def candidate(db):
            order = self._work(db, actor, order_id)
            if order["asset_id"] != asset_id:
                raise RiskError("原工单与工具标识不一致。", 409)
            if order["submitted_at"] and db.execute("""SELECT 1 FROM tool_work_sync
                WHERE order_id=? AND remote_id IN (SELECT remote_id FROM tool_workspace_items
                    WHERE session_id=? AND asset_id=? AND order_id='') AND synced=revision""",
                    (order_id, identity, asset_id)).fetchone():
                return order, None, None
            overlaps = self._workspace_legacy_overlaps(db, asset_id, order["kind"], order["month"])
            if (len(overlaps) != 1 or overlaps[0]["id"] != identity or order["outcome"] or order["submitted_at"]
                    or order["records"] != "[]" or order["report"] or self._workspace_for_order(db, order_id)
                    or db.execute("SELECT 1 FROM tool_work_reviews WHERE order_id=?", (order_id,)).fetchone()):
                raise RiskError("原工单已有业务数据或旧批次归属不唯一，请人工核对。", 409)
            row, items = self._workspace_session(db, actor, identity)
            item = next((entry for entry in items if entry["asset_id"] == asset_id), None)
            if (not item or row["status"] != "archived" or not item["remote_id"]
                    or json.loads(item["asset"])["scope"] != order["scope"]):
                raise RiskError("旧批次仍在补传或回执未确认。", 409)
            return order, row, item

        with self.connect() as db:
            order, row, item = candidate(db)
        if item is None:
            return {"order_id": order_id}
        draft = json.loads(item["draft"])
        external = row["kind"] == "external"
        if not external and draft.get("qualified") is not True:
            raise RiskError("旧表只标记了不合格，请人工核对最终处置，不能自动归为正常。", 409)
        report = row if external else item
        actual = row["submitted_at"][:10] if external else draft["reviewed_at"][:10]
        payload = json.loads(item["asset"]) | {"asset_id": asset_id, "kind": row["kind"],
            "qualified": draft.get("qualified"), "due": row["created_at"][:10]}
        verified = self.cloud.write_workspace_item(identity, payload, report["report_name"], report["report"], actual,
            file_token=report["file_token"], remote_id=item["remote_id"])
        if verified["remote_id"] != item["remote_id"]:
            raise RiskError("飞书回读与原回执不一致。", 409)
        with self.connect(True) as db:
            current, legacy, expected = candidate(db)
            if expected is None:
                return {"order_id": order_id}
            if current["revision"] != order["revision"] or legacy != row or expected != item:
                raise RiskError("核验期间工单已变化，请刷新后重试。", 409)
            dest = "inspection" if external else "maintenance"
            db.execute("""UPDATE tool_work_orders SET submitted_at=?,actual=?,outcome=?,owner=?,owner_name=?,
                report_name=?,report=?,revision=revision+1 WHERE id=?""",
                (row["submitted_at"], actual, "" if external else "normal", row["owner"], row["owner_name"],
                 report["report_name"], report["report"], order_id))
            self.enqueue(db, order_id, dest)
            db.execute("""UPDATE tool_work_sync SET synced=revision,remote_id=?,file_token=?,error='',next_try=0
                WHERE order_id=? AND dest=?""", (item["remote_id"], verified["file_token"], order_id, dest))
            db.execute("UPDATE tool_work_archives SET needs_refresh=1 WHERE month=?", (order["month"],))
            self.audit(db, order_id, actor, "核验并接续旧工作台归档", f"{identity} / {item['remote_id']}")
        return {"order_id": order_id}

    @staticmethod
    def _workspace_template_list(templates):
        return [{
            "id": row["id"], "name": row["name"], "label": row["name"],
            "version": row.get("digest") or digest(row.get("file_token") or row["id"]),
        } for row in templates]

    def workspace_list(self, actor):
        from .tool_management import can_scope

        self._workspace_access(actor)
        warning = ""
        try:
            templates = self._workspace_template_list(self.cloud.templates())
        except Exception:
            warning = "飞书模板暂时无法读取，已有办理记录仍可打开。"
            with self.connect() as db:
                templates = json.loads(self.state(db, "workspace_templates_last_good") or "[]")
        else:
            with self.connect(True) as db:
                self.set_state(db, "workspace_templates_last_good", dumps(templates))
        with self.connect() as db:
            assets = []
            for row in db.execute("SELECT * FROM tool_inventory WHERE active=1"):
                asset = self.effective(row)
                if not can_scope(actor, asset.get("scope")):
                    continue
                kind, reason = self._workspace_kind(asset)
                plan = self._workspace_order_plan(db, actor, asset, kind) if kind and not reason else {}
                reason = reason or plan.get("problem", "")
                assets.append(self._workspace_asset(asset) | {
                    "kind": kind, "eligible_kinds": [kind] if kind and plan.get("mode") in {"existing","planned","temporary"} else [],
                    "selectable": bool(kind and plan.get("mode") in {"existing","planned","temporary"}),
                    "disabled_reason": reason, "plan": plan,
                })
            sessions = []
            for row in db.execute("""
                SELECT * FROM tool_workspace_sessions
                WHERE (owner=? OR ? OR EXISTS(SELECT 1 FROM tool_workspace_items
                    WHERE session_id=tool_workspace_sessions.id AND order_id<>'')) ORDER BY updated_at DESC,id
            """, (actor["id"], self.administrator(actor))):
                items = self._workspace_visible_items(db, actor, row, self._workspace_rows(db, row["id"]))
                if items:
                    summary = self._workspace_detail(db, row, items, actor, status_only=True)
                    summary.pop("items")
                    sessions.append(summary)
        assets.sort(key=lambda item: (item["scope"], item["name"], item["id"]))
        return {"assets": assets, "templates": templates, "sessions": sessions, "warning": warning}

    def workspace_refresh(self, actor):
        self._workspace_access(actor)
        with self._workspace_refresh_lock:
            self.refresh()
        return self.workspace_list(actor)

    def workspace_get(self, actor, identity):
        self._workspace_access(actor)
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity)
            return self._workspace_detail(db, row, items, actor)

    def workspace_status(self, actor, identity):
        self._workspace_access(actor)
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity, status_only=True)
            return self._workspace_detail(db, row, items, actor, status_only=True)

    def workspace_resume(self, actor, identity, body=None):
        self._workspace_access(actor)
        allow_temporary = (body or {}).get("allow_temporary") is True
        with self._workspace_open_lock:
            with self.connect(True) as db:
                row, items = self._workspace_session(db, actor, identity)
                if row["status"] in {"draft","opening"} and all(not item["order_id"] for item in items):
                    if row["owner"] != actor["id"]:
                        raise RiskError("请由旧草稿的填报人接续原表。", 403)
                    assets = self._workspace_assets_for_open(db, actor, [item["asset_id"] for item in items], row["kind"])
                    self._workspace_check_scopes(db, items)
                    for item, asset in zip(items, assets):
                        order_id = self._workspace_claim_order(db, actor, asset, row["kind"], allow_temporary)
                        draft = json.loads(item["draft"])
                        draft["legacy_review"] = {key: draft.get(key) for key in ("reviewer", "reviewed_at", "signature", "qualified")}
                        draft.update(reviewer="", reviewed_at="", signature="", qualified=None, outcome="")
                        db.execute("UPDATE tool_workspace_items SET order_id=?,draft=? WHERE session_id=? AND asset_id=?",
                                   (order_id, dumps(draft), identity, item["asset_id"]))
                    self._workspace_bump(db, identity)
            with self.connect() as db:
                row, items = self._workspace_session(db, actor, identity)
                if row['status'] != 'opening':
                    return self._workspace_detail(db, row, items, actor)
                if row["owner"] != actor["id"]:
                    return self._workspace_detail(db, row, items, actor)
                self._workspace_assets_for_open(db, actor, [item['asset_id'] for item in items], row['kind'])
                self._workspace_check_scopes(db, items)
            self._workspace_apply_bindings(actor, identity)
            with self.connect(True) as db:
                self._workspace_finish_open(db, identity)
                return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    def _workspace_detail(self, db, row, items, actor, *, status_only=False):
        layouts = {}
        result = []
        for item in items:
            key = item["template_key"]
            if key and key not in layouts and not status_only:
                layouts[key] = json.loads(db.execute(
                    "SELECT layout FROM tool_workspace_templates WHERE cache_key=?", (key,),
                ).fetchone()[0])
            order = self._workspace_order(db, item, status_only=True)
            info = self.work_info(db, actor, order) if order else {}
            sync = info.get("sync", [])
            archive_status = (
                "archived" if item["remote_id"] else "verifying" if item["attempted_at"]
                else "retrying" if item["archive_error"] else row["status"]
            )
            archive_error = item["archive_error"]
            if order:
                archive_error = next((job["error"] for job in sync if job["error"]), "")
                archive_status = ("verifying" if any(job.get("verifying") for job in sync if job["state"] != "done")
                                  else "retrying" if archive_error else "pending") if order["submitted_at"] else info["status"]
                if sync and all(job["state"] == "done" for job in sync):
                    archive_status = "archived"
            status = {"id": item["asset_id"], "order_id": item["order_id"],
                      "order_revision": info.get("revision"), "order_status": info.get("status"),
                      "binding_error": item["binding_error"], "archive_status": archive_status, "archive_error": archive_error}
            if status_only:
                result.append(status)
                continue
            result.append(status | {
                "id": item["asset_id"], "asset_id": item["asset_id"], "asset": json.loads(item["asset"]),
                "template_id": item["template_id"], "template_version": item["template_version"],
                "template": layouts.get(key), **{name: value for name, value in json.loads(item["draft"]).items()
                                                  if name != "legacy_review"},
                "can_edit": bool(info.get("can_edit")) and not item["binding_error"] and row["status"] != "opening",
                "can_review": bool(info.get("can_review")), "review_note": info.get("review_note"),
                "remote_id": item["remote_id"],
            })
        summary = self._workspace_summary(row, items)
        if row["kind"] == "external" and not status_only:
            summary["actual"] = next((info["actual"] for item in items
                                      if (info := self._workspace_order(db, item, status_only=True)) and info["actual"]), "")
        if any(item["order_id"] for item in items) and row["status"] != "opening":
            states = {item["archive_status"] for item in result}
            summary.update(status="archived" if "archived" in states and states <= {"archived", "cancelled"} else "draft" if "pending" in {
                item["order_status"] for item in result
            } else "review_pending" if "review_pending" in states else "cancelled" if states == {"cancelled"} else "pending",
                archived_count=sum(item["archive_status"] == "archived" for item in result),
                can_edit=any(item.get("can_edit") for item in result))
        elif not status_only:
            summary["can_edit"] = False
            summary["legacy_draft"] = row["status"] in {"draft", "opening"} and row["owner"] == actor["id"]
        return summary | {"items": result}

    def _workspace_assets_for_open(self, db, actor, asset_ids, kind):
        assets = []
        for asset_id in asset_ids:
            row = db.execute("SELECT * FROM tool_inventory WHERE id=? AND active=1", (asset_id,)).fetchone()
            if not row:
                raise RiskError("工具不存在或已停用，请刷新台账。", 404)
            asset = self.effective(row)
            if not self._workspace_allowed(db, actor, asset, kind):
                raise RiskError("当前账号无权办理所选工具。", 403)
            actual_kind, reason = self._workspace_kind(asset)
            if actual_kind != kind:
                raise RiskError(reason or "所选工具的周期与办理类型不一致。", 409)
            assets.append(asset)
        return assets

    def _workspace_snapshot(self, template):
        from .tool_workspace_workbook import describe

        cache_key = digest([template["id"], template.get("file_token"), template.get("digest")])
        with self.connect() as db:
            row = db.execute("SELECT * FROM tool_workspace_templates WHERE cache_key=?", (cache_key,)).fetchone()
        if row:
            return cache_key, row["content"], json.loads(row["layout"])
        content = self.cloud.download_template(template)
        layout = describe(content)
        with self.connect(True) as db:
            db.execute("INSERT OR IGNORE INTO tool_workspace_templates VALUES(?,?,?)", (cache_key, content, dumps(layout)))
        return cache_key, content, layout

    def workspace_open(self, actor, body):
        self._workspace_access(actor)
        kind = body.get("kind")
        raw_ids = body.get("asset_ids")
        if kind not in ("self", "external") or not isinstance(raw_ids, list) or not 1 <= len(raw_ids) <= MAX_ASSETS:
            raise RiskError("请选择办理类型及 1 至 200 件工具。")
        asset_ids = [text(value, 120, True) for value in raw_ids]
        if len(set(asset_ids)) != len(asset_ids):
            raise RiskError("同一工具只能选择一次。")
        template_id = text(body.get("template_id") or "", 120)
        if kind == "external" and template_id:
            raise RiskError("送检直接上传报告。")
        operation = text(body.get("operation_id"), 128, True)
        allow_temporary = body.get("allow_temporary") is True
        fingerprint = digest([kind, sorted(asset_ids), template_id, allow_temporary])
        with self._workspace_open_lock:
            with self.connect() as db:
                existing = db.execute(
                    "SELECT * FROM tool_workspace_sessions WHERE owner=? AND operation=?", (actor["id"], operation),
                ).fetchone()
                if existing:
                    legacy_fingerprint = digest([kind, sorted(asset_ids), template_id])
                    if existing["fingerprint"] not in (fingerprint, legacy_fingerprint):
                        raise RiskError("同一操作编号已用于其他工具或模板。", 409)
                    row, items = self._workspace_session(db, actor, existing["id"])
                    if row["status"] != "opening":
                        return self._workspace_detail(db, row, items, actor)
                    if not any(item["order_id"] for item in items):
                        return self._workspace_detail(db, row, items, actor)
                assets = self._workspace_assets_for_open(db, actor, asset_ids, kind)
                if not existing:
                    plans = [self._workspace_order_plan(db, actor, asset, kind) for asset in assets]
                    if len(plans) == 1 and plans[0]["mode"] == "resume":
                        return self._workspace_detail(db, *self._workspace_session(db, actor, plans[0]["workspace_id"]), actor)
            if existing:
                identity = existing["id"]
            else:
                identity = self._workspace_create_session(actor, operation, fingerprint, kind, assets, template_id, allow_temporary)
            if kind == "self":
                self._workspace_apply_bindings(actor, identity)
            with self.connect(True) as db:
                row, items = self._workspace_session(db, actor, identity)
                self._workspace_finish_open(db, identity)
                return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    def _workspace_create_session(self, actor, operation, fingerprint, kind, assets, template_id, allow_temporary):
        templates = {row["id"]: row for row in self.cloud.templates()} if kind == "self" else {}
        identity = uuid.uuid4().hex
        snapshots = {}
        rows = []
        for index, asset in enumerate(assets):
            selected, version, cache_key = "", "", ""
            values = {}
            if kind == "self":
                from .tool_workspace_workbook import prefill

                selected = template_id or asset["template_id"]
                if selected not in templates:
                    raise RiskError(f"「{asset['name']}」缺少可用的关联模板，请选择模板。", 409)
                if selected not in snapshots:
                    snapshots[selected] = self._workspace_snapshot(templates[selected])
                cache_key, content, layout = snapshots[selected]
                version = layout["template_sha256"]
                values = prefill(content, asset)
            draft = {"values": values, "checks": {}, "reviewer": "", "reviewed_at": "",
                     "signature": "", "qualified": None}
            rows.append((identity, asset["id"], index, dumps(asset | {"template_id": selected}),
                         selected, version, cache_key, asset["template_id"], dumps(draft)))
        now = dt.datetime.now(TZ).isoformat(timespec="seconds")
        with self.connect(True) as db:
            prior = db.execute("SELECT id,fingerprint FROM tool_workspace_sessions WHERE owner=? AND operation=?",
                               (actor["id"], operation)).fetchone()
            if prior:
                if prior["fingerprint"] != fingerprint:
                    raise RiskError("同一操作编号已用于其他工具或模板。", 409)
                return prior["id"]
            current = self._workspace_assets_for_open(db, actor, [asset["id"] for asset in assets], kind)
            if current != assets:
                raise RiskError("工具台账已变化，请刷新后重试。", 409)
            order_ids = [self._workspace_claim_order(db, actor, asset, kind, allow_temporary) for asset in current]
            db.execute("""INSERT INTO tool_workspace_sessions
                (id,owner,owner_name,operation,fingerprint,kind,status,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)""",
                       (identity, actor["id"], actor["name"], operation, fingerprint, kind, "opening", now, now))
            db.executemany("""INSERT INTO tool_workspace_items
                (session_id,asset_id,position,asset,template_id,template_version,template_key,previous_template,draft,order_id)
                VALUES(?,?,?,?,?,?,?,?,?,?)""", [(*row, order_id) for row, order_id in zip(rows, order_ids)])
        return identity

    def _workspace_claim_order(self, db, actor, asset, kind, allow_temporary):
        plan = self._workspace_order_plan(db, actor, asset, kind)
        if plan["mode"] not in {"existing","planned","temporary"}:
            raise RiskError(f"「{asset['name']}」{plan.get('problem') or '已有办理记录，请打开原工单。'}", 409)
        order_id = plan["order_id"]
        if not order_id:
            if plan["mode"] == "temporary" and not allow_temporary:
                raise RiskError("请确认新增临时工单。", 409)
            order_id = uuid.uuid4().hex
            self.insert_work_order(db, order_id, plan["order_month"], asset, kind, plan["due"])
            self.audit(db, order_id, actor, "新增临时工单" if plan["mode"] == "temporary" else "生成计划工单")
            db.execute("UPDATE tool_work_archives SET needs_refresh=1 WHERE month=?", (plan["order_month"],))
        db.execute("UPDATE tool_work_orders SET owner=?,owner_name=?,revision=revision+1 WHERE id=? AND owner=''",
                   (actor["id"], actor["name"], order_id))
        self.audit(db, order_id, actor, "接续原表办理")
        return order_id

    @staticmethod
    def _workspace_finish_open(db, identity):
        if not db.execute("SELECT 1 FROM tool_workspace_items WHERE session_id=? AND binding_error<>''", (identity,)).fetchone():
            db.execute("UPDATE tool_workspace_sessions SET status='draft' WHERE id=? AND status='opening'", (identity,))

    def _workspace_apply_bindings(self, actor, identity):
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity)
            if row["status"] != "opening":
                return
            if row["owner"] != actor["id"]:
                raise RiskError("仅本批的领取人可核对模板关联。", 403)
            self._workspace_assets_for_open(db, actor, [item["asset_id"] for item in items], row["kind"])
            self._workspace_check_scopes(db, items)
        for item in items:
            template_id = item['template_id']
            previous = item["previous_template"]
            if not template_id:
                continue
            asset_id = item["asset_id"]
            try:
                if self.cloud.bind_template(asset_id, template_id, previous) != template_id:
                    raise RiskError("飞书未确认模板关联，请重试原操作。", 503)
            except Exception as exc:
                message = str(exc) if isinstance(exc, RiskError) else "飞书关联结果暂未确认，请重试。"
                with self.connect(True) as db:
                    db.execute("UPDATE tool_workspace_items SET binding_error=? WHERE session_id=? AND asset_id=?",
                               (message, identity, asset_id))
                if not item["order_id"]:
                    raise
                continue
            with self.connect(True) as db:
                db.execute("UPDATE tool_workspace_items SET binding_error='' WHERE session_id=? AND asset_id=?", (identity, asset_id))
                current = db.execute("SELECT source FROM tool_inventory WHERE id=? AND active=1", (asset_id,)).fetchone()
                if current:
                    source = json.loads(current["source"])
                    if (source.get("template_id") or "") == previous:
                        source["template_id"] = template_id
                        db.execute("UPDATE tool_inventory SET source=? WHERE id=?", (dumps(source), asset_id))

    def workspace_resolve_binding(self, actor, identity, body):
        self._workspace_access(actor)
        asset_id = text(body.get("asset_id"), 120, True)
        template_id = text(body.get("template_id") or "", 120)
        with self._workspace_open_lock:
            with self.connect() as db:
                row, items = self._workspace_session(db, actor, identity)
                item = next((entry for entry in items if entry["asset_id"] == asset_id), None)
                if row["status"] != "opening" or not item or not item["binding_error"] or row["owner"] != actor["id"]:
                    raise RiskError("此模板关联不可调整。", 409)
                self.check_revision(row, body.get("revision"))
                asset = self._workspace_assets_for_open(db, actor, [asset_id], row["kind"])[0]
            if template_id:
                template = next((entry for entry in self.cloud.templates() if entry["id"] == template_id), None)
                if not template:
                    raise RiskError("模板已停用或不存在。", 409)
                key, content, layout = self._workspace_snapshot(template)
                from .tool_workspace_workbook import prefill

                draft = json.loads(item["draft"])
                draft["values"] = prefill(content, asset)
            with self.connect(True) as db:
                current, _ = self._workspace_session(db, actor, identity)
                self.check_revision(current, body.get("revision"))
                if template_id:
                    db.execute("""UPDATE tool_workspace_items SET template_id=?,template_key=?,template_version=?,
                        previous_template=?,binding_error='',draft=? WHERE session_id=? AND asset_id=?""",
                        (template_id, key, layout["template_sha256"], asset["template_id"], dumps(draft), identity, asset_id))
                else:
                    db.execute("DELETE FROM tool_workspace_items WHERE session_id=? AND asset_id=?", (identity, asset_id))
                    order = self._workspace_order(db, item)
                    if order and order["owner"] == actor["id"] and not order["submitted_at"]:
                        db.execute("UPDATE tool_work_orders SET owner='',owner_name='',revision=revision+1 WHERE id=?", (order["id"],))
                        self.audit(db, order["id"], actor, "移出原表办理批次")
                    if not self._workspace_rows(db, identity):
                        db.execute("DELETE FROM tool_workspace_sessions WHERE id=?", (identity,))
                        return {"removed": True}
                self._workspace_bump(db, identity)
            self._workspace_apply_bindings(actor, identity)
            with self.connect(True) as db:
                self._workspace_finish_open(db, identity)
                return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    @staticmethod
    def _workspace_draft(row, revision):
        if type(revision) is not int or revision != row["revision"]:
            raise RiskError("办理会话已更新，请刷新后核对再保存。", 409)
        if row["status"] != "draft":
            raise RiskError("此批次已提交，表格与报告已锁定。", 409)

    def workspace_save(self, actor, identity, body):
        from .tool_workspace_workbook import validate_inputs

        self._workspace_access(actor)
        patches = body.get("items")
        if not isinstance(patches, list) or not patches or len(patches) > MAX_ASSETS:
            raise RiskError("没有可保存的工具表格。")
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity)
            self._workspace_draft(row, body.get("revision"))
            if row["kind"] != "self":
                raise RiskError("送检批次请使用上传报告。")
            by_id = {item["asset_id"]: item for item in items}
            updates = {}
            contents = {}
            for patch in patches:
                if not isinstance(patch, dict):
                    raise RiskError("工具填写内容格式无效。")
                asset_id = text(patch.get("asset_id") or patch.get("id"), 120, True)
                if asset_id not in by_id or asset_id in updates or patch.get("id", asset_id) != asset_id:
                    raise RiskError("工具填写记录不存在、重复或标识冲突。")
                item = by_id[asset_id]
                order = self._workspace_order(db, item)
                if not order or not self.work_info(db, actor, order)["can_edit"]:
                    raise RiskError("仅已领取且待填写的工单可保存。", 403)
                draft = json.loads(item["draft"])
                draft.update({name: patch[name] for name in EDIT_FIELDS if name in patch})
                draft["reason"] = text(draft.get("reason") or "", 1000)
                key = item["template_key"]
                if key not in contents:
                    contents[key] = db.execute(
                        "SELECT content FROM tool_workspace_templates WHERE cache_key=?", (key,),
                    ).fetchone()[0]
                validate_inputs(contents[key], draft["values"], draft["checks"], expected_sha256=item["template_version"])
                updates[asset_id] = dumps(draft)
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            self._workspace_draft(row, body.get("revision"))
            for item in items:
                if item["asset_id"] in updates:
                    order = self._workspace_order(db, item)
                    if not order or not self.work_info(db, actor, order)["can_edit"]:
                        raise RiskError("原工单已变化，请刷新。", 409)
            db.executemany("UPDATE tool_workspace_items SET draft=? WHERE session_id=? AND asset_id=?",
                           [(draft, identity, asset_id) for asset_id, draft in updates.items()])
            self._workspace_bump(db, identity)
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    @staticmethod
    def _workspace_bump(db, identity):
        now = dt.datetime.now(TZ).isoformat(timespec="seconds")
        db.execute("UPDATE tool_workspace_sessions SET revision=revision+1,updated_at=? WHERE id=?", (now, identity))

    def workspace_report(self, actor, identity, revision, name, data):
        from .tool_management import report_file

        self._workspace_access(actor)
        name = report_file(name, data)
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            if row["kind"] != "external":
                raise RiskError("维护报告由各工具的自检表生成。")
            self._workspace_draft(row, revision)
            orders = [self._workspace_order(db, item) for item in items]
            editable = [order for order in orders if order and not order["submitted_at"] and not order["outcome"]]
            if (row["owner"] != actor["id"] or not editable
                    or any(not self.work_info(db, actor, order)["can_edit"] for order in editable)):
                raise RiskError("仅本批填报人可上传送检报告。", 403)
            if any((order := self._workspace_order(db, item)) and order["submitted_at"]
                   for item in self._workspace_rows(db, identity)):
                raise RiskError("本批已有送检报告归档，不能更换原附件。", 409)
            if row["report_name"] != name or row["report"] != data:
                db.execute("UPDATE tool_workspace_sessions SET report_name=?,report=? WHERE id=?", (name, data, identity))
                self._workspace_bump(db, identity)
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    def workspace_submit(self, actor, identity, body):
        self._workspace_access(actor)
        from .tool_management import clock, date_value
        from .tool_workspace_workbook import validate_inputs

        revision = body.get("revision")
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            if type(revision) is int and revision == row["submitted_revision"]:
                if row["kind"] == "external" and any(
                    order["actual"] != body.get("actual") for item in items
                    if (order := self._workspace_order(db, item)) and order["submitted_at"]
                ):
                    raise RiskError("原提交已使用其他检测日期，请刷新核对。", 409)
                return self._workspace_detail(db, row, items, actor)
            self._workspace_draft(row, revision)
            now = clock()
            if row["kind"] == "external" and row["owner"] != actor["id"]:
                if not all(self.workspace_can_review(db, actor, order) for item in items
                           if (order := self._workspace_order(db, item))):
                    raise RiskError("仅本批填报人或获授权人员可提交报告。", 403)
            count = 0
            for item in items:
                order = self._workspace_order(db, item)
                if not order or not self.work_info(db, actor, order)["can_edit"]:
                    continue
                self._workspace_check_scopes(db, [item])
                asset = self._workspace_assets_for_open(db, actor, [item["asset_id"]], row["kind"])[0]
                if self._workspace_legacy_overlaps(db, item["asset_id"], row["kind"], order["month"]):
                    raise RiskError("旧工作台批次可能已归档，请先核对原台账回执。", 409)
                count += 1
                draft = json.loads(item["draft"])
                if row["kind"] == "external":
                    if not row["report"] and not order["report"]:
                        raise RiskError("请先上传送检报告。")
                    actual = date_value(body.get("actual"), required=True)
                    if actual > now.date().isoformat():
                        raise RiskError("实际检测日期不能晚于今天。")
                    db.execute("""UPDATE tool_work_orders SET actual=?,submitted_at=?,report_name=?,report=?,
                        revision=revision+1 WHERE id=?""",
                        (actual, now.isoformat(), row["report_name"] or order["report_name"],
                         row["report"] or order["report"], order["id"]))
                    self.audit(db, order["id"], actor, "提交送检工单")
                    self.enqueue(db, order["id"], "inspection")
                    db.execute("UPDATE tool_work_archives SET needs_refresh=1 WHERE month=?", (order["month"],))
                    continue
                snapshot = db.execute("SELECT content,layout FROM tool_workspace_templates WHERE cache_key=?",
                                      (item["template_key"],)).fetchone()
                validate_inputs(snapshot["content"], draft["values"], draft["checks"],
                                complete=True, expected_sha256=item["template_version"])
                draft = self._workspace_check_result(db, order, draft, snapshot["layout"], now)
                db.execute("""INSERT INTO tool_work_reviews(order_id,status,payload,submitted_by,submitted_at)
                    VALUES(?,'pending',?,?,?) ON CONFLICT(order_id) DO UPDATE SET
                    status='pending',payload=excluded.payload,submitted_by=excluded.submitted_by,
                    submitted_at=excluded.submitted_at,reviewer='',reviewed_at='',note=''""",
                    (order["id"], dumps(draft), actor["id"], now.isoformat()))
                db.execute("UPDATE tool_workspace_items SET draft=? WHERE session_id=? AND asset_id=?",
                           (dumps(draft), identity, asset["id"]))
                db.execute("UPDATE tool_work_orders SET revision=revision+1 WHERE id=?", (order["id"],))
                self.audit(db, order["id"], actor, "提交自检待审核")
            if not count:
                raise RiskError("没有可提交的待填写工单。", 409)
            db.execute("UPDATE tool_workspace_sessions SET submitted_revision=?,submitted_at=? WHERE id=?",
                       (revision, now.isoformat(), identity))
            self._workspace_bump(db, identity)
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    def _workspace_check_result(self, db, order, draft, layout, now):
        schema = json.loads(layout)["schema"]
        address = "G11" if schema.get("layout") == "hvac" else "G8"
        value = draft["values"].get(schema["sheet"] + "!" + address, "").strip().replace(" ", "T", 1)
        if len(value) == 10:
            value += "T00:00"
        actual = timestamp(value)
        if not actual or dt.datetime.fromisoformat(actual).replace(tzinfo=TZ) > now:
            raise RiskError("请在原表填写有效检查日期或时间，不能晚于当前时间。")
        outcome = draft.get("outcome")
        if outcome not in {"normal", "repair", "scrap"}:
            raise RiskError("请选择逐台检查结果。")
        if outcome == "normal" and "否" in draft["checks"].values():
            raise RiskError("检查项存在「否」，检测结果不能选「正常」。")
        if outcome == "repair":
            text(draft.get("reason") or "", 1000, True)
        elif outcome == "scrap":
            self.scrap_request(db, order, draft)
        return draft | {"actual": actual[:10], "qualified": outcome == "normal",
                        "reviewer": "", "reviewed_at": "", "signature": ""}

    def workspace_review(self, actor, identity, body):
        from .tool_management import MAX_REPORT, clock
        from .tool_workspace_workbook import fill, validate_signature

        self._workspace_access(actor)
        approval = body.get("action") == "approve"
        if not approval and body.get("action") != "reject":
            raise RiskError("请选择通过或驳回。")
        selected = body.get("items")
        if not isinstance(selected, list) or not 1 <= len(selected) <= MAX_ASSETS:
            raise RiskError("请选择待审核工具。")
        note = text(body.get("note") or "", 1000, not approval)
        shared = body.get("signature") or ""
        if approval and shared:
            validate_signature(shared)
        now = clock()
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            by_id = {item["asset_id"]: item for item in items}
            seen = set()
            for entry in selected:
                if not isinstance(entry, dict):
                    raise RiskError("审核项目格式无效。")
                asset_id = text(entry.get("asset_id"), 120, True)
                if asset_id not in by_id or asset_id in seen:
                    raise RiskError("审核项目无权访问或重复。", 403)
                seen.add(asset_id)
                item = by_id[asset_id]
                order = self._workspace_order(db, item)
                if not order or not self.workspace_can_review(db, actor, order):
                    raise RiskError("仅本楼楼长或获授权的代审人可审核。", 403)
                review = db.execute("SELECT * FROM tool_work_reviews WHERE order_id=?", (order["id"],)).fetchone()
                state = "approved" if approval else "rejected"
                if (review and review["status"] == state and review["reviewer"] == actor["id"]
                        and entry.get("revision") == order["revision"] - 1 and review["note"] == note):
                    continue
                self.check_revision(order, entry.get("revision"))
                if not review or review["status"] != "pending" or order["submitted_at"] or order["outcome"]:
                    raise RiskError("原工单已变化，请刷新后审核。", 409)
                self._workspace_check_scopes(db, [item])
                if approval:
                    if self._workspace_legacy_overlaps(db, asset_id, row["kind"], order["month"]):
                        raise RiskError("旧工作台批次可能已归档，请先核对原台账回执。", 409)
                    draft = json.loads(review["payload"])
                    signature = entry.get("signature") or shared
                    validate_signature(signature)
                    draft.update(reviewer=actor["name"], reviewed_at=now.strftime("%Y-%m-%dT%H:%M"), signature=signature)
                    content = db.execute("SELECT content FROM tool_workspace_templates WHERE cache_key=?",
                                         (item["template_key"],)).fetchone()[0]
                    asset = json.loads(item["asset"])
                    report = fill(content, draft["values"], draft["reviewer"], draft["reviewed_at"], signature,
                                  draft["checks"], asset, expected_sha256=item["template_version"])
                    if len(report) > MAX_REPORT:
                        raise RiskError("单份归档报告超过 12 MB。", 413)
                    outcome = draft["outcome"]
                    updates = {"outcome": outcome, "actual": draft["actual"], "submitted_at": now.isoformat(),
                               "report_name": "自检记录单.xlsx", "report": report, "reason": draft.get("reason", "")}
                    if outcome == "scrap":
                        updates.update(self.scrap_request(db, order, draft))
                    self.enqueue(db, order["id"], *{"normal": ("maintenance",), "repair": ("repair", "change"),
                                                   "scrap": ("scrap", "change")}[outcome])
                    fields = ",".join(f"{name}=?" for name in updates)
                    db.execute(f"UPDATE tool_work_orders SET {fields},revision=revision+1 WHERE id=?",
                               (*updates.values(), order["id"]))
                    db.execute("UPDATE tool_work_archives SET needs_refresh=1 WHERE month=?", (order["month"],))
                    db.execute("UPDATE tool_workspace_items SET draft=? WHERE session_id=? AND asset_id=?",
                               (dumps(draft), identity, asset_id))
                else:
                    db.execute("UPDATE tool_work_orders SET revision=revision+1 WHERE id=?", (order["id"],))
                db.execute("UPDATE tool_work_reviews SET status=?,reviewer=?,reviewed_at=?,note=? WHERE order_id=?",
                           (state, actor["id"], now.isoformat(), note, order["id"]))
                label = ("通过" if approval else "驳回") + ("（楼长）" if self.is_building_leader(db, actor, order["scope"]) else "（代审）")
                self.audit(db, order["id"], actor, label, note)
            self._workspace_bump(db, identity)
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    def workspace_can_review(self, db, actor, order):
        from .tool_management import can_scope, full_access

        return not actor.get("_tool_impersonation") and can_scope(actor, order["scope"]) and (
            self.is_building_leader(db, actor, order["scope"]) or full_access(actor)
        )

    def _workspace_check_scopes(self, db, items):
        for item in items:
            current = db.execute('SELECT * FROM tool_inventory WHERE id=? AND active=1',
                                 (item['asset_id'],)).fetchone()
            if not current or self.effective(current)['scope'] != json.loads(item['asset'])['scope']:
                raise RiskError('工具归属楼栋已变化，请重新打开填写核对。', 409)

    def workspace_download(self, actor, identity):
        self._workspace_access(actor)
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity)
            if any(item["order_id"] for item in items):
                orders = [order for item in items if (order := self._workspace_order(db, item))
                          and order["outcome"] != "cancelled"]
                if not orders or any(not order["submitted_at"] for order in orders):
                    raise RiskError("请先完成工单提交。", 409)
                output = io.BytesIO()
                self.package_work(db, row["created_at"][:7], orders, output)
                return output.getvalue()
            if not row["package"]:
                raise RiskError("请先提交本批工具报告。", 409)
            return row["package"]

    def workspace_sync_one(self):
        """Archive one tool at a time; the session lease serializes shared file upload."""
        now, lease = int(time.time()), uuid.uuid4().hex
        with self.connect(True) as db:
            item = db.execute("""SELECT item.* FROM tool_workspace_items item
                JOIN tool_workspace_sessions session ON session.id=item.session_id
                WHERE session.status='pending' AND item.order_id='' AND session.lease_until<=?
                  AND item.remote_id='' AND item.next_try<=?
                ORDER BY item.next_try,session.submitted_at,session.id,item.position LIMIT 1""", (now, now)).fetchone()
            if not item:
                return False
            identity = item["session_id"]
            row = db.execute("SELECT * FROM tool_workspace_sessions WHERE id=?", (identity,)).fetchone()
            db.execute("UPDATE tool_workspace_sessions SET lease=?,lease_until=? WHERE id=?",
                       (lease, now + ARCHIVE_LEASE_SECONDS, identity))
        asset, draft = json.loads(item["asset"]), json.loads(item["draft"])
        actual = draft["reviewed_at"][:10] if row["kind"] == "self" else row["submitted_at"][:10]
        payload = {**asset, "asset_id": item["asset_id"], "kind": row["kind"],
                   "qualified": draft["qualified"], "due": row["created_at"][:10]}

        external = row["kind"] == "external"
        report = row if external else item

        def save_file(token):
            if not isinstance(token, str) or not token:
                raise RiskError("飞书未确认报告附件。", 503)
            with self.connect(True) as db:
                self._workspace_check_lease(db, identity, lease)
                self._workspace_set_file_token(db, identity, item["asset_id"], external, token)

        def save_attempt(attempted_at):
            with self.connect(True) as db:
                self._workspace_check_lease(db, identity, lease)
                result = db.execute("""UPDATE tool_workspace_items SET attempted_at=?
                    WHERE session_id=? AND asset_id=? AND attempted_at=0""",
                    (attempted_at, identity, item["asset_id"]))
                if not result.rowcount:
                    raise RiskError("原归档任务已有写入凭据。", 409)

        try:
            result = self.cloud.write_workspace_item(
                identity, payload, report["report_name"], report["report"], actual,
                file_token=report["file_token"], save_file=save_file,
                attempted_at=item["attempted_at"], save_attempt=save_attempt,
            )
            remote_id = result.get("remote_id") if isinstance(result, dict) else None
            file_token = result.get("file_token") if isinstance(result, dict) else None
            if not isinstance(remote_id, str) or not re.fullmatch(r"rec[A-Za-z0-9]+", remote_id):
                raise RiskError("飞书未确认工具台账写入。", 503)
            if not isinstance(file_token, str) or not file_token.strip():
                raise RiskError("飞书未确认工具台账的报告附件。", 503)
        except Exception as exc:
            remote_id = ""
            error = str(exc) if isinstance(exc, RiskError) else "飞书同步结果未确认，后台将重试。"
        else:
            error = ""
        with self.connect(True) as db:
            current = db.execute("SELECT lease FROM tool_workspace_sessions WHERE id=?", (identity,)).fetchone()
            if current["lease"] != lease:
                return True
            if remote_id:
                self._workspace_set_file_token(db, identity, item["asset_id"], external, file_token)
            db.execute("""UPDATE tool_workspace_items SET remote_id=?,archive_error=?,next_try=?
                WHERE session_id=? AND asset_id=?""",
                (remote_id, error, int(time.time()) + ARCHIVE_RETRY_SECONDS if error else 0,
                 identity, item["asset_id"]))
            remaining = db.execute(
                "SELECT 1 FROM tool_workspace_items WHERE session_id=? AND remote_id='' LIMIT 1", (identity,),
            ).fetchone()
            db.execute("UPDATE tool_workspace_sessions SET status=?,lease='',lease_until=0 WHERE id=?",
                       ("pending" if remaining else "archived", identity))
        return True

    @staticmethod
    def _workspace_check_lease(db, identity, lease):
        if not db.execute("SELECT 1 FROM tool_workspace_sessions WHERE id=? AND lease=?", (identity, lease)).fetchone():
            raise RiskError("原归档任务租约已失效。", 409)

    @staticmethod
    def _workspace_set_file_token(db, identity, asset_id, external, token):
        if external:
            db.execute("UPDATE tool_workspace_sessions SET file_token=? WHERE id=?", (token, identity))
        else:
            db.execute("UPDATE tool_workspace_items SET file_token=? WHERE session_id=? AND asset_id=?",
                       (token, identity, asset_id))
