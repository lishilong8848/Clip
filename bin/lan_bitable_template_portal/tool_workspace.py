"""Persistent tool workspace sessions and per-tool archive receipts."""

import base64
import datetime as dt
import io
import json
import logging
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
EDIT_FIELDS = ("values", "checks", "completion", "reviewer_id", "outcome", "reason", "quantity", "actual", "scrap_photos")
ORDER_STATUS_FIELDS = """id,month,scope,kind,due,revision,owner,owner_name,submitted_at,outcome,
    report_name,actual,reason,resolution,scrap_qty,asset,supervisor,manager,inspection_meta,workspace_direct,repair_started_at"""
WORKSPACE_SESSION_SUMMARY_FIELDS = "id,revision,kind,status,owner,owner_name,created_at,updated_at,submitted_at,report_name"
WORKSPACE_ITEM_STATUS_FIELDS = "asset_id,asset,template_key,order_id,binding_error,binding_pending,remote_id,attempted_at,archive_error,deleted_at"
CHECKED_AT = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}(?:[T ][0-9]{2}:[0-9]{2}(?::[0-9]{2}(?:\.[0-9]{1,6})?)?)?')


def workspace_text(value, label, limit, *, required=False):
    if value is None:
        value = ""
    if not isinstance(value, str) or "\x00" in value:
        raise RiskError(f"{label}格式不正确。")
    if len(value) > limit:
        raise RiskError(f"{label}不能超过 {limit} 字。")
    value = value.strip()
    if required and not value:
        raise RiskError(f"请填写{label}。")
    return value


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
                CREATE TABLE IF NOT EXISTS tool_workspace_review_notices(
                    order_id TEXT NOT NULL, revision INTEGER NOT NULL, next_try REAL NOT NULL DEFAULT 0,
                    lease_until REAL NOT NULL DEFAULT 0, lease TEXT NOT NULL DEFAULT '',
                    sent_to TEXT NOT NULL DEFAULT '[]', card TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(order_id, revision)
                );
            """)
            db.execute("BEGIN IMMEDIATE")
            columns = {row["name"] for row in db.execute("PRAGMA table_info(tool_workspace_items)")}
            for name in ("order_id", "binding_error", "deleted_at", "ledger_table"):
                if name not in columns:
                    db.execute(f"ALTER TABLE tool_workspace_items ADD COLUMN {name} TEXT NOT NULL DEFAULT ''")
            if 'binding_pending' not in columns:
                db.execute('ALTER TABLE tool_workspace_items ADD COLUMN binding_pending INTEGER NOT NULL DEFAULT 0')
            db.execute("""CREATE UNIQUE INDEX IF NOT EXISTS tool_workspace_order
                ON tool_workspace_items(order_id) WHERE order_id<>''""")
            db.execute('CREATE INDEX IF NOT EXISTS tool_workspace_recent ON tool_workspace_sessions(updated_at DESC,id DESC)')

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

        return can_scope(actor, asset.get('scope'))

    @staticmethod
    def _workspace_inventory_asset(row):
        source = json.loads(row["source"])
        config = json.loads(row["config"])
        if config.get("supervisor"):
            source["supervisor"] = config["supervisor"]
        return source

    @staticmethod
    def _workspace_asset(asset):
        return {key: asset.get(key) or "" for key in (
            "id", "name", "scope", "model", "brand", "location", "template_id", "self_cycle", "external_cycle", "quantity",
        )}

    def _workspace_visible_items(self, db, actor, session, items):
        from .tool_management import can_scope

        if any(item["order_id"] for item in items):
            orders = self._workspace_orders(db, items)
            leaders = json.loads(self.state(db, 'leaders') or '{}')
            return [item for item in items if (order := orders.get(item['order_id']))
                    and self.visible(db, actor, order, leaders=leaders)]
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
    def _workspace_orders(db, items):
        ids = tuple(item['order_id'] for item in items if item['order_id'])
        if not ids:
            return {}
        placeholders = ','.join('?' for _ in ids)
        return {row['id']: row for row in db.execute(
            f'SELECT {ORDER_STATUS_FIELDS} FROM tool_work_orders WHERE id IN ({placeholders})', ids)}

    @staticmethod
    def _workspace_rows(db, identity, *, status_only=False, include_files=True):
        fields = WORKSPACE_ITEM_STATUS_FIELDS
        if not status_only:
            fields = "*" if include_files else fields + ",template_id,template_version,draft"
        return db.execute(
            f"SELECT {fields} FROM tool_workspace_items WHERE session_id=? ORDER BY position", (identity,),
        ).fetchall()

    def _workspace_session(self, db, actor, identity, *, status_only=False, include_files=True):
        fields = "*" if include_files and not status_only else WORKSPACE_SESSION_SUMMARY_FIELDS
        row = db.execute(f"SELECT {fields} FROM tool_workspace_sessions WHERE id=?", (identity,)).fetchone()
        items = self._workspace_visible_items(db, actor, row, self._workspace_rows(
            db, identity, status_only=status_only, include_files=include_files)) if row else []
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
    def _workspace_legacy_overlaps(db, asset_id, kind, month=None):
        return db.execute("""SELECT session.id,session.status,session.submitted_at,item.remote_id,item.asset
            FROM tool_workspace_items item JOIN tool_workspace_sessions session ON session.id=item.session_id
            WHERE item.asset_id=? AND item.order_id='' AND session.kind=?
              AND (session.status='pending' OR
                   (session.status='archived' AND substr(session.submitted_at,1,7)=?))
            ORDER BY session.submitted_at,session.id""", (asset_id, kind, month)).fetchall()

    def _workspace_asset_state(self, db, actor, asset, kind):
        batch = db.execute("""SELECT work.id,work.scope,work.kind,work.supervisor,work.manager,work.owner,work.workspace_direct,
                  work.revision,work.outcome,work.submitted_at,item.session_id
            FROM tool_work_orders work JOIN tool_workspace_items item ON item.order_id=work.id
            WHERE work.asset_id=? AND work.outcome<>'cancelled' AND
                (work.submitted_at IS NULL OR EXISTS (SELECT 1 FROM tool_work_sync
                    WHERE order_id=work.id AND synced<revision))
            ORDER BY work.submitted_at IS NOT NULL,work.month,work.id LIMIT 1""", (asset["id"],)).fetchone()
        if batch:
            if (batch['scope'] != asset['scope'] and not batch['submitted_at'] and not batch['outcome']
                    and self.workspace_can_review(db, actor, {'scope': asset['scope']})):
                return {'mode': 'transfer', 'order_id': batch['id'], 'revision': batch['revision'],
                        'problem': '原楼栋有未完成的办理记录，移交后可重新办理。'}
            if batch["scope"] != asset["scope"] or not self.visible(db, actor, batch):
                return {"mode": "blocked", "problem": "工具已有办理记录，请由原楼栋或获授权人员处理。"}
            return {"mode": "resume", "workspace_id": batch["session_id"],
                    "problem": "归档待同步" if batch["submitted_at"] else "已有办理记录"}
        if self._workspace_legacy_overlaps(db, asset["id"], kind):
            return {"mode": "blocked", "problem": "旧批次正在归档，可在办理记录查看。"}
        if not self.tool_available(db, asset):
            return {"mode": "blocked", "problem": "工具在检修、报废审批中或已全部报废。"}
        return {"mode": "direct"}

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
            file_token=report["file_token"], remote_id=item["remote_id"], unified=item['ledger_table'] == 'unified')
        if verified["remote_id"] != item["remote_id"]:
            raise RiskError("飞书回读与原回执不一致。", 409)
        with self.connect(True) as db:
            current, legacy, expected = candidate(db)
            if expected is None:
                return {"order_id": order_id}
            if current["revision"] != order["revision"] or legacy != row or expected != item:
                raise RiskError("核验期间工单已变化，请刷新后重试。", 409)
            dest = "maintenance"
            db.execute("""UPDATE tool_work_orders SET submitted_at=?,actual=?,outcome=?,owner=?,owner_name=?,
                report_name=?,report=?,revision=revision+1 WHERE id=?""",
                (row["submitted_at"], actual, "" if external else "normal", row["owner"], row["owner_name"],
                 report["report_name"], report["report"], order_id))
            self.enqueue(db, order_id, dest)
            if external and item['ledger_table'] != 'unified':
                db.execute("""UPDATE tool_work_sync SET file_token=?
                    WHERE order_id=? AND dest=?""", (verified["file_token"], order_id, dest))
            else:
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

    def workspace_list(self, actor, *, refresh_templates=False):
        from .tool_management import can_scope

        self._workspace_access(actor)
        warning = ""
        try:
            templates = self._workspace_template_list(self.selectable_feishu_templates(actor, fresh=refresh_templates))
        except Exception:
            warning = "飞书模板暂时无法读取，已有办理记录仍可打开。"
            with self.connect() as db:
                templates = self.filter_selectable_feishu_templates(
                    json.loads(self.state(db, "workspace_templates_last_good") or "[]"))
        else:
            with self.connect(True) as db:
                self.set_state(db, "workspace_templates_last_good", dumps(templates))
        with self.connect() as db:
            assets = []
            for row in db.execute("SELECT * FROM tool_inventory WHERE active=1"):
                asset = self._workspace_inventory_asset(row)
                if not can_scope(actor, asset.get("scope")):
                    continue
                kind, reason = self._workspace_kind(asset)
                plan = self._workspace_asset_state(db, actor, asset, kind) if kind else {}
                selectable = plan.get('mode') == 'direct' or bool(kind and self.tool_available(db, asset))
                if not selectable:
                    reason = reason or "工具在检修、报废审批中或已全部报废。"
                assets.append(self._workspace_asset(asset) | {
                    "kind": kind, "eligible_kinds": [kind] if selectable else [],
                    "selectable": selectable,
                    "disabled_reason": reason, "plan": plan,
                })
            history = self._workspace_history_page(db, actor)
            inventory_error = self.state(db, "inventory_error")
        assets.sort(key=lambda item: (item["scope"], item["name"], item["id"]))
        return {"assets": assets, "templates": templates, "warning": warning,
                "inventory_error": inventory_error, **history}

    def _workspace_history_page(self, db, actor, before='', before_id='', *, kind='', asset_id=''):
        from .tool_management import BUILDINGS, can_scope

        allowed = [scope for scope in BUILDINGS if can_scope(actor, scope)]
        slots = ','.join('?' for _ in allowed)
        sessions = []
        cursor = None
        rows = db.execute(f'''SELECT {WORKSPACE_SESSION_SUMMARY_FIELDS},operation FROM tool_workspace_sessions s
            WHERE (?='' OR (s.updated_at,s.id) < (?,?)) AND (?='' OR s.kind=?)
            AND EXISTS (SELECT 1 FROM tool_workspace_items i WHERE i.session_id=s.id
                        AND i.deleted_at='' AND (?='' OR i.asset_id=?))
            AND (s.owner=? OR EXISTS (SELECT 1 FROM tool_workspace_items i
                LEFT JOIN tool_work_orders w ON w.id=i.order_id WHERE i.session_id=s.id
                AND (w.scope IN ({slots}) OR (? AND i.order_id=''))))
            ORDER BY s.updated_at DESC,s.id DESC''',
            (before, before, before_id, kind, kind, asset_id, asset_id, actor['id'], *allowed, self.administrator(actor)))
        for row in rows:
            items = self._workspace_visible_items(db, actor, row, self._workspace_rows(db, row['id'], status_only=True))
            items = [item for item in items if not item['deleted_at'] and (not asset_id or item['asset_id'] == asset_id)]
            if not items:
                continue
            if len(sessions) == 30:
                last = sessions[-1]
                cursor = {'before': last['updated_at'], 'id': last['id']}
                break
            summary = self._workspace_detail(db, row, items, actor, status_only=True)
            summary['history_items'] = summary.pop('items')
            if row['owner'] == actor['id']:
                summary['operation_id'] = row['operation']
            sessions.append(summary)
        return {'sessions': sessions, 'history_cursor': cursor}

    def workspace_history(self, actor, before='', before_id='', kind='', asset_id=''):
        self._workspace_access(actor)
        if len(before) > 40 or len(before_id) > 128 or bool(before) != bool(before_id) or kind not in ('', 'self', 'external'):
            raise RiskError('办理记录分页参数无效。')
        asset_id = text(asset_id, 120)
        with self.connect() as db:
            db.execute('BEGIN')
            return self._workspace_history_page(db, actor, before, before_id, kind=kind, asset_id=asset_id)

    def workspace_refresh(self, actor):
        self._workspace_access(actor)
        with self._workspace_refresh_lock:
            self.refresh()
        return self.workspace_list(actor, refresh_templates=True)

    def workspace_get(self, actor, identity):
        self._workspace_access(actor)
        with self.connect() as db:
            db.execute('BEGIN')
            row, items = self._workspace_session(db, actor, identity, include_files=False)
            return self._workspace_detail(db, row, items, actor)

    def workspace_status(self, actor, identity, known=''):
        self._workspace_access(actor)
        with self.connect() as db:
            db.execute('BEGIN')
            row, items = self._workspace_session(db, actor, identity, status_only=True)
            result = self._workspace_detail(db, row, items, actor, status_only=True)
        version = digest([result['revision'], result['status'], result['items']])
        if version == known:
            return {'unchanged': True, 'status_version': version}
        return result | {'status_version': version}

    def workspace_people(self, actor, identity):
        self._workspace_access(actor)
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity, status_only=True)
            scopes = {json.loads(item["asset"])["scope"] for item in items}
            leaders = json.loads(self.state(db, "leaders") or "{}")
        leader_ids = set().union(*(leaders.get(scope, ()) for scope in scopes))
        directory = self.workspace_directory() if self.workspace_directory else []
        candidates = {actor["id"]: actor}
        for person in directory:
            person_id = person.get("record_id")
            if (not person_id or person_id in candidates or not person.get("selectable") or person.get("departed")
                    or person.get("open_id") not in leader_ids and person.get("name") not in {"李世龙", "刘小杏"}):
                continue
            try:
                candidates[person_id] = self.person_for(person_id)
            except RiskError:
                continue
        with self.connect() as db:
            return {scope: [{"id": person["id"], "name": person["name"]} for person in candidates.values()
                            if self.workspace_can_review(db, person, {"scope": scope})] for scope in scopes}

    def workspace_resume(self, actor, identity, body=None):
        self._workspace_access(actor)
        with self._workspace_open_lock:
            with self.connect(True) as db:
                row, items = self._workspace_session(db, actor, identity)
                if row["status"] in {"draft","opening"} and all(not item["order_id"] for item in items):
                    if row["owner"] != actor["id"]:
                        raise RiskError("请由旧草稿的填报人接续原表。", 403)
                    assets = self._workspace_assets_for_open(db, actor, [item["asset_id"] for item in items], row["kind"])
                    self._workspace_check_scopes(db, items)
                    for item, asset in zip(items, assets):
                        order_id = self._workspace_create_item(db, actor, asset, row["kind"])
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
        from collections import defaultdict
        from .tool_management import full_access, tool_ledger_status

        layouts = {}
        result = []
        orders = self._workspace_orders(db, items)
        leaders = json.loads(self.state(db, 'leaders') or '{}') if orders else {}
        reviews, jobs = {}, defaultdict(list)
        if orders:
            slots = ','.join('?' for _ in orders)
            reviews = {entry['order_id']: entry for entry in db.execute(
                f'SELECT order_id,status,note FROM tool_work_reviews WHERE order_id IN ({slots})', tuple(orders))}
            for job in db.execute(f'''SELECT order_id,revision,synced,error,attempted_at,remote_id
                    FROM tool_work_sync WHERE order_id IN ({slots}) ORDER BY dest''', tuple(orders)):
                jobs[job['order_id']].append(job)
        for item in items:
            key = item["template_key"]
            if key and key not in layouts and not status_only:
                layouts[key] = json.loads(db.execute(
                    "SELECT layout FROM tool_workspace_templates WHERE cache_key=?", (key,),
                ).fetchone()[0])
            order = orders.get(item['order_id'])
            review = reviews.get(item['order_id'])
            sync = jobs[item['order_id']]
            pending_review = bool(review and review['status'] == 'pending')
            open_order = bool(order and not order['submitted_at'] and order['outcome'] != 'cancelled')
            order_status = ('cancelled' if order['outcome'] == 'cancelled' else 'review_pending' if pending_review
                            else 'pending' if open_order else 'submitted') if order else None
            archive_status = (
                "archived" if item["remote_id"] else "verifying" if item["attempted_at"]
                else "retrying" if item["archive_error"] else row["status"]
            )
            archive_error = item["archive_error"]
            if order:
                archive_error = next((job["error"] for job in sync if job["error"]), "")
                archive_status = ("verifying" if any(job['attempted_at'] and not job['remote_id']
                                                        for job in sync if job['synced'] != job['revision'])
                                  else "retrying" if archive_error else "pending") if order["submitted_at"] else order_status
                if sync and all(job['synced'] == job['revision'] for job in sync):
                    archive_status = "archived"
            status = {"id": item["asset_id"], "order_id": item["order_id"],
                      "order_revision": order['revision'] if order else None, "order_status": order_status,
                      "binding_error": item["binding_error"], 'binding_pending': bool(item['binding_pending']),
                      "archive_status": archive_status, "archive_error": archive_error}
            if order and order['submitted_at']:
                status['result_label'], status['tool_status_label'] = tool_ledger_status(order)
            eligible = bool(order and self.eligible(db, actor, order, leaders=leaders))
            owner = bool(order and (self.owns_workspace_item(actor, order) or eligible and order['owner'] == actor['id']))
            mine = owner or bool(eligible and full_access(actor) and order['owner'])
            can_edit = bool(open_order and not pending_review and mine)
            manager = bool(order and self.workspace_can_review(db, actor, order, leaders=leaders))
            can_manage = bool(manager and not order['submitted_at'] and not order['outcome'])
            can_release = bool(order and not order['submitted_at'] and not order['outcome'] and (can_manage or owner))
            if status_only:
                asset = json.loads(item['asset'])
                if order:
                    can_delete = order['outcome'] not in {'repair', 'scrap'} and (owner or manager) and (
                        archive_status == 'archived' or (not order['submitted_at'] and order['outcome'] in ('', 'cancelled')))
                else:
                    can_delete = row['status'] == 'archived' and archive_status == 'archived'
                result.append(status | {'asset_name': asset['name'], 'scope': asset['scope'],
                                        'can_release': can_release, 'can_delete': can_delete})
                continue
            draft = json.loads(item["draft"])
            result.append(status | {
                "id": item["asset_id"], "asset_id": item["asset_id"], "asset": json.loads(item["asset"]),
                "template_id": item["template_id"], "template_version": item["template_version"],
                "template": layouts.get(key), **{name: value for name, value in draft.items() if name != "legacy_review"},
                "can_edit": can_edit and not item['binding_pending'] and not item['binding_error'],
                "can_review": bool(can_manage and pending_review and draft.get('reviewer_id') in (None, '', actor['id'])),
                'can_reassign': bool(can_manage and pending_review),
                'can_withdraw': bool(order and not order['outcome'] and pending_review
                                     and (can_manage or self.owns_workspace_item(actor, order)
                                          or order['owner'] == actor['id'] and eligible)),
                'can_release': can_release,
                "review_note": review['note'] if review else '',
                "remote_id": item["remote_id"],
            })
        summary = self._workspace_summary(row, items)
        summary['binding_pending'] = any(item['binding_pending'] for item in items)
        if row['status'] == 'opening' and not status_only:
            summary['can_edit'] = any(item.get('can_edit') for item in result)
        summary["current_actor_name"] = actor["name"]
        if row["kind"] == "external" and not status_only:
            summary['report_available'] = len(items) == db.execute(
                'SELECT count(*) FROM tool_workspace_items WHERE session_id=?', (row['id'],)).fetchone()[0]
            completed = next((order for order in orders.values() if order['actual']), None)
            summary["actual"] = completed['actual'] if completed else ''
            summary["inspection"] = json.loads(completed['inspection_meta']) if completed else {}
            if "confirmed_asset_ids" in summary["inspection"]:
                summary["inspection"]["confirmed_asset_ids"] = [
                    item["asset_id"] for item in items
                    if item["asset_id"] in summary["inspection"]["confirmed_asset_ids"]
                ]
            if completed:
                from .tool_management import inspection_end
                summary["inspection_expires_on"] = inspection_end(
                    json.loads(completed['asset']), completed['actual'], summary["inspection"])
                summary['inspection']['tools'] = {}
                for entry in result:
                    order = orders.get(entry['order_id'])
                    if order and order['submitted_at']:
                        meta = json.loads(order['inspection_meta'])
                        summary['inspection']['tools'][entry['id']] = {
                            'result': meta.get('result', ''),
                            'expires_on': inspection_end(json.loads(order['asset']), order['actual'], meta),
                        }
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
            asset = self._workspace_inventory_asset(row)
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
            layout = json.loads(row["layout"])
            if 'inspector_default' in layout.get('prefill_fields', {}):
                return cache_key, row["content"], layout
            content = row["content"]
        else:
            with self.connect() as db:
                catalog = db.execute("""SELECT c.entry,t.content FROM tool_check_template_catalog c
                    JOIN tool_check_templates t ON t.id=c.id AND t.version=c.version WHERE c.id=?""",
                                     (template["id"],)).fetchone()
            content = (catalog["content"] if catalog and self._library_stamp(json.loads(catalog["entry"]))
                       == self._library_stamp(template) else self.cloud.download_template(template))
        layout = describe(content)
        with self.connect(True) as db:
            db.execute("""INSERT INTO tool_workspace_templates VALUES(?,?,?)
                ON CONFLICT(cache_key) DO UPDATE SET layout=excluded.layout""", (cache_key, content, dumps(layout)))
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
        template_ids = body.get("template_ids", {})
        if not isinstance(template_ids, dict) or set(template_ids) - set(asset_ids) or template_id and template_ids:
            raise RiskError("工具与模板关联格式不正确。")
        template_ids = {key: text(value, 120, True) for key, value in template_ids.items()}
        if kind == "external" and (template_id or template_ids):
            raise RiskError("送检直接上传报告。")
        operation = text(body.get("operation_id"), 128, True)
        new_record = body.get('new_record') is True
        allow_temporary = body.get("allow_temporary") is True
        defer_bindings = body.get('defer_bindings') is True and kind == 'self' and not actor.get('_tool_impersonation')
        fingerprint = digest([kind, sorted(asset_ids), template_id, allow_temporary])
        if template_ids:
            fingerprint = digest([kind, sorted(asset_ids), sorted(template_ids.items())])
        if new_record:
            fingerprint = digest([fingerprint, 'new_record'])
        with self._workspace_open_lock:
            with self.connect() as db:
                existing = db.execute(
                    "SELECT * FROM tool_workspace_sessions WHERE owner=? AND operation=?", (actor["id"], operation),
                ).fetchone()
                if existing:
                    legacy_fingerprint = digest([kind, sorted(asset_ids), template_id])
                    if existing["fingerprint"] != fingerprint and (new_record or template_ids or existing["fingerprint"] != legacy_fingerprint):
                        raise RiskError("同一操作编号已用于其他工具或模板。", 409)
                    row, items = self._workspace_session(db, actor, existing["id"])
                    if row["status"] != "opening":
                        return self._workspace_detail(db, row, items, actor)
                    if not any(item["order_id"] for item in items):
                        return self._workspace_detail(db, row, items, actor)
                assets = self._workspace_assets_for_open(db, actor, asset_ids, kind)
                if not existing:
                    if kind == 'external' and len({asset['scope'] for asset in assets}) != 1:
                        raise RiskError('共用送检报告只能选择同一楼栋的工具，请按楼栋分批办理。')
                    if not new_record and len(assets) == 1:
                        plan = self._workspace_asset_state(db, actor, assets[0], kind)
                        if plan["mode"] == "resume":
                            return self._workspace_detail(db, *self._workspace_session(db, actor, plan["workspace_id"]), actor)
            if existing:
                identity = existing["id"]
            else:
                identity = self._workspace_create_session(actor, operation, fingerprint, kind, assets, template_id, template_ids,
                                                          new_record=new_record)
            if kind == "self" and not defer_bindings:
                self._workspace_apply_bindings(actor, identity)
            with self.connect(True) as db:
                row, items = self._workspace_session(db, actor, identity)
                self._workspace_finish_open(db, identity)
                return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    def _workspace_create_session(self, actor, operation, fingerprint, kind, assets, template_id, template_ids, *, new_record=False):
        from .tool_workspace_workbook import initial_values

        templates = {row["id"]: row for row in self.selectable_feishu_templates(actor)} if kind == "self" else {}
        identity = uuid.uuid4().hex
        snapshots = {}
        rows = []
        now = dt.datetime.now(TZ)
        for index, asset in enumerate(assets):
            selected, version, cache_key = "", "", ""
            values = {}
            if kind == "self":
                selected = template_ids.get(asset["id"]) or template_id or asset["template_id"]
                if selected not in templates:
                    raise RiskError(f"「{asset['name']}」缺少可用的关联模板，请选择模板。", 409)
                if selected not in snapshots:
                    snapshots[selected] = self._workspace_snapshot(templates[selected])
                cache_key, _, layout = snapshots[selected]
                version = layout["template_sha256"]
                values = initial_values(layout, asset, actor["name"], now.date().isoformat())
            draft = {"values": values, "checks": {}, "reviewer": "", "reviewed_at": "",
                     "signature": "", "qualified": None}
            rows.append((identity, asset["id"], index, dumps(asset | {"template_id": selected}),
                         selected, version, cache_key, asset["template_id"], dumps(draft)))
        now = now.isoformat(timespec="seconds")
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
            order_ids = [self._workspace_create_item(db, actor, asset, kind, new_record=new_record) for asset in current]
            db.execute("""INSERT INTO tool_workspace_sessions
                (id,owner,owner_name,operation,fingerprint,kind,status,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)""",
                       (identity, actor["id"], actor["name"], operation, fingerprint, kind, "opening", now, now))
            db.executemany("""INSERT INTO tool_workspace_items
                (session_id,asset_id,position,asset,template_id,template_version,template_key,previous_template,draft,order_id,binding_pending)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)""", [(*row, order_id, int(bool(row[4]) and row[4] != row[7]))
                                                   for row, order_id in zip(rows, order_ids)])
        return identity

    def _workspace_create_item(self, db, actor, asset, kind, *, new_record=False):
        from .tool_management import clock

        if new_record:
            if not self.tool_available(db, asset):
                raise RiskError(f"「{asset['name']}」在检修、报废审批中或已全部报废。", 409)
        else:
            state = self._workspace_asset_state(db, actor, asset, kind)
            if state["mode"] != "direct":
                raise RiskError(f"「{asset['name']}」{state['problem']}，请刷新工作台。", 409)
        order_id = uuid.uuid4().hex
        today = clock().date()
        self.insert_work_order(db, order_id, today.strftime('%Y-%m'), asset, kind, today.isoformat(), workspace_direct=True)
        db.execute("UPDATE tool_work_orders SET owner=?,owner_name=?,revision=revision+1 WHERE id=? AND owner=''",
                   (actor["id"], actor["name"], order_id))
        self.audit(db, order_id, actor, "工作台新建办理记录")
        return order_id

    @staticmethod
    def _workspace_finish_open(db, identity):
        if not db.execute("""SELECT 1 FROM tool_workspace_items i LEFT JOIN tool_work_orders w ON w.id=i.order_id
                WHERE i.session_id=? AND (i.binding_pending=1 OR i.binding_error<>'')
                  AND COALESCE(w.outcome,'')<>'cancelled'""", (identity,)).fetchone():
            db.execute("UPDATE tool_workspace_sessions SET status='draft' WHERE id=? AND status='opening'", (identity,))

    def _workspace_bind_templates(self, entries):
        batch = getattr(self.cloud, 'bind_templates', None)
        if callable(batch):
            yield from batch(entries)
            return
        for asset_id, template_id, previous in entries:
            try:
                yield asset_id, self.cloud.bind_template(asset_id, template_id, previous), ''
            except RiskError as exc:
                yield asset_id, '', str(exc)
            except Exception:
                yield asset_id, '', '飞书关联结果暂未确认，请重试。'

    @staticmethod
    def _workspace_save_inventory_binding(db, asset_id, previous, template_id):
        current = db.execute('SELECT source FROM tool_inventory WHERE id=? AND active=1', (asset_id,)).fetchone()
        if current:
            source = json.loads(current['source'])
            if (source.get('template_id') or '') == previous:
                source['template_id'] = template_id
                db.execute('UPDATE tool_inventory SET source=? WHERE id=?', (dumps(source), asset_id))

    def _workspace_apply_bindings(self, actor, identity, *, pending_only=False):
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity)
            if row["status"] != "opening":
                return
            if row["owner"] != actor["id"]:
                raise RiskError("仅本批的领取人可核对模板关联。", 403)
            items = [item for item in items if not item['order_id'] or
                     (order := self._workspace_order(db, item, status_only=True)) and order['outcome'] != 'cancelled']
            selectable = {template['id'] for template in self.list_templates(db, False)}
        records = {}
        rejected = {}
        for item in items:
            template_id = item['template_id']
            previous = item["previous_template"]
            if not template_id or pending_only and not item['binding_pending']:
                continue
            if template_id == previous and not item['binding_error'] and not item['binding_pending']:
                continue
            records[item['asset_id']] = item
            if pending_only and len(records) == 8:
                break
        with self.connect() as db:
            for asset_id, item in records.items():
                try:
                    self._workspace_assets_for_open(db, actor, [asset_id], row['kind'])
                    self._workspace_check_scopes(db, [item])
                    if item['template_id'] not in selectable:
                        raise RiskError('模板已停用，请选择其他模板。', 409)
                except RiskError as exc:
                    rejected[asset_id] = str(exc)
        entries = [(asset_id, record['template_id'], record['previous_template'])
                   for asset_id, record in records.items() if asset_id not in rejected]
        outcomes = self._workspace_bind_templates(entries)
        from itertools import chain

        for asset_id, bound, error in chain(((asset_id, '', reason) for asset_id, reason in rejected.items()), outcomes):
            item = records[asset_id]
            template_id, previous = item['template_id'], item['previous_template']
            message = '' if template_id == bound else error or '飞书未确认模板关联，请重试原操作。'
            with self.connect(True) as db:
                db.execute('UPDATE tool_workspace_items SET binding_pending=0,binding_error=? WHERE session_id=? AND asset_id=?',
                           (message, identity, asset_id))
                if not message:
                    self._workspace_save_inventory_binding(db, asset_id, previous, template_id)
                    db.execute('UPDATE tool_workspace_items SET previous_template=? WHERE session_id=? AND asset_id=?',
                               (template_id, identity, asset_id))
                if pending_only:
                    self._workspace_finish_open(db, identity)
            if message and not item['order_id']:
                raise RiskError(message, 503)

    def workspace_binding_tick(self):
        with self._workspace_open_lock, self.connect() as db:
            row = db.execute("""SELECT s.id,s.owner FROM tool_workspace_sessions s WHERE s.status='opening'
                AND EXISTS (SELECT 1 FROM tool_workspace_items i JOIN tool_work_orders w ON w.id=i.order_id
                    WHERE i.session_id=s.id AND i.binding_pending=1 AND w.outcome<>'cancelled')
                ORDER BY s.updated_at,s.id LIMIT 1""").fetchone()
        if not row:
            return False
        with self._workspace_open_lock:
            try:
                actor = self.person_for(row['owner'])
                self._workspace_access(actor)
                self._workspace_apply_bindings(actor, row['id'], pending_only=True)
            except RiskError as exc:
                with self.connect(True) as db:
                    db.execute('UPDATE tool_workspace_items SET binding_error=?,binding_pending=0 WHERE session_id=? AND binding_pending=1',
                               (str(exc), row['id']))
        return True

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
                template = next((entry for entry in self.selectable_feishu_templates(actor) if entry["id"] == template_id), None)
                if not template:
                    raise RiskError("模板已停用或不存在。", 409)
                key, _, layout = self._workspace_snapshot(template)
                from .tool_workspace_workbook import initial_values

                draft = json.loads(item["draft"])
                draft["values"] = initial_values(layout, asset, actor["name"], dt.datetime.now(TZ).date().isoformat())
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
    def _workspace_draft(row, revision, *, allow_opening=False):
        if type(revision) is not int or revision != row["revision"]:
            raise RiskError("办理会话已更新，请刷新后核对再保存。", 409)
        if row["status"] != "draft" and not (allow_opening and row['status'] == 'opening'):
            raise RiskError("此批次已提交，表格与报告已锁定。", 409)

    def workspace_save(self, actor, identity, body):
        from .tool_review_cards import validated_photos
        from .tool_workspace_workbook import validate_inputs

        self._workspace_access(actor)
        patches = body.get("items")
        if not isinstance(patches, list) or not patches or len(patches) > MAX_ASSETS:
            raise RiskError("没有可保存的工具表格。")
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity)
            self._workspace_draft(row, body.get("revision"), allow_opening=True)
            if row["kind"] != "self":
                raise RiskError("送检批次请使用上传报告。")
            by_id = {item["asset_id"]: item for item in items}
            updates = {}
            contents = {}
            for patch in patches:
                if not isinstance(patch, dict):
                    raise RiskError("工具填写内容格式无效。")
                asset_id = workspace_text(patch.get("asset_id") or patch.get("id"), "工具记录编号", 120, required=True)
                if asset_id not in by_id or asset_id in updates or patch.get("id", asset_id) != asset_id:
                    raise RiskError("工具填写记录不存在、重复或标识冲突。")
                item = by_id[asset_id]
                if item['binding_pending'] or item['binding_error']:
                    raise RiskError('该工具的模板关联尚未核验，请核对后再填写。', 409)
                order = self._workspace_order(db, item)
                if not order or not self.work_info(db, actor, order)["can_edit"]:
                    raise RiskError("仅已领取且待填写的工单可保存。", 403)
                draft = json.loads(item["draft"])
                label = f"「{json.loads(order['asset'])['name']}」"
                previous_reviewer = draft.get("reviewer_id", "")
                draft.update({name: patch[name] for name in EDIT_FIELDS if name in patch})
                if 'scrap_photos' in patch:
                    validated_photos(patch['scrap_photos'])
                if draft.get("completion", "") not in ("", "completed", "incomplete"):
                    raise RiskError("请选择有效的完成情况。")
                if "reviewer_id" in patch:
                    reviewer_id = workspace_text(patch["reviewer_id"], label + "审核人", 130)
                    if reviewer_id and reviewer_id != previous_reviewer:
                        reviewer = actor if reviewer_id == actor["id"] else self.person_for(reviewer_id)
                        if not self.workspace_can_review(db, reviewer, order):
                            raise RiskError("只能选择本楼楼长或获授权的审核人。", 403)
                        draft["reviewer_name"] = reviewer["name"]
                    if not reviewer_id:
                        draft["reviewer_name"] = ""
                    draft["reviewer_id"] = reviewer_id
                draft["reason"] = workspace_text(draft.get("reason"), label + "处理原因", 1000)
                key = item["template_key"]
                if key not in contents:
                    contents[key] = db.execute(
                        "SELECT content FROM tool_workspace_templates WHERE cache_key=?", (key,),
                    ).fetchone()[0]
                validate_inputs(contents[key], draft["values"], draft["checks"], expected_sha256=item["template_version"])
                updates[asset_id] = dumps(draft)
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            self._workspace_draft(row, body.get("revision"), allow_opening=True)
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

    @staticmethod
    def _queue_workspace_review_notice(db, order):
        db.execute("INSERT OR IGNORE INTO tool_workspace_review_notices(order_id,revision) VALUES(?,?)",
                   (order["id"], order["revision"] + 1))

    def workspace_review_notice_one(self, deliver, link_for, resolve_leader):
        from .tool_review_cards import batch_card, delivery_groups, plain, remember_delivery

        now, lease = time.time(), uuid.uuid4().hex
        pending = """FROM tool_work_orders w JOIN tool_work_reviews r ON r.order_id=w.id
            WHERE w.id=n.order_id AND w.revision=n.revision AND w.kind='self'
              AND w.submitted_at IS NULL AND w.outcome='' AND r.status='pending'"""
        with self.connect(True) as db:
            db.execute(f"""DELETE FROM tool_workspace_review_notices AS n
                WHERE n.lease_until<=? AND NOT EXISTS (SELECT 1 {pending})""", (now,))
            rows_join = f"""FROM tool_workspace_review_notices n JOIN tool_work_orders w ON w.id=n.order_id
                JOIN tool_workspace_items i ON i.order_id=w.id JOIN tool_work_reviews r ON r.order_id=w.id
                WHERE n.next_try<=? AND n.lease_until<=? AND EXISTS (SELECT 1 {pending})"""
            seed = db.execute(f"SELECT i.session_id {rows_join} ORDER BY n.next_try,n.order_id LIMIT 1",
                              (now, now)).fetchone()
            if not seed:
                return False
            session = seed['session_id']
            jobs = db.execute(f"""SELECT n.*,w.scope,w.asset,w.owner_name,i.asset_id,r.payload,r.submitted_at
                {rows_join} AND i.session_id=? ORDER BY n.order_id""", (now, now, session)).fetchall()
            db.executemany("""UPDATE tool_workspace_review_notices SET lease_until=?,lease=?
                WHERE order_id=? AND revision=?""",
                [(now + ARCHIVE_LEASE_SECONDS, lease, job['order_id'], job['revision']) for job in jobs])
            leaders = json.loads(self.state(db, 'leaders') or '{}')

        by_recipient, resolved, expected = {}, {}, {}
        sent = {job['order_id']: set(json.loads(job['sent_to'])) for job in jobs}
        for job in jobs:
            scope = job['scope']
            with self.connect() as db:
                try:
                    self._workspace_check_scopes(db, [job])
                except RiskError:
                    continue
            picked = json.loads(job['payload']).get('reviewer_id')
            targets = (picked,) if picked else tuple(dict.fromkeys(leaders.get(scope, ())))
            expected[job['order_id']] = len(targets) or 1
            for target in targets:
                try:
                    key = (bool(picked), target)
                    if key not in resolved:
                        resolved[key] = self.person_for(target) if picked else resolve_leader(target)
                    actor = resolved[key]
                    open_id = actor.get('open_id', '')
                    with self.connect() as db:
                        if (target != (actor.get('id') if picked else open_id)
                                or not re.fullmatch(r'ou_[A-Za-z0-9]+', open_id)
                                or not self.workspace_can_review(db, actor, {'scope': scope}, leaders=leaders)):
                            continue
                    if open_id not in sent[job['order_id']]:
                        by_recipient.setdefault(open_id, []).append(job)
                except RiskError:
                    continue

        groups = ((open_id, delivery, members) for open_id, rows in by_recipient.items()
                  for delivery, members in delivery_groups(rows, open_id))
        for open_id, delivery, group in groups:
            try:
                with self.connect() as db:
                    group = [job for job in group if db.execute(f"""SELECT 1
                        FROM tool_workspace_review_notices n WHERE n.order_id=? AND n.revision=? AND n.lease=?
                        AND EXISTS (SELECT 1 {pending})""", (job['order_id'], job['revision'], lease)).fetchone()]
                if not group:
                    continue
                if delivery:
                    identifier, card = delivery['id'], delivery['card']
                else:
                    entries = [{'order': job['order_id'], 'asset': job['asset_id'], 'revision': job['revision']} for job in group]
                    identifier = str(uuid.uuid5(uuid.NAMESPACE_URL, f'clipflow:tool-review:{session}:{entries}:{open_id}'))
                    card = next((saved for job in group
                                 if (saved := json.loads(job['card'] or '{}').get(identifier))), None)
                    if card is None:
                        elements = [{'tag': 'div', 'text': plain(f'{len(group)} 台工具待审核。通过时使用你的已登记签名。')}]
                        for job in group[:12]:
                            draft, asset = json.loads(job['payload']), json.loads(job['asset'])
                            inspector = draft.get('inspector_name') or job['owner_name']
                            result = {'normal': '合格', 'repair': '不合格·转检修', 'scrap': '不合格·申请报废'}.get(draft.get('outcome'), '—')
                            elements.append({'tag': 'div', 'text': plain(
                                f"{job['scope']}楼 · {asset['name']} · {asset.get('model') or '—'}\n检查人：{inspector}\n检查结论：{result}")})
                        first = group[0]
                        link = link_for(first['scope'], session, first['asset_id'])
                        card = batch_card('工具自检待审核', 'blue', elements,
                                          {'review': 'self', 'session': session, 'items': entries}, link, open_id)
                    with self.connect(True) as db:
                        for job in group:
                            params = (job['order_id'], job['revision'], lease)
                            stored = db.execute("""SELECT card FROM tool_workspace_review_notices
                                WHERE order_id=? AND revision=? AND lease=?""", params).fetchone()
                            if stored:
                                db.execute("""UPDATE tool_workspace_review_notices SET card=?
                                    WHERE order_id=? AND revision=? AND lease=?""",
                                    (remember_delivery(stored['card'], open_id, identifier, card), *params))
                if deliver(open_id, card, identifier) is True:
                    with self.connect(True) as db:
                        for job in group:
                            sent[job['order_id']].add(open_id)
                            db.execute("""UPDATE tool_workspace_review_notices SET sent_to=?
                                WHERE order_id=? AND revision=? AND lease=?""",
                                (dumps(sorted(sent[job['order_id']])), job['order_id'], job['revision'], lease))
            except Exception:
                logging.exception('Tool self-check review reminder deferred')
        with self.connect(True) as db:
            for job in jobs:
                params = (job['order_id'], job['revision'], lease)
                if len(sent[job['order_id']]) < expected.get(job['order_id'], 1):
                    db.execute("""UPDATE tool_workspace_review_notices SET next_try=?,lease='',lease_until=0
                        WHERE order_id=? AND revision=? AND lease=?""", (time.time()+ARCHIVE_RETRY_SECONDS, *params))
                else:
                    db.execute("DELETE FROM tool_workspace_review_notices WHERE order_id=? AND revision=? AND lease=?", params)
        return True

    def workspace_report(self, actor, identity, revision, name, data):
        from .tool_management import report_file

        self._workspace_access(actor)
        name = report_file(name, data)
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            if row["kind"] != "external":
                raise RiskError("维护报告由各工具的自检表生成。")
            self._check_workspace_report_scope(db, actor, row, items)
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

    @staticmethod
    def _workspace_inspection_receipts(body, assets, actual, *, required=True):
        from .tool_management import inspection_meta

        raw = body.get('inspection')
        overrides = raw.get('tools', {}) if isinstance(raw, dict) else {}
        ids = {asset['id'] for asset in assets}
        if not isinstance(overrides, dict) or set(overrides) - ids or any(
                not isinstance(value, dict) or set(value) - {'result', 'expires_on'} for value in overrides.values()):
            raise RiskError('逐台检测结论与本批工具不一致。')
        return {asset_id: inspection_meta(
            {'inspection': raw | override} if (override := overrides.get(asset_id)) else body,
            assets, actual, required=required) for asset_id in ids}

    def workspace_submit(self, actor, identity, body, *, require_report_check=False):
        self._workspace_access(actor)
        from .tool_management import clock, date_value
        from .tool_workspace_workbook import validate_inputs

        revision = body.get("revision")
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            if row['kind'] == 'external':
                self._check_workspace_report_scope(db, actor, row, items)
            if type(revision) is int and revision == row["submitted_revision"]:
                if row["kind"] == "external":
                    previous = [order for item in items if (order := self._workspace_order(db, item)) and order["submitted_at"]]
                    if any(order["actual"] != body.get("actual") for order in previous):
                        raise RiskError("原提交已使用其他检测日期，请刷新核对。", 409)
                    if previous and body.get("inspection") is not None:
                        saved = json.loads(previous[0]["inspection_meta"])
                        participants = [{"id": key} for key in saved.get("confirmed_asset_ids", [])]
                        receipts = self._workspace_inspection_receipts(body, participants, previous[0]['actual'])
                        if any(receipts.get(order['asset_id']) != json.loads(order['inspection_meta']) for order in previous):
                            raise RiskError("送检报告已按原核对结果提交，请刷新查看。", 409)
                return self._workspace_detail(db, row, items, actor)
            self._workspace_draft(row, revision)
            now = clock()
            if row["kind"] == "external" and row["owner"] != actor["id"]:
                if not all(self.workspace_can_review(db, actor, order) for item in items
                           if (order := self._workspace_order(db, item))):
                    raise RiskError("仅本批填报人或获授权人员可提交报告。", 403)
            editable = []
            for item in items:
                order = self._workspace_order(db, item)
                if order and self.work_info(db, actor, order)["can_edit"]:
                    editable.append((item, order))
            if not editable:
                raise RiskError("没有可提交的待填写工单。", 409)
            actual = None
            if row["kind"] == "external":
                actual = date_value(body.get("actual"), required=True)
                if actual > now.date().isoformat():
                    raise RiskError("实际检测日期不能晚于今天。")
                receipts = self._workspace_inspection_receipts(body, [json.loads(order['asset']) for _, order in editable],
                                                              actual, required=require_report_check)
            for item, order in editable:
                self._workspace_check_scopes(db, [item], require_available=True)
                asset = json.loads(order['asset'])
                if not order["workspace_direct"] and self._workspace_legacy_overlaps(
                        db, item["asset_id"], row["kind"], order["month"]):
                    raise RiskError("旧工作台批次可能已归档，请先核对原台账回执。", 409)
                draft = json.loads(item["draft"])
                if row["kind"] == "external":
                    if not row["report"] and not order["report"]:
                        raise RiskError("请先上传送检报告。")
                    db.execute("""UPDATE tool_work_orders SET actual=?,submitted_at=?,report_name=?,report=?,
                        inspection_meta=?,revision=revision+1 WHERE id=?""",
                        (actual, now.isoformat(), row["report_name"] or order["report_name"],
                         None if row['report'] else order['report'], dumps(receipts[asset['id']]), order["id"]))
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
                self._queue_workspace_review_notice(db, order)
                self.audit(db, order["id"], actor, "提交自检待审核")
            db.execute("UPDATE tool_workspace_sessions SET submitted_revision=?,submitted_at=? WHERE id=?",
                       (revision, now.isoformat(), identity))
            self._workspace_bump(db, identity)
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    def _workspace_check_result(self, db, order, draft, layout, now):
        workbook = json.loads(layout)
        schema = workbook["schema"]
        address = workbook.get("prefill_fields", {}).get("checked_at") or ("G11" if schema.get("layout") == "hvac" else "G8")
        label = f"「{json.loads(order['asset'])['name']}」"
        cell = schema["sheet"] + "!" + address
        value = draft["values"].get(cell, "").strip()
        date_label = f"{label}检查日期（{cell}）"
        try:
            if not CHECKED_AT.fullmatch(value):
                raise ValueError
            actual = dt.datetime.fromisoformat(value).replace(tzinfo=TZ)
        except ValueError:
            raise RiskError(f"{date_label}无效，请填写正确日期。") from None
        if actual > now:
            raise RiskError(f"{date_label}不能晚于当前时间。")
        outcome = draft.get("outcome")
        if outcome not in {"normal", "repair", "scrap"}:
            raise RiskError("请选择逐台检查结果。")
        if outcome == "normal" and "否" in draft["checks"].values():
            raise RiskError("检查项存在「否」，检测结果不能选「正常」。")
        draft["reason"] = workspace_text(draft.get("reason"), label + "处理原因", 1000,
                                         required=outcome in {"repair", "scrap"})
        if outcome == "scrap":
            self.scrap_request(db, order, draft)
        inspector = workbook.get('prefill_fields', {}).get('inspector') or ('G6' if schema.get('layout') == 'hvac' else 'G4')
        inspector_name = draft['values'].get(schema['sheet'] + '!' + inspector, '').strip()
        return draft | {"actual": actual.date().isoformat(), "qualified": outcome == "normal",
                        'inspector_name': inspector_name, 'checked_at_text': value,
                        "reviewer": "", "reviewed_at": "", "signature": ""}

    def workspace_review(self, actor, identity, body, *, from_card=False):
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
        if approval and body.get("use_saved_signature") is True:
            if not self.workspace_signature or actor.get("_tool_impersonation") or actor["id"].startswith("account:"):
                raise RiskError("当前账号没有可用的本人签名，请手绘签名。", 403)
            try:
                saved_signature = self.workspace_signature(actor["id"])
                shared = "data:image/png;base64," + base64.b64encode(saved_signature).decode("ascii")
            except Exception:
                raise RiskError("读取本人已登记签名失败，请核对签名或手绘签名。", 409) from None
            validate_signature(shared)
        now = clock()
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            by_id = {item["asset_id"]: item for item in items}
            seen = set()
            reviewed = 0
            for entry in selected:
                if not isinstance(entry, dict):
                    raise RiskError("审核项目格式无效。")
                asset_id = text(entry.get("asset_id"), 120, True)
                if asset_id in seen or (asset_id not in by_id and not from_card):
                    raise RiskError("审核项目无权访问或重复。", 403)
                seen.add(asset_id)
                item = by_id.get(asset_id)
                if item is None:
                    continue
                order = self._workspace_order(db, item)
                if entry.get('order_id') and (not order or entry['order_id'] != order['id']):
                    if from_card:
                        continue
                    raise RiskError('卡片对应的原工单已变化，请打开最新记录。', 409)
                if from_card and entry["revision"] != order["revision"]:
                    continue
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
                draft = json.loads(review["payload"])
                if draft.get("reviewer_id") not in (None, "", actor["id"]):
                    raise RiskError("请选择该记录指定的审核人账号登录确认。", 403)
                self._workspace_check_scopes(db, [item], require_available=approval)
                if approval:
                    if not order["workspace_direct"] and self._workspace_legacy_overlaps(
                            db, asset_id, row["kind"], order["month"]):
                        raise RiskError("旧工作台批次可能已归档，请先核对原台账回执。", 409)
                    signature = entry.get("signature") or shared
                    validate_signature(signature)
                    selected_time = entry.get("reviewed_at") or body.get("reviewed_at") or ""
                    reviewed_at = timestamp(selected_time) if selected_time else now.strftime("%Y-%m-%dT%H:%M")
                    if not reviewed_at or dt.datetime.fromisoformat(reviewed_at).replace(tzinfo=TZ) > now:
                        raise RiskError("请选择有效的审核日期和时间，不能晚于当前时间。")
                    draft.update(reviewer=actor["name"], reviewed_at=reviewed_at, signature=signature)
                    content = db.execute("SELECT content FROM tool_workspace_templates WHERE cache_key=?",
                                         (item["template_key"],)).fetchone()[0]
                    asset = json.loads(item["asset"])
                    report = fill(content, draft["values"], draft["reviewer"], draft["reviewed_at"], signature,
                                  draft["checks"], asset, expected_sha256=item["template_version"],
                                  completion=draft.get("completion", ""))
                    if len(report) > MAX_REPORT:
                        raise RiskError("单份归档报告超过 12 MB。", 413)
                    outcome = draft["outcome"]
                    updates = {"outcome": outcome, "actual": draft["actual"], "submitted_at": now.isoformat(),
                               "report_name": "自检记录单.xlsx", "report": report, "reason": draft.get("reason", "")}
                    if outcome == "scrap":
                        updates.update(self.scrap_request(db, order, draft))
                    if outcome == "repair":
                        updates['repair_started_at'] = now.isoformat()
                    self.enqueue(db, order["id"], "maintenance")
                    fields = ",".join(f"{name}=?" for name in updates)
                    db.execute(f"UPDATE tool_work_orders SET {fields},revision=revision+1 WHERE id=?",
                               (*updates.values(), order["id"]))
                    if updates['outcome'] == 'scrap':
                        self._queue_scrap_votes(db, order['id'], order['revision'] + 1)
                    db.execute("UPDATE tool_work_archives SET needs_refresh=1 WHERE month=?", (order["month"],))
                    db.execute("UPDATE tool_workspace_items SET draft=? WHERE session_id=? AND asset_id=?",
                               (dumps(draft), identity, asset_id))
                else:
                    db.execute("UPDATE tool_work_orders SET revision=revision+1 WHERE id=?", (order["id"],))
                db.execute("UPDATE tool_work_reviews SET status=?,reviewer=?,reviewed_at=?,note=? WHERE order_id=?",
                           (state, actor["id"], now.isoformat(), note, order["id"]))
                label = ("通过" if approval else "驳回") + ("（楼长）" if self.is_building_leader(db, actor, order["scope"]) else "（代审）")
                self.audit(db, order["id"], actor, label, note)
                reviewed += 1
            if reviewed or not from_card:
                self._workspace_bump(db, identity)
            if from_card:
                return {'card_review_count': reviewed}
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity), actor)

    def workspace_can_review(self, db, actor, order, *, leaders=None):
        from .tool_management import can_scope, full_access

        return not actor.get("_tool_impersonation") and can_scope(actor, order["scope"]) and (
            self.is_building_leader(db, actor, order["scope"], leaders=leaders) or full_access(actor)
        )

    def _workspace_check_scopes(self, db, items, *, require_available=False):
        for item in items:
            current = db.execute('SELECT * FROM tool_inventory WHERE id=? AND active=1',
                                 (item['asset_id'],)).fetchone()
            asset = self._workspace_inventory_asset(current) if current else None
            if not asset or asset['scope'] != json.loads(item['asset'])['scope']:
                raise RiskError('工具归属楼栋已变化，请重新打开填写核对。', 409)
            if require_available and not self.tool_available(db, asset):
                raise RiskError('工具正在检修或报废，暂不能提交原工单。', 409)

    def _workspace_release_order(self, db, actor, order, session_id, note):
        if (order['submitted_at'] or order['outcome'] or db.execute(
                'SELECT 1 FROM tool_work_sync WHERE order_id=?', (order['id'],)).fetchone()):
            raise RiskError('已形成归档或处置记录，不能释放原办理。', 409)
        db.execute("UPDATE tool_work_orders SET outcome='cancelled',resolution=?,revision=revision+1 WHERE id=?",
                   (note, order['id']))
        db.execute("UPDATE tool_work_reviews SET status='cancelled' WHERE order_id=? AND status='pending'", (order['id'],))
        db.execute('UPDATE tool_workspace_items SET binding_pending=0 WHERE order_id=?', (order['id'],))
        db.execute('UPDATE tool_work_archives SET needs_refresh=1 WHERE month=?', (order['month'],))
        if not db.execute('''SELECT 1 FROM tool_workspace_items i JOIN tool_work_orders w ON w.id=i.order_id
                             WHERE i.session_id=? AND w.outcome<>'cancelled' ''', (session_id,)).fetchone():
            db.execute("UPDATE tool_workspace_sessions SET status='cancelled' WHERE id=?", (session_id,))
        self.audit(db, order['id'], actor, '释放原工具办理', note)
        self._workspace_bump(db, session_id)

    def workspace_transfer(self, actor, body):
        self._workspace_access(actor)
        asset_id = text(body.get('asset_id'), 120, True)
        expected_scope = text(body.get('scope'), 10, True)
        order_id = text(body.get('order_id', ''), 120)
        with self._workspace_open_lock, self.connect(True) as db:
            current = db.execute('SELECT * FROM tool_inventory WHERE id=? AND active=1', (asset_id,)).fetchone()
            asset = self._workspace_inventory_asset(current) if current else None
            if not asset or asset['scope'] != expected_scope:
                raise RiskError('工具所在楼栋已变化，请刷新后重试。', 409)
            if not self.workspace_can_review(db, actor, {'scope': expected_scope}):
                raise RiskError('仅接收楼栋楼长或获授权人员可办理移交。', 403)
            orders = db.execute('''SELECT w.*,i.session_id FROM tool_work_orders w
                JOIN tool_workspace_items i ON i.order_id=w.id WHERE w.asset_id=? AND w.scope<>?
                    AND w.submitted_at IS NULL AND w.outcome='' AND (?='' OR w.id=?)''',
                (asset_id, expected_scope, order_id, order_id)).fetchall()
            if len(orders) != 1:
                raise RiskError('未找到唯一的原办理记录，请刷新核对。', 409)
            order = orders[0]
            self.check_revision(order, body.get('revision'))
            self._workspace_release_order(db, actor, order, order['session_id'],
                                          f"工具由{order['scope']}移交至{expected_scope}，保留原楼栋草稿和审核记录。")
        return {'transferred': True}

    def workspace_manage_item(self, actor, identity, body):
        self._workspace_access(actor)
        asset_id = text(body.get('asset_id'), 120, True)
        action = body.get('action')
        with self._workspace_open_lock, self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity, include_files=False)
            item = next((entry for entry in items if entry['asset_id'] == asset_id), None)
            order = self._workspace_order(db, item, status_only=False) if item else None
            if action == 'delete':
                if item is None or not self._workspace_detail(db, row, [item], actor, status_only=True)['items'][0]['can_delete']:
                    raise RiskError('仅可删除获授权且未进入归档处理的办理记录，或已完成归档的记录。', 409)
                if not item['deleted_at']:
                    self.check_revision(order if order else row, body.get('revision'))
                    from .tool_management import clock

                    if order and not order['submitted_at'] and not order['outcome']:
                        self._workspace_release_order(db, actor, order, identity, '删除办理记录，保留原草稿。')
                    else:
                        self._workspace_bump(db, identity)
                        if order and order['outcome'] in {'repaired', 'scrapped', 'rejected'}:
                            from .tool_management import HIDE_FOLLOWUP
                            self.audit(db, order['id'], actor, HIDE_FOLLOWUP, '保留飞书台账与归档文件')
                    db.execute("UPDATE tool_workspace_items SET deleted_at=? WHERE session_id=? AND asset_id=?",
                               (clock().isoformat(timespec='seconds'), identity, asset_id))
                    self.audit(db, order['id'] if order else identity, actor, '删除工作台办理记录', asset_id)
                return {'deleted': True, 'batch_hidden': not db.execute(
                        "SELECT 1 FROM tool_workspace_items WHERE session_id=? AND deleted_at='' LIMIT 1",
                        (identity,)).fetchone()}
            if not order or order['submitted_at'] or order['outcome']:
                raise RiskError('工具办理已结束或无权操作。', 409)
            self.check_revision(order, body.get('revision'))
            review = db.execute('SELECT status,payload FROM tool_work_reviews WHERE order_id=?', (order['id'],)).fetchone()
            pending = bool(review and review['status'] == 'pending')
            manager = self.workspace_can_review(db, actor, order)
            owner = self.owns_workspace_item(actor, order) or order['owner'] == actor['id'] and self.eligible(db, actor, order)
            if action == 'reassign':
                if not manager or not pending:
                    raise RiskError('仅本楼楼长或获授权人员可改派待审核记录。', 403)
                self._workspace_check_scopes(db, [item])
                reviewer_id = text(body.get('reviewer_id') or '', 130)
                if reviewer_id == (json.loads(review['payload']).get('reviewer_id') or ''):
                    return self._workspace_detail(db, *self._workspace_session(db, actor, identity, include_files=False), actor)
                reviewer = self.person_for(reviewer_id) if reviewer_id else None
                if reviewer and (reviewer.get('inactive') or not self.workspace_can_review(db, reviewer, order)):
                    raise RiskError('只能改派本楼楼长或获授权审核人。', 403)
                fields = {'reviewer_id': reviewer_id, 'reviewer_name': reviewer['name'] if reviewer else ''}
                draft, payload = json.loads(item['draft']) | fields, json.loads(review['payload']) | fields
                db.execute('UPDATE tool_workspace_items SET draft=? WHERE session_id=? AND asset_id=?',
                           (dumps(draft), identity, asset_id))
                db.execute('UPDATE tool_work_reviews SET payload=? WHERE order_id=?', (dumps(payload), order['id']))
                self._queue_workspace_review_notice(db, order)
                self.audit(db, order['id'], actor, '改派审核人', fields['reviewer_name'] or '由本楼楼长确认')
            elif action == 'withdraw':
                if not pending or not (manager or owner):
                    raise RiskError('当前记录无法撤回审核。', 409)
                self._workspace_check_scopes(db, [item])
                db.execute("UPDATE tool_work_reviews SET status='withdrawn' WHERE order_id=?", (order['id'],))
                self.audit(db, order['id'], actor, '撤回审核并保留草稿')
            elif action == 'release':
                if not (manager or owner):
                    raise RiskError('仅获授权人员可释放原办理。', 403)
                self._workspace_release_order(db, actor, order, identity, text(body.get('note'), 500, True))
                return self._workspace_detail(db, *self._workspace_session(db, actor, identity, include_files=False), actor)
            else:
                raise RiskError('请选择有效的办理操作。')
            db.execute('UPDATE tool_work_orders SET revision=revision+1 WHERE id=?', (order['id'],))
            self._workspace_bump(db, identity)
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity, include_files=False), actor)

    def workspace_download(self, actor, identity):
        self._workspace_access(actor)
        with self.connect() as db:
            db.execute('BEGIN')
            row, items = self._workspace_session(db, actor, identity, include_files=False)
            if row['kind'] == 'external':
                self._check_workspace_report_scope(db, actor, row, items)
            if any(item["order_id"] for item in items):
                orders = [order for order in self._workspace_orders(db, items).values() if order['outcome'] != 'cancelled']
                if not orders or any(not order["submitted_at"] for order in orders):
                    raise RiskError("请先完成工单提交。", 409)
                output = io.BytesIO()
                self.package_work(db, row['created_at'][:7], (db.execute('SELECT * FROM tool_work_orders WHERE id=?',
                                                                        (order['id'],)).fetchone() for order in orders), output)
                return output.getvalue()
            package = db.execute('SELECT package FROM tool_workspace_sessions WHERE id=?', (identity,)).fetchone()[0]
            if not package:
                raise RiskError("请先提交本批工具报告。", 409)
            return package

    def workspace_report_attachment(self, actor, identity):
        self._workspace_access(actor)
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity, status_only=True)
            if row['kind'] != 'external':
                raise RiskError('没有可查看的送检报告。', 404)
            self._check_workspace_report_scope(db, actor, row, items)
            stored = db.execute('SELECT report_name,report FROM tool_workspace_sessions WHERE id=?', (identity,)).fetchone()
            if stored['report']:
                return stored['report_name'], stored['report']
            for item in items:
                order = self._workspace_order(db, item)
                if order and order['report']:
                    return order['report_name'], order['report']
            raise RiskError('没有可查看的送检报告。', 404)

    def _check_workspace_report_scope(self, db, actor, session, visible=None):
        items = self._workspace_rows(db, session['id'], status_only=True)
        visible = self._workspace_visible_items(db, actor, session, items) if visible is None else visible
        if len(items) != len(visible):
            raise RiskError('共用送检报告包含当前账号无权查看的工具，请由有权限的人员办理。', 403)

    def _check_order_report_scope(self, db, actor, order):
        if order['kind'] != 'external':
            return
        linked = db.execute('SELECT session_id FROM tool_workspace_items WHERE order_id=?', (order['id'],)).fetchone()
        if linked:
            session = db.execute('SELECT id,owner FROM tool_workspace_sessions WHERE id=?', (linked['session_id'],)).fetchone()
            if session:
                self._check_workspace_report_scope(db, actor, session)

    @staticmethod
    def _workspace_order_report(db, order, report_cache=None):
        name, content = order['report_name'], order['report']
        if order['kind'] != 'external' or content:
            return '', name, content
        linked = db.execute('SELECT session_id FROM tool_workspace_items WHERE order_id=?', (order['id'],)).fetchone()
        if not linked:
            return '', name, content
        key = linked['session_id']
        if report_cache is None or key not in report_cache:
            row = db.execute('SELECT report_name,report FROM tool_workspace_sessions WHERE id=?', (key,)).fetchone()
            attachment = (row['report_name'], row['report']) if row else (name, content)
            if report_cache is not None:
                report_cache[key] = attachment
        else:
            attachment = report_cache[key]
        return key, *attachment

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
            if item['ledger_table'] != 'unified':
                db.execute("""UPDATE tool_workspace_items SET ledger_table='unified',
                    attempted_at=CASE WHEN ?='external' THEN 0 ELSE attempted_at END
                    WHERE session_id=? AND asset_id=?""", (row['kind'], identity, item['asset_id']))
                item = db.execute("SELECT * FROM tool_workspace_items WHERE session_id=? AND asset_id=?",
                                  (identity, item['asset_id'])).fetchone()
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
                attempted_at=item["attempted_at"], save_attempt=save_attempt, unified=True,
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
