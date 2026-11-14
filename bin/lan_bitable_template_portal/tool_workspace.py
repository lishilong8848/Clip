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
EDIT_FIELDS = ("values", "checks", "reviewer", "reviewed_at", "signature", "qualified")


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

    def _workspace_access(self, actor):
        self.access(actor)
        if not self.available_to(actor):
            raise RiskError("当前账号没有工具管理权限。", 403)

    @staticmethod
    def _workspace_kind(asset):
        if asset.get("retired") or (asset.get("quantity") or 0) <= 0:
            return "", "工具已停用、报废或数量无效。"
        self_cycle, external_cycle = bool(asset.get("self_cycle")), bool(asset.get("external_cycle"))
        if self_cycle == external_cycle:
            return "", "同时配置了维护和送检周期，请核对。" if self_cycle else "尚未配置维护或送检周期。"
        return ("self" if self_cycle else "external"), ""

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
            "id", "name", "scope", "model", "brand", "location", "template_id", "self_cycle", "external_cycle",
        )}

    def _workspace_visible(self, actor, session, items):
        from .tool_management import can_scope

        return (session["owner"] == actor["id"] or self.administrator(actor)) and all(
            can_scope(actor, json.loads(item["asset"])["scope"]) for item in items
        )

    @staticmethod
    def _workspace_rows(db, identity):
        return db.execute(
            "SELECT * FROM tool_workspace_items WHERE session_id=? ORDER BY position", (identity,),
        ).fetchall()

    def _workspace_session(self, db, actor, identity):
        row = db.execute("SELECT * FROM tool_workspace_sessions WHERE id=?", (identity,)).fetchone()
        items = self._workspace_rows(db, identity) if row else []
        if not row or not self._workspace_visible(actor, row, items):
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
                if kind and not self._workspace_allowed(db, actor, asset, kind):
                    reason = "当前账号无权办理此工具。"
                assets.append(self._workspace_asset(asset) | {
                    "kind": kind, "eligible_kinds": [kind] if kind and not reason else [],
                    "selectable": bool(kind and not reason), "disabled_reason": reason,
                })
            sessions = []
            for row in db.execute("""
                SELECT * FROM tool_workspace_sessions
                WHERE (owner=? OR ?) ORDER BY updated_at DESC,id
            """, (actor["id"], self.administrator(actor))):
                items = self._workspace_rows(db, row["id"])
                if self._workspace_visible(actor, row, items):
                    sessions.append(self._workspace_summary(row, items))
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
            return self._workspace_detail(db, row, items)

    def workspace_resume(self, actor, identity):
        self._workspace_access(actor)
        with self._workspace_open_lock:
            with self.connect() as db:
                row, items = self._workspace_session(db, actor, identity)
                if row['status'] != 'opening':
                    return self._workspace_detail(db, row, items)
                self._workspace_assets_for_open(db, actor, [item['asset_id'] for item in items], row['kind'])
                self._workspace_check_scopes(db, items)
            self._workspace_apply_bindings(actor, identity)
            with self.connect(True) as db:
                db.execute("UPDATE tool_workspace_sessions SET status='draft' WHERE id=? AND status='opening'", (identity,))
                return self._workspace_detail(db, *self._workspace_session(db, actor, identity))

    def _workspace_detail(self, db, row, items):
        layouts = {}
        result = []
        for item in items:
            key = item["template_key"]
            if key and key not in layouts:
                layouts[key] = json.loads(db.execute(
                    "SELECT layout FROM tool_workspace_templates WHERE cache_key=?", (key,),
                ).fetchone()[0])
            result.append({
                "id": item["asset_id"], "asset_id": item["asset_id"], "asset": json.loads(item["asset"]),
                "template_id": item["template_id"], "template_version": item["template_version"],
                "template": layouts.get(key), **json.loads(item["draft"]),
                "archive_status": (
                    "archived" if item["remote_id"] else "verifying" if item["attempted_at"]
                    else "retrying" if item["archive_error"] else row["status"]
                ),
                "archive_error": item["archive_error"], "remote_id": item["remote_id"],
            })
        return self._workspace_summary(row, items) | {"items": result}

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
            assets.append(self._workspace_asset(asset))
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
        fingerprint = digest([kind, sorted(asset_ids), template_id])
        with self._workspace_open_lock:
            with self.connect() as db:
                existing = db.execute(
                    "SELECT * FROM tool_workspace_sessions WHERE owner=? AND operation=?", (actor["id"], operation),
                ).fetchone()
                if existing:
                    if existing["fingerprint"] != fingerprint:
                        raise RiskError("同一操作编号已用于其他工具或模板。", 409)
                    row, items = self._workspace_session(db, actor, existing["id"])
                    if row["status"] != "opening":
                        return self._workspace_detail(db, row, items)
                assets = self._workspace_assets_for_open(db, actor, asset_ids, kind)
            if existing:
                identity = existing["id"]
            else:
                identity = self._workspace_create_session(actor, operation, fingerprint, kind, assets, template_id)
            if template_id:
                self._workspace_apply_bindings(actor, identity)
            with self.connect(True) as db:
                row, items = self._workspace_session(db, actor, identity)
                if row["status"] == "opening":
                    db.execute("UPDATE tool_workspace_sessions SET status='draft' WHERE id=?", (identity,))
                return self._workspace_detail(db, *self._workspace_session(db, actor, identity))

    def _workspace_create_session(self, actor, operation, fingerprint, kind, assets, template_id):
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
            db.execute("""INSERT INTO tool_workspace_sessions
                (id,owner,owner_name,operation,fingerprint,kind,status,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?)""",
                       (identity, actor["id"], actor["name"], operation, fingerprint, kind, "opening", now, now))
            db.executemany("""INSERT INTO tool_workspace_items
                (session_id,asset_id,position,asset,template_id,template_version,template_key,previous_template,draft)
                VALUES(?,?,?,?,?,?,?,?,?)""", rows)
        return identity

    def _workspace_apply_bindings(self, actor, identity):
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity)
            if row["status"] != "opening":
                return
        for item in items:
            template_id = item['template_id']
            previous = item["previous_template"]
            if not template_id or previous == template_id:
                continue
            asset_id = item["asset_id"]
            if self.cloud.bind_template(asset_id, template_id, previous) != template_id:
                raise RiskError("飞书未确认模板关联，请重试原操作。", 503)
            with self.connect(True) as db:
                current = db.execute("SELECT source FROM tool_inventory WHERE id=? AND active=1", (asset_id,)).fetchone()
                if current:
                    source = json.loads(current["source"])
                    if (source.get("template_id") or "") == previous:
                        source["template_id"] = template_id
                        db.execute("UPDATE tool_inventory SET source=? WHERE id=?", (dumps(source), asset_id))

    @staticmethod
    def _workspace_draft(row, revision):
        if type(revision) is not int or revision != row["revision"]:
            raise RiskError("办理会话已更新，请刷新后核对再保存。", 409)
        if row["status"] != "draft":
            raise RiskError("此批次已提交，表格与报告已锁定。", 409)

    def workspace_save(self, actor, identity, body):
        from .tool_workspace_workbook import validate_inputs, validate_signature

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
                draft = json.loads(item["draft"])
                draft.update({name: patch[name] for name in EDIT_FIELDS if name in patch})
                draft["reviewer"] = text(draft["reviewer"], 120)
                draft["reviewed_at"] = self._workspace_review_time(draft["reviewed_at"])
                if draft["signature"]:
                    validate_signature(draft["signature"])
                elif draft["signature"] != "":
                    raise RiskError("签名格式无效。")
                if draft["qualified"] is not None and type(draft["qualified"]) is not bool:
                    raise RiskError("请选择工具检测结果。")
                key = item["template_key"]
                if key not in contents:
                    contents[key] = db.execute(
                        "SELECT content FROM tool_workspace_templates WHERE cache_key=?", (key,),
                    ).fetchone()[0]
                validate_inputs(contents[key], draft["values"], draft["checks"], expected_sha256=item["template_version"])
                self._workspace_result(draft)
                updates[asset_id] = dumps(draft)
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            self._workspace_draft(row, body.get("revision"))
            db.executemany("UPDATE tool_workspace_items SET draft=? WHERE session_id=? AND asset_id=?",
                           [(draft, identity, asset_id) for asset_id, draft in updates.items()])
            self._workspace_bump(db, identity)
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity))

    @staticmethod
    def _workspace_bump(db, identity):
        now = dt.datetime.now(TZ).isoformat(timespec="seconds")
        db.execute("UPDATE tool_workspace_sessions SET revision=revision+1,updated_at=? WHERE id=?", (now, identity))

    @staticmethod
    def _workspace_review_time(value):
        value = timestamp(value)
        if value and dt.datetime.fromisoformat(value).replace(tzinfo=TZ) > dt.datetime.now(TZ):
            raise RiskError("审核时间不能晚于当前时间。")
        return value

    @staticmethod
    def _workspace_result(draft):
        if draft["qualified"] is True and "否" in draft["checks"].values():
            raise RiskError("检查项存在「否」，检测结果不能选「合格」。")

    def workspace_report(self, actor, identity, revision, name, data):
        from .tool_management import report_file

        self._workspace_access(actor)
        name = report_file(name, data)
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            if row["kind"] != "external":
                raise RiskError("维护报告由各工具的自检表生成。")
            self._workspace_draft(row, revision)
            if row["report_name"] != name or row["report"] != data:
                db.execute("UPDATE tool_workspace_sessions SET report_name=?,report=? WHERE id=?", (name, data, identity))
                self._workspace_bump(db, identity)
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity))

    def workspace_submit(self, actor, identity, body):
        self._workspace_access(actor)
        revision = body.get("revision")
        with self.connect() as db:
            row, items = self._workspace_session(db, actor, identity)
            if row["status"] in ("pending", "archived") and type(revision) is int and revision == row["submitted_revision"]:
                return self._workspace_detail(db, row, items)
            self._workspace_draft(row, revision)
            self._workspace_assets_for_open(db, actor, [item["asset_id"] for item in items], row["kind"])
            self._workspace_check_scopes(db, items)
            contents = {key: db.execute("SELECT content FROM tool_workspace_templates WHERE cache_key=?", (key,)).fetchone()[0]
                        for key in {item["template_key"] for item in items if item["template_key"]}}
        files = self._workspace_reports(row, items, contents)
        output = io.BytesIO()
        with ZipFile(output, "w", ZIP_DEFLATED) as package:
            for _, name, content in files:
                package.writestr(name, content)
        bundle = output.getvalue()
        from .tool_management import MAX_REPORT
        if any(len(content) > MAX_REPORT for _, _, content in files):
            raise RiskError("单份归档报告超过 12 MB。", 413)
        now = dt.datetime.now(TZ).isoformat(timespec="seconds")
        with self.connect(True) as db:
            row, items = self._workspace_session(db, actor, identity)
            if row["status"] in ("pending", "archived") and revision == row["submitted_revision"]:
                return self._workspace_detail(db, row, items)
            self._workspace_draft(row, revision)
            self._workspace_assets_for_open(db, actor, [item["asset_id"] for item in items], row["kind"])
            self._workspace_check_scopes(db, items)
            if row["kind"] == "self":
                db.executemany("""UPDATE tool_workspace_items SET report_name=?,report=?
                    WHERE session_id=? AND asset_id=?""",
                    [(name, content, identity, asset_id) for asset_id, name, content in files])
            db.execute("""UPDATE tool_workspace_sessions SET status='pending',submitted_revision=?,submitted_at=?,
                package=? WHERE id=?""", (revision, now, bundle, identity))
            self._workspace_bump(db, identity)
            return self._workspace_detail(db, *self._workspace_session(db, actor, identity))

    def _workspace_check_scopes(self, db, items):
        for item in items:
            current = db.execute('SELECT * FROM tool_inventory WHERE id=? AND active=1',
                                 (item['asset_id'],)).fetchone()
            if not current or self.effective(current)['scope'] != json.loads(item['asset'])['scope']:
                raise RiskError('工具归属楼栋已变化，请重新打开填写核对。', 409)

    def _workspace_reports(self, row, items, contents):
        if row["kind"] == "external":
            if not row["report"]:
                raise RiskError("请先上传送检报告。")
            return [("", row["report_name"], row["report"])]
        from .tool_workspace_workbook import fill

        files = []
        for index, item in enumerate(items, 1):
            asset = json.loads(item["asset"])
            draft = json.loads(item["draft"])
            if type(draft["qualified"]) is not bool or not all(
                draft[key] for key in ("reviewer", "reviewed_at", "signature")
            ):
                raise RiskError(f"请补齐「{asset['name']}」的审核人、时间、签名及检测结果。")
            self._workspace_review_time(draft["reviewed_at"])
            self._workspace_result(draft)
            content = fill(contents[item["template_key"]], draft["values"], draft["reviewer"],
                           draft["reviewed_at"], draft["signature"], draft["checks"], asset,
                           expected_sha256=item["template_version"])
            name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", asset["name"])[:100].strip(". ") or "工具"
            files.append((item["asset_id"], f"{index:02d}-{name}-自检记录单.xlsx", content))
        return files

    def workspace_download(self, actor, identity):
        self._workspace_access(actor)
        with self.connect() as db:
            row, _ = self._workspace_session(db, actor, identity)
            if not row["package"]:
                raise RiskError("请先提交本批工具报告。", 409)
            return row["package"]

    def workspace_sync_one(self):
        """Archive one tool at a time; the session lease serializes shared file upload."""
        now, lease = int(time.time()), uuid.uuid4().hex
        with self.connect(True) as db:
            item = db.execute("""SELECT item.* FROM tool_workspace_items item
                JOIN tool_workspace_sessions session ON session.id=item.session_id
                WHERE session.status='pending' AND session.lease_until<=?
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
