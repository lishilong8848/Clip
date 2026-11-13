"""Background refresh of the original Zhihang Bitable tables."""
from __future__ import annotations

import datetime as dt
import hashlib
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo

import httpx

from . import plan_convergence_auth as auth
from .cabinet_power import CabinetFeishu, _stable_uuid4_client_token, equivalent, text_value

NS = "zhihang_data"
TZ = ZoneInfo("Asia/Shanghai")
BASE = "IrIibPkUOa6udGsMhu2cbOqhnWg"
TASKS = {
    "risk": ("风险排查", ["08:30"], ("tblNddknaoXp2ASG", "vewG4XuaPu")),
    "change": ("变更", ["08:07", "13:08", "15:38"], ("tblqMJvYW5dxFFfU", "vew0UNl8ZR")),
    "drill": ("演练", ["08:33"], ("tbleCzxRd2yF7lT7", "vew0UNl8ZR")),
    "event": ("事件", ["08:37", "13:38", "16:08"], ("tbl06hMoUMdGVEvn", "vew0xMcHto")),
    "inspection": ("巡检", ["07:00", "11:00", "17:00", "23:30"], ("tblTXHrDH4Mv0971", "vewgfEzEZl")),
}


class SyncError(ValueError):
    pass


class Pending(SyncError):
    pass


class ZhihangBitable(CabinetFeishu):
    def list_all(self, path="records"):
        items = []
        seen = set()
        cursor = ""
        total = None
        while True:
            params = {"page_size": 500}
            if cursor:
                params["page_token"] = cursor
            page = self.request("GET", path, params=params)
            if (not isinstance(page, dict) or not isinstance(page.get("items"), list)
                    or type(page.get("has_more")) is not bool):
                raise SyncError("飞书返回的分页信息不完整，保留上次数据。")
            count = page.get("total")
            if type(count) is int:
                if total is not None and count != total:
                    raise SyncError("飞书分页期间原表记录数发生变化，请重试。")
                total = count
            items.extend(page["items"])
            if not page["has_more"]:
                if total is not None and len(items) != total:
                    raise SyncError("飞书分页未读全，保留上次数据。")
                return items
            cursor = page.get("page_token")
            if not isinstance(cursor, str) or not cursor or cursor in seen or not page["items"]:
                raise SyncError("飞书分页游标异常，保留上次数据。")
            seen.add(cursor)


def date_value(value):
    text = str(value).strip()
    if re.fullmatch(r"(?:[0-9]{10}|[0-9]{13})(?:\.[0-9]+)?", text):
        return dt.datetime.fromtimestamp(float(text) / (1000 if len(text.split('.')[0]) == 13 else 1), TZ)
    parsed = dt.datetime.fromisoformat(text.replace("/", "-"))
    return parsed.astimezone(TZ) if parsed.tzinfo else parsed.replace(tzinfo=TZ)


def coerce(fields, schema):
    result = {}
    for name, value in fields.items():
        kind = schema.get(name)
        if kind not in {1, 2, 3, 4, 5, 7, 15}:
            raise SyncError(f"目标表字段缺失或类型不支持：{name}")
        if value is None or value == "":
            result[name] = None
        elif kind == 2:
            result[name] = float(value)
        elif kind == 5:
            result[name] = int(date_value(value).timestamp() * 1000)
        elif kind == 4:
            result[name] = value if isinstance(value, list) else [str(value)]
        elif kind == 7:
            if not isinstance(value, bool):
                raise SyncError(f"复选框字段值无效：{name}")
            result[name] = value
        elif kind == 15:
            result[name] = value if isinstance(value, dict) else {"text": str(value), "link": str(value)}
        else:
            result[name] = text_value(value)
    return result


def matches(expected, actual):
    for name, value in expected.items():
        other = actual.get(name)
        if value == other or value is None and other in ("", [], {}):
            continue
        if isinstance(value, list):
            if sorted(map(str, value)) == sorted(map(str, other or [])):
                continue
            return False
        if isinstance(value, dict) or isinstance(value, bool):
            return False
        if not equivalent({name: value}, actual):
            return False
    return True


def credential_revision(store):
    return hashlib.sha256(auth.load_config(store)["token"].encode()).hexdigest()


def describe_error(exc):
    if isinstance(exc, SyncError):
        return str(exc)
    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (401, 403):
        return "智航认证已失效或当前账号无子系统访问权限。"
    if isinstance(exc, httpx.HTTPError):
        return "数据请求失败，请检查灯塔主机 VPN 或飞书连接。"
    from .zhihang_data_sources import ZhihangSourceError
    if isinstance(exc, ZhihangSourceError):
        return str(exc)
    code = re.search(r"code=(\d+)", str(exc))
    if code:
        return f"飞书多维表请求失败（{code[1]}），请核对当前应用权限或稍后重试。"
    if isinstance(exc, ValueError):
        message = str(exc)
        if message.startswith(("请管理员点击登录智航", "智航登录已过期", "智航认证", "上一认证窗口", "未找到 Edge")):
            return message[:160]
        return "来源数据格式或提交参数校验失败。"
    return "任务执行失败，请查看后台记录。"


class ZhihangSync:
    def __init__(self, store, *, cloud=None, pull=None, notify=None):
        self.store = store
        self.cloud = cloud or (lambda table: ZhihangBitable(table, BASE))
        self.pull = pull or self._pull
        self.notify = notify
        self.lock = threading.RLock()
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ZhihangData")
        self.worker = None
        self.closing = threading.Event()

    @staticmethod
    def _pull(kind, now):
        from .zhihang_data_sources import pull
        return pull(kind, now)

    def close(self):
        self.closing.set()
        self.executor.shutdown(wait=False, cancel_futures=True)

    def read(self, key):
        return self.store.get_document(NS, key) or {}

    def save(self, key, value):
        self.store.put_document(NS, key, value)

    def settings(self, kind):
        if kind not in TASKS:
            raise SyncError("未知智航数据类型。")
        return {"enabled": False, "confirmed": False, "notify": False, "times": TASKS[kind][1],
                **self.read("settings:" + kind)}

    def configure(self, kind, payload):
        with self.lock:
            before = self.settings(kind)
            times = payload.get("times", before["times"])
            if not isinstance(times, list) or not 1 <= len(times) <= 24 or any(
                not isinstance(value, str) or not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value)
                for value in times
            ):
                raise SyncError("定时请填写 1 至 24 个北京时间，格式为 HH:MM。")
            value = dict(before, times=sorted(set(times)))
            for key in ("enabled", "confirmed", "notify"):
                if key in payload:
                    if type(payload[key]) is not bool:
                        raise SyncError("开关必须为布尔值。")
                    value[key] = payload[key]
            if value["enabled"] and not value["confirmed"]:
                raise SyncError("请确认旧工具已停用后再启用灯塔定时同步。")
            if any(value[key] != before[key] for key in ("enabled", "confirmed", "times")):
                value["changed_at"] = dt.datetime.now(TZ).isoformat()
            self.save("settings:" + kind, value)
            return value

    @staticmethod
    def public(run):
        keys = ("id", "kind", "status", "mode", "created_at", "finished_at", "error",
                "counts", "notification", "preview_id", "slot")
        return {key: run[key] for key in keys if key in run}

    def history(self):
        return sorted((doc["payload"] for doc in self.store.list_documents(NS, key_prefix="run:")),
                      key=lambda run: run["created_at"], reverse=True)

    def prune(self):
        counts = {}
        for run in self.history():
            if run["status"] not in {"success", "failed"}:
                continue
            key = (run["kind"], run["mode"])
            counts[key] = counts.get(key, 0) + 1
            if counts[key] <= 20:
                continue
            for prefix in ("rows:", "journal:", "card:", "run:"):
                self.store.delete_document(NS, prefix + run["id"])

    def overview(self):
        from .zhihang_data_cards import notification_targets

        history = self.history()
        now = dt.datetime.now(TZ)
        tasks = []
        for kind, (label, _, (table, view)) in TASKS.items():
            cfg = self.settings(kind)
            runs = [run for run in history if run["kind"] == kind]
            next_at = min((slot for slot in self.slots(cfg, now) if slot > now), default=None)
            tasks.append({"kind": kind, "label": label, **cfg,
                          "targets": [{"label": f"智航{label}原表", "url": f"https://vnet.feishu.cn/base/{BASE}?table={table}&view={view}"}],
                          "notification_targets": notification_targets(kind),
                          "latest": self.public(runs[0]) if runs else None,
                          "last_preview": next((self.public(r) for r in runs if r["mode"] == "preview" and r["status"] == "success"), None),
                          "last_sync": next((self.public(r) for r in runs if r["mode"] in {"sync", "scheduled"} and r["status"] == "success"), None),
                          "next_at": next_at.isoformat() if cfg["enabled"] and next_at else ""})
        return {"auth": auth.settings_view(self.store), "tasks": tasks,
                "history": [self.public(run) for run in history[:50]]}

    def detail(self, run_id, page=1):
        if not re.fullmatch(r"[a-f0-9]{32}", run_id):
            raise SyncError("无效的执行编号。")
        run = self.read("run:" + run_id)
        if not run:
            raise FileNotFoundError("执行记录不存在。")
        rows = list(self.read("rows:" + run_id).values())
        start = (page - 1) * 50
        return dict(self.public(run), rows=rows[start:start + 50], total=len(rows), page=page, page_size=50)

    @staticmethod
    def slots(cfg, now):
        for shift in (-1, 0, 1):
            day = now.date() + dt.timedelta(days=shift)
            for value in cfg["times"]:
                yield dt.datetime.combine(day, dt.time.fromisoformat(value), TZ)

    def enqueue(self, kind, mode, preview_id="", slot=""):
        with self.lock:
            cfg = self.settings(kind)
            if mode != "preview" and not cfg["confirmed"]:
                raise SyncError("请确认旧工具已停用后再同步原表。")
            if mode == "sync":
                if not re.fullmatch(r"[a-f0-9]{32}", preview_id):
                    raise SyncError("请先完成本类数据的拉取预览。")
                preview = self.read("run:" + preview_id)
                if (preview.get("kind"), preview.get("mode"), preview.get("status")) != (kind, "preview", "success"):
                    raise SyncError("请先完成本类数据的拉取预览。")
                if dt.datetime.now(TZ) - date_value(preview["created_at"]) > dt.timedelta(hours=1):
                    raise SyncError("预览已过期，请重新拉取。")
            for old in self.history():
                if old["kind"] != kind:
                    continue
                if (slot and old.get("slot") == slot) or (preview_id and old.get("preview_id") == preview_id):
                    if old["status"] != "failed":
                        return self.public(old)
                    if slot:
                        return self.public(old)
                if old["status"] in {"queued", "running", "pending_verify"}:
                    if old["mode"] == mode:
                        return self.public(old)
                    if mode == "preview" and old["status"] == "pending_verify":
                        continue
                    raise SyncError("该类数据已有执行或待核验任务。")
            run = {"id": uuid.uuid4().hex, "kind": kind, "mode": mode, "status": "queued",
                   "preview_id": preview_id, "slot": slot, "created_at": dt.datetime.now(TZ).isoformat()}
            self.persist(run)
            self.kick()
            return self.public(run)

    def persist(self, run):
        self.save("run:" + run["id"], run)

    def kick(self):
        with self.lock:
            if not self.closing.is_set() and (self.worker is None or self.worker.done()):
                self.worker = self.executor.submit(self.drain)

    def tick(self):
        now = dt.datetime.now(TZ)
        for kind in TASKS:
            cfg = self.settings(kind)
            if not cfg["enabled"] or not cfg["confirmed"]:
                continue
            paused = self.read("pause:" + kind)
            if paused and paused.get("revision") == credential_revision(self.store):
                continue
            eligible = [slot for slot in self.slots(cfg, now)
                        if cfg.get("changed_at", "") <= slot.isoformat() and slot <= now]
            if eligible:
                try:
                    self.enqueue(kind, "scheduled", slot=max(eligible).isoformat())
                except SyncError:
                    pass  # A previous batch is still active or waiting for read-back.
        self.kick()

    def drain(self):
        from .zhihang_data_sources import ZhihangSourceError

        while True:
            with self.lock:
                now = dt.datetime.now(TZ).isoformat()
                run = None if self.closing.is_set() else next((
                    item for item in reversed(self.history())
                    if item["status"] in {"queued", "running", "pending_verify"}
                    and item.get("next_attempt", "") <= now
                    and (item["mode"] == "preview" or self.settings(item["kind"])["confirmed"])
                ), None)
                if run is None:
                    self.worker = None
                    return
                run.update(status="running", error="")
                run["attempts"] = run.get("attempts", 0) + 1
                self.persist(run)
            try:
                self.execute(run)
                run.update(status="success", finished_at=dt.datetime.now(TZ).isoformat())
                self.store.delete_document(NS, "pause:" + run["kind"])
            except Exception as exc:
                has_journal = bool(self.read("journal:" + run["id"]))
                transient = (isinstance(exc, httpx.TransportError) or
                             isinstance(exc, ZhihangSourceError) and exc.code == "source_unavailable")
                retry = transient and run["mode"] == "scheduled" and run["attempts"] < 3
                run.update(status="pending_verify" if has_journal else "queued" if retry else "failed",
                           error=describe_error(exc))
                if run["status"] == "failed":
                    run["finished_at"] = dt.datetime.now(TZ).isoformat()
                else:
                    run["next_attempt"] = (dt.datetime.now(TZ) + dt.timedelta(minutes=2)).isoformat()
                if isinstance(exc, ZhihangSourceError) and exc.code in {"auth_required", "access_denied"}:
                    self.save("pause:" + run["kind"], {"revision": credential_revision(self.store)})
            with self.lock:
                self.persist(run)
                self.prune()

    def execute(self, run):
        from . import zhihang_data_sources as source

        kind = run["kind"]
        target_kind = "change_basic" if kind == "change" else kind
        reference = date_value(run["created_at"])
        client = self.cloud(TASKS[kind][2][0])
        row_key = "rows:" + run["id"]
        rows = self.read(row_key)
        if not rows:
            if run["preview_id"]:
                preview = self.read("run:" + run["preview_id"])
                rows = self.read("rows:" + preview["id"])
                reference = date_value(preview["created_at"])
            else:
                batches = self.pull(kind, reference)
                rows = batches.get(target_kind, {})
                if kind == "change" and set(rows) != set(batches.get("change_workorder", {})):
                    raise SyncError("变更工单及基础信息数量不一致，保留上次数据。")
                # The original configuration points both change feeds at the same table.
                # Its scheduled publisher writes the enriched basic fields once per work order.
            if not rows:
                raise SyncError("本轮没有可核验的完整记录，保留上次数据。")
            run["reference_at"] = reference.isoformat()
            self.store.put_documents(NS, {"run:" + run["id"]: run, row_key: rows})
        reference = date_value(run.get("reference_at") or run["created_at"])
        remote = {}
        for item in client.list_all():
            if item["record_id"] in remote:
                raise SyncError("飞书分页返回重复记录，请重试。")
            remote[item["record_id"]] = item["fields"]
        saved = self.read("journal:" + run["id"])
        if not saved:
            schema = {field["field_name"]: field["type"] for field in client.list_all("fields")}
            journal, counts, expected = self.diff(kind, target_kind, rows, remote, schema, reference, source)
            run["counts"] = counts
            if run["mode"] == "preview":
                return
            saved = {"items": journal, "expected": expected}
            self.store.put_documents(NS, {"run:" + run["id"]: run, "journal:" + run["id"]: saved})
        self.publish(run, client, remote, saved, target_kind)
        self.verify_batch(client, target_kind, saved["expected"])
        if self.settings(kind)["notify"] and self.notify and not run.get("notification"):
            try:
                run["notification"] = self.notify(kind, rows, run.get("slot", ""), run["id"])
            except Exception as exc:
                run["notification"] = {"status": "failed", "error": describe_error(exc)}
            self.persist(run)

    def diff(self, kind, target_kind, rows, remote, schema, reference, source):
        managed = self.managed(kind)
        by_key = {}
        for record_id, fields in remote.items():
            key = source.identity(target_kind, fields)
            if key:
                by_key.setdefault(key, []).append(record_id)
        journal = []
        counts = {"total": len(rows), "create": 0, "update": 0, "delete": 0, "unchanged": 0, "unmanaged": 0}
        expected = {}
        for raw in rows.values():
            key = source.identity(target_kind, raw)
            if not key or key in expected or len(by_key.get(key, [])) > 1:
                raise SyncError("源数据或原表存在缺失、重复的业务键，请先核对。")
            fields = coerce(raw, schema)
            expected[key] = fields
            record_id = (by_key.get(key) or [""])[0]
            if record_id and matches(fields, remote[record_id]):
                counts["unchanged"] += 1
                continue
            action = "update" if record_id else "create"
            counts[action] += 1
            journal.append({"action": action, "id": record_id, "key": key, "fields": fields,
                            "before": remote[record_id] if record_id else {}})
        for record_id, owned in managed.items():
            fields = remote.get(record_id)
            if (fields and owned["key"] not in expected and fields == owned["fields"]
                    and source.in_refresh_scope(target_kind, fields, reference)):
                journal.append({"action": "delete", "id": record_id, "key": owned["key"], "before": fields})
                counts["delete"] += 1
        counts["unmanaged"] = sum(
            1 for record_id, fields in remote.items()
            if record_id not in managed and source.identity(target_kind, fields) not in expected
            and source.in_refresh_scope(target_kind, fields, reference)
        )
        return journal, counts, expected

    def managed(self, kind):
        prefix = "owned:" + kind + ":"
        return {doc["key"].removeprefix(prefix): doc["payload"]
                for doc in self.store.list_documents(NS, key_prefix=prefix) if doc["payload"]}

    def publish(self, run, client, remote, journal, target_kind):
        from .zhihang_data_sources import identity

        kind = run["kind"]
        by_key = {}
        for record_id, fields in remote.items():
            by_key.setdefault(identity(target_kind, fields), []).append(record_id)
        verified = False
        for index, item in enumerate(journal["items"]):
            if self.closing.is_set() or not self.settings(kind)["confirmed"]:
                raise Pending("同步已暂停，保留已受理批次等待核验。")
            if item.get("done"):
                continue
            action, record_id = item["action"], item["id"]
            ownership_key = "owned:" + kind + ":" + record_id
            if action == "delete" and not verified:
                self.verify_batch(client, target_kind, journal["expected"])
                verified = True
            current = remote.get(record_id)
            fields = item.get("fields", {})
            if action == "create":
                candidates = by_key.get(item["key"], [])
                if len(candidates) > 1 or (candidates and not matches(fields, remote[candidates[0]])):
                    raise Pending("目标表已有同键冲突记录，请核对后重试。")
                if record_id and candidates and record_id != candidates[0]:
                    raise Pending("目标表同键记录与新增回执不一致，请核对后重试。")
                record_id = candidates[0] if candidates else record_id
                if not record_id:
                    token = _stable_uuid4_client_token(f"zhihang:{run['id']}:{index}")
                    if not item.get("attempted_at"):
                        item["attempted_at"] = dt.datetime.now(TZ).isoformat()
                        self.save("journal:" + run["id"], journal)
                    elif dt.datetime.now(TZ) - date_value(item["attempted_at"]) > dt.timedelta(minutes=10):
                        raise Pending("新增回执仍未确认，请核对原表；已暂停重复写入。")
                    created = client.request("POST", "records", {"fields": fields}, params={"client_token": token})
                    record_id = created["record"]["record_id"]
                    item["id"] = record_id
                    self.save("journal:" + run["id"], journal)
            elif action == "update" and not matches(fields, current or {}):
                if current is None or item["before"] != client.get(record_id)["fields"]:
                    raise Pending("原表数据在同步期间被修改，请核对冲突。")
                client.update(record_id, fields)
            elif action == "delete" and current is not None:
                if item["before"] != client.get(record_id)["fields"]:
                    raise Pending("待清理记录已被修改，保留原表记录。")
                client.delete(record_id)
                if any(row["record_id"] == record_id for row in client.batch_get([record_id])):
                    raise Pending("清理结果等待飞书回读。")
            if action != "delete":
                actual = client.get(record_id)["fields"]
                if not matches(fields, actual):
                    raise Pending("飞书已受理写入，字段回读尚未一致。")
                ownership_key = "owned:" + kind + ":" + record_id
                owned = self.read(ownership_key)
                if (action == "create" and item["id"] == record_id) or (
                    owned and owned["fields"] == item["before"]
                ):
                    self.save(ownership_key, {"key": item["key"], "fields": actual})
            elif self.read(ownership_key):
                self.store.delete_document(NS, ownership_key)
            item["done"] = True
            self.save("journal:" + run["id"], journal)

    @staticmethod
    def verify_batch(client, kind, expected):
        from .zhihang_data_sources import identity

        seen = set()
        for row in client.list_all():
            fields = row["fields"]
            key = identity(kind, fields)
            if key in expected:
                if key in seen or not matches(expected[key], fields):
                    raise Pending("目标表本批数据尚未完整读回，保留待核验任务。")
                seen.add(key)
        if seen != expected.keys():
            raise Pending("目标表本批数据尚未完整读回，保留待核验任务。")
