"""Direct and scheduled refresh of the original Zhihang Bitable tables."""
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
HISTORY_PAGE_SIZE = 10
UPLOAD_BATCH_SIZE = 100
ACTIVE_STATUSES = {"queued", "running", "pending_verify", "retry_wait"}
BASE = "IrIibPkUOa6udGsMhu2cbOqhnWg"
TASKS = {
    "risk": ("风险排查", ["08:30"], ("tblNddknaoXp2ASG", "vewG4XuaPu")),
    "change": ("变更", ["08:07", "13:08", "15:38"], ("tblqMJvYW5dxFFfU", "vew0UNl8ZR")),
    "drill": ("演练", ["08:33"], ("tbleCzxRd2yF7lT7", "vew0UNl8ZR")),
    "event": ("事件", ["08:37", "13:38", "16:08"], ("tbl06hMoUMdGVEvn", "vew0xMcHto")),
    "inspection": ("巡检", ["07:00", "11:00", "17:00", "23:30"], ("tblTXHrDH4Mv0971", "vewgfEzEZl")),
}
PULL_SCOPE = {"risk": "本月 A–E 楼排查明细", "change": "配置数据中心内的全部可见工单",
              "drill": "上月及本月演练计划", "event": "近 30 天创建或发生的事件",
              "inspection": "含今天的近 3 天巡检工单"}


class SyncError(ValueError):
    pass


class SyncBusy(SyncError):
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


_INSPECTION_ROW_METADATA = {"序号", "自动编码", "创建时间"}


def _inspection_business_fields(fields):
    return {name: value for name, value in fields.items() if name not in _INSPECTION_ROW_METADATA}


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
    namespace = NS
    tasks = TASKS

    def __init__(self, store, *, cloud=None, pull=None, notify=None):
        self.store = store
        self.cloud = cloud or (lambda table: ZhihangBitable(table, BASE))
        self.pull = pull
        self.notify = notify
        self.digests = None
        self.lock = threading.RLock()
        self.executor = ThreadPoolExecutor(max_workers=len({entry[2][0] for entry in self.tasks.values()}),
                                           thread_name_prefix="ZhihangData")
        self.active = {}
        self.closing = threading.Event()

    @staticmethod
    def _pull(kind, now, **options):
        from .zhihang_data_sources import pull
        return pull(kind, now, **options)

    def close(self):
        with self.lock:
            self.closing.set()
        if self.digests is not None:
            self.digests.close()
        self.executor.shutdown(wait=False, cancel_futures=True)

    def read(self, key):
        return self.store.get_document(self.namespace, key) or {}

    def save(self, key, value):
        self.store.put_document(self.namespace, key, value)

    def settings(self, kind):
        if kind not in self.tasks:
            raise SyncError("未知智航数据类型。")
        return {"enabled": False, "confirmed": False, "notify": False, "times": self.tasks[kind][1],
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
        return sorted((doc["payload"] for doc in self.store.list_documents(self.namespace, key_prefix="run:")),
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
                self.store.delete_document(self.namespace, prefix + run["id"])

    def overview(self, *, history_page=None):
        from .zhihang_data_cards import notification_targets

        history = self.history()
        now = dt.datetime.now(TZ)
        tasks = []
        for kind, (label, _, (table, view)) in self.tasks.items():
            cfg = self.settings(kind)
            runs = [run for run in history if run["kind"] == kind]
            next_at = min((slot for slot in self.slots(cfg, now) if slot > now), default=None)
            tasks.append({"kind": kind, "label": label, **cfg,
                          "pull_scope": PULL_SCOPE.get(kind, ""),
                          "targets": [{"label": f"智航{label}原表", "url": f"https://vnet.feishu.cn/base/{BASE}?table={table}&view={view}"}],
                          "notification_targets": notification_targets(kind),
                          "latest": self.public(runs[0]) if runs else None,
                          "last_preview": next((self.public(r) for r in runs if r["mode"] == "preview" and r["status"] == "success"), None),
                          "last_sync": next((self.public(r) for r in runs if r["mode"] in {"sync", "scheduled"} and r["status"] == "success"), None),
                          "next_at": next_at.isoformat() if cfg["enabled"] and next_at else ""})
        result = {"auth": auth.settings_view(self.store), "tasks": tasks}
        if self.digests is not None:
            result["notification_digests"] = self.digests.status()
        if history_page is None:
            result["history"] = [self.public(run) for run in history[:50]]
            return result
        total = len(history)
        last_page = max(1, (total + HISTORY_PAGE_SIZE - 1) // HISTORY_PAGE_SIZE)
        page = min(max(1, history_page), last_page)
        start = (page - 1) * HISTORY_PAGE_SIZE
        result["history"] = [self.public(run) for run in history[start:start + HISTORY_PAGE_SIZE]]
        result["history_pagination"] = {"page": page, "page_size": HISTORY_PAGE_SIZE, "total": total}
        return result

    def detail(self, run_id, page=1):
        if not re.fullmatch(r"[a-f0-9]{32}", run_id):
            raise SyncError("无效的执行编号。")
        run = self.read("run:" + run_id)
        if not run:
            raise FileNotFoundError("执行记录不存在。")
        rows = list(self.read("rows:" + run_id).values())
        start = (page - 1) * 50
        return dict(self.public(run), stage=run.get("stage"), checks=run.get("checks", {}),
                    rows=rows[start:start + 50], total=len(rows), page=page, page_size=50)

    @staticmethod
    def slots(cfg, now):
        for shift in (-1, 0, 1):
            day = now.date() + dt.timedelta(days=shift)
            for value in cfg["times"]:
                yield dt.datetime.combine(day, dt.time.fromisoformat(value), TZ)

    def enqueue(self, kind, mode, preview_id="", slot=""):
        with self.lock:
            run = self.find_run(kind, mode, preview_id, slot)
            if run is not None:
                return self.public(run)
            if self.tasks[kind][2][0] in self.active:
                raise SyncBusy("该原表正在处理另一批数据，请查看执行记录。")
            run = self.new_run(kind, mode, preview_id, slot)
            self.submit_run(run)
            return self.public(run)

    def find_run(self, kind, mode, preview_id, slot):
        if self.closing.is_set():
            raise SyncBusy("智航数据服务正在关闭，请稍后重试。")
        cfg = self.settings(kind)
        if mode != "preview" and not cfg["confirmed"]:
            raise SyncError("请确认旧工具已停用后再同步原表。")
        history = [run for run in self.history() if run["kind"] == kind]
        if slot:
            existing = next((run for run in history if run.get("slot") == slot), None)
            if existing:
                return existing
        if mode == "sync":
            if not re.fullmatch(r"[a-f0-9]{32}", preview_id):
                raise SyncError("请先完成本类数据的拉取预览。")
            existing = next((run for run in history if run.get("preview_id") == preview_id
                             and run["status"] != "failed"), None)
            if existing:
                return existing
            preview = self.read("run:" + preview_id)
            if (preview.get("kind"), preview.get("mode"), preview.get("status")) != (kind, "preview", "success"):
                raise SyncError("请先完成本类数据的拉取预览。")
            if dt.datetime.now(TZ) - date_value(preview["created_at"]) > dt.timedelta(hours=1):
                raise SyncError("预览已过期，请重新拉取。")
        for old in history:
            if old["status"] not in ACTIVE_STATUSES:
                continue
            if old["mode"] == mode and mode != "sync":
                return old
            if mode == "preview" and old["status"] in {"pending_verify", "retry_wait"}:
                continue
            raise SyncBusy("该类数据已有执行或待核验任务，请查看执行记录。")
        return None

    @staticmethod
    def new_run(kind, mode, preview_id, slot=""):
        return {"id": uuid.uuid4().hex, "kind": kind, "mode": mode, "preview_id": preview_id,
                "slot": slot, "created_at": dt.datetime.now(TZ).isoformat()}

    def start_now(self, kind, mode, preview_id=""):
        """Claim the target immediately. An in-flight duplicate shares its receipt."""
        with self.lock:
            run = self.find_run(kind, mode, preview_id, "")
            if run and run["status"] == "success":
                return self.public(run), None
            active = self.active.get(self.tasks[kind][2][0])
            if active:
                run_id, future = active
                if run and run["id"] == run_id:
                    return self.public(run), future
                raise SyncBusy("该原表正在处理另一批数据，请查看执行记录。")
            run = run or self.new_run(kind, mode, preview_id)
            future = self.submit_run(run)
            return self.public(run), future

    def persist(self, run):
        self.save("run:" + run["id"], run)

    def kick(self):
        with self.lock:
            if self.closing.is_set():
                return []
            now = dt.datetime.now(TZ).isoformat()
            started = []
            for run in reversed(self.history()):
                kind = run["kind"]
                if (run["status"] not in ACTIVE_STATUSES
                        or run.get("next_attempt", "") > now
                        or self.tasks[kind][2][0] in self.active
                        or (run["mode"] != "preview" and not self.settings(kind)["confirmed"])):
                    continue
                started.append(self.submit_run(run))
            return started

    def submit_run(self, run):
        target = self.tasks[run["kind"]][2][0]
        run.update(status="running", error="", stage="", checks={})
        run["attempts"] = run.get("attempts", 0) + 1
        self.persist(run)
        future = self.executor.submit(self.run_batch, dict(run), target)
        self.active[target] = (run["id"], future)
        return future

    def config_revision(self):
        return credential_revision(self.store)

    def tick(self):
        if self.digests is not None:
            self.digests.tick()
        now = dt.datetime.now(TZ)
        for kind in self.tasks:
            cfg = self.settings(kind)
            if not cfg["enabled"] or not cfg["confirmed"]:
                continue
            paused = self.read("pause:" + kind)
            if paused and paused.get("revision") == self.config_revision():
                continue
            eligible = [slot for slot in self.slots(cfg, now)
                        if cfg.get("changed_at", "") <= slot.isoformat() and slot <= now]
            if eligible:
                try:
                    self.enqueue(kind, "scheduled", slot=max(eligible).isoformat())
                except SyncError:
                    pass  # A previous batch is still active or waiting for read-back.
        self.kick()

    def run_batch(self, run, target):
        from .zhihang_data_sources import ZhihangSourceError

        try:
            try:
                self.execute(run)
                run.update(status="success", finished_at=dt.datetime.now(TZ).isoformat())
                self.store.delete_document(self.namespace, "pause:" + run["kind"])
            except Exception as exc:
                has_journal = bool(self.read("journal:" + run["id"]))
                transient = (isinstance(exc, httpx.TransportError) or
                             isinstance(exc, ZhihangSourceError) and exc.code == "source_unavailable")
                retry = transient and run["mode"] == "scheduled" and run["attempts"] < 3
                run.update(status="pending_verify" if has_journal else "retry_wait" if retry else "failed",
                           error=describe_error(exc))
                if run["status"] == "failed":
                    run["finished_at"] = dt.datetime.now(TZ).isoformat()
                else:
                    run["next_attempt"] = (dt.datetime.now(TZ) + dt.timedelta(minutes=2)).isoformat()
                if isinstance(exc, ZhihangSourceError) and exc.code in {"auth_required", "access_denied"}:
                    self.save("pause:" + run["kind"], {"revision": self.config_revision()})
            with self.lock:
                self.persist(run)
                self.prune()
                return self.public(run)
        finally:
            with self.lock:
                self.active.pop(target, None)

    def execute(self, run):
        from . import zhihang_data_sources as source

        kind = run["kind"]
        target_kind = "change_basic" if kind == "change" else kind
        reference = date_value(run["created_at"])

        def report(stage, **facts):
            run["stage"] = stage
            if facts:
                run.setdefault("checks", {}).setdefault(stage, {}).update(
                    {key: value for key, value in facts.items() if value is not None})

        row_key = "rows:" + run["id"]
        rows = self.read(row_key)
        limited = kind in {"inspection", "drill"}
        if limited and rows and not run.get("recent_window") and not self.read("journal:" + run["id"]):
            raise SyncError("拉取范围已调整，请重新预览本类数据。")
        if not rows:
            if run["preview_id"]:
                preview = self.read("run:" + run["preview_id"])
                if limited and not preview.get("recent_window"):
                    raise SyncError("这份预览使用旧日期范围，请重新拉取预览。")
                rows = self.read("rows:" + preview["id"])
                reference = date_value(preview["created_at"])
            else:
                report("读取智航源数据")
                batches = (self.pull(kind, reference) if self.pull is not None else
                           self._pull(kind, reference, report=report, preview=run["mode"] == "preview"))
                rows = batches.get(target_kind, {})
                if kind == "change" and set(rows) != set(batches.get("change_workorder", {})):
                    raise SyncError("变更工单及基础信息数量不一致，保留上次数据。")
                # The original configuration points both change feeds at the same table.
                # Its scheduled publisher writes the enriched basic fields once per work order.
            if not rows:
                raise SyncError("本轮没有可核验的完整记录，保留上次数据。")
            run["reference_at"] = reference.isoformat()
            if limited:
                run["recent_window"] = True
            self.store.put_documents(self.namespace, {"run:" + run["id"]: run, row_key: rows})
        reference = date_value(run.get("reference_at") or run["created_at"])
        report("读取飞书原表", 源数据已核验=len(rows))
        client = self.cloud(self.tasks[kind][2][0])
        remote = {}
        for item in client.list_all():
            if item["record_id"] in remote:
                raise SyncError("飞书分页返回重复记录，请重试。")
            remote[item["record_id"]] = item["fields"]
        saved = self.read("journal:" + run["id"])
        if not saved:
            report("核对飞书表结构与变更", 原表记录=len(remote))
            schema = {field["field_name"]: field["type"] for field in client.list_all("fields")}
            schema = self.prepare_schema(run, client, schema)
            journal, counts, expected = self.diff(kind, target_kind, rows, remote, schema, reference, source, report=report)
            run["counts"] = counts
            if run["mode"] == "preview":
                report("预览核验完成")
                return
            saved = {"items": journal, "expected": expected}
            if kind in TASKS:
                saved["batch_size"] = UPLOAD_BATCH_SIZE
            self.store.put_documents(self.namespace, {"run:" + run["id"]: run, "journal:" + run["id"]: saved})
        report("同步飞书原表")
        self.publish(run, client, remote, saved, target_kind)
        report("回读核验")
        self.verify_batch(client, target_kind, saved["expected"])
        if self.settings(kind)["notify"] and self.notify and not run.get("notification"):
            try:
                run["notification"] = self.notify(kind, rows, run.get("slot", ""), run["id"])
            except Exception as exc:
                run["notification"] = {"status": "failed", "error": describe_error(exc)}
            self.persist(run)

    def prepare_schema(self, run, client, schema):
        return schema

    def diff(self, kind, target_kind, rows, remote, schema, reference, source, *, report=None):
        managed = self.managed(kind)
        by_key = {}
        for record_id, fields in remote.items():
            key = source.identity(target_kind, fields)
            if key:
                by_key.setdefault(key, []).append(record_id)
        journal = []
        counts = {"total": len(rows), "create": 0, "update": 0, "delete": 0, "unchanged": 0, "unmanaged": 0}
        expected = {}
        equivalent_duplicates = 0
        for raw in rows.values():
            key = source.identity(target_kind, raw)
            if not key or key in expected:
                raise SyncError("源数据存在缺失或重复的业务键，请先核对。")
            ids = by_key.get(key, [])
            if len(ids) > 1:
                if target_kind != "inspection":
                    raise SyncError("飞书原表存在重复的业务键，请先核对。")
                first = _inspection_business_fields(remote[ids[0]])
                if any(_inspection_business_fields(remote[record_id]) != first for record_id in ids[1:]):
                    if report:
                        report("核对飞书业务键", 冲突工单=source._first(raw, "工单名称"),
                               计划区间=source._first(raw, "计划区间"), 同键原表记录=len(ids))
                    raise SyncError("飞书原表的同一巡检工单有不同的业务内容，请核对执行详情。")
                equivalent_duplicates += len(ids) - 1
            fields = coerce(raw, schema)
            if target_kind == "inspection" and ids:
                # A rolling source window must not renumber existing inspection records.
                fields = _inspection_business_fields(fields)
            expected[key] = fields
            for record_id in ids or [""]:
                if record_id and matches(fields, remote[record_id]):
                    counts["unchanged"] += 1
                    continue
                action = "update" if record_id else "create"
                counts[action] += 1
                journal.append({"action": action, "id": record_id, "key": key, "fields": fields,
                                "before": remote[record_id] if record_id else {}})
        if report and target_kind == "inspection":
            report("核对飞书业务键", 业务相同的额外原表记录=equivalent_duplicates)
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
                for doc in self.store.list_documents(self.namespace, key_prefix=prefix) if doc["payload"]}

    def check_upload_enabled(self, kind):
        if self.closing.is_set() or not self.settings(kind)["confirmed"]:
            raise Pending("同步已暂停，保留已受理批次等待核验。")

    @staticmethod
    def read_upload_rows(client, ids):
        rows = client.batch_get(ids)
        result = {row["record_id"]: row["fields"] for row in rows}
        if len(rows) != len(ids) or set(result) != set(ids):
            raise Pending("飞书批量回读尚未返回全部记录，保留原批次等待核验。")
        return result

    def create_upload_group(self, run, client, group, journal, target_kind, by_key, remote):
        from .zhihang_data_sources import identity

        existing = {}
        missing = {}
        for item in group:
            key = item["key"]
            candidates = by_key.get(key, [])
            if len(candidates) > 1 or (candidates and not matches(item["fields"], remote[candidates[0]])):
                raise Pending("飞书原表有同键冲突，保留原批次等待核验。")
            if candidates:
                existing[key] = candidates[0]
            else:
                missing[key] = item["fields"]

        journal_key = "journal:" + run["id"]
        attempt = journal.get("create_batch")
        if missing and not attempt:
            attempt = {"keys": list(missing), "at": dt.datetime.now(TZ).isoformat(),
                       "token": _stable_uuid4_client_token(f"zhihang:batch:{run['id']}:{group[0]['key']}")}
            journal["create_batch"] = attempt
            self.save(journal_key, journal)
        receipts = attempt.get("ids", []) if attempt else []
        if missing and not receipts:
            if list(missing) != attempt["keys"] or dt.datetime.now(TZ) - date_value(attempt["at"]) > dt.timedelta(minutes=10):
                raise Pending("批量新增回执尚未核实，等待原批次回读。")
            self.check_upload_enabled(run["kind"])
            response = client.request("POST", "records/batch_create",
                                      {"records": [{"fields": fields} for fields in missing.values()]},
                                      params={"client_token": attempt["token"]})
            receipts = [row["record_id"] for row in response["records"]]
            if len(receipts) != len(missing) or len(set(receipts)) != len(receipts) or not all(receipts):
                raise Pending("飞书批量新增回执不完整，等待原批次回读。")
            attempt["ids"] = receipts
            self.save(journal_key, journal)

        ids = list(dict.fromkeys([*existing.values(), *receipts]))
        if len(ids) != len(group):
            raise Pending("飞书新增回执与原表记录不一致，等待核验。")
        actual = self.read_upload_rows(client, ids)
        confirmed = {identity(target_kind, fields): record_id for record_id, fields in actual.items()}
        if (set(confirmed) != {item["key"] for item in group}
                or any(confirmed[key] != record_id for key, record_id in existing.items())
                or (receipts and {identity(target_kind, actual[rid]) for rid in receipts} != set(attempt["keys"]))):
            raise Pending("飞书新增回读的业务键与本批不一致，等待核验。")
        for item in group:
            item["id"] = confirmed[item["key"]]
        return actual, set(receipts)

    @staticmethod
    def update_fields(run, item, actual):
        changes = {name: value for name, value in item["fields"].items() if not matches({name: value}, actual)}
        # List and batch read-back can return different shapes for the same Feishu field.
        conflicts = [name for name in changes if not matches({name: item["before"].get(name)}, actual)]
        if conflicts:
            facts = run.setdefault("checks", {}).setdefault("同步飞书原表", {})
            facts.update({"冲突记录": item["id"], "冲突字段": "、".join(conflicts)})
            raise Pending("原表数据在同步期间被修改，请核对冲突。")
        return changes

    @staticmethod
    def verified_update_snapshot(item, actual):
        # A concurrent edit outside the write set must not become an owned value eligible for cleanup.
        names = item["fields"]
        snapshot = {name: value for name, value in item["before"].items() if name not in names}
        snapshot.update({name: actual[name] for name in names if name in actual})
        return snapshot

    def update_upload_group(self, run, client, group):
        ids = [item["id"] for item in group]
        actual = self.read_upload_rows(client, ids)
        changes = []
        for item in group:
            record_id = item["id"]
            fields = self.update_fields(run, item, actual[record_id])
            if fields:
                changes.append({"record_id": record_id, "fields": fields})
        if changes:
            self.check_upload_enabled(run["kind"])
            client.batch_update(changes)
            actual = self.read_upload_rows(client, ids)
        return actual

    def publish_groups(self, run, client, remote, journal, target_kind, by_key):
        kind = run["kind"]
        size = journal["batch_size"]
        owned = self.managed(kind)
        for action in ("create", "update"):
            items = [item for item in journal["items"] if item["action"] == action]
            for start in range(0, len(items), size):
                group = items[start:start + size]
                if all(item.get("done") for item in group):
                    continue
                self.check_upload_enabled(kind)
                receipt_ids = set()
                if action == "create":
                    actual, receipt_ids = self.create_upload_group(run, client, group, journal, target_kind, by_key, remote)
                else:
                    actual = self.update_upload_group(run, client, group)
                documents = {}
                for item in group:
                    rid = item["id"]
                    if not matches(item["fields"], actual[rid]):
                        raise Pending("飞书已受理写入，字段回读尚未一致。")
                    if rid in receipt_ids:
                        documents["owned:" + kind + ":" + rid] = {"key": item["key"], "fields": actual[rid]}
                    elif action == "update" and rid in owned and owned[rid]["fields"] == item["before"]:
                        documents["owned:" + kind + ":" + rid] = {
                            "key": item["key"], "fields": self.verified_update_snapshot(item, actual[rid]),
                        }
                for item in group:
                    item["done"] = True
                journal.pop("create_batch", None)
                facts = run.setdefault("checks", {}).setdefault("同步飞书原表", {})
                facts["已核验新增" if action == "create" else "已核验更新"] = start + len(group)
                documents.update({"run:" + run["id"]: run, "journal:" + run["id"]: journal})
                self.store.put_documents(self.namespace, documents)

    def publish(self, run, client, remote, journal, target_kind):
        from .zhihang_data_sources import identity, in_refresh_scope

        kind = run["kind"]
        by_key = {}
        for record_id, fields in remote.items():
            by_key.setdefault(identity(target_kind, fields), []).append(record_id)
        # Older interrupted creates must retain their original single-record idempotency tokens.
        if journal.get("batch_size"):
            self.publish_groups(run, client, remote, journal, target_kind, by_key)
        verified = False
        for index, item in enumerate(journal["items"]):
            if item.get("done"):
                continue
            self.check_upload_enabled(kind)
            action, record_id = item["action"], item["id"]
            if (action == "delete" and kind in {"drill", "inspection"} and
                    not in_refresh_scope(target_kind, item["before"], date_value(run.get("reference_at") or run["created_at"]))):
                continue
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
                if current is None:
                    raise Pending("待更新记录已从原表移除，保留原批次等待核对。")
                changes = self.update_fields(run, item, client.get(record_id)["fields"])
                if changes:
                    client.update(record_id, changes)
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
                if action == "create" and item["id"] == record_id:
                    self.save(ownership_key, {"key": item["key"], "fields": actual})
                elif action == "update" and owned and owned["fields"] == item["before"]:
                    self.save(ownership_key, {"key": item["key"], "fields": self.verified_update_snapshot(item, actual)})
            elif self.read(ownership_key):
                self.store.delete_document(self.namespace, ownership_key)
            item["done"] = True
            self.save("journal:" + run["id"], journal)

    @staticmethod
    def verify_batch(client, kind, expected):
        from .zhihang_data_sources import identity

        seen = {}
        for row in client.list_all():
            fields = row["fields"]
            key = identity(kind, fields)
            if key in expected:
                if not matches(expected[key], fields):
                    raise Pending("目标表本批数据尚未完整读回，保留待核验任务。")
                if key in seen and (kind != "inspection" or
                                    _inspection_business_fields(fields) != _inspection_business_fields(seen[key])):
                    raise Pending("目标表本批数据存在同键业务冲突，保留待核验任务。")
                seen[key] = fields
        if seen.keys() != expected.keys():
            raise Pending("目标表本批数据尚未完整读回，保留待核验任务。")
