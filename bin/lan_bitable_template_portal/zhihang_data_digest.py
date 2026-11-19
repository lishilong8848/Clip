"""Read-only preparation and durable delivery for the two legacy Zhihang summaries."""
from __future__ import annotations

import calendar
import logging
import re
import threading
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, time, timedelta

from . import zhihang_data_cards as cards
from . import zhihang_data_sources as sources
from .zhihang_data_sync import NS, TASKS, SyncError, describe_error

DAILY = "daily"
MONTH_END = "month_end"
DIGESTS = {
    DAILY: ("智航数据同步", "DAILY_COMBINED_NOTIFY", ("inspection", "change", "event")),
    MONTH_END: ("风险排查与演练推进同步", "MONTH_END_PROGRESS_NOTIFY", ("risk", "drill")),
}
BUILDINGS = tuple(f"{letter}楼" for letter in "ABCDE")
RETRY_DELAY = timedelta(minutes=10)
DELIVERY_WINDOW = timedelta(hours=1)
TERMINAL = {"sent", "delivery_unknown", "cancelled"}
log = logging.getLogger(__name__)


def _times(kind):
    if kind == DAILY:
        return cards._times("DAILY_COMBINED_NOTIFY_TIME", ("08:00",))
    return cards._times("MONTH_END_PROGRESS_NOTIFY_SCHEDULES", ("09:00", "15:00"))


def _start_day():
    value = cards._setting("MONTH_END_PROGRESS_NOTIFY_START_DAY")
    return int(value) if re.fullmatch(r"(?:[1-9]|1[0-9]|2[0-8])", value) else 25


def _prepare_time():
    return cards._times("DAILY_COMBINED_PREPARE_TIME", ("00:10",))[0]


def _at(now, label):
    return datetime.combine(now.date(), time.fromisoformat(label), cards.BEIJING)


def _last_day(now):
    return calendar.monthrange(now.year, now.month)[1]


def _target(kind):
    prefix = DIGESTS[kind][1]
    chat_id = cards._chat_id(prefix + "_CHAT_ID")
    chat_name = cards._setting(prefix + "_CHAT_NAME")
    if not chat_id and not chat_name:
        chat_id = cards._chat_id("FEISHU_NOTIFY_CHAT_ID")
        chat_name = cards._setting("FEISHU_NOTIFY_CHAT_NAME")
    if not chat_id and not chat_name:
        chat_id = cards.DEFAULT_CHAT_ID
    return {"chat_id": chat_id, "chat_name": chat_name, "label": f"汇总通知群（{chat_id or chat_name}）"}


def _daily_section(kind, rows, reference):
    if kind == "inspection":
        items, _ = cards._inspection_details(rows, reference, reference, report_day=reference.date())
        return {"lines": [f"异常工单 {len(items)} 张", cards._detail(items, 20)], "important": bool(items)}
    if kind == "change":
        _, important, tips = cards._change_focus(rows, reference)
        return {"lines": [f"重要 {len(important)} 项｜普通提示 {len(tips)} 项",
                          cards._detail(important, 10), cards._detail(tips, 5)],
                "important": bool(important), "warning": bool(tips)}
    groups = {"I2": [], "已恢复待解决": [], "非真实": []}
    day = reference.date()
    for row in rows:
        primary = sources._date(row.get("发生时间")) or sources._date(row.get("创建时间") or row.get("通知时间"))
        recovered = sources._date(row.get("恢复时间"))
        status = sources._text(row.get("处理状态") or row.get("事件状态"))
        if status in {"", "未知状态"}:
            status = sources._text(row.get("流程状态"))
        level = sources._first(row, "日报等级", "最高等级", "当前等级", "首次等级")
        is_i2 = re.fullmatch(r"I2(?:级)?", level, re.IGNORECASE)
        title = cards._plain(row.get("事件标题") or row.get("事件编号")) or "未命名事件"
        if primary and primary.date() == day:
            line = f"{title}｜{primary:%m-%d %H:%M}｜{cards._plain(status) or '无状态'}"
            if is_i2:
                groups["I2"].append(line)
            if sources._text(row.get("真实事件")).lower() in {"否", "false", "0", "no", "n"}:
                groups["非真实"].append(line)
        if (recovered and recovered.date() == day and sources._text(row.get("告警状态")) == "已恢复"
                and status not in {"", "未知状态", "完成", "已终止"}):
            groups["已恢复待解决"].append(f"{title}｜恢复 {recovered:%m-%d %H:%M}｜{cards._plain(status)}")
    lines = ["｜".join(f"{label} {len(items)} 条" for label, items in groups.items())]
    lines.extend(f"**{label}**\n{cards._detail(items, 5)}" for label, items in groups.items() if items)
    return {"lines": lines, "important": bool(groups["I2"]),
            "warning": bool(groups["已恢复待解决"] or groups["非真实"])}


def _month_section(kind, rows, reference):
    total, complete = Counter(), Counter()
    current_count = 0
    for row in rows:
        if kind == "drill":
            year = sources._text(row.get("演练年份"))
            month = sources._drill_month(sources._text(row.get("演练月份")))
            if not year.isdecimal() or not month:
                date = next((value for key in ("计划演练时间", "实际演练时间", "创建时间")
                             if (value := sources._date(row.get(key)))), None)
                if not date:
                    raise SyncError("演练记录的月份无法核验。")
                year, month = str(date.year), date.month
            if (int(year), month) != (reference.year, reference.month):
                continue
            current_count += 1
            status = sources._text(row.get("演练状态"))
            if status == "终止":
                continue
            areas = {part.strip() for part in sources._text(row.get("实施区域")).split("、")}
            done = status == "完成"
        else:
            current_count += 1
            areas = {sources._text(row.get("楼栋"))}
            done = all(sources._text(row.get(key)).lower() not in {"", "-", "null", "undefined"}
                       for key in ("排查时间", "审核时间"))
        if not areas or not areas.issubset(BUILDINGS):
            raise SyncError(f"{TASKS[kind][0]}含楼栋归属待核对的记录。")
        total.update(areas)
        if done:
            complete.update(areas)
    if not current_count or (kind == "risk" and set(total) != set(BUILDINGS)):
        raise SyncError(f"{TASKS[kind][0]}当月数据不完整。")
    return {"progress": {building: [complete[building], total[building]] for building in BUILDINGS},
            "complete": total == complete}


def _build_card(kind, members, data, slot, reference):
    title = DIGESTS[kind][0]
    if kind == DAILY:
        important = any(item.get("important") for item in data.values())
        warning = any(item.get("warning") or item.get("error") for item in data.values())
        sections = []
        for member in members:
            item = data[member]
            lines = item.get("lines") or [f"数据待核对 · {cards._plain(item.get('error'))}"]
            sections.append((TASKS[member][0], "\n".join(filter(None, [*lines, f"[查看原表]({cards._table_url(member)})"]))))
        return cards._report_card(title, "red" if important else "yellow" if warning else "green",
                                  [f"**报告日期：{reference:%Y-%m-%d}**"], sections)
    deadline = slot.replace(day=_last_day(slot))
    left = (deadline.date() - slot.date()).days
    sections = []
    for building in BUILDINGS:
        lines = []
        for member in members:
            done, total = data[member]["progress"][building]
            text = f"完成 {done}/{total}｜完成率 {done / total:.0%}｜未完成 {total - done}" if total else "0/0（无任务）"
            lines.append(f"{TASKS[member][0]}：{text}")
        sections.append((building, "\n".join(lines)))
    links = " ｜ ".join(f"[{TASKS[member][0]}原表]({cards._table_url(member)})" for member in members)
    return cards._report_card(title, "green" if all(item["complete"] for item in data.values()) else "yellow",
                              [f"**截止日期：{deadline:%Y-%m-%d}**", f"距截止还有 {left} 天" if left else "今日截止", links],
                              sections)


def _pull(kind, reference):
    if kind == "event":
        return sources.pull_daily_report_events(reference).get("event", {})
    target_kind = "change_basic" if kind == "change" else kind
    result = sources.pull(kind, reference)
    rows = result.get(target_kind, {})
    if kind == "change" and set(rows) != set(result.get("change_workorder", {})):
        raise SyncError("变更工单及基础信息数量不一致。")
    return rows


def _deliver(card, chat_id, identity):
    from upload_event_module.services.robot_webhook import _deliver_many

    _, _, (result,) = _deliver_many("interactive", card, [chat_id], "chat_id", identity)
    return result


class ZhihangDigests:
    def __init__(self, sync, *, pull=None, deliver=None, clock=None):
        self.sync = sync
        self.store = sync.store
        self.pull = pull or _pull
        self.deliver = deliver or _deliver
        self.clock = clock or (lambda: datetime.now(cards.BEIJING))
        self.executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="ZhihangDigests")
        self.closing = threading.Event()
        self.lock = threading.Lock()
        self.worker = None

    def close(self):
        with self.lock:
            self.closing.set()
            self.executor.shutdown(wait=False, cancel_futures=True)

    def tick(self):
        with self.lock:
            if not self.closing.is_set() and (self.worker is None or self.worker.done()):
                self.worker = self.executor.submit(self.run)

    def members(self, kind):
        _, key, members = DIGESTS[kind]
        if not cards._enabled(key + "_ENABLED"):
            return []
        configured = {name: self.sync.settings(name) for name in members}
        return [name for name in members if all(configured[name].get(flag) for flag in ("enabled", "confirmed", "notify"))]

    def status(self):
        result = []
        for kind, (title, key, names) in DIGESTS.items():
            if not cards._enabled(key + "_ENABLED"):
                continue
            latest = self.store.get_document(NS, "digest-latest:" + kind) or {}
            result.append({"kind": kind, "label": title, "times": _times(kind),
                           "prepare_at": _prepare_time() if kind == DAILY else "",
                           "start_day": _start_day() if kind == MONTH_END else None,
                           "modules": list(names), "enabled_modules": self.members(kind), "target": _target(kind),
                           "latest": {key: latest[key] for key in ("slot", "status", "message") if key in latest}})
        return result

    def _save(self, key, state):
        documents = {key: state, "digest-latest:" + state["kind"]: {
            label: state.get(label, "") for label in ("slot", "status", "message")
        }}
        if state.get("complete"):
            documents["digest-complete:month_end"] = {
                "month": state["slot"][:7], "slot": state["slot"], "modules": state["members"], "status": state["status"],
            }
        self.store.put_documents(NS, documents)

    def run(self):
        for kind in DIGESTS:
            if self.closing.is_set():
                return
            try:
                if members := self.members(kind):
                    self._run(kind, members, self.clock())
            except Exception:
                log.exception("智航汇总任务异常 kind=%s", kind)

    def _run(self, kind, members, now):
        times = _times(kind)
        slot = _at(now, times[0])
        if kind == DAILY:
            if now < _at(now, _prepare_time()) and now < slot:
                return
            reference = _at(slot, "00:00") - timedelta(microseconds=1)
            identity = reference.date().isoformat()
        else:
            if now.day < _start_day():
                return
            if now.day == _last_day(now) and len(times) > 1 and now >= _at(now, times[1]):
                slot = _at(now, times[1])
            if not slot <= now < slot + timedelta(hours=6):
                return
            reference = now
            identity = slot.isoformat()
            completed = self.store.get_document(NS, "digest-complete:month_end") or {}
            if (completed.get("month") == slot.strftime("%Y-%m")
                    and set(members).issubset(completed.get("modules", []))
                    and (completed.get("status") in {"sent", "delivery_unknown"}
                         or completed.get("status") == "sending" and completed.get("slot") != slot.isoformat())):
                return
        key = f"digest:{kind}:{identity}"
        state = self.store.get_document(NS, key) or {"kind": kind, "slot": slot.isoformat(), "data": {}}
        if state.get("status") in TERMINAL:
            return
        if state.get("card") and members != state["members"]:
            state.update(status="delivery_unknown" if state.get("uncertain_at") else "cancelled",
                         message="通知范围已变化，已停止本张汇总卡片的补发。")
            self._save(key, state)
            return
        if now < (sources._date(state.get("next_attempt")) or slot.replace(hour=0, minute=0)):
            return
        if not state.get("card"):
            due = now >= slot
            if not due and all(state["data"].get(member) and not state["data"][member].get("error") for member in members):
                return
            if kind == DAILY and not due and state.get("prepare_attempts", 0) >= 3:
                return
            if kind == MONTH_END and state.get("prepare_attempts", 0) >= 6:
                return
            self._prepare(kind, members, reference, state)
            if self.closing.is_set():
                return
            data = {member: state["data"][member] for member in members}
            errors = [f"{TASKS[member][0]} · {data[member]['error']}" for member in members if data[member].get("error")]
            state["prepare_attempts"] = state.get("prepare_attempts", 0) + 1
            if not due or (kind == MONTH_END and errors):
                state.update(status="failed" if errors else "prepared", message="；".join(errors) or "已准备，等候发送。",
                             next_attempt=(now + RETRY_DELAY).isoformat())
                self._save(key, state)
                return
            state.update(card=_build_card(kind, members, data, slot, reference), target=_target(kind), members=members,
                         complete=kind == MONTH_END and all(item["complete"] for item in data.values()), data_errors=errors)
        self._send(key, state, members, slot, identity)

    def _prepare(self, kind, members, reference, state):
        for member in members:
            if self.closing.is_set():
                return
            if state["data"].get(member) and not state["data"][member].get("error"):
                continue
            try:
                rows = self.pull(member, reference)
                if not rows and member != "event":
                    raise SyncError("源数据为空，无法核对。")
                formatter = _daily_section if kind == DAILY else _month_section
                state["data"][member] = formatter(member, rows.values(), reference)
            except Exception as exc:
                state["data"][member] = {"error": describe_error(exc)}

    def _send(self, key, state, members, slot, identity):
        kind = state["kind"]
        now = self.clock()
        if self.closing.is_set() or self.members(kind) != members or now.date() != slot.date():
            return
        if kind == MONTH_END and now >= slot + timedelta(hours=6):
            return
        uncertain = sources._date(state.get("uncertain_at"))
        attempts = state.get("attempts", 0)
        if uncertain and (kind == MONTH_END or not timedelta(0) <= now - uncertain < DELIVERY_WINDOW or attempts >= 6):
            state.update(status="delivery_unknown", message="飞书投递结果待人工核对。")
            self._save(key, state)
            return
        if attempts >= 6:
            return
        target = state["target"]
        chat_id = state.get("chat_id") or cards._resolve_chat(target)
        state.update(attempts=attempts + 1, next_attempt=(now + RETRY_DELAY).isoformat())
        if not chat_id:
            state.update(status="failed", message="当前飞书应用无法唯一识别汇总通知群。")
            self._save(key, state)
            return
        if self.closing.is_set() or self.members(kind) != members:
            return
        request_uuid = state.get("message_uuid") or str(uuid.uuid5(
            uuid.NAMESPACE_URL, f"lighthouse:zhihang:digest:{kind}:{identity}:{chat_id}",
        ))
        state.update(status="sending", message="正在等待飞书投递回执。", chat_id=chat_id, message_uuid=request_uuid,
                     uncertain_at=(uncertain or now).isoformat())
        self._save(key, state)
        try:
            result = self.deliver(state["card"], chat_id, request_uuid)
        except Exception:
            result = {"failure_kind": "uncertain"}
        if result.get("ok") and result.get("message_id") and result.get("chat_id") == chat_id:
            message = "飞书已确认发送。"
            if state["data_errors"]:
                message += " 卡片包含待核对的数据。"
            state.update(status="sent", message=message, message_id=result["message_id"], uncertain_at="")
        elif not uncertain and result.get("failure_kind") in {"not_sent", "blocked", "bot_unavailable", "agent_required"}:
            state.update(status="failed", message="飞书未接受卡片，请核对当前应用及目标群。", uncertain_at="")
        elif kind == MONTH_END:
            state.update(status="delivery_unknown", message="飞书投递结果待人工核对。")
        else:
            state.update(status="pending", message="飞书投递待确认，稍后核验。")
        self._save(key, state)
