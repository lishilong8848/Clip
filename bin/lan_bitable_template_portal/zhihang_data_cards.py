"""Post-verification cards for the original Zhihang standalone notifications."""
from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
import os
import re
import time
import uuid
from urllib.parse import urlencode

from .zhihang_data_sources import _date, _drill_month, _plan_range, _text

NAMESPACE = "zhihang_data"
BEIJING = timezone(timedelta(hours=8))
DEFAULT_CHAT_ID = "oc_3bc648b9b761f24a65366a9b04b32eb2"
PREFIXES = {"risk": "", "drill": "DRILL_", "change": "CHANGE_", "event": "EVENT_", "inspection": "INSPECT_"}
TITLES = {"risk": "风险排查同步", "drill": "演练推进同步", "change": "变更工单同步", "event": "事件追踪同步"}
_INSPECTION_LIMIT = 20


def _setting(*keys):
    return next((value for key in keys if (value := os.environ.get(key, "").strip())), "")


def _enabled(key, default=True):
    value = _setting(key).lower()
    if value in {"1", "true", "yes", "y", "on"}:
        return True
    if value in {"0", "false", "no", "n", "off"}:
        return False
    return default


def _times(key, default):
    return sorted({value.strip() for value in _setting(key).split(",")
                   if re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", value.strip())}) or list(default)


def _chat_id(*keys):
    return next((value for key in keys if re.fullmatch(r"oc_[A-Za-z0-9_-]+", value := _setting(key))), "")


def _standalone_enabled(kind):
    if kind in {"risk", "drill"}:
        return not _enabled("MONTH_END_PROGRESS_NOTIFY_ENABLED", False)
    if kind in {"change", "event"} and _enabled("DAILY_COMBINED_NOTIFY_ENABLED", False):
        return False
    return _enabled(PREFIXES[kind] + "AUTO_SYNC_NOTIFY_ENABLED")


def notification_targets(kind):
    """Configured chat descriptors, without Feishu API calls or app credentials."""
    if kind not in PREFIXES or not _standalone_enabled(kind):
        return []
    prefix = PREFIXES[kind]
    configured_id = _chat_id("FEISHU_NOTIFY_CHAT_ID")
    common_name = _setting("FEISHU_NOTIFY_CHAT_NAME")
    common_id = configured_id or ("" if common_name else DEFAULT_CHAT_ID)
    chat_id = (_chat_id(prefix + "FEISHU_NOTIFY_CHAT_ID") if prefix else "") or configured_id
    chat_name = (_setting(prefix + "FEISHU_NOTIFY_CHAT_NAME") if prefix else "") or common_name
    if not chat_id and not chat_name:
        chat_id = common_id
    targets = [{"role": "primary", "label": f"主通知群（{chat_id or chat_name}）",
                "chat_id": chat_id, "chat_name": chat_name}]
    if kind == "change" and _enabled("CHANGE_IMPORTANT_NOTIFY_ENABLED"):
        important = _chat_id("CHANGE_IMPORTANT_NOTIFY_CHAT_ID", "CHANGE_DAILY_NOTIFY_CHAT_ID") or common_id
        if important and important != chat_id:
            targets.append({"role": "important", "label": f"重要变更群（{important}）", "chat_id": important,
                            "times": _times("CHANGE_IMPORTANT_NOTIFY_SCHEDULES", ("15:38",))})
    if kind in {"inspection", "change", "event"} and _enabled("EXTRA_ABNORMAL_NOTIFY_ENABLED"):
        extra = _chat_id("EXTRA_ABNORMAL_NOTIFY_CHAT_ID", "FEISHU_ABNORMAL_NOTIFY_CHAT_ID") or common_id
        if extra and extra != chat_id:
            targets.append({"role": "extra", "label": f"异常通知群（{extra}）", "chat_id": extra})
    return targets


def _plain(value):
    text = " ".join(_text(value).split())[:240]
    return re.sub(r"([\\`*_\[\]~])", r"\\\1", text.replace("&", "＆").replace("<", "＜").replace(">", "＞"))


def _detail(lines, limit):
    result = [f"{index}\\. {line}" for index, line in enumerate(lines[:limit], 1)]
    if len(lines) > limit:
        result.append(f"另有 {len(lines) - limit} 项未展开")
    return "\n".join(result)


def _card(kind, title, color, lines, sections):
    from .zhihang_data_sync import BASE, TASKS

    table, view = TASKS[kind][2]
    url = f"https://vnet.feishu.cn/base/{BASE}?" + urlencode({"table": table, "view": view})
    elements = [{"tag": "markdown", "content": "\n".join([*lines, f"[打开多维表]({url})"])}]
    for heading, content in sections:
        if content:
            elements.extend(({"tag": "hr"}, {"tag": "markdown", "content": f"**{heading}**\n{content}"}))
    return {"config": {"wide_screen_mode": True, "enable_forward": True},
            "header": {"template": color, "title": {"tag": "plain_text", "content": title}},
            "elements": elements}


def _inspection_card(rows, scheduled, observed):
    details = []
    invalid = 0
    for row in rows:
        status = _text(row.get("工单状态"))
        # A normal completion is authoritative for the card, including when the source counts 点位异常.
        if "".join(status.split()) == "正常完成":
            continue
        times = _plan_range(row.get("计划区间"))
        if len(times) != 2 or not all(times) or times[0] >= times[1]:
            invalid += 1
            continue
        start, end = times
        if not start <= scheduled < end:
            continue
        reasons = []
        if match := re.search(r"异常\s*[:：]\s*(\d+(?:\.\d+)?)", _text(row.get("巡检结果"))):
            if float(match[1]) > 0:
                reasons.append(f"异常点 {match[1]}")
        submitted = _text(row.get("提交时间"))
        overdue_reason = _text(row.get("逾期原因"))
        if "逾期" in status or "overdue" in status.lower() or overdue_reason:
            reasons.append("逾期完成" if submitted else "逾期未提交")
        if start.hour >= 20:
            due = (start + timedelta(days=1)).replace(hour=2, minute=30, second=0, microsecond=0)
        else:
            hour = 9 if start.hour < 9 else 15 if start.hour < 15 else 20
            due = start.replace(hour=hour, minute=0, second=0, microsecond=0)
        if not submitted and observed >= due:
            reasons.append("逾班未提交")
        if overdue_reason:
            reasons.append(_plain(overdue_reason))
        if not reasons and _text(row.get("异常标")) == "异常":
            reasons.append("巡检异常")
        if reasons:
            details.append((start, "｜".join([
                _plain(row.get("楼栋")) or "未识别楼栋", _plain(row.get("巡检人")) or "未分配",
                f"{start:%Y-%m-%d %H:%M}-{end:%Y-%m-%d %H:%M}", _plain(status) or "无状态",
                "，".join(dict.fromkeys(reasons)),
            ])))
    if not details:
        return None, 0, invalid
    lines = [line for _, line in sorted(details)]
    card = _card("inspection", f"巡检异常提醒（{scheduled:%H:%M}班次）", "red", [
        f"**班次时刻**：{scheduled:%Y-%m-%d %H:%M}", f"**异常数量**：{len(lines)} 单",
    ], [("本班异常明细", _detail(lines, _INSPECTION_LIMIT))])
    return card, len(lines), invalid


def _risk_summary(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[_text(row.get("楼栋")) or "未识别楼栋"].append(row)
    complete = True
    lines = []
    for building, items in sorted(groups.items()):
        def count(column):
            return sum(_text(row.get(column)).lower() not in {"", "-", "null", "undefined"} for row in items)

        checked, reviewed = count("排查时间"), count("审核时间")
        total = len(items)
        complete = complete and checked == reviewed == total
        lines.append(f"{_plain(building)}：排查 {checked}/{total}，审核 {reviewed}/{total}")
    active = sum(_text(row.get("当前风险状态")) not in {"", "无", "否", "无风险", "暂无", "正常", "0"} for row in rows)
    return [("楼栋进度", "\n".join(lines))], bool(active), complete and bool(rows)


def _drill_summary(rows, observed):
    statuses = Counter()
    buildings = defaultdict(Counter)
    overdue = 0
    for row in rows:
        status = _text(row.get("演练状态")) or "待演练"
        if status == "终止":
            continue
        year, month = _text(row.get("演练年份")), _drill_month(_text(row.get("演练月份")))
        if not year.isdecimal() or not month:
            date = next((date for key in ("计划演练时间", "计划结束时间", "实际演练时间", "触发时间", "创建时间")
                         if (date := _date(row.get(key)))), None)
            if not date:
                continue
            year, month = str(date.year), date.month
        period = int(year), month
        if period > (observed.year, observed.month) or period < (observed.year, observed.month) and status == "完成":
            continue
        overdue += status != "完成" and _text(row.get("是否逾期")) == "是"
        if period == (observed.year, observed.month):
            statuses[status] += 1
        buildings[_text(row.get("实施区域")) or "未识别楼栋"][status] += 1
    sections = [("本月状态", "、".join(f"{_plain(status)} {count}" for status, count in statuses.items())),
                ("楼栋进展", "\n".join(f"{_plain(building)}：完成 {counts['完成']}/{counts.total()}"
                                       for building, counts in sorted(buildings.items()))),
                ("提示", f"逾期未完成 {overdue} 项" if overdue else "")]
    return sections, bool(overdue)


def _change_summary(rows, observed):
    statuses = Counter()
    focus = []
    important = False
    for row in rows:
        status = _text(row.get("状态")) or "未知状态"
        if any(value in status for value in ("终止", "实施成功", "关闭")):
            continue
        statuses[status] += 1
        node = _text(row.get("当前节点"))
        delayed = row.get("计划延时结束时间") or row.get("延迟结束时间")
        deadline = _date(delayed or row.get("计划结束时间"))
        reasons = []
        if "客服审核" in node:
            reasons.append("客服审核待处理")
            important = True
        if deadline and not _date(row.get("实际结束时间")):
            days = (deadline.date() - observed.date()).days
            if days <= 3:
                label = "延迟结束" if delayed else "计划结束"
                remaining = f"已超期 {-days} 天" if days < 0 else "今天到期" if days == 0 else f"还剩 {days} 天"
                reasons.append(label + remaining)
                important = important or days <= 1
        if reasons:
            title = _plain(row.get("标题") or row.get("工单号")) or "未命名工单"
            focus.append(f"{title}｜{_plain(status)}｜{'，'.join(reasons)}")
    return [("状态分布", "、".join(f"{_plain(status)} {count}" for status, count in statuses.items())),
            ("需要跟进", _detail(focus, 20))], important, bool(focus)


def _event_summary(rows):
    sections = []
    important = False
    records = sorted(rows, key=lambda row: _text(row.get("发生时间") or row.get("创建时间")), reverse=True)
    for focus in ("重要", "未完成", "非真实"):
        lines = []
        for row in records:
            if focus not in _text(row.get("同步关注类型")).split("、"):
                continue
            lines.append("｜".join(filter(None, (_plain(row.get("事件标题") or row.get("事件编号")),
                                                 _plain(row.get("发生时间")), _plain(row.get("事件状态")),
                                                 _plain(row.get("告警状态"))))))
        important = important or (focus in {"重要", "未完成"} and bool(lines))
        if lines:
            sections.append((f"{focus}事件（近30天，共 {len(lines)} 条）", _detail(lines, 5)))
    return sections, important


def _event_snapshot(rows):
    columns = ("告警状态", "告警状态编码", "处理状态", "处理状态编码", "流程状态", "流程状态编码")
    return {key: [_text(row.get("处理状态") or row.get("事件状态")), *(_text(row.get(column)) for column in columns)]
            for key, row in rows.items()}


def _scheduled_policy(kind, scheduled, catchup, summary, state, store):
    """Use the original slot for cadence and the recorded enqueue time for catch-up."""
    if scheduled is None:
        return True
    label = scheduled.strftime("%H:%M")
    day = scheduled.date().isoformat()
    if kind in {"risk", "drill"}:
        if catchup or not _enabled("AUTO_SYNC_MESSAGE_ENABLED", False):
            return False
        if label not in _times("AUTO_SYNC_MESSAGE_SCHEDULES", ("08:30",)):
            return False
        if kind == "risk" and summary:
            month = day[:7]
            last_day = (scheduled + timedelta(days=1)).month != scheduled.month
            return state.get("complete_month") != month or (last_day and state.get("last_complete_month") != month)
        weekday, after = _setting("AUTO_SYNC_MESSAGE_WEEKDAY"), _setting("AUTO_SYNC_MESSAGE_MONTH_END_AFTER_DAY")
        weekday = int(weekday) if weekday.isdecimal() and int(weekday) <= 6 else 4
        after = int(after) if after.isdecimal() and 1 <= int(after) <= 30 else 25
        return (scheduled.weekday() + 1) % 7 == weekday or scheduled.day > after
    if kind == "event" and catchup:
        return False
    if summary:
        return True
    from .zhihang_data_sync import TASKS

    times = (store.get_document(NAMESPACE, "settings:" + kind) or {}).get("times") or TASKS[kind][1]
    return not catchup and label == max(times) and state.get("notified_day") != day


def _resolve_chat(target):
    if target["chat_id"]:
        return target["chat_id"]
    from upload_event_module.services.robot_webhook import _get_chat_cache

    chats, error = _get_chat_cache()
    matches = {chat["chat_id"] for chat in chats if chat.get("chat_id") and chat.get("name") == target.get("chat_name")}
    return next(iter(matches)) if not error and len(matches) == 1 else ""


def _eligible_target(target, important, scheduled, catchup):
    if target["role"] == "primary":
        return True
    if not important:
        return False
    if target["role"] == "extra":
        return True
    return bool(scheduled and not catchup and scheduled.strftime("%H:%M") in target["times"])


def _plan(kind, rows, slot, run_id, scheduled, targets, store):
    run = store.get_document(NAMESPACE, "run:" + run_id) or {}
    created_at = _date(run.get("created_at"))
    observed = _date(run.get("reference_at")) or created_at or scheduled or datetime.now(BEIJING)
    catchup = bool(scheduled and created_at and created_at.replace(second=0, microsecond=0) > scheduled)
    state = store.get_document(NAMESPACE, "cadence:" + kind) or {}
    count, invalid = 0, 0
    complete = False
    next_snapshot = None
    if kind == "inspection":
        card, count, invalid = _inspection_card(rows.values(), scheduled, observed)
        important = bool(count)
    else:
        if kind == "risk":
            sections, important, complete = _risk_summary(rows.values())
            policy_summary = complete
        elif kind == "drill":
            sections, important = _drill_summary(rows.values(), observed)
            policy_summary = False
        elif kind == "change":
            sections, important, policy_summary = _change_summary(rows.values(), observed)
        else:
            sections, important = _event_summary(rows.values())
            next_snapshot = _event_snapshot(rows)
            previous = store.get_document(NAMESPACE, "event:snapshot") or {}
            policy_summary = any(previous.get(row_key) != value or value[0] != "完成"
                                 for row_key, value in next_snapshot.items())
        card = None
        if _scheduled_policy(kind, scheduled, catchup, policy_summary, state, store):
            card = _card(kind, TITLES[kind], "red" if important else "blue", [
                f"**同步时间**：{observed:%Y-%m-%d %H:%M}", f"**覆盖记录**：{len(rows)} 条",
            ], sections)
    eligible = [target for target in targets if _eligible_target(target, important, scheduled, catchup)] if card else []
    planned = {"kind": kind, "slot": slot, "card": card, "targets": eligible, "results": {},
               "complete": complete, "abnormal_count": count, "invalid_plan_range_count": invalid}
    documents = {"card:" + run_id: planned}
    if next_snapshot is not None:
        documents["event:snapshot"] = next_snapshot
    store.put_documents(NAMESPACE, documents)
    return planned


def _deliver_target(target, receipt, chat_id, card, kind, identity):
    if receipt.get("status") == "sent":
        return receipt
    if not chat_id:
        return dict(target, status="failed", message="当前飞书应用无法唯一识别目标群。")
    from upload_event_module.services.robot_webhook import send_interactive_to_chat_id

    message_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"lighthouse:zhihang:{kind}:{identity}:{chat_id}"))
    attempts, delay = 1, 0
    if kind == "inspection":
        attempt_setting = _setting("INSPECT_NOTIFY_RETRY_ATTEMPTS")
        delay_setting = _setting("INSPECT_NOTIFY_RETRY_DELAY_MS")
        attempts = min(10, max(1, int(attempt_setting))) if re.fullmatch(r"[0-9]{1,5}", attempt_setting) else 3
        delay = min(60_000, int(delay_setting)) / 1000 if re.fullmatch(r"[0-9]{1,8}", delay_setting) else 2
    for attempt in range(attempts):
        try:
            success, _ = send_interactive_to_chat_id(card, chat_id, message_uuid=message_uuid)
        except Exception:
            success = False
        if success:
            break
        if delay and attempt + 1 < attempts:
            time.sleep(delay)
    return dict(target, chat_id=chat_id, message_uuid=message_uuid, status="sent" if success else "failed",
                **({} if success else {"message": "当前飞书应用的卡片投递未确认。"}))


def notify(kind, rows, slot, run_id, *, store=None):
    """Send only after verified sync. ``rows`` is {stable_key: mapped_fields}.

    ``slot`` is the original Beijing ISO slot or an empty string for manual sync.
    Returns ``status``, ``message``/``error`` and per-target receipts. Retry with the
    same run ID and slot. Neither this hook nor its state writes change source rows.
    """
    if kind not in PREFIXES or not isinstance(rows, Mapping) or not run_id:
        raise ValueError("智航通知缺少有效的类型、数据或执行编号。")
    targets = notification_targets(kind)
    if not targets:
        return {"status": "skipped", "message": "卡片通知已关闭。", "targets": []}
    scheduled = _date(slot)
    if slot and scheduled is None:
        raise ValueError("智航通知的定时时点无效。")
    if kind == "inspection" and scheduled is None:
        return {"status": "skipped", "message": "手动同步巡检表不发送班次卡片。", "targets": []}
    if not rows:
        return {"status": "skipped", "message": "本批没有可通知的记录。", "targets": []}
    if store is None:
        from .state_store import LanPortalStateStore

        store = LanPortalStateStore()
    if (store.get_document(NAMESPACE, "settings:" + kind) or {}).get("notify") is False:
        return {"status": "skipped", "message": "卡片通知已关闭。", "targets": []}
    # Persist the decision before sending so retries reuse the same card, targets and UUIDs.
    key = "card:" + run_id
    planned = store.get_document(NAMESPACE, key) or _plan(kind, rows, slot, run_id, scheduled, targets, store)
    if (planned["kind"], planned["slot"]) != (kind, slot):
        raise ValueError("同一执行编号的智航通知类型或定时时点不一致。")
    if not planned["card"]:
        message = "本班次无异常巡检工单。" if kind == "inspection" else "本次同步未到卡片发送条件。"
        return {"status": "skipped", "message": message, "targets": [],
                "abnormal_count": planned["abnormal_count"], "invalid_plan_range_count": planned["invalid_plan_range_count"]}

    results = planned["results"]
    used_chats = set()
    primary_sent = False
    receipts = []
    for target in planned["targets"]:
        role = target["role"]
        if kind == "inspection" and role == "extra" and not primary_sent:
            receipts.append(dict(target, status="pending", message="主通知群投递待确认。"))
            continue
        receipt = results.get(role, {})
        chat_id = receipt.get("chat_id") or _resolve_chat(target)
        if chat_id in used_chats:
            continue
        if chat_id:
            used_chats.add(chat_id)
            if not receipt.get("chat_id"):
                results[role] = dict(target, chat_id=chat_id, status="pending")
                store.put_documents(NAMESPACE, {key: planned})
        identity = scheduled.isoformat() if scheduled else run_id
        receipt = _deliver_target(target, receipt, chat_id, planned["card"], kind, identity)
        results[role] = receipt
        receipts.append(receipt)
        if role == "primary":
            primary_sent = receipt["status"] == "sent"
        documents = {key: planned}
        if primary_sent and scheduled and kind in {"risk", "change", "event"}:
            state = store.get_document(NAMESPACE, "cadence:" + kind) or {}
            state["notified_day"] = scheduled.date().isoformat()
            if kind == "risk" and planned["complete"]:
                state["complete_month"] = scheduled.strftime("%Y-%m")
                if (scheduled + timedelta(days=1)).month != scheduled.month:
                    state["last_complete_month"] = state["complete_month"]
            documents["cadence:" + kind] = state
        store.put_documents(NAMESPACE, documents)
    sent = sum(item["status"] == "sent" for item in receipts)
    result = {"status": "sent" if sent == len(receipts) else "partial" if sent else "failed", "targets": receipts,
              "abnormal_count": planned["abnormal_count"], "invalid_plan_range_count": planned["invalid_plan_range_count"]}
    result["message" if sent == len(receipts) else "error"] = (
        f"已确认发送至 {sent} 个群。" if sent == len(receipts)
        else f"已确认发送至 {sent} 个群，另有 {len(receipts) - sent} 个群未确认。"
    )
    return result
