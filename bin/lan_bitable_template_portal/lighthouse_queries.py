"""Business meanings and exact event counts for the assistant, not business writes."""
import asyncio
import calendar
import datetime as dt
import html
import re
import time
from typing import Literal
from urllib.parse import urlencode

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .lighthouse_ai import AssistantError, safe_text
from .lighthouse_pending import pending_status, visible
from .lighthouse_sources import SCOPES, record_codes

TZ = dt.timezone(dt.timedelta(hours=8))
COUNT = re.compile(r"多少|几(?:条|个|项|份|单|件|道|台|柜|起|宗|次)|数量|总数")
DETAIL = re.compile(r"明细|详情|哪些|列出|清单|列表|逐条|分别是")
PENDING = re.compile(r"未(?:结束|完成|闭环|处理|提交|开始|发|答题|作答)|待(?:完成|处理|提交|开始|发|答题|作答)|进行中|处理中|维修中|待办|没(?:做完|完成|答题)")
# One vocabulary for routing and model instructions. Broad 'work' never wins over a named module.
DOMAINS = (
    ("events", r"事件|事件通告|事件通报", "事件通告", "发生数量按事件发生时间，含已结束事件；未闭环才按当前状态。不是检修或工作汇总。"),
    ("repairs", r"维修(?:单|项目|管理)?", "维修项目", "维修项目以自身record_id计数，不按关联事件去重；当前维修进度来自维修单及跟进。"),
    ("followups", r"跟进记录|维修跟进", "维修跟进", "一条跟进是一条跟进记录，不是一个维修项目。"),
    ("repair_notices", r"检修", "检修计划与通告", "未发=待开始计划；未结束=已开始检修通告；泛问检修工作才补充维修项目，分别计数。"),
    ("notices", r"维保|变更|轮巡|调整|通告", "其他通告", "按维保/变更/检修/轮巡/调整/上下电分别查询；计划、未结束及历史记录分开。"),
    ("mops", r"维护单|MOP", "维护单", "维护单、生成文件与维保通告是不同对象，不混计。"),
    ("orders", r"SOP|工单|操作步骤", "SOP及工单", "SOP是模板，工单是执行实例，步骤不是工单；查询时区分模板/工单/步骤。"),
    ("batches", r"机柜|包间|上下电待办|正式电|测试电", "机柜上下电", "区分机柜存量状态、操作次数、待办批次及批次内机柜数。"),
    ("water", r"水耗|水表|用水", "水耗", "耗水量需单位和日期；已录入水耗不是未完成任务。"),
    ("drills", r"演练", "演练", "区分演练定义、楼栋执行任务、记录表和评估表；任务按执行状态。"),
    ("guard", r"重保", "重保", "区分重保任务和楼栋检查表；未提交检查表是待办。"),
    ("learning", r"学练|题单|题库|答题|错题", "画像学练", "题目、题单、已答题数及完成率分开；只统计当前楼栋账号。"),
    ("daily", r"日常|每日任务|晨会|日报|工作报告", "日常工作", "日常任务、工作报告和晨会不是事件通告，按相应日期及状态。"),
    ("convergence", r"收敛|智航|核对台|规则配置", "计划收敛审查", "规则、核对结果、告警及检修核对分开，不把命中规则数当事件数。"),
    ("people", r"人员|工号|值班账号|签名", "人员", "只提供授权姓名和工号；不提供签名图片、身份证、住址和凭证。"),
    ("history", r"通告历史|历史通告", "通告历史", "查询历史记录，不用当前未完成数据代替。"),
)


def business_domains(text):
    found = {key for key, pattern, _, _ in DOMAINS if re.search(pattern, text, re.I)}
    if found & {"events", "repair_notices"} and not re.search(r"维保|变更|轮巡|调整|上电通告|下电通告|其他通告", text):
        found.discard("notices")
    if "followups" in found:
        found.discard("repairs")
    if "history" in found:
        found.discard("notices")
    return found


def semantic_context(text):
    selected = business_domains(text)
    entries = DOMAINS if not selected else [entry for entry in DOMAINS if entry[0] in selected]
    return "\n".join(f"{label}：{meaning}" for _, _, label, meaning in entries)


def effective_question(turn):
    latest = turn["question"]
    # A new explicit subject supersedes an interrupted general summary. Scope-only supplements retain the subject.
    return latest if business_domains(latest) else (turn.get("prompt") or latest)


def read_only_question(text):
    normal = text.replace("未发送", "未发").replace("未修改", "未改")
    return not re.search(r"删除|修改|发送|创建|保存|绑定|导出|上传|为什么|如何|原因|怎么", normal)


def current_pending_query(text, today=None):
    """'Today's outstanding work' is a current backlog, not today's new records."""
    if not PENDING.search(text):
        return False
    window = date_window(text, today)
    if window is None:
        return True
    today = today or dt.datetime.now(TZ).date()
    if window != (today, today):
        return False
    return not re.search(r"发生|新建|新增|创建|发布|下发|分配|开始时间|发生时间|创建时间", text)


def all_pending_modules(text):
    return bool(re.search(r"(?:所有|全部|各个|各)(?:的)?(?:功能|模块|业务)|各类(?:工作|任务)|(?:所有|全部)(?:未完成|未结束)?(?:的)?(?:工作|任务)", text))


class ReadOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    api_id: str = Field(min_length=5, description="Exact method and path returned by discover, e.g. GET /api/events/monthly")
    params: dict = Field(default_factory=dict, description="Query string arguments")
    path_params: dict = Field(default_factory=dict, description="Values for path placeholders")
    body: dict = Field(default_factory=dict)
    files: dict[str, list[str]] = Field(default_factory=dict, description="Owned assistant file IDs for read-only parsers, never local paths")


def date_window(text, today=None):
    today = today or dt.datetime.now(TZ).date()
    # Last date constraint wins when the user supplements 'not today, yesterday'.
    matches = list(re.finditer(r"\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}日?|今天|今日|昨天|昨日|前天|本周|这周|上周|本月|这个月|上个月|上月|今年|本年|最近\d{1,3}天|\d{4}[-/年]\d{1,2}月?", text))
    if not matches:
        return None
    value = matches[-1].group()
    full = r"\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}日?"
    def parsed(raw):
        return dt.date(*map(int, re.findall(r"\d+", raw)))
    try:
        if re.fullmatch(full, value):
            end = parsed(value)
            if len(matches) > 1 and re.fullmatch(full, matches[-2].group()) and re.fullmatch(r"\s*(?:至|到|~|～|—|-)\s*", text[matches[-2].end():matches[-1].start()]):
                return parsed(matches[-2].group()), end
            return end, end
        if value in {"今天", "今日", "昨天", "昨日", "前天"}:
            date = today - dt.timedelta(days={"昨天": 1, "昨日": 1, "前天": 2}.get(value, 0))
            return date, date
        if value in {"本周", "这周", "上周"}:
            start = today - dt.timedelta(days=today.weekday() + (7 if value == "上周" else 0))
            return start, start + dt.timedelta(days=6) if value == "上周" else today
        if value in {"本月", "这个月"}:
            return today.replace(day=1), today
        if value in {"上月", "上个月"}:
            end = today.replace(day=1) - dt.timedelta(days=1)
            return end.replace(day=1), end
        if value in {"今年", "本年"}:
            return today.replace(month=1, day=1), today
        if value.startswith("最近"):
            days = int(re.search(r"\d+", value).group())
            if not 1 <= days <= 366:
                raise ValueError()
            return today - dt.timedelta(days=days - 1), today
        year, month = map(int, re.findall(r"\d+", value))
        return dt.date(year, month, 1), dt.date(year, month, calendar.monthrange(year, month)[1])
    except ValueError:
        raise AssistantError("日期无效，请填写明确的年月日。") from None


class EventQuery(BaseModel):
    """An inclusive Beijing occurrence-date range, never a work-summary date."""
    model_config = ConfigDict(extra="forbid")
    start_date: dt.date
    end_date: dt.date
    date_field: Literal["occurrence_time", "end_time"] = "occurrence_time"
    status: Literal["all", "open", "closed"] = "all"
    level: str = Field(default="", max_length=20)
    keyword: str = Field(default="", max_length=100)

    @model_validator(mode="after")
    def valid_range(self):
        if not 0 <= (self.end_date - self.start_date).days <= 366:
            raise ValueError("事件查询范围须为不超过366天的有效日期区间")
        return self


def event_date(value):
    value = str(value or "").strip().replace("：", ":")
    if not value:
        return None
    try:
        # Offset timestamps must be compared as Beijing calendar dates.
        if re.search(r"(?:Z|[+-]\d\d:\d\d)$", value):
            return dt.datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(TZ).date()
        matched = re.match(r"(\d{4})[-/.年](\d{1,2})[-/.月](\d{1,2})(?:日|(?=$|[^0-9]))", value)
        return dt.date(*map(int, matched.groups())) if matched else None
    except ValueError:
        return None


async def collect_events(actor, query: EventQuery, invoke):
    scopes = sorted(set(actor["scopes"]) & SCOPES)
    if not scopes:
        raise AssistantError("当前账号没有可查询的楼栋。", 403)
    candidates, warnings, refreshed = {}, [], []
    deadline = time.monotonic() + 35
    month = query.start_date.replace(day=1)
    while month <= query.end_date:
        for scope in (["ALL"] if set(scopes) == set(SCOPES) else scopes):
            params = {"scope": scope, "month": month.strftime("%Y-%m")}
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                warnings.append("本轮查询时间已到，剩余月份尚未读取，完整数量无法确认")
                break
            try:
                result = await asyncio.wait_for(invoke({"api_id": "GET /api/events/monthly", "params": params}), min(10, remaining))
                if not result.get("ok") or result.get("truncated") and "_raw" not in result:
                    raise AssistantError("事件数据未完整返回")
                data = result.get("_raw", result.get("data"))
                if not isinstance(data, dict) or not isinstance(data.get("records"), list) or not data.get("snapshot_exists"):
                    raise AssistantError("事件月份资料尚未初始化，无法确认数量")
                if data.get("config_missing"):
                    warnings.append("事件来源配置不完整，使用已有本地快照")
                failed = data.get("last_failed") or {}
                if failed and float(failed.get("updated_at") or 0) >= float(data.get("last_refreshed_at") or 0):
                    warnings.append("事件来源上次同步未成功，数量可能不是最新")
                if data.get("last_refreshed_at"):
                    refreshed.append(float(data["last_refreshed_at"]))
                for item in data["records"]:
                    if not isinstance(item, dict):
                        raise AssistantError("事件资料格式不完整")
                    if not record_codes(item):
                        warnings.append("部分事件缺少楼栋信息，未计入")
                        continue
                    if not visible(item, scopes, actor.get("allowed_scopes", scopes)) or item.get("deleted_at"):
                        continue
                    identity = str(item.get("record_id") or "")
                    if not identity:
                        warnings.append("部分事件缺少记录编号，无法确认完整数量")
                        continue
                    version = float(data.get("last_refreshed_at") or 0)
                    old = candidates.get(identity)
                    if old is None or version > old[0]:
                        candidates[identity] = (version, item)
                    elif version == old[0] and any(old[1].get(key) != item.get(key) for key in ("occurrence_time", "end_time", "status", "level")):
                        warnings.append("同一事件的月份快照状态不一致，完整数量待核对")
            except (AssistantError, asyncio.TimeoutError) as exc:
                warnings.append(f"{scope} / {params['month']}：{safe_text(str(exc)) or '读取超时，无法确认数量'}")
        if time.monotonic() >= deadline:
            warnings.append("本轮查询时间已到，结果可能不完整，数量待核对")
            break
        month = (month.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    rows = []
    for identity, (_, item) in candidates.items():
        when = event_date(item.get(query.date_field))
        if when is None:
            if query.date_field == "occurrence_time" or item.get("end_time"):
                warnings.append("部分事件缺少有效统计时间，无法确认完整数量")
            continue
        if not query.start_date <= when <= query.end_date:
            continue
        closed = bool(item.get("end_time")) or not pending_status(item.get("status"))
        if query.status == "open" and closed or query.status == "closed" and not closed:
            continue
        if query.level and str(item.get("level", "")).upper() != query.level.upper():
            continue
        if query.keyword and query.keyword.casefold() not in str(item.get("title") or item.get("alarm_desc") or "").casefold():
            continue
        rows.append({"record_id": identity, "title": safe_text(item.get("title") or item.get("alarm_desc") or identity),
            "occurrence_time": safe_text(item.get("occurrence_time")), "end_time": safe_text(item.get("end_time")),
            "status": safe_text(item.get("status") or "处理中"), "scopes": sorted(record_codes(item) & set(scopes))})
    rows.sort(key=lambda row: (row[query.date_field], row["record_id"]))
    from .portal_service import MaintenancePortalService
    stats = MaintenancePortalService._event_stats_for_records(rows)
    return {"kind": "event_notices", "scopes": scopes, "query": query.model_dump(mode="json"),
        "count": len(rows) if not warnings else None, "known_count": len(rows), "complete": not warnings,
        "items": rows[:20], "remaining": max(0, len(rows) - 20), "warnings": list(dict.fromkeys(warnings)),
        "stats": {key: stats[key] for key in ("total", "processing", "pending", "ended", "statuses")},
        "last_refreshed_at": min(refreshed) if refreshed else None,
        "url": "/event-management?" + urlencode({"scope": scopes[0] if len(scopes) == 1 else "ALL", "month": query.start_date.strftime("%Y-%m")})}


def event_reply(data, *, details=False, status_details=False):
    query = data["query"]
    dates = query["start_date"] + ("至" + query["end_date"] if query["start_date"] != query["end_date"] else "")
    scope = "、".join(code + ("站" if code == "110" else "楼") for code in data["scopes"])
    qualifier = {"all": "", "open": "、目前未闭环", "closed": "、目前已闭环"}[query["status"]]
    count = f"**{data['count']} 条事件通告**" if data["complete"] else f"已核对 **{data['known_count']} 条事件通告，完整数量暂无法确认**"
    verb = "结束" if query.get("date_field") == "end_time" else "发生"
    lines = [f"{scope}在 {dates} {verb}{qualifier}的事件：{count}。[1]", f"按事件{verb}时间统计，不含检修、维修单等其他工作。"]
    if status_details:
        stats = data.get("stats") or {}
        lines.append(("其中" if data["complete"] else "已核对记录中") + f"处理中 **{stats.get('processing', 0)} 条**，已结束 **{stats.get('ended', 0)} 条**。")
    if details:
        for row in data["items"]:
            text = f"{row[query.get('date_field', 'occurrence_time')]} · {row['title']}（{row['status']}）"
            lines.append("- " + re.sub(r"([\\`*_\[\]()])", r"\\\1", html.escape(text).replace("\n", " ")))
        if data["remaining"]:
            lines.append(f"另有 {data['remaining']} 条，可在事件管理中查看。")
    if data.get("last_refreshed_at"):
        lines.append("数据同步于 " + dt.datetime.fromtimestamp(data["last_refreshed_at"], TZ).strftime("%Y-%m-%d %H:%M") + "。")
    lines.extend(data["warnings"])
    return "\n\n".join(lines)
