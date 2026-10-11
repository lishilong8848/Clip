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

from .lighthouse_ai import AssistantError, BUSINESS_QUERY, MODEL_QUESTION, explicit_general_question, safe_data, safe_text
from .lighthouse_pending import NOTICE_TYPES, pending_status, visible
from .lighthouse_sources import SCOPES, record_codes

TZ = dt.timezone(dt.timedelta(hours=8))
COUNT = re.compile(r"多少|几(?:条|个|项|份|单|件|道|台|柜|起|宗|次)|数量|总数")
DETAIL = re.compile(r"明细|详情|哪些|哪几(?:条|个|项)|列出|列一下|清单|列表|逐条|分别(?:是|哪些|什么)|具体(?:是|有)?什么|(?:还有|有)(?:什么|啥)(?:工作|任务|活|事)")
PENDING = re.compile(r"未(?:结束|完成|闭环|处理|提交|开始|发|答题|作答)|待(?:完成|处理|提交|开始|发|答题|作答)|进行中|处理中|维修中|待办|没(?:做完|完成|答题)|维修.{0,16}(?:还没好|没修好|尚未修复|没有修好)")
DATE_TOKEN = re.compile(
    r"\d{4}[-/.年]\d{1,2}[-/.月]\d{1,2}日?|\d{4}[-/年]\d{1,2}月?|(?<!\d)\d{1,2}月\d{1,2}日|(?<!\d)\d{1,2}月份?"
    r"|今天|今日|昨天|昨日|前天|明天|明日|后天|本周|这周|上周|本星期|这个星期|上星期"
    r"|本月|这个月|上个月|上月|今年|本年|去年"
    r"|(?:最近|近|过去)(?:\d{1,3}天|一个星期|一星期|一个月|一周|两周|二周|三周|四周|一月|半年|一年)"
)
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
    ("question_bank", r"题库|题目|试题|面试题|选择题", "题库资料", "仅只读查询。管理员可查完整题库；值班账号只能读取原本可见的题目和已开放答案，不自动查看答案，不办理学练操作。"),
    ("learning", r"学练|题单|答题|错题", "画像学练", "只查询原权限内题单、进度、未答数量、历史和复习资料；未发布与已发布未答分开。答题、答案提示、编辑、发布和质疑提交走原页面，不在助手办理。"),
    ("daily", r"日常|每日任务|晨会|日报|工作报告", "日常工作", "日常任务、工作报告和晨会不是事件通告，按相应日期及状态。"),
    ("convergence", r"收敛|智航|核对台|规则配置", "计划收敛审查", "规则、核对结果、告警及检修核对分开，不把命中规则数当事件数。"),
    ("people", r"人员|工号|值班账号|签名", "人员", "只提供授权姓名和工号；不提供签名图片、身份证、住址和凭证。"),
    ("history", r"通告历史|历史通告", "通告历史", "查询历史记录，不用当前未完成数据代替。"),
)


def business_domains(text):
    if explicit_general_question(text):
        return set()
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
    prompt = turn.get("prompt") or latest
    if prompt == latest:
        return latest
    if not query_refinement(latest):
        return latest
    latest_domains, prior_domains = business_domains(latest), business_domains(prompt[:-len(latest)] if prompt.endswith(latest) else prompt)
    if latest_domains and prior_domains and not latest_domains <= prior_domains:
        return latest
    if prompt.endswith(latest):
        earlier = prompt[:-len(latest)]
        if re.search(r"已(?:结束|闭环|完成)", latest):
            earlier = PENDING.sub("", earlier)
        elif PENDING.search(latest):
            earlier = re.sub(r"已(?:结束|闭环|完成)", "", earlier)
        if COUNT.search(latest):
            earlier = COUNT.sub("", earlier)
        return earlier + latest
    return prompt


def query_refinement(text):
    """Recognize bounded follow-ups, not a new subject containing '还有'."""
    text = text.strip()
    if len(text) > 150 or MODEL_QUESTION.fullmatch(text) or re.search(r"你是谁|你(?:还有|能|会|是).*能力|什么(?:模型|能力)|你好|翻译|写一|解释一下", text):
        return False
    if re.fullmatch(r'(?:请|需要|可以|那就|帮我)?(?:联网|上网)(?:查(?:询|一下)?|搜(?:索|一下)?|核对(?:一下)?|核实(?:一下)?)?[。！!？?\s]*', text):
        return True
    if re.search(r"^(?:那|那么|现在)?(?:只看|仅看|只要|仅限|也看|也包括|加上|再加上|还是|改为|改成)", text):
        return True
    date_text = re.sub(r"(?:的)?(?:呢)?[?？。！!\s]*$", "", re.sub(r"^(?:那么|那)", "", text))
    if DATE_TOKEN.fullmatch(date_text):
        return True
    if re.fullmatch(r"(?:那|那么)?(?:已结束|已闭环|已完成|未结束|未闭环|未完成|未开始|进行中|处理中)(?:的)?(?:呢|有哪些|有多少|有几条)?[?？。！!\s]*", text):
        return True
    if re.fullmatch(r"(?:还有呢|继续|详细点|再详细一点|明细呢|看明细|展开明细|第二条|第[一二三四五六七八九]条)[?？。！!\s]*", text):
        return True
    if re.fullmatch(r"(?:那|那么)?(?:请)?(?:分别(?:是)?(?:哪些|什么)|具体(?:是|有)?(?:哪些|什么|哪几(?:条|个|项))|有哪些|都是哪些|哪几(?:条|个|项)|列(?:出来|一下|出(?:明细|清单|列表)))[?？。！!\s]*", text):
        return True
    if re.fullmatch(r"(?:那|那么)?(?:还有|还剩|剩余|剩下)(?:多少|几(?:条|个|项|份|道))(?:呢|[?？。！!\s])*", text):
        return True
    return bool(re.match(r"(?:还有|也包括|刚才|上面|之前|这条|那条|这个|那个)", text) and BUSINESS_QUERY.search(text))


def read_only_question(text):
    if past_action_question(text):
        return True
    normal = text.replace("未发送", "未发").replace("未修改", "未改")
    return not re.search(r"删除|修改|发送|创建|保存|绑定|导出|上传|为什么|如何|原因|怎么", normal)


def past_action_question(text):
    """A question about completed actions is not a command to repeat them."""
    if not (COUNT.search(text) or DETAIL.search(text)):
        return False
    if re.search(r"帮我|请(?:你)?(?:把|将|再|执行)|我要|我想|然后|并且|顺便|再(?:发|更新|删除|创建)|改成|改为|设置为", text):
        return False
    action = r"发送|发布|发起|更新|修改|删除|上传|创建|新增|保存|绑定|导出|发"
    return bool(re.search(rf"(?:{action})(?:了|过)|(?:已|已经)(?:经)?(?:{action})", text))


def query_result_state(result):
    """Describe only the returned page; an empty page is not an empty database."""
    if not result.get("ok"):
        return "unavailable", "此次读取未成功，不代表没有记录；权限错误不要重试扩大范围。"
    raw = result.get("_raw", result.get("data"))
    if result.get("scope_warning") or result.get("truncated") and "_raw" not in result:
        return "partial", "结果不完整，不能按已展示条数报全量或回答没有记录。"
    if isinstance(raw, dict):
        if raw.get("complete") is False:
            return "partial", "部分资料尚未取得，分别说明已取得和无法确认的内容。"
        lists = [raw[key] for key in ("records", "items", "rows", "tasks", "pending", "ongoing") if isinstance(raw.get(key), list)]
        if lists and not any(lists):
            return "empty_page", "本次筛选或当前页没有记录，不等于全库没有。先检查分页、日期和业务分类；若有search/keyword可换一次核心词，必须保留用户的楼栋、日期、状态约束。不要反复调用同一查询。"
        if lists:
            return "found", "有候选记录，请核对标题和条件；完整数量以接口统计或完整分页为准。"
    return "read", "已取得本次接口结果；不要把资料目录、字段配置或样本长度当业务数量。"


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


class PortalOperation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    api_id: str = Field(min_length=5, description="Exact method and path returned by discover")
    params: dict = Field(default_factory=dict, description="URL query string arguments only; NOT POST/PUT/PATCH JSON fields")
    path_params: dict = Field(default_factory=dict, description="Values for path placeholders such as {record_id}")
    body: dict = Field(default_factory=dict, description="Original API JSON request body; actions, changes and notice_command/patch belong here, not in params")
    files: dict[str, list[str]] = Field(default_factory=dict, description="Owned assistant file IDs for declared native upload fields, never local paths")


def date_window(text, today=None):
    today = today or dt.datetime.now(TZ).date()
    periods = {"一周": 7, "一个星期": 7, "一星期": 7, "两周": 14, "二周": 14,
               "三周": 21, "四周": 28, "一个月": 30, "一月": 30, "半年": 183, "一年": 365}
    text = re.sub(r"(?:最近|近|过去)(一个星期|一星期|一个月|一周|两周|二周|三周|四周|一月|半年|一年)",
                  lambda match: "最近" + str(periods[match[1]]) + "天", text)
    text = re.sub(r"(?:最近|近|过去)(\d{1,3})天", r"最近\1天", text)
    for old, new in (("本星期", "本周"), ("这个星期", "本周"), ("上星期", "上周")):
        text = text.replace(old, new)
    # Last date constraint wins when the user supplements 'not today, yesterday'.
    matches = list(DATE_TOKEN.finditer(text))
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
        if re.fullmatch(r"\d{1,2}月\d{1,2}日", value):
            month, day = map(int, re.findall(r"\d+", value))
            date = dt.date(today.year, month, day)
            return date, date
        if value in {"今天", "今日", "昨天", "昨日", "前天", "明天", "明日", "后天"}:
            date = today - dt.timedelta(days={"昨天": 1, "昨日": 1, "前天": 2, "明天": -1, "明日": -1, "后天": -2}.get(value, 0))
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
        if value == "去年":
            return dt.date(today.year - 1, 1, 1), dt.date(today.year - 1, 12, 31)
        if value.startswith("最近"):
            days = int(re.search(r"\d+", value).group())
            if not 1 <= days <= 366:
                raise ValueError()
            return today - dt.timedelta(days=days - 1), today
        parts = list(map(int, re.findall(r"\d+", value)))
        year, month = (today.year, parts[0]) if len(parts) == 1 else parts
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
    from lan_bitable_template_portal.portal_service import MaintenancePortalService
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


def sent_notice_question(text):
    """Only count questions, never an instruction to send or modify a notice."""
    if not COUNT.search(text) or not business_domains(text) <= {"notices", "repair_notices", "history"}:
        return False
    if not business_domains(text) or PENDING.search(text):
        return False
    if re.search(r"请(?:再)?发|帮我|删除|修改|绑定|导出|上传|填写|为什么|如何|怎么|原因|名称|标题|涉及|包含|比较|对比|分别|各发|今天.*昨天|昨天.*今天", text):
        return False
    return bool(re.search(r"发了|已发|发出|发送|发布|发起", text))


async def collect_notice_sends(actor, start_date, end_date, invoke, *, work_types=()):
    # The native history contains successful Qt/web actions, not unsent plans.
    try:
        query = EventQuery(start_date=start_date, end_date=end_date)
    except ValueError:
        raise AssistantError("通告查询范围须为不超过366天的有效日期区间。") from None
    kinds = set(work_types) or set(NOTICE_TYPES)
    if kinds - set(NOTICE_TYPES):
        raise AssistantError("通告类型不支持，事件请使用事件通告查询。")
    scopes = sorted(set(actor["scopes"]) & SCOPES)
    if not scopes:
        raise AssistantError("当前账号没有可查询的楼栋。", 403)
    actions, notices, warnings = {}, {}, []
    deadline = time.monotonic() + 35
    month = query.start_date.replace(day=1)
    while month <= query.end_date:
        for scope in (["ALL"] if set(scopes) == set(SCOPES) else scopes):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                warnings.append("部分月份或楼栋尚未读取，完整数量暂无法确认。")
                break
            try:
                result = await asyncio.wait_for(invoke({"api_id": "GET /api/history-summary",
                    "params": {"scope": scope, "month": month.strftime("%Y-%m"), "work_type": "all"}}), min(10, remaining))
                data = result.get("_raw", result.get("data"))
                if not result.get("ok") or result.get("truncated") and "_raw" not in result or not isinstance(data, dict) or not isinstance(data.get("days"), list):
                    raise AssistantError("通告发送历史未完整返回，不能按零条统计。")
                for day in data["days"]:
                    when = event_date(day.get("date")) if isinstance(day, dict) else None
                    if when is None or not isinstance(day.get("items"), list):
                        raise AssistantError("通告历史日期或明细不完整。")
                    if not query.start_date <= when <= query.end_date:
                        continue
                    for item in day["items"]:
                        if not isinstance(item, dict):
                            raise AssistantError("通告历史记录格式不完整。")
                        kind = item.get("work_type")
                        if kind not in kinds:
                            continue
                        if not record_codes(item):
                            warnings.append("部分发送记录缺少楼栋，未计入。")
                            continue
                        if not visible(item, scopes, actor.get("allowed_scopes", scopes)):
                            continue
                        identity = item.get("target_record_id") or item.get("record_id") or item.get("active_item_id") or item.get("source_record_id") or item.get("key")
                        if item.get("actions") == [] and not any(item.get(field) for field in ("started_at", "last_updated_at", "ended_at")):
                            continue
                        if not identity or not isinstance(item.get("actions"), list) or not item["actions"]:
                            warnings.append("部分通告缺少完整的发送动作记录，数量暂无法确认。")
                            continue
                        for action in item["actions"]:
                            if not isinstance(action, dict) or action.get("action") not in {"start", "update", "end"}:
                                continue
                            sent = event_date(action.get("time"))
                            if sent is None:
                                warnings.append("部分发送记录缺少实际发送时间，未计入。")
                                continue
                            if not query.start_date <= sent <= query.end_date:
                                continue
                            record_key = (kind, str(identity))
                            action_key = (kind, str(action.get("job_id") or "") or (str(identity), action["action"], str(action["time"]), str(action.get("text") or action.get("progress") or "")))
                            actions[action_key] = action["action"]
                            notices[record_key] = {"title": safe_text(item.get("title") or "通告"), "work_type": kind,
                                "scopes": sorted(record_codes(item) & set(scopes))}
            except (AssistantError, asyncio.TimeoutError) as exc:
                warnings.append(f"{scope} / {month:%Y-%m}：{safe_text(str(exc)) or '读取超时，数量未知'}")
        if time.monotonic() >= deadline:
            break
        month = (month.replace(day=28) + dt.timedelta(days=4)).replace(day=1)
    return {"kind": "notice_sends", "scopes": scopes, "start_date": query.start_date.isoformat(), "end_date": query.end_date.isoformat(),
        "complete": not warnings, "count": len(notices) if not warnings else None, "known_count": len(notices),
        "send_count": len(actions), "actions": {kind: sum(value == kind for value in actions.values()) for kind in ("start", "update", "end")},
        "by_type": {NOTICE_TYPES[kind]: sum(key[0] == kind for key in notices) for kind in NOTICE_TYPES if kind in kinds},
        "items": list(notices.values())[:20], "remaining": max(0, len(notices)-20), "warnings": list(dict.fromkeys(warnings)),
        "url": "/daily-tasks?" + urlencode({"scope": scopes[0] if len(scopes) == 1 else "ALL", "date": query.start_date.isoformat()})}


def notice_sends_reply(data, *, details=False):
    dates = data["start_date"] + (" 至 " + data["end_date"] if data["start_date"] != data["end_date"] else "")
    scope = "、".join(code + ("站" if code == "110" else "楼") for code in data["scopes"])
    count = f"已发送 **{data['count']} 条通告，共 {data['send_count']} 次**" if data["complete"] else f"已核对 **{data['known_count']} 条通告**，完整数量暂无法确认"
    lines = [f"{scope} {dates}：{count}。[1]"]
    counts = data["actions"]
    if data["complete"] and data["send_count"]:
        lines.append(f"开始 {counts['start']} 次，更新 {counts['update']} 次，结束 {counts['end']} 次。")
    lines.append("按本机成功发送记录统计，同一通告去重；不含事件通告和未发送的计划。")
    if details:
        for row in data["items"]:
            label = NOTICE_TYPES[row["work_type"]] + " · " + row["title"]
            lines.append("- " + re.sub(r"([\\`*_\[\]()])", r"\\\1", html.escape(label).replace("\n", " ")))
        if data["remaining"]:
            lines.append(f"另有 {data['remaining']} 条，可在每日任务中查看。")
    lines.extend(data["warnings"])
    return "\n\n".join(lines)


def query_result_page(data, path, offset=0, limit=20):
    """Bounded page over an already-loaded native list, with per-key safe traversal.

    No fetching, network, writes, eval, SQL or snapshot store. loaded_count is the
    number of items already present in ``data``, not a database total. Traversal
    follows source references without copying; only the returned slice is sanitized.
    """
    from .lighthouse_agent import _read_path

    if not isinstance(path, list) or len(path) > 12:
        raise AssistantError("查询路径无效。")
    if not all(isinstance(seg, str) and seg and len(seg) <= 200 for seg in path):
        raise AssistantError("查询路径段无效。")
    if type(offset) is not int or isinstance(offset, bool) or offset < 0:
        raise AssistantError("分页偏移无效。")
    if type(limit) is not int or isinstance(limit, bool) or not 1 <= limit <= 40:
        raise AssistantError("分页大小无效。")
    selected = data
    for name in path:
        if isinstance(selected, dict) and name in selected:
            probe = selected[name] if name == "signature_time" else None
            allowed = safe_data({name: probe})
            if not allowed or name == "path" or name.lower() == "relative_path" or name.startswith("_"):
                raise AssistantError("不能读取受限字段：" + name)
            _read_path({name: probe}, [name])
            selected = selected[name]
        elif isinstance(selected, (list, tuple)) and name.isascii() and name.isdigit() and int(name) < len(selected):
            selected = selected[int(name)]
        else:
            raise AssistantError("未返回需要的关联字段：" + name)
    if not isinstance(selected, (list, tuple)):
        raise AssistantError("查询结果必须是列表。")
    total = len(selected)
    page = safe_data(selected[offset:offset + limit])
    next_offset = offset + limit if offset + limit < total else None
    return {"items": page, "loaded_count": total, "offset": offset, "next_offset": next_offset,
            "has_more": next_offset is not None}
