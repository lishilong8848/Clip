"""Business meanings and exact event counts for the assistant, not business writes."""
import asyncio
import calendar
import copy
import datetime as dt
import html
import json
import re
import time
from typing import Literal
from urllib.parse import urlencode

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .lighthouse_ai import AssistantError, BUSINESS_QUERY, MODEL_QUESTION, explicit_general_question, safe_data, safe_text
from .lighthouse_pending import NOTICE_TYPES, pending_status, visible
from .lighthouse_sources import SCOPES, record_codes, record_title

TZ = dt.timezone(dt.timedelta(hours=8))
QUERY_TIME_GUIDANCE = (
    'reference_time仅为本轮理解今天/昨天等相对日期的北京时间基准，不是查询、统计或数据更新时间。'
    '每条来源的queried_at仅表示该份资料快照的读取时间，不代表云端同步、数据生效或统计截止时间。'
    '缓存与列表续读沿用原快照读取时间，不能称为刚刚重新查询。'
    '统计日期、班次、发生时间、更新时间及同步时间分别按原字段说明；不同来源的时间不能互相替代。'
    '未提供的时间保持未知，不从reference_time、历史回答或其他来源补造；没有必要无需附带时间。')
LEARNING_COMPLETION_MEANING = (
    'GET /api/learning/completion按排班日统计：total是人员日记录数，summary是整个人员日的状态分布，不是参与人数。'
    'shift只筛选含该班次任务的人员日，不裁剪tasks；行answered和status仍可能包含其他班次，不能直接当作所选班次结果。'
    '须按tasks中的scope、shift核对answered，已答大于0才算该班次参与，按person_id去重；仅待答、开始或已发送不算参与。'
    '夜班跨零点仍属原排班日；自主练习按实际提交日另列，任务班次或资料未确认时说明限制，不推断为零。')
LEARNING_PARTICIPATION_MEANING = (
    'learning_participation是按实际提交日期的参与统计，不按班次。summary.participants及participants为已去重人数与名单；'
    'answered是首次作答题数，practice是重复练习题次，answer_attempts是二者合计。'
    'answered=0但practice>0的人员也参与了答题，不能排除或称其从未作答；若只问首次作答人员才按answered>0另计。'
    '本统计不套用completion的班次任务规则；范围、完整性和姓名缺失仍按实际回执说明。')


def source_field_contract(api_id='', source_kind=''):
    """Program-owned field meanings, selected by source identity, never question text."""
    if api_id is not None and not isinstance(api_id, str):
        return None
    paths = {
        'GET /api/cabinet-power/overview': (['racks[].state'], ['counts', 'rooms[].counts']),
        'GET /api/cabinet-power/rooms': (['racks[].state'], ['counts', 'rooms[].counts']),
        'GET /api/cabinet-power/racks': (['items[].state'], []),
        'GET /api/cabinet-power/rooms/{room_id}/layout': (['racks[].state'], ['summary']),
    }.get(api_id)
    if not api_id and source_kind == 'local_cabinet_inventory':
        paths = (['state'], ['counts'])
    if paths is None:
        return None
    from lan_bitable_template_portal.cabinet_power_batches import POWER_STATE_LABELS
    return {
        'subject': '机柜台账供电状态', 'count_unit': '机柜',
        'state_fields': paths[0], 'count_fields': paths[1],
        'state_labels': {**POWER_STATE_LABELS, 'unknown': '待核实'},
        'state_source_labels': {'operation': '成功操作记录推导', 'baseline': '保存的基线',
                                'unconfirmed': '尚未确认', 'empty': '无有效操作记录且无基线'},
        'relations': ['已上电=正式电(formal)+测试电(test)。',
            '上电率=已上电/机柜总数(total)，分母包含待核实；总数为0或缺失时比例未知。',
            'powered是formal与test的合计，不能再次加到状态分类中；存量数不是操作次数、机房数或服务器台数。'],
        'limits': ['off包含未上电和已下电，仅凭off不能推断发生过下电或其时间。',
            'count_unit仅用于count_fields；rooms列表条数是机房数，record_count是业务记录数，不套用机柜单位。',
            '供电台账状态不证明服务器或业务系统已关机、在运行或健康，也不是实时电力测量。',
            'state_source为empty或baseline时不推断存在成功操作；动作和时间须核对原操作证据。',
            '机柜列表可能仅含已定位条目，不能用样本条数替代原生概览或机房汇总的total。',
            'unknown及未识别状态不得归入off；未配置、未就绪、缺失字段和读取失败不等于实际零。',
            '字段路径相对每楼原响应；列表片段须结合原path定位。只解释本来源字段，不证明已读全或授予权限。'],
    }
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
    ("learning", r"学练|题单|答题|错题", "画像学练", "题单status=pending表示尚未完成答题，不是待发布；completed表示已完成答题。stats.total是有效题数，stats.answered是有效已答题数，二者差为剩余题数；题单列表total是题单条数，不是题数或参与人数。发布和消息投递须看相应回执，不能从答题状态推断。只查询题单、进度、历史和复习资料；答题、答案提示、编辑、发布和质疑提交走原页面，不在助手办理。"
     "参与统计先区分实际提交日期与排班日/班次：前者用learning_participants或GET /api/learning/profile(view=participation)，后者用GET /api/learning/completion。不能只用completion的tasks/answered替代实际提交统计；各来源按自身query_guidance解释，不能互套口径。"),
    ("daily", r"日常|每日任务|晨会|日报|工作报告", "日常工作", "日常任务、工作报告和晨会不是事件通告，按相应日期及状态。"),
    ("convergence", r"收敛|智航|核对台|规则配置", "计划收敛审查", "规则、核对结果、告警及检修核对分开，不把命中规则数当事件数。"),
    ("people", r"人员|工号|值班账号|排班|白班|夜班|当班|上班|出勤|签名", "人员", "排班人数与名单必须读取对应日期和班次的真实资料，不等同实时签到。只提供授权姓名和工号；不提供签名图片、身份证、住址和凭证。"),
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


def named_clarification_reply(question, previous):
    """A full name from the last authorized result is a choice, not a new task."""
    name = question.strip(' \t\r\n"\'“”‘’')
    if not 2 <= len(name) <= 300:
        return False
    pending = [source.get('data') for source in previous.get('sources', []) if source.get('available', True)]
    for _ in range(2000):
        if not pending:
            break
        item = pending.pop()
        if isinstance(item, dict):
            fields = item.get('display_fields') or item.get('fields') or {}
            if isinstance(fields, dict) and safe_text(record_title(item, '')).strip() == name:
                return True
            pending.extend(value for value in item.values() if isinstance(value, (dict, list)))
        elif isinstance(item, list):
            pending.extend(item[:100])
    return False


def effective_question(turn):
    latest = turn["question"]
    prompt = turn.get("prompt") or latest
    if prompt == latest:
        return latest
    if turn.get('_clarification_reply'):
        return prompt
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
    # A bare read retry names no subject or date; it only repeats the already authorized question.
    if re.fullmatch(r'(?:(?:请|麻烦|帮我|帮忙|再|你|您|先|继续|重新)[，,]?\s*)*'
                    r'(?:查看|查阅|查找|查查|查|检索|核实|核对|确认|复查|复核|看看)(?:一下|一遍|下)?'
                    r'(?:再|又)?(?:查看|查阅|查找|查查|查|检索|核实|核对|确认|复查|复核|看看)?(?:一下|一遍|下)?'
                    r'(?:吧|呢|嘛)?[。！!？?\s]*', text):
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
    if not result.get("ok") or result.get("available") is False:
        return "unavailable", "此次读取未成功，不代表没有记录；权限错误不要重试扩大范围。"
    raw = result.get("_raw", result.get("data"))
    source_note = (" 学练history含待答题单，total是题单条数，不是参与人数；参与名单及人数须查询GET /api/learning/profile（view=participation），不能从history推算。"
                     if result.get("api_id") == "GET /api/learning/history" else "")
    if result.get('api_id') == 'GET /api/learning/completion':
        source_note = ' ' + LEARNING_COMPLETION_MEANING
    elif isinstance(raw, dict) and (raw.get('source_kind') if raw.get('snapshot_only') is True else raw.get('kind')) == 'learning_participation':
        source_note = ' ' + LEARNING_PARTICIPATION_MEANING
    contract = source_field_contract(result.get('api_id'), result.get('source_kind'))
    if contract:
        source_note = ' 程序字段口径（不是当前记录或权限依据）：' + json.dumps(contract, ensure_ascii=False, separators=(',', ':'))
    if result.get("scope_warning") or result.get("complete") is False or result.get("truncated") and "_raw" not in result:
        return "partial", "结果不完整，不能按已展示条数报全量或回答没有记录。" + source_note
    if isinstance(raw, dict):
        if raw.get("available") is False:
            return "unavailable", "此次资料未取得，不代表没有记录。" + source_note
        collected = raw.get("query_completeness")
        if isinstance(collected, dict):
            lists = collected.get("lists")
            if (collected.get("complete") is not True or not isinstance(lists, dict) or not lists
                    or any(not isinstance(item, dict) or item.get("complete") is not True
                           or type(item.get("count")) is not int or item["count"] < 0
                           or item["count"] != item.get("declared_total") or item["count"] != item.get("loaded_count")
                           or not isinstance(raw.get(key), list) or len(raw[key]) != item["count"] for key, item in lists.items())):
                return "partial", "分页尚未完整读取，只能报告已知记录下限；完整数量未知。" + source_note
            if lists and all(item.get("count") == 0 for item in lists.values()):
                return "empty", "本次授权范围和筛选条件已完整读取，结果确认为零；不代表其他范围或日期没有记录。" + source_note
            return "found", "已完整读取本次筛选的记录；记录数、去重人数、作答题次和题目数量须分别报告。" + source_note
        if raw.get("snapshot_exists") is False or raw.get("source_snapshot_ready") is False:
            return "unavailable", "来源快照尚未就绪，不能把资料缺失解释为零条。" + source_note
        if raw.get("warnings") or raw.get("schema_warnings"):
            return "partial", "来源存在资料警告，完整数量未知；不能把缺失资料当零条。" + source_note
        if raw.get("list_complete") is False:
            return "partial", "当前名单未完整保留；统计完整性另看complete和count，不能把已展示样本当完整名单。" + source_note
        for key in ("records", "ongoing"):
            rows, meta = raw.get(key), raw.get(key + "_pagination")
            if isinstance(rows, list) and isinstance(meta, dict) and (
                    meta.get("has_more") or type(meta.get("total")) is int and meta["total"] > len(rows)
                    or type(meta.get("page")) is int and meta["page"] > 1):
                return "partial", "当前工作台列表只是一页，请保留楼栋、类型及日期条件补读缺失页；计划与进行中通告分别计数。"
        if (raw.get("complete") is False or raw.get("truncated") or raw.get("has_more")
                or raw.get("next_offset") is not None):
            return "partial", "资料尚未读取完整，保留原查询条件继续读取后页；不能按当前名单或total推断完整人数。" + source_note
        lists = [raw[key] for key in ("records", "items", "rows", "tasks", "pending", "ongoing", "participants") if isinstance(raw.get(key), list)]
        if lists and not any(lists):
            if raw.get("complete") is True and type(raw.get("total")) is int and raw["total"] == 0 and raw.get("page", 1) == 1:
                return "empty", "本次条件已完整读取，结果确认为零；范围与来源日期仍以本次查询为准。" + source_note
            return "empty_page", "本次筛选或当前页没有记录，不等于全库没有。先检查分页、日期和业务分类；若有search/keyword可换一次核心词，必须保留用户的楼栋、日期、状态约束。不要反复调用同一查询。" + source_note
        if lists:
            if (type(raw.get("total")) is int and raw["total"] > max(map(len, lists))
                    or type(raw.get("page")) is int and raw["page"] > 1
                    or type(raw.get("remaining")) is int and raw["remaining"] > 0):
                return "partial", "当前只取得部分记录，须按原条件读取完整分页；total只代表接口声明的记录数，不是完整名单或去重人数。" + source_note
            return "found", "有候选记录，请核对标题和条件；完整数量以接口统计或完整分页为准。" + source_note
    return "read", "已取得本次接口结果；不要把资料目录、字段配置或样本长度当业务数量。" + source_note


# Only native list contracts whose pagination has been checked here may be followed.
# (list key, pagination key, position parameter, size parameter, count unit)
QUERY_PAGE_SCHEMAS = {
    "GET /api/workbench": (
        ("records", "records_pagination", "records_page", "records_page_size", "source_records"),
        ("ongoing", "ongoing_pagination", "ongoing_page", "ongoing_page_size", "notices"),
    ),
    "GET /api/repair-management/records": (("records", "", "offset", "limit", "repair_records"),),
    "GET /api/assistant/work-orders": (("items", "", "page", "page_size", "work_orders"),),
    "GET /api/assistant/question-bank": (("items", "", "page", "page_size", "question_versions"),),
    "GET /api/learning/history": (("items", "", "page", "page_size", "papers"),),
    "GET /api/learning/completion": (("items", "", "page", "page_size", "person_days"),),
    "GET /api/learning/papers": (("items", "", "page", "page_size", "papers"),),
}


def query_has_pending_pages(api_id, result):
    """Only declared source pagination, never missing data or snapshot slices."""
    data = result.get('_raw', result.get('data'))
    if (not result.get('ok') or result.get('available') is False or result.get('complete') is False
            or result.get('scope_warning') or result.get('truncated') and '_raw' not in result
            or not isinstance(data, dict) or data.get('available') is False or data.get('complete') is False
            or data.get('truncated') or data.get('snapshot_only') or data.get('warnings') or data.get('schema_warnings')
            or data.get('snapshot_exists') is False or data.get('source_snapshot_ready') is False):
        return False
    for key, pagination, position, _, _ in QUERY_PAGE_SCHEMAS.get(api_id, ()):
        rows, meta = data.get(key), data.get(pagination) if pagination else data
        if not isinstance(rows, list) or not isinstance(meta, dict):
            continue
        total = meta.get('total')
        cursor = meta.get(position, 0 if position == 'offset' else 1)
        if type(total) is int and total > len(rows) and type(cursor) is int:
            return True
    return False


async def collect_query_pages(operation, invoke, *, first_result=None, max_pages=20,
                              max_items=2000, timeout=35):
    """Complete a native list through the caller's scoped/authenticated read callback.

    Returns the ordinary result envelope, retaining full lists in _raw and a safe
    display sample in data. first_result must be THIS operation's freshly authorized
    result from this turn, never another user's or a persisted/global cached result.
    There is no cache here: the callback owns authentication on every missing page.
    Limits apply across all requested lists; authorization failures discard prior data.
    """
    op = operation.model_dump() if isinstance(operation, PortalOperation) else copy.deepcopy(operation)
    op = PortalOperation.model_validate(op).model_dump()
    specs = QUERY_PAGE_SCHEMAS.get(op["api_id"])
    if not specs or op["body"] or op["files"] or op["path_params"]:
        raise AssistantError("此接口没有受支持的只读完整分页规则，请使用原查询或read_query。", 422)
    if any(key in op["params"] and str(op["params"][key]).lower() not in {"", "0", "false", "none"}
           for key in ("refresh", "force_refresh", "reload", "force_reload", "sync")):
        raise AssistantError("完整读取不能触发管理刷新或业务同步。", 403)
    if (type(max_pages) is not int or not 1 <= max_pages <= 20
            or type(max_items) is not int or not 1 <= max_items <= 2000
            or isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 35):
        raise AssistantError("完整分页读取预算无效。")
    if op["api_id"] == "GET /api/workbench" and op["params"].get("sections"):
        sections = {section.strip() for section in str(op["params"]["sections"]).split(",")}
        specs = tuple(spec for spec in specs if spec[0] in sections)
        if not specs:
            raise AssistantError("所选工作台区段不支持完整列表读取。", 422)
    if first_result is not None and not isinstance(first_result, dict):
        raise AssistantError("已有分页结果格式无效。", 422)
    for _, pagination, position, size_param, _ in specs:
        initial = (first_result or {}).get("_raw", (first_result or {}).get("data"))
        initial_meta = initial.get(pagination) if pagination and isinstance(initial, dict) else initial
        default_size = initial_meta.get("page_size", initial_meta.get("limit")) if isinstance(initial_meta, dict) else None
        op["params"].setdefault(size_param, default_size if type(default_size) is int and default_size > 0 else 40)
        for param, minimum in ((position, 0 if position == "offset" else 1), (size_param, 1)):
            value = op["params"].get(param, minimum)
            if isinstance(value, str) and value.isascii() and value.isdigit():
                value = int(value)
            if type(value) is not int or value < minimum:
                raise AssistantError("分页位置或大小无效。", 422)
            op["params"][param] = value
    deadline, calls, item_count = time.monotonic() + timeout, int(first_result is not None), 0
    base, envelope, summaries, freshness, warnings = {}, {}, {}, [], []
    snapshot = None

    def raw_page(result):
        if not isinstance(result, dict):
            raise AssistantError("分页未返回有效读取结果。", 502)
        if not result.get("ok") or result.get("available") is False:
            raise AssistantError(safe_text(result.get("error") or "分页读取失败。"), result.get("status") or 503)
        if result.get("complete") is False or result.get("scope_warning") or result.get("truncated") and "_raw" not in result:
            raise AssistantError("分页范围不完整或内容已截断，完整数量未知。", 502)
        data = result.get("_raw", result.get("data"))
        if (not isinstance(data, dict) or data.get("available") is False or data.get("complete") is False or data.get("truncated")
                or data.get("snapshot_exists") is False or data.get("source_snapshot_ready") is False):
            raise AssistantError("来源快照缺失或不完整，不能按零条统计。", 503)
        if data.get("warnings") or data.get("schema_warnings"):
            raise AssistantError("来源报告了资料警告，完整数量待核对。", 503)
        return data

    # Reuse a supplied first page for every workbench section it already contains.
    available = {}
    if first_result is not None:
        if isinstance(first_result, dict) and first_result.get("api_id", op["api_id"]) != op["api_id"]:
            raise AssistantError("已有结果与本次查询接口不一致。", 403)
        for key, _, position, _, _ in specs:
            available[key, str(op["params"].get(position, 0 if position == "offset" else 1))] = first_result

    for key, pagination, position, size_param, unit in specs:
        rows, identities, expected, size = [], set(), None, None
        cursor, complete = (0 if position == "offset" else 1), False
        while True:
            params = {**op["params"], position: cursor}
            if pagination:
                params["sections"] = key
            summary = {"complete": False, "count": None, "loaded_count": len(rows),
                       "declared_total": expected, "unit": unit, "next_params": params}
            try:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssistantError("已达完整读取时间上限，剩余资料尚未读取。", 503)
                cached = available.pop((key, str(cursor)), None)
                if cached is not None:
                    result = cached
                else:
                    if item_count >= max_items:
                        raise AssistantError("已达完整读取条数上限，剩余资料尚未读取。", 503)
                    if calls >= max_pages:
                        raise AssistantError("已达完整读取页数上限，剩余资料尚未读取。", 503)
                    calls += 1
                    result = await asyncio.wait_for(invoke({**op, "params": params}), min(10, remaining))
                data = result.get("_raw", result.get("data")) if isinstance(result, dict) else None
                source = {"requested_scope": op["params"].get("scope"), **{name: data[name] for name in ("scope", "scopes", "month", "from", "to", "source",
                          "last_loaded_at", "last_refreshed_at", "updated_at", "cache_updated_at", "source_freshness",
                          "source_cache_ttl_seconds", "payload_version") if isinstance(data, dict) and name in data}}
                source['queried_at'] = result.get('_queried_at')
                if source not in freshness:
                    freshness.append(source)
                data = raw_page(result)
                if not base:
                    excluded = {spec[0] for spec in QUERY_PAGE_SCHEMAS[op["api_id"]]}
                    excluded.update(spec[1] for spec in QUERY_PAGE_SCHEMAS[op["api_id"]] if spec not in specs and spec[1])
                    base = copy.deepcopy({name: value for name, value in data.items() if name not in excluded})
                    envelope = dict(result)
                version = tuple(data.get(name) for name in ("payload_version", "version", "last_loaded_at", "last_refreshed_at", "updated_at"))
                if snapshot is not None and snapshot != version:
                    raise AssistantError("分页期间来源快照发生变化，完整数量待核对。", 409)
                snapshot = version
                page, meta = data.get(key), data.get(pagination) if pagination else data
                if not isinstance(page, list) or any(not isinstance(row, dict) for row in page) or not isinstance(meta, dict):
                    raise AssistantError("分页列表或分页元数据缺失。", 502)
                total = meta.get("total")
                if type(total) is not int or total < 0 or expected is not None and total != expected:
                    raise AssistantError("分页总数缺失、无效或发生变化。", 502)
                expected = total
                if position in meta and (type(meta[position]) is not int or meta[position] != cursor):
                    raise AssistantError("分页位置与请求不一致。", 502)
                if pagination and "page" in meta and (type(meta["page"]) is not int or meta["page"] != cursor):
                    raise AssistantError("工作台分页位置与请求不一致。", 502)
                if "offset" in meta and (type(meta["offset"]) is not int or meta["offset"] != len(rows)):
                    raise AssistantError("分页偏移与已读取条数不一致。", 502)
                page_size = meta.get("page_size", meta.get("limit", op["params"].get(size_param, len(page))))
                if type(page_size) is not int or page_size < len(page) or size is not None and page_size != size:
                    raise AssistantError("分页大小发生变化或无效。", 502)
                size = page_size
                marks = []
                for row in page:
                    identity = next((row[name] for name in ("record_id", "id", "group_id", "batch_id", "active_item_id") if row.get(name)), None)
                    marks.append(json.dumps([row.get("work_type"), identity, row.get("version") if unit == "question_versions" else None]
                                            if identity is not None else row, sort_keys=True, ensure_ascii=False))
                if len(set(marks)) != len(marks) or identities.intersection(marks):
                    raise AssistantError("分页记录重复，未把重复页当作完整名单。", 502)
                loaded = len(rows) + len(page)
                if (loaded > total or not page and loaded < total
                        or "has_more" in meta and (type(meta["has_more"]) is not bool or meta["has_more"] != (loaded < total))
                        or "next_offset" in meta and (meta["next_offset"] != (loaded if loaded < total else None)
                            or meta["next_offset"] is not None and type(meta["next_offset"]) is not int)
                        or loaded < total and position != "offset" and len(page) != size):
                    raise AssistantError("分页条数与总数或后续页标记不一致。", 502)
                take = min(len(page), max_items - item_count)
                rows.extend(copy.deepcopy(page[:take]))
                item_count += take
                identities.update(marks)
                if take != len(page):
                    raise AssistantError("已达完整读取条数上限，剩余资料尚未读取。", 503)
                if loaded == total:
                    complete = True
                    break
                cursor = loaded if position == "offset" else cursor + 1
            except (AssistantError, asyncio.TimeoutError) as exc:
                if isinstance(exc, AssistantError) and exc.status in {401, 403}:
                    return {"ok": False, "status": exc.status, "api_id": op["api_id"],
                            "error": safe_text(str(exc)), "available": False, "complete": False, "data": None}
                warnings.append(safe_text(str(exc)) or "分页读取超时，完整数量未知。")
                break
        summary.update(complete=complete, count=len(rows) if complete else None,
                       loaded_count=len(rows), declared_total=expected,
                       next_params=None if complete else {**op["params"], position: cursor, **({"sections": key} if pagination else {})})
        summaries[key] = summary
        base[key] = rows
    complete = all(summary["complete"] for summary in summaries.values())
    declared_count = base.get("count")
    for key, pagination, _, _, _ in specs:
        summary = summaries[key]
        meta = base.get(pagination) if pagination else base
        summary["declared_pagination"] = {name: meta[name] for name in
            ("total", "count", "page", "page_size", "offset", "returned", "total_pages", "has_more", "next_offset", "remaining")
            if isinstance(meta, dict) and name in meta}
        if not isinstance(meta, dict):
            meta = {}
            if pagination:
                base[pagination] = meta
        meta.update(complete=summary["complete"], has_more=not summary["complete"],
                    next_offset=None if summary["complete"] else summary["loaded_count"])
        if "page" in meta:
            meta["page"] = 1
        if "offset" in meta:
            meta["offset"] = 0
        if "returned" in meta:
            meta["returned"] = summary["loaded_count"]
        if "page_size" in meta:
            meta["page_size"] = summary["loaded_count"]
        if "total_pages" in meta:
            meta["total_pages"] = 1 if summary["complete"] else None
        if "remaining" in meta:
            meta["remaining"] = max(0, summary["declared_total"] - summary["loaded_count"]) if summary["declared_total"] is not None else None
        if "count" in meta:
            meta["count"] = summary["count"]
    if op["api_id"] == "GET /api/workbench" and "count" in base:
        base["count"] = summaries.get("records", next(iter(summaries.values())))["count"]
    remaining_rows = (sum(max(0, summary["declared_total"] - summary["loaded_count"]) for summary in summaries.values())
                      if all(summary["declared_total"] is not None for summary in summaries.values()) else None)
    base.update(available=bool(envelope.get("ok")), complete=complete, list_complete=complete, remaining=remaining_rows,
                has_more=not complete, next_offset=None if complete else
                next(summary["loaded_count"] for summary in summaries.values() if not summary["complete"]))
    base["query_completeness"] = {"complete": complete, "lists": summaries, "pages_read": calls,
                                  "declared_count": declared_count, "warnings": list(dict.fromkeys(warnings)), "source_freshness": freshness}
    sample_limited = any(summary["loaded_count"] > 40 for summary in summaries.values())
    return {**envelope, "api_id": op["api_id"], "ok": bool(envelope.get("ok")), "available": bool(envelope.get("ok")),
            "complete": complete, "list_complete": complete, "remaining": remaining_rows,
            "_raw": base, "data": safe_data(base), "truncated": sample_limited, "sample_limited": sample_limited,
            "error": None if complete else "; ".join(dict.fromkeys(warnings))}


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


async def collect_events(actor, query: EventQuery, invoke, *, item_limit=20):
    """Retain bounded known rows; complete gates counts, list_complete gates omissions."""
    if type(item_limit) is not int or not 1 <= item_limit <= 2000:
        raise AssistantError("事件明细保留条数须为1至2000。")
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
        "items": rows[:item_limit], "remaining": max(0, len(rows) - item_limit),
        "list_complete": len(rows) <= item_limit, "warnings": list(dict.fromkeys(warnings)),
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


class NoticeQuery(BaseModel):
    """Current planned/open notices or successful sends within explicit dates."""
    model_config = ConfigDict(extra="forbid")
    status: Literal["planned", "open", "sent"]
    work_types: list[Literal["maintenance", "change", "repair", "power", "polling", "adjust"]] = Field(default_factory=list)
    start_date: dt.date | None = None
    end_date: dt.date | None = None

    @model_validator(mode="after")
    def valid_dates(self):
        if self.status == "sent":
            if self.start_date is None or self.end_date is None:
                raise ValueError("发送统计须提供实际发送日期范围")
            EventQuery(start_date=self.start_date, end_date=self.end_date)
        elif self.start_date is not None or self.end_date is not None:
            raise ValueError("待发和未结束查询反映当前状态，不能代替指定日期的历史状态")
        return self


async def collect_notices(actor, query: NoticeQuery, invoke):
    """Typed convenience over the existing notice collectors, without a second chain.

    invoke must reauthenticate and enforce scopes/modules on EVERY read. No caches,
    refreshes or writes are introduced. Counts are distinct notices; send_count is
    successful actions and remains separate. Current plans/open work has no date cut.
    """
    from .lighthouse_pending import collect_pending

    scopes = set(actor.get("scopes") or []) & set(actor.get("allowed_scopes", actor.get("scopes") or [])) & SCOPES
    profiles = actor.get("_topic_capabilities") or [actor]
    grants = {kind: sorted(scopes & set().union(*(set(profile.get("scopes") or [])
              for profile in profiles if kind in profile.get("modules", [])))) for kind in NOTICE_TYPES}
    kinds = list(dict.fromkeys(query.work_types)) or [kind for kind in NOTICE_TYPES if grants[kind]]
    if not kinds or any(not grants[kind] for kind in kinds):
        raise AssistantError("当前账号没有所选通告类型和楼栋的查询权限。", 403)
    deadline, calls, denied = time.monotonic() + 35, 0, None
    freshness, reports = [], []

    async def read(op):
        nonlocal calls, denied
        remaining = deadline - time.monotonic()
        if remaining <= 0 or calls >= 20:
            raise AssistantError("本轮通告读取已达预算，完整数量未知。", 503)
        calls += 1
        try:
            result = await asyncio.wait_for(invoke(op), min(10, remaining))
        except AssistantError as exc:
            if exc.status in {401, 403}:
                denied = exc
            raise
        if not isinstance(result, dict):
            raise AssistantError("通告读取结果无效。", 502)
        if result.get("status") in {401, 403}:
            denied = AssistantError("通告查询权限已变化，请重新查询。", result["status"])
            raise denied
        data = result.get("_raw", result.get("data"))
        if isinstance(data, dict):
            freshness.append({"scope": op.get("params", {}).get("scope"),
                              'queried_at': result.get('_queried_at'),
                              **{key: data[key] for key in ("last_loaded_at", "last_refreshed_at", "source_freshness", "payload_version") if key in data}})
            if (data.get("complete") is False or data.get("available") is False or data.get("warnings")
                    or data.get("schema_warnings") or result.get("scope_warning") or result.get("complete") is False
                    or result.get("available") is False or data.get("snapshot_exists") is False or data.get("source_snapshot_ready") is False):
                raise AssistantError("通告来源尚未完整返回，完整数量未知。", 503)
        return result

    for kind in kinds:
        first_read = len(freshness)
        scoped = {**actor, "scopes": grants[kind]}
        if query.status == "sent":
            data = await collect_notice_sends(scoped, query.start_date, query.end_date, read, work_types=[kind], item_limit=2000)
            reports.append({"work_type": kind, **data})
        else:
            group = "plans" if query.status == "planned" else "notices"
            data = await collect_pending(scoped, "", read, None, groups_only={group}, notice_type=kind, item_limit=2000)
            reports.append({"work_type": kind, **data["groups"][0], "list_complete": data["groups"][0]["remaining"] == 0})
        reports[-1]['queried_at'] = max((row['queried_at'] for row in freshness[first_read:]
            if row.get('queried_at')), default=None)
        if denied is not None:
            raise denied
    complete = all(row.get("complete", row.get("available", False)) for row in reports)
    return {"kind": "notice_query", "query": query.model_dump(mode="json"), "scopes": sorted(set().union(*(set(grants[kind]) for kind in kinds))),
            "complete": complete, "count": sum(row["count"] for row in reports) if complete else None,
            "list_complete": all(row["remaining"] == 0 for row in reports),
            "remaining": sum(row["remaining"] for row in reports),
            "known_count": sum(row["known_count"] for row in reports), "unit": "distinct_notices",
            "date_basis": "action.time" if query.status == "sent" else "current_status",
            "send_count": sum(row["send_count"] for row in reports) if query.status == "sent" and complete else None,
            "groups": reports, "source_freshness": freshness,
            "queried_at": max((row['queried_at'] for row in freshness if row.get('queried_at')), default=None)}


async def collect_notice_sends(actor, start_date, end_date, invoke, *, work_types=(), item_limit=20):
    # The native history contains successful Qt/web actions, not unsent plans.
    if type(item_limit) is not int or not 1 <= item_limit <= 2000:
        raise AssistantError("通告明细保留条数须为1至2000。")
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
        "items": list(notices.values())[:item_limit], "remaining": max(0, len(notices)-item_limit),
        "list_complete": len(notices) <= item_limit, "warnings": list(dict.fromkeys(warnings)),
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


def _query_value(data, path):
    from .lighthouse_agent import _read_path
    if not isinstance(path, list) or len(path) > 12:
        raise AssistantError("查询路径无效。")
    if not all(isinstance(seg, str) and seg and len(seg) <= 200 for seg in path):
        raise AssistantError("查询路径段无效。")
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
    return selected


def query_result_page(data, path, offset=0, limit=20):
    """Page over an authorized snapshot, not native API pagination or fresh data."""
    if type(offset) is not int or offset < 0:
        raise AssistantError("分页偏移无效。")
    if type(limit) is not int or not 1 <= limit <= 40:
        raise AssistantError("分页大小无效。")
    selected = _query_value(data, path)
    if isinstance(selected, dict):
        fields = list(safe_data(selected))
        next_offset = offset + limit if offset + limit < len(fields) else None
        return {'kind': 'object_fields', 'fields': fields[offset:offset + limit], 'loaded_count': len(fields), 'offset': offset,
                'next_offset': next_offset, 'has_more': next_offset is not None,
                'note': '这里只列可读字段名，使用原query_ref和path加字段名读取字段值。'}
    if not isinstance(selected, (list, tuple)):
        if offset:
            raise AssistantError('单个字段值不支持分页偏移。')
        value = safe_data(selected)
        if path:
            parent = _query_value(data, path[:-1])
            if isinstance(parent, dict):
                visible = safe_data(parent, list_limit=None)
                if path[-1] not in visible:
                    raise AssistantError('不能读取受限字段。')
                value = visible[path[-1]]
        return {'value': value, 'value_limited': value != selected, 'next_offset': None, 'has_more': False}
    total = len(selected)
    page = safe_data(selected[offset:offset + limit])
    next_offset = offset + limit if offset + limit < total else None
    return {"items": page, "loaded_count": total, "offset": offset, "next_offset": next_offset,
            "has_more": next_offset is not None}


def query_statistics(data, path, operation='count', field=None):
    """Deterministic snapshot aggregates. Missing/partial data never become zero totals."""
    import math
    from decimal import Decimal
    if operation not in {'count', 'distinct', 'group_count', 'sum', 'mean'}:
        raise AssistantError('不支持此统计方式。')
    rows = _query_value(data, path)
    if not isinstance(rows, (list, tuple)) or len(rows) > 5000:
        raise AssistantError('统计需要不超过5000项的已授权列表，请先缩小范围。')
    values, missing = [], 0
    if operation != 'count':
        if not field:
            raise AssistantError('请指定原记录中实际存在的统计字段路径。')
        for row in rows:
            try:
                value = _query_value(row, field)
            except AssistantError as exc:
                if not str(exc).startswith('未返回需要的关联字段：'):
                    raise
                missing += 1
                continue
            if value is None or value == '':
                missing += 1
                continue
            if not isinstance(value, (str, int, float)) or isinstance(value, bool) \
                    or isinstance(value, float) and not math.isfinite(value):
                raise AssistantError('统计字段必须是有限数值或文本标识，不接受嵌套对象。')
            if safe_data({field[-1]: value}).get(field[-1]) != value:
                raise AssistantError('统计字段含受限资料，不能用于分组或计算。')
            values.append(value)
    complete = query_result_state({'ok': True, 'data': data})[0] in {'found', 'empty'}
    result = {'operation': operation, 'loaded_count': len(rows), 'missing_count': missing,
              'complete': complete and not missing, 'snapshot_only': True,
              'scope': 'loaded_snapshot', 'note': '仅统计本轮已读取快照；不代表完整数据库或刚同步云端。'}
    if operation == 'count':
        result['value'] = len(rows)
    elif operation == 'distinct':
        result['value'] = len(set(values)) if values or not rows else None
    elif operation == 'group_count':
        from collections import Counter
        counts = Counter(values)
        if len(counts) > 200:
            raise AssistantError('分组超过200组，请先缩小范围。')
        result['groups'] = [{'value': value, 'count': count} for value, count in counts.items()]
    else:
        if any(type(value) not in {int, float} for value in values):
            raise AssistantError('数值统计只接受原字段中的数值，不猜测文本、百分号或单位。')
        total = sum((Decimal(str(value)) for value in values), Decimal(0))
        result['value'] = float(total / len(values) if operation == 'mean' and values else total) if values else None
        if result['value'] is not None and not math.isfinite(result['value']):
            raise AssistantError('统计结果超出有效数值范围。')
    return safe_data(result, list_limit=None)
