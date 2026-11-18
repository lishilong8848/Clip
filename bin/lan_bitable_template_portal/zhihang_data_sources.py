"""Read-only Zhihang adapters ported from scripts/risk-local-api.js.

The old collector used each subsystem's browser session. The shared Lighthouse
credential is tried directly against the fixed APIs. If a subsystem requires
separate SSO state, that read fails explicitly and produces no batch.
"""

import calendar
import json
import math
import re
import time
import unicodedata
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

import httpx

from . import plan_convergence_auth as auth


TARGET_KEYS = {
    "risk": ("risk",),
    "change": ("change_workorder", "change_basic"),
    "drill": ("drill",),
    "event": ("event",),
    "inspection": ("inspection",),
}

_DOMAIN = ".meta42.indc.vnet.com"
_RISK_PATH = "/api/ab-bpm/biz/bizCustGrid/view/"
_EVENT_PATH = "/api/event/eventOrder/selEventMsg"
_ENDPOINTS = {
    "risk_list": ("POST", "agilebpm", _RISK_PATH + "fxgl_xcydfxpcjlsjlb"),
    "risk_list_alt": ("POST", "agilebpm", _RISK_PATH + "list_fxgl_xcydfxpcjlsjlb"),
    "risk_detail": ("POST", "agilebpm", _RISK_PATH + "list_fxgl_xcydfxpcmxsjlb"),
    "change_list": ("POST", "change2", "/api/change/change/order/listPage"),
    "change_detail": ("POST", "change2", "/api/change/changeDetails/getBasicInformation"),
    "drill_list": ("POST", "emergencydrill", "/api/emergencydrill/exercisePlan/queryExercisePlanList"),
    "drill_evaluation": ("POST", "emergencydrill", _EVENT_PATH),
    "h_drill_list": ("POST", "emergencydrill", _EVENT_PATH),
    "h_drill_buildings": ("GET", "emergencydrill", "/api/event/eventOrder/selBuildingOptionByAuth"),
    "drill_detail": ("GET", "emergencydrill", "/api/emergencydrill/exerciseEvaluation/selExerciseEvaluationTemplate"),
    "event_list": ("POST", "event-live", _EVENT_PATH),
    "inspection_list": ("GET", "inspect2", "/api/inspect/inspection/job/list"),
}
_BEIJING = timezone(timedelta(hours=8))
_BUILDINGS = dict(zip("56789", ("A楼", "B楼", "C楼", "D楼", "E楼")))
_MONTHS = "January February March April May June July August September October November December".split()
# Original change list filters. The user ID is an API selector, not a credential.
_CHANGE_USER = "18871"
_CHANGE_DC = "INSTANCE-7691"
_TIMEOUT = httpx.Timeout(connect=5, read=30, write=10, pool=5)


class ZhihangSourceError(RuntimeError):
    """Sanitized failure for job status; no response bodies or credentials."""

    def __init__(self, code, message, *, status=None):
        super().__init__(message)
        self.code = code
        self.status = status


def _incomplete(label, detail):
    return ZhihangSourceError("incomplete", f"智航 {label}：{detail}")


class _RepeatedPageRecord(ZhihangSourceError):
    def __init__(self, endpoint, first_page, page):
        super().__init__("incomplete", f"智航 {endpoint}：第 {page} 页出现重复记录"
                         f"（首次见于第 {first_page} 页），无法确认是否漏页。")


class _InspectionShortBatch(ZhihangSourceError):
    def __init__(self, rows, total):
        super().__init__("incomplete", f"智航巡检应有 {total} 张工单，去重后仅 {len(rows)} 张。")
        self.rows = rows
        self.total = total


class _EventRangeMismatch(ZhihangSourceError):
    def __init__(self, label):
        super().__init__("incomplete", f"智航 event_list：{label}返回了区间外事件，无法确认日期筛选生效。")


def _inspection_source_key(row):
    work_id = _first(row, "id", "jobId")
    return ("id", work_id) if work_id else ("row", json.dumps(row, sort_keys=True, ensure_ascii=False))


class _Source:
    def __init__(self, report=None, *, preview=False, credentials=None):
        self.report = report or (lambda stage, **facts: None)
        self.preview = preview
        try:
            self.headers, self.cookies = credentials if credentials is not None else auth.build_auth()
        except ValueError:
            raise ZhihangSourceError("auth_required", "请在计划收敛审查中登录智航后重试。") from None

    def request(self, endpoint, values):
        method, host, path = _ENDPOINTS[endpoint]
        origin = "https://" + host + _DOMAIN
        headers = self.headers | {
            "Origin": origin, "Referer": origin + "/", "X-Requested-With": "XMLHttpRequest",
        }
        try:
            response = auth.request(
                method, origin + path, headers=headers, cookies=self.cookies, timeout=_TIMEOUT,
                follow_redirects=False, **({"params": values} if method == "GET" else {"json": values}),
            )
        except (httpx.HTTPError, OSError):
            raise ZhihangSourceError("source_unavailable", f"无法连接智航 {host}，请检查正式机 VPN 与内网连接。") from None
        status = response.status_code
        if status == 401 or 300 <= status < 400:
            raise ZhihangSourceError("auth_required", f"智航 {host} 需要有效的子系统登录。", status=status)
        if status == 403:
            raise ZhihangSourceError("access_denied", f"当前共享登录无法访问智航 {host}，请核对子系统权限或会话。", status=status)
        if not 200 <= status < 300:
            raise ZhihangSourceError("source_unavailable", f"智航 {host} 返回 HTTP {status}。", status=status)
        try:
            data = response.json()
        except ValueError:
            if "html" in response.headers.get("Content-Type", "").lower():
                raise ZhihangSourceError("auth_required", f"智航 {host} 返回网页，当前共享登录可能缺少子系统会话。") from None
            raise _incomplete(endpoint, "响应不是有效 JSON。") from None
        if not isinstance(data, dict):
            raise _incomplete(endpoint, "响应结构无效。")
        code = _text(data.get("code"))
        if code in {"401", "403"}:
            error = "auth_required" if code == "401" else "access_denied"
            raise ZhihangSourceError(error, f"智航 {host} 拒绝当前共享登录（{code}）。", status=int(code))
        is_risk = endpoint.startswith("risk_")
        risk_ok = _text(data.get("isOk")).casefold() in {"true", "1"}
        allowed_codes = {"", "0", "200"} if endpoint == "inspection_list" else {"", "200"}
        # AgileBPM confirms success with isOk. The other subsystems use code/success.
        failed = not risk_ok if is_risk else (
            code not in allowed_codes or data.get("success") is False
            or (endpoint.startswith("change_") and (code != "200" or data.get("success") is not True))
        )
        if failed:
            indicators = []
            if re.fullmatch(r"[0-9]{1,8}", code):
                indicators.append(f"业务码 {code}")
            if is_risk:
                indicators.append("isOk 缺失" if "isOk" not in data else "isOk 未确认成功")
            context = f"（{'，'.join(indicators)}）" if indicators else ""
            raise _incomplete(endpoint, f"接口报告查询失败{context}。")
        return data


def _text(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    if isinstance(value, list):
        return "".join(_text(item) for item in value)
    if isinstance(value, dict):
        return _text(value.get("text", value.get("name", value.get("value"))))
    return str(value).strip()


def _first(record, *keys):
    return next((text for key in keys if (text := _text(record.get(key)))), "")


def _local(now):
    return now.replace(tzinfo=_BEIJING) if now.tzinfo is None else now.astimezone(_BEIJING)


_DATE_RE = re.compile(r"(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})(?:日)?(?:[ T](\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?")


def _date(value):
    text = _text(value)
    try:
        if text.isdecimal() and len(text) in {10, 13}:
            return datetime.fromtimestamp(int(text) / (1000 if len(text) == 13 else 1), _BEIJING)
        if match := _DATE_RE.fullmatch(text):
            return datetime(*(int(part or 0) for part in match.groups()), tzinfo=_BEIJING)
        if _DATE_RE.match(text):
            return _local(datetime.fromisoformat(text.replace("Z", "+00:00")))
    except (ValueError, OverflowError, OSError):
        return None
    return None


def _plan_range(value):
    matches = list(_DATE_RE.finditer(_text(value)))
    return tuple(_date(match[0]) for match in matches)


def identity(kind, fields):
    """Identify a target Feishu row using its existing business columns."""
    if kind in ("change_workorder", "change_basic"):
        number = _first(fields, "工单号")
        return f"order:{number}" if number else None
    if kind == "event":
        value = _first(fields, "事件ID", "事件编号")
        return f"event:{value}" if value else None
    if kind == "h_duty_drill":
        value = _first(fields, "演练编号")
        return f"number:{value}" if value else None
    if kind == "drill":
        if value := _first(fields, "计划ID"):
            return f"id:{value}"
        year, month = _first(fields, "演练年份"), _first(fields, "演练月份")
        date = _date(fields.get("计划演练时间"))
        month_number = _drill_month(month)
        year = year or (str(date.year) if date else "")
        month = str(month_number or (date.month if date else ""))
        parts = (year, month, _first(fields, "计划编号"))
    elif kind == "risk":
        if value := _first(fields, "风险ID", "排查明细ID"):
            return f"id:{value}"
        building, number = _first(fields, "楼栋"), _first(fields, "风险编号")
        parts = (building, number) if number else (
            building, _first(fields, "检查项目"), _first(fields, "检查地点"), _first(fields, "排查周期"),
        )
    elif kind == "inspection":
        if value := _first(fields, "工单ID", "巡检工单ID"):
            return f"id:{value}"
        times = _plan_range(fields.get("计划区间"))
        if len(times) != 2 or any(item is None for item in times):
            return None
        parts = (
            _first(fields, "楼栋"), _first(fields, "工单名称"), _first(fields, "巡检类型"),
            *(item.isoformat(timespec="minutes") for item in times),
            _first(fields, "位置信息"),
        )
        if parts[0] == "未识别楼栋" or not all(parts[:-1]):
            return None
        # Location is optional in the original API. Ambiguous rows fail the batch.
        return "inspection:" + json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
    else:
        return None
    return kind + ":" + json.dumps(parts, ensure_ascii=False, separators=(",", ":")) if all(parts) else None


def in_refresh_scope(kind, fields, now):
    """Scope predicate only. Callers must separately prove row ownership."""
    if not identity(kind, fields):
        return False
    now = _local(now)
    if kind in ("change_workorder", "change_basic", "drill"):
        # Original base query is not bounded by date.
        return True
    if kind == "event":
        start = now.date() - timedelta(days=29)
        created = _first(fields, "创建时间", "通知时间")
        return any(start <= value.date() <= now.date() for value in (
            _date(fields.get("发生时间")), _date(created),
        ) if value is not None)
    if kind == "risk":
        # Old rows omit the parent inspection month. An audit date alone can
        # belong to an earlier month's inspection, so it cannot prove scope.
        checked_at = _date(fields.get("排查时间"))
        return (checked_at is not None and _first(fields, "楼栋") in _BUILDINGS.values()
                and (checked_at.year, checked_at.month) == (now.year, now.month))
    if kind == "inspection":
        times = _plan_range(fields.get("计划区间"))
        return (len(times) == 2 and all(times)
                and (times[0].year, times[0].month) == (now.year, now.month)
                and times[0] <= times[1])
    return False


def _metadata(sources, keys, label):
    for source in sources:
        for key in keys:
            if (value := source.get(key)) is not None and value != "":
                if isinstance(value, bool) or not re.fullmatch(r"\d+", str(value)):
                    raise _incomplete(label, "分页元数据无效。")
                return int(value)
    return None


def _pages(source, endpoint, make_request, size, *, limit=1000, workers=1, stage=None, probe=False,
           expected_total=None):
    def load(page):
        params = make_request(page)
        attempts = 3 if endpoint in {"event_list", "h_drill_list"} else 1
        for attempt in range(attempts):
            try:
                raw = source.request(endpoint, params)
                break
            except ZhihangSourceError as error:
                if error.code != "source_unavailable" or attempt == attempts - 1:
                    raise
                if error.status is not None and error.status not in {408, 429, 500, 502, 503, 504}:
                    raise
                time.sleep(1.2 * (attempt + 1))
        data = raw.get("data")
        data = data if isinstance(data, dict) else {}
        meta = data.get("pageBean")
        containers = (raw, data) if endpoint == "inspection_list" else (data, meta if isinstance(meta, dict) else {})
        if endpoint.startswith("risk_"):
            names = ("list",)
        elif endpoint == "inspection_list":
            names = ("rows", "records", "list")
        elif endpoint in ("event_list", "drill_evaluation", "h_drill_list"):
            names = ("dataList", "records", "list")
        else:
            names = ("records",)
        rows = next((container[key] for container in containers for key in names
                     if isinstance(container.get(key), list)), None)
        if rows is None or any(not isinstance(row, dict) or not row for row in rows):
            raise _incomplete(endpoint, f"第 {page} 页缺少有效记录列表。")
        if endpoint == "inspection_list" and (building := params.get("buildingCode")):
            if any(_first(row, "buildingCode") != building for row in rows):
                raise _incomplete(endpoint, f"第 {page} 页返回其他楼栋的工单，无法确认按楼过滤生效。")
        total = _metadata(containers, ("total", "count", "totalCount", "totalRecords"), endpoint)
        pages = _metadata(containers, ("pages", "totalPages", "pageCount"), endpoint)
        if endpoint in ("risk_detail", "inspection_list", "change_list", "drill_list") and total is None:
            raise _incomplete(endpoint, f"第 {page} 页缺少权威总数。")
        if pages is None and total is not None:
            pages = max(1, math.ceil(total / size))
        if pages == 0 and total == 0:
            pages = 1
        return rows, total, pages

    result = []
    seen = {}
    inspection = endpoint == "inspection_list"
    event = endpoint in {"event_list", "h_drill_list"}
    duplicates = 0

    def collect(page, rows):
        nonlocal duplicates
        for row in rows:
            if event:
                key = _first(row, "id", "eventNumber")
                if not key:
                    raise _incomplete(endpoint, f"第 {page} 页缺少事件 ID。")
            elif inspection:
                key = _inspection_source_key(row)
            else:
                key = json.dumps(row, sort_keys=True, ensure_ascii=False)
            if key not in seen:
                seen[key] = (page, row)
                result.append(row)
                continue
            first_page, first_row = seen[key]
            if not (inspection or event):
                raise _RepeatedPageRecord(endpoint, first_page, page)
            duplicates += 1
            if stage:
                source.report(stage, 重复条数=duplicates, 首次出现页=first_page, 重复出现页=page)
            if row != first_row:
                record_type = "事件" if event else "工单"
                raise _incomplete(endpoint, f"第 {page} 页的{record_type} ID 与第 {first_page} 页相同，但内容不同。")

    if stage:
        source.report(stage, 每页=size)
    first, total, pages = load(1)
    if stage:
        source.report(stage, 源端总数=total, 总页数=pages, 首页条数=len(first))
    if expected_total is not None and (total != expected_total or len(first) != min(size, expected_total)
                                       or pages != max(1, math.ceil(expected_total / size))):
        raise _incomplete(endpoint, f"该页长的首页或总数与基准 {expected_total} 条不一致。")
    if probe:
        if len(first) > size or (total is not None and len(first) != min(size, total)):
            raise _incomplete(endpoint, "诊断首页条数与请求大小或源端总数不一致。")
        if stage and endpoint == "event_list":
            statuses = sorted({_first(row, "orderStatus") for row in first})
            codes = [code for code in statuses if re.fullmatch(r"[0-9]{1,3}", code)]
            if codes:
                source.report(stage, 样本状态编码="、".join(codes))
        return first, total
    collect(1, first)
    if pages is None:
        if endpoint in ("event_list", "drill_evaluation", "h_drill_list"):
            raise _incomplete(endpoint, "缺少总数和总页数，无法核验批次。")
        # The original list endpoints may omit totals. Read until a short page.
        page, rows = 1, first
        while len(rows) >= size:
            page += 1
            if page > limit:
                raise _incomplete(endpoint, "超过分页上限。")
            if stage:
                source.report(stage, 当前页=page, 已读取=len(result))
            rows, new_total, new_pages = load(page)
            if new_total is not None or new_pages is not None:
                raise _incomplete(endpoint, "分页元数据在拉取期间改变。")
            collect(page, rows)
    else:
        if not 1 <= pages <= limit or (total is not None and total > size * pages):
            raise _incomplete(endpoint, "超过分页上限或页数无效。")

        def checked(page):
            rows, page_total, page_count = load(page)
            if not rows or (page_total, page_count) != (total, pages):
                raise _incomplete(endpoint, f"第 {page} 页缺失或总数改变。")
            if expected_total is not None and len(rows) != min(size, expected_total - (page - 1) * size):
                raise _incomplete(endpoint, f"第 {page} 页条数不完整。")
            return rows

        if pages > 1:
            if not first:
                raise _incomplete(endpoint, "首页为空但存在后续页。")
            with ThreadPoolExecutor(max_workers=workers) as executor:
                pending = iter(executor.map(checked, range(2, pages + 1)))
                for page in range(2, pages + 1):
                    if stage:
                        source.report(stage, 当前页=page, 已读取=len(result))
                    rows = next(pending)
                    collect(page, rows)
    if stage:
        source.report(stage, 已读取=len(result))
    if total is not None and len(result) != total:
        if inspection and duplicates and len(result) < total:
            raise _InspectionShortBatch(result, total)
        raise _incomplete(endpoint, f"应有 {total} 条，实际读取 {len(result)} 条。")
    return result


def _map_rows(kind, rows, mapper):
    result = {}
    for row in rows:
        fields = mapper(row)
        key = identity(kind, fields)
        if not key or key in result:
            raise _incomplete(kind, "业务主键缺失或重复。")
        result[key] = fields
    if not result:
        raise ZhihangSourceError("empty", f"智航 {kind} 返回空批次，保留上次已核验数据。")
    return result


def _raw_values(value):
    if isinstance(value, dict):
        for item in value.values():
            yield from _raw_values(item)
    elif isinstance(value, list):
        for item in value:
            yield from _raw_values(item)
    elif value is not None:
        yield _text(value)


def _building(value):
    text = unicodedata.normalize("NFKC", _text(value)).upper()
    match = re.search(r"([A-EH])\s*(?:楼|栋|幢|座)", text)
    return match[1] + "楼" if match else ""


_RISK_LEVEL = {value: label for label, values in {
    "低": ("1", "low", "l", "lo"), "中": ("2", "medium", "mid", "m", "in", "im"),
    "高": ("3", "high", "h"), "一般": ("ic",),
}.items() for value in values}
_RISK_PERIOD = {value: label for label, values in {
    "月度": ("monthly", "month", "m", "mo"), "季度": ("quarter", "quarterly", "q", "qtr"),
    "半年度": ("halfyear", "half"), "年度": ("yearly", "year", "annual", "y", "yr"),
    "周度": ("weekly", "week", "w"), "日度": ("daily", "day", "d"),
}.items() for value in values}


def _mapped(record, key, mapping):
    value = _first(record, key)
    return mapping.get(value, value)


def _fields(record, columns):
    return {key: _first(record, *value.split()) for key, value in columns.items()}


def _risk(source, now):
    def params(page, parent=""):
        query = [{"name": "pcjlid_", "con": "like", "val": parent}] if parent else []
        return {"orderBy": "", "pageSize": 50, "currentPage": page, "queryData": query}

    try:
        parents = _pages(source, "risk_list", params, 50, limit=10, stage="风险排查主列表")
    except ZhihangSourceError as error:
        if error.status != 404:
            raise
        parents = _pages(source, "risk_list_alt", params, 50, limit=10, stage="风险排查备用列表")

    candidates = defaultdict(list)
    months = (now.strftime("%Y-%m"), now.strftime("%Y/%m"))
    for index, row in enumerate(parents):
        values = list(_raw_values(row))
        joined = " ".join(values)
        if not any(month in joined for month in months):
            continue
        code = _first(row, "ldyh", "ldyh_", "ldyh_$rel", "ldyh$rel", "ldyhRel", "楼栋号", "楼栋号_")
        labels = {_BUILDINGS[code]} if code in _BUILDINGS else {_building(value) for value in values} - {""}
        if len(labels) != 1:
            continue
        label = next(iter(labels))
        value = _first(row, "id_", "id", "id_$rel", "ID_", "pcjlid_", "pcjlid_$rel")
        value = value or next((part for part in values if re.fullmatch(r"\d{16,22}", part)), "")
        match = re.search(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", joined)
        date = _date(match[0]) if match else None
        score = (date.timestamp() / 100_000_000 if date else 0) - index
        if value:
            candidates[label].append((score, value))
    source.report("风险排查楼栋归属", **{label: len(candidates[label]) for label in _BUILDINGS.values()})
    missing = [label for label in _BUILDINGS.values() if not candidates[label]]
    if missing:
        raise _incomplete("risk", f"无法确认本月{'、'.join(missing)}排查记录，保留原数据。")
    rows = []
    selected = {label: max(candidates[label], key=lambda entry: entry[0])[1] for label in _BUILDINGS.values()}
    if len(set(selected.values())) != len(selected):
        raise _incomplete("risk", "不同楼栋命中同一个排查记录，无法核验楼栋归属。")
    for label, parent in selected.items():
        detail = _pages(source, "risk_detail", lambda page: params(page, parent), 50, limit=100, workers=2,
                        stage=f"风险排查明细 · {label}")
        if not detail:
            raise _incomplete("risk", f"{label} 本月排查明细为空。")
        source_ids = [value for row in detail if (value := _first(row, "id_", "id", "id_$rel", "ID_"))]
        if len(source_ids) != len(set(source_ids)):
            raise _incomplete("risk", f"{label} 排查明细 ID 重复。")
        rows.extend(row | {"buildingName": label} for row in detail)

    def map_row(row):
        fields = _fields(row, {
            "楼栋": "buildingName", "风险编号": "fxxbh_", "检查项目": "jcxms_", "排查状态": "pczt_",
            "检查地点": "jcd_", "排查时间": "pcsj_", "审核时间": "pcwcsj_$rel", "风险现场情况": "fxxcqk_",
        })
        fields.update(风险等级=_mapped(row, "fxdj_", _RISK_LEVEL), 排查周期=_mapped(row, "pczq_", _RISK_PERIOD))
        for column, name in (("此前风险状态", "cqfxzt_"), ("当前风险状态", "dqfxzt_")):
            value = _first(row, name)
            fields[column] = ("无" if value in {"w", "无"} else "有") if value else ""
        return fields

    source.report("风险排查记录核验", 待核验=len(rows))
    result = _map_rows("risk", rows, map_row)
    source.report("风险排查记录核验", 核验通过=len(result))
    return {"risk": result}


_CHANGE_STATUS = dict(zip("0 1 2 3 4 5 8 13 14".split(),
                          "草稿 审批中 实施中 实施成功 实施失败 已关闭 终止 实施成功 逾期实施成功".split()))
_CHANGE_CATEGORY = {str(i): label for i, label in enumerate(
    "故障解决 预防问题 升级部署 主动性改善 业务需求 预防维护 升级改善 外部需求".split(), 1)}
_CHANGE_PROGRESS = {str(i * 10): label for i, label in enumerate(
    "新建变更工单 设施经理审批 区域经理审批 技术中心专业工程师审批 技术中心经理审批 运营支撑部总经理审批 客服审核 撰写实施结果 实施后审批 区域经理关闭 流程抄送".split(), 1)}
_CHANGE_PROGRESS.update({"61": "交付机房经理审批", "62": "交付高级经理审批", "63": "交付区域负责人审批"})


def _change_workorder(row):
    fields = _fields(row, {
        "工单号": "orderCode", "标题": "title", "提交人": "applicant", "变更类型": "changeType",
        "变更等级": "changeLevel", "当前节点": "currentNode", "申请人": "applicant", "申请时间": "applyTime",
        "计划开始时间": "planStartTime", "计划结束时间": "planEndTime", "实际开始时间": "actualStartTime",
        "实际结束时间": "actualEndTime", "延迟开始时间": "delayedStartTime", "延迟结束时间": "delayedEndTime",
        "延迟原因": "delayedReason",
    })
    return fields | {"状态": _mapped(row, "status", _CHANGE_STATUS),
                     "变更类别": _mapped(row, "changeCategory", _CHANGE_CATEGORY)}


def _change_basic(row):
    fields = _fields(row, {
        "工单号": "orderCode order_code", "标题": "title description name", "提交人": "applicant",
        "计划开始时间": "planStartTime plan_start_time", "计划结束时间": "planEndTime plan_end_time",
        "实际开始时间": "implementStartTime implement_start_time actualStartTime actual_start_time",
        "实际结束时间": "implementEndTime implement_end_time actualEndTime actual_end_time",
        "计划延时开始时间": "planDelayStartTime plan_delay_start_time delayedStartTime delayed_start_time",
        "计划延时结束时间": "planDelayEndTime plan_delay_end_time delayedEndTime delayed_end_time",
    })
    level = _first(row, "level", "changeLevel", "change_level")
    level_map = {"1": "I1级", "2": "I1级", "3": "I1级", "4": "I1级", "5": "I2级", "6": "I2级",
                 "7": "I2级", "8": "I3级", "9": "I3级", "10": "I3级", "20": "I2级", "30": "I1级", "40": "E级"}
    match = re.search(r"I[1-4]级", _first(row, "leadTime"))
    progress, status = _first(row, "progress"), _first(row, "status")
    node = next((value for key in (
        "currentNode", "current_node", "currentNodeName", "current_node_name", "nodeName", "node_name",
        "taskName", "task_name", "activityName", "activity_name",
    ) if (value := _first(row, key)) not in {"", "-", "null", "undefined"}), "")
    closed = {"3": "区域经理关闭", "5": "区域经理关闭", "8": "流程结束", "13": "区域经理关闭", "14": "区域经理关闭"}
    category = _first(row, "category", "changeCategory", "change_category")
    change_type = _first(row, "type", "changeType", "change_type")
    return fields | {
        "变更类型": {"1": "紧急变更", "2": "计划变更", "3": "计划变更", "4": "其他"}.get(change_type, _first(row, "type")),
        "变更等级": match[0] if match else level_map.get(level, level),
        "变更类别": _CHANGE_CATEGORY.get(category, category), "状态": _CHANGE_STATUS.get(status, status),
        "当前节点": _CHANGE_PROGRESS.get(progress) or node or (closed.get(status, "") if not progress else progress),
    }


def _change(source, _now):
    filters = dict.fromkeys("""
        customParam applyEndTime applyStartTime buildingCode category implementEndTime implementEndTimeEnd
        implementEndTimeStart implementStartTime level planEndTime planEndTimeEnd planEndTimeStart planStartTime
        actualStartTime actualStartTimeStart actualStartTimeEnd actualEndTime actualEndTimeStart actualEndTimeEnd
        delayedStartTime delayedStartTimeStart delayedStartTimeEnd delayedEndTime delayedEndTimeStart delayedEndTimeEnd
        delayedReason progress status type
    """.split(), "")
    rows = _pages(source, "change_list", lambda page: filters | {
        "pageNo": page, "pageSize": 15, "userId": _CHANGE_USER, "operType": "4", "dc": _CHANGE_DC,
    }, 15, workers=4)
    workorders = _map_rows("change_workorder", rows, _change_workorder)
    order_ids = [_first(row, "id", "orderId", "order_id") for row in rows]
    if any(not re.fullmatch(r"[1-9]\d*", order) for order in order_ids) or len(set(order_ids)) != len(order_ids):
        raise _incomplete("change", "工单详情 ID 缺失或重复。")

    def fetch_detail(order):
        data = source.request("change_detail", {"orderId": order, "userId": _CHANGE_USER}).get("data")
        if not isinstance(data, dict) or not data or data.get("error"):
            raise _incomplete("change", "工单基础信息缺失。")
        return data

    with ThreadPoolExecutor(max_workers=6) as executor:
        details = list(executor.map(fetch_detail, order_ids))
    by_key = {}
    for row, detail in zip(rows, details):
        fields = _change_basic(detail)
        code = _first(row, "orderCode")
        if fields["工单号"] and fields["工单号"] != code:
            raise _incomplete("change", "基础信息与工单编号不一致。")
        # Older detail responses can omit orderCode. The list supplies the same order's code.
        fields["工单号"] = code
        by_key[identity("change_basic", fields)] = fields
    return {"change_workorder": workorders, "change_basic": by_key}


_ORDER_STATUS = dict(zip("1 2 3 4 5 6 8".split(), "待响应 待确认 待解决 待关闭 完成 处理中 已终止".split()))
_EVENT_STATUS = dict(zip("1 2 3 4 5 6 8".split(), "待响应 待确认 处理中 待解决 待关闭 完成 处理中".split()))
_EVENT_TIMES = {
    "创建时间": "creatTime createTime", "发生时间": "happenTime", "通知时间": "notificationTime",
    "响应时间": "responseTime", "确认时间": "ackTime", "恢复时间": "incidentRecoveryTime",
    "解决时间": "solveTime", "关闭时间": "closeTime",
}


def _event_filters():
    return dict.fromkeys("""
        creator deviceName endAskDate endHappendDate endIncidentRecoveryDate endResponseDate eventCurrentLevel
        eventFirstLevel eventHighestLevel eventTitle eventType orderStatus startAskDate startHappendDate
        startIncidentRecoveryDate startResponseDate
    """.split(), "") | {"domainCodeList": list(_BUILDINGS)}


def _event_fields(row, now):
    fields = _fields(row, _EVENT_TIMES | {
        "事件ID": "id", "事件编号": "eventNumber", "事件标题": "eventTitle", "最高告警": "highestLevelTitle",
        "当前等级": "eventCurrentLevelName", "最高等级": "eventHighestLevelName", "首次等级": "eventFirstLevelName",
        "事件类型": "eventType", "事件来源": "eventSource", "告警状态编码": "eventAlarmStatus", "事件状态编码": "orderStatus",
        "处理状态编码": "orderStatus", "流程状态编码": "eventStatus", "数据中心编码": "dcCode", "数据中心": "dcName",
        "专业域编码": "domainCode", "位置": "location", "事件描述": "eventDescription", "创建人": "creator",
        "响应耗时": "responseTimeCost", "确认耗时": "ackTimeCost", "更新人": "updater", "更新时间": "updateTime",
        "事件目录ID": "eventCatalogueId", "战情室ID": "warRoomId", "房间类型": "roomType",
    })
    real = _first(row, "realEvent")
    order = _mapped(row, "orderStatus", _ORDER_STATUS)
    flow = _mapped(row, "eventStatus", _EVENT_STATUS)
    fields.update(
        告警状态=_mapped(row, "eventAlarmStatus", {"1": "未恢复", "0": "已恢复"}),
        事件状态=order, 处理状态=order, 流程状态=flow,
        真实事件="是" if real.lower() in {"true", "1", "yes"} else "否" if real.lower() in {"false", "0", "no"} else real,
    )
    recent = in_refresh_scope("event", fields, now)
    focus = []
    if recent:
        level = re.compile(r"(?<![A-Z0-9])(?:I[12]|P\d*)(?:级)?(?![A-Z0-9])")
        if any(level.search(_first(row, key).upper()) for key in (
            "eventHighestLevelName", "eventCurrentLevelName", "eventFirstLevelName", "highestLevelTitle",
            "eventHighestLevel", "eventCurrentLevel", "eventFirstLevel",
        )) or level.search(_first(row, "previousLevelName").upper()):
            focus.append("重要")
        status = order if order and order != "未知状态" else flow
        if status and status not in {"完成", "未知状态"}:
            focus.append("未完成")
        if real.lower() in {"false", "0", "no", "n", "否"}:
            focus.append("非真实")
    return fields | {"同步关注类型": "、".join(focus), "近30天同步范围": "是" if recent else "否"}


def _event_pages(source, label, filters, start_key, end_key, *, endpoint="event_list"):
    date_keys = ("creatTime", "createTime", "notificationTime") if start_key == "startDate" else ("happenTime",)
    start, end = (_date(filters[key]) for key in (start_key, end_key))

    def read_range(since, until):
        full_range = (since, until) == (start, end)
        prefix = f"{'近30天事件' if endpoint == 'event_list' else '当月演练'} · {label}" if full_range else f"补拉 · {label} {since:%m-%d}至{until:%m-%d}"
        query = filters | {start_key: f"{since:%Y-%m-%d %H:%M:%S}", end_key: f"{until:%Y-%m-%d %H:%M:%S}"}

        def params(page, size):
            return query | {"pageBean": {"pageNumber": page, "pageSize": size}}

        def validate_dates(rows):
            if full_range:
                return
            for row in rows:
                moment = _date(_first(row, *date_keys))
                if moment is None or not since <= moment <= until:
                    raise _EventRangeMismatch(prefix)

        def validate(rows, total, anchor):
            keys = {_first(row, "id", "eventNumber") for row in rows}
            if len(keys) != total or "" in keys or (rows and anchor not in keys):
                raise _incomplete("event_list", f"{prefix}有 {len(keys)} 个唯一事件 ID，无法与基准 {total} 条对账。")
            validate_dates(rows)
            return rows

        stage = f"{prefix}基准"
        first, total = _pages(source, endpoint, lambda page: params(page, 1), 1, stage=stage, probe=True)
        if total is None:
            raise _incomplete("event_list", f"{prefix}缺少源端总数，无法确认分页完整性。")
        validate_dates(first)
        anchor = _first(first[0], "id", "eventNumber") if first else ""
        if total <= 1:
            return validate(first, total, anchor)
        if not anchor:
            raise _incomplete("event_list", f"{prefix}基准样本缺少事件主键。")
        days = (until.date() - since.date()).days
        split = since.replace(hour=0, minute=0, second=0) + timedelta(days=(days + 1) // 2)
        can_split = since < split < until
        sizes = (50, 15) if endpoint == "event_list" else (15, 50)
        for size in (sizes if can_split else (*sizes, 1)):
            stage = f"{prefix}每页{size}条"
            try:
                rows = _pages(source, endpoint, lambda page: params(page, size), size, workers=5,
                              stage=stage, expected_total=total)
                validate(rows, total, anchor)
                source.report(stage, 唯一事件数=len(rows))
                return rows
            except ZhihangSourceError as error:
                if (size == 1 or isinstance(error, _EventRangeMismatch)
                        or (error.code != "incomplete" and error.status not in (400, 422))):
                    raise
                source.report(stage, 结果=str(error))

        # Both queries include midnight so events with fractional seconds are not missed.
        left, right = read_range(since, split), read_range(split, until)
        unique = {}
        for row in left + right:
            key = _first(row, "id", "eventNumber")
            if key in unique and row != unique[key]:
                raise _incomplete("event_list", f"{prefix}分段交界同一事件 ID 的内容不同。")
            unique[key] = row
        stage = f"{prefix}分段核验"
        source.report(stage, 源端总数=total, 分段返回=len(left) + len(right), 唯一事件数=len(unique))
        if len(unique) != total:
            raise _incomplete("event_list", f"{prefix}分段返回 {len(unique)} 条唯一事件，与基准 {total} 条不一致。")
        return validate(list(unique.values()), total, anchor)

    return read_range(start, end)


def _event(source, now):
    filters = _event_filters() | dict.fromkeys(
        "endDate eventAlarmStatus realEvent startDate userId".split(), "",
    ) | {"eventSourceList": [], "orderStatus": "1,2,3,4,5,6"}
    start = f"{now.date() - timedelta(days=29)} 00:00:00"
    end = f"{now.date()} 23:59:59"
    combined = {}
    counts = []
    source_total = 0
    for label, start_key, end_key in (("创建时间", "startDate", "endDate"),
                                      ("发生时间", "startHappendDate", "endHappendDate")):
        stage = f"近30天事件 · {label}"
        source.report(stage, 日期范围=f"{start} 至 {end}")
        rows = _event_pages(source, label, filters | {start_key: start, end_key: end}, start_key, end_key)
        source.report(stage, 已读取=len(rows))
        counts.append(f"按{label}查询返回 {len(rows)} 条")
        source_total += len(rows)
        seen = set()
        for row in rows:
            fields = _event_fields(row, now)
            key = identity("event", fields)
            if not key or key in seen:
                raise _incomplete("event", "事件主键缺失或重复。")
            if not any(_date(fields.get(column)) for column in ("创建时间", "发生时间", "通知时间")):
                raise _incomplete("event", "事件缺少有效的日期，无法核对范围。")
            seen.add(key)
            if fields["近30天同步范围"] == "是":
                combined.setdefault(key, fields)
    if not combined:
        if source.preview and source_total == 0:
            _diagnose_empty_event(source, filters, start, end)
        raise ZhihangSourceError("empty", f"智航事件近 30 天无有效记录（{'，'.join(counts)}），保留上次已核验数据。")
    return {"event": combined}


def _diagnose_empty_event(source, filters, start, end):
    # These controls only report totals. They never feed preview or sync rows.
    checks = (
        ("事件诊断 · 清空日期、楼栋和状态筛选", {"domainCodeList": [], "orderStatus": ""}),
        ("事件诊断 · 仅 A–E 楼", {"orderStatus": ""}),
        ("事件诊断 · A–E 楼及原状态", {}),
        ("事件诊断 · A–E 楼创建时间，原状态", {"startDate": start, "endDate": end}),
        ("事件诊断 · A–E 楼创建时间，不限状态", {"orderStatus": "", "startDate": start, "endDate": end}),
        ("事件诊断 · A–E 楼发生时间，原状态", {"startHappendDate": start, "endHappendDate": end}),
        ("事件诊断 · A–E 楼发生时间，不限状态",
         {"orderStatus": "", "startHappendDate": start, "endHappendDate": end}),
    )
    for stage, overrides in checks:
        params = filters | overrides | {"pageBean": {"pageNumber": 1, "pageSize": 1}}
        start_at = params["startDate"] or params["startHappendDate"]
        end_at = params["endDate"] or params["endHappendDate"]
        source.report(stage, 楼栋筛选编码="、".join(params["domainCodeList"]) or "空",
                      状态筛选编码=params["orderStatus"] or "空",
                      日期范围=f"{start_at} 至 {end_at}" if start_at else "空")
        try:
            _pages(source, "event_list", lambda page: params, 1, stage=stage, probe=True)
        except ZhihangSourceError as error:
            source.report(stage, 结果=str(error))
            if error.code in {"auth_required", "access_denied", "source_unavailable"}:
                break
    source.report("事件近30天结果核验", 有效记录=0)


def _inspection_rows(source, filters):
    def read(size, stage, *, building=""):
        return _pages(source, "inspection_list",
                      lambda page: filters | {"pageNum": page, "pageSize": size, "buildingCode": building},
                      size, workers=5 if size == 15 else 1, stage=stage)

    try:
        return read(15, "巡检首次分页")
    except _InspectionShortBatch as first:
        try:
            return read(50, "巡检换页重拉")
        except _InspectionShortBatch as second:
            source.report("巡检按楼分批核验", 源端总数=second.total)
            if first.total != second.total:
                raise _incomplete("inspection_list", "两次分页的源端总数不同，请重新预览。") from None
            observed = first.rows + second.rows
            buildings = sorted({_first(row, "buildingCode") for row in observed} - {""})
            if not buildings:
                # The source client supports larger pages; still require exact coverage.
                return read(500, "巡检扩大页长重拉")
            combined = {}
            for index, building in enumerate(buildings, 1):
                label = next((_building(row.get("buildingName")) for row in observed
                              if _first(row, "buildingCode") == building), "")
                batch = read(50, f"巡检分楼 {index} · {label or '未识别楼栋'}", building=building)
                for row in batch:
                    key = _inspection_source_key(row)
                    if key in combined:
                        raise _incomplete("inspection_list", "同一工单 ID 出现在不同楼栋。") from None
                    combined[key] = row
            source.report("巡检按楼分批核验", 唯一工单数=len(combined))
            if (len(combined) != second.total or
                    any(_inspection_source_key(row) not in combined for row in observed)):
                raise _incomplete("inspection_list", f"按楼拉取后有 {len(combined)} 张唯一工单，"
                                  f"无法与源端 {second.total} 张完整对账。") from None
            return list(combined.values())


def _inspection(source, now):
    filters = dict.fromkeys("""
        jobInfo jobExecuteStatus buildingCode withinDay userName executeCycle startDateTime endDateTime
        startSubmitTime endSubmitTime
    """.split(), "") | {
        "datacenterCode": "0.5.2.2.1.1",
        "planStartDatetime": f"{now:%Y-%m}-01 00:00:00",
        "planEndDatetime": f"{now:%Y-%m}-{calendar.monthrange(now.year, now.month)[1]:02d} 23:59:59",
    }
    rows = _inspection_rows(source, filters)

    def map_row(row):
        start, end = (_date(row.get(key)) for key in ("planStartDatetime", "planEndDatetime"))
        if start is None or end is None or start > end:
            raise _incomplete("inspection", "巡检工单计划区间缺失或无效。")
        fields = _fields(row, {
            "工单名称": "planName", "数据中心": "datacenterName", "位置信息": "locations", "巡检人": "userName",
            "提交时间": "submitTime", "接单时间": "orderReceiveTime execStartTime",
            "工单状态": "jobExecuteStatusName jobExecuteStatus", "逾期原因": "overdueReason",
        })
        params = row.get("params") or {}
        stats = (params.get("statMap") or {}) if isinstance(params, dict) else {}
        if not isinstance(stats, dict):
            raise _incomplete("inspection", "巡检结果结构无效。")
        total, normal, abnormal = (_number(stats.get(key)) or 0 for key in ("total", "zc", "yc"))
        hour = start.hour
        due = start.replace(hour=9 if hour < 9 else 15 if hour < 15 else 20 if hour < 20 else 2,
                            minute=0 if hour < 20 else 30, second=0, microsecond=0)
        if hour >= 20:
            due += timedelta(days=1)
        is_abnormal = (
            abnormal > 0 or "逾期" in fields["工单状态"] or bool(fields["逾期原因"])
            or "overdue" in _first(row, "jobExecuteStatus").lower()
            or (not fields["提交时间"] and now >= due)
        )
        building = _first(row, "buildingName")
        match = re.search(r"[A-E]", building, re.IGNORECASE)
        return fields | {
            "序号": row["_sequence"], "楼栋": match[0].upper() + "楼" if match else building or "未识别楼栋",
            "巡检类型": _mapped(row, "executeCycle", dict(zip(
                ("day", "week", "month", "quarter", "year"), ("日常巡检", "周巡检", "月度巡检", "季度巡检", "年度巡检"),
            ))),
            "计划区间": f"{start:%Y/%m/%d %H:%M}-{end:%Y/%m/%d %H:%M}",
            "巡检结果": f"总:{total:g} 正常:{normal:g} 异常:{abnormal:g}" if max(total, normal, abnormal) > 0 else "",
            "异常标": "异常" if is_abnormal else "",
        }

    # The original target has no source ID column. Sequence is display-only.
    numbered = (row | {"_sequence": str(index)} for index, row in enumerate(rows, 1))
    source.report("巡检工单核验", 待核验=len(rows))
    result = _map_rows("inspection", numbered, map_row)
    source.report("巡检工单核验", 核验通过=len(result))
    return {"inspection": result}


_DRILL_PLAN_FIELDS = {
    "计划ID": "id", "计划编号": "operationPlanNumber", "演练场景": "exerciseScenarioName", "专业": "exerciseMajor",
    "演练月份": "exerciseMonth", "演练年份": "exerciseYear", "计划演练时间": "plannedExerciseTime",
    "计划结束时间": "plannedExerciseEndTime", "实际演练时间": "actualExerciseTime", "触发时间": "triggerTime",
    "触发人": "triggerBy", "创建时间": "createTime", "创建人": "createBy", "班组": "exerciseObjectStr",
    "EOP流程": "eopFlowStr",
}
_DRILL_STATUS = dict(zip(("5", "11", "12", "13"), ("待评估", "完成", "评估待审批", "终止")))
_DRILL_SCORE_FIELDS = """
    drillEvaluationScore evaluationScore evaluateScore assessmentScore eventScore score totalScore finalScore
    realScore actualScore scoreResult gradeScore fraction
""".split()
_DETAIL_SCORE_FIELDS = """
    totalScore finalScore actualTotalScore evaluationTotalScore drillEvaluationScore evaluationScore score
""".split()


def _drill_month(text):
    value = text.strip().lower().rstrip("月")
    if value.isdecimal():
        number = int(value)
        return number if 1 <= number <= 12 else None
    if value == "sept":
        value = "sep"
    return next((i for i, month in enumerate(_MONTHS, 1) if value in {month.lower(), month[:3].lower()}), None)


def _objects(value):
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _number(value):
    try:
        number = float(_text(value))
        return number if math.isfinite(number) else None
    except (ValueError, OverflowError):
        return None


def _score(row, names):
    return next((value for key in names if (value := _first(row, key)) not in {"", "null", "undefined"}), "")


def _detail_score(payload):
    if not isinstance(payload, dict):
        return ""
    data = payload.get("data", payload)
    if not isinstance(data, dict):
        return ""
    if score := _score(data, _DETAIL_SCORE_FIELDS):
        return score

    def scores(items):
        for item in _objects(items):
            if (number := _number(item.get("actualScore"))) is not None:
                yield number
            for key in ("exerciseEvaluationDetailDtoList", "children", "childrenList", "detailList"):
                yield from scores(item.get(key))

    found = list(scores(data.get("exerciseEvaluationContentDtoList", data.get("evaluationContentList"))))
    return format(sum(found), "g") if found else ""


def _event_score(row):
    return (_score(row, _DRILL_SCORE_FIELDS) or _detail_score(row.get("evaluationDetail"))
            or _detail_score(row.get("evaluationDetailPayload")))


def _drill_area(row):
    labels = {}

    def scan(value):
        if isinstance(value, list):
            for item in value:
                scan(item)
        elif isinstance(value, dict):
            for key in ("datacenterName", "datacenterCode", "dcName", "dcCode", "buildingName", "buildingCode",
                        "name", "label", "value"):
                scan(value.get(key))
        else:
            text = unicodedata.normalize("NFKC", _text(value))
            if match := re.match(r"^([5-9])(?:\.|$)", text):
                labels[_BUILDINGS[match[1]]] = None
                return
            for part in re.split(r"[、,，;；|]+", text):
                if re.fullmatch("[A-E]", part.strip(), re.IGNORECASE):
                    labels[part.strip().upper() + "楼"] = None
            patterns = (r"数据中心[\s_/-]*([A-E])(?:楼|栋)?", r"中心[\s_/-]*([A-E])(?:楼|栋)?",
                        r"([A-E])(?:楼|栋)", r"(?:^|[_/\s-])([A-E])(?:$|[^\w])")
            for pattern in patterns:
                for letter in re.findall(pattern, text, re.IGNORECASE | re.ASCII):
                    labels[letter.upper() + "楼"] = None

    for key in ("implementationAreaStr", "implementationArea", "dcCode", "domainCode"):
        scan(row.get(key))
    for item in _objects(row.get("exerciseObjectList")):
        scan([item.get("datacenterName"), item.get("datacenterCode")])
    area = row.get("implementationArea")
    return "、".join(labels) or _first(row, "implementationAreaStr") or (
        "、".join(_text(item) for item in area) if isinstance(area, list) else ""
    )


def _drill_map(row, events, score):
    fields = _fields(row, _DRILL_PLAN_FIELDS)
    primary = next((item for item in events if _event_score(item)), events[0] if events else {})
    related = _objects(row.get("relatedEventList"))
    categories = {"plan": "计划演练", "surpriseAttack": "突袭演练", "surpriseAttackPlan": "突袭演练-计划性"}
    cycles = {value: label for label, values in {
        "年度": ("annual", "yearly", "year"), "季度": ("quarter", "quarterly"), "月度": ("month", "monthly"),
        "半年度": ("halfYear", "halfYearly", "semiannual", "semiAnnual"),
    }.items() for value in values}
    fields.update(
        演练类别=_mapped(row, "exerciseCategory", categories),
        演练类型=_mapped(row, "exerciseType", {"run": "跑位演练"}),
        实施区域=_drill_area(row),
        演练周期=_mapped(row, "exerciseCycle", cycles),
        审批状态=_mapped(row, "approvalStatus", dict(zip(
            ("NOAPPROVAL", "APPROVING", "COMPLETED", "REJECTED"), ("未审批", "审批中", "审批完成", "已驳回"),
        ))),
        执行状态=_mapped(row, "executeStatus", dict(zip(
            ("NOT_EXECUTED", "EXECUTING", "EXECUTED", "COMPLETED"), ("未执行", "执行中", "已执行", "已完成"),
        ))),
        演练状态=_mapped(row, "orderStatus", _DRILL_STATUS) or "待演练",
        关联事件="\n".join(" / ".join(filter(None, (_first(item, "eventNumber"), _first(item, "title"),
                         f"状态{item['orderStatus']}" if item.get("orderStatus") else ""))) for item in related),
        演练评分=score or _event_score(primary) or _event_score(row),
        评估事件="\n".join(filter(None, (" / ".join(filter(None, (
            _first(item, "eventNumber"), _first(item, "eventTitle", "highestLevelTitle"),
        ))) for item in events))),
        评估事件状态=_mapped(primary, "orderStatus", _DRILL_STATUS) or (
            "待演练" if primary.get("orderStatus") is not None else ""
        ),
        评估事件等级=_first(primary, "eventCurrentLevelName", "eventHighestLevelName", "eventFirstLevelName"),
        响应耗时=_first(primary, "responseTimeCost"),
        确认耗时=_first(primary, "ackTimeCost"),
        恢复时间=_first(primary, "incidentRecoveryTime"),
        是否逾期="是" if row.get("overdue") else "否",
        是否展示EOP="是" if row.get("showEop") else "否",
    )
    for column, name in (("值班人员", "dutyUserList"), ("负责人", "responsibleUserList"), ("评估人", "evaluationUserList")):
        fields[column] = "、".join(filter(None, (_first(item, "name", "jobNumber") for item in _objects(row.get(name)))))
    return fields


def _drill_detail(source, plan, event):
    number, event_id = _first(event, "eventNumber"), _first(event, "eventId", "id")
    related = next((item for item in _objects(plan.get("relatedEventList")) if (
        number and _first(item, "eventNumber") == number or event_id and _first(item, "id", "eventId") == event_id
    )), {})
    plan_id = _first(plan, "id")
    params = {
        "drillId": _first(event, "drillId") or (f"plan_id_{plan_id}" if plan_id else ""),
        "eventId": _first(event, "eventId") or _first(related, "eventId", "id") or _first(event, "id"),
        "endTime": _first(event, "endTime", "incidentRecoveryTime", "endIncidentRecoveryDate", "endResponseDate",
                          "finishTime", "closeTime", "updateTime") or _first(related, "endTime", "incidentRecoveryTime")
        or _first(plan, "evaluationCompleteTime", "actualExerciseTime", "triggerTime"),
        "jobNumber": _first(event, "evaluatorJobNumber") or next((
            _first(item, "jobNumber") for item in _objects(event.get("evaluationUserList"))
            + _objects(plan.get("evaluationUserList")) if _first(item, "jobNumber")
        ), "") or _first(event, "jobNumber", "creatorJobNumber"),
        "viewRole": "evaluator",
    }
    if not all(params.values()):
        # Pending evaluation events may not have an evaluator or end time yet.
        if _first(event, "orderStatus") in {"11", "12"}:
            raise _incomplete("drill", "已评估事件缺少评分详情参数。")
        return ""
    data = source.request("drill_detail", params)
    score = _detail_score(data)
    if not score and _first(event, "orderStatus") in {"11", "12"}:
        raise _incomplete("drill", "已评估事件的评分详情缺失。")
    return score


def _drill(source, now):
    filters = dict.fromkeys("""
        actualExerciseEndTime actualExerciseStartTime dcCode evaluationCompleteEndTime evaluationCompleteStartTime
        exerciseCategory exerciseMonth exerciseScenarioName overdue planApprovalStatusMulti
        plannedExerciseEndTime plannedExerciseStartTime
    """.split(), "") | dict.fromkeys(("dutyGroupId", "executeStatus", "hasEop"))
    month = _MONTHS[now.month - 1][:3]
    codes = [f"{number}.1.2.3.1.1" for number in _BUILDINGS]
    scopes = [("", "")] + [(code, month) for code in ["", *codes]] + [(code, "") for code in codes]
    plans = {}
    for code, month_filter in scopes:
        rows = _pages(source, "drill_list", lambda page: filters | {
            "pageNo": page, "pageSize": 100, "dcCode": code, "exerciseMonth": month_filter,
        }, 100, workers=6)
        seen = set()
        for row in rows:
            # The original stable key checks planId and exercisePlanId as well.
            row = row | {"id": _first(row, "id", "planId", "exercisePlanId")}
            key = identity("drill", _fields(row, _DRILL_PLAN_FIELDS))
            if not key or key in seen:
                raise _incomplete("drill", "演练计划主键缺失或重复。")
            seen.add(key)
            plans[key] = plans.get(key, {}) | row
    if not plans:
        raise ZhihangSourceError("empty", "智航演练计划为空，保留上次已核验数据。")

    filters = _event_filters() | {"drill": True}
    events = _pages(source, "drill_evaluation", lambda page: filters | {
        "pageBean": {"pageNumber": page, "pageSize": 100},
    }, 100, workers=6)
    by_key = defaultdict(list)
    known = set()
    for event in events:
        event_key = _first(event, "id", "eventNumber")
        if not event_key or event_key in known:
            raise _incomplete("drill", "评估事件主键缺失或重复。")
        known.add(event_key)
        for key in dict.fromkeys(filter(None, (_first(event, "drillId"), _first(event, "eventNumber")))):
            by_key[key].append((event_key, event))

    def enrich(plan):
        plan_id = _first(plan, "id")
        keys = ([f"plan_id_{plan_id}"] if plan_id else []) + [
            _first(item, "eventNumber") for item in _objects(plan.get("relatedEventList"))
        ]
        matched = dict(item for key in keys if key for item in by_key[key])
        matched = sorted(matched.values(), key=lambda item: (
            _date(_first(item, "updateTime", "creatTime")) or datetime.min.replace(tzinfo=_BEIJING)
        ), reverse=True)
        primary = next((item for item in matched if _event_score(item)), matched[0] if matched else {})
        score = _drill_detail(source, plan, primary) if primary and not _event_score(primary) else ""
        if not matched and _first(plan, "orderStatus") in {"11", "12"} and not _event_score(plan):
            raise _incomplete("drill", "已评估计划缺少对应评估事件。")
        return _drill_map(plan, matched, score)

    with ThreadPoolExecutor(max_workers=4) as executor:
        return {"drill": dict(zip(plans, executor.map(enrich, plans.values())))}


def pull(kind, now, *, report=None, preview=False):
    """Return a complete batch as {target_key: {identity: Feishu fields}}.

    Naive datetimes use Beijing time. A failed pull returns no partial data.
    """
    adapters = {"risk": _risk, "change": _change, "drill": _drill, "event": _event, "inspection": _inspection}
    if kind not in adapters:
        raise ZhihangSourceError("invalid_kind", "未知的智航数据类别。")
    if not isinstance(now, datetime):
        raise _incomplete(kind, "拉取时间必须是有效的日期时间。")
    return adapters[kind](_Source(report, preview=preview), _local(now))
