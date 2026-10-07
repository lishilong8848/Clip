# -*- coding: utf-8 -*-
"""纯助手侧适配水耗记录创建/修改表单（不改业务文件）。

* ``build_water_form(actor, operation, queries, index)`` 读取已经解析好的
  operation/query，校验楼栋、管理员、bootstrap 选项和记录详情，返回
  ``(native_body, control)``。
* ``water_payload(field, filled, actor)`` 校验用户填写结果，返回
  ``WaterConsumptionRecordRequest`` 的可编辑字段归一化请求体。

POST 仅管理员；PATCH 需要原记录详情、edit_policy.can_edit、同楼栋 bootstrap
和 expected_version。所有照片 token/url/路径等凭证不进入公开 children。
"""
from __future__ import annotations

import copy
import datetime as dt
import math
import re
from datetime import timezone, timedelta

from clipflow_backend.api_models import WaterConsumptionRecordRequest
from .lighthouse_ai import AssistantError
from .lighthouse_sources import codes

WATER_SCOPE_CODES = frozenset("ABCDEH")
_WATER_CREATE_API = "POST /api/capacity/water/records"
_WATER_PATCH_API = "PATCH /api/capacity/water/records/{record_id}"
_EDITABLE_KEYS = (
    "statistic_date",
    "meter",
    "frequency",
    "shift",
    "meter_value",
    "corrected_usage",
    "retained_image_ids",
)
_NATIVE_FIELDS = frozenset(WaterConsumptionRecordRequest.model_fields)
_SELECT_KEYS = {"meter": "水表", "frequency": "统计频次", "shift": "班次"}


def _beijing_today() -> str:
    return dt.datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")


def _scalar_text(value, *, label):
    """只接受文本/数字，绝不对任意对象做字符串化。"""
    if value is None:
        return ""
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise AssistantError("{}格式无效，请重新读取详情。".format(label))
    return str(value).strip()


def _data_scope(data, entry_scope=""):
    return str(data.get("scope_code") or data.get("scope") or entry_scope or "").strip().upper()


def _query_entries(queries):
    """展平 queries —— 直连数据或 {buildings:[{scope,ok,data}]} 包装。"""
    if not isinstance(queries, dict):
        return []
    entries = []
    for value in queries.values():
        if not isinstance(value, dict):
            continue
        buildings = value.get("buildings")
        if isinstance(buildings, list) and buildings:
            for building in buildings:
                if not isinstance(building, dict) or not building.get("ok"):
                    continue
                data = building.get("data")
                if isinstance(data, dict):
                    entries.append((building.get("scope"), data))
        else:
            entries.append((value.get("scope") or value.get("scope_code"), value))
    return entries


def _find_bootstrap(entries, scope):
    for entry_scope, data in reversed(entries):
        data_scope = _data_scope(data, entry_scope)
        if data_scope not in WATER_SCOPE_CODES or data_scope != str(scope).upper():
            continue
        options = data.get("options")
        if "options" not in data or "permissions" not in data:
            continue
        if not isinstance(options, dict):
            raise AssistantError("最新水耗基础数据未完整返回，请重新读取。")
        if not (
            isinstance(options.get("meters"), list)
            and isinstance(options.get("frequencies"), list)
            and isinstance(options.get("shifts"), list)
        ):
            raise AssistantError("最新水耗基础数据选项不完整，请重新读取。")
        if not isinstance(data.get("permissions"), dict):
            raise AssistantError("最新水耗基础数据权限不完整，请重新读取。")
        return data
    return None


def _find_detail(entries, scope, record_id):
    for entry_scope, data in reversed(entries):
        if str(data.get("record_id") or "") != str(record_id or ""):
            continue
        if _data_scope(data, entry_scope) != scope:
            raise AssistantError("最新水耗记录楼栋与本次填写不一致，请重新读取。", 403)
        return data
    return None


def _resolve_scope(actor, body, params, entries, is_patch, record_id=""):
    actor_s = {str(s).upper() for s in (actor.get("scopes") or [])} & WATER_SCOPE_CODES
    if not actor_s:
        raise AssistantError("无楼栋权限，不能操作水耗记录。", 403)

    supplied = []
    for source in (body, params):
        raw = source.get("scope") if isinstance(source, dict) else None
        if raw is None or str(raw).strip() == "":
            continue
        found = codes(raw)
        if len(found) != 1:
            raise AssistantError("水耗记录一次只能操作单一楼栋（A、B、C、D、E、H），不支持全部、多项或无效楼栋。")
        scope = next(iter(found))
        if scope not in WATER_SCOPE_CODES:
            raise AssistantError("水耗记录仅支持 A、B、C、D、E、H 楼。")
        if scope not in actor_s:
            raise AssistantError("水耗记录超出当前楼栋权限。", 403)
        supplied.append(scope)

    if supplied:
        if len(set(supplied)) != 1:
            raise AssistantError("水耗记录楼栋不一致，请核对后重试。")
        return supplied[0]

    if len(actor_s) == 1:
        return next(iter(actor_s))
    if is_patch:
        # 优先根据待编辑的 record_id 唯一匹配详情楼栋
        if str(record_id or "").strip():
            matched = {_data_scope(data, entry_scope) for entry_scope, data in entries
                       if isinstance(data, dict)
                       and str(data.get("record_id") or "") == str(record_id).strip()
                       and _data_scope(data, entry_scope) in WATER_SCOPE_CODES}
            if len(matched) == 1:
                scope = next(iter(matched))
                if scope not in actor_s:
                    raise AssistantError("水耗记录超出当前楼栋权限。", 403)
                return scope
        details = {_data_scope(data, entry_scope) for entry_scope, data in entries
                   if _data_scope(data, entry_scope) in WATER_SCOPE_CODES
                   and isinstance(data, dict) and data.get("record_id") and data.get("edit_policy")}
        if len(details) == 1:
            scope = next(iter(details))
            if scope not in actor_s:
                raise AssistantError("水耗记录超出当前楼栋权限。", 403)
            return scope
    raise AssistantError("请明确要操作的水耗记录楼栋（A、B、C、D、E、H）。")


def _str_options(raw):
    if raw is None:
        return []
    if not isinstance(raw, (list, tuple)):
        raise AssistantError("选项必须是数组。")
    result = []
    for item in raw:
        if isinstance(item, bool) or not isinstance(item, (str, int, float)):
            raise AssistantError("选项只能是文本或数字，不能是对象。")
        text = str(item).strip()
        if text:
            result.append(text)
    return result


_IMAGE_ID_RE = re.compile(r"[A-Za-z0-9_-]+")


def _safe_photos(photos):
    if photos is None:
        return []
    if not isinstance(photos, list):
        raise AssistantError("水耗记录照片列表无效，请重新读取详情。")
    result = []
    seen = set()
    for index, photo in enumerate(photos, 1):
        if not isinstance(photo, dict):
            raise AssistantError("水耗记录照片第 {} 项格式无效，请重新读取详情。".format(index))
        image_id = photo.get("image_id")
        if not isinstance(image_id, str) or not _IMAGE_ID_RE.fullmatch(image_id.strip()):
            raise AssistantError("水耗记录照片标识无效，请重新读取详情。".format(index))
        image_id = image_id.strip()
        if image_id in seen:
            raise AssistantError("水耗记录照片存在重复标识，请重新读取详情。".format(index))
        seen.add(image_id)
        name = ""
        for key in ("file_name", "name"):
            value = photo.get(key)
            if isinstance(value, str) and value.strip():
                name = value.strip()
                break
        if not name:
            name = "水表照片{}".format(index)
        result.append({"image_id": image_id, "name": name})
    return result


def _option_values(children, path):
    child = next((item for item in children or [] if item.get("path") == path), {})
    values = []
    for option in child.get("options") or []:
        if isinstance(option, dict):
            value = option.get("value")
            if isinstance(value, str) and value:
                values.append(value)
    return values


def _finite_number(value, *, label, allow_negative=True):
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        raise AssistantError(f"{label}必须是数字，不能是布尔值。")
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise AssistantError(f"{label}必须是数字。")
    if not math.isfinite(number):
        raise AssistantError(f"{label}必须是有效数字。")
    if not allow_negative and number < 0:
        raise AssistantError(f"{label}必须大于或等于 0。")
    return number


def build_water_form(actor, operation, queries, index):
    """读取已解析 operation/query，返回 (native_body, control)。"""
    if not isinstance(operation, dict):
        raise AssistantError("水耗操作无效。")
    api_id = operation.get("api_id") or ""
    is_patch = api_id == _WATER_PATCH_API
    if api_id not in {_WATER_CREATE_API, _WATER_PATCH_API}:
        raise AssistantError("水耗表单仅支持新建或修改水耗记录。")
    body = operation.get("body") or {}
    if not isinstance(body, dict):
        raise AssistantError("水耗记录请求体无效。")
    unknown = set(body) - _NATIVE_FIELDS
    if unknown:
        raise AssistantError("水耗记录请求包含不支持的字段：{}".format(", ".join(sorted(unknown))))

    params = operation.get("params") or {}
    path_params = operation.get("path_params") or {}
    files = operation.get("files") or {}
    for name, value in (("params", params), ("path_params", path_params), ("files", files)):
        if value is None:
            continue
        if not isinstance(value, dict):
            raise AssistantError("水耗操作请求参数格式无效。")
    if files:
        raise AssistantError("水耗记录操作不能附带文件上传。")
    unknown_params = set(params) - {"scope"}
    if unknown_params:
        raise AssistantError("水耗记录操作不支持其他查询参数：{}".format(", ".join(sorted(unknown_params))))
    allowed_path = {"record_id"} if is_patch else set()
    unknown_path = set(path_params) - allowed_path
    if unknown_path:
        raise AssistantError("水耗记录操作路径参数无效。")

    entries = _query_entries(queries)
    record_id = str((path_params or {}).get("record_id") or body.get("record_id") or "").strip()
    scope = _resolve_scope(actor, body, params, entries, is_patch, record_id)
    bootstrap = _find_bootstrap(entries, scope)
    if not bootstrap:
        raise AssistantError("请先读取 {} 楼完整的水耗基础数据（选项和权限），再准备操作。".format(scope))

    options = bootstrap.get("options") or {}
    meters = _str_options(options.get("meters"))
    frequencies = _str_options(options.get("frequencies"))
    shifts = _str_options(options.get("shifts"))
    if not meters or not frequencies or not shifts:
        raise AssistantError("水耗基础数据选项不完整，请重新读取。")

    # POST：管理员且 bootstrap 明确允许新增
    if not is_patch:
        if not actor.get("is_admin"):
            raise AssistantError("只有管理员可以新增水耗记录。", 403)
        permissions = bootstrap.get("permissions") or {}
        if permissions.get("can_create") is not True:
            raise AssistantError("当前账号无新增水耗记录权限。", 403)
        version = ""
        detail = None
        baseline = {}
        photos = []
    else:
        detail = _find_detail(entries, scope, record_id)
        if not detail:
            raise AssistantError("请先读取该水耗记录的完整详情（含照片和编辑权限），再准备修改。")
        if str(detail.get("scope_code") or "") != scope:
            raise AssistantError("水耗记录不在所选楼栋范围内。", 403)
        version = str(detail.get("version") or "").strip()
        if not version:
            raise AssistantError("水耗记录详情缺少版本号，请重新读取。")
        policy = detail.get("edit_policy")
        if not isinstance(policy, dict) or policy.get("can_edit") is not True:
            raise AssistantError("水耗记录详情缺少可编辑权限，请重新读取。", 409)
        if not isinstance(detail.get("photos"), list):
            raise AssistantError("水耗记录详情缺少水表照片列表，请重新读取。")
        body_expected = body.get("expected_version")
        if body_expected is not None and body_expected != "" and str(body_expected) != version:
            raise AssistantError("水耗记录版本已变化，请重新读取详情。", 409)
        photos = _safe_photos(detail.get("photos"))
        # 原记录可编辑基线（为何 patch 缺省时保留原值）
        baseline = {
            "statistic_date": _scalar_text(detail.get("statistic_date") or detail.get("statistic_date_key"), label="统计日期"),
            "meter": _scalar_text(detail.get("meter"), label="水表"),
            "frequency": _scalar_text(detail.get("frequency"), label="统计频次"),
            "shift": _scalar_text(detail.get("shift"), label="班次"),
            "meter_value": detail.get("meter_value"),
            "corrected_usage": detail.get("corrected_usage"),
            "retained_image_ids": [p["image_id"] for p in photos],
        }

    expected_version = version if is_patch else ""
    # 冻结 operation 元数据
    operation_id = str(body.get("operation_id") or "").strip()
    upload_ids = body.get("upload_ids")
    upload_ids = copy.deepcopy(list(upload_ids)) if isinstance(upload_ids, list) else []

    # 初始可编辑值（仅可编辑字段）
    statistic_date = _beijing_today()
    frequency_default = frequencies[0] if frequencies else ""
    shift_default = shifts[0] if shifts else ""
    if is_patch:
        initial = {
            "statistic_date": baseline["statistic_date"] or statistic_date,
            "meter": baseline["meter"],
            "frequency": baseline["frequency"],
            "shift": baseline["shift"],
            "meter_value": "" if detail.get("meter_value") is None else detail.get("meter_value"),
            "corrected_usage": "" if detail.get("corrected_usage") is None else detail.get("corrected_usage"),
            "retained_image_ids": list(copy.deepcopy(baseline["retained_image_ids"])),
        }
    else:
        initial = {
            "statistic_date": statistic_date,
            "meter": "",
            "frequency": frequency_default,
            "shift": shift_default,
            "meter_value": "",
            "corrected_usage": "",
            "retained_image_ids": [],
        }
    # 覆盖显式可编辑 body（保留显式 null）
    for key in _EDITABLE_KEYS:
        if key in body:
            initial[key] = copy.deepcopy(body[key])

    def _with_original(options, original, label):
        original = _scalar_text(original, label=label)
        if original and original not in [o.get("value") for o in options]:
            options = [*options, {"value": original, "label": "{}（原记录）".format(original)}]
        return options


    meter_options = [{"value": meter, "label": meter} for meter in meters]
    frequency_options = [{"value": item, "label": item} for item in frequencies]
    shift_options = [{"value": item, "label": item} for item in shifts]
    if is_patch and detail:
        meter_options = _with_original(meter_options, detail.get("meter"), "水表")
        frequency_options = _with_original(frequency_options, detail.get("frequency"), "统计频次")
        shift_options = _with_original(shift_options, detail.get("shift"), "班次")

    children = [
        {"path": "statistic_date", "literal_key": True, "type": "date", "label": "统计日期", "required": True},
        {"path": "meter", "literal_key": True, "type": "select", "label": "水表", "required": True,
         "options": meter_options},
        {"path": "frequency", "literal_key": True, "type": "select", "label": "统计频次", "required": True,
         "options": frequency_options},
        {"path": "shift", "literal_key": True, "type": "select", "label": "班次", "required": True,
         "options": shift_options},
        {"path": "meter_value", "literal_key": True, "type": "number", "label": "水表数值", "required": True,
         "min": 0, "step": "any"},
        {"path": "corrected_usage", "literal_key": True, "type": "number", "label": "当期耗水量（修正）",
         "required": False, "nullable": True, "allow_negative": True, "step": "any"},
        {"path": "retained_image_ids", "literal_key": True, "type": "multiselect", "choice_group": True,
         "label": "保留水表照片", "minItems": 0, "maxItems": 50, "hidden": not photos,
         "options": [{"value": photo["image_id"], "label": photo["name"] or "水表照片"} for photo in photos]},
    ]
    building_label = str(bootstrap.get("building") or "{}楼".format(scope))
    question_text = "当前水耗楼栋：{}".format(building_label)

    control = {
        "name": "step{}.water".format(index),
        "path": "",
        "label": "水耗记录" + ("修改" if is_patch else "新增"),
        "section": "body",
        "operation_index": index,
        "type": "object",
        "required": True,
        "native_water_record": True,
        "children": children,
        "question_text": question_text,
        "_initial_form": copy.deepcopy(initial),
        "_scope": scope,
        "_record_id": record_id,
        "_version": version,
        "_photos": copy.deepcopy(photos),
        "_baseline": copy.deepcopy(baseline),
    }

    native_body = {
        "operation_id": operation_id,
        "expected_version": expected_version,
        "large_change_confirmed": False,
        "abnormal_note": "",
        "scope": scope,
        "meter": str(initial.get("meter") or ""),
        "frequency": str(initial.get("frequency") or ""),
        "shift": str(initial.get("shift") or ""),
        "statistic_date": str(initial.get("statistic_date") or ""),
        "meter_value": initial.get("meter_value"),
        "corrected_usage": initial.get("corrected_usage"),
        "upload_ids": copy.deepcopy(upload_ids),
        "retained_image_ids": copy.deepcopy(list(initial.get("retained_image_ids") or [])),
    }
    return native_body, control


def water_payload(field, filled, actor):
    """校验填写结果，返回可编辑字段归一化后的 WaterConsumptionRecordRequest 请求体。"""
    if not isinstance(field, dict) or field.get("native_water_record") is not True:
        raise AssistantError("水耗表单状态无效。")
    actor_s = {str(s).upper() for s in (actor.get("scopes") or [])}
    scope = str(field.get("_scope") or "")
    if scope not in WATER_SCOPE_CODES or scope not in actor_s:
        raise AssistantError("水耗记录超出当前楼栋权限。", 403)
    if filled is None:
        filled = {}
    if not isinstance(filled, dict):
        raise AssistantError("水耗填写内容不是有效对象。")
    if set(filled) - set(_EDITABLE_KEYS):
        raise AssistantError("不能直接填写只读或系统字段。")

    is_patch = bool(field.get("_record_id"))
    baseline = field.get("_baseline") or {}
    if not isinstance(baseline, dict):
        baseline = {}
    values = {}

    def pick(key, default=""):
        if key in filled:
            raw = filled[key]
            if isinstance(raw, bool) or (raw is not None and not isinstance(raw, (str, int, float))):
                raise AssistantError("只能填写文本或数字字段。")
            return "" if raw is None else str(raw)
        value = baseline.get(key, default)
        return "" if value is None else str(value)

    # 日期必须为真实 YYYY-MM-DD
    statistic_date = pick("statistic_date")
    if not statistic_date:
        raise AssistantError("请选择统计日期。")
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", statistic_date):
        raise AssistantError("统计日期必须是有效的 YYYY-MM-DD。")
    try:
        dt.date.fromisoformat(statistic_date)
    except ValueError:
        raise AssistantError("统计日期必须是有效的 YYYY-MM-DD。")
    values["statistic_date"] = statistic_date

    # 水表/频次/班次：新建必须来自当前选项；修改可选择当前选项或保留原值
    for key, label in _SELECT_KEYS.items():
        raw = pick(key)
        if not raw:
            if not is_patch:
                raise AssistantError("请选择{}。".format(label))
            raise AssistantError("请选择{}；必填字段不能省略。".format(label))
        allowed = _option_values(field.get("children") or [], key)
        base_raw = baseline.get(key)
        if isinstance(base_raw, bool) or (base_raw is not None and not isinstance(base_raw, (str, int, float))):
            raise AssistantError("{}原记录值无效。".format(label))
        if raw in allowed or (is_patch and str(base_raw or "").strip() == raw):
            values[key] = raw
        else:
            raise AssistantError("所选{}不在当前楼栋可选范围内，请重新选择。".format(label))

    # 水表数值：必填、有限、>=0
    if "meter_value" in filled:
        meter_value = filled["meter_value"]
    else:
        meter_value = baseline.get("meter_value")
    if meter_value is None or meter_value == "":
        if not is_patch:
            raise AssistantError("请填写水表数值。")
        raise AssistantError("请填写水表数值；必填字段不能省略。")
    number = _finite_number(meter_value, label="水表数值", allow_negative=False)
    values["meter_value"] = number

    # 当期耗水量（修正）：可选、可空、允许负数、有限数字
    if "corrected_usage" in filled:
        corrected_raw = filled["corrected_usage"]
    else:
        corrected_raw = baseline.get("corrected_usage")
    if corrected_raw is None or corrected_raw == "":
        corrected = None
    else:
        corrected = _finite_number(corrected_raw, label="当期耗水量（修正）", allow_negative=True)
    values["corrected_usage"] = corrected

    # 保留照片：仅能选原记录照片（新建或无照片时只能为空）
    if "retained_image_ids" in filled:
        retained = filled["retained_image_ids"]
    else:
        retained = baseline.get("retained_image_ids", [])
    if not isinstance(retained, list):
        raise AssistantError("保留水表照片必须是列表。")
    photos = field.get("_photos") or []
    valid_ids = {p.get("image_id") for p in photos if isinstance(p, dict) and isinstance(p.get("image_id"), str)}
    clean_retained = []
    for item in retained:
        if not isinstance(item, str) or not item.strip():
            raise AssistantError("保留水表照片标识必须是非空文本。")
        value = item.strip()
        if value not in valid_ids:
            raise AssistantError("所选保留照片不在原记录水表照片中。")
        clean_retained.append(value)
    values["retained_image_ids"] = list(dict.fromkeys(clean_retained))

    return {
        "meter": values["meter"],
        "frequency": values["frequency"],
        "shift": values["shift"],
        "statistic_date": values["statistic_date"],
        "meter_value": values["meter_value"],
        "corrected_usage": values["corrected_usage"],
        "retained_image_ids": values["retained_image_ids"],
    }
