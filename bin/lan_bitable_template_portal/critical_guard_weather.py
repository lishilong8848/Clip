# -*- coding: utf-8 -*-
from __future__ import annotations

import datetime as dt
import hashlib
import json
import re
from typing import Any


WEATHER_SHEET_NAME_PARTS = (
    (("设备安全",), "设备安全"),
    (("环境安全",), "环境安全"),
    (("客户重保",), "客户重保"),
    (("灾害专项",), "灾害专项"),
    (("物资检查清单", "重保物资清单", "物资清单"), "物资检查清单"),
    (("重保联络清单", "中报联络清单", "联络清单"), "重保联络清单"),
)
SUPPORTED_WEATHER_SCHEMA_MAJOR = "2"
MIN_WEATHER_POLL_SECONDS = 10 * 60
DEFAULT_WEATHER_GUARD_ACTIONS = (
    "相关人员7*24 On call，通知相关设备供应商7*24 On call；",
    "现场团队进行内部预警通报；",
    "明确戒备期间相关人员，填写《重保戒备检查表-重保联络清单》，启动重保戒备检查，填写《重保戒备检查表》中对应检查页，并排查问题清零；",
    "明确戒备期间现场 7*24 岗值班人员排班信息，无特殊情况不得随意调换班/请假；",
    "应急储备物资清点，填写《重保戒备检查表-物资检查清单》，应急物资可用性检查、物资排查问题清零；",
)

_SHANGHAI_TIMEZONE = dt.timezone(dt.timedelta(hours=8))
_GUARD_LEVEL_RANKS = {"一级戒备": 1, "二级戒备": 2, "三级戒备": 3}


def _text(value: Any) -> str:
    return str(value or "").strip()


def _inline_text(value: Any, *, limit: int = 180) -> str:
    return " ".join(_text(value).split())[:limit]


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _format_clock(value: Any) -> str:
    text = _text(value)
    if not text:
        return ""
    normalized = text.replace("Z", "+00:00")
    try:
        parsed = dt.datetime.fromisoformat(normalized)
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(dt.timezone(dt.timedelta(hours=8)))
        return parsed.strftime("%m-%d %H:%M")
    except ValueError:
        return text[:16]


def _warning_local_day(value: Any) -> str:
    if isinstance(value, (int, float)) and value:
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp /= 1000
        try:
            parsed = dt.datetime.fromtimestamp(timestamp, _SHANGHAI_TIMEZONE)
            return parsed.date().isoformat()
        except (OSError, OverflowError, ValueError):
            return ""
    text = _text(value)
    if not text:
        return ""
    try:
        parsed = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        try:
            return dt.date.fromisoformat(text[:10]).isoformat()
        except ValueError:
            return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_SHANGHAI_TIMEZONE)
    return parsed.astimezone(_SHANGHAI_TIMEZONE).date().isoformat()


def weather_warning_family_key(warning: dict[str, Any]) -> tuple[str, str]:
    """Return the stable warning type/name pair used only for upgrade reuse."""
    warning_type = re.sub(
        r"[\s，,。；;：:、\-—_（）()【】\[\]]+",
        "",
        _text(warning.get("type")).lower(),
    )
    title = _text(warning.get("title")).lower()
    title = title.replace("升级发布", "发布").replace("继续发布", "发布")
    title = re.sub(r"(?:红色|橙色|黄色|蓝色)", "", title)
    title = re.sub(r"(?:一级|二级|三级)戒备", "", title)
    title = re.sub(r"[\s，,。；;：:、\-—_（）()【】\[\]]+", "", title)
    return warning_type, title


def is_same_day_weather_guard_upgrade(
    previous: dict[str, Any],
    current: dict[str, Any],
    *,
    previous_fallback_time: Any = "",
    current_fallback_time: Any = "",
) -> bool:
    previous_rank = _GUARD_LEVEL_RANKS.get(_text(previous.get("guard_level")))
    current_rank = _GUARD_LEVEL_RANKS.get(_text(current.get("guard_level")))
    if not previous_rank or not current_rank or current_rank >= previous_rank:
        return False
    previous_id = _text(previous.get("id"))
    current_id = _text(current.get("id"))
    previous_family = weather_warning_family_key(previous)
    same_warning = bool(previous_id and previous_id == current_id) or (
        all(previous_family) and previous_family == weather_warning_family_key(current)
    )
    if not same_warning:
        return False
    previous_day = next(
        (
            day
            for value in (
                previous.get("publish_time"),
                previous.get("start_time"),
                previous_fallback_time,
            )
            if (day := _warning_local_day(value))
        ),
        "",
    )
    current_day = next(
        (
            day
            for value in (
                current.get("publish_time"),
                current.get("start_time"),
                current_fallback_time,
            )
            if (day := _warning_local_day(value))
        ),
        "",
    )
    return bool(previous_day and previous_day == current_day)


def _weather_schema_version(payload: dict[str, Any]) -> str:
    version = _text(payload.get("schemaVersion"))
    if not version or version.split(".", 1)[0] != SUPPORTED_WEATHER_SCHEMA_MAJOR:
        raise ValueError(
            f"天气预警接口契约版本不受支持：{version or '缺失'}，需要 2.x。"
        )
    return version


def _sheet_from_required_form(value: Any) -> str:
    name = _text(value)
    if not name:
        return ""
    for aliases, sheet_name in WEATHER_SHEET_NAME_PARTS:
        if any(alias in name for alias in aliases):
            return sheet_name
    return ""


def required_weather_sheets(payload: dict[str, Any]) -> list[str]:
    guard = payload.get("guard") if isinstance(payload.get("guard"), dict) else {}
    if payload.get("hasGuard") is not True or guard.get("required") is not True:
        return []
    requirements = payload.get("registrationRequirements")
    if not isinstance(requirements, dict):
        raise ValueError("天气预警接口缺少 registrationRequirements 对象。")
    if requirements.get("required") is not True:
        return []
    required_forms = requirements.get("requiredForms")
    if not isinstance(required_forms, list):
        raise ValueError("天气预警接口缺少 registrationRequirements.requiredForms。")
    sheets: list[str] = []
    unsupported: list[str] = []
    for item in required_forms:
        sheet = _sheet_from_required_form(item)
        if sheet and sheet not in sheets:
            sheets.append(sheet)
        elif _text(item) and not sheet:
            unsupported.append(_text(item))
    if unsupported:
        raise ValueError(
            "天气预警接口返回了程序尚未支持的必填检查表："
            + "、".join(dict.fromkeys(unsupported))
        )
    return sheets


def normalize_weather_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("天气预警接口返回不是 JSON 对象。")
    schema_version = _weather_schema_version(payload)
    status = _text(payload.get("status")).lower()
    source_state = _text(payload.get("sourceState")).lower()
    decision_state = _text(payload.get("guardDecisionState")).lower()
    guard = payload.get("guard") if isinstance(payload.get("guard"), dict) else {}
    guard_level = _text(guard.get("level") or payload.get("guardLevel"))
    has_warning = payload.get("hasWarning") is True
    has_guard = payload.get("hasGuard") is True
    raw_warnings_value = payload.get("warnings")
    if not isinstance(raw_warnings_value, list):
        raise ValueError("天气预警接口缺少 warnings 数组。")
    raw_warnings = list(raw_warnings_value)
    if has_warning != bool(raw_warnings):
        raise ValueError("天气预警接口的 hasWarning 与 warnings 数据不一致。")
    if has_guard != (guard.get("required") is True):
        raise ValueError("天气预警接口的 hasGuard 与 guard.required 数据不一致。")
    sheet_types = required_weather_sheets(payload)
    warnings: list[dict[str, Any]] = []
    for raw in raw_warnings:
        if not isinstance(raw, dict):
            raise ValueError("天气预警接口 warnings 中存在无效记录。")
        if raw.get("guardRequired") is not True:
            continue
        warning_guard = _text(raw.get("guardLevel") or guard_level)
        title = _text(raw.get("title"))
        warning_id = _text(raw.get("id"))
        if not warning_id or not warning_guard or not title:
            raise ValueError("天气预警接口的戒备预警缺少 id、title 或 guardLevel。")
        warning = {
            "id": warning_id,
            "title": title,
            "type": _text(raw.get("type")),
            "color": _text(raw.get("color")).lower(),
            "guard_level": warning_guard,
            "publish_time": _text(raw.get("publishTime")),
            "start_time": _text(raw.get("startTime")),
            "end_time": _text(raw.get("endTime")),
            "status": _text(raw.get("status")),
            "sheet_types": list(sheet_types),
        }
        key_payload = {
            "id": warning_id,
            "title": title,
            "guard_level": warning_guard,
            "sheet_types": sorted(sheet_types),
        }
        warning["weather_key"] = hashlib.sha256(
            json.dumps(key_payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        warnings.append(warning)
    if has_guard and not warnings:
        raise ValueError("天气预警接口声明存在戒备，但没有可处理的戒备预警。")

    weather = payload.get("weather") if isinstance(payload.get("weather"), dict) else {}
    normalized_weather = {
        "condition": _text(weather.get("condition")),
        "temperature": weather.get("temperature"),
        "feels_like": weather.get("feelsLikeTemperature"),
        "humidity": weather.get("relativeHumidity"),
        "wind_direction": _text(weather.get("windDirection")),
        "wind_level": weather.get("windLevel"),
        "wind_scale": weather.get("windLevelText"),
        "dew_point": weather.get("dewPointTemperature"),
        "pressure": weather.get("atmosphericPressure"),
        "wet_bulb": weather.get("wetBulbTemperature"),
        "observed_at": _text(weather.get("observedAt")),
    }
    actions = _list(guard.get("actions"))
    requirements = (
        payload.get("registrationRequirements")
        if isinstance(payload.get("registrationRequirements"), dict)
        else {}
    )
    units = payload.get("units") if isinstance(payload.get("units"), dict) else {}
    try:
        poll_interval_seconds = max(
            MIN_WEATHER_POLL_SECONDS,
            int(payload.get("pollIntervalSeconds") or MIN_WEATHER_POLL_SECONDS),
        )
    except (TypeError, ValueError):
        raise ValueError("天气预警接口 pollIntervalSeconds 格式无效。") from None
    authoritative_for_state = (
        status in {"active", "clear"}
        and source_state == "fresh"
        and decision_state == "fresh"
    )
    fresh_for_publish = (
        authoritative_for_state
        and status == "active"
        and has_guard
        and guard.get("required") is True
        and bool(sheet_types)
        and bool(warnings)
    )
    return {
        "schema_version": schema_version,
        "status": status,
        "source_state": source_state,
        "guard_decision_state": decision_state,
        "authoritative_for_state": authoritative_for_state,
        "fresh_for_publish": fresh_for_publish,
        "snapshot_at": _text(payload.get("generatedAt")),
        "last_success_at": _text(payload.get("lastSuccessAt")),
        "poll_interval_seconds": poll_interval_seconds,
        "has_warning": has_warning,
        "has_guard": has_guard,
        "guard_level": guard_level,
        "primary_warning_id": _text(payload.get("primaryWarningId")),
        "warnings": warnings,
        "unmatched_warning_count": max(0, len(raw_warnings) - len(warnings)),
        "sheet_types": sheet_types,
        "actions": [_text(item) for item in actions if _text(item)],
        "initial_notification_enabled": guard.get("initialNotificationEnabled") is True,
        "progress_notification_enabled": guard.get("progressNotificationEnabled") is True,
        "reminder_interval_minutes": guard.get("reminderIntervalMinutes"),
        "registration": {
            "required": requirements.get("required") is True,
            "enabled": requirements.get("enabled") is True,
            "handling_mode": _text(requirements.get("handlingMode")),
            "summary": _text(requirements.get("summary")),
            "applicable_buildings": [
                _text(item)
                for item in _list(requirements.get("applicableBuildings"))
                if _text(item)
            ],
            "required_forms": [
                _text(item)
                for item in _list(requirements.get("requiredForms"))
                if _text(item)
            ],
            "required_contents": [
                _text(item)
                for item in _list(requirements.get("requiredContents"))
                if _text(item)
            ],
            "attachment_requirements": [
                _text(item)
                for item in _list(requirements.get("attachmentRequirements"))
                if _text(item)
            ],
        },
        "units": dict(units),
        "weather": normalized_weather,
        "raw": payload,
    }


def warning_task_name(warning: dict[str, Any]) -> str:
    title = _text(warning.get("title")) or "南通天气重保"
    level = _text(warning.get("guard_level"))
    return f"{title} · {level}" if level and level not in title else title


def weather_cells_patch(warning: dict[str, Any]) -> dict[str, Any]:
    title = _text(warning.get("title"))
    level = _text(warning.get("guard_level"))
    weather = {"level1": "", "level2": "", "current": title}
    if level == "一级戒备":
        weather["level1"] = title
    elif level == "二级戒备":
        weather["level2"] = title
    return {"weather": weather}


def progress_summary(task: dict[str, Any], scopes: tuple[str, ...]) -> dict[str, Any]:
    responses = [item for item in task.get("responses") or [] if isinstance(item, dict)]
    required_total = len(responses)
    submitted_total = sum(item.get("status") == "submitted" for item in responses)
    scope_rows: list[dict[str, Any]] = []
    registered = 0
    completed_scopes = 0
    abnormal = 0
    for scope in scopes:
        items = [item for item in responses if _text(item.get("scope")).upper() == scope]
        completed = sum(item.get("status") == "submitted" for item in items)
        if completed:
            registered += 1
        complete = bool(items) and completed == len(items)
        if complete:
            completed_scopes += 1
        scope_abnormal_items: list[str] = []
        for item in items:
            cells = item.get("cells") if isinstance(item.get("cells"), dict) else {}
            checks = cells.get("checks") if isinstance(cells.get("checks"), dict) else {}
            template_items = {
                _text(row.get("key")): row
                for row in cells.get("template_items") or []
                if isinstance(row, dict) and _text(row.get("key"))
            }
            sheet_type = _inline_text(item.get("sheet_type")) or "检查项"
            for check_key, check in checks.items():
                if not isinstance(check, dict) or _text(check.get("status")) != "abnormal":
                    continue
                abnormal += 1
                template_item = template_items.get(_text(check_key)) or {}
                check_content = _inline_text(
                    template_item.get("content") or template_item.get("category") or check_key
                )
                note = _inline_text(check.get("note")) or "存在异常项"
                scope_abnormal_items.append(
                    f"{sheet_type} · {check_content}：{note}"
                )
        scope_rows.append(
            {
                "scope": scope,
                "submitted": completed,
                "total": len(items),
                "complete": complete,
                "registered": completed > 0,
                "abnormal_count": len(scope_abnormal_items),
                "abnormal_items": list(dict.fromkeys(scope_abnormal_items)),
            }
        )
    return {
        "registered_scopes": registered,
        "completed_scopes": completed_scopes,
        "scope_count": len(scopes),
        "submitted": submitted_total,
        "total": required_total,
        "abnormal": abnormal,
        "scopes": scope_rows,
        "complete": bool(required_total) and submitted_total == required_total,
    }


def build_weather_guard_card(
    *,
    weather_task: dict[str, Any],
    progress: dict[str, Any],
    registration_url: str,
    template_url: str,
    message_kind: str = "initial",
    recipient_scope: str = "",
    include_actions: bool = True,
) -> dict[str, Any]:
    source = weather_task.get("source_payload") if isinstance(weather_task.get("source_payload"), dict) else {}
    snapshot = source.get("snapshot") if isinstance(source.get("snapshot"), dict) else source
    weather = snapshot.get("weather") if isinstance(snapshot.get("weather"), dict) else {}
    warning_title = _text(weather_task.get("warning_title"))
    guard_level = _text(weather_task.get("guard_level"))
    scope_completed = message_kind == "completed"
    all_completed = (
        scope_completed and bool(progress.get("complete")) and not recipient_scope
    )
    kind_prefix = "提醒" if message_kind == "reminder" else "发布"
    title = f"南通天气重保 · {guard_level}"
    if all_completed:
        title = "南通天气重保 · 全部楼栋已完成"
    elif scope_completed and recipient_scope:
        title = f"{recipient_scope}楼重保检查 · 本楼已完成"
    progress_line = (
        f"已登记 {progress.get('registered_scopes', 0)}/{progress.get('scope_count', 0)} · "
        f"完成 {progress.get('submitted', 0)}/{progress.get('total', 0)} · "
        f"异常 {progress.get('abnormal', 0)}"
    )
    scope_lines = []
    for item in progress.get("scopes") or []:
        state = "已完成" if item.get("complete") else "进行中" if item.get("registered") else "未登记"
        scope_lines.append(
            f"{item.get('scope')}楼 {item.get('submitted', 0)}/{item.get('total', 0)} · {state}"
        )
        scope_lines.extend(
            f"　异常：{_inline_text(detail)}"
            for detail in item.get("abnormal_items") or []
            if _inline_text(detail)
        )
    weather_lines = []
    if weather.get("temperature") not in (None, ""):
        weather_lines.append(f"温度 {weather.get('temperature')}℃")
    if weather.get("feels_like") not in (None, ""):
        weather_lines.append(f"体感 {weather.get('feels_like')}℃")
    if weather.get("humidity") not in (None, ""):
        weather_lines.append(f"湿度 {weather.get('humidity')}%")
    wind = " ".join(
        part for part in [_text(weather.get("wind_direction")), _text(weather.get("wind_scale"))] if part
    )
    if wind:
        weather_lines.append(f"风力 {wind}")
    actions = [_text(item) for item in source.get("actions") or [] if _text(item)]
    if not actions:
        actions = list(DEFAULT_WEATHER_GUARD_ACTIONS)
    if scope_completed:
        scope_row = next(
            (
                item
                for item in progress.get("scopes") or []
                if _text(item.get("scope")).upper() == _text(recipient_scope).upper()
            ),
            {},
        )
        if all_completed:
            completion_summary = (
                f"全部 {progress.get('scope_count', 0)} 个楼栋已完成本次重保检查，"
                "汇总结果已成功上传。"
            )
            completion_heading = "**整体完成情况**"
        else:
            completion_summary = (
                f"{recipient_scope}楼已完成本次 "
                f"{scope_row.get('submitted', 0)}/{scope_row.get('total', 0)} 项检查并提交结果。"
            )
            completion_heading = "**其他楼栋进度**"
        body_lines = [
            f"**{warning_title}**",
            completion_summary,
            "",
            f"**完成汇总**　{progress_line}",
            completion_heading,
            *scope_lines,
        ]
    else:
        body_lines = [
            f"**{warning_title}**",
            f"{kind_prefix} {_format_clock(source.get('snapshot_at')) or '刚刚'} · 数据已刷新",
            "",
            "　".join(weather_lines),
            "",
            f"**楼栋进度**　{progress_line}",
            *scope_lines,
        ]
    if actions:
        body_lines.extend(["", f"**戒备要求 · {len(actions)}项**"])
        body_lines.extend(f"{index}. {item}" for index, item in enumerate(actions, 1))
    elements: list[dict[str, Any]] = [
        {
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": "\n".join(line for line in body_lines if line is not None),
            },
        }
    ]
    if include_actions and not scope_completed:
        elements.append(
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "type": "primary",
                        "text": {"tag": "plain_text", "content": "提交戒备登记"},
                        "url": registration_url,
                    },
                    {
                        "tag": "button",
                        "type": "default",
                        "text": {"tag": "plain_text", "content": "下载重保检查单"},
                        "url": template_url,
                    },
                ],
            }
        )
    return {
        "config": {"wide_screen_mode": True, "enable_forward": True},
        "header": {
            "template": "turquoise" if all_completed else "green" if scope_completed else "blue",
            "title": {"tag": "plain_text", "content": title},
        },
        "elements": elements,
    }
