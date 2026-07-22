# -*- coding: utf-8 -*-
from __future__ import annotations

import time

from clipflow_backend.runtime_helpers import external_guard_status
from lan_bitable_template_portal.portal_service import (
    CHANGE_SOURCE_APP_TOKEN,
    CHANGE_SOURCE_TABLE_ID,
    DEFAULT_APP_TOKEN,
    DEFAULT_TABLE_ID,
    NOTICE_TYPE_CHANGE,
    NOTICE_TYPE_MAINTENANCE,
    NOTICE_TYPE_REPAIR,
    REPAIR_SOURCE_APP_TOKEN,
    REPAIR_SOURCE_TABLE_ID,
    ZHIHANG_CHANGE_APP_TOKEN,
    ZHIHANG_CHANGE_TABLE_ID,
    MaintenancePortalService,
)
from upload_event_module.config import config, get_field_config


def _field_names(metas: list) -> set[str]:
    names: set[str] = set()
    for meta in metas or []:
        field_name = str(getattr(meta, "field_name", "") or "").strip()
        if field_name:
            names.add(field_name)
    return names


def _missing_fields(
    names: set[str],
    fields: list[str] | None,
    aliases: dict[str, list[str]] | None = None,
) -> list[str]:
    aliases = aliases or {}
    missing: list[str] = []
    for field_name in fields or []:
        candidates = [str(field_name or "").strip()]
        candidates.extend(str(item or "").strip() for item in aliases.get(field_name, []))
        candidates = [item for item in candidates if item]
        if candidates and not any(candidate in names for candidate in candidates):
            missing.append(candidates[0])
    return missing


def _check_field_set(
    service: MaintenancePortalService,
    *,
    label: str,
    app_token: str,
    table_id: str,
    required: list[str],
    optional: list[str] | None = None,
    required_aliases: dict[str, list[str]] | None = None,
    optional_aliases: dict[str, list[str]] | None = None,
) -> dict:
    app_token = str(app_token or "").strip()
    table_id = str(table_id or "").strip()
    if not app_token or not table_id:
        return {
            "label": label,
            "status": "warning",
            "message": "app_token 或 table_id 未配置，已跳过字段检查。",
            "missing_required": list(required or []),
            "missing_optional": [],
            "field_count": 0,
        }
    if not hasattr(service, "_load_table_fields"):
        return {
            "label": label,
            "status": "warning",
            "message": "当前服务对象不支持字段预检。",
            "missing_required": [],
            "missing_optional": [],
            "field_count": 0,
        }
    try:
        metas, _ = service._load_table_fields(app_token=app_token, table_id=table_id)
        names = _field_names(metas)
        missing_required = _missing_fields(names, required, required_aliases)
        missing_optional = _missing_fields(names, optional, optional_aliases)
        status = "fail" if missing_required else "warning" if missing_optional else "ok"
        message = (
            "缺少必需字段。"
            if missing_required
            else "缺少可选字段，部分回填可能为空。"
            if missing_optional
            else "字段检查通过。"
        )
        return {
            "label": label,
            "status": status,
            "message": message,
            "missing_required": missing_required,
            "missing_optional": missing_optional,
            "field_count": len(names),
        }
    except Exception as exc:
        return {
            "label": label,
            "status": "fail",
            "message": str(exc),
            "missing_required": [],
            "missing_optional": [],
            "field_count": 0,
        }


def build_backend_preflight_report(service: MaintenancePortalService) -> dict:
    started = time.perf_counter()
    checks: list[dict] = []
    config_checks = [
        ("飞书 App ID", bool(str(config.app_id or "").strip())),
        ("飞书 App Secret", bool(str(config.app_secret or "").strip())),
        ("目标 app_token", bool(str(config.app_token or "").strip())),
        ("维保目标表", bool(str(config.table_id_weibao or "").strip())),
        ("变更目标表", bool(str(config.table_id_biangeng or "").strip())),
        ("检修目标表", bool(str(config.table_id_overhaul or "").strip())),
    ]
    for label, ok in config_checks:
        checks.append(
            {
                "label": label,
                "status": "ok" if ok else "warning",
                "message": "已配置。" if ok else "未配置，相关真实链路会失败。",
            }
        )
    checks.extend(
        [
            _check_field_set(
                service,
                label="维保源表字段",
                app_token=getattr(service, "app_token", "") or DEFAULT_APP_TOKEN,
                table_id=getattr(service, "table_id", "") or DEFAULT_TABLE_ID,
                required=[
                    "楼栋",
                    "维护总项",
                    "维护实施状态",
                    "计划维护月份",
                    "专业类别",
                    "维护周期",
                ],
                optional=["维护编号", "维护项目"],
            ),
            _check_field_set(
                service,
                label="变更源表字段",
                app_token=CHANGE_SOURCE_APP_TOKEN,
                table_id=CHANGE_SOURCE_TABLE_ID,
                required=[
                    "变更简述",
                    "变更进度",
                    "变更楼栋",
                    "专业",
                    "变更等级（阿里）",
                ],
                optional=["变更开始日期（阿里）", "变更结束日期（阿里）"],
            ),
            _check_field_set(
                service,
                label="检修源表字段",
                app_token=REPAIR_SOURCE_APP_TOKEN,
                table_id=REPAIR_SOURCE_TABLE_ID,
                required=["检修通告名称", "所属数据中心/楼栋-使用"],
                optional=[
                    "维修名称",
                    "所属专业",
                    "专业（推送消息用）",
                    "故障发生时间",
                    "维修开始时间",
                    "期望完成时间",
                    "事件描述",
                    "对应事件等级",
                    "对应来源",
                    "设备检修关联-L",
                ],
                optional_aliases={
                    "设备检修关联-L": ["设备检修关联"],
                },
            ),
            _check_field_set(
                service,
                label="智航变更源表字段",
                app_token=ZHIHANG_CHANGE_APP_TOKEN,
                table_id=ZHIHANG_CHANGE_TABLE_ID,
                required=["进展"],
                optional=["标题"],
                required_aliases={"进展": ["进度"]},
                optional_aliases={
                    "标题": ["名称", "变更名称", "变更标题", "变更简述", "工作内容"]
                },
            ),
            _check_field_set(
                service,
                label="维保目标表字段",
                app_token=config.app_token,
                table_id=config.table_id_weibao,
                required=list(get_field_config(NOTICE_TYPE_MAINTENANCE).values()),
            ),
            _check_field_set(
                service,
                label="变更目标表字段",
                app_token=config.app_token,
                table_id=config.table_id_biangeng,
                required=list(get_field_config(NOTICE_TYPE_CHANGE).values()),
            ),
            _check_field_set(
                service,
                label="检修目标表字段",
                app_token=config.app_token,
                table_id=config.table_id_overhaul,
                required=list(get_field_config(NOTICE_TYPE_REPAIR).values()),
            ),
        ]
    )
    failed = sum(1 for item in checks if item.get("status") == "fail")
    warnings = sum(1 for item in checks if item.get("status") == "warning")
    return {
        "checked_at": time.time(),
        "duration_ms": round((time.perf_counter() - started) * 1000.0, 1),
        "status": "fail" if failed else "warning" if warnings else "ok",
        "failed": failed,
        "warnings": warnings,
        "checks": checks,
        "external_guard": external_guard_status(),
    }
