# -*- coding: utf-8 -*-
import re
from datetime import datetime

from ...core.parser import extract_event_info
from ...config import (
    OVERHAUL_NOTICE_FIELDS,
    OVERHAUL_URGENCY_OPTIONS,
    OVERHAUL_DISCOVERY_OPTIONS,
    OPTION_SLASH,
    STATUS_NEW,
    STATUS_START,
    STATUS_END,
    STATUS_UPDATE,
)
from ...time_parser import parse_single_datetime, parse_time_only
from .base import BaseNoticeHandler, NoticePayload


class OverhaulNoticeHandler(BaseNoticeHandler):
    """设备检修处理"""

    notice_types = ("设备检修",)
    table_id_attr = "table_id_overhaul"

    def build_create_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        status = self._normalize_status(info.get("status", ""))
        title = info.get("title", "") or self._extract_section(payload.text, "标题")

        fields = {}
        if payload.buildings:
            fields[OVERHAUL_NOTICE_FIELDS["building"]] = self._normalize_buildings_multi(
                payload.buildings
            )
        fields[OVERHAUL_NOTICE_FIELDS["status"]] = status or STATUS_START
        if title:
            fields[OVERHAUL_NOTICE_FIELDS["title"]] = title

        specialty = self._extract_section(payload.text, "专业")
        if specialty:
            fields[OVERHAUL_NOTICE_FIELDS["specialty"]] = specialty

        location = self._extract_section(payload.text, "地点")
        if location:
            fields[OVERHAUL_NOTICE_FIELDS["location"]] = location

        level = self._match_level(self._extract_section(payload.text, "紧急程度"))
        if level:
            fields[OVERHAUL_NOTICE_FIELDS["urgency"]] = level

        repair_device = self._extract_section(payload.text, "维修设备")
        if repair_device:
            fields[OVERHAUL_NOTICE_FIELDS["repair_device"]] = repair_device

        repair_fault = self._extract_section(payload.text, "维修故障")
        if repair_fault:
            fields[OVERHAUL_NOTICE_FIELDS["repair_fault"]] = repair_fault

        fault_type = self._extract_section(payload.text, "故障类型")
        if fault_type:
            fields[OVERHAUL_NOTICE_FIELDS["fault_type"]] = fault_type

        repair_mode = self._extract_section(payload.text, "维修方式")
        if repair_mode:
            fields[OVERHAUL_NOTICE_FIELDS["repair_mode"]] = repair_mode

        impact = self._extract_section(payload.text, "影响范围")
        if impact:
            fields[OVERHAUL_NOTICE_FIELDS["impact"]] = impact

        discovery = self._match_option(
            self._extract_section(payload.text, "故障发现方式"),
            OVERHAUL_DISCOVERY_OPTIONS,
        )
        if discovery:
            fields[OVERHAUL_NOTICE_FIELDS["discovery"]] = discovery

        symptom = self._extract_section(payload.text, "故障现象")
        if symptom:
            fields[OVERHAUL_NOTICE_FIELDS["symptom"]] = symptom

        reason = self._extract_section(payload.text, "故障原因")
        if reason:
            fields[OVERHAUL_NOTICE_FIELDS["reason"]] = reason

        solution = self._extract_section(payload.text, "解决方案")
        if solution:
            fields[OVERHAUL_NOTICE_FIELDS["solution"]] = solution

        progress = self._extract_section(payload.text, "完成情况")
        if progress:
            fields[OVERHAUL_NOTICE_FIELDS["progress"]] = progress

        fault_time = self._parse_datetime(self._extract_section(payload.text, "发现故障时间"))
        if fault_time:
            fields[OVERHAUL_NOTICE_FIELDS["fault_time"]] = self._to_timestamp_ms(
                fault_time
            )

        expected_time = self._parse_datetime(self._extract_section(payload.text, "期望完成时间"))
        if expected_time:
            fields[OVERHAUL_NOTICE_FIELDS["expected_time"]] = self._to_timestamp_ms(
                expected_time
            )

        if payload.response_time:
            actual_start = self._apply_time_with_date(payload.response_time, expected_time)
            if actual_start:
                fields[OVERHAUL_NOTICE_FIELDS["actual_start"]] = self._to_timestamp_ms(
                    actual_start
                )

        if status == STATUS_END and payload.response_time:
            actual_end = self._apply_time_with_date(payload.response_time, expected_time)
            if actual_end:
                fields[OVERHAUL_NOTICE_FIELDS["actual_end"]] = self._to_timestamp_ms(
                    actual_end
                )

        if payload.file_tokens:
            fields[OVERHAUL_NOTICE_FIELDS["notice_images"]] = [
                {"file_token": token} for token in payload.file_tokens
            ]
        if payload.extra_file_tokens:
            fields[OVERHAUL_NOTICE_FIELDS["site_images"]] = [
                {"file_token": token} for token in payload.extra_file_tokens
            ]

        return fields

    def build_update_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        status = self._normalize_status(info.get("status", ""))
        title = info.get("title", "") or self._extract_section(payload.text, "标题")

        if not status:
            status = STATUS_END if self.is_end_state(payload) else STATUS_UPDATE

        fields = {}
        if payload.buildings:
            fields[OVERHAUL_NOTICE_FIELDS["building"]] = self._normalize_buildings_multi(
                payload.buildings
            )
        if status:
            fields[OVERHAUL_NOTICE_FIELDS["status"]] = status
        if title:
            fields[OVERHAUL_NOTICE_FIELDS["title"]] = title

        specialty = self._extract_section(payload.text, "专业")
        if specialty:
            fields[OVERHAUL_NOTICE_FIELDS["specialty"]] = specialty

        location = self._extract_section(payload.text, "地点")
        if location:
            fields[OVERHAUL_NOTICE_FIELDS["location"]] = location

        level = self._match_level(self._extract_section(payload.text, "紧急程度"))
        if level:
            fields[OVERHAUL_NOTICE_FIELDS["urgency"]] = level

        repair_device = self._extract_section(payload.text, "维修设备")
        if not repair_device:
            repair_device = self._extract_section(payload.text, "维修故障")
        if repair_device:
            fields[OVERHAUL_NOTICE_FIELDS["repair_device"]] = repair_device

        fault_type = self._extract_section(payload.text, "故障类型")
        if fault_type:
            fields[OVERHAUL_NOTICE_FIELDS["fault_type"]] = fault_type

        repair_mode = self._extract_section(payload.text, "维修方式")
        if repair_mode:
            fields[OVERHAUL_NOTICE_FIELDS["repair_mode"]] = repair_mode

        impact = self._extract_section(payload.text, "影响范围")
        if impact:
            fields[OVERHAUL_NOTICE_FIELDS["impact"]] = impact

        discovery = self._match_option(
            self._extract_section(payload.text, "故障发现方式"),
            OVERHAUL_DISCOVERY_OPTIONS,
        )
        if discovery:
            fields[OVERHAUL_NOTICE_FIELDS["discovery"]] = discovery

        symptom = self._extract_section(payload.text, "故障现象")
        if symptom:
            fields[OVERHAUL_NOTICE_FIELDS["symptom"]] = symptom

        reason = self._extract_section(payload.text, "故障原因")
        if reason:
            fields[OVERHAUL_NOTICE_FIELDS["reason"]] = reason

        solution = self._extract_section(payload.text, "解决方案")
        if solution:
            fields[OVERHAUL_NOTICE_FIELDS["solution"]] = solution

        progress = self._extract_section(payload.text, "完成情况")
        if progress:
            fields[OVERHAUL_NOTICE_FIELDS["progress"]] = progress

        fault_time = self._parse_datetime(self._extract_section(payload.text, "发现故障时间"))
        if fault_time:
            fields[OVERHAUL_NOTICE_FIELDS["fault_time"]] = self._to_timestamp_ms(
                fault_time
            )

        expected_time = self._parse_datetime(self._extract_section(payload.text, "期望完成时间"))
        if expected_time:
            fields[OVERHAUL_NOTICE_FIELDS["expected_time"]] = self._to_timestamp_ms(
                expected_time
            )

        if status == STATUS_END and payload.response_time:
            actual_end = self._apply_time_with_date(payload.response_time, expected_time)
            if actual_end:
                fields[OVERHAUL_NOTICE_FIELDS["actual_end"]] = self._to_timestamp_ms(
                    actual_end
                )

        notice_tokens = self.merge_tokens(
            payload.existing_file_tokens, payload.file_tokens
        )
        if notice_tokens:
            fields[OVERHAUL_NOTICE_FIELDS["notice_images"]] = [
                {"file_token": token} for token in notice_tokens
            ]

        extra_tokens = self.merge_tokens(
            payload.existing_extra_file_tokens, payload.extra_file_tokens
        )
        if extra_tokens:
            fields[OVERHAUL_NOTICE_FIELDS["site_images"]] = [
                {"file_token": token} for token in extra_tokens
            ]

        return fields

    def _normalize_status(self, status: str) -> str:
        if status == STATUS_NEW:
            return STATUS_START
        return status

    def _normalize_buildings_multi(self, buildings):
        if not buildings:
            return None
        if isinstance(buildings, (list, tuple)):
            return list(buildings)
        return [buildings]

    def _match_level(self, value: str) -> str:
        if not value:
            return ""
        for option in OVERHAUL_URGENCY_OPTIONS:
            if option == OPTION_SLASH:
                continue
            if option in value:
                return option
        return ""

    def _match_option(self, value: str, options):
        if not value:
            return ""
        value_upper = value.upper()
        for option in options:
            if option == OPTION_SLASH:
                continue
            if option.upper() in value_upper:
                return option
        return ""

    def _extract_section(self, text: str, label: str) -> str:
        pattern = re.compile(rf"【{re.escape(label)}】(.*?)(?=【|$)", re.DOTALL)
        match = pattern.search(text or "")
        if not match:
            return ""
        return match.group(1).strip().rstrip("；")

    def _parse_datetime(self, text: str):
        if not text:
            return None
        return parse_single_datetime(text)

    def _apply_time_with_date(self, response_time: str, base_dt: datetime | None):
        if not response_time:
            return None
        parsed_dt = parse_single_datetime(response_time)
        if parsed_dt:
            return parsed_dt
        if not base_dt:
            return None
        time_parts = parse_time_only(response_time)
        if not time_parts:
            return None
        hour, minute, sec = time_parts
        return datetime(base_dt.year, base_dt.month, base_dt.day, hour, minute, sec)

    def _to_timestamp_ms(self, dt: datetime) -> int:
        return int(dt.timestamp() * 1000)
