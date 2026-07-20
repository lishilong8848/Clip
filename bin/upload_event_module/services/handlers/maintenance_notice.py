import re
from datetime import datetime

from ...building_normalizer import normalize_building_name
from ...core.parser import extract_event_info
from ...config import (
    BUILDING_110,
    BUILDING_A,
    BUILDING_B,
    BUILDING_C,
    BUILDING_D,
    BUILDING_E,
    BUILDING_H,
    BUILDING_PARK,
    MAINTENANCE_NOTICE_FIELDS,
    OPTION_SLASH,
    SPECIALTY_OTHER,
    SPECIALTY_OPTIONS,
    STATUS_END,
)
from ...time_parser import parse_time_range, parse_single_datetime, parse_time_only
from .base import BaseNoticeHandler, NoticePayload


class MaintenanceNoticeHandler(BaseNoticeHandler):
    """维保通告处理"""

    notice_types = ("维保通告",)
    table_id_attr = "table_id_weibao"

    def build_create_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        status = info.get("status", "")
        title = info.get("title", "")
        time_str = info.get("time_str", "")

        start_dt, end_dt = self._parse_time_range(time_str)

        fields = {}
        if status:
            fields[MAINTENANCE_NOTICE_FIELDS["status"]] = status

        if title:
            fields[MAINTENANCE_NOTICE_FIELDS["name"]] = title

        specialty = self._normalize_specialty_single(payload.specialty)
        if specialty:
            fields[MAINTENANCE_NOTICE_FIELDS["specialty"]] = specialty
        buildings = self._normalize_buildings_multi(payload.buildings)
        if buildings:
            fields[MAINTENANCE_NOTICE_FIELDS["building"]] = buildings
        if payload.maintenance_cycle:
            fields[MAINTENANCE_NOTICE_FIELDS["maintenance_cycle"]] = payload.maintenance_cycle

        content = self._extract_section(payload.text, "内容")
        if content:
            fields[MAINTENANCE_NOTICE_FIELDS["content"]] = content

        impact = self._extract_section(payload.text, "影响")
        if impact:
            fields[MAINTENANCE_NOTICE_FIELDS["impact"]] = impact

        progress = self._extract_section(payload.text, "进度")
        if progress:
            fields[MAINTENANCE_NOTICE_FIELDS["progress"]] = progress

        reason = self._extract_section(payload.text, "原因")
        if reason:
            fields[MAINTENANCE_NOTICE_FIELDS["reason"]] = reason

        location = self._extract_section(payload.text, "位置")
        if location:
            fields[MAINTENANCE_NOTICE_FIELDS["location"]] = location

        if end_dt:
            fields[MAINTENANCE_NOTICE_FIELDS["plan_end"]] = self._to_timestamp_ms(
                end_dt
            )
        if start_dt:
            fields[MAINTENANCE_NOTICE_FIELDS["plan_start"]] = self._to_timestamp_ms(
                start_dt
            )
        actual_start_dt = self._parse_response_datetime(payload.response_time, start_dt)
        if actual_start_dt:
            fields[MAINTENANCE_NOTICE_FIELDS["actual_start"]] = self._to_timestamp_ms(
                actual_start_dt
            )

        if payload.file_tokens:
            fields[MAINTENANCE_NOTICE_FIELDS["notice_images"]] = [
                {"file_token": token} for token in payload.file_tokens
            ]
        if payload.extra_file_tokens:
            fields[MAINTENANCE_NOTICE_FIELDS["site_images"]] = [
                {"file_token": token} for token in payload.extra_file_tokens
            ]

        return fields

    def build_update_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        status = info.get("status", "")
        title = info.get("title", "")
        time_str = info.get("time_str", "")
        start_dt, end_dt = self._parse_time_range(time_str)

        fields = {}
        if status:
            fields[MAINTENANCE_NOTICE_FIELDS["status"]] = status

        if title:
            fields[MAINTENANCE_NOTICE_FIELDS["name"]] = title

        specialty = self._normalize_specialty_single(payload.specialty)
        if specialty:
            fields[MAINTENANCE_NOTICE_FIELDS["specialty"]] = specialty
        buildings = self._normalize_buildings_multi(payload.buildings)
        if buildings:
            fields[MAINTENANCE_NOTICE_FIELDS["building"]] = buildings
        if payload.maintenance_cycle:
            fields[MAINTENANCE_NOTICE_FIELDS["maintenance_cycle"]] = payload.maintenance_cycle

        content = self._extract_section(payload.text, "内容")
        if content:
            fields[MAINTENANCE_NOTICE_FIELDS["content"]] = content

        impact = self._extract_section(payload.text, "影响")
        if impact:
            fields[MAINTENANCE_NOTICE_FIELDS["impact"]] = impact

        progress = self._extract_section(payload.text, "进度")
        if progress:
            fields[MAINTENANCE_NOTICE_FIELDS["progress"]] = progress

        reason = self._extract_section(payload.text, "原因")
        if reason:
            fields[MAINTENANCE_NOTICE_FIELDS["reason"]] = reason

        location = self._extract_section(payload.text, "位置")
        if location:
            fields[MAINTENANCE_NOTICE_FIELDS["location"]] = location

        if end_dt:
            fields[MAINTENANCE_NOTICE_FIELDS["plan_end"]] = self._to_timestamp_ms(
                end_dt
            )
        if start_dt:
            fields[MAINTENANCE_NOTICE_FIELDS["plan_start"]] = self._to_timestamp_ms(
                start_dt
            )

        if status == STATUS_END and payload.response_time:
            end_time_dt = self._apply_end_time(payload.response_time, end_dt or start_dt)
            if end_time_dt:
                fields[MAINTENANCE_NOTICE_FIELDS["actual_end"]] = self._to_timestamp_ms(
                    end_time_dt
                )

        notice_tokens = self.merge_tokens(
            payload.existing_file_tokens, payload.file_tokens
        )
        if notice_tokens:
            fields[MAINTENANCE_NOTICE_FIELDS["notice_images"]] = [
                {"file_token": token} for token in notice_tokens
            ]

        extra_tokens = self.merge_tokens(
            payload.existing_extra_file_tokens, payload.extra_file_tokens
        )
        if extra_tokens:
            fields[MAINTENANCE_NOTICE_FIELDS["site_images"]] = [
                {"file_token": token} for token in extra_tokens
            ]

        return fields

    def _extract_section(self, text: str, label: str) -> str:
        pattern = re.compile(rf"【{label}】(.*?)(?=【|$)", re.DOTALL)
        match = pattern.search(text or "")
        if not match:
            return ""
        return match.group(1).strip()

    def _parse_time_range(self, text: str):
        return parse_time_range(text)

    def _normalize_buildings_multi(self, buildings):
        if not buildings:
            return []
        raw_values = (
            list(buildings)
            if isinstance(buildings, (list, tuple, set))
            else [buildings]
        )
        target_order = [
            BUILDING_A,
            BUILDING_B,
            BUILDING_C,
            BUILDING_D,
            BUILDING_E,
            BUILDING_H,
            BUILDING_110,
        ]
        selected: list[str] = []

        def append(value: str) -> None:
            if value in target_order and value not in selected:
                selected.append(value)

        for raw_value in raw_values:
            text = str(raw_value or "").strip()
            if not text:
                continue
            normalized = normalize_building_name(text)
            if normalized in {BUILDING_PARK, "园区", "CAMPUS"}:
                for value in (
                    BUILDING_A,
                    BUILDING_B,
                    BUILDING_C,
                    BUILDING_D,
                    BUILDING_E,
                ):
                    append(value)
                continue
            if normalized in target_order:
                append(normalized)
                continue

            upper_text = text.upper()
            if upper_text in {"A", "B", "C", "D", "E", "H"}:
                append(f"{upper_text}楼")
                continue
            if upper_text == "110":
                append(BUILDING_110)
                continue
            if "110" in upper_text:
                append(BUILDING_110)
            for match in re.finditer(r"(?<![A-Z0-9])([ABCDEH])\s*(?:楼|栋)", upper_text):
                append(f"{match.group(1)}楼")

        return [value for value in target_order if value in selected]

    @staticmethod
    def _normalize_specialty_single(specialty) -> str:
        text = str(specialty or "").strip()
        if not text:
            return ""
        writable_options = [
            option
            for option in SPECIALTY_OPTIONS
            if option and option != OPTION_SLASH
        ]
        if text in writable_options:
            return text
        compact = re.sub(r"\s+|专业", "", text)
        for option in writable_options:
            if compact == re.sub(r"\s+|专业", "", option):
                return option
        return SPECIALTY_OTHER

    def _to_timestamp_ms(self, dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    def _apply_end_time(self, response_time: str, base_dt: datetime):
        if not response_time:
            return None
        parsed_dt = parse_single_datetime(response_time)
        if parsed_dt:
            return parsed_dt
        time_parts = parse_time_only(response_time)
        if not time_parts:
            return None
        hour, minute, sec = time_parts

        if base_dt:
            return datetime(
                base_dt.year,
                base_dt.month,
                base_dt.day,
                hour,
                minute,
                sec,
            )

        now = datetime.now()
        return datetime(now.year, now.month, now.day, hour, minute, sec)

