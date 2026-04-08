import re
from datetime import datetime, timedelta

from ...core.parser import extract_event_info
from ...config import (
    EVENT_NOTICE_FIELDS,
    EVENT_LEVEL_OPTIONS,
    EVENT_LEVEL_UPGRADE_I3_TO_I2,
    EVENT_LEVEL_UPGRADE_I3_TO_I1,
    EVENT_SOURCE_OPTIONS,
    EVENT_SOURCE_BA,
    EVENT_SOURCE_BMS,
    EVENT_SOURCE_PPM,
    EVENT_SOURCE_FIRE,
    EVENT_SOURCE_CHANGE,
    EVENT_SOURCE_PATROL,
    EVENT_SOURCE_CUSTOMER,
    EVENT_SOURCE_CCTV,
    EVENT_SOURCE_ACCESS,
    EVENT_SOURCE_DINGPING,
    OPTION_SLASH,
    LEVEL_I3,
    LEVEL_I2,
    LEVEL_I1,
    LEVEL_E4,
    LEVEL_E3,
    LEVEL_E2,
    LEVEL_E1,
    LEVEL_E0,
)
from ...time_parser import parse_single_datetime, parse_time_only
from .base import BaseNoticeHandler, NoticePayload


class EventNoticeHandler(BaseNoticeHandler):
    """事件通告处理"""

    notice_types = ("事件通告",)
    table_id_attr = "table_id_shijian"

    EVENT_LEVEL_OPTIONS = set(EVENT_LEVEL_OPTIONS)

    def build_create_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        time_str = payload.occurrence_date or info.get("time_str", "")
        occurrence_dt = self._parse_event_datetime(time_str)
        response_dt = self._parse_response_datetime(payload.response_time, occurrence_dt)

        fields = {}

        summary = self._extract_section(payload.text, "概述")
        if summary:
            fields[EVENT_NOTICE_FIELDS["alarm_desc"]] = summary

        level = self._normalize_event_level(payload.level, payload.text)
        if summary and "负载功率过高" in summary:
            level = LEVEL_I3
        if level:
            fields[EVENT_NOTICE_FIELDS["level"]] = level

        if payload.buildings:
            fields[EVENT_NOTICE_FIELDS["building"]] = self.normalize_buildings(
                payload.buildings
            )

        if payload.specialty:
            fields[EVENT_NOTICE_FIELDS["specialty"]] = payload.specialty

        source = payload.event_source or self._detect_event_source(payload.text)
        if source:
            fields[EVENT_NOTICE_FIELDS["source"]] = source

        if payload.transfer_to_overhaul is not None:
            fields[EVENT_NOTICE_FIELDS["transfer_to_overhaul"]] = bool(
                payload.transfer_to_overhaul
            )

        if occurrence_dt:
            fields[EVENT_NOTICE_FIELDS["occurrence_time"]] = self._to_timestamp_ms(
                occurrence_dt
            )

        if response_dt:
            fields[EVENT_NOTICE_FIELDS["response_time"]] = self._to_timestamp_ms(
                response_dt
            )

        if payload.file_tokens:
            fields[EVENT_NOTICE_FIELDS["response_snapshot"]] = [
                {"file_token": token} for token in payload.file_tokens
            ]

        if payload.recover and response_dt:
            fields[EVENT_NOTICE_FIELDS["recover_time"]] = self._to_timestamp_ms(
                response_dt
            )
            if payload.file_tokens:
                fields[EVENT_NOTICE_FIELDS["recover_snapshot"]] = [
                    {"file_token": token} for token in payload.file_tokens
                ]

        return fields

    def build_update_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        time_str = payload.occurrence_date or info.get("time_str", "")
        occurrence_dt = self._parse_event_datetime(time_str)
        response_dt = self._parse_response_datetime(payload.response_time, occurrence_dt)

        fields = {}

        summary = self._extract_section(payload.text, "概述")
        if summary:
            fields[EVENT_NOTICE_FIELDS["alarm_desc"]] = summary

        level = self._normalize_event_level(payload.level, payload.text)
        if summary and "负载功率过高" in summary:
            level = LEVEL_I3
        if level:
            fields[EVENT_NOTICE_FIELDS["level"]] = level

        if payload.buildings:
            fields[EVENT_NOTICE_FIELDS["building"]] = self.normalize_buildings(
                payload.buildings
            )

        if payload.specialty:
            fields[EVENT_NOTICE_FIELDS["specialty"]] = payload.specialty

        source = payload.event_source or self._detect_event_source(payload.text)
        if source:
            fields[EVENT_NOTICE_FIELDS["source"]] = source

        if payload.transfer_to_overhaul is not None:
            fields[EVENT_NOTICE_FIELDS["transfer_to_overhaul"]] = bool(
                payload.transfer_to_overhaul
            )

        if occurrence_dt:
            fields[EVENT_NOTICE_FIELDS["occurrence_time"]] = self._to_timestamp_ms(
                occurrence_dt
            )

        is_end = self.is_end_state(payload)
        if is_end:
            if response_dt:
                fields[EVENT_NOTICE_FIELDS["end_time"]] = self._to_timestamp_ms(
                    response_dt
                )
            if payload.file_tokens:
                fields[EVENT_NOTICE_FIELDS["end_snapshot"]] = [
                    {"file_token": token} for token in payload.file_tokens
                ]
            return fields

        if response_dt:
            fields[EVENT_NOTICE_FIELDS["response_time"]] = self._to_timestamp_ms(
                response_dt
            )

            if not payload.recover:
                existing_text = self._normalize_response_text(
                    payload.existing_response_time
                )
                fields[EVENT_NOTICE_FIELDS["progress_update"]] = (
                    self._append_progress_update(existing_text, response_dt)
                )

        if not payload.recover:
            merged_tokens = self.merge_tokens(
                payload.existing_file_tokens, payload.file_tokens
            )
            if merged_tokens:
                fields[EVENT_NOTICE_FIELDS["progress_snapshot"]] = [
                    {"file_token": token} for token in merged_tokens
                ]

        if payload.recover and response_dt:
            fields[EVENT_NOTICE_FIELDS["recover_time"]] = self._to_timestamp_ms(
                response_dt
            )
            recover_tokens = self.merge_tokens(
                payload.existing_extra_file_tokens, payload.file_tokens
            )
            if recover_tokens:
                fields[EVENT_NOTICE_FIELDS["recover_snapshot"]] = [
                    {"file_token": token} for token in recover_tokens
                ]

        return fields

    def build_robot_message(self, payload: NoticePayload):
        title, content, notice_type, level = super().build_robot_message(payload)
        summary = self._extract_section(payload.text or "", "概述")
        payload_level = str(payload.level or "").strip()
        normalized_level = self._normalize_event_level(payload.level, payload.text)
        if not payload_level and summary and "负载功率过高" in summary:
            normalized_level = LEVEL_I3
        return title, content, notice_type, self._route_event_group_level(normalized_level)

    def _extract_section(self, text: str, label: str) -> str:
        if not text:
            return ""
        pattern = re.compile(rf"【{label}】(.*?)(?=【|$)", re.DOTALL)
        match = pattern.search(text)
        if not match:
            return ""
        return match.group(1).strip()

    def _normalize_event_level(self, level: str, text: str) -> str:
        raw_level = (level or "").strip().upper()
        if raw_level:
            if re.search(rf"{LEVEL_I3}\s*[→>\\-]\s*{LEVEL_I2}", raw_level):
                return EVENT_LEVEL_UPGRADE_I3_TO_I2
            if re.search(rf"{LEVEL_I3}\s*[→>\\-]\s*{LEVEL_I1}", raw_level):
                return EVENT_LEVEL_UPGRADE_I3_TO_I1
            if raw_level in self.EVENT_LEVEL_OPTIONS:
                return raw_level

        return self._detect_event_level_from_text(text)

    def _route_event_group_level(self, level: str) -> str:
        normalized = str(level or "").strip()
        if normalized == LEVEL_I3:
            return LEVEL_I3
        return LEVEL_I2

    def _detect_event_level_from_text(self, text: str) -> str:
        title = self._extract_section(text or "", "标题") or (text or "")
        title_upper = title.upper()
        if re.search(rf"{LEVEL_I3}\s*[→>\\-]\s*{LEVEL_I2}", title_upper):
            return EVENT_LEVEL_UPGRADE_I3_TO_I2
        if re.search(rf"{LEVEL_I3}\s*[→>\\-]\s*{LEVEL_I1}", title_upper):
            return EVENT_LEVEL_UPGRADE_I3_TO_I1
        for option in [
            LEVEL_I3,
            LEVEL_I2,
            LEVEL_I1,
            LEVEL_E4,
            LEVEL_E3,
            LEVEL_E2,
            LEVEL_E1,
            LEVEL_E0,
        ]:
            if option == OPTION_SLASH:
                continue
            if option in title_upper:
                return option
        return ""

    def _detect_event_source(self, text: str) -> str:
        source_text = self._extract_section(text or "", "来源")
        if not source_text:
            return ""
        source_upper = source_text.upper()
        if "盯屏" in source_text:
            return EVENT_SOURCE_DINGPING
        if "BA" in source_upper:
            return EVENT_SOURCE_BA
        if "BMS" in source_upper:
            return EVENT_SOURCE_BMS
        if "维护" in source_text or "维保" in source_text:
            return EVENT_SOURCE_PPM
        if "消防" in source_text:
            return EVENT_SOURCE_FIRE
        if "变更" in source_text:
            return EVENT_SOURCE_CHANGE
        if "巡检" in source_text:
            return EVENT_SOURCE_PATROL
        if "客户" in source_text:
            return EVENT_SOURCE_CUSTOMER
        if "CCTV" in source_upper:
            return EVENT_SOURCE_CCTV
        if "门禁" in source_text:
            return EVENT_SOURCE_ACCESS

        for option in EVENT_SOURCE_OPTIONS:
            if option == OPTION_SLASH:
                continue
            if option in source_text:
                return option
        return ""

    def _parse_event_datetime(self, text: str):
        return parse_single_datetime(text)

    def _parse_response_datetime(self, response_time: str, occurrence_dt: datetime):
        if not response_time:
            return None
        parsed_dt = parse_single_datetime(response_time)
        if parsed_dt:
            return parsed_dt
        time_parts = parse_time_only(response_time)
        if not time_parts:
            return None
        hour, minute, sec = time_parts

        base_date = occurrence_dt.date() if occurrence_dt else datetime.now().date()
        if occurrence_dt and occurrence_dt.hour >= 23 and hour == 0:
            base_date = base_date + timedelta(days=1)

        try:
            return datetime(
                base_date.year,
                base_date.month,
                base_date.day,
                hour,
                minute,
                sec,
            )
        except ValueError:
            return None

    def _append_progress_update(self, existing_text: str, response_dt: datetime) -> str:
        if not response_dt:
            return existing_text or ""
        existing_text = (existing_text or "").strip()
        index = 1
        if existing_text:
            matches = re.findall(r"(\d+)、", existing_text)
            if matches:
                try:
                    index = max(int(i) for i in matches) + 1
                except ValueError:
                    index = 1

        entry = f"{index}、{response_dt.strftime('%Y/%m/%d %H:%M')}"
        if not existing_text:
            return entry
        return f"{existing_text}   {entry}"

    def _to_timestamp_ms(self, dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

