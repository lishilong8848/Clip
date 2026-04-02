import re
from datetime import datetime, timedelta

from ...core.parser import extract_event_info
from ...config import (
    CHANGE_NOTICE_FIELDS,
    CHANGE_ALI_LEVEL_OPTIONS,
    CHANGE_ZHIHANG_LEVEL_OPTIONS,
    ALI_LEVEL_ULTRA_LOW,
    ALI_LEVEL_LOW,
    ALI_LEVEL_MEDIUM,
    ALI_LEVEL_HIGH,
    LEVEL_I1,
    LEVEL_I2,
    LEVEL_I3,
    LEVEL_E0,
    OPTION_SLASH,
    STATUS_NEW,
    STATUS_START,
    STATUS_UPDATE,
    STATUS_END,
)
from ...time_parser import parse_time_range, parse_single_datetime, parse_time_only
from .base import BaseNoticeHandler, NoticePayload


class ChangeNoticeHandler(BaseNoticeHandler):
    """设备变更/变更通告处理"""

    notice_types = ("设备变更", "变更通告")
    table_id_attr = "table_id_biangeng"

    ALI_LEVEL_OPTIONS = tuple(CHANGE_ALI_LEVEL_OPTIONS)
    ZHIHANG_LEVEL_MAP = {
        ALI_LEVEL_ULTRA_LOW: LEVEL_I3,
        ALI_LEVEL_LOW: LEVEL_I3,
        ALI_LEVEL_MEDIUM: LEVEL_I2,
        ALI_LEVEL_HIGH: LEVEL_I1,
    }

    def _normalize_change_level(self, payload: NoticePayload) -> str:
        raw_level = (payload.level or "").upper()
        if raw_level in (LEVEL_I1, LEVEL_I2, LEVEL_I3, LEVEL_E0):
            return raw_level
        return LEVEL_I3

    def build_robot_message(self, payload: NoticePayload):
        title, content, notice_type, level = super().build_robot_message(payload)
        normalized_level = self._normalize_change_level(payload)
        level = LEVEL_I2 if normalized_level in (LEVEL_I1, LEVEL_E0) else normalized_level
        return title, content, notice_type, level

    def build_create_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        status = self._normalize_status(info.get("status", ""), is_end=False)
        title = info.get("title", "") or self._extract_section(payload.text, "名称")
        time_str = info.get("time_str", "")
        start_dt, end_dt = self._parse_time_range(time_str)

        fields = {}
        if status:
            fields[CHANGE_NOTICE_FIELDS["status"]] = status

        if title:
            fields[CHANGE_NOTICE_FIELDS["title"]] = title

        ali_level = self._detect_ali_level(payload.text, info.get("level", ""))
        if ali_level:
            fields[CHANGE_NOTICE_FIELDS["level_ali"]] = ali_level
        zhihang_level = None
        if payload.level == OPTION_SLASH:
            zhihang_level = OPTION_SLASH
        elif (payload.level or "").upper() in (LEVEL_I1, LEVEL_I2, LEVEL_I3, LEVEL_E0):
            zhihang_level = payload.level
        else:
            zhihang_level = self.ZHIHANG_LEVEL_MAP.get(ali_level or "")
        if zhihang_level:
            fields[CHANGE_NOTICE_FIELDS["level_zhihang"]] = zhihang_level

        if start_dt:
            fields[CHANGE_NOTICE_FIELDS["start_time"]] = self._to_timestamp_ms(start_dt)

        if payload.buildings:
            fields[CHANGE_NOTICE_FIELDS["building"]] = self._normalize_buildings_multi(
                payload.buildings
            )

        location = self._extract_section(payload.text, "位置")
        if location:
            fields[CHANGE_NOTICE_FIELDS["location"]] = location

        content = self._extract_section(payload.text, "内容")
        if content:
            fields[CHANGE_NOTICE_FIELDS["content"]] = content

        reason = self._extract_section(payload.text, "原因")
        if reason:
            fields[CHANGE_NOTICE_FIELDS["reason"]] = reason

        impact = self._extract_section(payload.text, "影响")
        if impact:
            fields[CHANGE_NOTICE_FIELDS["impact"]] = impact

        progress = self._extract_section(payload.text, "进度")
        if progress:
            fields[CHANGE_NOTICE_FIELDS["progress"]] = progress

        if payload.file_tokens:
            fields[CHANGE_NOTICE_FIELDS["start_snapshot"]] = [
                {"file_token": token} for token in payload.file_tokens
            ]

        return fields

    def build_update_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        is_end = self.is_end_state(payload)
        status = self._normalize_status(info.get("status", ""), is_end=is_end)
        title = info.get("title", "") or self._extract_section(payload.text, "名称")
        time_str = info.get("time_str", "")
        start_dt, end_dt = self._parse_time_range(time_str)

        fields = {}
        if status:
            fields[CHANGE_NOTICE_FIELDS["status"]] = status

        if title:
            fields[CHANGE_NOTICE_FIELDS["title"]] = title

        ali_level = self._detect_ali_level(payload.text, info.get("level", ""))
        if ali_level:
            fields[CHANGE_NOTICE_FIELDS["level_ali"]] = ali_level
        zhihang_level = None
        if payload.level == OPTION_SLASH:
            zhihang_level = OPTION_SLASH
        elif (payload.level or "").upper() in (LEVEL_I1, LEVEL_I2, LEVEL_I3, LEVEL_E0):
            zhihang_level = payload.level
        else:
            zhihang_level = self.ZHIHANG_LEVEL_MAP.get(ali_level or "")
        if zhihang_level:
            fields[CHANGE_NOTICE_FIELDS["level_zhihang"]] = zhihang_level

        if start_dt:
            fields[CHANGE_NOTICE_FIELDS["start_time"]] = self._to_timestamp_ms(start_dt)

        if payload.buildings:
            fields[CHANGE_NOTICE_FIELDS["building"]] = self._normalize_buildings_multi(
                payload.buildings
            )

        location = self._extract_section(payload.text, "位置")
        if location:
            fields[CHANGE_NOTICE_FIELDS["location"]] = location

        content = self._extract_section(payload.text, "内容")
        if content:
            fields[CHANGE_NOTICE_FIELDS["content"]] = content

        reason = self._extract_section(payload.text, "原因")
        if reason:
            fields[CHANGE_NOTICE_FIELDS["reason"]] = reason

        impact = self._extract_section(payload.text, "影响")
        if impact:
            fields[CHANGE_NOTICE_FIELDS["impact"]] = impact

        progress = self._extract_section(payload.text, "进度")
        if progress:
            fields[CHANGE_NOTICE_FIELDS["progress"]] = progress

        if is_end:
            response_dt = self._parse_response_datetime(
                payload.response_time, end_dt or start_dt
            )
            if response_dt:
                fields[CHANGE_NOTICE_FIELDS["end_time"]] = self._to_timestamp_ms(
                    response_dt
                )
            if payload.file_tokens:
                fields[CHANGE_NOTICE_FIELDS["end_snapshot"]] = [
                    {"file_token": token} for token in payload.file_tokens
                ]
            return fields

        response_dt = self._parse_response_datetime(payload.response_time, start_dt)
        if response_dt:
            existing_text = self._normalize_response_text(payload.existing_response_time)
            fields[CHANGE_NOTICE_FIELDS["update_time"]] = self._append_progress_update(
                existing_text, response_dt
            )

        merged_tokens = self.merge_tokens(
            payload.existing_file_tokens, payload.file_tokens
        )
        if merged_tokens:
            fields[CHANGE_NOTICE_FIELDS["update_snapshot"]] = [
                {"file_token": token} for token in merged_tokens
            ]

        return fields

    def _normalize_status(self, status: str, is_end: bool) -> str:
        if status == STATUS_NEW:
            return STATUS_START
        if status:
            return status
        return STATUS_END if is_end else STATUS_UPDATE

    def _detect_ali_level(self, text: str, fallback: str = "") -> str:
        source = (self._extract_section(text, "等级") or fallback or "").strip()
        if not source:
            return ""
        for option in self.ALI_LEVEL_OPTIONS:
            if option in source:
                return option
        return ""

    def _extract_section(self, text: str, label: str) -> str:
        pattern = re.compile(rf"【{label}】(.*?)(?=【|$)", re.DOTALL)
        match = pattern.search(text or "")
        if not match:
            return ""
        return match.group(1).strip().rstrip("；;")

    def _parse_time_range(self, text: str):
        return parse_time_range(text)

    def _normalize_buildings_multi(self, buildings):
        if not buildings:
            return None
        if isinstance(buildings, (list, tuple, set)):
            return [item for item in buildings if item]
        return [buildings]

    def _parse_response_datetime(self, response_time: str, base_dt: datetime):
        if not response_time:
            return None
        parsed_dt = parse_single_datetime(response_time)
        if parsed_dt:
            return parsed_dt
        time_parts = parse_time_only(response_time)
        if not time_parts:
            return None
        hour, minute, sec = time_parts

        base_date = base_dt.date() if base_dt else datetime.now().date()
        if base_dt and base_dt.hour >= 23 and hour == 0:
            base_date = base_date + timedelta(days=1)

        try:
            return datetime(
                base_date.year, base_date.month, base_date.day, hour, minute, sec
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

