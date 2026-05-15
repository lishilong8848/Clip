import re
from datetime import datetime

from ...core.parser import extract_event_info
from ...config import MAINTENANCE_NOTICE_FIELDS, STATUS_END
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

        if payload.specialty:
            fields[MAINTENANCE_NOTICE_FIELDS["specialty"]] = payload.specialty
        if payload.buildings:
            fields[MAINTENANCE_NOTICE_FIELDS["building"]] = self._normalize_buildings_multi(
                payload.buildings
            )
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

        if payload.specialty:
            fields[MAINTENANCE_NOTICE_FIELDS["specialty"]] = payload.specialty
        if payload.buildings:
            fields[MAINTENANCE_NOTICE_FIELDS["building"]] = self._normalize_buildings_multi(
                payload.buildings
            )
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
            return None
        if isinstance(buildings, (list, tuple, set)):
            return [item for item in buildings if item]
        return [buildings]

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

