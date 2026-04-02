import re
from datetime import datetime

from ...core.parser import extract_event_info
from ...config import (
    POWER_NOTICE_FIELDS,
    POWER_DEFAULT_SPECIALTY,
    STATUS_NEW,
    STATUS_START,
    STATUS_UPDATE,
    STATUS_END,
)
from ...time_parser import parse_time_range, parse_single_datetime, parse_time_only
from .base import BaseNoticeHandler, NoticePayload


class PowerNoticeHandler(BaseNoticeHandler):
    """上下电通告处理"""

    notice_types = ("上下电通告", "上电通告", "下电通告")
    table_id_attr = "table_id_power"

    def build_create_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        status = self._normalize_status(info.get("status", ""))
        title = info.get("title", "") or self._extract_section(payload.text, "名称")
        time_str = info.get("time_str", "")
        start_dt, end_dt = self._parse_time_range(time_str)

        fields = {}
        if title:
            fields[POWER_NOTICE_FIELDS["title"]] = title
        if payload.buildings:
            fields[POWER_NOTICE_FIELDS["building"]] = self._normalize_buildings_multi(
                payload.buildings
            )
        fields[POWER_NOTICE_FIELDS["status"]] = status or STATUS_START
        fields[POWER_NOTICE_FIELDS["specialty"]] = (
            POWER_DEFAULT_SPECIALTY
        )  # TODO: 目前固定值，后续根据通告内容优化

        cabinet = self._extract_section(payload.text, "柜号")
        if cabinet:
            fields[POWER_NOTICE_FIELDS["cabinet"]] = cabinet

        quantity = self._extract_quantity(payload.text)
        if quantity is not None:
            fields[POWER_NOTICE_FIELDS["quantity"]] = quantity

        progress = self._extract_section(payload.text, "进度")
        if progress:
            fields[POWER_NOTICE_FIELDS["progress"]] = progress

        if start_dt:
            ts_start = self._to_timestamp_ms(start_dt)
            fields[POWER_NOTICE_FIELDS["plan_start"]] = ts_start
        actual_start_dt = self._parse_response_datetime(payload.response_time, start_dt)
        if actual_start_dt:
            fields[POWER_NOTICE_FIELDS["actual_start"]] = self._to_timestamp_ms(
                actual_start_dt
            )
        if end_dt:
            fields[POWER_NOTICE_FIELDS["plan_end"]] = self._to_timestamp_ms(end_dt)

        if payload.file_tokens:
            fields[POWER_NOTICE_FIELDS["notice_images"]] = [
                {"file_token": token} for token in payload.file_tokens
            ]

        return fields

    def build_update_fields(self, payload: NoticePayload) -> dict:
        info = extract_event_info(payload.text) or {}
        status = self._normalize_status(info.get("status", ""))
        title = info.get("title", "") or self._extract_section(payload.text, "名称")
        time_str = info.get("time_str", "")
        start_dt, end_dt = self._parse_time_range(time_str)

        if not status:
            status = STATUS_END if self.is_end_state(payload) else STATUS_UPDATE

        fields = {}
        if status:
            fields[POWER_NOTICE_FIELDS["status"]] = status
        if title:
            fields[POWER_NOTICE_FIELDS["title"]] = title
        if payload.buildings:
            fields[POWER_NOTICE_FIELDS["building"]] = self._normalize_buildings_multi(
                payload.buildings
            )

        cabinet = self._extract_section(payload.text, "柜号")
        if cabinet:
            fields[POWER_NOTICE_FIELDS["cabinet"]] = cabinet

        quantity = self._extract_quantity(payload.text)
        if quantity is not None:
            fields[POWER_NOTICE_FIELDS["quantity"]] = quantity

        progress = self._extract_section(payload.text, "进度")
        if progress:
            fields[POWER_NOTICE_FIELDS["progress"]] = progress

        if start_dt:
            ts_start = self._to_timestamp_ms(start_dt)
            fields[POWER_NOTICE_FIELDS["plan_start"]] = ts_start
        if end_dt:
            fields[POWER_NOTICE_FIELDS["plan_end"]] = self._to_timestamp_ms(end_dt)

        if status == STATUS_END and payload.response_time:
            end_time_dt = self._apply_end_time(payload.response_time, end_dt or start_dt)
            if end_time_dt:
                fields[POWER_NOTICE_FIELDS["actual_end"]] = self._to_timestamp_ms(
                    end_time_dt
                )

        notice_tokens = self.merge_tokens(
            payload.existing_file_tokens, payload.file_tokens
        )
        if notice_tokens:
            fields[POWER_NOTICE_FIELDS["notice_images"]] = [
                {"file_token": token} for token in notice_tokens
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

    def _extract_section(self, text: str, label: str) -> str:
        pattern = re.compile(rf"【{label}】(.*?)(?=【|$)", re.DOTALL)
        match = pattern.search(text or "")
        if not match:
            return ""
        return match.group(1).strip().rstrip("；;")

    def _extract_quantity(self, text: str):
        raw = self._extract_section(text, "数量")
        if not raw:
            return None
        match = re.search(r"(\d+(?:\.\d+)?)", raw)
        if not match:
            return None
        value = match.group(1)
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return None

    def _parse_time_range(self, text: str):
        return parse_time_range(text)

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

