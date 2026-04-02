# -*- coding: utf-8 -*-
from dataclasses import dataclass
from datetime import datetime
import re
from typing import List, Optional, Sequence, Union

from ...core.parser import extract_event_info
from ...config import STATUS_END
from ...time_parser import parse_single_datetime, parse_time_only
from ..robot_webhook import send_robot_title_and_content


@dataclass
class NoticePayload:
    """封装通告处理所需字段，便于在各处理器之间传递。"""

    text: str
    level: Optional[str] = None
    buildings: Optional[List[str]] = None
    specialty: Optional[str] = None
    event_source: Optional[str] = None
    file_tokens: Optional[List[str]] = None
    extra_file_tokens: Optional[List[str]] = None
    response_time: Optional[str] = None
    occurrence_date: Optional[str] = None
    existing_file_tokens: Optional[List[str]] = None
    existing_extra_file_tokens: Optional[List[str]] = None
    existing_response_time: Optional[Union[str, List[dict], dict]] = None
    transfer_to_overhaul: Optional[bool] = None
    recover: Optional[bool] = None


class BaseNoticeHandler:
    """各通告类型处理器的基类，负责构建创建/更新所需字段。"""

    # 子类通过 notice_types 声明可处理的通告类型
    notice_types: Sequence[str] = tuple()
    # 对应 config 中的表格 ID 属性名
    table_id_attr: str = ""

    def __init__(self, notice_type: Optional[str] = None):
        self.notice_type = notice_type or (
            self.notice_types[0] if self.notice_types else ""
        )

    @classmethod
    def matches(cls, notice_type: str) -> bool:
        return notice_type in cls.notice_types

    def get_table_id(self, cfg) -> str:
        if self.table_id_attr and hasattr(cfg, self.table_id_attr):
            return getattr(cfg, self.table_id_attr) or ""
        return cfg.get_table_id(self.notice_type)

    def build_create_fields(self, payload: NoticePayload) -> dict:
        raise NotImplementedError

    def build_update_fields(self, payload: NoticePayload) -> dict:
        raise NotImplementedError

    def is_end_state(self, payload: NoticePayload) -> bool:
        text = payload.text or ""
        return STATUS_END in text[:20] or STATUS_END in text[-20:]

    def merge_tokens(
        self,
        existing_tokens: Optional[List[str]],
        new_tokens: Optional[List[str]],
    ) -> List[str]:
        tokens: List[str] = []
        if existing_tokens:
            tokens.extend(existing_tokens)
        if new_tokens:
            tokens.extend(new_tokens)
        return tokens

    def send_group_robot_message(self, title: str, content: str, notice_type: str, level: str):
        return send_robot_title_and_content(title, content, notice_type, level)

    def build_robot_message(self, payload: NoticePayload):
        info = extract_event_info(payload.text or "")
        status = info.get("status", "") if info else ""
        notice_type = info.get("notice_type", self.notice_type) if info else self.notice_type
        level = self._detect_i_level(payload.text or "")
        payload_level = (payload.level or "").upper()
        if re.fullmatch(r"I[123]", payload_level):
            level = payload_level

        buildings = payload.buildings or []
        if isinstance(buildings, (list, tuple)):
            building_text = "、".join([str(item) for item in buildings if item])
        else:
            building_text = str(buildings) if buildings else ""

        title_parts = [part for part in [building_text, status] if part]
        title = " ".join(title_parts) if title_parts else ""
        content = (payload.text or "").strip()
        return title, content, notice_type, level

    def _detect_i_level(self, text: str) -> str:
        match = re.search(r"I[23]", text, re.IGNORECASE)
        if not match:
            return ""
        return match.group(0).upper()

    def _strip_status_line(self, text: str) -> str:
        lines = text.splitlines()
        cleaned = []
        removed = False
        status_pattern = re.compile(r"状态\s*[:：]")
        for line in lines:
            if not removed and status_pattern.search(line):
                removed = True
                continue
            cleaned.append(line)
        return "\n".join(cleaned).strip()

    def normalize_buildings(self, buildings: Optional[List[str]], allow_multi: bool = False):
        """
        楼栋字段兼容单选/多选：
        - 默认返回单个字符串（即使传入多个值也取第一个）
        - allow_multi=True 时：1 个值用字符串，多值用列表
        """
        if not buildings:
            return None
        if not allow_multi:
            return buildings[0]
        if len(buildings) == 1:
            return buildings[0]
        return buildings

    def _normalize_response_text(self, response_time: Union[str, List[dict], dict, None]) -> str:
        if isinstance(response_time, list) and response_time:
            # 飞书多行文本可能返回多个片段，拼接避免丢失已有记录
            parts = []
            for seg in response_time:
                if isinstance(seg, dict):
                    parts.append(seg.get("text", ""))
                else:
                    parts.append(str(seg))
            return "".join(parts)
        if isinstance(response_time, dict):
            return response_time.get("text", "")
        if isinstance(response_time, str):
            return response_time
        return ""

    def _parse_response_datetime(
        self, response_time: str, base_dt: datetime | None = None
    ) -> datetime | None:
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
