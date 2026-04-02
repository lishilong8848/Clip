from .base import BaseNoticeHandler, NoticePayload
from .event_notice import EventNoticeHandler
from .change_notice import ChangeNoticeHandler
from .maintenance_notice import MaintenanceNoticeHandler
from .power_notice import PowerNoticeHandler
from .polling_notice import PollingNoticeHandler
from .overhaul_notice import OverhaulNoticeHandler
from .device_adjust_notice import AdjustNoticeHandler
from .generic_notice import GenericNoticeHandler

__all__ = [
    "NoticePayload",
    "BaseNoticeHandler",
    "get_notice_handler",
]


HANDLER_CLASSES = [
    EventNoticeHandler,
    ChangeNoticeHandler,
    MaintenanceNoticeHandler,
    PowerNoticeHandler,
    PollingNoticeHandler,
    OverhaulNoticeHandler,
    AdjustNoticeHandler,
]


def get_notice_handler(notice_type: str) -> BaseNoticeHandler:
    for handler_cls in HANDLER_CLASSES:
        if handler_cls.matches(notice_type):
            return handler_cls(notice_type)
    # 默认使用变更类处理器逻辑，便于在未声明类型时仍提供基本能力
    return GenericNoticeHandler(notice_type)
