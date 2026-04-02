from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from ..logger import log_error, log_info


@dataclass
class ConnectionItem:
    sender_name: str
    signal_name: str
    slot: Any
    connection_type: Any | None


class ConnectionRegistry:
    def __init__(self) -> None:
        self._items: list[ConnectionItem] = []

    def connect(
        self,
        sender_name: str,
        sender: Any,
        signal_name: str,
        slot: Any,
        connection_type: Any | None = None,
    ) -> None:
        signal = getattr(sender, signal_name)
        if connection_type is None:
            signal.connect(slot)
        else:
            signal.connect(slot, connection_type)
        self._items.append(
            ConnectionItem(sender_name, signal_name, slot, connection_type)
        )

    def disconnect_all(self, sender_mapping: dict[str, Any] | None = None) -> None:
        for item in self._items:
            sender = None
            if sender_mapping:
                sender = sender_mapping.get(item.sender_name)
            if sender is None:
                continue
            try:
                signal = getattr(sender, item.signal_name)
                signal.disconnect(item.slot)
            except Exception as exc:
                log_error(
                    f"ConnectionRegistry: 断开失败 {item.sender_name}.{item.signal_name}: {exc}"
                )

    def reconnect_all(self, sender_mapping: dict[str, Any]) -> None:
        for item in self._items:
            sender = sender_mapping.get(item.sender_name)
            if sender is None:
                continue
            try:
                signal = getattr(sender, item.signal_name)
                if item.connection_type is None:
                    signal.connect(item.slot)
                else:
                    signal.connect(item.slot, item.connection_type)
            except Exception as exc:
                log_error(
                    f"ConnectionRegistry: 重连失败 {item.sender_name}.{item.signal_name}: {exc}"
                )
        log_info("ConnectionRegistry: 已重连所有信号")
