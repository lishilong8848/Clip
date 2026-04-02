import time
from pathlib import Path
from typing import Any

from ..logger import log_info
from .state_store import build_state_payload, get_user_data_dir, read_state_safe, write_state_atomic


class RestartGuard:
    def __init__(
        self,
        window_seconds: int,
        max_restarts: int,
        state_path: Path | None = None,
    ) -> None:
        self.window_seconds = window_seconds
        self.max_restarts = max_restarts
        self.state_path = state_path or (get_user_data_dir() / "restart_guard.json")

    def _load(self) -> list[float]:
        payload = read_state_safe(self.state_path)
        if not payload:
            return []
        timestamps = payload.get("data", {}).get("timestamps", [])
        if not isinstance(timestamps, list):
            return []
        return [float(x) for x in timestamps]

    def _save(self, timestamps: list[float]) -> None:
        payload = build_state_payload({"timestamps": timestamps})
        write_state_atomic(self.state_path, payload)

    def record_restart(self) -> int:
        now = time.time()
        timestamps = [t for t in self._load() if now - t <= self.window_seconds]
        timestamps.append(now)
        self._save(timestamps)
        return len(timestamps)

    def should_enter_safe_mode(self) -> bool:
        now = time.time()
        timestamps = [t for t in self._load() if now - t <= self.window_seconds]
        if len(timestamps) > self.max_restarts:
            log_info(
                f"RestartGuard: {len(timestamps)} 次重启在 {self.window_seconds}s 内，进入安全模式"
            )
            return True
        return False

    def clear(self) -> None:
        self._save([])
        log_info("RestartGuard: 已清空重启计数")
