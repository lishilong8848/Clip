import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Callable, Iterable

from ..logger import log_info


class Decision(str, Enum):
    IGNORE = "IGNORE"
    RELOAD = "RELOAD"
    RESTART = "RESTART"


@dataclass
class ChangeBatch:
    paths: set[Path]
    start_ts: float
    end_ts: float
    decision: Decision
    reason: str
    event_count: int
    filtered_count: int


class ChangeBatcher:
    def __init__(
        self,
        debounce_ms: int,
        classifier: Callable[[Iterable[Path]], tuple[Decision, str]],
        on_batch: Callable[[ChangeBatch], None],
    ) -> None:
        self.debounce_ms = debounce_ms
        self.classifier = classifier
        self.on_batch = on_batch
        self._lock = threading.Lock()
        self._timer: threading.Timer | None = None
        self._paths: set[Path] = set()
        self._start_ts = 0.0
        self._end_ts = 0.0
        self._event_count = 0
        self._filtered_count = 0
        self._accepting = True

    def resume(self) -> None:
        with self._lock:
            self._accepting = True

    def shutdown(self) -> None:
        with self._lock:
            self._accepting = False
            if self._timer:
                self._timer.cancel()
                self._timer = None
            self._paths.clear()
            self._event_count = 0
            self._filtered_count = 0

    def record_ignored(self) -> None:
        with self._lock:
            if not self._accepting:
                return
            self._filtered_count += 1

    def push(self, path: Path) -> None:
        now = time.time()
        with self._lock:
            if not self._accepting:
                return
            if not self._paths:
                self._start_ts = now
            self._end_ts = now
            self._paths.add(path)
            self._event_count += 1

            if self._timer:
                self._timer.cancel()
            self._timer = threading.Timer(self.debounce_ms / 1000.0, self._flush)
            self._timer.daemon = True
            self._timer.start()

    def _flush(self) -> None:
        with self._lock:
            if not self._accepting:
                self._timer = None
                return
            if not self._paths:
                return
            paths = set(self._paths)
            start_ts = self._start_ts
            end_ts = self._end_ts
            event_count = self._event_count
            filtered_count = self._filtered_count

            self._paths.clear()
            self._event_count = 0
            self._filtered_count = 0
            self._timer = None

        decision, reason = self.classifier(paths)
        batch = ChangeBatch(
            paths=paths,
            start_ts=start_ts,
            end_ts=end_ts,
            decision=decision,
            reason=reason,
            event_count=event_count,
            filtered_count=filtered_count,
        )
        if decision != Decision.IGNORE:
            log_info(
                "HotReload: batch ready | events=%s paths=%s decision=%s reason=%s filtered=%s"
                % (
                    event_count,
                    len(paths),
                    decision,
                    reason,
                    filtered_count,
                )
            )
        self.on_batch(batch)


def demo_batcher_coalesce() -> int:
    """演示：连续 push 10 次同文件，最终只产生 1 个 batch。"""
    results: list[ChangeBatch] = []

    def classifier(_: Iterable[Path]) -> tuple[Decision, str]:
        return Decision.RELOAD, "demo"

    def on_batch(batch: ChangeBatch) -> None:
        results.append(batch)

    batcher = ChangeBatcher(debounce_ms=200, classifier=classifier, on_batch=on_batch)
    for _ in range(10):
        batcher.push(Path("demo.py"))
    time.sleep(0.4)
    return len(results)
