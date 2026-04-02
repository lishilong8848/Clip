from dataclasses import dataclass
from .change_batcher import ChangeBatch


@dataclass(frozen=True)
class ReloadCommand:
    batch: ChangeBatch


@dataclass(frozen=True)
class RestartCommand:
    batch: ChangeBatch


@dataclass(frozen=True)
class ShowErrorCommand:
    title: str
    detail: str
