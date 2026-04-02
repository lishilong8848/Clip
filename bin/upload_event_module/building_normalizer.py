import re
import unicodedata
from typing import Any

from .config import BUILDING_DETECT_ALIASES


def _normalized_lookup(value: Any) -> str:
    text = str(value or "").strip(" \t\r\n,，;；。")
    if not text:
        return ""
    return "".join(unicodedata.normalize("NFKC", text).split()).upper()


def normalize_building_name(value: Any) -> str:
    text = str(value or "").strip(" \t\r\n,，;；。")
    if not text:
        return ""

    normalized = _normalized_lookup(text)
    if not normalized:
        return ""

    # X楼/X栋统一成X楼，避免同义值并存。
    match = re.fullmatch(r"([A-E])(?:楼|栋)", normalized)
    if match:
        return f"{match.group(1)}楼"

    for alias, canonical in BUILDING_DETECT_ALIASES:
        if _normalized_lookup(alias) == normalized:
            return str(canonical or "").strip()

    return text


def normalize_buildings_value(value: Any) -> list[str]:
    if isinstance(value, str):
        raw_items = [value]
    elif isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        return []

    normalized: list[str] = []
    for item in raw_items:
        canonical = normalize_building_name(item)
        if canonical and canonical not in normalized:
            normalized.append(canonical)
    return normalized
