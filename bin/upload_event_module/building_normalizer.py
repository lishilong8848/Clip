import re
import unicodedata
from typing import Any

from .config import BUILDING_DETECT_ALIASES


BUILDING_CODE_ORDER = ("110", "A", "B", "C", "D", "E", "H")
_LETTER_BUILDING_ORDER = ("A", "B", "C", "D", "E", "H")


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
    match = re.fullmatch(r"([A-EH])(?:楼|栋)", normalized)
    if match:
        return f"{match.group(1)}楼"

    for alias, canonical in BUILDING_DETECT_ALIASES:
        if _normalized_lookup(alias) == normalized:
            return str(canonical or "").strip()

    return text


def extract_building_codes(value: Any) -> list[str]:
    """Extract explicit building codes without treating letters in words as buildings."""
    raw_values: list[Any] = []

    def append_values(raw: Any) -> None:
        if isinstance(raw, (list, tuple, set)):
            for item in raw:
                append_values(item)
            return
        raw_values.append(raw)

    append_values(value)
    found: list[str] = []

    def add(code: str) -> None:
        normalized_code = str(code or "").strip().upper()
        if normalized_code in BUILDING_CODE_ORDER and normalized_code not in found:
            found.append(normalized_code)

    for raw in raw_values:
        text = _normalized_lookup(raw)
        if not text:
            continue

        if re.search(r"110(?:站|楼|机房|数据中心|DC|KV)?", text):
            add("110")

        exact = re.fullmatch(r"(?:南通)?([A-EH])(?:楼|栋)?", text)
        if exact:
            add(exact.group(1))

        for match in re.finditer(
            r"(?<![A-Z0-9])([A-EH])(?:楼|栋)?(?:-|－|—|–|~|～|至|到)([A-EH])(?:楼|栋)?",
            text,
        ):
            start, end = match.groups()
            start_index = _LETTER_BUILDING_ORDER.index(start)
            end_index = _LETTER_BUILDING_ORDER.index(end)
            if start_index <= end_index:
                for code in _LETTER_BUILDING_ORDER[start_index : end_index + 1]:
                    add(code)

        for compact in re.findall(
            r"(?<![A-Z0-9])([A-EH]{2,6})(?:楼|栋|座|区|机房|数据中心|DC)",
            text,
        ):
            for code in compact:
                add(code)

        separated_pattern = (
            r"(?<![A-Z0-9])"
            r"((?:[A-EH](?:楼|栋)?(?:、|,|，|/|及|和|与|\+))+"
            r"[A-EH](?:楼|栋)?)"
            r"(?![A-Z0-9])"
        )
        for separated in re.findall(separated_pattern, text):
            for code in re.findall(r"[A-EH]", separated):
                add(code)

        for code in _LETTER_BUILDING_ORDER:
            patterns = (
                rf"(?<![A-Z0-9]){code}(?:楼|栋|座|区|机房|数据中心|DC)",
                rf"(?:楼栋|楼宇|数据中心){code}(?![A-Z0-9])",
                rf"(?<![A-Z0-9]){code}[-－]\d",
            )
            if any(re.search(pattern, text) for pattern in patterns):
                add(code)

    return [code for code in BUILDING_CODE_ORDER if code in found]


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
