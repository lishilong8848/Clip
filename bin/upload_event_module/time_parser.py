import re
from datetime import datetime


_TIME_PART = (
    r"(?P<h>\d{1,2})\s*(?:[:：.．]\s*)?"
    r"(?P<min>\d{1,2})(?:\s*[:：.．]\s*(?P<sec>\d{1,2}))?"
)

_FULL_DT = re.compile(
    rf"(?P<y>\d{{4}})[./-](?P<m>\d{{1,2}})[./-](?P<d>\d{{1,2}})(?:\s+|\s*-\s*|T)?{_TIME_PART}"
)
# 更宽松的日期时间匹配：允许任意非数字分隔符（含中文“年/月/日”或空格）
_FLEX_DT = re.compile(
    rf"(?P<y>\d{{4}})\D+(?P<m>\d{{1,2}})\D+(?P<d>\d{{1,2}})(?:\D+|T)?{_TIME_PART}"
)
_MD_DT = re.compile(
    rf"(?P<m>\d{{1,2}})[./-](?P<d>\d{{1,2}})(?:\s+|\s*-\s*|T)?{_TIME_PART}"
)
_D_DT = re.compile(rf"(?P<d>\d{{1,2}})(?:\s+|\s*-\s*|T){_TIME_PART}")
_TIME_ONLY = re.compile(_TIME_PART)


def _normalize_time_text(text: str, slash_as_digit: bool = False) -> str:
    if not text:
        return ""
    normalized = text.strip()
    # 汉字容错先执行，避免被后续拆解
    normalized = normalized.replace("听天", "昨天").replace("昨夭", "昨天")

    lookalike_map = {
        "O": "0",
        "o": "0",
        "D": "0",
        "Q": "0",
        "〇": "0",
        "C": "0",
        "U": "0",
        "I": "1",
        "l": "1",
        "i": "1",
        "|": "1",
        "!": "1",
        "一": "1",
        "丨": "1",
        "亅": "1",
        "[": "1",
        "]": "1",
        "\\": "1",
        "Z": "2",
        "z": "2",
        "乙": "2",
        "己": "2",
        "ㄗ": "2",
        "E": "3",
        "З": "3",
        "彐": "3",
        "ヨ": "3",
        "}": "3",
        "A": "4",
        "H": "4",
        "Ч": "4",
        "+": "4",
        "S": "5",
        "s": "5",
        "$": "5",
        "ㄎ": "5",
        "G": "6",
        "b": "6",
        "占": "6",
        "T": "7",
        "┐": "7",
        "〉": "7",
        "了": "7",
        "B": "8",
        "S": "8",
        "&": "8",
        "吕": "8",
        "串": "8",
        "g": "9",
        "q": "9",
        "P": "9",
        "p": "9",
        ";": ":",
        ".": ":",
        ",": ":",
        "：": ":",
        "．": ":",
        "。": ":",
        "巳": ":",
        "·": ":",
        "听": "昨",
        "夭": "天",
        "大": "天",
        "夫": "天",
        "俞": "前",
        "口": "日",
        "闩": "月",
        "点": ":",
        "时": ":",
        "分": "",
        "，": ",",
        "－": "-",
        "—": "-",
        "–": "-",
        "～": "-",
        "~": "-",
    }

    if slash_as_digit:
        lookalike_map["/"] = "1"

    normalized = "".join([lookalike_map.get(char, char) for char in normalized])
    normalized = re.sub(r"[年月]", "-", normalized)
    normalized = re.sub(r"[日号]", " ", normalized)
    normalized = re.sub(r"[时点]", ":", normalized)
    normalized = normalized.replace("分", "").replace("秒", "")
    normalized = re.sub(r"[至到]", "-", normalized)
    normalized = re.sub(r"\s*-\s*", "-", normalized)
    normalized = re.sub(r"\s*:\s*", ":", normalized)
    normalized = re.sub(r"\s*\.\s*", ".", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _build_datetime(
    match: re.Match,
    default_year: int | None = None,
    default_month: int | None = None,
    default_day: int | None = None,
):
    if not match:
        return None
    groupdict = match.groupdict()
    y = int(groupdict.get("y")) if groupdict.get("y") else default_year
    m = int(groupdict.get("m")) if groupdict.get("m") else default_month
    d = int(groupdict.get("d")) if groupdict.get("d") else default_day
    h = int(groupdict.get("h")) if groupdict.get("h") else 0
    minute = int(groupdict.get("min")) if groupdict.get("min") else 0
    sec = int(groupdict.get("sec")) if groupdict.get("sec") else 0
    if not (y and m and d):
        return None
    try:
        return datetime(y, m, d, h, minute, sec)
    except ValueError:
        return None


def parse_time_range(text: str):
    """
    Parse a date/time range from the given text.

    Supported patterns (after normalization):
      - YYYY-M-D HH:MM - YYYY-M-D HH:MM
      - YYYY-M-D HH:MM - M-D HH:MM
      - YYYY-M-D HH:MM - D HH:MM
      - YYYY-M-D HH:MM - HH:MM
      - YYYY-M-D HH:MM
      - YYYY-M-D HH:MM:SS
    Range connector can be any symbol (e.g. '-', '~', '至', '到').
    """
    if not text:
        return None, None
    normalized = _normalize_time_text(text, slash_as_digit=False)
    if not normalized:
        return None, None

    start_match = _FULL_DT.search(normalized)
    if not start_match:
        start_match = _FLEX_DT.search(normalized)
    if not start_match:
        return None, None
    start_dt = _build_datetime(start_match)
    if not start_dt:
        return None, None

    tail = normalized[start_match.end() :]
    tail = re.sub(r"^[\s\-~～—–至到]+", "", tail)
    end_dt = None

    for pattern in (_FULL_DT, _FLEX_DT, _MD_DT, _D_DT, _TIME_ONLY):
        match = pattern.search(tail)
        if not match:
            continue
        if pattern is _FULL_DT:
            end_dt = _build_datetime(match)
        elif pattern is _FLEX_DT:
            end_dt = _build_datetime(match)
        elif pattern is _MD_DT:
            end_dt = _build_datetime(match, default_year=start_dt.year)
        elif pattern is _D_DT:
            end_dt = _build_datetime(
                match, default_year=start_dt.year, default_month=start_dt.month
            )
        else:
            end_dt = _build_datetime(
                match,
                default_year=start_dt.year,
                default_month=start_dt.month,
                default_day=start_dt.day,
            )
        if end_dt:
            break

    return start_dt, end_dt


def parse_single_datetime(text: str):
    start_dt, _ = parse_time_range(text)
    return start_dt


def parse_time_only(text: str):
    if not text:
        return None
    normalized = _normalize_time_text(text, slash_as_digit=True)
    if not normalized:
        return None
    match = re.search(r"(\d{1,2})[:：.．](\d{1,2})(?:[:：.．](\d{1,2}))?", normalized)
    if not match:
        match = _TIME_ONLY.search(normalized)
    if not match:
        return None
    try:
        hour = int(match.group(1))
        minute = int(match.group(2))
        sec = int(match.group(3)) if match.group(3) else 0
    except (TypeError, ValueError):
        return None
    if hour > 23 or minute > 59 or sec > 59:
        return None
    return hour, minute, sec
