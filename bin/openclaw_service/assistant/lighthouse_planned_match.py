# -*- coding: utf-8 -*-
"""Pure local name matcher for planned-notice records.

No network / model / vector calls and no runtime credential or data access
beyond caller-supplied ``items``.  ``match_candidates`` returns a dict with
``status`` (``unique`` | ``choose`` | ``no_match``), ``selected_id``, a list
of ``candidates`` and a ``recall`` list (<= 20 best compatible items for an
optional one-shot reranker).  Items are returned unchanged plus ``score``
(0-100), ``exact`` (bool) and a short ``match_reason``.
"""
from __future__ import annotations

import math
import re
import unicodedata
from typing import Any, Iterable

_WHITESPACE = re.compile(r"\s+")
# Site / building prefixes.  Chinese has no \w word boundaries, so these are
# removed as literal substrings (safe because they are distinctive markers).
_SITE_RE = re.compile(r"(?:ea118(?:_?c01)?|c01机房|110站|南通|园区|数据中心|机房)", re.I)
_BUILDING_TOKEN = re.compile(r"(?<![A-Za-z0-9])([a-eh])(?:楼|栋)", re.I)
# Generic work-type / notice words that carry no identifying signal.  Power
# direction words (上电/下电) are intentionally preserved.
_GENERIC_WORDS = re.compile(
    r"(?:维保通告|维护通告|变更通告|检修通告|轮巡通告|调整通告|上下电通告|上电通告|下电通告|"
    r"设备检修|设备轮巡|设备调整|事件通告|通告|维保|维护|变更|检修|维修|轮巡|调整|设备|事件)"
)
_PUNCT = re.compile(r"[，。、；;：:,，．!?！？()（）\[\]【】{}·・]+")
# Alphanumeric equipment identifiers, e.g. A-445-TRB-202-3 or A-127-2#.
_DEVICE_CODE_RE = re.compile(
    r"(?<![A-Za-z0-9])([A-EH])(?:[-_－—])?([A-Za-z0-9]+(?:[-_－—][A-Za-z0-9]+)*)(?![A-Za-z0-9])",
    re.I,
)

_CYCLE_CANON = {
    "每月": "monthly", "月度": "monthly", "monthly": "monthly", "月": "monthly",
    "每季": "quarterly", "每季度": "quarterly", "季度": "quarterly", "季": "quarterly",
    "quarterly": "quarterly",
    "每年": "yearly", "年度": "yearly", "yearly": "yearly", "年": "yearly",
    "半年": "half_year", "semiannual": "half_year",
    "每两年": "every_2y", "每三年": "every_3y", "每五年": "every_5y",
    "非计划性": "unplanned", "unplanned": "unplanned",
    "冬季保温每日": "winter_daily",
}
_NOTICE_WORD_TO_TYPE = {
    "维保通告": "maintenance", "维护通告": "maintenance", "维保": "maintenance", "维护": "maintenance",
    "变更通告": "change", "变更": "change",
    "设备检修": "repair", "检修通告": "repair", "检修": "repair", "维修": "repair",
    "设备轮巡": "polling", "轮巡通告": "polling", "轮巡": "polling",
    "设备调整": "adjust", "调整通告": "adjust", "调整": "adjust",
    "上下电通告": "power", "上电通告": "power", "下电通告": "power",
    "上电": "power", "下电": "power",
}
_NOTICE_LONG_FIRST = tuple(sorted(_NOTICE_WORD_TO_TYPE, key=len, reverse=True))
_BUILDING_LETTERS = frozenset("ABCDEH")
_STOP_TOKENS = frozenset({"ea118", "110", "南通", "园区", "数据中心", "机房", "上电", "下电"})


def _norm_text(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).strip().lower()
    return _WHITESPACE.sub(" ", _PUNCT.sub(" ", text)).strip()


def _extract_device_codes(text: str) -> list[tuple[str, ...]]:
    found: list[tuple[str, ...]] = []
    source = _SITE_RE.sub(" ", unicodedata.normalize("NFKC", str(text or "")).lower())
    for match in _DEVICE_CODE_RE.finditer(source):
        letter = match.group(1).upper()
        parts = [p for p in re.split(r"[-_－—]+", match.group(2)) if p]
        if not parts:
            continue
        code = tuple([letter] + parts)
        if code not in found:
            found.append(code)
    return found


def _device_string(code: tuple[str, ...]) -> str:
    return "-".join(code).lower()


def _building_codes(text: str) -> frozenset[str]:
    codes: set[str] = set()
    for match in _BUILDING_TOKEN.finditer(text):
        codes.add(match.group(1).upper())
    for match in re.finditer(r"(?<![A-Za-z0-9])([A-EH])(?:楼|栋)?(?![A-Za-z0-9-])", text, re.I):
        letter = match.group(1).upper()
        if letter in _BUILDING_LETTERS:
            codes.add(letter)
    return frozenset(codes)


def _detect_cycle(value: Any) -> str | None:
    text = _norm_text(value)
    if not text:
        return None
    if "半年" in text:
        return "half_year"
    for alias in sorted(_CYCLE_CANON, key=len, reverse=True):
        if alias in {"年", "月", "季"} and text != alias:
            continue
        if alias in text:
            return _CYCLE_CANON[alias]
    return None


def _normalize_cycle_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"/", "-", "—"}:
        return None
    return _detect_cycle(text) or _norm_text(text)


def _detect_work_types(text: str) -> frozenset[str]:
    types: set[str] = set()
    for word in _NOTICE_LONG_FIRST:
        if word in text:
            types.add(_NOTICE_WORD_TO_TYPE[word])
    return frozenset(types)


def _detect_direction(text: str) -> str | None:
    if "测试电转正式电" in text:
        return "to_formal"
    if "正式电转测试电" in text:
        return "to_test"
    up = bool(re.search(r"上(?:正式|测试)?电", text))
    down = bool(re.search(r"下(?:正式|测试)?电", text))
    if up and down:
        return None
    return "up" if up else ("down" if down else None)


def _meaning(text: str) -> str:
    """Meaningful text for Chinese substring/bigram similarity."""
    s = unicodedata.normalize("NFKC", str(text or "")).lower()
    s = _SITE_RE.sub(" ", s)
    s = _BUILDING_TOKEN.sub(" ", s)
    s = _GENERIC_WORDS.sub(" ", s)
    s = _DEVICE_CODE_RE.sub(" ", s)
    s = _PUNCT.sub(" ", s)
    s = re.sub(r"[^a-z0-9\u4e00-\u9fff]", " ", s)
    return _WHITESPACE.sub("", s)


def _char_bigrams(s: str) -> frozenset[str]:
    return frozenset(s[i:i + 2] for i in range(len(s) - 1)) if len(s) > 1 else frozenset()


def _core_set(text: str, device_codes: list[tuple[str, ...]]) -> frozenset[str]:
    s = unicodedata.normalize("NFKC", str(text or "")).lower()
    s = _SITE_RE.sub(" ", s)
    s = _BUILDING_TOKEN.sub(" ", s)
    s = s.replace("上下电", " 上电 下电 ")
    s = _GENERIC_WORDS.sub(" ", s)
    s = _DEVICE_CODE_RE.sub(" ", s)
    tokens = frozenset(re.findall(r"[a-z0-9\u4e00-\u9fff]+", s)) - _STOP_TOKENS
    device_tokens = {_device_string(code) for code in device_codes}
    lower = unicodedata.normalize("NFKC", str(text or "")).lower()
    if "上电" in lower and "下电" in lower:
        device_tokens |= {"p_up", "p_down"}
    elif "上电" in lower:
        device_tokens |= {"p_up"}
    elif "下电" in lower:
        device_tokens |= {"p_down"}
    return frozenset(device_tokens | tokens)


def _has_discriminator(features: dict[str, Any]) -> bool:
    return bool(features["core"])


def _parse(query_or_title: str) -> dict[str, Any]:
    raw = unicodedata.normalize("NFKC", str(query_or_title or "")).lower()
    device_codes = _extract_device_codes(raw)
    meaning = _meaning(raw)
    return {
        "device_codes": device_codes,
        "device_token_set": frozenset(_device_string(c) for c in device_codes),
        "building_codes": _building_codes(raw),
        "work_types": _detect_work_types(raw),
        "direction": _detect_direction(raw),
        "cycle": _detect_cycle(raw),
        "meaning": meaning,
        "bigrams": _char_bigrams(meaning),
        "core": _core_set(raw, device_codes),
    }


def _parse_query(query: Any) -> dict[str, Any]:
    return _parse(query)


def _parse_item(item: dict[str, Any]) -> dict[str, Any]:
    parsed = _parse(item.get("title"))
    parsed["work_type"] = str(item.get("work_type") or "").strip().lower()
    if not parsed["building_codes"] and item.get("building"):
        codes = _building_codes(str(item.get("building") or ""))
        parsed["building_codes"] = codes
    cycle = _normalize_cycle_value(item.get("maintenance_cycle")) or parsed["cycle"]
    parsed["cycle"] = cycle
    return parsed


def _item_source_id(item: dict[str, Any]) -> str:
    value = item.get("source_record_id")
    return value if isinstance(value, str) else (str(value) if value is not None else "")


def _device_compatible(q_codes: Iterable[tuple[str, ...]], i_codes: Iterable[tuple[str, ...]]) -> bool:
    """Query codes constrain item codes; missing query codes are never a conflict."""
    q_list = list(q_codes)
    i_list = list(i_codes)
    if not q_list:
        return True
    if not i_list:
        return True
    for q in q_list:
        if not any(ic == q or ic[: len(q)] == q for ic in i_list):
            return False
    return True


def _is_eligible(query_feat: dict[str, Any], item_feat: dict[str, Any]) -> tuple[bool, str]:
    if query_feat["device_codes"] and item_feat["device_codes"]:
        if not _device_compatible(query_feat["device_codes"], item_feat["device_codes"]):
            return False, "设备编号不同"
    if query_feat["work_types"] and item_feat["work_type"]:
        if item_feat["work_type"] not in query_feat["work_types"]:
            return False, "通告类型不同"
    q_dir, i_dir = query_feat["direction"], item_feat["direction"]
    if q_dir and i_dir and item_feat["work_type"] == "power" and q_dir != i_dir:
        return False, "上/下电方向不同"
    if query_feat["cycle"] and item_feat["cycle"] and query_feat["cycle"] != item_feat["cycle"]:
        return False, "保养周期不同"
    return True, ""


def _local_score(query_feat: dict[str, Any], item_feat: dict[str, Any]) -> tuple[int, bool, str]:
    q_core, i_core = query_feat["core"], item_feat["core"]
    exact = bool(q_core) and q_core == i_core
    shared_dev = query_feat["device_token_set"] & item_feat["device_token_set"]
    if exact:
        reason = f"exact device {sorted(shared_dev)[0]}" if shared_dev else "exact"
        return 100, True, reason

    overlap = len(q_core & i_core)
    denom = len(q_core) + len(i_core)
    token_sim = (2.0 * overlap / denom) if denom else 0.0

    bg_overlap = len(query_feat["bigrams"] & item_feat["bigrams"])
    bg_denom = len(query_feat["bigrams"]) + len(item_feat["bigrams"])
    bg_sim = (2.0 * bg_overlap / bg_denom) if bg_denom else 0.0

    score = int(round(max(token_sim, bg_sim) * 60.0))

    q_m, i_m = query_feat["meaning"], item_feat["meaning"]
    if q_m and i_m and (q_m in i_m or i_m in q_m):
        score += 20

    if shared_dev:
        score += 60
    elif query_feat["device_codes"] and item_feat["device_codes"] and \
            _device_compatible(query_feat["device_codes"], item_feat["device_codes"]):
        score += 50

    if query_feat["work_types"] and item_feat["work_type"]:
        if item_feat["work_type"] in query_feat["work_types"]:
            score += 20
    elif query_feat["work_types"] == frozenset({"power"}) or item_feat["work_type"] == "power":
        score += 5

    if query_feat["building_codes"] and item_feat["building_codes"] and \
            query_feat["building_codes"] & item_feat["building_codes"]:
        score += 15
    if query_feat["cycle"] and item_feat["cycle"] and query_feat["cycle"] == item_feat["cycle"]:
        score += 10
    if query_feat["direction"] and item_feat["direction"] and \
            query_feat["direction"] == item_feat["direction"]:
        score += 10

    score = max(0, min(100, score))
    reason = f"device {sorted(shared_dev)[0]}" if shared_dev else "fuzzy"
    return score, False, reason


def _validate_semantic(semantic_scores: Any, recall_ids: set[str]) -> None:
    if semantic_scores is None:
        return
    if not isinstance(semantic_scores, (list, tuple)):
        raise ValueError("semantic_scores must be a list")
    seen: set[str] = set()
    for entry in semantic_scores:
        if not isinstance(entry, dict):
            raise ValueError("semantic score entry must be a dict")
        sid = entry.get("source_record_id")
        score = entry.get("score")
        if not isinstance(sid, str) or not sid:
            raise ValueError("semantic score requires a source_record_id")
        if sid in seen:
            raise ValueError(f"duplicate semantic score for {sid}")
        if sid not in recall_ids:
            raise ValueError(f"semantic score outside recall: {sid}")
        seen.add(sid)
        if isinstance(score, bool) or not isinstance(score, (int, float)):
            raise ValueError(f"non-numeric semantic score for {sid}")
        if math.isnan(score) or math.isinf(score) or not (0.0 <= score <= 100.0):
            raise ValueError(f"semantic score out of range for {sid}")


def _enrich(item: dict[str, Any], score: int, exact: bool, reason: str) -> dict[str, Any]:
    out = dict(item)
    out["score"] = score
    out["exact"] = exact
    out["match_reason"] = reason
    return out


def match_candidates(
    query: str,
    items: list[dict[str, Any]],
    semantic_scores: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Match a planned-notice name against compatible records."""
    if not isinstance(items, (list, tuple)):
        raise ValueError("items must be a list")
    items = list(items)
    query_feat = _parse_query(query)

    item_feats: list[tuple[int, dict[str, Any]]] = []
    eligible: set[int] = set()
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            continue
        parsed = _parse_item(item)
        ok, _ = _is_eligible(query_feat, parsed)
        if ok:
            eligible.add(index)
        item_feats.append((index, parsed))

    scored: list[tuple[int, int, dict[str, Any], bool, str]] = []
    for index, parsed in item_feats:
        if index not in eligible:
            continue
        item = items[index]
        score, exact, reason = _local_score(query_feat, parsed)
        scored.append((score, index, item, exact, reason))

    scored.sort(key=lambda r: (-r[0], r[1], _item_source_id(r[2])))
    recall = scored[:20]
    recall_ids = {_item_source_id(row[2]) for row in recall}

    if semantic_scores is not None:
        _validate_semantic(semantic_scores, recall_ids)

    semantic_map: dict[str, int] = {}
    if semantic_scores:
        for entry in semantic_scores:
            semantic_map[entry["source_record_id"]] = int(entry["score"])

    def effective(row):
        return semantic_map.get(_item_source_id(row[2]), row[0])

    def enrich_row(row, *, score=None, reason=None):
        local, idx, item, exact, local_reason = row
        final_reason = reason if reason is not None else local_reason
        return _enrich(item, local if score is None else score, exact, final_reason)

    def make_result(status, selected_id, candidates, recall_rows=None):
        rows = recall_rows if recall_rows is not None else recall
        recall_list = [_enrich(r[2], r[0], r[3], r[4]) for r in rows]
        return {
            "status": status,
            "selected_id": selected_id,
            "candidates": candidates,
            "recall": recall_list,
        }

    if not _has_discriminator(query_feat):
        return make_result("no_match", "", [])

    exact_matches = [row for row in scored if row[3]]
    if exact_matches:
        if len(exact_matches) == 1 and len(query_feat["work_types"]) <= 1:
            row = exact_matches[0]
            return make_result("unique", _item_source_id(row[2]), [enrich_row(row, score=100)])
        candidates = [enrich_row(row, score=100) for row in exact_matches]
        return make_result("choose", "", candidates)

    # Semantic auto-select only when every recalled candidate is scored.
    semantic_winner = None
    if semantic_map and recall and len(query_feat["work_types"]) <= 1 and all(_item_source_id(row[2]) in semantic_map for row in recall):
        ranked = sorted(
            ((semantic_map[_item_source_id(row[2])], row) for row in recall),
            key=lambda x: (-x[0], _item_source_id(x[1][2]), x[1][1]),
        )
        top_score, top_row = ranked[0]
        second = ranked[1][0] if len(ranked) > 1 else -1
        if top_score >= 90 and (top_score - second) >= 15:
            selected_id = _item_source_id(top_row[2])
            candidate = enrich_row(top_row, score=top_score, reason=f"semantic {top_score}")
            semantic_winner = {"selected_id": selected_id, "candidate": candidate}
    if semantic_winner is not None:
        return make_result("unique", semantic_winner["selected_id"], [semantic_winner["candidate"]])

    candidate_rows = [row for row in scored if effective(row) >= 70]
    candidate_rows.sort(key=lambda r: (-effective(r), r[1], _item_source_id(r[2])))
    candidates = []
    for row in candidate_rows:
        eff = int(effective(row))
        reason = f"semantic {eff}" if _item_source_id(row[2]) in semantic_map else None
        candidates.append(enrich_row(row, score=eff, reason=reason))
    if candidates:
        return make_result("choose", "", candidates)

    return make_result("no_match", "", [])
