"""Resolve query scope without widening the authenticated actor's permissions."""
import re
import unicodedata

from .lighthouse_ai import AssistantError
from .lighthouse_sources import SCOPES, codes

ALL = re.compile(r"(?<![A-Z0-9])ALL(?![A-Z0-9])|全部(?:楼栋|楼|范围)|所有楼栋|全楼|^\s*(?:看)?全部\s*$")
CAMPUS = re.compile(r"(?<![A-Z0-9])(?:CAMPUS|ABCDE)(?![A-Z0-9])|园区")


def resolve_scopes(actor, text, previous=None):
    allowed = set(actor.get("scopes") or ()) & SCOPES
    text = unicodedata.normalize("NFKC", str(text or "")).upper()
    named_text = CAMPUS.sub(" ", ALL.sub(" ", text))
    named = codes(named_text)
    named.update(re.findall(r"(?:只看|仅看|查看|范围|楼栋|还有|也看|也包括|加上)\s*[:：]?\s*(110|[ABCDEH])(?![A-Z0-9])", named_text))
    for group in re.findall(r"([A-Z](?:\s*[、,，/和与]\s*[A-Z])+)(?:楼|栋)", named_text):
        named.update(re.findall(r"[A-Z]", group))
    named.update(re.findall(r"(?<![A-Z0-9])([A-Z])(?=\s*[楼栋])", named_text))
    if named - allowed:
        raise AssistantError("当前账号无权查询问题中涉及的楼栋。", 403)
    if named:
        if previous and re.search(r"还有|也看|也包括|再加上|加上|增加", text) and not re.search(r"只看|仅看|只要|仅限", text):
            return sorted(named | (allowed & set(previous)))
        return sorted(named)
    if ALL.search(text):
        return sorted(allowed)
    if CAMPUS.search(text):
        return sorted(allowed & set("ABCDE"))
    return sorted(allowed & set(previous) if previous is not None else allowed)
