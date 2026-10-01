# -*- coding: utf-8 -*-
"""当前程序未结束检修通告与智航屏蔽中记录核对，不读取飞书。"""
import re
import time
from upload_event_module.core.parser import is_notice_confirmed_ended

# 房间号: A-120 / C-241 / A178 / B-077 等。
# lookbehind 排除 EA118 中误提取的 A118(E 为字母前缀);lookahead 排除 A-1201 四位数截断
ROOM_RE = re.compile(r"(?<![A-Za-z0-9])[ABCDEH]-?\d{3}(?![0-9])", re.IGNORECASE)
# 楼栋: 「A楼」/「南通A楼」/「C楼」等（字母+楼）
BUILDING_RE = re.compile(r"([A-Z])\s*楼", re.IGNORECASE)
# 设备关键词: 英文字母开头的词段(HUM-01 / EA118 / ADD-001 等)
DEV_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{1,}")

def _as_text(v):
    """Text 字段[{text:..}]/SingleSelect 字符串/MultiSelect 列表 → 纯文本。"""
    if v is None:
        return ""
    if isinstance(v, str):
        return v
    if isinstance(v, list):
        return "".join(
            seg.get("text", "") if isinstance(seg, dict) else str(seg) for seg in v
        )
    if isinstance(v, dict):
        return v.get("text") or v.get("name") or str(v)
    return str(v)


def _as_time(v):
    """毫秒(或秒)时间戳 → 'YYYY-MM-DD HH:MM'。"""
    if not v:
        return ""
    try:
        n = float(v)
        sec = n / 1000.0 if n > 10 ** 12 else n
        return time.strftime("%Y-%m-%d %H:%M", time.localtime(sec))
    except Exception:
        return str(v)


def ongoing_records(items):
    """Normalize the portal's shared local projection without querying remote records."""
    out = []
    for item in items:
        if item.get('work_type') != 'repair' and item.get('notice_type') not in {'设备检修', '检修通告'}:
            continue
        if is_notice_confirmed_ended(item):
            continue
        fields = {}
        for key in ('raw_fields', 'fields', 'display_fields'):
            if isinstance(item.get(key), dict):
                fields.update(item[key])

        def value(key, *names):
            direct = _as_text(item.get(key)).strip()
            return direct or next((_as_text(fields[name]).strip() for name in names if fields.get(name)), '')

        record_id = value('target_record_id') or value('record_id') or value('active_item_id') or value('source_record_id')
        if not record_id:
            raise ValueError('本地检修通告缺少记录标识，请刷新当前程序的通告列表')
        rec = {
            'record_id': record_id,
            'name': value('title', '名称（标题）', '名称', '标题'),
            'status': value('status', '检修状态'),
            'location': value('location', '位置', '地点'),
            'device': value('repair_device', '维修设备'),
            'fault': value('repair_fault', '维修故障') or value('symptom', '故障现象'),
            'building': value('building', '楼栋') or '、'.join(
                f'{code}楼' if code in {'A', 'B', 'C', 'D', 'E', 'H'} else str(code)
                for code in item.get('building_codes') or []),
            'major': value('specialty', '专业'),
            'fault_time': _as_time(value('fault_time', '发生故障时间', '发现故障时间')),
            'start_time': _as_time(value('started_at', '实际开始时间') or value('actual_start_time')),
            'end_time': _as_time(value('actual_end_time', '实际结束时间')),
        }
        hay = " ".join([rec["location"], rec["name"], rec["device"]])
        rec["rooms"] = sorted(set(x.upper() for x in ROOM_RE.findall(hay)))
        out.append(rec)
    return out


def _device_keywords(text):
    """从设备描述提取英文词段(HUM-01/EA118/ADD-001),大写归一。"""
    return sorted(set(w.upper() for w in DEV_WORD_RE.findall(text or "")))


def _longest_common_substring(a, b, min_len=6):
    """最长公共子串(楼栋级匹配展示用),短于 min_len 返回空。
    min_len=6 可滤掉「EA118」「A楼A-1」这类巧合片段。"""
    if not a or not b:
        return ""
    prev = [0] * (len(b) + 1)
    best, best_end = 0, 0
    for i in range(1, len(a) + 1):
        cur = [0] * (len(b) + 1)
        for j in range(1, len(b) + 1):
            if a[i - 1] == b[j - 1]:
                cur[j] = prev[j - 1] + 1
                if cur[j] > best:
                    best, best_end = cur[j], i
        prev = cur
    return a[best_end - best:best_end] if best >= min_len else ""


def _block_time(b):
    for k in ("startTime", "start_time", "beginTime", "createTime"):
        v = b.get(k)
        if v:
            return _as_time(v)
    return ""


def _extract_building_letters(*texts):
    """从文本中提取楼栋字母集合(「A楼」/「南通A楼」→A;房间号 A-120 →A)。

    仅返回 已知楼栋字母;提取不到返回空集合(表示无法确定楼栋)。
    屏蔽记录名称无楼栋但详情含楼栋时也可传入 multiple 文本。
    """
    letters = set()
    for t in texts:
        s = str(t or "")
        letters.update(m.group(1).upper() for m in BUILDING_RE.finditer(s))
        # 房间号 A-120 / B-077 首字母也是楼栋线索(与 ROOM_RE 同款防误提取)
        letters.update(r[0].upper() for r in ROOM_RE.findall(s))
    return letters


def match_records(records, blocks, detail_of):
    """核心匹配。

    records: ongoing_records 输出;
    blocks: 屏蔽中记录列表(status==1,字段 blockId/blockName/status 等);
    detail_of: blockId → getAlarmBlockDetail 的 data(alarmBlockDetailResultList)。

    匹配两级(先名称、后空间位置),并在最前按「楼栋」预筛候选(减少匹配量):
      0) 楼栋预筛: 检修记录 building → 只对同楼栋屏蔽做匹配;
         屏蔽无确定楼栋(名称/空间/房间号都提取不到)时保留为候选(避免误杀)。
      A) 按屏蔽记录「名称」匹配(名称未命中才走 B):
         A1) 房间号交集: 检修(位置/名称/设备)提取的房间号 ∩ 屏蔽名称中的房间号
         A2) 设备关键词: 检修的英文设备词出现在屏蔽 blockName 或实例名中
         A3) 语义兜底: 屏蔽名 vs 检修名/设备/故障 LCS ≥6(不含空间串,防巧合)
      B) 按屏蔽「空间位置」兜底(仅当名称一级完全未命中):
         检修房间号 ∩ 屏蔽详情 spaceModel 中的房间号
         reason 标注「空间位置」以便人工复核。

    返回 (results, orphan_block_ids):
      results: 每条检修记录 + hits(命中屏蔽列表,含 reason)
      orphan_block_ids: 未命中任何检修的屏蔽 blockId(可能漏登记或多屏蔽)
    """
    block_info = {}
    for b in blocks:
        bid = str(b.get("blockId") or b.get("id") or "")
        if not bid:
            continue
        name = str(b.get("blockName") or "")
        detail = detail_of.get(bid) or {}
        spaces, insts = [], []
        for d in detail.get("alarmBlockDetailResultList") or []:
            sm = str(d.get("spaceModel") or "")
            if sm:
                spaces.append(sm)
            for i in (d.get("instances") or []):
                if isinstance(i, dict):
                    insts.append(str(i.get("instanceName") or i.get("name") or ""))
                else:
                    insts.append(str(i))
        name_rooms = set(x.upper() for x in ROOM_RE.findall(name))
        space_rooms = set()
        for sm in spaces:
            space_rooms.update(x.upper() for x in ROOM_RE.findall(sm))
        # 楼栋线索: 名称 + 空间位置 + 名称/空间里的房间号首字母
        block_bld = _extract_building_letters(name, "；".join(spaces))
        # 信息完整度: 名称/房间号/开始时间/空间/实例 是否齐全(打分用)
        start_t = _block_time(b)
        block_info[bid] = {
            "block": b, "name": name, "spaces": spaces, "insts": insts,
            "name_rooms": name_rooms, "space_rooms": space_rooms,
            "name_upper": name.upper(),
            "insts_upper": [i.upper() for i in insts],
            "building": block_bld,   # 空集合=无法确定楼栋(保留候选)
            "has_name": bool(name.strip()),
            "has_name_room": bool(name_rooms),
            "has_space": bool(spaces),
            "has_space_room": bool(space_rooms),
            "has_start": bool(start_t),
            "has_inst": bool(insts),
        }

    results, hit_ids = [], set()
    for rec in records:
        rec_rooms = set(rec["rooms"])
        dev_words = _device_keywords(rec["device"] + " " + rec["name"])
        # 0) 楼栋预筛: 检修端楼栋字母
        rec_bld = _extract_building_letters(rec.get("building", ""))
        hits = []
        for bid, bi in block_info.items():
            if rec_bld and bi["building"] and not (rec_bld & bi["building"]):
                continue  # 楼栋不相符 → 跳过该屏蔽(减少匹配量)
            name_reason = []
            score = 0
            # ---- A) 名称级匹配 ----
            common = sorted(rec_rooms & bi["name_rooms"])
            if common:
                name_reason.append("房间号 " + "、".join(common))
                score += 12 * len(common)
            else:
                dev_hit = ""
                for w in dev_words:
                    if w in bi["name_upper"] or any(w in i for i in bi["insts_upper"]):
                        dev_hit = w
                        break
                if dev_hit:
                    name_reason.append(f"设备关键词「{dev_hit}」")
                    score += 8
                else:
                    # 语义兜底:仅对比「屏蔽名 vs 检修名/设备/故障描述」,
                    # 不含 spaceModel(EA118 等空间编号会大量巧合命中)。
                    best_seg = ""
                    for rn in [rec["name"], rec["device"], rec["fault"]]:
                        seg = _longest_common_substring(rn, bi["name"])
                        if len(seg) > len(best_seg):
                            best_seg = seg
                    if best_seg:
                        name_reason.append(f"名称含「{best_seg}」")
                        # LCS 长度编码: ≥6 → 5 分,每多 2 字符 +1,封顶 10
                        score += min(5 + max(0, (len(best_seg) - 6) // 2), 10)
            if name_reason:
                # 完整度加分: 屏蔽记录信息越全越可信(名称/房间号/空间/时间/实例)
                score += (2 if bi["has_name"] else 0)
                score += (3 if bi["has_name_room"] else 0)
                score += (2 if bi["has_space"] else 0)
                score += (3 if bi["has_space_room"] else 0)
                score += (2 if bi["has_start"] else 0)
                score += (1 if bi["has_inst"] else 0)
                hits.append({
                    "blockId": bid,
                    "blockName": bi["name"],
                    "startTime": _block_time(bi["block"]),
                    "spaces": bi["spaces"][:8],
                    "instances": bi["insts"][:8],
                    "reason": "；".join(name_reason),
                    "score": score,
                })
                hit_ids.add(bid)
                continue
            # ---- B) 空间位置兜底(名称未命中才看) ----
            space_common = sorted(rec_rooms & bi["space_rooms"])
            if space_common:
                hits.append({
                    "blockId": bid,
                    "blockName": bi["name"],
                    "startTime": _block_time(bi["block"]),
                    "spaces": bi["spaces"][:8],
                    "instances": bi["insts"][:8],
                    "reason": "空间位置 " + "、".join(space_common),
                    "score": 3 * len(space_common)
                             + (2 if bi["has_name"] else 0)
                             + (3 if bi["has_space_room"] else 0)
                             + (2 if bi["has_start"] else 0)
                             + (1 if bi["has_inst"] else 0),
                })
                hit_ids.add(bid)
        # 按分数降序(最高分最前/最左侧);同分按 blockId 稳定
        hits.sort(key=lambda h: (-h.get("score", 0), h["blockId"]))
        results.append(dict(rec, hits=hits))

    orphans = [bid for bid in block_info if bid not in hit_ids]
    return results, orphans

