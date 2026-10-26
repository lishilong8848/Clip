"""Bounded pure helpers for personnel counts and roster evidence (no IO)."""
from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Sequence

from .lighthouse_ai import AssistantError
from .lighthouse_sources import SCOPES, codes

__all__ = ["personnel_count_request", "summarize_staff", "summarize_roster"]

_HOW = re.compile(r"怎么|如何|怎样|何以|方法|步骤|教程|示例|例子|例题|统计出来|数出来|算一算|怎么数|怎么算|如何计算|怎么统计", re.I)
_WEATHER = re.compile(r"天气|下雨|降雨|雨雪|气温|温度|湿度|风力|风速|风向|预报|weather|temperature|humidity|rain|forecast", re.I)
_CAREER = re.compile(r"求职|招聘|简历|工资|薪资|薪酬|社保|公积金|职业规划|职业发展|职业生涯|面试|失业|退休|跳槽|行业|就业|职业选择", re.I)
_ATTENDANCE = re.compile(r"请假|上班|下班|值班|加班|迟到|早退|休假|出差|旷工|缺勤|在休|在假|离职|调岗|调动|异动|调休|出勤|考勤|休班", re.I)
_GENDER = re.compile(r"女性|男性|女职工|男职工")
_QUALIFIER = re.compile(r"职位|岗位|工种|学历|职称|部门|班组|性别|工程师|主管|经理|班长|组长|主任|领导|年龄段|工龄")
_SEND = re.compile(r"发给|发送|转发|转给|上报|汇报|抄送|转达")
_PAST = re.compile(r'去年|前年|上月|上个月|上周|昨天|历史|\d{4}年|\d{1,2}月|\d{4}-\d{2}')
_IN_SERVICE = re.compile(r"在职|在岗|上岗")
_STAFF = re.compile(r"员工|人员|职工|同事")
_COUNT = re.compile(r"多少(?:位|个|人)?|几(?:名|位|个|人)?|人数|数量|总数|总人数|多少人|多少名|几名")


def personnel_count_request(question: Any) -> bool:
    if not isinstance(question, str):
        return False
    text = unicodedata.normalize("NFKC", question).strip()
    if not text or _HOW.search(text) or _WEATHER.search(text) or _CAREER.search(text):
        return False
    if _ATTENDANCE.search(text) or _GENDER.search(text) or _QUALIFIER.search(text) or _SEND.search(text) or _PAST.search(text):
        return False
    return bool((_IN_SERVICE.search(text) or _STAFF.search(text)) and _COUNT.search(text))


def summarize_roster(rows, actor, *, day, shift='', observed_at, source_url, complete_source=False):
    allowed = set(actor.get('scopes') or [])
    if not allowed or not allowed <= set(SCOPES):
        raise AssistantError('当前账号未配置可查询的楼栋范围。', 403)
    shifts = [shift] if shift in {'白', '夜'} else ['白', '夜']
    people = {}
    for row in rows:
        scope, duty_shift, name = row.get('scope'), row.get('shift'), str(row.get('name') or '').strip()
        if scope not in allowed or duty_shift not in shifts:
            continue
        if not name:
            raise AssistantError('排班名单姓名缺失，不能确认人数。', 503)
        key = (scope, duty_shift, name)
        oid = row.get('open_id') or ''
        people[key] = oid if key not in people or people[key] == oid else ''
    if not people and not complete_source:
        raise AssistantError('未读取到所选日期和楼栋的排班名单，人数未知，不能按零人处理。', 503)
    missing = [] if complete_source else [scope + '楼' + duty + '班' for scope in sorted(allowed) for duty in shifts
               if not any(key[:2] == (scope, duty) for key in people)]
    counts = [{'scope': scope, 'shift': duty, 'count': sum(key[:2] == (scope, duty) for key in people)}
              for scope in sorted(allowed) for duty in shifts if complete_source or any(key[:2] == (scope, duty) for key in people)]
    unique = len(set(people.values())) if all(people.values()) else None
    prefix = '已读取排班' if missing else '排班'
    basis = f'{day}，' + (f'{shift}班' if shift else '白夜班') + f'{prefix}合计 **{len(people)} 个排班姓名项**'
    basis += f'，按飞书身份去重 **{unique} 人**。' if unique is not None else '；部分人员身份未唯一确认，暂不提供跨楼栋、班次的去重总人数。'
    basis += '\n\n' + '；'.join(f"{r['scope']}楼{r['shift']}班 {r['count']} 人" for r in counts) + '。'
    if missing:
        basis += '\n\n未读到排班：' + '、'.join(missing) + '；未计作零人，以上不是全部楼栋的完整总数。'
    basis += ('\n\n口径：按员工信息池当日班组、班次及机楼/专业统计，白班包含长白人员，班次空白表示当天不上班；不代表实时签到。'
              if complete_source else '\n\n口径：按排班日期和人员（文本）统计，不含长白；同一人跨班次或楼栋分别计项，重名或身份缺失时不推断总人数，不代表实时签到。')
    kinds = {(row.get('scope'), row.get('shift'), row.get('name')): row.get('duty_kind', row.get('shift')) for row in rows}
    return {'name': '值班排班人数统计', 'date': day, 'shift': shift, 'assignment_count': len(people),
            'items': [{'scope': scope, 'shift': duty, 'name': name, **({'duty_kind': kinds[(scope,duty,name)]} if complete_source else {})} for scope, duty, name in sorted(people)],
            'unique_people': unique, 'per_building': counts, 'complete': not missing, 'missing': missing,
            'observed_at': observed_at, 'source_url': source_url, 'basis': basis}


def building_codes(value: Any):
    found = set(codes(value))
    for part in re.split(r"[/、,，;；&\s]+", str(value or "").upper()):
        part = part.strip()
        if re.fullmatch(r"[ABCDEH]", part):
            found.add(part)
        else:
            found.update(re.findall(r"[ABCDEH](?=楼|栋)", part))
    return found


def summarize_staff(rows: Sequence[Dict[str, Any]], actor: Dict[str, Any], *, observed_at: Any, source_url: Any) -> Dict[str, Any]:
    allowed = set(actor.get("scopes") or ())
    if not allowed or not allowed <= set(SCOPES):
        raise AssistantError("当前账号未配置可查询的楼栋范围。", 403)
    global_cover = allowed == set(SCOPES)
    candidates = [r for r in rows if isinstance(r, dict) and
                  ((rc := building_codes(r.get("building"))) and rc <= allowed or not rc and global_cover)]

    grouped: Dict[str, List[Dict]] = {}
    for row in candidates:
        rid = str(row.get("record_id") or "").strip()
        if not rid:
            raise AssistantError("人员快照存在缺少记录ID的记录，无法统计。")
        grouped.setdefault(rid, []).append(row)

    projected = ("name", "employee_no", "open_id", "building", "inactive")
    records, record_id_duplicates = [], 0
    for rid, group in grouped.items():
        sig = tuple(group[0].get(k) for k in projected)
        if any(tuple(r.get(k) for k in projected) != sig for r in group[1:]):
            raise AssistantError("人员快照中记录ID字段不一致，快照不完整。")
        records.append(group[0])
        record_id_duplicates += len(group) - 1

    def ident(row):
        oid = str(row.get("open_id") or "").strip()
        return "open:" + oid if oid else "record:" + str(row.get("record_id"))

    active, unknown_status_count = [], 0
    for row in records:
        if row.get("inactive") is True:
            continue
        elif row.get("inactive") is False:
            active.append(row)
        else:
            unknown_status_count += 1

    ids = [ident(r) for r in active]
    raw_active_records = len(active)
    unique_people = len(set(ids))
    open_groups = {}
    for oid in (str(r.get("open_id") or "").strip() for r in active):
        if oid:
            open_groups[oid] = open_groups.get(oid, 0) + 1
    duplicate_record_count = sum(n - 1 for n in open_groups.values())
    duplicate_open_id_count = sum(1 for n in open_groups.values() if n > 1)

    pb: Dict[str, set] = {}
    missing_building_count = 0
    for r, i in zip(active, ids):
        bc = building_codes(r.get("building"))
        if not bc:
            missing_building_count += 1
        else:
            for c in bc:
                pb.setdefault(c, set()).add(i)
    per_building = {c: len(v) for c, v in sorted(pb.items())}

    missing_employee_no = sum(1 for r in active if not str(r.get("employee_no") or "").strip())
    name_ids = {}
    for r, i in zip(active, ids):
        n = str(r.get("name") or "").strip()
        if n:
            name_ids.setdefault(n, set()).add(i)
    ambiguous_name_duplicates = sum(1 for s in name_ids.values() if len(s) > 1)

    warnings = []
    if missing_employee_no:
        warnings.append(f"有 {missing_employee_no} 条有效记录缺少员工工号。")
    if ambiguous_name_duplicates:
        warnings.append(f"有 {ambiguous_name_duplicates} 个重名人员（同姓名对应多个不同身份）。")

    def label(c):
        return "110站" if c == "110" else c + "楼"

    basis = (f"本次共统计在职人员 {unique_people} 人"
             f"（去重后原始记录 {raw_active_records} 条；相同记录ID合并 {record_id_duplicates} 条，同名/同工号不合并。")
    if duplicate_record_count:
        basis += f"通过同样的 open_id 识别出 {duplicate_record_count} 条重复记录。"
    elif duplicate_open_id_count:
        basis += f"通过同样的 open_id 识别出 {duplicate_open_id_count} 个重复身份。"
    basis += "）"
    if per_building:
        basis += "楼栋分布：" + "，".join(f"{label(c)} {n} 人" for c, n in per_building.items()) + "。"
    else:
        basis += "楼栋分布：无。"
    if unknown_status_count:
        basis += f"另有 {unknown_status_count} 条未明确在职状态的记录未纳入统计。"
    if missing_building_count:
        basis += f"其中无楼栋归属（未分配）的记录共 {missing_building_count} 条。"
    if warnings:
        basis += "；".join(warnings)

    return {
        "observed_at": observed_at, "source_url": source_url, "total": unique_people,
        "unique_people": unique_people, "raw_active_records": raw_active_records,
        "record_id_duplicates": record_id_duplicates, "duplicate_record_count": duplicate_record_count,
        "duplicate_open_id_count": duplicate_open_id_count, "unknown_status_count": unknown_status_count,
        "missing_building_count": missing_building_count, "missing_employee_no": missing_employee_no,
        "ambiguous_name_duplicates": ambiguous_name_duplicates, "per_building": per_building,
        "warnings": warnings, "basis": basis,
    }
