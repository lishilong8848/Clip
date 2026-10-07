"""Bounded local-cache retrieval. No cloud calls, file traversal or SQL from AI."""
import base64
import json
import difflib
import hashlib
import re
import sqlite3
import time
import unicodedata
from contextlib import closing
from pathlib import Path
from urllib.parse import urlencode

from .lighthouse_ai import AssistantError, PENDING_QUERY, is_business_query, safe_data, safe_text

SCOPES = frozenset({"110", "A", "B", "C", "D", "E", "H"})
MODULES = [
    ("通告与事件", "/?entry=notice"), ("维修单与跟进", "/?entry=repair_management"),
    ("机柜上下电", "/cabinet-power"), ("水耗管理", "/?entry=water"),
    ("维护单", "/engineer/mop"), ("SOP与工单", "/?entry=tools"),
    ("演练", "/drill-management"), ("日常工作", "/?entry=daily"),
    ("计划收敛审查", "/plan-convergence"), ("题库资料", "/learning"),
    ("重保管理", "/critical-guard"), ("人员与签名管理", "/signature-management"),
    ("通告历史", "/history-memory"), ("参考人生指南", "/life-guide"),
]
MODULE_HELP = {
    "通告与事件": "计划事项、解析粘贴和手填通告；开始、更新、结束、源表及目标记录绑定、现场图片、事件转检修。",
    "维修单与跟进": "维修项目、关联事件与检修通告、CMDB设备、维修进度、设备资料、费用、备件和跟进记录。",
    "机柜上下电": "各楼平面图、机柜台账、上下电操作历史、批量识别、逐柜证明、待办确认及回退、五楼月度归档和本地导出。",
    "水耗管理": "各楼水耗记录、图片、查询、修改及汇总。",
    "维护单": "维护单填写、人员姓名占位、正式文件签名、文件生成和通告关联。",
    "SOP与工单": "维保、轮巡和调整SOP；查询工单步骤、状态、延时提醒及结束条件，工单执行继续使用原入口。",
    "演练": "演练模板、楼栋任务、步骤签名人员、审核人和评估人、独立时间、网页姓名占位及正式Excel生成。",
    "日常工作": "楼栋日常任务、工作报告、晨会和事项状态。",
    "计划收敛审查": "智航认证、核对台、规则配置、本地规则库和当前未结束检修通告核对。",
    "题库资料": "只读查询笔试、值班工程师和专业工程师题库，沿用原题目和答案可见权限；不办理画像学练流程。",
    "画像学练": "只查询原权限内今日题单、未答数量、答题进度和历史资料；未发布与待完成分开说明，答题、发布和编辑继续使用原页面。",
    "重保管理": "重保任务、按楼检查表、填写和提交情况、天气相关检查。",
    "人员与签名管理": "签名管理全部业务仅使用原页面，不在助手办理；其他业务表单可选择签名人员，签名图像、身份证、住址、联系方式和凭证不可提供。",
    "通告历史": "查询已结束通告及历史操作；活动事项状态以当前缓存为准。",
    "参考人生指南": "首页参考人生指南入口，查看项目内的参考页面。",
}
DOCS = {"polling_sop": ("SOP", "/?entry=tools", True),
        "polling_work_order": ("工单", "/?entry=tools", False),
        "daily_work_report": ("日常工作", "/?entry=daily", False),
        "daily_morning_meeting": ("晨会", "/?entry=daily", False),
        "drill_definition": ("演练模板", "/drill-management", True),
        "drill_execution": ("演练", "/drill-management", False)}
REPAIR = {"repair_projects": "维修单", "repair_followups": "维修跟进", "repair_events": "事件",
          "repair_notices": "检修通告", "repair_cmdb": "CMDB设备",
          **{"notice_target." + kind: label for kind, label in
             (("maintenance", "维保通告"), ("change", "变更通告"), ("polling", "轮巡通告"),
              ("adjust", "调整通告"), ("power", "上下电通告"))}}


def codes(value):
    if isinstance(value, dict):
        return set().union(*(codes(v) for k, v in value.items() if k in {"text", "name", "value"}))
    if isinstance(value, (list, tuple, set)):
        return set().union(*(codes(v) for v in value)) if value else set()
    text = str(value or "").strip().upper()
    if text in {"ALL", "全部楼栋", "全楼"}:
        return set(SCOPES)
    if text in {"CAMPUS", "园区", "ABCDE", "园区（ABCDE楼）"}:
        return set("ABCDE")
    if text in SCOPES:
        return {text}
    found = set(re.findall(r"(?<![A-Z0-9])([ABCDEH])(?=楼|栋|[-_])", text))
    if "110站" in text or "110KV" in text:
        found.add("110")
    return found


SCOPE_FIELDS = {"scope", "scope_code", "building", "building_codes", "scope_codes", "scopes", "楼栋", "所属楼栋", "建筑名称", "area", "站点", "机楼", "所属数据中心/楼栋-使用", "所属数据中心/楼栋（关联CMDB唯一ID关联,DE不选）"}


def record_codes(item):
    result = set()
    if not isinstance(item, dict):
        return result
    for key in SCOPE_FIELDS:
        result.update(codes(item.get(key)))
    for key in ("fields", "display_fields", "form", "data"):
        result.update(record_codes(item.get(key)))
    return result


def scope_hint(item):
    if not isinstance(item, dict):
        return False
    return any(item.get(k) for k in SCOPE_FIELDS) or any(scope_hint(item.get(k)) for k in ("fields", "display_fields", "form", "data"))


def project_payload(item):
    if isinstance(item.get("display_fields"), dict):
        return {k: item[k] for k in ("record_id", "title", "scope", "work_type", "notice_type", "status") if k in item} | {"fields": item["display_fields"]}
    return {k: v for k, v in item.items() if k != "raw_fields"}


def record_title(item, fallback="事项"):
    fields = item.get("display_fields") or item.get("fields") or {}
    return next((str(data[k]) for data in (item, fields) for k in ("title", "name", "名称", "标题", "维修名称", "维护总项", "名称（标题）", "标题（名称）", "事件简述", "故障现象", "设备名称", "room") if data.get(k)), fallback)


def unfinished(status):
    from lan_bitable_template_portal.portal_service import MaintenancePortalService
    text = str(status or "").strip()
    if not text or text.lower() in {"completed", "cancelled", "deleted", "synced", "submitted", "stopped", "archived"} or any(t in text for t in ("取消", "作废", "删除")):
        return False
    return not MaintenancePortalService._target_status_is_finished(text)


def query_terms(query):
    query = re.sub(r"[ABCDEH](?:楼|栋)|南通|园区|110站", " ", query, flags=re.I)
    parts = re.findall(r"[A-Za-z0-9]+(?:[-_.][A-Za-z0-9]+)+|[A-Z]\d{2}|\d{3}|[\u4e00-\u9fff]{2,}", query)
    result = []
    for part in parts:
        part = re.sub(r"未完成|未结束|未闭环|待处理|待跟进|待办|剩余|现在|请问|请|查看|查找|帮我|有哪些|有那些|那些|哪些|还有|有多少|多少|目前|当前|一下|怎么|如何|什么|情况|查询|所有|全部|记录|灯塔|详细|统计|工作|任务|事项|清单|的|了", " ", part)
        for term in part.split():
            if len(term) > 1 and term not in result:
                result.append(term)
                if len(term) > 2 and re.fullmatch(r"[\u4e00-\u9fff]+", term):
                    result.extend(term[n:n + 2] for n in range(len(term) - 1))
    return list(dict.fromkeys(result))[:12]


def question_bank(service, actor, query):
    """Read native question visibility without revealing answers or changing study state."""
    from lan_bitable_template_portal.learning import LearningError

    allowed = set(actor.get("scopes") or []) & set(actor.get("learning_scopes") or [])
    if not actor.get("is_admin") and not allowed:
        raise AssistantError("当前账号没有题库查询权限，请使用原有学练授权账号。", 403)
    requested = str(query.get("scope") or "ALL").upper()
    selected = codes(requested)
    if not selected or (requested not in {"ALL", "CAMPUS"} and selected - allowed):
        raise AssistantError("无权查询所选楼栋的题目。", 403)
    if not actor.get("is_admin") and not selected & allowed:
        raise AssistantError("所选楼栋不在题目查询权限内。", 403)
    if service is None:
        raise AssistantError("题库本地缓存尚未就绪，请在画像学练原页面完成初始化。", 503)
    try:
        page, size = int(query.get("page", 1)), int(query.get("page_size", 20))
        if page < 1 or not 1 <= size <= 40:
            raise ValueError()
    except (TypeError, ValueError):
        raise AssistantError("题库分页参数无效，每页须为1至40条。") from None
    filters = {key: str(query.get(key) or "").strip() for key in ("search", "bank")}
    if len(filters["search"]) > 200 or filters["bank"] not in {"", "written", "duty", "professional"}:
        raise AssistantError("题库筛选参数无效。")
    terms = list(dict.fromkeys(unicodedata.normalize("NFKC", term).casefold()
                              for term in re.split(r"[\s,，;；、]+", filters["search"]) if len(term) > 1))
    if filters["search"] and (not terms or len(terms) > 8):
        raise AssistantError("请使用1至8个检索词，每词至少2个字符，以空格分隔。")
    fields = ("id", "version", "bank", "stem", "type", "type_label", "options", "topic",
              "specialty", "difficulty", "invalid", "correction", "attachments", "answer")
    try:
        sync = service.sync_status()
        deadline = time.monotonic() + 3
        unique = {}
        with closing(sqlite3.connect(Path(service.db).resolve().as_uri() + "?mode=ro", uri=True, timeout=.1)) as conn:
            conn.set_progress_handler(lambda: time.monotonic() >= deadline, 2000)
            conn.execute("BEGIN")
            if actor.get("is_admin"):
                service._admin(actor)
                fields += ("status", "correct_option_ids", "answer_text", "analysis", "hint")
                rows = conn.execute("SELECT payload FROM documents WHERE kind='question' "
                                    "AND COALESCE(json_extract(payload,'$.status'),'')!='deleted' ORDER BY updated DESC,key")
                for row in rows:
                    if time.monotonic() >= deadline:
                        raise sqlite3.OperationalError("query budget exceeded")
                    q = service.admin_question(json.loads(row[0]))
                    unique[q["id"], q["version"]] = {key: q[key] for key in fields if key in q}
            else:
                scopes = sorted(selected & allowed)
                for scope in scopes:
                    service._scope({**actor, "scope": scope}, scope)
                rows = conn.execute("SELECT p.payload,r.payload FROM documents p LEFT JOIN documents r "
                    "ON r.kind='record' AND r.key=p.key WHERE p.kind='paper' "
                    "AND COALESCE(json_extract(p.payload,'$.deleted_at'),'')='' "
                    "AND json_extract(p.payload,'$.scope') IN (" + ",".join("?" for _ in scopes) + ") "
                    "ORDER BY json_extract(p.payload,'$.date') DESC,p.key", scopes)
                for row in rows:
                    if time.monotonic() >= deadline:
                        raise sqlite3.OperationalError("query budget exceeded")
                    paper = json.loads(row[0])
                    public = service.public_paper(paper, {**actor, "scope": paper["scope"]}, record=json.loads(row[1] or "{}"))
                    for q in public["questions"]:
                        item = unique.setdefault((q["id"], q["version"]), {key: q[key] for key in fields if key in q})
                        # A previously opened answer stays visible for that same assigned version.
                        if q.get("answer"):
                            item["answer"] = {**item.get("answer", {}), **q["answer"]}
        items = [item for item in unique.values() if not filters["bank"] or item["bank"] == filters["bank"]]
        result = {"items": items, "total": len(items), "page": page, "page_size": size}
    except LearningError as exc:
        raise AssistantError(str(exc), exc.status) from None
    except sqlite3.OperationalError:
        raise AssistantError("题库查询超时或缓存暂忙，尚未取得完整结果，请稍后重试。", 503) from None
    if terms:
        ranked = []
        for item in result["items"]:
            if time.monotonic() >= deadline:
                raise AssistantError("题库候选检索超时，尚未取得完整结果，请缩小关键词范围后重试。", 503)
            title = unicodedata.normalize("NFKC", str(item.get("stem") or "") + " " + str(item.get("topic") or "")).casefold()
            parts = [title, *(str(item.get(key) or "") for key in ("answer_text", "analysis", "hint", "specialty")),
                     *(str(option.get("text") or "") for option in item.get("options") or []),
                     *(str((item.get("answer") or {}).get(key) or "") for key in ("answer_text", "analysis", "hint"))]
            content = unicodedata.normalize("NFKC", " ".join(parts)).casefold()
            score = sum((4 if term in title else 1) * len(term) for term in terms if term in content)
            for term in terms:
                if term not in content and re.fullmatch(r"[\u4e00-\u9fff]{3,20}", term):
                    matched = sum(block.size for block in difflib.SequenceMatcher(None, term, title, autojunk=False).get_matching_blocks())
                    if matched >= 2 and matched * 3 >= len(term) * 2:
                        score += matched
            if score:
                ranked.append((score, item))
        result["items"] = [item for _, item in sorted(ranked, key=lambda pair: -pair[0])]
        result["total"] = len(ranked)
    result["items"] = result["items"][(page - 1) * size:page * size]
    for item in result["items"]:
        media = ((item.get("answer") or {}).get("attachments") or []) + [a for a in item.get("attachments") or [] if a.get("kind") == "answer"]
        files = [*(item.get("attachments") or []), *((item.get("answer") or {}).get("attachments") or [])]
        item["materials"] = list({entry["id"]: {key: entry[key] for key in ("id", "name", "kind") if key in entry}
                                  for entry in files if entry.get("id")}.values())
        if media:
            item["reference_note"] = "参考答案包含图示，助手尚未读取图示内容，可读取资料文字核对。"
    if not result["total"] and not sync.get("updated_at"):
        raise AssistantError("题库缓存尚未完成初始化，不能确认题目数量。", 503)
    result.update(source="题库本地缓存", updated_at=sync.get("updated_at", ""),
                  match_note="按关键词及近似措辞召回候选，须结合原题核对语义，不代表完全相同的题目。" if terms else "",
                  scope_note="题库为通用资料，不按楼栋划分。" if actor.get("is_admin") else "只读取当前值班账号原本可见的题目。",
                  visibility="管理员完整题库" if actor.get("is_admin") else "原本可见的题目及已开放答案，不含未开放答案",
                  warning="题库正在同步或尚未完成同步，以下是已有缓存。" if sync.get("status") in {"syncing", "error"} or not sync.get("updated_at") else "")
    return result


def question_material_file(service, actor, query):
    """The portal authorizes the native file; only the assistant extracts it."""
    from lan_bitable_template_portal.learning import LearningError
    from .lighthouse_files import ALLOWED_EXTENSIONS, MAX_FILE_BYTES

    allowed = set(actor.get("scopes") or []) & set(actor.get("learning_scopes") or [])
    if not actor.get("is_admin") and not allowed:
        raise AssistantError("当前账号没有题目资料查询权限。", 403)
    scope = str(query.get("scope") or "ALL").upper()
    if not codes(scope):
        raise AssistantError("题目资料查询范围无效。")
    chosen = codes(scope) & allowed if scope in {"ALL", "CAMPUS"} else codes(scope)
    if not actor.get("is_admin") and (not chosen or chosen - allowed):
        raise AssistantError("无权读取所选楼栋的题目资料。", 403)
    identity = str(query.get("material_id") or "").strip()
    if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", identity):
        raise AssistantError("请选择题目查询结果中的资料。")
    try:
        offset, length = int(query.get("offset", 0)), int(query.get("length", 4000))
        if offset < 0 or not 1 <= length <= 6000:
            raise ValueError()
    except (TypeError, ValueError):
        raise AssistantError("资料读取范围无效，每次最多6000字。") from None
    if service is None:
        raise AssistantError("题库本地缓存尚未就绪。", 503)
    native_actor = {**actor, "scope": "" if actor.get("is_admin") else next(iter(sorted(chosen)))}
    try:
        meta = service._get("attachment", identity)
        if not meta or not meta.get("question_id") or meta.get("issue_id"):
            raise AssistantError("题目资料不存在。", 404)
        service._attachment_allowed(meta, native_actor)
        suffix = Path(str(meta.get("name") or "")).suffix.lower()
        if suffix not in ALLOWED_EXTENSIONS:
            raise AssistantError("该格式暂不能提取文字，请在原学练页面查看。", 422)
        path, name, mime = service.attachment(identity, native_actor)
        if path.stat().st_size > MAX_FILE_BYTES:
            raise AssistantError("题目资料超过20MiB读取上限。", 413)
        content = path.read_bytes()
        if len(content) > MAX_FILE_BYTES:
            raise AssistantError("题目资料超过20MiB读取上限。", 413)
    except LearningError as exc:
        raise AssistantError(str(exc), exc.status) from None
    except (OSError, sqlite3.Error):
        raise AssistantError("题目资料读取未完成或本地缓存暂忙，请稍后重试。", 503) from None
    return {"material_id": identity, "name": safe_text(name), "kind": meta.get("kind"), "mime": mime,
            "offset": offset, "length": length, "suffix": suffix,
            "content_base64": base64.b64encode(content).decode('ascii'),
            "digest": hashlib.sha256(suffix.encode() + b"\0" + content).hexdigest()}


def question_material_text(store, source):
    from .lighthouse_files import ALLOWED_EXTENSIONS, MAX_FILE_BYTES, extract_text
    try:
        suffix = source['suffix']
        offset, length = source['offset'], source['length']
        if (suffix not in ALLOWED_EXTENSIONS or type(offset) is not int or offset < 0
                or type(length) is not int or not 1 <= length <= 6000
                or not isinstance(source['content_base64'], str)
                or len(source['content_base64']) > (MAX_FILE_BYTES + 2) // 3 * 4):
            raise ValueError()
        content = base64.b64decode(source['content_base64'], validate=True)
        digest = hashlib.sha256(suffix.encode() + b"\0" + content).hexdigest()
        if len(content) > MAX_FILE_BYTES or digest != source['digest']:
            raise ValueError()
    except (KeyError, TypeError, ValueError):
        raise AssistantError("题目资料返回不完整，未使用不完整数据。", 502) from None
    try:
        cached = store.get_document("lighthouse_question_text", digest)
        if cached is None:
            try:
                text = extract_text(content, suffix)
            except AssistantError:
                raise
            except Exception:
                raise AssistantError("题目资料文字提取未完成，请在原学练页面核对文件。", 422) from None
            if not text.strip():
                raise AssistantError("未提取到可读文字，请在原学练页面核对资料。", 422)
            cached = {"text": text}
            store.put_document("lighthouse_question_text", digest, cached)
        text = cached["text"]
    except (OSError, sqlite3.Error):
        raise AssistantError("题目资料读取未完成或本地缓存暂忙，请稍后重试。", 503) from None
    return {"material_id": source['material_id'], "name": source['name'], "kind": source['kind'], "text": text[offset:offset + length],
            "offset": offset, "next_offset": offset + length if offset + length < len(text) else None,
            "source": "题库原资料", "reading_method": "图片文字识别；图形含义须核对原图。" if source['mime'].startswith("image/") else "文档文字提取"}


def question_material(service, store, actor, query):
    return question_material_text(store, question_material_file(service, actor, query))


def work_order_records(store, actor, query):
    """Project stored order progress without execution tokens, photos or signer credentials."""
    from lan_bitable_template_portal.polling_work_orders import PollingWorkOrderService

    allowed = set(actor.get("scopes") or []) & SCOPES
    scope = str(query.get("scope") or "ALL").upper()
    selected = codes(scope)
    if not allowed or not selected or (scope not in {"ALL", "CAMPUS"} and selected - allowed):
        raise AssistantError("无权查询所选楼栋的工单。", 403)
    selected &= allowed
    if not selected:
        raise AssistantError("所选范围内没有可查询的工单楼栋。", 403)
    state = str(query.get("state") or "all")
    work_type = str(query.get("work_type") or "")
    search = str(query.get("search") or "").strip().casefold()
    identity = str(query.get("group_id") or "").strip()
    if state not in {"all", "pending", "active", "upload_pending", "completed", "cancelled", "stopped"} or work_type not in {"", "maintenance", "polling", "adjust"} or len(search) > 200 or len(identity) > 200:
        raise AssistantError("工单查询参数无效。")
    try:
        page, size = int(query.get("page", 1)), int(query.get("page_size", 20))
        if page < 1 or not 1 <= size <= 40:
            raise ValueError()
    except (TypeError, ValueError):
        raise AssistantError("工单分页参数无效，每页须为1至40条。") from None
    if identity:
        item = store.get_document("polling_work_order", identity)
        if not isinstance(item, dict):
            raise AssistantError("工单不存在。", 404)
        if not record_codes(item) or record_codes(item) - allowed or not record_codes(item) & selected:
            raise AssistantError("无权查询该工单。", 403)
        documents = [{"key": identity, "payload": item}]
    else:
        documents = store.list_documents("polling_work_order")
    result = []
    for document in documents:
        item = document.get("payload") or {}
        found = record_codes(item)
        if not found or found - allowed or not found & selected or item.get("deleted_at"):
            continue
        if state == "pending" and item.get("state") not in {"active", "upload_pending"} or state not in {"all", "pending"} and item.get("state") != state:
            continue
        if work_type and item.get("work_type", "polling") != work_type:
            continue
        if search and search not in (str(item.get("title") or "") + " " + str(item.get("sop_name") or "") + " " + str(document.get("key") or "")).casefold():
            continue
        steps = item.get("steps") or []
        row = {key: item.get(key) for key in ("title", "scope", "work_type", "sop_name", "state", "created_at", "updated_at")}
        row.update(group_id=str(item.get("group_id") or document.get("key")),
                   target_record_id=str(item.get("target_record_id") or document.get("key")),
                   step_count=len(steps), completed_steps=sum(PollingWorkOrderService._step_done(step) for step in steps),
                   delay_status=PollingWorkOrderService.end_delay_status(item))
        row["uploaded_file_count"] = len(item.get("uploaded_file_tokens") or [])
        row["upload_completed"] = item.get("state") == "completed"
        current = int(item.get("current_index") or 0)
        row["current_step_number"] = current + 1 if item.get("state") == "active" and 0 <= current < len(steps) else None
        row["current_run_index"] = PollingWorkOrderService._selected_run_index(item, steps)
        for role in ("operator", "reviewer"):
            person = item.get(role) or {}
            row[role] = {key: person[key] for key in ("name", "employee_no", "staff_no") if key in person}
        runs = item.get("runs") or []
        row["runs"] = [{"run_index": index,
                        **{key: runs[index - 1].get(key) for key in ("from_unit", "to_unit") if 1 <= index <= len(runs)},
                        "step_count": len(run_steps),
                        "completed_steps": sum(PollingWorkOrderService._step_done(step) for step in run_steps)}
                       for index in sorted({int(step.get("run_index") or 0) for step in steps})
                       for run_steps in [[step for step in steps if int(step.get("run_index") or 0) == index]]]
        if identity:
            row["steps"] = []
            for index, step in enumerate(steps):
                public = PollingWorkOrderService._step_public({**step, "global_index": index}, current)
                row["steps"].append({"number": index + 1, **{key: step.get(key) for key in (
                    "run_index", "run_label", "step_index", "source_step_index", "repeat_round", "content",
                    "operator_required", "reviewer_required", "delay_reminder_minutes")},
                    "operator_confirmed": bool(step.get("operator_confirmation")), "reviewer_confirmed": bool(step.get("reviewer_confirmation")),
                    "confirmations": {role: {key: safe_text(str((step.get(role + "_confirmation") or {}).get(key) or ""))
                                            for key in ("assigned_name", "actual_name", "confirmed_at")}
                                      for role in ("operator", "reviewer")},
                    "completed": PollingWorkOrderService._step_done(step),
                    "activated": float(step.get("activated_at_ts") or 0) > 0,
                    **{key: public.get(key) for key in ("position", "time_limit_seconds", "timer_started", "confirm_available_at", "remaining_seconds")},
                    "photo_required": PollingWorkOrderService._step_photo_required(step), "photo_count": len(step.get("photos") or []),
                    "delay_status": PollingWorkOrderService.end_delay_status({"steps": [step]})})
        result.append(row)
    result.sort(key=lambda item: (str(item.get("created_at") or ""), item["group_id"]), reverse=True)
    return {"items": result[(page - 1) * size:page * size], "total": len(result), "page": page, "page_size": size,
            "source": "本地工单状态", "scopes": sorted(selected), "execution_available": False}


class LocalAssistantSources:
    def __init__(self, store, *, root=None):
        self.store = store
        self.root = Path(root) if root is not None else Path(store.db_path).parent

    @staticmethod
    def _read(path, sql, params=()):
        if not path.is_file():
            return []
        # Large cached tables have a hard query budget, independent of SQLite locks.
        deadline = time.monotonic() + .6
        try:
            with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=.1)) as conn:
                conn.row_factory = sqlite3.Row
                conn.set_progress_handler(lambda: time.monotonic() >= deadline, 2000)
                return [dict(row) for row in conn.execute(sql, params)]
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):
                return []
            raise AssistantError("部分本地资料暂忙，未使用不完整数据回答，请稍后重试。", 503) from None

    def __call__(self, query, actor):
        if not is_business_query(query):
            return [], []
        allowed = set(actor["scopes"])
        requested = codes(query)
        if requested - allowed:
            raise AssistantError("当前账号无权查询问题中涉及的楼栋。", 403)
        scopes = sorted(requested or allowed)
        terms = query_terms(query)
        if PENDING_QUERY.search(query) and not re.search(r"为什么|原因|怎么|如何|建议|删除|修改|设置|why|how", query, re.I):
            return self._pending(query, actor, scopes, terms)
        hits = []
        warnings = []
        def read(path, sql, params=()):
            try:
                return self._read(path, sql, params)
            except AssistantError:
                message = "部分本地资料读取超时或暂不可用，本轮仅使用已成功读取的资料，不能据此计算全量数量。"
                if message not in warnings:
                    warnings.append(message)
                return []

        def add(label, payload, href, authoritative=None, shared=False, priority=0):
            if not isinstance(payload, dict):
                return
            found = record_codes(payload) or set(authoritative or [])
            if found - allowed or (not found and (not shared or scope_hint(payload))):
                return
            if payload.get("deleted_at") or payload.get("status") in {"deleted", "已删除"}:
                return
            clean = safe_data(project_payload(payload))
            content = json.dumps(clean, ensure_ascii=False)
            score = sum(4 if len(t) > 3 else 1 for t in terms if t.casefold() in (label + content).casefold())
            if not score:
                return
            title = record_title(clean, label)
            hits.append({"title": label + " · " + title[:100], "url": href, "scopes": sorted(found),
                         "data": clean, "_score": score + priority, "_size": len(content)})

        db = Path(self.store.db_path)
        wildcard = " OR ".join("payload_json LIKE ? ESCAPE '\\'" for _ in terms) or "0"
        values = tuple("%" + t.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%" for t in terms)
        placeholders = ",".join("?" for _ in scopes)
        allowed_list = sorted(allowed)
        allowed_placeholders = ",".join("?" for _ in allowed_list)
        def scoped_sql(column, shared=False):
            return ("NOT EXISTS (SELECT 1 FROM json_each(" + column + ") WHERE value NOT IN (" + allowed_placeholders + ")) AND "
                    "(EXISTS (SELECT 1 FROM json_each(" + column + ") WHERE value IN (" + placeholders + "))"
                    + (" OR json_array_length(" + column + ")=0" if shared else "") + ")")
        # Only the active per-source plan snapshots, never stale historical snapshots.
        rows = read(db, "SELECT payload_json,scope,work_type FROM source_table_snapshot_records "
            "WHERE snapshot_id IN (SELECT snapshot_id FROM source_table_snapshot_manifest WHERE status='active') "
            "AND scope IN (" + placeholders + ",'ALL','CAMPUS') AND (" + wildcard + ") LIMIT 80", (*scopes, *values))
        for row in rows:
            add("计划通告", json.loads(row["payload_json"]), "/workbench-lite?" + urlencode({"scope": row["scope"], "work_type": row["work_type"] or "maintenance"}), codes(row["scope"]))
        for row in read(db, "SELECT payload_json,building_codes_json FROM ongoing_items WHERE " + scoped_sql("building_codes_json") + " AND (" + wildcard + ") LIMIT 40", (*allowed_list, *scopes, *values)):
            item = json.loads(row["payload_json"])
            found = codes(json.loads(row["building_codes_json"]))
            add("进行中通告", item, "/workbench-lite?" + urlencode({"scope": next(iter(sorted(found)), scopes[0]), "work_type": item.get("work_type", "maintenance"), "active_item_id": item.get("active_item_id", "")}), found)
        # CMDB's blank-building records are intentionally shared by existing business rules.
        for kind, label in REPAIR.items():
            if kind == "repair_cmdb" and not re.search(r"CMDB|智航|设备|品牌|型号|唯一.?ID|柴发|柴油|冷水|冷机|制冷|水泵|HVDC|UPS", query, re.I):
                continue
            indexed_text = wildcard.replace("payload_json", "CASE WHEN search_text='' THEN payload_json ELSE search_text END")
            projected = "json_object('record_id',record_id,'title',title,'status',status,'fields',json(COALESCE(json_extract(payload_json,'$.display_fields'),json_extract(payload_json,'$.fields'),'{}'))) AS payload_json"
            rows = read(db, "SELECT " + projected + ",scope_codes_json,record_id FROM repair_snapshot_records "
                "WHERE source_key=? AND " + scoped_sql("scope_codes_json", kind == "repair_cmdb") + " AND (" + indexed_text + ") LIMIT 25", (kind, *allowed_list, *scopes, *values))
            for row in rows:
                found = codes(json.loads(row["scope_codes_json"]))
                href = "/repair-management" if kind.startswith("repair_") else "/workbench-lite"
                add(label, json.loads(row["payload_json"]), href + "?" + urlencode({"scope": next(iter(sorted(found)), scopes[0]), "record_id": row["record_id"]}), found, kind == "repair_cmdb")
        for namespace, (label, href, shared) in DOCS.items():
            for row in read(db, "SELECT payload_json FROM json_documents WHERE namespace=? AND (" + wildcard + ") LIMIT 20", (namespace, *values)):
                item = json.loads(row["payload_json"])
                if namespace == "daily_work_report":
                    for scope in scopes:
                        report = (item.get("reports") or {}).get(scope)
                        if report:
                            add(label, {"scope": scope, "title": scope + "楼工作报告", "report": report, "generated_at": item.get("generated_at")}, href, {scope})
                elif namespace == "daily_morning_meeting":
                    public = {k: item.get(k) for k in ("date", "file_name", "actor_name", "generated_at")}
                    if actor.get("is_admin"):
                        public["model"] = item.get("model")
                    add(label, public, href, shared=True)
                elif namespace == "drill_definition":
                    if codes(item.get("assigned_scopes")) & set(scopes) or actor.get("is_admin"):
                        add(label, {k: item.get(k) for k in ("name", "year", "month", "status", "configuration")}, href, shared=True)
                else:
                    add(label, item, href, shared=shared)
        for row in read(db, "SELECT payload_json FROM qt_active_items WHERE deleted_at IS NULL AND (" + wildcard + ") ORDER BY updated_at DESC LIMIT 30", values):
            item = json.loads(row["payload_json"])
            add("Qt通告", item, "/workbench-lite?" + urlencode({"scope": next(iter(sorted(record_codes(item))), scopes[0]), "work_type": item.get("work_type", "event")}))
        for row in read(db, "SELECT payload_json,scope_code FROM water_consumption_records WHERE scope_code IN (" + placeholders + ") AND (" + wildcard + ") LIMIT 20", (*scopes, *values)):
            add("水耗", json.loads(row["payload_json"]), "/water-management?scope=" + row["scope_code"], {row["scope_code"]})
        # Personnel records are projected to names/staff numbers only, never signature data.
        for row in read(db, "SELECT payload_json FROM json_documents WHERE namespace='signature_management_person' AND (" + wildcard + ") LIMIT 15", values):
            item = json.loads(row["payload_json"])
            found = record_codes(item)
            if found - allowed or (not found and scope_hint(item)):
                continue
            allowed_fields = {k: item[k] for k in ("name", "姓名", "employee_no", "staff_no", "工号", "scope", "building") if k in item}
            add("人员", allowed_fields, "/signature-management", found, shared=True)
        if any(t in query for t in ("机柜", "上电", "下电", "测试电", "正式电", "包间")) or re.search(r"[A-Z]\d{2}", query):
            self._cabinets(scopes, add, warnings)
        for row in read(self.root / "cabinet_power" / "batches.sqlite3", "SELECT payload_json,scopes_json,batch_id FROM batches WHERE status!='deleted' AND (" + wildcard + ") LIMIT 15", values):
            found = codes(json.loads(row["scopes_json"]))
            add("上下电待办", json.loads(row["payload_json"]), "/cabinet-power/batches?batch_id=" + row["batch_id"], found)
        for row in read(db, "SELECT payload_json,scope,notice_title,status FROM engineer_mop_local_files WHERE deleted_at IS NULL AND scope IN (" + placeholders + ") AND (" + wildcard + ") LIMIT 20", (*scopes, *values)):
            add("维护单", {**json.loads(row["payload_json"]), "title": row["notice_title"], "scope": row["scope"], "status": row["status"]}, "/engineer/mop?scope=" + row["scope"], {row["scope"]})
        task_filter = " OR ".join("task_name LIKE ?" for _ in terms) or "0"
        for row in read(db, "SELECT task_name,status,target_scopes_json,created_by_name FROM critical_guard_tasks WHERE " + scoped_sql("target_scopes_json") + " AND (" + task_filter + ") LIMIT 15", (*allowed_list, *scopes, *values)):
            add("重保任务", row, "/critical-guard", codes(json.loads(row["target_scopes_json"])))
        for row in read(db, "SELECT t.task_name,r.scope_code,r.status,r.check_date,r.cells_json FROM critical_guard_responses r JOIN critical_guard_tasks t ON t.task_id=r.task_id WHERE r.scope_code IN (" + placeholders + ") AND (" + task_filter + ") LIMIT 20", (*scopes, *values)):
            add("重保填写记录", {"scope": row["scope_code"], "title": row["task_name"], "status": row["status"], "check_date": row["check_date"], "cells": json.loads(row["cells_json"])}, "/critical-guard", {row["scope_code"]})
        rule_db = self.root / "plan_convergence" / "rule_sets.sqlite3"
        rule_filter = " OR ".join("(inst_name||' '||point_name||' '||rule_name||' '||COALESCE(obj_name,'')) LIKE ?" for _ in terms) or "0"
        for row in read(rule_db, "SELECT * FROM rule_set_item WHERE (" + rule_filter + ") LIMIT 20", values):
            add("收敛规则配置", row, "/plan-convergence", shared=True)
        catalog = self.root / "plan_convergence" / "catalog.sqlite3"
        device_filter = " OR ".join("(inst_name||' '||COALESCE(position,'')||' '||COALESCE(ins_standard_id,'')) LIKE ?" for _ in terms) or "0"
        if re.search(r"智航|收敛|测点|规则", query):
            for row in read(catalog, "SELECT inst_name,ins_standard_id,obj_name,classification_name,position FROM zh_device WHERE (" + device_filter + ") LIMIT 20", values):
                add("智航设备目录", row, "/plan-convergence", codes(str(row.get("inst_name", "")) + " " + str(row.get("position", ""))), shared=True)
            rule_filter = " OR ".join("(alarm_name||' '||COALESCE(rule_desc,'')) LIKE ?" for _ in terms) or "0"
            for row in read(catalog, "SELECT alarm_name,classify_model,rule_desc FROM zh_rules WHERE (" + rule_filter + ") LIMIT 20", values):
                add("智航告警规则", row, "/plan-convergence", shared=True)
        for row in read(db, "SELECT key,payload_json FROM json_documents WHERE namespace='cabinet_power' AND (" + wildcard + ") LIMIT 20", values):
            item = json.loads(row["payload_json"])
            add("机柜导出与设置", item, "/cabinet-power", shared=bool(actor.get("is_admin")))
        guide = {"title": "灯塔功能说明", "url": "/?entry=tools", "scopes": [], "data": {"模块": [{"名称": label, "说明": MODULE_HELP[label]} for label, _ in MODULES if not terms or any(t in label + MODULE_HELP[label] for t in terms)]}}
        if not guide["data"]["模块"]:
            guide["data"]["模块"] = [{"名称": label, "说明": MODULE_HELP[label]} for label, _ in MODULES]
        ranked = sorted(hits, key=lambda row: row["_score"], reverse=True)
        chosen, seen, size = [], set(), 0
        for hit in ranked:
            identity = json.dumps(hit["data"], ensure_ascii=False, sort_keys=True)
            if identity in seen:
                continue
            seen.add(identity)
            if size + hit["_size"] > 24000 or len(chosen) >= 12:
                continue
            size += hit.pop("_size")
            hit.pop("_score")
            chosen.append(hit)
        if not chosen:
            warnings.append("未检索到匹配的本地业务资料；可补充楼栋、名称、机柜号或设备编号。")
        elif len(ranked) > len(chosen):
            warnings.append("本次只使用部分相关记录，不代表完整数量统计。")
        chosen.append(guide)
        for n, hit in enumerate(chosen, 1):
            hit["number"] = n
        return chosen, warnings

    def _pending(self, query, actor, scopes, terms):
        """Read complete status sets, then scope/dedupe before counting or sampling."""
        db, allowed = Path(self.store.db_path), set(actor["scopes"])
        placeholders = ",".join("?" for _ in scopes)
        groups, warnings = [], []

        def rows(path, sql, params=()):
            if not path.is_file():
                return None
            try:
                return self._read(path, sql, params)
            except AssistantError:
                return None

        def visible(item, fallback=()):
            found = record_codes(item) or set(fallback)
            return bool(found & set(scopes)) and not found - allowed

        def group(label, items, href, available=True):
            if terms and not any(t in label for t in terms):
                items = [item for item in items if any(t.casefold() in json.dumps(item, ensure_ascii=False).casefold() for t in terms)]
                if not items:
                    return
            groups.append({"模块": label, "数量": len(items) if available else None,
                           "读取状态": "已读取" if available else "本地资料尚未初始化或暂不可用",
                           "明细": safe_data(items[:5]), "其余数量": max(0, len(items) - 5), "href": href})
            if not available:
                warnings.append(label + "资料尚未读取，未将它当作零条。")

        ongoing = rows(db, "SELECT payload_json,building_codes_json FROM ongoing_items")
        notices, seen = [], set()
        for row in ongoing or []:
            item = json.loads(row["payload_json"])
            if visible(item, codes(json.loads(row["building_codes_json"]))) and unfinished(item.get("status")):
                identity = item.get("record_id") or item.get("active_item_id")
                if identity in seen:
                    continue
                seen.add(identity)
                notices.append({"名称": record_title(item), "状态": item.get("status"), "楼栋": sorted(record_codes(item)), "进度": safe_text(item.get("progress", ""))[:180]})

        fields = "COALESCE(json_extract(payload_json,'$.display_fields'),json_extract(payload_json,'$.fields'),'{}')"
        status_fields = ("流程", "事件状态", "检修状态", "维保状态", "变更状态", "轮巡状态", "调整状态", "上电状态", "下电状态")
        status_sql = "COALESCE(NULLIF(status,'')," + ",".join("NULLIF(json_extract(" + fields + ",'$.\"" + key + "\"'),'')" for key in status_fields) + ",'')"
        keys = [key for key in REPAIR if key != "repair_cmdb" and key != "repair_followups"]
        records = rows(db, "SELECT source_key,record_id,title,scope_codes_json," + status_sql + " AS state FROM repair_snapshot_records WHERE source_key IN (" + ",".join("?" for _ in keys) + ")", keys)
        repairs, events = [], []
        for row in records or []:
            found = codes(json.loads(row["scope_codes_json"]))
            if not visible({}, found) or not unfinished(row["state"]):
                continue
            item = {"名称": row["title"], "状态": row["state"], "楼栋": sorted(found), "记录ID": row["record_id"]}
            if row["source_key"] == "repair_projects":
                repairs.append(item)
            elif row["source_key"] == "repair_events":
                events.append(item)
            elif row["record_id"] not in seen:
                seen.add(row["record_id"])
                notices.append(item)
        manifests = rows(db, "SELECT source_key,status FROM repair_snapshot_sources")
        initialized = {r["source_key"] for r in manifests or [] if r["status"] not in {"empty", "initializing"}}
        loaded = {r["source_key"] for r in records or []}
        group("进行中通告", notices, "/workbench-lite?scope=" + (scopes[0] if len(scopes) == 1 else "ALL"), ongoing is not None and records is not None)
        group("未完成维修项目", repairs, "/repair-management?scope=" + (scopes[0] if len(scopes) == 1 else "ALL"), records is not None and "repair_projects" in initialized | loaded)
        group("未闭环事件", events, "/event-management", records is not None and "repair_events" in initialized | loaded)

        plans = rows(db, "SELECT payload_json,scope,work_type FROM source_table_snapshot_records WHERE snapshot_id IN (SELECT snapshot_id FROM source_table_snapshot_manifest WHERE status='active')")
        pending_plans, seen_plans = [], set()
        for row in plans or []:
            item = json.loads(row["payload_json"])
            found = record_codes(item) or codes(row["scope"])
            if not visible(item, found):
                continue
            fields = item.get("display_fields") or item.get("fields") or {}
            if row["work_type"] == "repair":
                state = "已结束" if fields.get("维修结束时间") or fields.get("维修结束时间（2026）") else "进行中" if fields.get("维修开始时间") else "未开始"
            else:
                state = next((fields[k] for k in ("维护实施状态", "变更进度", "变更状态", "实施状态", "状态") if fields.get(k)), "未开始")
            identity = (row["work_type"], item.get("record_id"))
            if identity in seen_plans or not unfinished(state):
                continue
            seen_plans.add(identity)
            pending_plans.append({"名称": record_title(item), "状态": state, "楼栋": sorted(found), "计划时间": fields.get("计划开始维护时间") or fields.get("计划开始时间") or ""})
        group("待执行计划事项", pending_plans, "/workbench-lite?scope=" + (scopes[0] if len(scopes) == 1 else "ALL"), plans is not None and bool(plans))

        documents = rows(db, "SELECT namespace,key,payload_json FROM json_documents WHERE namespace IN ('polling_work_order','polling_sop','drill_definition','drill_execution')")
        docs = {ns: {} for ns in ("polling_work_order", "polling_sop", "drill_definition", "drill_execution")}
        for row in documents or []:
            docs[row["namespace"]][row["key"]] = json.loads(row["payload_json"])
        orders = []
        for item in docs["polling_work_order"].values():
            found = record_codes(item) or record_codes(docs["polling_sop"].get(item.get("sop_id"), {}))
            if visible(item, found) and unfinished(item.get("state")):
                orders.append({"名称": record_title(item), "状态": item["state"], "楼栋": sorted(found), "当前步骤": int(item.get("current_index") or 0) + 1, "总步骤": len(item.get("steps") or [])})
        group("SOP工单", orders, "/?entry=tools", documents is not None)
        drills = []
        for definition in docs["drill_definition"].values():
            if definition.get("status") != "published":
                continue
            for scope in sorted(codes(definition.get("assigned_scopes")) & set(scopes)):
                execution = docs["drill_execution"].get(str(definition.get("drill_id")) + ":" + scope, {})
                state = execution.get("status") or "未开始"
                if unfinished(state):
                    drills.append({"名称": definition.get("name", "演练"), "楼栋": scope, "状态": state})
        group("演练任务", drills, "/drill-management", documents is not None)

        batches = rows(self.root / "cabinet_power" / "batches.sqlite3", "SELECT batch_id,payload_json,scopes_json,status FROM batches WHERE status NOT IN ('deleted','cancelled','completed')")
        cabinet_tasks = []
        for row in batches or []:
            item = json.loads(row["payload_json"])
            if not visible({}, codes(json.loads(row["scopes_json"]))) or (item.get("source_notice") or {}).get("deleted_at"):
                continue
            active = [r for r in item.get("rows", []) if not r.get("notice_removed") and r.get("status") not in {"completed", "duplicate"} and not str(r.get("status", "")).startswith("excluded_")]
            if active or row["status"] in {"recognizing", "running", "failed"}:
                cabinet_tasks.append({"名称": (item.get("source_notice") or {}).get("title") or "批次 " + row["batch_id"][:8], "状态": row["status"], "楼栋": sorted(codes(json.loads(row["scopes_json"]))), "未确认机柜": len(active), "批次ID": row["batch_id"]})
        group("上下电待办", cabinet_tasks, "/cabinet-power/batches?status=todo", batches is not None)

        responses = rows(db, "SELECT t.task_name,r.scope_code,r.status,r.sheet_type FROM critical_guard_responses r JOIN critical_guard_tasks t ON t.task_id=r.task_id WHERE t.status='active' AND r.scope_code IN (" + placeholders + ")", scopes)
        guard = [{"名称": r["task_name"], "楼栋": r["scope_code"], "状态": r["status"], "表类型": r["sheet_type"]} for r in responses or [] if unfinished(r["status"])]
        group("重保检查表", guard, "/critical-guard", responses is not None)
        files = rows(db, "SELECT notice_title,scope,status FROM engineer_mop_local_files WHERE deleted_at IS NULL AND scope IN (" + placeholders + ")", scopes)
        group("维护单待处理文件", [{"名称": r["notice_title"], "楼栋": r["scope"], "状态": r["status"]} for r in files or [] if unfinished(r["status"]) and r["status"] != "uploaded"], "/engineer/mop", files is not None)
        overview = {"title": "未完成工作概览", "url": "/?entry=daily", "scopes": scopes, "number": 1, "data": {
            "查询楼栋": scopes, "口径": "本地最新状态；各模块可能关联同一工作，分组数量不可相加为独立工作总数。每组列出至多5条，完整内容可在对应页面查看。",
            "分组": [{k: v for k, v in g.items() if k not in {"明细", "href"}} for g in groups]}}
        result = [overview]
        for g in groups:
            if g["数量"]:
                result.append({"title": g["模块"], "url": g["href"], "scopes": scopes, "number": len(result) + 1, "data": {k: v for k, v in g.items() if k != "href"}})
        return result, warnings

    def _cabinets(self, scopes, add, warnings):
        from lan_bitable_template_portal.cabinet_power_data import from_feishu
        from lan_bitable_template_portal.cabinet_power_excel import derive_records
        for scope in scopes:
            if scope not in "ABCDE":
                continue
            path = self.root / "cabinet_power" / "buildings" / (scope + ".sqlite3")
            try:
                # One SELECT freezes metadata, directory and operations together.
                snapshot = self._read(path, "SELECT 'meta' AS kind,key,payload FROM meta WHERE key IN ('config','baseline') "
                    "UNION ALL SELECT 'inventory','',payload FROM inventory UNION ALL SELECT 'record','',payload FROM records")
            except AssistantError:
                warnings.append(scope + "楼机柜资料读取未完成，本轮未使用该楼机柜状态或数量。")
                continue
            metadata = {r["key"]: json.loads(r["payload"]) for r in snapshot if r["kind"] == "meta"}
            config = metadata.get("config")
            if not config:
                continue
            config["inventory"] = [json.loads(r["payload"]) for r in snapshot if r["kind"] == "inventory"]
            if not config["inventory"]:
                warnings.append(scope + "楼机柜目录暂不可用，本轮未使用该楼机柜状态或数量。")
                continue
            config["baseline_event_ids"] = metadata.get("baseline", [])
            operations = [from_feishu(json.loads(r["payload"])) for r in snapshot if r["kind"] == "record"]
            # Reuse the same baseline/state derivation as the actual floor plan.
            derived = derive_records(config, operations)
            href = "/cabinet-power?scope=" + scope
            add("机柜上下电库存汇总", {"scope": scope, "counts": derived["counts"]}, href, {scope}, priority=50)
            for rack in derived["racks"]:
                add("机柜", {"scope": scope, "title": rack["room"] + " / " + rack["rack"],
                    "last_successful_operation_time": rack.get("last_operation", ""),
                    **{k: rack.get(k) for k in ("room", "rack", "rack_type", "state", "state_source", "power")}}, href, {scope}, priority=12)
