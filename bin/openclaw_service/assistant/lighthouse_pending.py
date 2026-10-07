"""Assistant-only unfinished work projection; never changes business records."""
import asyncio
import datetime as dt
import html
import re
import time
from urllib.parse import urlencode

from .lighthouse_ai import AssistantError, safe_text
from .lighthouse_sources import SCOPES, codes, record_codes, record_title, unfinished

NOTICE_TYPES = {"maintenance": "维保", "change": "变更", "repair": "检修", "power": "上下电", "polling": "轮巡", "adjust": "调整"}
DONE = {"completed", "cancelled", "deleted", "synced", "submitted", "uploaded", "stopped", "archived"}
# 整轮查询与整体加载的时间预算。整组 loader 也被 bound，避免分页/多楼栋的早期
# 分组独占全部并发槽位直到整轮 deadline 才让位于 guard/learning。测试可 patch 成毫秒级。
ROUND_DEADLINE_SECONDS = 35
WHOLE_LOADER_BUDGET_SECONDS = 10
PER_CALL_TIMEOUT_SECONDS = 10
PER_CACHED_TIMEOUT_SECONDS = 10
# 共享的通告在途读取也有独立预算：它被 notices/plans 复用并经 shield 保护，
# 不能因为某个消费方 wait_for 超时就被取消，但也不能无限在途，始终 bound 在轮次剩余时间内。
SHARED_BOARD_BUDGET_SECONDS = WHOLE_LOADER_BUDGET_SECONDS
# 重保检查表响应的可访问楼栋(A-E)与已提交终态。
GUARD_SCOPES = frozenset("ABCDE")
GUARD_SUBMITTED = "submitted"
_GUARD_AUTO_CELL_KEYS = frozenset({"check_date", "check_date_updated", "template_revision", "template_customized"})


def _guard_unsubmitted(status):
    """重保响应尚未提交即视为待办(pending/draft), submitted 不计。"""
    return str(status or "").strip().lower() != GUARD_SUBMITTED


def _guard_value_filled(value):
    """0/False 也是有效填写值;只有空串/空白/None/空结构才算未填写。"""
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return True
    if isinstance(value, str):
        return value.strip() != ""
    if isinstance(value, dict):
        return any(_guard_value_filled(v) for k, v in value.items() if k not in _GUARD_AUTO_CELL_KEYS)
    if isinstance(value, (list, tuple, set)):
        return any(_guard_value_filled(v) for v in value)
    return True


def _guard_cells_filled(cells):
    """复用值判定：0/False 也算有效填写，系统自动键不计。"""
    return _guard_value_filled(cells)


def _cell_text(value):
    text = html.escape(safe_text(str(value or "—"))).replace("|", "&#124;").replace("\n", " ")
    return re.sub(r"([\\`*_\[\]()])", r"\\\1", text)


def pending_status(value):
    text = str(value or "").strip()
    if any(word in text for word in ("未结束", "待结束", "未完成", "待完成", "未闭环", "待闭环", "未提交", "待提交")):
        return True
    return text.lower() not in DONE and (not text or unfinished(text))


def visible(item, scopes, allowed=None):
    found = record_codes(item)
    return bool(found & set(scopes)) and not found - set(allowed if allowed is not None else scopes)


def project(item, scopes, *, title="事项", status="待处理", identity="", url=""):
    fields = item.get("display_fields") or item.get("fields") or {}
    return {"id": str(identity or item.get("target_record_id") or item.get("record_id") or item.get("active_item_id") or item.get("id") or ""),
            "title": safe_text(record_title(item, title))[:200], "scopes": sorted(record_codes(item) & set(scopes)),
            "status": safe_text(item.get("status_label") or item.get("workflow") or item.get("status") or item.get("state") or fields.get("流程") or status)[:80],
            "url": url,
            **{key: item[key] for key in ("record_id", "active_item_id", "target_record_id", "source_record_id", "batch_id", "drill_id") if item.get(key)}}


def _events_local_snapshot(store, service):
    """Serve events from an already-initialized local repair snapshot only.

    Mirrors the native _load_repair_snapshot_source local-serving branch: it
    validates the source app/table identity and workflow freshness, and it never
    performs a synchronous remote refresh. Missing/expired/no-record snapshots
    raise instead of inventing an empty count.
    """
    app_token, table_id, _source_key = service._event_source_config()
    snapshot = store.get_repair_snapshot("repair_events")
    if snapshot.get("records") and (
        str(snapshot.get("app_token") or "").strip() != str(app_token or "").strip()
        or str(snapshot.get("table_id") or "").strip() != str(table_id or "").strip()
    ):
        # Same guarded table-switch check the native loader performs.
        snapshot = {}
    if service._repair_snapshot_requires_workflow_refresh(snapshot, table_id):
        snapshot = {}
    from lan_bitable_template_portal.portal_service import (
        REPAIR_SNAPSHOT_SOURCE_EVENTS,
        REPAIR_SNAPSHOT_TTL_SECONDS,
    )
    refreshed_at = float(snapshot.get("refreshed_at") or 0)
    ttl = float(REPAIR_SNAPSHOT_TTL_SECONDS.get(REPAIR_SNAPSHOT_SOURCE_EVENTS) or 300)
    fresh = refreshed_at > 0 and time.time() - refreshed_at <= ttl
    if not fresh and not snapshot.get("records"):
        raise AssistantError(
            "事件本地快照尚未初始化或已过期且无记录，为避免远程同步已跳过重载；不能判断为零条。", 503
        )
    records = service._repair_snapshot_from_local(snapshot)[2]
    return records, {'kind': 'local_cache', 'last_cloud_sync_at': refreshed_at,
                     'includes_qt_changes': True}


def cached_items(kind, scopes, runtime, *, allowed=None):
    """Use native service/store methods behind the authenticated assistant API."""
    store, service = runtime.state_store, runtime.service
    if kind == "events":
        meta = store.get_repair_snapshot_meta("repair_events")
        if not meta.get("exists"):
            raise AssistantError("事件资料尚未初始化，不能判断为零条。", 503)
        records, freshness = _events_local_snapshot(store, service)
        items, seen = [], set()
        for raw in records:
            item = service._repair_management_event_item(raw)
            fields = item.get("display_fields") or {}
            # An event closed and transferred to repair is not an open event.
            statuses = [fields.get(key) for key in ("事件状态", "事件目前进展", "最终状态") if fields.get(key)]
            ended = any(not pending_status(state) for state in statuses)
            identity = item.get("record_id")
            if ended:
                seen.add(identity)
            if not ended and visible(item, scopes, allowed):
                seen.add(identity)
                row = project(item, scopes, title="事件", status="处理中")
                row["url"] = "/?" + urlencode({"scope": row["scopes"][0], "mode": "events"})
                items.append(row)
        # Unsent Qt changes remain pending; parse their original business fields,
        # not a daily task's abbreviated/possibly blank title.
        from upload_event_module.core.parser import is_notice_confirmed_ended
        for wrapper in store.list_visible_qt_active_items():
            payload = dict(wrapper.get("payload") or wrapper)
            if str(payload.get("notice_type") or wrapper.get("notice_type")) != "事件通告" or is_notice_confirmed_ended(payload):
                continue
            identity = wrapper.get("record_id") or payload.get("record_id") or wrapper.get("active_item_id")
            if identity in seen:
                continue
            item = service._repair_management_event_from_notice_payload(record_id=str(identity or ""), notice_data=payload, remote_fields={}, scope=str(payload.get("scope") or ""))
            item["building_codes"] = record_codes(payload) or item.get("building_codes") or []
            if visible(item, scopes, allowed):
                seen.add(identity)
                row = project(item, scopes, title="事件草稿", status="未上传" if not wrapper.get("record_id") else "处理中")
                row["url"] = "/?" + urlencode({"scope": row["scopes"][0], "mode": "events"})
                items.append(row)
        return items, [], {'source_freshness': [freshness]}
    if kind == "orders":
        result = []
        for document in store.list_documents("polling_work_order"):
            item = document.get("payload") or {}
            if visible(item, scopes, allowed) and pending_status(item.get("state")):
                row = project(item, scopes, title="SOP工单", identity=document.get("key"), url="/workbench-lite?" + urlencode({"scope": item.get("scope"), "work_type": item.get("work_type", "maintenance")}))
                row["status"] = "待完成" if item.get("state") == "active" else safe_text(item.get("state") or "待完成")
                result.append(row)
        return result, []
    if kind == "mops":
        # ponytail: native file-list limit; report truncation rather than invent a full count.
        files = store.list_engineer_mop_local_files(limit=2001)
        result = [project(item, scopes, title=str(item.get("notice_title") or "维护单文件"), identity=item.get("file_id"), url="/engineer/mop?" + urlencode({"scope": item.get("scope")}))
                  for item in files if visible(item, scopes, allowed) and pending_status(item.get("status"))]
        return result, ["维护单文件超过读取上限，数量为已读取下限。"] if len(files) >= 2001 else []
    raise AssistantError("未知的未完成工作类别。")


async def collect_pending(actor, query, invoke, read_cached, *, groups_only=None, notice_type="", on_progress=None):
    requested = codes(query)
    if re.search(r"园区|ABCDE|(?<![A-Z])CAMPUS(?![A-Z])", query, re.I):
        requested.update("ABCDE")
    if requested - set(actor["scopes"]):
        raise AssistantError("当前账号无权查询问题中涉及的楼栋。", 403)
    scopes = sorted(requested or set(actor["scopes"]))
    if groups_only == {'learning'} and not set(scopes) & set(actor.get('learning_scopes') or []):
        raise AssistantError('当前账号无权查询本轮范围的学练题单，不能判断为零条。', 403)
    scope_params = ["ALL"] if set(scopes) == set(SCOPES) else scopes
    deadline = time.monotonic() + ROUND_DEADLINE_SECONDS
    notice_board_tasks = {}
    group_out = {}

    async def call(api, params=None, path_params=None):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssistantError("本轮查询时间已到，此模块尚未读取。", 503)
        try:
            result = await asyncio.wait_for(invoke({"api_id": "GET " + api, "params": params or {}, "path_params": path_params or {}}), min(remaining, PER_CALL_TIMEOUT_SECONDS))
        except asyncio.TimeoutError:
            raise AssistantError("此模块读取超时，不能判断为零条。", 503) from None
        if not result.get("ok"):
            raise AssistantError(safe_text(result.get("error") or "此模块暂不可用。"), result.get("status") or 503)
        data = result.get("_raw", result.get("data"))
        if not isinstance(data, dict):
            raise AssistantError("此模块未返回有效数据。", 502)
        return data

    async def pages(api, params, key, mode="page", pagination=""):
        result, seen_pages, page = [], set(), 1
        while True:
            values = {**params, ("offset" if mode == "offset" else key + "_page" if pagination else "page"): len(result) if mode == "offset" else page}
            data = await call(api, values)
            rows = data.get(key)
            if not isinstance(rows, list):
                raise AssistantError("此模块未返回有效列表。", 502)
            if pagination and data.get("source_snapshot_ready") is False:
                raise AssistantError("通告资料正在初始化，暂不能判断完整数量。", 503)
            mark = tuple(str(row.get("record_id") or row.get("active_item_id") or row.get("batch_id") or row.get("id") or "") for row in rows)
            meta = data.get(pagination) or data
            total = int(meta.get("total", len(rows)))
            if mark in seen_pages or (not rows and len(result) < total):
                raise AssistantError("分页内容不完整，未将部分数据当作全量。", 502)
            seen_pages.add(mark)
            result.extend(rows)
            if len(result) >= total and not meta.get("has_more"):
                return result, data
            page += 1

    async def add(order_index, key, label, url, loader):
        items, warnings, stats, error = [], [], None, ""
        if on_progress:
            await on_progress("正在核对" + label)
        try:
            # 整组 loader 也设独立预算：即使分页/多楼栋的早期分组，也不能独占
            # 全部并发槽位直到整轮 deadline。超时按 unknown 处理，绝不当作零条。
            budget = min(WHOLE_LOADER_BUDGET_SECONDS, max(0.001, deadline - time.monotonic()))
            loaded = await asyncio.wait_for(loader(), budget)
            if isinstance(loaded, (tuple, list)) and len(loaded) == 3:
                items, warnings, stats = loaded
            else:
                items, warnings = loaded
        except asyncio.TimeoutError:
            error = "此模块整体读取超时，不能判断为零条。"
        except AssistantError as exc:
            error = safe_text(str(exc))
        except Exception:
            error = "此模块资料暂不可用，不能判断为零条。"
        unique = {}
        for item in items:
            identity = item.get("id")
            if not identity:
                warnings.append("部分记录缺少稳定标识，未计入准确数量。")
                continue
            unique.setdefault(identity, item)
        items = list(unique.values())
        group_out[order_index] = {"key": key, "label": label, "url": url, "count": len(items) if not error and not warnings else None,
                       "known_count": len(items), "available": not error and not warnings, "error": error,
                       "warnings": list(dict.fromkeys(warnings)), "items": items[:10], "remaining": max(0, len(items) - 10),
                       "stats": stats if not error and not warnings else None,
                       "source_freshness": (stats or {}).get('source_freshness', []),
                       "type_counts": {kind: sum(row.get("type") == kind for row in items) for kind in NOTICE_TYPES.values()} if key in {"notices", "plans"} else {}}
        if on_progress:
            await on_progress(label + ("：已核对" if not error and not warnings else "：部分资料暂不可用"))

    def board_notice_params(scope):
        return {"scope": scope, "sections": "ongoing", "ongoing_page_size": 200,
                **({"work_type": notice_type} if notice_type else {})}

    async def _board_notices_uncached(scope):
        return await pages("/api/workbench", board_notice_params(scope), "ongoing", pagination="ongoing_pagination")

    async def _board_notices_bounded(scope):
        # 共享读取自身也 bound 在轮次剩余时间内，避免分页数据集无限在途、占用整轮。
        budget = min(SHARED_BOARD_BUDGET_SECONDS, max(0.001, deadline - time.monotonic()))
        try:
            return await asyncio.wait_for(_board_notices_uncached(scope), budget)
        except asyncio.TimeoutError:
            raise AssistantError("此模块整体读取超时，不能判断为零条。", 503) from None

    async def board_notices(scope):
        task = notice_board_tasks.get(scope)
        if task is None:
            task = asyncio.ensure_future(_board_notices_bounded(scope))
            notice_board_tasks[scope] = task
        # Shield 共享的在途读取：wait_for 对其一个消费者的超时会取消那个调用方，
        # 但不允许取消正被另一个消费者（如 plans）复用的同一个在途任务。
        return await asyncio.shield(task)

    async def notices():
        from upload_event_module.core.parser import is_notice_confirmed_ended
        items = []
        for scope in scope_params:
            rows, _ = await board_notices(scope)
            for item in rows:
                if item.get("deleted_at") or is_notice_confirmed_ended(item) or (not pending_status(item.get("status")) and not item.get("_has_unuploaded_changes")):
                    continue
                row = project(item, scopes, title="通告", status="未结束")
                kind = str(item.get("work_type") or "maintenance")
                if notice_type and kind != notice_type:
                    continue
                row["type"] = NOTICE_TYPES.get(kind, safe_text(item.get("notice_type") or "通告"))
                if item.get("_has_unuploaded_changes") and not pending_status(item.get("status")):
                    row["status"] = "结束草稿待发送（仍未结束）"
                row["id"] = kind + ":" + row["id"] if row["id"] else ""
                row["url"] = "/workbench-lite?" + urlencode({"scope": scope, "work_type": kind, "active_item_id": item.get("active_item_id") or item.get("target_record_id") or item.get("record_id") or ""})
                items.append(row)
        return items, []

    async def plans():
        from lan_bitable_template_portal.identity_utils import canonical_source_record_id
        from lan_bitable_template_portal.portal_service import MaintenancePortalService
        items, warnings = [], []
        for scope in scope_params:
            ongoing, _ = await board_notices(scope)
            linked = {(row.get("work_type"), canonical_source_record_id(row)) for row in ongoing}
            rows, data = await pages("/api/workbench", {"scope": scope, "sections": "records", "records_page_size": 200,
                **({"work_type": notice_type} if notice_type else {})}, "records", pagination="records_pagination")
            for item in rows:
                if not visible(item, scopes, actor.get("allowed_scopes", scopes)) or item.get("deleted_at"):
                    continue
                kind, identity = item.get("work_type"), item.get("record_id")
                if kind not in NOTICE_TYPES or (notice_type and kind != notice_type) or item.get("linked_ongoing") or (kind, identity) in linked:
                    continue
                if "source_progress" not in item and "source_status" not in item:
                    warnings.append("部分计划缺少可开始状态，完整计划数量待核对。")
                    continue
                progress = item.get("source_progress", item.get("source_status"))
                if not MaintenancePortalService._source_progress_allows_start(progress):
                    continue
                row = project(item, scopes, title="计划通告", identity=f"{kind}:{identity}" if identity else "",
                    url="/workbench-lite?" + urlencode({"scope": scope, "work_type": kind, "record_id": identity}))
                row.update(type=NOTICE_TYPES[kind], status="未开始")
                items.append(row)
            warnings.extend(data.get("warnings") or [])
        return items, warnings

    async def cached(kind):
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise AssistantError("本轮查询时间已到，此模块尚未读取。", 503)
        try:
            return await asyncio.wait_for(asyncio.to_thread(read_cached, kind, scopes), min(remaining, PER_CACHED_TIMEOUT_SECONDS))
        except asyncio.TimeoutError:
            raise AssistantError("此模块读取超时，不能判断为零条。", 503) from None

    async def repairs():
        items, warnings = [], []
        for scope in scope_params:
            rows, data = await pages("/api/repair-management/records", {"scope": scope, "state": "active", "period": "all", "limit": 200, "summary_only": "1"}, "records", mode="offset")
            warnings.extend(data.get("schema_warnings") or [])
            for row in rows:
                if row.get('is_completed') or not pending_status(row.get('workflow') or row.get('status_label') or row.get('status')):
                    continue
                if not record_codes(row):
                    warnings.append('部分维修项目缺少楼栋信息，完整数量待核对。')
                    continue
                if not visible(row, scopes, actor.get('allowed_scopes', scopes)):
                    warnings.append('部分维修项目超出本轮可核对范围，完整数量待核对。')
                    continue
                items.append(project(row, scopes, title='维修项目', status='未完成',
                    url='/repair-management?' + urlencode({'scope': scope, 'record_id': row.get('record_id')})))
        return items, warnings

    async def batches():
        items = []
        for scope in (code for code in scopes if code in "ABCDE"):
            rows, _ = await pages("/api/cabinet-power/batches", {"scope": scope, "status": "todo", "page_size": 100}, "items")
            for item in rows:
                if item.get("is_todo") and not item.get("source_notice_deleted"):
                    row = project(item, scopes, title="上下电待办", identity=item.get("batch_id"), url="/cabinet-power/batches?" + urlencode({"scope": scope, "batch_id": item.get("batch_id"), "status": "todo"}))
                    row["status"] = "待确认" + (" · " + str(item["pending_rows"]) + "柜" if item.get("pending_rows") else "")
                    items.append(row)
        return items, []

    async def drills():
        items = []
        for scope in (code for code in scopes if code in "ABCDE"):
            data = await call("/api/drills", {"scope": scope})
            if not isinstance(data.get("items"), list):
                raise AssistantError("演练任务列表未完整返回。", 502)
            for item in data["items"]:
                execution = item.get("execution") or {}
                if item.get("status") == "published" and pending_status(execution.get("status")):
                    items.append(project({**item, "scope": scope, "status": execution.get("status") or "未开始"}, scopes, title="演练任务", identity=str(item.get("drill_id") or "") + ":" + scope, url="/drill-management?" + urlencode({"scope": scope, "drill_id": item.get("drill_id")})))
        return items, []

    async def learning():
        allowed = set(scopes) & set(actor.get('learning_scopes') or []) & set('ABCDEH')
        if not allowed:
            raise AssistantError('本轮范围没有可访问的学练楼栋。', 403)
        items, unpublished = [], []
        for scope in sorted(allowed):
            rows, data = await pages('/api/learning/papers', {'scope': scope, 'today': '1'}, 'items')
            if not rows:
                unpublished.append(scope)
                items.append({'id': 'learning-unpublished:' + scope, 'title': '今日学练题单未发布', 'scopes': [scope], 'status': '未发布', 'url': '/learning'})
            for row in rows:
                if row.get('scope') != scope or not isinstance(row.get('stats'), dict):
                    raise AssistantError('学练题单状态未完整返回。', 502)
                if row.get('status') == 'completed':
                    continue
                stats = row['stats']
                total, answered = stats.get('total'), stats.get('answered')
                if type(total) is not int or type(answered) is not int or not 0 <= answered <= total:
                    raise AssistantError('学练答题进度无效，数量未知。', 502)
                items.append(project({**row, 'title': row['date'] + '学练题单', 'status': f'待答 {total - answered} 题'}, scopes, identity=row['id'], url='/learning'))
        return items, [], {'unpublished_scopes': unpublished}

    async def guard():
        items = []
        guard_scopes = [code for code in scopes if code in GUARD_SCOPES]
        unsupported = sorted(set(scopes) - GUARD_SCOPES)
        # H/110 单独范围且 A-E 交集为空：重保无支持，是缺数据而非零条。
        if not guard_scopes:
            label = "、".join(code + ("站" if code == "110" else "楼") for code in sorted(set(scopes) - GUARD_SCOPES))
            raise AssistantError(f"{label}尚无重保模块支持（仅ABCDE楼支持），暂不能判断重保状态。", 404)
        # 全部原数据可靠返回才出 stats；任一失败直接抛错，绝不出假零。
        tasks = {}
        responses_by_task = {}
        for scope in guard_scopes:
            data = await call("/api/critical-guard/tasks", {"scope": scope})
            if not isinstance(data.get("tasks"), list):
                raise AssistantError("重保任务列表未完整返回。", 502)
            for task in data["tasks"]:
                task_id = str(task.get("task_id") or "").strip()
                if not task_id:
                    raise AssistantError("重保任务缺少稳定标识，不能判断准确数量。", 502)
                detail = await call("/api/critical-guard/tasks/{task_id}", {"scope": scope}, {"task_id": task_id})
                if not isinstance(detail.get("responses"), list):
                    raise AssistantError("重保检查表状态未完整返回。", 502)
                # 任务发布到当前查询楼栋却无该楼请求形状，说明明细不完整，不能当“任务1未填0”。
                def well_formed(item):
                    return isinstance(item, dict) and str(item.get("scope") or "").strip().upper() == scope \
                        and str(item.get("response_id") or "").strip() and isinstance(item.get("cells"), dict)
                if any(isinstance(item, dict) and str(item.get("scope") or "").strip().upper() == scope and not well_formed(item) for item in detail["responses"]):
                    raise AssistantError("重保检查表明细不完整（响应缺少稳定标识或填写数据），不能判断准确数量。", 502)
                if not any(well_formed(item) for item in detail["responses"]):
                    raise AssistantError(f"重保任务{task_id}在当前查询楼栋{scope}缺少检查表明细，不能判断准确数量。", 502)
                tasks.setdefault(task_id, task)
                responses_by_task.setdefault(task_id, []).extend(
                    item for item in detail["responses"] if well_formed(item))
        pending_scopes, unfilled_scopes, seen, unsubmitted = set(), set(), set(), 0
        for task_id, response_list in responses_by_task.items():
            for response in response_list:
                rid = str(response.get("response_id") or "").strip()
                if rid and rid in seen:
                    continue
                if rid:
                    seen.add(rid)
                if not _guard_unsubmitted(response.get("status")):
                    continue
                unsubmitted += 1
                rscope = str(response.get("scope") or "").strip().upper()
                if not rscope:
                    continue
                pending_scopes.add(rscope)
                if not _guard_cells_filled(response.get("cells")):
                    unfilled_scopes.add(rscope)
        task_count = len(tasks)
        pending_buildings_count, unfilled_buildings_count = len(pending_scopes), len(unfilled_scopes)
        stats = {
            "available": True,
            "task_count": task_count,
            "pending_buildings_count": pending_buildings_count,
            "pending_scopes": sorted(pending_scopes),
            "unfilled_buildings_count": unfilled_buildings_count,
            "unfilled_scopes": sorted(unfilled_scopes),
            "filled_not_submitted_count": max(0, pending_buildings_count - unfilled_buildings_count),
            "pending_checklist_count": unsubmitted,
            "unsupported_scopes": unsupported,
            "tasks": [{"task_id": task_id, "name": safe_text(task.get("name") or task.get("task_name") or "重保任务"),
                "scopes": sorted(codes(task.get("target_scopes")) & set(guard_scopes))} for task_id, task in tasks.items()],
        }
        for task_id, response_list in responses_by_task.items():
            task = tasks.get(task_id) or {"task_id": task_id}
            for response in response_list:
                scope = str(response.get("scope") or "").strip().upper()
                if not _guard_unsubmitted(response.get("status")):
                    continue
                items.append(project({"title": str(task.get("name") or task.get("task_name") or "重保") + " · " + str(response.get("sheet_type") or "检查表"), "scope": scope, "status": response.get("status") or "未开始"}, scopes, identity=response.get("response_id"), url="/critical-guard?" + urlencode({"scope": scope, "task_id": task_id})))
        return items, [], stats

    scope_link = "ALL" if set(scopes) == set(SCOPES) else "CAMPUS" if set(scopes) == set("ABCDE") else scopes[0]
    scope_url = urlencode({"scope": scope_link})
    group_defs = []
    for key, label, url, loader in (
        ("notices", "未结束通告", "/workbench-lite?" + scope_url, notices),
        ("plans", "未发计划通告", "/workbench-lite?" + scope_url, plans),
        ("events", "未闭环事件", "/?mode=events&" + scope_url, lambda: cached("events")),
        ("repairs", "未完成维修项目", "/repair-management?" + scope_url, repairs),
        ("batches", "上下电待办", "/cabinet-power/batches?status=todo&" + scope_url, batches),
        ("orders", "SOP工单", "/workbench-lite?" + scope_url, lambda: cached("orders")),
        ("mops", "维护单待处理文件", "/engineer/mop?" + scope_url, lambda: cached("mops")),
        ("drills", "演练任务", "/drill-management?" + scope_url, drills),
        ("guard", "重保检查表", "/critical-guard?" + scope_url, guard),
        ("learning", "今日学练", "/learning", learning),
    ):
        if groups_only is not None and key not in groups_only:
            continue
        if key == 'learning' and not set(scopes) & set(actor.get('learning_scopes') or []):
            continue
        group_defs.append((key, label, url, loader))
    order = list(range(len(group_defs)))
    concurrency_limit = 4
    sem = asyncio.Semaphore(concurrency_limit)

    async def _bounded_group(order_index, key, label, url, loader):
        async with sem:
            await add(order_index, key, label, url, loader)

    runner_tasks = [asyncio.ensure_future(_bounded_group(i, *group_defs[i])) for i in order]
    try:
        await asyncio.gather(*runner_tasks)
    finally:
        # 取消并等待仍在途的共享通告读取，保证 collect 结束（含取消）时不遗留
        # detached 请求。正常的完整结束下这些任务已完成，这里是无操作。
        for task in notice_board_tasks.values():
            if not task.done():
                task.cancel()
        if notice_board_tasks:
            await asyncio.gather(*notice_board_tasks.values(), return_exceptions=True)
        # 防御性收尾：runner 若因取消/异常仍未结束，一并取消等待，不放飞。
        for task in runner_tasks:
            if not task.done():
                task.cancel()
        if runner_tasks:
            await asyncio.gather(*runner_tasks, return_exceptions=True)
    groups = [group_out[i] for i in order]
    return {"scopes": scopes, "queried_at": dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(timespec="seconds"),
            "coverage": "当前状态，不按今天或当月筛掉较早的未结束通告；水耗已录入记录不作为未完成工作。",
            "complete": all(group["available"] for group in groups), "groups": groups}


async def collect_repair_overview(actor, invoke, *, only_keys=None):
    """Read native repair projections; group counts are never an independent-work total.

    Workbench records include started plans. Its pending count excludes linked
    sources and non-startable progress before serialization applies local status
    overrides. Require both full paging and agreement with that native count;
    a mismatch is unknown, not a fabricated complete plan count.
    """
    from upload_event_module.core.parser import is_notice_confirmed_ended
    from lan_bitable_template_portal.identity_utils import canonical_source_record_id
    from .lighthouse_scope import resolve_scopes
    from lan_bitable_template_portal.portal_service import MaintenancePortalService

    scopes = resolve_scopes(actor, "")
    scope_params = ["ALL"] if set(scopes) == set(SCOPES) else scopes
    groups = [{"key": key, "label": label, "items": {}, "warnings": [], "error": "", "source_freshness": []}
              for key, label in (("planned_repairs", "待开始检修计划"), ("repair_notices", "未结束检修通告"), ("repairs", "未完成维修项目"))]
    plans, notices, repairs = groups
    wanted = {group["key"] for group in groups} if only_keys is None else set(only_keys)
    if not wanted or wanted - {group["key"] for group in groups}:
        raise AssistantError("检修查询分类无效。")
    # Plans need ongoing notices to exclude sources that have already started.
    required = wanted | ({"repair_notices"} if "planned_repairs" in wanted else set())
    deadline = time.monotonic() + ROUND_DEADLINE_SECONDS

    def identity(item, key):
        if key == "repair_notices":
            return str(item.get("target_record_id") or item.get("record_id") or item.get("active_item_id") or item.get("id") or "").strip()
        # A repair project's event/notice relation is never its own identity.
        return str(item.get("record_id") or item.get("id") or "").strip()

    async def pages(api, params, key, group):
        workbench = api == "/api/workbench"
        page, offset, expected, seen = 1, 0, None, set()
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssistantError("本轮查询时间已到，完整数量待确认。", 503)
            values = {**params, key + "_page" if workbench else "offset": page if workbench else offset}
            try:
                result = await asyncio.wait_for(invoke({"api_id": "GET " + api, "params": values}), min(remaining, 10))
            except asyncio.TimeoutError:
                raise AssistantError("此模块读取超时，不能判断为零条。", 503) from None
            if not isinstance(result, dict) or not result.get("ok"):
                raise AssistantError("此模块暂不可用，完整数量待确认。", 503)
            if result.get("truncated") and "_raw" not in result:
                raise AssistantError("返回资料已截断，完整数量待确认。", 502)
            data = result.get("_raw", result.get("data"))
            if not isinstance(data, dict) or not isinstance(data.get(key), list) or any(not isinstance(row, dict) for row in data[key]):
                raise AssistantError("此模块未返回有效列表。", 502)
            if data.get("source_snapshot_ready") is False:
                raise AssistantError("源资料正在初始化，暂不能判断完整数量。", 503)
            rows = data[key]
            mark = tuple(identity(row, group["key"]) for row in rows)
            if mark in seen:
                raise AssistantError("分页内容重复，完整数量待确认。", 502)
            seen.add(mark)
            yield rows, data
            meta = data.get(key + "_pagination") if workbench else data
            if not isinstance(meta, dict) or not isinstance(meta.get("total"), int) or isinstance(meta["total"], bool) or meta["total"] < 0:
                raise AssistantError("缺少有效分页总数，完整数量待确认。", 502)
            total = meta["total"]
            if expected is not None and expected != total:
                raise AssistantError("查询期间分页总数变化，完整数量待确认。", 502)
            expected = total
            if ("offset" in meta and meta["offset"] != offset) or (workbench and "page" in meta and meta["page"] != page):
                raise AssistantError("分页位置不一致，完整数量待确认。", 502)
            offset += len(rows)
            if offset > total or (not rows and offset < total) or ("has_more" in meta and bool(meta["has_more"]) != (offset < total)):
                raise AssistantError("分页内容不完整，完整数量待确认。", 502)
            if offset == total:
                return
            page += 1

    for scope in scope_params:
        query_scopes = scopes if scope == "ALL" else [scope]
        linked_sources = set()
        notices_complete = False
        workbench_version = None
        for group, api, key, params in (
            (notices, "/api/workbench", "ongoing", {"sections": "records,ongoing", "work_type": "repair", "records_page_size": 200, "ongoing_page_size": 200}),
            (plans, "/api/workbench", "records", {"sections": "records,ongoing", "work_type": "repair", "records_page_size": 200, "ongoing_page_size": 200}),
            (repairs, "/api/repair-management/records", "records", {"state": "active", "period": "all", "limit": 200, "summary_only": "1"}),
        ):
            if group["key"] not in required:
                continue
            scoped_ids, native_counts, warnings = set(), set(), []
            try:
                async for rows, data in pages(api, {**params, "scope": scope}, key, group):
                    for name in ("warnings", "schema_warnings"):
                        warnings.extend(safe_text(value) for value in data.get(name) or [])
                    freshness = {name: data[name] for name in ("last_loaded_at", "source_snapshot_ready", "source_cache_ttl_seconds", "payload_version") if name in data}
                    if freshness:
                        freshness = {"scope": scope, **freshness}
                        if freshness not in group["source_freshness"]:
                            group["source_freshness"].append(freshness)
                    if group is not repairs:
                        version = data.get("payload_version") or data.get("last_loaded_at")
                        if version:
                            if workbench_version is not None and workbench_version != version:
                                raise AssistantError("查询期间工作台快照变化，完整数量待确认。", 503)
                            workbench_version = version
                    if group is plans:
                        native = (data.get("record_type_counts") or {}).get("repair")
                        if isinstance(native, int) and not isinstance(native, bool) and native >= 0:
                            native_counts.add(native)
                        else:
                            warnings.append("原生工作台未提供待开始检修数量，完整计划数量待确认。")
                    for item in rows:
                        if not record_codes(item):
                            warnings.append("部分记录缺少楼栋信息，未计入准确数量。")
                            continue
                        if not visible(item, query_scopes, actor.get("allowed_scopes", scopes)):
                            continue
                        if group is not repairs and item.get("work_type") != "repair":
                            continue
                        item_id = identity(item, group["key"])
                        if group is notices:
                            source_id = canonical_source_record_id(item)
                            if source_id:
                                linked_sources.add(source_id)
                            if item.get("deleted_at") or is_notice_confirmed_ended(item) or (not pending_status(item.get("status")) and not item.get("_has_unuploaded_changes")):
                                continue
                        elif group is plans:
                            if not notices_complete:
                                continue
                            if item_id in linked_sources or item.get("linked_ongoing"):
                                continue
                            if "source_progress" not in item and "source_status" not in item:
                                warnings.append("部分计划缺少原生可开始状态，未计入准确数量。")
                                continue
                            progress = item.get("source_progress", item.get("source_status"))
                            if not MaintenancePortalService._source_progress_allows_start(progress):
                                continue
                        else:
                            if item.get("followup_state_verified") is False:
                                warnings.append("部分维修项目跟进状态尚未确认，完整数量待确认。")
                                continue
                            if item.get("is_completed") or not pending_status(item.get("workflow") or item.get("status_label") or item.get("status")):
                                continue
                        if item.get("deleted_at"):
                            continue
                        if not item_id:
                            warnings.append("部分记录缺少自身稳定标识，未计入准确数量。")
                            continue
                        scoped_ids.add(item_id)
                        url = ("/repair-management?" + urlencode({"scope": scope, "record_id": item_id}) if group is repairs else
                               "/workbench-lite?" + urlencode({"scope": scope, "work_type": "repair", "record_id" if group is plans else "active_item_id": item.get("active_item_id") or item_id}))
                        row = project(item, scopes, title=group["label"], status="未完成", identity=item_id, url=url)
                        if group is plans:
                            row["status"] = safe_text(progress or "未开始")[:80]
                        elif group is notices and item.get("_has_unuploaded_changes") and not pending_status(item.get("status")):
                            row["status"] = "结束草稿待发送（仍未结束）"
                        group["items"].setdefault(item_id, row)
                if group is notices:
                    notices_complete = not warnings
                elif group is plans:
                    if native_counts != {len(scoped_ids)}:
                        warnings.append("原生待开始检修计数与已核对计划不一致，完整计划数量待确认。")
                    if not notices_complete:
                        warnings.append("检修通告未完整核对，计划的已开始排除结果待确认。")
            except AssistantError as exc:
                group["error"] = safe_text(str(exc))
            except Exception:
                group["error"] = "此模块资料暂不可用，不能判断为零条。"
            group["warnings"].extend(warnings)

    groups = [group for group in groups if group["key"] in wanted]
    for group in groups:
        items = list(group["items"].values())
        group["warnings"] = list(dict.fromkeys(group["warnings"]))
        available = not group["error"] and not group["warnings"]
        group.update(count=len(items) if available else None, known_count=len(items), available=available,
                     items=items[:30], remaining=max(0, len(items) - 30))
        group["url"] = ("/repair-management?" if group is repairs else "/workbench-lite?work_type=repair&") + urlencode({"scope": scopes[0] if len(scopes) == 1 else "ALL"})
    return {"kind": "repair", "scopes": scopes, "queried_at": dt.datetime.now(dt.timezone(dt.timedelta(hours=8))).isoformat(timespec="seconds"),
            "coverage": "检修计划、检修通告、维修项目分别计数；各组可能关联同一工作，不相加为独立工作总数。",
            "complete": all(group["available"] for group in groups), "groups": groups}


def _cached_source_note(group):
    sources = [item for item in group.get('source_freshness', [])
               if isinstance(item, dict) and item.get('kind') == 'local_cache']
    if not sources:
        return ''
    times = []
    for source in sources:
        try:
            stamp = float(source.get('last_cloud_sync_at') or 0)
            if stamp > 0:
                times.append(dt.datetime.fromtimestamp(stamp, dt.timezone(dt.timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'))
        except (ValueError, TypeError, OverflowError, OSError):
            pass
    updated = '、'.join(sorted(set(times))) or '未知'
    return ('按本机已知记录统计，事件表最近同步云端：' + updated + '。'
            '已结合 Qt 本地未结束记录；本轮未重新同步云端。')


def pending_reply(data, *, details=True, include_zero=False):
    """Counts and omissions are deterministic; a model cannot rewrite this evidence."""
    if not data["scopes"]:
        return ("检修与维修" if data.get("kind") == "repair" else "未完成工作") + "：当前范围没有可访问楼栋，未执行查询。"
    def cell(value):
        text = html.escape(safe_text(str(value or "—"))).replace("|", "&#124;").replace("\n", " ")
        return re.sub(r"([\\`*_\[\]()])", r"\\\1", text)
    groups = [group for group in data["groups"] if include_zero or not group["available"] or group["count"]]
    if not groups:
        scope = "、".join(code + ("站" if code == "110" else "楼") for code in data["scopes"])
        label = data["groups"][0]["label"] if len(data["groups"]) == 1 else "检修与维修未完成事项" if data.get("kind") == "repair" else "未完成事项"
        notes = [_cached_source_note(group) for group in data['groups']]
        notes = list(dict.fromkeys(note for note in notes if note))
        answer = scope + ('本机已知记录中的' if notes else '当前') + label + "为 **0 项**。[1]"
        return answer + ('\n\n' + '\n'.join(notes) if notes else '')
    scope_label = "、".join(code + ("站" if code == "110" else "楼") for code in data["scopes"])
    if len(data["groups"]) == 1 and groups[0]["available"] and not details:
        group = groups[0]
        unit = "条" if group["key"] in {"notices", "plans", "events", "planned_repairs", "repair_notices"} else "项"
        note = _cached_source_note(group)
        answer = scope_label + ('本机已知记录中**' if note else '当前**') + group["label"] + " " + str(group["count"]) + " " + unit + "**。"
        types = [kind + " " + str(count) + " 条" for kind, count in (group.get("type_counts") or {}).items() if count or include_zero]
        if types:
            answer += "\n\n" + "，".join(types) + "。"
        return answer + ('\n\n' + note if note else '') + "\n\n查询于 " + data["queried_at"].replace("T", " ").replace("+08:00", "") + "。[1]"
    heading = data["groups"][0]["label"] if len(data["groups"]) == 1 else "检修与维修" if data.get("kind") == "repair" else "未完成工作"
    lines = ["## " + heading, "**查询范围：** " + scope_label,
             "**查询时间：** " + data["queried_at"].replace("T", " ").replace("+08:00", ""),
             "按当前状态查询。" if len(data["groups"]) == 1 else "按当前状态查询，包含较早开始但尚未结束的通告。各模块可能关联同一工作，**不相加为独立工作总数**。", "", "| 模块 | 未完成数量 |", "| --- | ---: |"]
    for group in groups:
        count_text = str(group["count"]) if group["available"] else (
            '待确认（已核对 ' + str(group.get('known_count') or 0) + '）' if group.get('known_count') else '待确认')
        lines.append("| [" + cell(group["label"]) + "](" + group["url"] + ") | " + count_text + " |")
    for group in groups:
        note = _cached_source_note(group)
        if note:
            lines.extend(['', '**' + group['label'] + '：** ' + note])
        if not group["available"]:
            lines.extend(["", "**" + group["label"] + "：** <span style=\"color:#e3c186\">" + cell(group["error"] or "；".join(group["warnings"])) + "</span>"])
        if not details or not group["items"]:
            continue
        lines.extend(["", "### " + group["label"], "| 事项 | 楼栋 | 状态 |", "| --- | --- | --- |"])
        for item in group["items"]:
            title = (item.get("type", "") + " · " if item.get("type") else "") + item["title"]
            lines.append("| " + cell(title) + " | " + cell("、".join(item["scopes"])) + " | " + cell(item["status"]) + " |")
        if group["remaining"]:
            lines.append("另有 **" + str(group["remaining"]) + " 项**，请在对应页面查看完整列表。")
        if group.get("type_counts"):
            lines.append("通告分类：" + "，".join(kind + " **" + str(count) + "** 项" for kind, count in group["type_counts"].items() if count or include_zero))
    lines.extend(["", "资料编号[1]。"])
    return "\n".join(lines)


def guard_reply(data):
    """简洁答复重保任务/填写/提交情况；源数据不可靠时明确未知，不造假零。"""
    guard = next((group for group in data.get("groups") or [] if group.get("key") == "guard"), None)
    stats = (guard or {}).get("stats") or {}
    unsupported = sorted(str(code) for code in (stats.get("unsupported_scopes") or []))

    def scope_label(code):
        return str(code) + ("站" if str(code) == "110" else "楼")

    unsupported_note = ""
    if unsupported:
        unsupported_note = "；" + "、".join(scope_label(code) for code in unsupported) + "重保暂不支持，未计入，不能作为零条。"
    if not guard or not guard.get("available") or not stats.get("available"):
        reason = safe_text((guard or {}).get("error") or (("此模块未返回有效数据" + (unsupported_note or "")) if not guard else "资料暂不可用"))
        return "重保任务情况暂无法确认（" + _cell_text(reason) + "）。" + (unsupported_note or "")
    task_count = int(stats.get("task_count") or 0)
    unfilled = int(stats.get("unfilled_buildings_count") or 0)
    filled_but_unsubmitted = int(stats.get("filled_not_submitted_count") or 0)
    answer = "当前重保任务 %d 个，%d 栋楼尚未填写，%d 栋楼已填写但未提交。" % (task_count, unfilled, filled_but_unsubmitted)
    checklist_count = int(stats.get("pending_checklist_count") or 0)
    if checklist_count:
        answer += " 另有 %d 份检查表未提交。" % checklist_count
    if unsupported_note:
        answer += unsupported_note
    return answer
