"""Pure-function native download-link synthesis.

Re-derives public download URLs from already-authenticated native response
envelopes without network, server, or filesystem access.
"""

from __future__ import annotations

import datetime as _dt
import re
from urllib.parse import quote

from .lighthouse_ai import safe_text

_ABE = frozenset("ABCDE")
_H = frozenset("H")
_INVALID = object()

_SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,160}$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Cabinet export/file is usable only when native status says it is done and the
# record is still present/available.  ``generated``/``completed`` are legitimate
# done phases in the native export/job records.
_CABINET_DONE = frozenset({"done", "succeeded", "completed", "generated"})

_DRILL_APIS = frozenset(
    {
        "GET /api/drills/{drill_id}/execution",
        "POST /api/drills/{drill_id}/generate",
        "POST /api/drills/{drill_id}/retry-sync",
    }
)
_CABINET_JOB_APIS = frozenset(
    {
        "GET /api/cabinet-power/jobs/{job_id}",
        "POST /api/cabinet-power/exports",
    }
)
_CABINET_HISTORY_APIS = frozenset({"GET /api/cabinet-power/export-history"})
_CABINET_BATCH_APIS = frozenset(
    {
        "POST /api/cabinet-power/export-batches",
        "GET /api/cabinet-power/export-batches/{batch_id}",
        "POST /api/cabinet-power/export-batches/{batch_id}/resume",
    }
)
_CRITICAL_TASK_APIS = frozenset({"GET /api/critical-guard/tasks/{task_id}"})
_CRITICAL_RESPONSE_APIS = frozenset({"PUT /api/critical-guard/responses/{response_id}"})
_CRITICAL_SOURCE_APIS = frozenset({"POST /api/critical-guard/source-files"})
_MORNING_APIS = frozenset(
    {
        "POST /api/daily-tasks/morning-meeting/generate",
        "GET /api/daily-tasks/morning-meeting/preview",
    }
)


def _scopes(actor) -> frozenset:
    if isinstance(actor, dict):
        values = actor.get("scopes")
    else:
        values = getattr(actor, "scopes", None)
    if not isinstance(values, (list, tuple, set, frozenset)):
        return frozenset()
    return frozenset(str(value).strip().upper() for value in values if str(value).strip())


def _result_data(result) -> dict | None:
    """Return the authenticated native response body, or None when unusable."""
    if not isinstance(result, dict):
        return None
    if not result.get("ok"):
        return None
    data = result.get("_raw")
    if not isinstance(data, dict):
        data = result.get("data")
    return data if isinstance(data, dict) else None


def _is_safe_id(value) -> bool:
    return isinstance(value, str) and bool(_SAFE_ID_RE.fullmatch(value))


def _positive_int(value):
    """Return int when explicit positive integer (never bool/float), else None."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if value > 0 else None


def _request_scope(operation, allowed) -> object:
    """Resolve the single request scope from params/body/path, or ``_INVALID``.

    A scope conflict between the locations, or a scope outside ``allowed``, is
    treated as invalid and never silently resolved.
    """
    if not isinstance(operation, dict):
        return ""
    values: list[str] = []
    for source in ("params", "body", "path_params"):
        section = operation.get(source)
        if not isinstance(section, dict):
            continue
        value = section.get("scope")
        if value is None or not str(value).strip():
            continue
        values.append(str(value).strip().upper())
    if not values:
        return ""
    first = values[0]
    if any(value != first for value in values) or first not in allowed:
        return _INVALID
    return first


def _basename_str(value) -> str:
    """Return a trailing path segment using pure string splitting (no FS access)."""
    text = str(value or "").strip()
    parts = re.split(r"[/\\]+", text)
    return parts[-1].strip()


def _safe_filename_label(value, default="") -> str:
    base = _basename_str(value)
    if not base:
        return default
    cleaned = safe_text(base).strip()
    return cleaned[:200] or default


def _same_positive_versions(execution) -> bool:
    execution_version = _positive_int(execution.get("execution_version"))
    generated_version = _positive_int(execution.get("generated_version"))
    return execution_version is not None and generated_version == execution_version


def _drill_links(actor, operation, data) -> list[dict]:
    if not isinstance(operation, dict):
        return []
    path_params = operation.get("path_params") if isinstance(operation.get("path_params"), dict) else {}
    drill_id = str(path_params.get("drill_id") or "").strip()
    if not _is_safe_id(drill_id):
        return []
    request_scope = _request_scope(operation, _ABE)
    if request_scope is _INVALID:
        return []
    execution = data.get("execution") if isinstance(data.get("execution"), dict) else data
    if not isinstance(execution, dict):
        return []
    # Native drill_id must match the request path when the response supplies one.
    if execution.get("drill_id") is not None and str(execution["drill_id"]).strip() != drill_id:
        return []
    scope = str(execution.get("scope") or "").strip().upper()
    if scope not in _ABE or scope not in _scopes(actor):
        return []
    if request_scope and request_scope != scope:
        return []
    generated = execution.get("generated") if isinstance(execution.get("generated"), dict) else {}
    if not str(generated.get("name") or "").strip():
        return []
    # The original frontend rule: ``generation_rule_current !== false``.  Only an
    # explicit False blocks the file; missing/None is acceptable.
    if execution.get("generation_rule_current") is False:
        return []
    if not _same_positive_versions(execution):
        return []
    name = _safe_filename_label(generated.get("name")) or "演练记录"
    return [{"name": name, "url": f"/api/drills/{drill_id}/download?scope={scope}"}]


def _export_link(item, actor_scopes, *, context_scope=None) -> dict | None:
    if not isinstance(item, dict):
        return None
    export_id = str(item.get("export_id") or "").strip()
    if not _is_safe_id(export_id):
        return None
    scope = str(item.get("scope") or "").strip().upper()
    if scope not in _ABE or scope not in actor_scopes:
        return None
    if context_scope is not None and scope != context_scope:
        return None
    status = str(item.get("status") or item.get("phase") or "").lower()
    if status and status not in _CABINET_DONE:
        return None
    if item.get("deleted"):
        return None
    if item.get("file_available") is False:
        return None
    name = _safe_filename_label(item.get("filename") or item.get("file_name")) or f"{scope}楼导出文件"
    return {"name": name, "url": f"/api/cabinet-power/exports/{export_id}/download"}


def _cabinet_job_links(actor, operation, data) -> list[dict]:
    if not isinstance(operation, dict) or not isinstance(data, dict):
        return []
    path_params = operation.get("path_params") if isinstance(operation.get("path_params"), dict) else {}
    request_job_id = str(path_params.get("job_id") or "").strip()
    if request_job_id and not _is_safe_id(request_job_id):
        return []
    request_scope = _request_scope(operation, _ABE)
    if request_scope is _INVALID:
        return []
    actor_scopes = _scopes(actor)
    # Job identity: native job_id, when provided, must be a safe identifier and
    # must match the requested one when a request path supplies it.
    if data.get("job_id") is not None:
        native_job_id = str(data["job_id"]).strip()
        if not _is_safe_id(native_job_id):
            return []
        if request_job_id and native_job_id != request_job_id:
            return []
    job_scope = str(data.get("scope") or "").strip().upper()
    if job_scope not in _ABE or job_scope not in actor_scopes:
        return []
    if request_scope and job_scope != request_scope:
        return []
    kind = str(data.get("kind") or "").lower()
    if kind and kind != "export":
        return []
    result = data.get("result") if isinstance(data.get("result"), dict) else {}
    # Nested result.scope, when provided, must agree with the job's scope.
    if result.get("scope") is not None and str(result["scope"]).strip().upper() != job_scope:
        return []
    merged = {
        **result,
        "scope": job_scope,
        "export_id": result.get("export_id") or data.get("export_id"),
        "filename": result.get("filename") or data.get("filename"),
        "file_name": result.get("file_name") or data.get("file_name"),
        "status": data.get("status") if data.get("status") is not None else result.get("status"),
        "deleted": data.get("deleted") if data.get("deleted") is not None else result.get("deleted"),
        "file_available": data.get("file_available")
        if data.get("file_available") is not None
        else result.get("file_available"),
        "phase": data.get("phase") if data.get("phase") is not None else result.get("phase"),
    }
    link = _export_link(merged, actor_scopes)
    return [link] if link else []


def _cabinet_history_links(actor, operation, data) -> list[dict]:
    request_scope = _request_scope(operation, _ABE)
    if request_scope is _INVALID or not request_scope:
        return []
    actor_scopes = _scopes(actor)
    if request_scope not in actor_scopes:
        return []
    if not isinstance(data, dict):
        return []
    items = data.get("items") if isinstance(data.get("items"), list) else []
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        item_scope = str(item.get("scope") or "").strip().upper()
        if item_scope != request_scope:
            continue
        link = _export_link(item, actor_scopes)
        if link:
            out.append(link)
    return out


def _cabinet_batch_links(actor, operation, data) -> list[dict]:
    if not isinstance(operation, dict) or not isinstance(data, dict):
        return []
    path_params = operation.get("path_params") if isinstance(operation.get("path_params"), dict) else {}
    body = operation.get("body") if isinstance(operation.get("body"), dict) else {}
    path_batch = str(path_params.get("batch_id") or "").strip() if isinstance(path_params, dict) else ""
    body_batch = str(body.get("batch_id") or "").strip() if isinstance(body, dict) else ""
    # A batch_id supplied in both the path and body must agree; a conflict is
    # never silently resolved in favour of one location.
    if path_batch and body_batch and path_batch != body_batch:
        return []
    request_batch = path_batch or body_batch
    if request_batch and not _is_safe_id(request_batch):
        return []
    if data.get("batch_id") is not None:
        native_batch = str(data["batch_id"]).strip()
        if not _is_safe_id(native_batch):
            return []
        if request_batch and native_batch != request_batch:
            return []
    request_scope = _request_scope(operation, _ABE)
    if request_scope is _INVALID:
        return []
    actor_scopes = _scopes(actor)
    raw_items = data.get("items") if isinstance(data.get("items"), dict) else {}
    out = []
    for dict_scope, child in sorted(raw_items.items()):
        if not isinstance(child, dict):
            continue
        # The child dictionary's declared scope must match the items key bucket.
        if child.get("scope") is not None and str(child["scope"]).strip().upper() != str(dict_scope).upper():
            continue
        child_scope = str(dict_scope or child.get("scope") or "").strip().upper()
        if child_scope not in _ABE or child_scope not in actor_scopes:
            continue
        if request_scope and child_scope != request_scope:
            continue
        if str(child.get("status") or "").lower() != "succeeded":
            continue
        result = child.get("result") if isinstance(child.get("result"), dict) else {}
        result_scope = None
        if result.get("scope") is not None:
            raw_result_scope = str(result["scope"]).strip().upper()
            # Nested result.scope must equal the child dictionary's scope.
            if raw_result_scope != child_scope:
                continue
            result_scope = raw_result_scope
        else:
            result_scope = child_scope
        link = _export_link({**result, "scope": result_scope}, actor_scopes, context_scope=child_scope)
        if link:
            out.append(link)
    return out


def _iter_critical_rows(data) -> list:
    """Yield native critical-guard response/source rows."""
    if not isinstance(data, dict):
        return []
    rows: list = []
    responses = data.get("responses")
    if isinstance(responses, list):
        rows.extend(item for item in responses if isinstance(item, dict))
    single = data.get("response")
    if isinstance(single, dict):
        rows.append(single)
    # A public response row (PUT responses/{id} echo or source-files upload
    # result) is itself a row and may carry nested source_file.
    if "response_id" in data:
        rows.append(data)
    return rows


def _critical_label(scope, sheet_type, check_date, kind, *, file_name=None) -> str:
    parts = [file_name or kind, f"{scope}楼"]
    if sheet_type:
        parts.append(str(sheet_type).strip())
    if check_date:
        parts.append(str(check_date).strip())
    return safe_text("-".join(parts))


def _critical_links(actor, operation, data) -> list[dict]:
    if not isinstance(operation, dict):
        return []
    api_id = str(operation.get("api_id") or "")
    path_params = operation.get("path_params") if isinstance(operation.get("path_params"), dict) else {}
    body = operation.get("body") if isinstance(operation.get("body"), dict) else {}
    request_scope = _request_scope(operation, _ABE)
    if request_scope is _INVALID:
        return []
    actor_scopes = _scopes(actor)

    response_constraint = ""
    source_response_constraint = ""
    task_id = ""

    if api_id in _CRITICAL_TASK_APIS:
        task_id = str(path_params.get("task_id") or "").strip()
        if not _is_safe_id(task_id):
            return []
        if data.get("task_id") is not None and str(data["task_id"]).strip() != task_id:
            return []
    elif api_id in _CRITICAL_RESPONSE_APIS:
        response_constraint = str(path_params.get("response_id") or "").strip()
        if not _is_safe_id(response_constraint):
            return []
    elif api_id in _CRITICAL_SOURCE_APIS:
        source_response_constraint = str(body.get("response_id") or "").strip()
        if source_response_constraint and not _is_safe_id(source_response_constraint):
            return []

    out = []
    for row in _iter_critical_rows(data):
        # A response row that declares its own task_id must agree with the
        # request's task_id when the canonical task route was used.
        if task_id and row.get("task_id") is not None and str(row["task_id"]).strip() != task_id:
            continue
        scope = str(row.get("scope") or "").strip().upper()
        if scope not in _ABE or scope not in actor_scopes:
            continue
        if request_scope and scope != request_scope:
            continue
        response_id = str(row.get("response_id") or "").strip()
        if response_id and not _is_safe_id(response_id):
            continue
        if response_constraint and response_id != response_constraint:
            continue
        if source_response_constraint and response_id != source_response_constraint:
            continue
        sheet_type = str(row.get("sheet_type") or "").strip()
        check_date = str(row.get("check_date") or "").strip()
        if not check_date and isinstance(row.get("cells"), dict):
            check_date = str(row["cells"].get("check_date") or "").strip()
        if response_id and row.get("has_image"):
            out.append({
                "name": _critical_label(scope, sheet_type, check_date, "重保检查表图片"),
                "url": f"/api/critical-guard/images/{response_id}",
            })
        if response_id and row.get("has_workbook"):
            out.append({
                "name": _critical_label(scope, sheet_type, check_date, "重保工作簿"),
                "url": f"/api/critical-guard/workbooks/{response_id}",
            })
        source = row.get("source_file")
        if isinstance(source, dict):
            if source.get("scope") is not None and str(source["scope"]).strip().upper() != scope:
                continue
            sf_sheet = str(source.get("sheet_type") or sheet_type or "").strip()
            if source.get("sheet_type") is not None and sf_sheet != sheet_type:
                continue
            file_id = str(source.get("file_id") or "").strip()
            if not _is_safe_id(file_id):
                continue
            file_name = _safe_filename_label(source.get("file_name") or source.get("filename")) or "重保源文件"
            sf_date = str(source.get("check_date") or check_date or "").strip()
            out.append({
                "name": _critical_label(scope, sf_sheet, sf_date, "重保源文件", file_name=file_name),
                "url": f"/api/critical-guard/source-files/{file_id}",
            })
    if api_id in _CRITICAL_TASK_APIS and actor.get("is_admin"):
        targets = data.get("target_scopes")
        allowed = {request_scope} if request_scope else actor_scopes
        if isinstance(targets, list) and targets and all(isinstance(scope, str) and scope in _ABE and scope in allowed for scope in targets):
            sheets = dict.fromkeys(row.get("sheet_type") for row in _iter_critical_rows(data)
                if row.get("has_image") is True and row.get("scope") in targets and isinstance(row.get("sheet_type"), str) and row["sheet_type"]
                and (row.get("task_id") is None or str(row["task_id"]).strip() == task_id))
            for sheet in sheets:
                out.append({"name": safe_text(sheet + "检查图片打包.zip") or "检查图片打包.zip",
                            "url": f"/api/critical-guard/tasks/{task_id}/download?sheet_type={quote(sheet, safe='')}"})
    return out


def _valid_canonical_date(value) -> str:
    text = str(value or "").strip()
    if not _DATE_RE.fullmatch(text):
        return ""
    try:
        _dt.date.fromisoformat(text)
    except ValueError:
        return ""
    return text


def _morning_links(actor, operation, data) -> list[dict]:
    api_id = str((operation or {}).get("api_id") or "")
    if api_id not in _MORNING_APIS:
        return []
    if "H" not in _scopes(actor):
        return []
    if not isinstance(operation, dict) or not isinstance(data, dict):
        return []
    section = (operation.get("params") or {}) if isinstance(operation.get("params"), dict) else {}
    body = (operation.get("body") or {}) if isinstance(operation.get("body"), dict) else {}
    path_params = (operation.get("path_params") or {}) if isinstance(operation.get("path_params"), dict) else {}
    # Any explicitly supplied request date must be canonical and must agree
    # across all request locations (params/body/path).
    raw_dates = [
        section.get("date") if isinstance(section, dict) else None,
        body.get("date") if isinstance(body, dict) else None,
        path_params.get("date") if isinstance(path_params, dict) else None,
    ]
    supplied = [str(value).strip() for value in raw_dates if value is not None and str(value).strip()]
    if supplied:
        first = supplied[0]
        if any(value != first for value in supplied[1:]):
            return []
        request_date = _valid_canonical_date(first)
        if not request_date:
            # A supplied but invalid date is rejected, never silently repaired by
            # falling back to the response's (valid) date.
            return []
    else:
        request_date = ""
    # The response must always carry the canonical date used for linkage.
    response_date = _valid_canonical_date(data.get("date"))
    if not response_date:
        return []
    # Fallback to the response's date is allowed only when no request date was
    # supplied.
    if request_date:
        if request_date != response_date:
            return []
        date = request_date
    else:
        date = response_date
    if not date:
        return []
    expected = f"/api/daily-tasks/morning-meeting/download?date={date}"
    if str(data.get("download_url") or "") != expected:
        return []
    return [{"name": f"晨会表-{date}", "url": expected}]


def native_download_links(actor, operation, result) -> list[dict]:
    """Return unique ``{name, url}`` download links from native metadata.

    Pure function: no network, server, filesystem, or recursive scanning.
    Returned links are stable and ordered; all valid native rows are returned
    (no silent count truncation).
    """
    operation = operation if isinstance(operation, dict) else {}
    result = result if isinstance(result, dict) else {}
    data = _result_data(result)
    if data is None:
        return []

    api_id = str(operation.get("api_id") or "")
    if api_id in _DRILL_APIS:
        links = _drill_links(actor, operation, data)
    elif api_id in _CABINET_JOB_APIS:
        links = _cabinet_job_links(actor, operation, data)
    elif api_id in _CABINET_HISTORY_APIS:
        links = _cabinet_history_links(actor, operation, data)
    elif api_id in _CABINET_BATCH_APIS:
        links = _cabinet_batch_links(actor, operation, data)
    elif (
        api_id in _CRITICAL_TASK_APIS
        or api_id in _CRITICAL_RESPONSE_APIS
        or api_id in _CRITICAL_SOURCE_APIS
    ):
        links = _critical_links(actor, operation, data)
    elif api_id in _MORNING_APIS:
        links = _morning_links(actor, operation, data)
    else:
        links = []

    seen: set[str] = set()
    out: list[dict] = []
    for link in links:
        if not isinstance(link, dict):
            continue
        url = str(link.get("url") or "")
        name = str(link.get("name") or "").strip() or "下载文件"
        if not url or url in seen:
            continue
        seen.add(url)
        out.append({"name": name, "url": url})
    return out
