"""Equipment ledger cache and cumulative repair-project associations."""
from __future__ import annotations

import re
import threading
from pathlib import Path

from .repair_ledger_catalog import LedgerCatalog, TEXT_FIELDS

APP_TOKEN = "WMcQbPn71aXwoGsNv0vcnzz2n6c"
TABLE_ID = "tblPIgIZ5fyEeQnY"
NAMESPACE = "repair_project_ledger"
LINK_FIELDS = {"台账关联状态": "待确认", "台账关联说明": "", "关联台账记录ID": ""}


def catalog(service):
    with service._repair_management_record_lock("equipment-ledger-cache"):
        if not hasattr(service, "_equipment_ledger_catalog"):
            service._equipment_ledger_catalog = LedgerCatalog(
                Path(service._state_store.db_path).parent / "repair_equipment" / "ledger.sqlite3"
            )
        return service._equipment_ledger_catalog


def allowed_scopes(service, scope):
    scope = service._normalize_scope(scope)
    return None if scope == "ALL" else list("ABCDE") if scope == "CAMPUS" else [scope]


def cache_status(service):
    value = catalog(service).status()
    worker = getattr(service, "_equipment_ledger_worker", None)
    value["refreshing"] = bool(worker and worker.is_alive())
    return value


def start_refresh(service, *, force=True):
    from upload_event_module.services.http_client import FeishuHttpClient
    from upload_event_module.services.process_lifetime import lower_current_thread_priority

    def run():
        lower_current_thread_priority()
        client = FeishuHttpClient(retries=3)
        try:
            _metas, metadata = service._load_table_fields(app_token=APP_TOKEN, table_id=TABLE_ID)
            if "设备编号" not in metadata or "机楼" not in metadata:
                raise ValueError("设备台账缺少设备编号或机楼字段，未替换本地缓存。")
            records = service._search_table_records(
                app_token=APP_TOKEN, table_id=TABLE_ID, meta_by_name=metadata,
                work_type="repair", notice_type="检修通告",
                field_names=[*TEXT_FIELDS, "楼栋标识"], limit=None, http_client=client, page_interval=0.2,
            )
            def rows():
                for record in records:
                    fields = record.get("display_fields") or {}
                    row = {name: service._repair_management_plain_text(fields.get(name)).strip() for name in TEXT_FIELDS}
                    row["record_id"] = record["record_id"]
                    row["scope_codes"] = service._repair_building_codes_from_value(row["机楼"] or fields.get("楼栋标识"))
                    # A nonempty unrecognized building must not become globally visible.
                    if not row["机楼"] and fields.get("楼栋标识") and not row["scope_codes"]:
                        row["scope_codes"] = ["UNKNOWN"]
                    yield row
            catalog(service).replace(rows())
        except Exception as exc:
            message = str(exc)
            if "99991400" in message:
                message = "飞书暂时限制请求频率（99991400），本次同步未完成，已有本地台账保持不变，请稍后刷新重试。"
            catalog(service).mark_error(message)
        finally:
            client.close()

    with service._repair_management_record_lock("equipment-ledger-cache"):
        if not force:
            current = cache_status(service)
            if current["ready"] or current["error"]:
                return current
        worker = getattr(service, "_equipment_ledger_worker", None)
        if not worker or not worker.is_alive():
            worker = threading.Thread(target=run, name="repair-equipment-cache", daemon=True)
            service._equipment_ledger_worker = worker
            worker.start()
    return cache_status(service)


def candidates(service, *, scope="ALL", query="", filters=None, page=1):
    status = cache_status(service)
    if not status["ready"] and not status["error"] and not status["refreshing"]:
        start_refresh(service, force=False)
    result = catalog(service).query(query=query[:500], filters=filters, allowed_scopes=allowed_scopes(service, scope), page=page)
    result["cache"] = cache_status(service)
    return result


def associations(service, summary_id):
    return service._state_store.get_document(NAMESPACE, summary_id) or {}


def validate_selection(service, summary_id, ids, scope, *, followup_id=""):
    from .portal_service import PortalError
    if ids is None and followup_id:
        ids = (associations(service, summary_id).get("followups") or {}).get(followup_id)
    if ids is None or ids == [] or ids == ():
        raise PortalError("请选择至少一台台账设备后保存跟进记录。")
    if not isinstance(ids, (list, tuple)) or len(ids) > 500 or any(
        not isinstance(item, str) or not re.fullmatch(r"rec[A-Za-z0-9_]+", item) for item in ids
    ):
        raise PortalError("台账设备选择无效，一次最多选择 500 台。")
    selected = set(ids)
    records = catalog(service).get_records(selected, allowed_scopes=allowed_scopes(service, scope))
    if selected != {row["record_id"] for row in records}:
        raise PortalError("所选台账设备不在当前权限范围或本地缓存中，请刷新设备台账后重新选择。")
    return list(dict.fromkeys(ids))


def remember_selection(service, summary_id, followup_id, ids):
    if ids is None:
        return
    # Callers hold the existing per-project lock, including recovery after a write.
    previous = associations(service, summary_id)
    service._state_store.put_document(NAMESPACE, summary_id, {
        "record_ids": list(dict.fromkeys([*(previous.get("record_ids") or []), *ids])),
        "followups": {**(previous.get("followups") or {}), followup_id: list(dict.fromkeys(ids))},
    })


def summary_fields(service, summary_id, raw_fields):
    ids = service._repair_management_record_ids(raw_fields.get("关联台账记录ID"))
    ids = list(dict.fromkeys([*ids, *(associations(service, summary_id).get("record_ids") or [])]))
    if not ids:
        return {}
    return {"台账关联状态": "已关联", "关联台账记录ID": ",".join(ids)}


def add_followup_selections(service, summary_id, records, scope):
    saved = associations(service, summary_id).get("followups") or {}
    ids = list(dict.fromkeys(item for row in records for item in saved.get(row["record_id"], [])))
    devices = {row["record_id"]: row for row in catalog(service).get_records(ids, allowed_scopes=allowed_scopes(service, scope))} if ids else {}
    for row in records:
        row["ledger_device_ids"] = [item for item in saved.get(row["record_id"], []) if item in devices]
        row["ledger_devices"] = [devices[item] for item in row["ledger_device_ids"] if item in devices]
