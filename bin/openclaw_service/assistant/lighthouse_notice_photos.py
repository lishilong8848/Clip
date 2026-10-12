"""Assistant photo controls and staging through the native notice upload API."""
import asyncio
import copy
import time

from .lighthouse_ai import AssistantError

NOTICE_WRITES = {"POST /api/workbench-actions", "POST /api/maintenance-actions"}


def photo_fields(index, work_type):
    return [{"name": f"step{index}.notice_{kind}_photos", "path": f"notice_{kind}_photos",
             "section": "body", "operation_index": index, "type": "file",
             "label": "现场照片" if kind == "site" else "阿里确认截图",
             "native_notice_photos": kind, "value": [], "required": False,
             "accept": ".png,.jpg,.jpeg,.webp,.gif,.bmp", "max_bytes": 8 * 1024 * 1024,
             "purpose": "notice_photo",
             "maxItems": 10 if kind == "site" else 1}
            for kind in (["site", "ali"] if work_type == "change" else ["site"])]


def restore_photo_form(plan, index, message, *, required=False):
    forms = copy.deepcopy(plan.get("_review_fields") or plan.get("fields") or [])
    operation = plan["operations"][index]
    existing = [field for field in forms if field.get("native_notice_photos") and field["operation_index"] == index]
    if not existing:
        existing = photo_fields(index, operation.get("body", {}).get("work_type"))
        forms.extend(existing)
    for field in existing:
        field["value"] = list(plan.get("_notice_files", {}).get(str(index), {}).get(field["native_notice_photos"], []))
        field.pop("_edit_value", None)
        if required and field["native_notice_photos"] == "site":
            field["required"] = True
    plan.update(status="needs_input", version=plan["version"] + 1, fields=forms, error="", explanation=message)


def select_photos(agent, actor, plan, field, value):
    if not isinstance(value, list) or len(value) > field["maxItems"] or any(not isinstance(item, str) for item in value) or len(set(value)) != len(value):
        raise AssistantError("请选择有效通告图片，不能重复或超过数量限制。")
    operation = plan["operations"][field["operation_index"]]
    for identity in value:
        agent._check_plan_file(actor, operation, identity)
        if identity not in plan["file_ids"]:
            plan["file_ids"].append(identity)
    plan.setdefault("_notice_files", {}).setdefault(str(field["operation_index"]), {})[field["native_notice_photos"]] = list(value)


async def stage_photos(agent, actor, plan, index, operation, request):
    selected = plan.get("_notice_files", {}).get(str(index), {})
    receipts = plan.setdefault("_notice_uploads", {}).setdefault(str(index), {})
    for kind, identities in selected.items():
        uploaded = []
        for identity in identities:
            item = await asyncio.to_thread(agent._check_plan_file, actor, operation, identity)
            receipt = receipts.get(identity) or {}
            if receipt.get("expires_at", 0) <= time.time() + 30:
                result = await agent._invoke(actor, {"api_id": "POST /api/notice-attachments",
                    "params": {"file_name": item["name"], "kind": kind}, "files": {"file": [identity]}}, request, uploads=True)
                data = result.get("_raw", result.get("data")) or {}
                if not result.get("ok") or not data.get("upload_id") or not isinstance(data.get("expires_at"), (int, float)) or data["expires_at"] <= time.time() + 30:
                    raise AssistantError(result.get("error") or "通告照片上传未完成，请重新添加图片。")
                receipt = receipts[identity] = {key: data[key] for key in ("upload_id", "expires_at")}
                await agent._durable_save(actor, plan)
            uploaded.append({"upload_id": receipt["upload_id"], "file_name": item["name"], "mime_type": item["mime"]})
        if uploaded:
            key = "extra_images" if kind == "site" else "ali_confirmation_images"
            operation.setdefault("body", {}).setdefault("patch", {})[key] = uploaded
