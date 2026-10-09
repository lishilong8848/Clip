"""One-time, explicit import into the link directory. Never run at startup."""
import argparse
import hashlib
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lan_bitable_template_portal.link_directory import LinkRemote, SCHEMA, identity, to_fields, validate_item


def import_rows(remote, values, apply=False):
    rows = [validate_item(value) for value in values]
    keys = [identity(row["url"]) for row in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("导入数据存在重复表链接")
    fields, records = remote.list_all("fields"), remote.list_all()
    existing = {field["field_name"]: field for field in fields}
    for name, kind in SCHEMA.items():
        if name in existing and existing[name]["type"] != kind:
            raise ValueError("字段类型不匹配：" + name)
    known = set()
    blank = []
    for record in records:
        data = record.get("fields") or {}
        if not any(data.values()):
            blank.append(record["record_id"])
            continue
        link = data.get("链接")
        if isinstance(link, dict) and link.get("link"):
            known.add(identity(link["link"]))
        else:
            raise ValueError("云端已有非目录内容，停止导入以免覆盖")
    missing = [row for row in rows if identity(row["url"]) not in known]
    print(f"已存在 {len(known)} 条，待导入 {len(missing)} 条，可复用空行 {len(blank)} 条", flush=True)
    if not apply:
        return
    if "表名" not in existing:
        primary = next((field for field in fields if field.get("is_primary")), None)
        if primary and primary["type"] == 1 and not known:
            remote.request("PUT", "fields/" + primary["field_id"], {"field_name": "表名", "type": 1})
            existing["表名"] = primary
    for name, kind in SCHEMA.items():
        if name not in existing:
            remote.request("POST", "fields", {"field_name": name, "type": kind})
    count = min(len(blank), len(missing))
    if count:
        remote.request("POST", "records/batch_update", {"records": [
            {"record_id": record_id, "fields": to_fields(row)} for record_id, row in zip(blank[:count], missing[:count])]})
    pending = missing[count:]
    for start in range(0, len(pending), 100):
        batch = {"records": [{"fields": to_fields(row)} for row in pending[start:start + 100]]}
        digest = hashlib.sha256(json.dumps(batch, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        token = str(uuid.UUID(digest[:32], version=4))
        remote.request("POST", "records/batch_create", batch, {"client_token": token})
        print(f"导入进度 {count + min(start + 100, len(pending))}/{len(missing)}", flush=True)
    from lan_bitable_template_portal.link_directory import from_record
    after = [from_record(row) for row in remote.list_all() if any((row.get("fields") or {}).values())]
    actual = {identity(row["url"]): row for row in after}
    if len(actual) != len(after) or not set(keys).issubset(actual):
        raise ValueError("回读数量不一致，请核对原导入结果，不要重复新增")
    for row in missing:
        saved = actual[identity(row["url"])]
        if any(saved[key] != value for key, value in row.items()):
            raise ValueError("回读字段不一致：" + row["name"])
    print(f"回读核验通过：{len(after)} 条链接，无重复，新增字段与导入内容一致。", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    from upload_event_module.services.http_client import FeishuHttpClient
    from upload_event_module.services.feishu_token_manager import token_manager
    client = FeishuHttpClient(retries=0)
    service = SimpleNamespace(_http_client=client, _auth_headers=lambda: {"Authorization": "Bearer " + token_manager.get_tenant_token()})
    import_rows(LinkRemote(service), json.loads(args.source.read_text(encoding="utf-8")), args.apply)
