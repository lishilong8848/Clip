"""Cached staff recipients and durable, owner-scoped Feishu deliveries."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re
import threading
import time
import uuid

from pydantic import BaseModel, ConfigDict, Field

NS = "message_delivery"
DIRECTORY_NS = "message_recipient_directory"
MAX_FILE = 20 * 1024 * 1024
MAX_TOTAL = 100 * 1024 * 1024


class DeliveryError(RuntimeError):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


class MessageDeliveryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operation_id: str = Field(min_length=8, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    recipient_ids: list[str] = Field(min_length=1, max_length=10)
    text: str = Field(default="", max_length=50000)
    retry_attempt: int = Field(default=0, ge=0, le=1000)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


class FeishuDeliveryTransport:
    def _call(self, path, *, payload=None, file=None):
        from upload_event_module.services import robot_webhook as robot
        for refresh in (False, True):
            token, error = robot._get_tenant_access_token(force_refresh=refresh)
            if error:
                raise DeliveryError("飞书凭证暂不可用，请稍后继续发送。", 503)
            headers = {"Authorization": "Bearer " + token}
            url = "https://open.feishu.cn/open-apis/im/v1/" + path
            if file:
                result = robot._HTTP_CLIENT.request_file_json("POST", url, headers=headers,
                    file_path=file["path"], file_name=file["name"], data={"file_type": "stream", "file_name": file["name"]}, retries=0)
            else:
                result = robot._HTTP_CLIENT.request_json("POST", url, headers=headers,
                    params={"receive_id_type": "open_id"}, json_payload=payload, retries=0)
            if not robot._is_token_error_result(result) or refresh:
                break
        if result.get("code") != 0:
            raise DeliveryError("飞书未接受消息：" + str(result.get("msg") or result.get("code"))[:240])
        return result.get("data") or {}

    def upload(self, file):
        key = self._call("files", file=file).get("file_key")
        if not key:
            raise DeliveryError("飞书文件上传未返回有效回执。", 502)
        return key

    def send(self, recipient, kind, content, message_uuid):
        result = self._call("messages", payload={"receive_id": recipient, "msg_type": kind,
            "content": json.dumps(content, ensure_ascii=False), "uuid": message_uuid})
        if not result.get("message_id"):
            raise DeliveryError("飞书发送回执不完整，结果尚未确认。", 502)
        return result["message_id"]


class MessageDelivery:
    def __init__(self, service, transport=None, clock=time.time):
        self.service, self.store = service, service._state_store
        self.root = Path(self.store.db_path).parent / "message_delivery"
        self.transport = transport or FeishuDeliveryTransport()
        self.clock = clock
        self._directory_lock = threading.RLock()
        self._directory = None
        self._lock = threading.RLock()
        self._running = set()

    @staticmethod
    def _person(row):
        return {key: row.get(key, "") for key in ("record_id", "name", "employee_no", "building", "account_nature", "open_id", "can_receive_message")}

    @staticmethod
    def _eligible(person):
        return bool(person.get("can_receive_message") and person.get("open_id") and
                    str(person.get("account_nature") or "").strip().upper() == "VNET")

    def directory(self):
        with self._directory_lock:
            cached = self._directory or self.store.get_document(DIRECTORY_NS, "staff") or {}
            shared = getattr(self.service, "_signature_people_cache", None) or {}
            if shared.get("loaded_ts", 0) > cached.get("loaded_at", 0):
                cached = {"loaded_at": shared["loaded_ts"], "people": [self._person(p) for p in shared.get("people", [])]}
                self.store.put_document(DIRECTORY_NS, "staff", cached)
            if "people" not in cached or self.clock() - cached.get("loaded_at", 0) > 3600:
                people = self.service._load_signature_people(force=False)
                cached = {"loaded_at": self.clock(), "people": [self._person(p) for p in people]}
                self.store.put_document(DIRECTORY_NS, "staff", cached)
            self._directory = cached
            return cached

    def recipients(self, owner, q=""):
        snapshot = self.directory()
        terms = str(q or "").casefold().split()
        rows = []
        for person in snapshot["people"]:
            if not self._eligible(person):
                continue
            if not person.get("name") or not person.get("employee_no") and person["open_id"] != owner:
                continue
            if terms and not all(term in (person["name"] + " " + str(person["employee_no"])).casefold() for term in terms):
                continue
            rows.append({key: person.get(key, "") for key in ("record_id", "name", "employee_no", "account_nature")})
        me = next((p for p in snapshot["people"] if p.get("open_id") == owner and self._eligible(p)), None)
        return {"people": rows[:100], "total": len(rows), "loaded_at": snapshot["loaded_at"],
                "self": {key: me.get(key, "") for key in ("record_id", "name", "employee_no", "account_nature")} if me else None}

    def _selected(self, owner, ids):
        rows = self.directory()["people"]
        selected = []
        for identity in dict.fromkeys(ids):
            matches = [p for p in rows if (p.get("open_id") == owner if identity == "__self__" else p["record_id"] == identity)]
            if len(matches) != 1:
                raise DeliveryError("收件人不存在或有重复身份，请按姓名和工号重新选择。")
            person = matches[0]
            if not self._eligible(person):
                raise DeliveryError("该人员不可直接接收应用消息，不能发送。")
            if identity != "__self__" and (not person.get("name") or not person.get("employee_no")):
                raise DeliveryError("收件人姓名或工号不完整，请先维护人员目录。")
            if not any(p["open_id"] == person["open_id"] for p in selected):
                selected.append(dict(person))
        return selected

    def _verify_person(self, owner, person):
        fields = self.service.signature_management._fields("staff", person["record_id"])
        s = self.service
        from .portal_service import SIGNATURE_INACTIVE_FIELD, SIGNATURE_USER_FIELD, SIGNATURE_NAME_FIELD
        open_id = s._signature_user_info(fields.get(SIGNATURE_USER_FIELD)).get("open_id") or s._signature_open_id_from_any(
            next((fields[key] for key in ("openid", "OpenID", "open_id", "飞书OpenID", "飞书 openid", "飞书openid", "人员openid") if fields.get(key)), None))
        if s._signature_person_inactive(fields.get(SIGNATURE_INACTIVE_FIELD)) or s._mop_field_text(fields, ["账号性质"]).upper() != "VNET" or not open_id:
            raise DeliveryError("该人员已离职/异动或不可直接接收应用消息。")
        name = s._mop_field_text(fields, [SIGNATURE_NAME_FIELD]) or s._signature_user_info(fields.get(SIGNATURE_USER_FIELD)).get("name")
        if open_id != person["open_id"] or name != person["name"] or s._mop_field_text(fields, ["员工工号", "工号"]) != person["employee_no"]:
            raise DeliveryError("收件人身份已变化，请重新选择；未改发给其他人。")

    def prepare(self, owner, payload, files):
        model = MessageDeliveryRequest.model_validate(payload)
        if not model.text.strip() and not files:
            raise DeliveryError("请填写发送内容或选择文件。")
        if len(files) > 10 or sum(len(content) for _, content in files) > MAX_TOTAL:
            raise DeliveryError("最多10个文件，合计不超过100MiB。", 413)
        if any(not content or len(content) > MAX_FILE for _, content in files):
            raise DeliveryError("文件须非空，单个不超过20MiB。", 413)
        normalized = [(re.sub(r"[\x00-\x1f]", "", str(name).replace("\\", "/").split("/")[-1])[:180] or "文件", content) for name, content in files]
        stamp = digest({"text": model.text, "recipients": model.recipient_ids,
                        "files": [(name, hashlib.sha256(content).hexdigest()) for name, content in normalized]})
        identity = digest([owner, model.operation_id])
        with self._lock:
            old = self.store.get_document(NS, identity)
            if old:
                if old["fingerprint"] != stamp:
                    raise DeliveryError("原发送内容已变化，请重新核对后创建新的发送任务。", 409)
                return self.public(old)
            selected = self._selected(owner, model.recipient_ids)
            directory = self.root / identity
            directory.mkdir(parents=True, exist_ok=True)
            attachments = []
            seen = set()
            for name, content in normalized:
                sha = hashlib.sha256(content).hexdigest()
                if sha in seen:
                    continue
                seen.add(sha)
                path = directory / sha
                path.write_bytes(content)
                attachments.append({"name": name, "sha256": sha, "path": str(path), "size": len(content)})
            job = {"id": identity, "owner": owner, "fingerprint": stamp, "text": model.text, "recipients": selected,
                   "files": attachments, "parts": {}, "status": "queued", "created_at": self.clock(), "error": ""}
            self.store.put_document(NS, identity, job)
            return self.public(job)

    @staticmethod
    def public(job):
        return {"delivery_id": job["id"], "status": job["status"], "created_at": job["created_at"], "error": job.get("error", ""),
                "recipients": [{k: p.get(k, "") for k in ("name", "employee_no")} for p in job["recipients"]],
                "files": [{k: f[k] for k in ("name", "size")} for f in job["files"]],
                "sent_count": sum(p.get("status") == "sent" for p in job["parts"].values()),
                "failed_count": sum(p.get("status") in {"failed", "sending"} for p in job["parts"].values())}

    def get(self, owner, identity):
        with self._lock:
            job = self.store.get_document(NS, identity)
            if not job or job.get("owner") != owner:
                raise DeliveryError("发送任务不存在或无权访问。", 404)
            result = self.public(job)
            if job['status'] == 'running' and identity not in self._running:
                result.update(status='interrupted', error='上次发送执行已中断，可继续原任务；已发送部分不会重复发送。')
            return result

    def run(self, owner, identity):
        with self._lock:
            self.get(owner, identity)
            if identity in self._running:
                return
            self._running.add(identity)
        try:
            job = self.store.get_document(NS, identity)
            if job["status"] == "completed":
                return
            job.update(status="running", error="")
            self.store.put_document(NS, identity, job)
            labels = (['text'] if job['text'].strip() else []) + ['file:' + f['sha256'] for f in job['files']]
            for person in job["recipients"]:
                if any(job['parts'].get(digest([person['open_id'], label]), {}).get('status') != 'sent' for label in labels):
                    self._verify_person(owner, person)
            for file in job["files"]:
                if not file.get("file_key"):
                    file["file_key"] = self.transport.upload(file)
                    self.store.put_document(NS, identity, job)
            parts = ([('text', {"text": job["text"]})] if job["text"].strip() else []) + [
                ('file:' + file["sha256"], {"file_key": file["file_key"]}) for file in job["files"]]
            failures = []
            for person in job["recipients"]:
                for label, content in parts:
                    key = digest([person["open_id"], label])
                    part = job["parts"].setdefault(key, {})
                    if part.get("status") == "sent":
                        continue
                    if part.get("status") == "sending" and self.clock() - part["attempted_at"] > 3500:
                        failures.append("上次发送结果未确认且已超出去重时限，请先在飞书核对；未重复发送。")
                        continue
                    part.update(status="sending", attempted_at=part.get("attempted_at") or self.clock())
                    self.store.put_document(NS, identity, job)
                    try:
                        message_id = self.transport.send(person["open_id"], 'text' if label == 'text' else 'file', content,
                                                         str(uuid.uuid5(uuid.NAMESPACE_URL, identity + key)))
                        part.update(status="sent", message_id=message_id)
                    except Exception as exc:
                        if isinstance(exc, DeliveryError) and exc.status < 500:
                            part["status"] = "failed"
                        failures.append(person['name'] + '：' + (str(exc) if isinstance(exc, DeliveryError) else '发送结果未确认，请继续原任务。'))
                    self.store.put_document(NS, identity, job)
            job.update(status="failed" if failures else "completed", error="；".join(dict.fromkeys(failures)))
            self.store.put_document(NS, identity, job)
        except Exception as exc:
            job = self.store.get_document(NS, identity)
            job.update(status="failed", error=str(exc) if isinstance(exc, DeliveryError) else "消息发送未完成，原内容已保留，可继续原任务。")
            self.store.put_document(NS, identity, job)
        finally:
            with self._lock:
                self._running.discard(identity)
