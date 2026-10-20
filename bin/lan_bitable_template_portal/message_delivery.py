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
        from upload_event_module.services.feishu_token_manager import token_manager
        credentials = token_manager._credentials()
        for refresh in (False, True):
            if token_manager._credentials() != credentials:
                raise DeliveryError("Qt 飞书凭据已变化，未重发，请核验原请求。", 503)
            token, error = robot._get_tenant_access_token(force_refresh=refresh)
            if error or not token:
                raise DeliveryError("飞书凭证暂不可用，请稍后继续发送。", 503)
            if token_manager._credentials() != credentials:
                raise DeliveryError("Qt 飞书凭据已变化，未重发，请核验原请求。", 503)
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

    def _self_person(self, owner, rows):
        from .personnel_password_login import DIRECTORY_NS as LOGIN_NS, DIRECTORY_KEY
        people = (self.store.get_document(LOGIN_NS, DIRECTORY_KEY) or {}).get("people") or {}
        links = [(rid, person) for rid, person in people.items() if owner in (person.get("login_ids") or [])]
        if links and (len(links) != 1 or links[0][1].get("selectable") is not True
                      or links[0][1].get("inactive") or links[0][1].get("disabled_reason")
                      or len(links[0][1].get("login_ids") or []) != 1):
            return None
        matches = [person for person in rows if person.get("open_id") == owner
                   or links and person.get("record_id") == links[0][0]]
        if len(matches) != 1 or not self._eligible(matches[0]):
            return None
        person = matches[0]
        if sum(row.get("open_id") == person["open_id"] for row in rows) != 1:
            return None
        trusted = people.get(person.get("record_id"))
        if trusted:
            identities = trusted.get("login_ids") or []
            if (trusted.get("selectable") is not True or trusted.get("inactive") or trusted.get("disabled_reason")
                    or len(identities) != 1 or sum(
                    identities[0] in (entry.get("login_ids") or []) for entry in people.values()) != 1):
                return None
        return person

    def directory(self):
        from upload_event_module.services.feishu_token_manager import token_manager
        with self._directory_lock:
            credentials = token_manager._credentials()
            app_id = credentials[0]
            cached = self._directory or self.store.get_document(DIRECTORY_NS, "staff") or {}
            shared = getattr(self.service, "_signature_people_cache", None) or {}
            refresh = bool(cached and cached.get("app_id") != app_id or shared and shared.get("app_id") != app_id)
            if not refresh and shared.get("app_id") == app_id and shared.get("loaded_ts", 0) > cached.get("loaded_at", 0):
                cached = {"app_id": app_id, "loaded_at": shared["loaded_ts"], "people": [self._person(p) for p in shared.get("people", [])]}
                self.store.put_document(DIRECTORY_NS, "staff", cached)
            if refresh or "people" not in cached or self.clock() - cached.get("loaded_at", 0) > 3600:
                people = self.service._load_signature_people(force=refresh)
                if token_manager._credentials() != credentials:
                    raise DeliveryError("Qt 飞书凭据已变化，请重新读取投递目录。", 503)
                shared = getattr(self.service, "_signature_people_cache", None)
                if isinstance(shared, dict) and shared.get("people") == people:
                    shared["app_id"] = app_id
                cached = {"app_id": app_id, "loaded_at": self.clock(), "people": [self._person(p) for p in people]}
                self.store.put_document(DIRECTORY_NS, "staff", cached)
            self._directory = cached
            return cached

    def recipients(self, owner, q=""):
        snapshot = self.directory()
        me = self._self_person(owner, snapshot["people"])
        terms = str(q or "").casefold().split()
        rows = []
        for person in snapshot["people"]:
            if not self._eligible(person):
                continue
            if not person.get("name") or not person.get("employee_no") and person is not me:
                continue
            if terms and not all(term in (person["name"] + " " + str(person["employee_no"])).casefold() for term in terms):
                continue
            rows.append({key: person.get(key, "") for key in ("record_id", "name", "employee_no", "account_nature")})
        return {"people": rows[:100], "total": len(rows), "loaded_at": snapshot["loaded_at"],
                "self": {key: me.get(key, "") for key in ("record_id", "name", "employee_no", "account_nature")} if me else None}

    def _selected(self, owner, ids):
        rows = self.directory()["people"]
        selected = []
        for identity in dict.fromkeys(ids):
            if identity == "__self__":
                me = self._self_person(owner, rows)
                matches = [me] if me is not None else []
            else:
                matches = [p for p in rows if p["record_id"] == identity]
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

    def _migrated_operation(self, owner, operation_id):
        matches = []
        for row in self.store.list_documents(NS):
            job = row.get('payload') or {}
            migration = job.get('identity_migration') or {}
            previous = migration.get('previous_owner') if isinstance(migration, dict) else None
            # This proof addresses an existing operation, never another login identity.
            if (job.get('owner') == owner and isinstance(previous, str) and previous
                    and job.get('id') == row.get('key') == digest([previous, operation_id])):
                if job.get('operation_id') not in (None, operation_id):
                    raise DeliveryError('原发送任务的操作号记录不一致，未创建新任务。', 409)
                matches.append(job)
        if len(matches) > 1:
            raise DeliveryError('原操作号对应多条迁移记录，请先核验；未创建新任务。', 409)
        return matches[0] if matches else None

    @staticmethod
    def _origin_app(job, part=None):
        if part and part.get('sender_app_id'):
            return part['sender_app_id']
        migration = job.get('identity_migration')
        values = [job.get('origin_app_id')]
        if isinstance(migration, dict):
            values.extend((migration.get('source_app_id'), migration.get('origin_app_id')))
        else:
            values.append(job.get('app_id'))
        origins = {value for value in values if isinstance(value, str) and value}
        return next(iter(origins)) if len(origins) == 1 else None

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
            if old and (old.get('owner') != owner or old.get('id') != identity):
                raise DeliveryError('发送任务不存在或无权访问。', 404)
            if not old:
                old = self._migrated_operation(owner, model.operation_id)
            if old:
                if old.get('operation_id') not in (None, model.operation_id) or old.get("fingerprint") != stamp:
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
            job = {"id": identity, "owner": owner, "operation_id": model.operation_id,
                   "origin_app_id": self.directory()['app_id'], "fingerprint": stamp, "text": model.text, "recipients": selected,
                   "files": attachments, "parts": {}, "status": "queued", "created_at": self.clock(), "error": ""}
            self.store.put_document(NS, identity, job)
            return self.public(job)

    @staticmethod
    def public(job):
        return {"delivery_id": job["id"], "status": job["status"], "created_at": job["created_at"], "error": job.get("error", ""),
                "recipients": [{k: p.get(k, "") for k in ("name", "employee_no")} for p in job["recipients"]],
                "files": [{k: f[k] for k in ("name", "size")} for f in job["files"]],
                "sent_count": sum(p.get("status") == "sent" for p in job["parts"].values()),
                "failed_count": sum(p.get("status") in {"failed", "sending", "uncertain"} for p in job["parts"].values())}

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
            if job["status"] in {"completed", "migration_blocked"}:
                return
            migration = job.get('identity_migration') or {}
            if isinstance(migration, dict) and (migration.get('blocked') or migration.get('archived')
                                                 or migration.get('resume_allowed') is False):
                job.update(status='migration_blocked', error='原发送任务已暂停待核验，历史回执已保留；未自动重发。')
                self.store.put_document(NS, identity, job)
                return
            from upload_event_module.services.feishu_token_manager import token_manager
            credentials = token_manager._credentials()
            app_id = credentials[0]
            if not app_id:
                raise DeliveryError('当前飞书应用未就绪，原发送任务已保留。', 503)
            labels = (['text'] if job['text'].strip() else []) + ['file:' + f['sha256'] for f in job['files']]
            expected = {digest([person['open_id'], label]) for person in job['recipients'] for label in labels}
            if any((part.get('status') in {'sending', 'uncertain'} and self._origin_app(job, part) != app_id)
                   or (job.get('identity_migration') and key not in expected
                       and part.get('status') in {'sent', 'sending', 'uncertain'}) for key, part in job['parts'].items()):
                job.update(status='migration_blocked', error='原应用的发送结果或分片身份尚未核实，请先在飞书核验；未自动重发。')
                self.store.put_document(NS, identity, job)
                return
            job.update(status="running", error="")
            self.store.put_document(NS, identity, job)
            for person in job["recipients"]:
                if any(job['parts'].get(digest([person['open_id'], label]), {}).get('status') != 'sent' for label in labels):
                    self._verify_person(owner, person)
            for file in job["files"]:
                if all(job['parts'].get(digest([person['open_id'], 'file:' + file['sha256']]), {}).get('status') == 'sent'
                       for person in job['recipients']):
                    continue
                if file.get('file_key') and (file.get('file_key_app_id') or self._origin_app(job)) != app_id:
                    file.pop('file_key')
                    file.pop('file_key_app_id', None)
                if not file.get("file_key"):
                    path = Path(file['path'])
                    if (path.stat().st_size != file['size'] or not 0 < file['size'] <= MAX_FILE
                            or hashlib.sha256(path.read_bytes()).hexdigest() != file['sha256']):
                        raise DeliveryError('原附件内容已变化，未重新上传或发送。', 409)
                    if token_manager._credentials() != credentials:
                        raise DeliveryError('Qt飞书凭据已变化，原任务已保留，未继续发送。', 503)
                    file["file_key"] = self.transport.upload(file)
                    file['file_key_app_id'] = app_id
                    self.store.put_document(NS, identity, job)
            parts = ([('text', {"text": job["text"]})] if job["text"].strip() else []) + [
                ('file:' + file["sha256"], {"file_key": file.get("file_key")}) for file in job["files"]]
            failures = []
            for person in job["recipients"]:
                for label, content in parts:
                    key = digest([person["open_id"], label])
                    part = job["parts"].setdefault(key, {})
                    if part.get("status") == "sent":
                        continue
                    if part.get("status") in {"sending", "uncertain"}:
                        attempted = part.get('attempted_at')
                        if type(attempted) not in (int, float) or not 0 <= self.clock() - attempted <= 3500:
                            failures.append("上次发送结果未确认且已超出去重时限，请先在飞书核对；未重复发送。")
                            continue
                    if token_manager._credentials() != credentials:
                        raise DeliveryError('Qt飞书凭据已变化，原任务已保留，未继续发送。', 503)
                    part.update(status="sending", sender_app_id=app_id,
                                attempted_at=part.get("attempted_at") or self.clock())
                    self.store.put_document(NS, identity, job)
                    try:
                        message_id = self.transport.send(person["open_id"], 'text' if label == 'text' else 'file', content,
                                                         part.get('message_uuid') or str(uuid.uuid5(uuid.NAMESPACE_URL, identity + key)))
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
