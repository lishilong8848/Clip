"""Feishu attachment parity through the existing assistant file/knowledge APIs.

No business executor is involved.  This module only bridges Feishu's official
resource endpoints and the portal's /api/assistant/files and knowledge APIs.

Hook contract (caller side):
  * parse_resources(message, content) -> list[dict]
        message: parsed Feishu message (message_id, sender_id, union_id,
                 chat_id, chat_type, question).  content: the JSON-decoded
                 raw message content.
        Returns metadata for image / file / post-inline images.
  * handle_files(bot, message, request, actor, conversation)
        bot has runtime.state_store, ready(), messenger.client/headers and
        _call(request, actor, path, body, *, method, params).  message must
        also carry the raw parsed ``content`` under message["content"].
        Returns:
          None                              -> not a resource command
          {"text": ...}                     -> reply to send
          {"file_ids": [...], "question": .}-> continue model flow with files
"""
from __future__ import annotations

import asyncio
import hashlib
import mimetypes
import re
import time
from pathlib import Path

from openclaw_service.assistant.lighthouse_ai import AssistantError, safe_text

NAMESPACE = "feishu_assistant_attachments"
CONFIRM_READ = "确认读取附件"
CONFIRM_KB = "确认知识库操作"
MAX_ITEMS = 10
MAX_FILE = 20 * 1024 * 1024
MAX_BATCH = 100 * 1024 * 1024
USE_HOURS = 3600
_KEY = re.compile(r"(?:img|file)_[A-Za-z0-9_-]{1,200}")
NOT_QTEXT = {"[不支持的消息格式]", CONFIRM_READ}
_HEAVY_LIMIT = 4
_KB_PAGE_GUARD = 25

_HEAVY_SEM = None
_HEAVY_SEM_LOOP = None


def _heavy():
    """Bounded concurrency for heavy attachment download/upload only."""
    global _HEAVY_SEM, _HEAVY_SEM_LOOP
    loop = asyncio.get_running_loop()
    if _HEAVY_SEM is None or _HEAVY_SEM_LOOP is not loop:
        _HEAVY_SEM = asyncio.Semaphore(_HEAVY_LIMIT)
        _HEAVY_SEM_LOOP = loop
    return _HEAVY_SEM


def sanitize_name(name, fallback="附件"):
    name = str(name or "").replace("\\", "/").split("/")[-1]
    name = re.sub(r"[\x00-\x1f]", "", name).strip()
    return (name or fallback)[:180]


def _key_kind(key, kind):
    key = str(key)
    prefix = key.split("_", 1)[0] if "_" in key else ""
    if prefix == "img":
        return kind == "image"
    if prefix == "file":
        return kind == "file"
    return False


def parse_resources(message, content):
    """Return attachment metadata for image/file/post-inline images.

    All valid, non-duplicate resources are returned (never silently truncated);
    the caller must enforce MAX_ITEMS explicitly so an over-limit message can
    fail loudly instead of being silently sliced.
    """
    if not isinstance(content, dict):
        return []
    mid = str(message.get("message_id") or "")
    if not re.fullmatch(r"om_[A-Za-z0-9]+", mid):
        return []
    found = []

    def push(kind, key, name=None):
        if not key or not _KEY.fullmatch(str(key)) or not _key_kind(key, kind):
            return
        if any(item["key"] == str(key) for item in found):
            return
        found.append({"kind": kind, "key": str(key),
                      "name": sanitize_name(name) if name else None,
                      "original_mid": mid, "type": kind})

    post = content.get("zh_cn") if isinstance(content.get("zh_cn"), dict) else content
    if isinstance(post.get("content"), list):
        for row in post["content"]:
            cells = row if isinstance(row, list) else [row]
            for cell in cells:
                if isinstance(cell, dict) and cell.get("tag") == "img":
                    push("image", cell.get("image_key"))
    push("image", content.get("image_key"))
    push("file", content.get("file_key"), content.get("file_name"))
    return found


def _identity(actor, message):
    actor_id = str(actor.get("id") or message.get("sender_id") or "")
    channel = str(actor.get("channel") or "feishu:" + str(message.get("chat_id") or ""))
    owner = hashlib.sha256((actor_id + ":" + channel).encode()).hexdigest()
    return owner, actor_id, channel


def _pending_key(owner, message_id):
    # One pending attachment bundle per actor+channel; the message id only
    # distinguishes redelivery of the same bundle (idempotent) from a new one.
    return owner


def _kb_key(owner, op):
    # One pending KB operation per actor+channel.
    return "kb:" + owner


async def handle_files(bot, message, request, actor, conversation):
    store = getattr(bot, "runtime", None)
    store = getattr(store, "state_store", None) if store else None
    if store is None:
        return None
    content = message.get("content") or {}
    question = (message.get("question") or "").strip()
    owner, actor_id, channel = _identity(actor, message)

    resources = [r for r in parse_resources(message, content)
                 if r.get("original_mid") == message.get("message_id")]
    if resources:
        if len(resources) > MAX_ITEMS:
            return {"text": f"一次最多接收{MAX_ITEMS}个附件，本次收到{len(resources)}个，请分批发送。"}
        # Auto photo attachment ONLY accepts real image resources. An explicit
        # caption (现场照片 / 资料截图 / 阿里… — but never generic 图片 / 识图 / OCR)
        # may attach to any editable notice photo field, required or optional;
        # a plan that is still waiting on a required-empty photo auto-attaches an
        # uncaptioned image. Ordinary OCR/recognition images fall through.
        image_only = all(r["kind"] == "image" for r in resources)
        if image_only and _explicit_photo_intent(question):
            photo_target = _photo_attach_target(conversation, required_only=False)
            caption_type = _caption_photo_type(question)
            if caption_type and photo_target and photo_target[0] == "choose":
                typed = [(p, f) for (p, f) in photo_target[1]
                         if str(f.get("native_notice_photos")) == caption_type]
                if typed:
                    photo_target = ("choose", typed) if len(typed) > 1 else ("unique", typed[0][0], typed[0][1])
            if photo_target and photo_target[0] == "unique":
                plan, field = photo_target[1], photo_target[2]
                return await _attach_photos_to_plan(bot, message, request, actor, store,
                                                    owner, actor_id, channel, resources,
                                                    question, conversation, plan, field)
            if photo_target and photo_target[0] == "choose":
                return await _stash_photos_for_selection(bot, message, request, actor, store,
                                                         owner, actor_id, channel, resources,
                                                         conversation, photo_target[1], question)
        target = _photo_attach_target(conversation, required_only=True) if question in {"", "请读取附件，结合当前会话继续处理。", "[图片]"} else None
        if target and target[0] == "unique" and image_only:
            plan, field = target[1], target[2]
            if _photo_sent_count(field) < 1:
                return await _attach_photos_to_plan(bot, message, request, actor, store,
                                                    owner, actor_id, channel, resources,
                                                    question, conversation, plan, field)
        if target and target[0] == "choose" and image_only:
            # Never guess between multiple plans or multiple site/ali photo
            # fields: stage the image receipts and ask the user to pick a stable
            # plan+photo-type number. Every site/ali is selectable here.
            return await _stash_photos_for_selection(bot, message, request, actor, store,
                                                     owner, actor_id, channel, resources,
                                                     conversation, target[1], question)
        return await _take_pending(bot, message, store, owner, actor_id, channel, resources, question, conversation.get('conversation_id'))

    if question in {'取消读取附件', '取消知识库操作'}:
        key = owner if question == '取消读取附件' else _kb_key(owner, {})
        await asyncio.to_thread(store.delete_document, NAMESPACE, key)
        return {'text': '已取消待确认操作。已经上传或完成的文件不会被撤销。'}

    if question == CONFIRM_READ:
        pending = await asyncio.to_thread(store.get_document, NAMESPACE, owner)
        if pending and pending.get('conversation_id') and pending['conversation_id'] != conversation.get('conversation_id'):
            return {'text': '原会话已清空，附件确认已失效，请重新发送附件。'}
        async with _heavy():
            return await _confirm_read(bot, message, request, actor, store, owner, actor_id, channel)

    selected_match = re.fullmatch(r"补照片计划\s*(\d+)", question)
    if selected_match:
        return await _attach_stashed_photos(bot, message, request, actor, store, owner, actor_id, channel,
                                            conversation, int(selected_match.group(1)))

    for preamble, action in (("将附件加入知识库：", "add"), ("更新知识库：", None),
                             ("删除知识库：", "delete"), ("恢复知识库：", "restore")):
        if question.startswith(preamble):
            target = question[len(preamble):].strip()
            if not target:
                return {"text": "命令后需要填写文件名，例如：" + preamble + "文件名"}
            return await _prepare_kb(bot, message, request, actor, conversation, store,
                                     owner, actor_id, channel, action, preamble, target)

    if question == CONFIRM_KB:
        pending = await asyncio.to_thread(store.get_document, NAMESPACE, _kb_key(owner, {}))
        if pending and pending.get('conversation_id') and pending['conversation_id'] != conversation.get('conversation_id'):
            return {'text': '原会话已清空，知识库操作确认已失效，请重新发起。'}
        async with _heavy():
            return await _confirm_kb(bot, message, request, actor, store, owner, actor_id, channel)
    return None


async def _take_pending(bot, message, store, owner, actor_id, channel, resources, question, conversation_id=None):
    key = _pending_key(owner, message["message_id"])
    now = time.time()
    existing = await asyncio.to_thread(store.get_document, NAMESPACE, key)
    if existing and existing.get("message_id") == message.get("message_id"):
        # Redelivery of the same bundle: keep imported receipts, refresh expiry.
        existing["expires"] = now + USE_HOURS
        existing["question"] = question or existing.get("question")
        await asyncio.to_thread(store.put_document, NAMESPACE, key, existing)
    else:
        # New upload replaces the single pending bundle for this actor+channel.
        pending = {
            "owner": owner, "actor": actor_id, "channel": channel,
            "message_id": message["message_id"], "question": question,
            "resources": resources, "expires": now + USE_HOURS, "imported": False,
            'conversation_id': conversation_id,
        }
        await asyncio.to_thread(store.put_document, NAMESPACE, key, pending)
    names = "、".join(r["name"] or ("图片" if r["kind"] == "image" else "文件") for r in resources)
    return {"text": f"已收到{len(resources)}个附件（{names}）。回复“{CONFIRM_READ}”以读取并纳入本会话"
                    f"（每个≤20MiB、合计≤100MiB，仅当前会话使用，不自动加入公司知识库）；也可回复“取消读取附件”。"}


async def _confirm_read(bot, message, request, actor, store, owner, actor_id, channel):
    now = time.time()
    key = _pending_key(owner, message.get("message_id"))
    doc = await asyncio.to_thread(store.get_document, NAMESPACE, key)
    if not doc:
        return {"text": "当前没有待确认读取的附件（可能已过期或不属于当前会话），请重新发送附件。"}
    if doc.get("actor") != actor_id or doc.get("channel") != channel or doc.get("expires", 0) <= now:
        if doc.get("expires", 0) <= now:
            await asyncio.to_thread(store.delete_document, NAMESPACE, key)
        return {"text": "当前没有待确认读取的附件（可能已过期或不属于当前会话），请重新发送附件。"}

    # Whole-bundle byte budget includes already-imported sizes.
    resources = doc.get("resources", [])
    running_total = sum(int(r.get("size", 0) or 0) for r in resources if r.get("file_id"))
    file_ids = []
    for res in resources:
        if res.get("file_id"):
            file_ids.append(res["file_id"])
            continue
        content = await _download(bot, message, res)
        size = len(content)
        running_total += size
        if running_total > MAX_BATCH:
            raise AssistantError("附件合计超过100MiB，请分批发送。", 413)
        if size > MAX_FILE:
            raise AssistantError("附件超过20MiB，未读取。", 413)
        name = _upload_name(res, content)
        fid = await _upload(bot, request, actor, name, content)
        res["file_id"] = fid
        res["size"] = size
        if not res.get("name"):
            res["name"] = name
        file_ids.append(fid)
        # Persist each imported receipt immediately so a later file failure does
        # not lose the already-imported ones.
        doc["imported"], doc["imported_at"] = True, now
        await asyncio.to_thread(store.put_document, NAMESPACE, key, doc)

    stored_q = doc.get("question") or ""
    question = (stored_q if stored_q and stored_q not in NOT_QTEXT
                else "请读取以上附件，结合当前会话继续处理；这些内容仅在本会话使用，不自动加入公司知识库。")
    return {"file_ids": [f for f in file_ids if f], "question": question}


async def _download(bot, message, res):
    client = await bot.messenger.client()
    headers = await bot.messenger.headers()
    url = ("https://open.feishu.cn/open-apis/im/v1/messages/" + res["original_mid"]
           + "/resources/" + res["key"])
    kind = "image" if res["kind"] == "image" else "file"
    buffer, total = bytearray(), 0
    async with client.stream("GET", url, headers=headers, params={"type": kind},
                                 follow_redirects=False) as response:
        if response.status_code != 200:
            raise AssistantError("飞书附件下载未完成，请重新发送附件。", 502)
        async for chunk in response.aiter_bytes():
            total += len(chunk)
            if total > MAX_FILE:
                raise AssistantError("附件超过20MiB，未读取。", 413)
            buffer.extend(chunk)
    if not buffer:
        raise AssistantError("飞书附件为空，未读取。", 422)
    return bytes(buffer)


def _image_extension(content):
    if content[:8] == b"\x89PNG\r\n\x1a\n":
        return ".png"
    if content[:3] == b"\xff\xd8\xff":
        return ".jpg"
    if content[:6] in (b"GIF87a", b"GIF89a"):
        return ".gif"
    if content[:4] == b"RIFF" and content[8:12] == b"WEBP":
        return ".webp"
    if content[:2] == b"BM":
        return ".bmp"
    raise AssistantError("无法识别图片的真实格式，请重新发送图片。", 422)


def _upload_name(res, content):
    if res["kind"] == "image":
        token = hashlib.sha1((res["original_mid"] + "|" + res["key"]).encode()).hexdigest()[:10]
        return "feishu_image_" + token + _image_extension(content)
    name = sanitize_name(res.get("name"))
    if not Path(name).suffix.lower():
        if content[:5] == b"%PDF-":
            name += ".pdf"
        else:
            raise AssistantError("文件没有可识别的扩展名，无法安全导入；请发送带文件名的文件。", 422)
    return name


async def _multipart(bot, request, actor, path, files, params=None):
    current, authority = await bot.ready()
    context = authority.context(request, actor)
    headers = {"Authorization": "Bearer " + current.key, "x-clipflow-instance": current.instance,
               "x-clipflow-lease": current.lease, "x-clipflow-context": context}
    try:
        client = await current.http_client()
        response = await client.post(
            "http://127.0.0.1:" + str(current.descriptor["port"]) + "/api/assistant/" + path,
            headers=headers, files=files, params=params or {}, timeout=360)
        result = response.json()
        if not result.get("ok"):
            raise AssistantError(result.get("error") or "读取接口未完成。", response.status_code)
        return result.get("data") or {}
    finally:
        authority.contexts.pop(context, None)


async def _upload(bot, request, actor, name, content, *, purpose=None):
    mime = mimetypes.guess_type(name)[0] or "application/octet-stream"
    params = {"purpose": purpose} if purpose else None
    data = await _multipart(bot, request, actor, "files",
                            [("files", (name, content, mime))], params=params)
    item = next((f for f in (data or {}).get("files", []) if f.get("id")), None)
    if not item:
        raise AssistantError("附件读取成功但未返回有效文件。", 503)
    return item["id"]


async def _assistant_download(bot, request, actor, file_id):
    current, authority = await bot.ready()
    context = authority.context(request, actor)
    headers = {"Authorization": "Bearer " + current.key, "x-clipflow-instance": current.instance,
               "x-clipflow-lease": current.lease, "x-clipflow-context": context}
    try:
        client = await current.http_client()
        url = "http://127.0.0.1:" + str(current.descriptor["port"]) + "/api/assistant/files/" + file_id
        buffer, total = bytearray(), 0
        async with client.stream("GET", url, headers=headers, follow_redirects=False) as response:
            if response.status_code != 200:
                raise AssistantError("会话附件下载失败，请重新确认。", response.status_code)
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > MAX_FILE:
                    raise AssistantError("附件超过20MiB。", 413)
                buffer.extend(chunk)
        return bytes(buffer)
    finally:
        authority.contexts.pop(context, None)


async def _kb_doc(bot, request, actor, name, *, deleted=False):
    """Find one doc whose name exactly matches, paginating until complete."""
    matches = []
    page = 1
    page_total = None
    last_page_len = 0
    while page <= _KB_PAGE_GUARD:
        data = await bot._call(request, actor, "knowledge",
                               params={"q": name, "page": str(page),
                                       "deleted": "1" if deleted else "0"})
        items = data.get("items") or []
        page_total = data.get("total")
        page_len = len(items)
        last_page_len = page_len
        for item in items:
            if item.get("name") == name:
                matches.append(item)
        if page_total is not None:
            reached = page * 20 >= int(page_total)
        else:
            reached = page_len < 20
        if reached:
            break
        page += 1
    if page > _KB_PAGE_GUARD or (last_page_len >= 20 and page_total is not None
                                 and page * 20 < int(page_total)):
        raise AssistantError("知识库检索未取得完整结果，请稍后重试或使用更准确的名字。", 502)
    if len(matches) > 1:
        raise AssistantError("知识库中存在多个同名文档，请使用更完整准确的文档名。", 409)
    return matches[0] if matches else None


async def _owned_files(bot, message, conversation, store, owner, actor_id, channel):
    found, now = {}, time.time()
    doc = await asyncio.to_thread(store.get_document, NAMESPACE, owner)
    if doc and doc.get("actor") == actor_id and doc.get("channel") == channel and doc.get("expires", 0) > now:
        for res in doc.get("resources", []):
            if res.get("file_id"):
                found[res["file_id"]] = res.get("name") or res.get("key")
    for turn in (conversation or {}).get("turns", []) or []:
        for group in ("output_files", "attachments"):
            for item in turn.get(group) or []:
                if isinstance(item, dict) and item.get("id"):
                    found[item["id"]] = item.get("name") or item["id"]
    return [{"id": fid, "name": name} for fid, name in found.items()]


def _photo_sent_count(field):
    """How many photos have already been picked for a notice photo field.

    ``required`` means at least ONE photo; we must never compare against the
    optional ``maxItems`` upper bound (audit #1). A missing/optional photo is
    therefore only a candidate when it is required and currently empty.
    """
    value = field.get("value")
    if value is None:
        value = field.get("selected_files") or []
    if isinstance(value, (list, tuple)):
        return sum(1 for item in value if item not in (None, ""))
    return 1 if value not in (None, "") else 0


def _plain_photo_candidates(conversation):
    """Return (plan, [missing required photo field]) entries of the current
    person that are still waiting for notice photos. Optional photo fields are
    never surfaced as missing (audit #1). Never guesses across plans."""
    if not isinstance(conversation, dict):
        return []
    out = []
    for turn in conversation.get("turns") or []:
        plan = turn.get("plan")
        if not isinstance(plan, dict):
            continue
        if plan.get("status") not in {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}:
            continue
        missing = [field for field in (plan.get("fields") or []) if field.get("native_notice_photos")
                   and field.get("required") and _photo_sent_count(field) < 1]
        if missing:
            out.append((plan, missing))
    return out


def _missing_photo_entries(conversation):
    """Flat, stable list of ``(plan, field)`` waiting for a required photo.

    Used both for the "choose" listing (each plan+photo type gets one stable
    index) and for the deterministic auto-attach decision. Order is stable:
    turns are scanned in order, then fields in order.
    """
    entries = []
    for plan, missing in _plain_photo_candidates(conversation):
        for field in missing:
            entries.append((plan, field))
    return entries


def _field_protected_any(field):
    return bool(field and (field.get("readonly") or field.get("read_only") or field.get("disabled")
                           or field.get("hidden") or field.get("只读")))


def _editable_notice_photo_entries(conversation):
    """``(plan, field)`` for every editable (non-protected) notice photo field of
    an editable plan, whether required or optional.

    The UI invites sending optional site/ali photos too, so an explicit caption
    must be able to attach to an optional field as well as a required-empty one.
    """
    if not isinstance(conversation, dict):
        return []
    out = []
    for turn in conversation.get("turns") or []:
        plan = turn.get("plan")
        if not isinstance(plan, dict):
            continue
        if plan.get("status") not in {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}:
            continue
        for field in (plan.get("fields") or []):
            if not field.get("native_notice_photos"):
                continue
            if _field_protected_any(field):
                continue
            out.append((plan, field))
    return out


_PHOTO_TYPE_SITE_HINTS = ("现场照片", "现场", "补现场", "现场图片")
_PHOTO_TYPE_ALI_HINTS = ("阿里", "截图", "资料", "确认截图")


def _explicit_photo_intent(question):
    """True when the caption explicitly asks to add notice photos.

    Generic OCR / recognition keywords (图片 / 识图 / 识别 / OCR) are excluded so an
    ordinary image-forwarding or OCR query never auto-attaches to an optional
    photo field.
    """
    q = str(question or "")
    if re.search(r"识图|识别|OCR|ocr|图片", q):
        return False
    return bool(re.search(r"现场照片|补照片|照片|资料截图|截图|阿里|补现场|现场", q))


def _caption_photo_type(question):
    """Return 'site' | 'ali' | None based on an explicit caption keyword."""
    q = str(question or "")
    if any(k in q for k in _PHOTO_TYPE_SITE_HINTS):
        return "site"
    if any(k in q for k in _PHOTO_TYPE_ALI_HINTS):
        return "ali"
    return None


def _photo_attach_target(conversation, *, required_only=True):
    """Return ('unique', plan, field) | ('choose', [(plan, field), ...]) | None."""
    if required_only:
        entries = _missing_photo_entries(conversation)
    else:
        entries = _editable_notice_photo_entries(conversation)
    if not entries:
        return None
    if len(entries) > 1:
        return ("choose", entries)
    plan, field = entries[0]
    return ("unique", plan, field)


def _plan_from_conversation(conversation, plan_id):
    for turn in (conversation or {}).get("turns", []) or []:
        p = turn.get("plan")
        if p and str(p.get("id") or "") == str(plan_id or ""):
            return p
    return None


def _current_photo_target(conversation, snapshot):
    """Look up the *exact* current ``(plan, field)`` for a snapshot entry.

    Returns ``(None, None)`` when the plan or photo field no longer exists so the
    caller can reject instead of silently reindexing to a different candidate.
    """
    plan_id = str((snapshot or {}).get("plan_id") or "")
    field_name = str((snapshot or {}).get("field_name") or "")
    if not plan_id or not field_name:
        return None, None
    plan = _plan_from_conversation(conversation, plan_id)
    if plan is None:
        return None, None
    field = next((f for f in (plan.get("fields") or [])
                  if str(f.get("name") or "") == field_name), None)
    if field is None:
        return None, None
    return plan, field


def _photo_receipt_key(actor_id, channel, conversation_id, message_id, resource_key):
    raw = "|".join(str(x) for x in (actor_id, channel, conversation_id, message_id, resource_key))
    return "photo_receipt:" + hashlib.sha256(raw.encode("utf-8")).hexdigest()[:48]


async def _import_photo_file_ids(bot, message, request, actor, resources, conversation=None):
    """Download Feishu images and upload them as assistant files only (no model
    recognition), tagged ``purpose=notice_photo`` so the API can avoid OCR.

    Each successful import is persisted as a durable receipt bound to
    actor/channel/conversation/source message/resource key, then reused on
    redelivery so the same image is never imported twice (even after a partial
    failure). The ``file_id`` is also written back onto the resource dict.
    """
    store = getattr(getattr(bot, "runtime", None), "state_store", None)
    actor_id = str(actor.get("id") or message.get("sender_id") or "")
    channel = str(actor.get("channel") or "feishu:" + str(message.get("chat_id") or ""))
    conversation_id = str((conversation or {}).get("conversation_id") or "")
    mid = str(message.get("message_id") or "")
    now = time.time()
    file_ids = []
    for res in resources:
        if res.get("file_id"):
            file_ids.append(str(res["file_id"]))
            continue
        receipt_key = _photo_receipt_key(actor_id, channel, conversation_id, mid,
                                         str(res.get("key") or res.get("original_mid") or ""))
        if store is not None:
            receipt = await asyncio.to_thread(store.get_document, NAMESPACE, receipt_key)
            if receipt and receipt.get("file_id") and receipt.get("expires", 0) > now:
                res["file_id"] = receipt["file_id"]
                file_ids.append(str(receipt["file_id"]))
                continue
        async with _heavy():
            content = await _download(bot, message, res)
            if len(content) > 8 * 1024 * 1024:
                raise AssistantError("单张通告照片超过8MiB，请压缩后重新发送。", 413)
            name = _upload_name(res, content)
            fid = await _upload(bot, request, actor, name, content, purpose="notice_photo")
        res["file_id"] = fid
        if store is not None:
            await asyncio.to_thread(store.put_document, NAMESPACE, receipt_key, {
                "actor": actor_id, "channel": channel, "conversation_id": conversation_id,
                "message_id": mid, "resource_key": str(res.get("key") or ""),
                "file_id": fid, "name": name, "created_at": now,
                "expires": now + USE_HOURS,
            })
        file_ids.append(fid)
    return file_ids


def _photo_display_label(field):
    kind = str(field.get("native_notice_photos"))
    return safe_text(str(field.get("label") or ""), limit=60) or ("现场照片" if kind == "site" else "资料截图")


def _choice_item(entry):
    """Normalize a candidate entry into ``(title, label)`` for listing.

    Accepts both the live ``(plan, field)`` tuples and the persisted snapshot
    dicts (used for redelivery so the numbering stays identical)."""
    if isinstance(entry, dict):
        title = safe_text(str(entry.get("title") or "业务操作"), limit=80)
        label = safe_text(str(entry.get("label") or ""), limit=60) or (
            "现场照片" if entry.get("kind") == "site" else "资料截图")
        return title, label
    plan, field = entry
    return (safe_text(str(plan.get("title") or "业务操作"), limit=80),
            _photo_display_label(field))


def _photo_choice_lines(entries):
    """Stable, numbered list of (plan title + photo type) for user selection.

    Each plan+photo-type gets its own number so site and ali within one plan are
    both selectable in Feishu (the numbering always matches ``补照片计划 N``).
    """
    entries = list(entries or [])
    lines = ["检测到 %d 个待补充现场照片的通告/照片类别，为避免贴错请选择：" % len(entries)]
    for index, entry in enumerate(entries, 1):
        title, label = _choice_item(entry)
        lines.append("%d. 通告「%s」：%s" % (index, title, label))
    lines.append("回复“补照片计划 N”选择要补充的照片类别（N 为上面对应的数字）。照片已暂存，选好后会自动补充。")
    return "\n".join(lines)


async def _stash_photos_for_selection(bot, message, request, actor, store, owner, actor_id, channel,
                                      resources, conversation, entries, question):
    """Stage image receipts for a multi-candidate notice and list stable choices.

    The whole bundle is imported once and its receipts are persisted; a
    redelivery of the same ``message_id`` reuses the imported ``file_id`` values
    instead of re-importing.  The selected plan/photo-type is validated against
    ``conversation_id``/``actor``/plan version later at attach time.
    """
    now = time.time()
    key = "photo:" + owner
    existing = await asyncio.to_thread(store.get_document, NAMESPACE, key)
    if existing and existing.get("message_id") == message.get("message_id") and existing.get("imported_file_ids"):
        for res, fid in zip(resources, existing["imported_file_ids"]):
            if fid:
                res["file_id"] = fid
        existing["expires"] = now + USE_HOURS
        await asyncio.to_thread(store.put_document, NAMESPACE, key, existing)
        return {"text": _photo_choice_lines(existing.get("entries") or entries), "photo_choices": True}

    receipt_resources = resources
    new_ids = await _import_photo_file_ids(bot, message, request, actor, receipt_resources, conversation)
    candidate_snapshot = [{
        "plan_id": str(plan.get("id") or ""),
        "plan_version": plan.get("version"),
        "field_name": str(field.get("name") or ""),
        "kind": str(field.get("native_notice_photos")),
        "label": _photo_display_label(field),
        "title": safe_text(str(plan.get("title") or "业务操作"), limit=80),
    } for plan, field in entries]
    await asyncio.to_thread(store.put_document, NAMESPACE, key, {
        "owner": owner, "actor": actor_id, "channel": channel,
        "message_id": message["message_id"], "question": question,
        "resources": receipt_resources, "imported_file_ids": new_ids,
        "entries": candidate_snapshot, "conversation_id": (conversation or {}).get("conversation_id"),
        "expires": now + USE_HOURS,
    })
    return {"text": _photo_choice_lines(entries), "photo_choices": True}


async def _attach_photos_to_plan(bot, message, request, actor, store, owner, actor_id, channel,
                                 resources, question, conversation, plan, field, *, imported_ids=None):
    """Stage the given image file ids onto one notice photo field.

    Uses the kernel's ``atomic attach`` contract
    ``PATCH plans/{id} {action: attach_files, version, values: {fieldname:[fileids]}}``
    which only stages the photos and keeps every other incomplete field intact.

    The matching plan is re-read before attaching so a redelivery whose attach
    response was lost never re-stages ids that are already on the field.
    """
    if imported_ids is not None:
        new_ids = list(dict.fromkeys(str(fid) for fid in imported_ids if fid))
    else:
        new_ids = list(dict.fromkeys(str(fid) for fid in await _import_photo_file_ids(
            bot, message, request, actor, resources, conversation) if fid))
    current_plan = _plan_from_conversation(conversation, plan.get("id")) or plan
    current_field = next((f for f in (current_plan.get("fields") or [])
                          if str(f.get("name") or "") == str(field.get("name") or "")), None)
    if current_field is None:
        raise AssistantError("通告照片字段已不存在，请重新发送照片。", 409)
    existing = [str(item) for item in (current_field.get("value") or current_field.get("selected_files") or [])
                if item not in (None, "")]
    pending = [fid for fid in new_ids if fid not in existing]
    if not pending:
        label = _photo_display_label(current_field)
        return {"text": (f"照片已补充到通告“{safe_text(str(current_plan.get('title') or '业务操作'), limit=60)}”"
                         f"（{label}），无需重复上传。"), "already_attached": True,
                "plan": current_plan}
    max_items = int(current_field.get("maxItems") or 0)
    if max_items and len(existing) + len(pending) > max_items:
        raise AssistantError(
            "该照片字段最多 " + str(max_items) + " 张，当前已有 " + str(len(existing))
            + " 张，本次新增 " + str(len(pending)) + " 张，已超上限，未提交。请分配合适数量。", 422)
    field_name = str(current_field.get("name") or "")
    result = await bot._call(request, actor, "plans/" + str(current_plan["id"]),
                             {"action": "attach_files", "version": current_plan["version"],
                              "values": {field_name: [*existing, *pending]}},
                             method="PATCH")
    label = _photo_display_label(current_field)
    result_plan = result if (isinstance(result, dict) and result.get("id")) else current_plan
    reply = (f"已将 {len(pending)} 张照片补充到通告“{safe_text(str(current_plan.get('title') or '业务操作'), limit=60)}”"
             f"（{label}）。照片仅暂存为附件，未送模型识图；其它填写保持不变，"
             f"请继续核对并确认，不会重复提交。")
    # Fresh editable card so the operator can finish/confirm natively in Feishu,
    # even when the plan only has photo fields (defect #4).
    try:
        from .feishu_notice_cards import prepare_card
        card_result = await prepare_card(bot, message, actor, conversation, result_plan)
        return {"text": reply, "card": card_result.get("card"), "ticket": card_result.get("ticket"),
                "plan": result_plan}
    except Exception:
        return {"text": reply, "plan": result_plan}


async def _attach_stashed_photos(bot, message, request, actor, store, owner, actor_id, channel,
                                 conversation, index):
    now = time.time()
    doc = await asyncio.to_thread(store.get_document, NAMESPACE, "photo:" + owner)
    if not doc or doc.get("actor") != actor_id or doc.get("channel") != channel or doc.get("expires", 0) <= now:
        return {"text": "当前没有可补充的暂存照片（可能已过期或不属于当前会话），请重新发送照片。"}
    if doc.get('conversation_id') and doc['conversation_id'] != (conversation or {}).get('conversation_id'):
        return {"text": "原会话已清空，请重新发送照片。补照片未执行。"}
    # The choice index refers to the SNAPSHOT captured when the photos were
    # staged, never the current entries, so a reordered/disappeared candidate
    # can never shift the number to a different plan (defect #1).
    snapshot_entries = doc.get("entries") or []
    if not snapshot_entries:
        return {"text": "当前没有可补充的暂存照片，请重新发送照片。"}
    if not 1 <= index <= len(snapshot_entries):
        return {"text": f"请输入正确的照片类别编号（1-{len(snapshot_entries)}）。\n" + _photo_choice_lines(snapshot_entries)}
    chosen = snapshot_entries[index - 1]
    plan, field = _current_photo_target(conversation, chosen)
    if plan is None or field is None:
        return {"text": "所选照片类别已不存在或内容已更新，请重新发送照片后选择。暂存照片已保留，未自动补到旧内容。"}
    if int(str(chosen.get("plan_version") or -1)) != int(plan.get("version") or -2):
        # The plan moved (version bumped) since the photos were staged: keep the
        # staged receipts and ask for a fresh choice matched to the current plan.
        return {"text": ("对应通告内容已更新（版本变化），请重新发送照片后选择。暂存照片已保留，不会自动补到旧内容。")}
    new_ids = list(dict.fromkeys(str(fid) for fid in (doc.get("imported_file_ids") or []) if fid))
    try:
        return await _attach_photos_to_plan(bot, message, request, actor, store, owner, actor_id, channel,
                                            doc.get("resources") or [], message.get("question") or "",
                                            conversation, plan, field, imported_ids=new_ids)
    except AssistantError:
        # On failure keep the staged images so the user can retry without having
        # to re-send them; do NOT delete the stash in a finally block.
        raise


def _kb_summary(op):
    if op["action"] == "add":
        return f"将把附件“{op['file_name']}”上传到公司知识库并排队入库（有权限用户稍后可检索）。"
    if op["action"] == "replace":
        return f"将用附件“{op['file_name']}”更新知识库文档“{op['document_name']}”（当前版本 {op['revision']}）。"
    if op["action"] == "delete":
        return f"将把知识库文档“{op['document_name']}”移入回收站（当前版本 {op['revision']}）。"
    return f"将恢复知识库文档“{op['document_name']}”（当前版本 {op['revision']}）。"


async def _prepare_kb(bot, message, request, actor, conversation, store, owner, actor_id, channel,
                      action, preamble, target):
    files = await _owned_files(bot, message, conversation, store, owner, actor_id, channel)
    now = time.time()

    if preamble == "将附件加入知识库：":
        match = [f for f in files if f["name"] == target]
        if not match:
            return {"text": "未找到你的会话附件“" + safe_text(target, limit=120) + "”。"}
        if len(match) > 1:
            return {"text": "存在多个同名附件，无法唯一确定，请使用完整准确的文件名。"}
        op = {"action": "add", "file_id": match[0]["id"], "file_name": target, "deleted": False}
    elif preamble == "更新知识库：":
        if " <- " not in target or len(target.split(" <- ")) != 2:
            return {"text": "请按“更新知识库：文档名 <- 附件名”填写。"}
        dname, fname = [part.strip() for part in target.split(" <- ")]
        doc = await _kb_doc(bot, request, actor, dname)
        if not doc:
            return {"text": "知识库中未找到文档“" + safe_text(dname, limit=120) + "”。"}
        if not doc.get("can_edit"):
            return {"text": "只有上传者可更新此共享文件。"}
        match = [f for f in files if f["name"] == fname]
        if not match:
            return {"text": "未找到你的会话附件“" + safe_text(fname, limit=120) + "”。"}
        if len(match) > 1:
            return {"text": "存在多个同名附件，无法唯一确定。"}
        op = {"action": "replace", "document_id": doc["id"], "revision": int(doc["version"]),
              "file_id": match[0]["id"], "file_name": fname, "document_name": dname, "deleted": False}
    else:
        deleted = action == "restore"
        doc = await _kb_doc(bot, request, actor, target, deleted=deleted)
        if not doc:
            return {"text": "知识库中未找到文档“" + safe_text(target, limit=120) + "”。"}
        if not doc.get("can_edit"):
            return {"text": "只有上传者可" + ("恢复" if action == "restore" else "删除") + "此共享文件。"}
        if action == "restore" and doc.get("status") != "deleted":
            return {"text": "该文档不在回收站中。"}
        if action == "delete" and doc.get("status") == "deleted":
            return {"text": "该文档已在回收站。"}
        op = {"action": action, "document_id": doc["id"], "revision": int(doc["version"]),
              "document_name": target, "deleted": deleted}
    key = _kb_key(owner, op)
    await asyncio.to_thread(store.put_document, NAMESPACE, key,
                            {**op, "owner": owner, "actor": actor_id, "channel": channel,
                             "expires": now + USE_HOURS, "receipt": None, 'conversation_id': conversation.get('conversation_id')})
    return {"text": _kb_summary(op) + f"\n回复“{CONFIRM_KB}”以继续（1小时内有效），或回复“取消知识库操作”。"}


def _kb_confirm_text(op):
    if op["action"] == "add":
        return f"已将附件“{op['file_name']}”上传到公司知识库，正在排队入库（尚未检索可用）。"
    if op["action"] == "replace":
        return f"已用附件“{op['file_name']}”更新知识库文档“{op['document_name']}”，正在排队入库（尚未检索可用）。"
    if op["action"] == "delete":
        return f"已将知识库文档“{op['document_name']}”移入回收站。"
    return f"已恢复知识库文档“{op['document_name']}”。"


async def _kb_upload_file(bot, request, actor, op, content):
    mime = mimetypes.guess_type(op["file_name"])[0] or "application/octet-stream"
    params = None
    if op["action"] == "replace":
        params = {"document_id": op["document_id"], "version": str(op["revision"])}
    data = await _multipart(bot, request, actor, "knowledge/files",
                                [("files", (op["file_name"], content, mime))], params=params)
    items = data.get("items") or []
    errors = data.get("errors") or []
    if errors:
        raise AssistantError("、".join(str(e.get("error") or "上传失败") for e in errors), 502)
    if not items:
        raise AssistantError("知识库未返回已接收文件，请重试。", 502)
    if op["action"] == "replace" and len(items) != 1:
        raise AssistantError("知识库替换应恰好返回一个文档，返回结果不完整。", 502)
    return items[0] if items else None


async def _confirm_kb(bot, message, request, actor, store, owner, actor_id, channel):
    now = time.time()
    key = _kb_key(owner, {})
    op = await asyncio.to_thread(store.get_document, NAMESPACE, key)
    if not op:
        return {"text": "当前没有待确认的知识库操作（可能已过期），请重新发起命令。"}
    if op.get("actor") != actor_id or op.get("channel") != channel or op.get("expires", 0) <= now:
        if op.get("expires", 0) <= now:
            await asyncio.to_thread(store.delete_document, NAMESPACE, key)
        return {"text": "当前没有待确认的知识库操作（可能已过期），请重新发起命令。"}
    if op.get("receipt"):
        return {"text": "该知识库操作已完成，未重复写入。"}
    try:
        if op["action"] in ("add", "replace"):
            content = await _assistant_download(bot, request, actor, op["file_id"])
            await _kb_upload_file(bot, request, actor, op, content)
        else:
            doc = await _kb_doc(bot, request, actor, op["document_name"], deleted=bool(op.get("deleted")))
            if not doc or int(doc.get("version", -1)) != op["revision"] or doc["id"] != op["document_id"]:
                return {"text": "文档状态已变化，未执行" + ("恢复" if op["action"] == "restore" else "删除")
                        + "；请重新发起命令。"}
            path = "knowledge/documents/" + doc["id"] + ("/restore" if op["action"] == "restore" else "")
            method = "POST" if op["action"] == "restore" else "DELETE"
            await bot._call(request, actor, path, {"version": int(doc["version"])}, method=method)
    except AssistantError as exc:
        return {"text": safe_text(str(exc))}
    op["receipt"] = {"at": time.time(), "action": op["action"], "document_id": op.get("document_id"),
                     "revision": op.get("revision")}
    await asyncio.to_thread(store.put_document, NAMESPACE, key, {**op, "expires": now + USE_HOURS})
    return {"text": _kb_confirm_text(op)}
