"""Durable assistant messages and cancellable runs, independent of browser sockets."""
import asyncio
import copy
import json
import logging
import re
import time
import traceback
import uuid

from .lighthouse_ai import AssistantError, NAMESPACE, private_identifier, safe_text
from .lighthouse_model import LighthouseModel
from .lighthouse_scope import resolve_scopes

RUNS = "lighthouse_runs"
MESSAGES = "lighthouse_messages"
ACTIVE = {"queued", "running"}
PROTECTED_PLANS = {"running", "submitted", "completed"}


def failure_detail(exc):
    """Classify without returning provider bodies, credentials or tool arguments."""
    name, status = type(exc).__name__, getattr(exc, "status_code", None)
    if not isinstance(status, int):
        status = None
    if isinstance(exc, AssistantError):
        return "business_query", safe_text(str(exc))
    if status in {401, 403}:
        return "model_auth", "模型认证未通过，请管理员核对模型设置。"
    if status == 429:
        return "model_busy", "模型服务繁忙或额度不足，请稍后重试原消息。"
    if "Timeout" in name:
        return "timeout", "模型或业务服务响应超时，原消息已保留，可重试。"
    if "Connection" in name or "ConnectError" in name:
        return "model_connection", "未能连接模型服务，请稍后重试；这不代表网页与灯塔的连接中断。"
    if status and status >= 500:
        return "model_service", "模型服务暂时异常，请稍后重试原消息。"
    if status in {400, 404, 422}:
        return "model_request", "模型接口不接受当前请求，请核对模型地址及工具调用支持。"
    if name in {"UsageLimitExceeded", "UnexpectedModelBehavior", "ToolRetryError", "ToolFailedError", "ValidationError", "APIResponseValidationError"}:
        return "model_protocol", "模型未生成有效的查询或回答，本轮已停止；可重新提问或切换模型。"
    return "query_adapter", "查询处理遇到程序异常，已记录诊断编号；请重试原消息或联系管理员。"


def public_turn(turn):
    return {key: copy.deepcopy(value) for key, value in turn.items()
            if not key.startswith("_") and key not in {"prompt", "file_ids", "submitted_file_ids"}}


class LighthouseStream:
    def __init__(self, portal_agent, *, cached_reader=None, engine=None):
        self.portal = portal_agent
        self.assistant = portal_agent.assistant
        self.store = self.assistant.store
        self.engine = engine or LighthouseModel(portal_agent, cached_reader=cached_reader)
        # ponytail: one Portal process owns runs; shared coordination is needed before enabling multiple workers.
        self.workers, self.live, self.locks = {}, {}, {}
        self.slots = asyncio.Semaphore(3)
        self.closing = False

    def _lock(self, actor):
        return self.locks.setdefault(actor["id"], asyncio.Lock())

    def _read(self, actor):
        with self.assistant._lock:
            return copy.deepcopy(self.assistant._state(actor))

    def _save_run(self, run):
        snapshot = {key: copy.deepcopy(value) for key, value in tuple(run.items()) if not key.startswith("_")}
        with self.assistant._lock:
            saved = self.store.get_document(RUNS, run["id"])
            if not saved or saved.get("revision", 0) <= snapshot["revision"]:
                self.store.put_document(RUNS, run["id"], snapshot)

    def _patch_turn(self, actor, run, changes, *, finish=False):
        with self.assistant._lock:
            state = self.assistant._state(actor)
            if state["id"] != run["conversation_id"]:
                return False
            turn = next((item for item in state["turns"] if item["operation_id"] == run["operation_id"]), None)
            if not turn or turn.get("run_id") != run["id"] or turn.get("status") != "pending":
                return False
            turn.update(copy.deepcopy(changes))
            turn["process"] = copy.deepcopy(run.get("process", []))
            self.store.put_document(MESSAGES, state["id"] + ":" + turn["operation_id"], turn)
            if state.get("active_run_id") == run["id"]:
                state["phase"] = "" if finish else run.get("label", "正在处理")
                if finish:
                    state["active_run_id"] = ""
                    self.assistant._active.discard(actor["id"])
            state["revision"] = state.get("revision", 0) + 1
            self.store.put_document(NAMESPACE, self.assistant._key(actor), state)
            return True

    def _accept(self, actor, payload):
        operation = payload.get("operation_id")
        attempt = payload.get("attempt_id") or operation
        if any(not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{16,80}", value) for value in (operation, attempt)):
            raise AssistantError("消息编号无效。")
        question = payload.get("question", "")
        files = payload.get("file_ids") or []
        if not isinstance(question, str) or len(question) > 12000 or not question.strip() and not files:
            raise AssistantError("请填写问题或附带文件，文字最多12000字。")
        private_request = private_identifier(question)
        clean_question = safe_text(question.strip(), limit=12000) or ("隐私相关问题" if private_request else "请处理附件")
        if not isinstance(files, list) or len(files) > 10 or any(not isinstance(file, str) for file in files):
            raise AssistantError("附件列表格式无效。")
        attachments = [self.portal.files.public(self.portal.files.get(actor, file)) for file in files]
        with self.assistant._lock:
            state = self.assistant._state(actor)
            if state["id"] != payload.get("conversation_id"):
                raise AssistantError("会话已变化，请重新读取，原输入未提交。", 409)
            old = next((item for item in state["turns"] if item["operation_id"] == operation), None)
            if old and old.get("attempt_id", operation) == attempt:
                if old["question"] != clean_question or old.get("submitted_file_ids", old.get("file_ids", [])) != files:
                    raise AssistantError("消息编号不能用于不同内容。", 409)
                return None, {"conversation_id": state["id"], "run_id": old.get("run_id", ""), "turn": public_turn(old)}
            if old and (old.get("plan") or {}).get("status") in PROTECTED_PLANS:
                raise AssistantError("此消息的业务操作已执行或已提交，请查询原任务，不重新执行。", 409)
            scopes = resolve_scopes(actor, question, state.get("query_scopes"))
            if not scopes:
                raise AssistantError("所选范围内没有可访问楼栋，请重新选择。", 403)
            selected = self.assistant._selected(state, self.assistant.model.settings())
            profile = self.assistant.model.profile(selected["id"] if selected else "")
            prior = next((item for item in state["turns"] if item.get("run_id") == state.get("active_run_id") and item.get("status") == "pending"), None)
            prompt = (old.get("prompt") if old and old["question"] == clean_question else "") or clean_question
            if prior and prior is not old:
                active_run = self.live.get(prior.get("run_id")) or {}
                prior["answer"] = active_run.get("answer", prior.get("answer", ""))
                prompt = (prior.get("prompt") or prior["question"]) + "\n用户补充（采用最新条件）：" + prompt
                if len(prompt) > 18000:
                    raise AssistantError("补充内容较多，请先停止当前回答后分步提问。")
                files = list(dict.fromkeys([*prior.get("file_ids", []), *files]))
                if len(files) > 10:
                    raise AssistantError("本轮补充后附件超过10个，请分步处理。")
                prior.update(status="superseded", error="已结合后续补充继续处理。", superseded_by=operation)
                self.store.put_document(MESSAGES, state["id"] + ":" + prior["operation_id"], prior)
            elif len(question.strip()) < 50 and re.search(r"只看|仅看|还有|也包括|继续|刚才|第二|第[一二三四五六七八九]|全部楼|所有楼", question):
                previous = next((item for item in reversed(state["turns"]) if item is not old and item.get("question")), None)
                if previous:
                    prompt = "之前的问题：" + previous["question"] + "\n本次补充（采用最新条件）：" + prompt
            # New input invalidates only unsubmitted proposals, never submitted jobs.
            for item in state["turns"]:
                plan = item.get("plan") or {}
                if plan.get("status") in {"needs_input", "awaiting_confirmation", "awaiting_second_confirmation"}:
                    raw = self.store.get_document("lighthouse_agent_plans", plan["id"])
                    if raw:
                        raw.update(status="cancelled", version=raw.get("version", 1) + 1, error="用户补充信息，原确认已失效。")
                        self.store.put_document("lighthouse_agent_plans", raw["id"], raw)
                    plan.update(status="cancelled", error="用户补充信息，原确认已失效。")
            run_id = uuid.uuid4().hex
            turn = old or {"operation_id": operation, "at": time.time()}
            if old:
                turn.setdefault("attempts", []).append({key: copy.deepcopy(old.get(key)) for key in ("run_id", "answer", "status", "error", "at")})
            turn.update(question=clean_question, prompt=prompt, file_ids=files, submitted_file_ids=payload.get("file_ids") or [], _private_request=private_request or bool(old and old.get("_private_request")),
                        attachments=attachments, output_files=[], status="pending", answer="", error="", error_code="", process=[], sources=[], scopes=scopes,
                        access_scopes=actor["scopes"],
                        run_id=run_id, attempt_id=attempt, model_name=profile["name"], plan=None)
            if not old:
                state["turns"].append(turn)
            # Migrate the previous generation once, then only save changed turns.
            if not state.get("message_archive_ready"):
                for item in state["turns"]:
                    self.store.put_document(MESSAGES, state["id"] + ":" + item["operation_id"], item)
                state["message_archive_ready"] = True
            else:
                self.store.put_document(MESSAGES, state["id"] + ":" + turn["operation_id"], turn)
            self.assistant._trim_turns(state)
            state.update(active_run_id=run_id, query_scopes=scopes, streaming=True, phase="正在接收消息", revision=state.get("revision", 0) + 1)
            self.store.put_document(NAMESPACE, self.assistant._key(actor), state)
            self.assistant._active.add(actor["id"])
            run = {"id": run_id, "owner": actor["id"], "conversation_id": state["id"], "operation_id": operation,
                   "scopes": scopes, "access_scopes": actor["scopes"], "status": "queued", "label": "等待处理", "events": [], "revision": 1,
                   "answer": "", "process": [], "created_at": time.time(), "_turn": {**copy.deepcopy(turn), "_profile": profile},
                   "_history": copy.deepcopy([t for t in state["turns"] if t is not turn and t.get("answer") and t.get("status") == "completed" and self.assistant._allowed(t, actor)]),
                   "_context": copy.deepcopy(state.get("context") or {})}
            self._save_run(run)
            return run, {"conversation_id": state["id"], "run_id": run_id, "turn": public_turn(turn)}

    async def submit(self, actor, payload, request, authorize):
        if self.closing:
            raise AssistantError("服务正在关闭，请稍后继续。", 503)
        async with self._lock(actor):
            previous_state = await asyncio.to_thread(self._read, actor)
            run, result = await asyncio.to_thread(self._accept, actor, payload)
            if run is None:
                return result
            previous = self.workers.get(actor["id"])
            previous_run = self.live.get(previous_state.get("active_run_id"))
            if previous_run and previous_run["status"] in ACTIVE:
                previous_run.update(status="stopped", label="已采用补充信息", revision=previous_run["revision"] + 1)
                await asyncio.to_thread(self._save_run, previous_run)
            if previous and not previous.done():
                previous.cancel()
            self.live[run["id"]] = run
            task = asyncio.create_task(self._execute(actor, run, request, authorize, previous))
            self.workers[actor["id"]] = task
            # Completed streams are persisted; only a bounded replay cache stays in RAM.
            for key in list(self.live):
                if len(self.live) <= 32:
                    break
                if self.live[key]["status"] not in ACTIVE:
                    self.live.pop(key, None)
            return result

    async def _execute(self, actor, run, request, authorize, previous):
        from pydantic_ai.ui.vercel_ai.response_types import AbortChunk, DataChunk, FinishChunk, StartChunk, TextDeltaChunk, TextEndChunk, TextStartChunk
        public_actor = {**actor, "scopes": run["scopes"], "allowed_scopes": actor["scopes"]}
        text_id = "text_" + run["id"]
        owner_task = asyncio.current_task()
        last_flush = 0.0
        def chunk(value):
            run["events"].append(value.model_dump(mode="json", by_alias=True, exclude_none=True))
            run["revision"] += 1
        async def emit(kind, data):
            nonlocal last_flush
            if self.workers.get(actor["id"]) is not owner_task:
                raise asyncio.CancelledError()
            if kind == "text":
                delta = data["delta"][:max(0, 16000 - len(run["answer"]))]
                run["answer"] += delta
                if delta:
                    chunk(TextDeltaChunk(id=text_id, delta=delta))
            else:
                run["label"] = safe_text(data.get("label", "正在处理"), limit=120) or "正在处理"
                process = run.setdefault("process", [])
                if not process or process[-1]["label"] != run["label"]:
                    process.append({"label": run["label"], "at": time.time()})
                    del process[:-24]
                chunk(DataChunk(type="data-progress", data={"run_id": run["id"], "operation_id": run["operation_id"],
                    "label": run["label"], "process": copy.deepcopy(process)}, transient=True))
            if time.monotonic() - last_flush > .4:
                last_flush = time.monotonic()
                await asyncio.to_thread(self._save_run, run)
        try:
            if previous:
                await asyncio.gather(previous, return_exceptions=True)
            chunk(StartChunk(message_id="reply_" + run["id"], message_metadata={"run_id": run["id"], "operation_id": run["operation_id"]}))
            chunk(TextStartChunk(id=text_id))
            async with self.slots:
                run["status"] = "running"
                context = run["_context"]
                if set(context.get("scopes", [])) - set(run["scopes"]) or not self.assistant._allowed(context, actor):
                    context = {}
                eligible = [t for t in run["_history"] if set(t.get("scopes", [])) <= set(run["scopes"])]
                # Keep the archive; only compress what is sent to a model.
                cutoff = next((n + 1 for n, t in enumerate(eligible) if t["operation_id"] == context.get("through")), 0)
                older = eligible[cutoff:-6]
                if older and sum(len(t.get("question", "")) + len(t.get("answer", "")) for t in eligible[cutoff:]) > 12000:
                    await emit("status", {"label": "正在整理上下文"})
                    try:
                        summary = await asyncio.wait_for(self.engine.summarize(public_actor, older, context.get("summary", ""), run["_turn"]["_profile"]), 35)
                        context = {"summary": summary, "through": older[-1]["operation_id"], "scopes": run["scopes"], "access_scopes": actor["scopes"],
                                   "compressed_turns": context.get("compressed_turns", 0) + len(older), "last_compressed_at": time.time()}
                        await asyncio.to_thread(self._save_context, actor, run, context)
                    except Exception:
                        await emit("status", {"label": "上下文整理暂未完成，保留原记录继续"})
                result = await self.engine.answer(public_actor, run["_turn"], eligible, request, emit, authorize, context)
                current = await authorize()
                if current["id"] != actor["id"] or set(actor["scopes"]) - set(current["scopes"]):
                    raise AssistantError("登录权限已变化，请重新提问。", 403)
                run.update(status="completed", answer=result.get("answer") or run["answer"], label="回答完成")
                changes = {**result, "status": "completed", "error": "", "error_code": "", "answer": run["answer"], "process": run["process"]}
                await asyncio.to_thread(self._patch_turn, actor, run, changes, finish=True)
                chunk(DataChunk(type="data-turn", data={"operation_id": run["operation_id"], "run_id": run["id"], **public_turn(changes)}, transient=True))
                chunk(TextEndChunk(id=text_id))
                chunk(FinishChunk(finish_reason="stop"))
        except asyncio.CancelledError:
            run.update(status="stopped", label="已停止")
            await asyncio.to_thread(self._patch_turn, actor, run, {"status": "stopped", "answer": run["answer"], "error": "回答已停止，可继续或重新提问。"}, finish=True)
            chunk(AbortChunk(reason="已停止当前回答"))
        except Exception as exc:
            error_code, error = failure_detail(exc)
            error += "（诊断编号：" + run["id"][:8] + "）"
            frames = [f"{frame.name}:{frame.lineno}" for frame in traceback.extract_tb(exc.__traceback__)[-6:]]
            logging.getLogger(__name__).warning("Assistant run failed: run=%s category=%s type=%s http_status=%s frames=%s",
                run["id"], error_code, type(exc).__name__, getattr(exc, "status_code", None), ",".join(frames))
            run.update(status="failed", label="处理未完成", error=error, error_code=error_code)
            changes = {"status": "failed", "error": error, "error_code": error_code, "answer": run["answer"], "process": run["process"]}
            await asyncio.to_thread(self._patch_turn, actor, run, changes, finish=True)
            chunk(DataChunk(type="data-turn", data={"operation_id": run["operation_id"], "run_id": run["id"], **changes}, transient=True))
            chunk(TextEndChunk(id=text_id))
            chunk(FinishChunk(finish_reason="error"))
        finally:
            run["revision"] += 1
            run["finished_at"] = time.time()
            await asyncio.to_thread(self._save_run, run)

    def _save_context(self, actor, run, context):
        with self.assistant._lock:
            state = self.assistant._state(actor)
            if state.get("active_run_id") == run["id"]:
                state["context"] = context
                self.store.put_document(NAMESPACE, self.assistant._key(actor), state)

    def get_run(self, actor, identity, *, cancelling=False):
        if not isinstance(identity, str) or not re.fullmatch(r"[a-f0-9]{32}", identity):
            raise AssistantError("回答任务不存在。", 404)
        run = self.live.get(identity) or self.store.get_document(RUNS, identity)
        if not run or run["owner"] != actor["id"]:
            raise AssistantError("回答任务不存在或无权访问。", 404)
        if not cancelling and set(run.get("access_scopes", run["scopes"])) - set(actor["scopes"]):
            raise AssistantError("当前账号无权查看此回答。", 403)
        if run["conversation_id"] != self._read(actor)["id"]:
            raise AssistantError("原会话已清空。", 404)
        return run

    async def stop(self, actor, identity):
        async with self._lock(actor):
            run = await asyncio.to_thread(self.get_run, actor, identity, cancelling=True)
            state = await asyncio.to_thread(self._read, actor)
            if state.get("active_run_id") == identity:
                task = self.workers.get(actor["id"])
                if task and not task.done():
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
                if run["status"] in ACTIVE:
                    run.update(status="stopped", label="已停止", revision=run["revision"] + 1)
                    await asyncio.to_thread(self._patch_turn, actor, run, {"status": "stopped", "error": "上次回答已中断，可继续。"}, finish=True)
                    await asyncio.to_thread(self._save_run, run)
            return {"run_id": identity, "status": "stopped" if run["status"] in ACTIVE else run["status"]}

    async def conversation(self, actor):
        state = await asyncio.to_thread(self._read, actor)
        identity = state.get("active_run_id")
        task = self.workers.get(actor["id"])
        active = self.live.get(identity)
        if active and set(active.get("access_scopes", active["scopes"])) - set(actor["scopes"]) and task and not task.done():
            await self.stop(actor, identity)
        if identity and (not task or task.done()):
            try:
                run = await asyncio.to_thread(self.get_run, actor, identity)
                if run["status"] in ACTIVE:
                    await asyncio.to_thread(self._patch_turn, actor, run,
                        {"status": "stopped", "answer": run.get("answer", ""), "error": "服务重启或上次回答中断，原消息已保留，可继续。"}, finish=True)
                    run.update(status="stopped", revision=run["revision"] + 1)
                    await asyncio.to_thread(self._save_run, run)
                else:
                    terminal = next((event["data"] for event in reversed(run["events"]) if event.get("type") == "data-turn"), {})
                    file_ids = next((turn.get("file_ids", []) for turn in state["turns"] if turn.get("run_id") == identity), [])
                    await asyncio.to_thread(self._patch_turn, actor, run,
                        {"status": run["status"], "answer": run.get("answer", ""), "error": run.get("error", "") or ("回答已停止，可继续。" if run["status"] == "stopped" else ""),
                         **{key: terminal[key] for key in ("sources", "plan", "output_files") if key in terminal},
                         "file_ids": list(dict.fromkeys([*file_ids, *[file["id"] for file in terminal.get("output_files", []) if isinstance(file, dict) and file.get("id")]]))}, finish=True)
                    await asyncio.to_thread(self._clear_active, actor, identity)
            except AssistantError:
                await asyncio.to_thread(self._clear_active, actor, identity)
        result = await asyncio.to_thread(self.assistant.conversation, actor)
        state = await asyncio.to_thread(self._read, actor)
        result.update(active_run_id=state.get("active_run_id", ""), query_scopes=state.get("query_scopes") or actor["scopes"], revision=state.get("revision", 0), stream_protocol=1)
        active = self.live.get(result["active_run_id"])
        if active and not set(active["scopes"]) - set(actor["scopes"]):
            result["phase"] = active.get("label", result["phase"])
            for turn in result["turns"]:
                if turn.get("run_id") == active["id"] and turn.get("status") == "pending":
                    turn["answer"] = active.get("answer", "")
                    turn["process"] = copy.deepcopy(active.get("process", []))
        result["turns"] = [public_turn(t) for t in result["turns"]]
        return result

    def _clear_active(self, actor, identity):
        with self.assistant._lock:
            state = self.assistant._state(actor)
            if state.get("active_run_id") == identity:
                state.update(active_run_id="", phase="")
                self.assistant._active.discard(actor["id"])
                self.store.put_document(NAMESPACE, self.assistant._key(actor), state)

    async def history(self, actor, before):
        state = await asyncio.to_thread(self._read, actor)
        if not isinstance(before, (int, float)) or before <= 0:
            raise AssistantError("历史分页位置无效。")
        documents = await asyncio.to_thread(self.store.list_documents, MESSAGES, key_prefix=state["id"] + ":")
        turns = [doc["payload"] for doc in documents if doc["payload"].get("at", 0) < before and self.assistant._allowed(doc["payload"], actor)]
        turns.sort(key=lambda item: item.get("at", 0))
        return {"turns": [public_turn(t) for t in turns[-30:]], "has_more": len(turns) > 30}

    async def stream(self, actor, identity, authorize):
        from pydantic_ai.ui.vercel_ai.response_types import DoneChunk
        # Replays start from the canonical message ID. The SDK builds a fresh
        # message, and the UI replaces that run's reply rather than appending twice.
        run = await asyncio.to_thread(self.get_run, actor, identity)
        cursor, last_auth = 0, 0.0
        while True:
            if time.monotonic() - last_auth > 1:
                try:
                    current = await authorize()
                except AssistantError:
                    break
                if current["id"] != actor["id"] or set(run.get("access_scopes", run["scopes"])) - set(current["scopes"]):
                    break
                last_auth = time.monotonic()
            events = run["events"][cursor:]
            for event in events:
                yield "data: " + json.dumps(event, ensure_ascii=False, separators=(",", ":")) + "\n\n"
                cursor += 1
            if run["status"] not in ACTIVE:
                yield "data: " + DoneChunk().encode(6) + "\n\n"
                break
            await asyncio.sleep(.08)

    async def close(self):
        self.closing = True
        tasks = [task for task in self.workers.values() if not task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
