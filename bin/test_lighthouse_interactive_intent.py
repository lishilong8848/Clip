"""Requested business forms must not degrade into prose field questionnaires."""
import copy
import json
import unittest
from pathlib import Path

from . import test_lighthouse_reference_workflows as ref


class InteractiveIntentTests(unittest.IsolatedAsyncioTestCase):
    def test_workflow_instructions_keep_all_shared_guards_and_only_relevant_manuals(self):
        from lan_bitable_template_portal.lighthouse_model import INSTRUCTIONS, _WORKFLOW_TOPICS, instructions_for_question
        common = [line for line in INSTRUCTIONS.splitlines() if line and not any(line.startswith(prefix) for prefix in _WORKFLOW_TOPICS)]
        for prefix in _WORKFLOW_TOPICS:
            self.assertEqual(sum(line.startswith(prefix) for line in INSTRUCTIONS.splitlines()), 1, prefix)
        for question in ("关联机柜截图", "发送重保签名使用确认", "查询事件", "修改题库", "修改维修跟进"):
            instructions = instructions_for_question(question)
            for line in common:
                self.assertIn(line, instructions)
            self.assertLess(len(instructions), len(INSTRUCTIONS) * .8)
        self.assertIn("机柜截图人工关联", instructions_for_question("关联机柜截图"))
        self.assertNotIn("演练填报先读取", instructions_for_question("关联机柜截图"))
        mixed = instructions_for_question("先填重保，再查询机柜和维修单")
        for prefix in ("重保填报", "补填现有机柜批次", "维修单填写"):
            self.assertIn(prefix, mixed)
        self.assertEqual(instructions_for_question("重新填写全部内容"), INSTRUCTIONS)

    def setUp(self):
        self.fixture = ref.ReferenceWorkflowTests()
        self.fixture.setUp()
        for callback, args, kwargs in self.fixture._cleanups:
            self.addCleanup(callback, *args, **kwargs)
        self.fixture._cleanups.clear()
        service = ref.DrillManagementService(self.fixture.store, data_root=Path(self.fixture.tmp.name) / "forms")
        definition = service.create_definition(name="交互演练", year=2026, month=10, file_name="fixture.xlsx", source=ref._fixture_xlsx(with_evaluator=True))
        definition = service.publish(definition["drill_id"], expected_version=definition["version"])
        execution = service.get_execution(definition["drill_id"], "A", create=True)
        execution.update(drill_date="2026-09-29", first_start_time="09:00", version=12, execution_version=2, status="synced")
        self.definition, self.execution = definition, execution
        self.reads = []

        @self.fixture.app.get("/api/drills/{drill_id}/execution")
        async def state(drill_id: str, scope: str):
            self.assertEqual((drill_id, scope), (definition["drill_id"], "A"))
            self.reads.append(drill_id)
            return {"ok": True, "data": {"drill": definition, "execution": copy.deepcopy(execution)}}

        @self.fixture.app.put("/api/drills/{drill_id}/execution")
        async def save(drill_id: str, body: ref.DrillExecutionRequest, scope: str):
            self.fixture.writes.append(body)
            raise AssertionError("Opening a form must not save the drill")

    def stream(self, *, read_first=True):
        async def stream(messages, info):
            tools = self.fixture.tools(messages)
            retried = any(getattr(part, "part_kind", "") == "retry-prompt" for message in messages for part in message.parts)
            if not tools and (read_first or retried):
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/drills/{drill_id}/execution", "path_params": {"drill_id": self.definition["drill_id"]}, "params": {"scope": "A"}}}))}
            else:
                yield "当前 version=12。请一次性提供演练日期、开始时间、指挥人和签名人，我再准备表单。"
        return stream

    async def test_read_then_prose_is_replaced_by_native_form_without_writing(self):
        result = await self.fixture.answer(self.stream(), "重新填写这份A楼演练的全部内容")
        self.assertTrue(self.reads)
        self.assertEqual(self.fixture.writes, [])
        self.assertNotIn("一次性提供", result["answer"])
        self.assertNotIn("version", result["answer"])
        plan = result["plan"]
        self.assertEqual(plan["status"], "needs_input")
        field = next(field for field in plan["fields"] if field.get("native_drill"))
        children = {child["path"]: child for child in field["children"]}
        self.assertEqual(children["drill_date"]["type"], "date")
        self.assertEqual(children["first_start_time"]["type"], "time")
        for key in ("commander", "evaluator"):
            self.assertTrue(children[key]["person_picker"])
        self.assertIn("step_signers", children)
        self.assertEqual(plan["operations"][0]["body"]["expected_version"], 12)

    async def test_signature_usage_wrong_context_recovers_to_queried_task_without_requery(self):
        from clipflow_backend.api_models import SignatureUsageConfirmationSendRequest
        @self.fixture.app.get("/api/critical-guard/tasks/{task_id}")
        async def task(task_id: str, scope: str):
            self.reads.append(task_id)
            return {"ok": True, "data": {"task_id": task_id, "task_name": "签名用途测试", "responses": [{"scope": scope}]}}
        @self.fixture.app.post("/api/signatures/usage-confirmations/send")
        async def send(body: SignatureUsageConfirmationSendRequest):
            raise AssertionError("Opening a recipient form must not send messages")
        async def stream(messages, info):
            results = self.fixture.tools(messages)
            if not results:
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/critical-guard/tasks/{task_id}", "path_params": {"task_id": "guard-test"}, "params": {"scope": "A"}}}))}
            else:
                operation = {"api_id": "POST /api/signatures/usage-confirmations/send", "body": {"scope": "A", "context_type": "mop", "notice_key": "wrong"}} if len(results) == 1 else results[-1]["editable_forms"][0]
                if len(results) > 1:
                    self.assertEqual(operation["body"]["notice_key"], "critical_guard:guard-test:A")
                yield {0: ref.DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "发送使用确认", "operations": [operation]}))}
        result = await self.fixture.answer(stream, "给A楼重保任务发送签名使用确认，先打开人员选择表单")
        self.assertEqual(len(self.reads), 1)
        self.assertTrue(result["plan"]["fields"][0]["native_usage_confirmation"])
        self.assertEqual(self.fixture.writes, [])

    async def test_no_read_questionnaire_retries_read_then_opens_form(self):
        result = await self.fixture.answer(self.stream(read_first=False), "重新填写全部内容")
        self.assertEqual(len(self.reads), 1)
        self.assertTrue(result["plan"]["fields"])
        self.assertEqual(self.fixture.writes, [])

    async def test_read_then_change_request_recovers_missing_target_without_requery(self):
        async def stream(messages, info):
            results = self.fixture.tools(messages)
            if not results:
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/drills/{drill_id}/execution", "path_params": {"drill_id": self.definition["drill_id"]}, "params": {"scope": "A"}}}))}
            else:
                if len(results) == 1:
                    self.assertIn("editable_form", results[-1])
                    operation = {"api_id": "PUT /api/drills/{drill_id}/execution", "params": {"scope": "A"}}
                else:
                    self.assertFalse(results[-1]["ok"])
                    self.assertFalse(results[-1]["business_written"])
                    operation = results[-1]["editable_forms"][0]
                operation = {**operation, "body": {"signature_time": "2026-10-02T10:30", "evaluation_time": "2026-10-02T11:45"}}
                yield {0: ref.DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "更改时间", "operations": [operation]}))}
        result = await self.fixture.answer(stream, "读取A楼演练的执行信息，保留评估人及其他内容，只把审核人签名时间改成2026-10-02T10:30、评估时间改成2026-10-02T11:45。准备保存修改的待确认操作，不实际提交。")
        self.assertEqual(len(self.reads), 1)
        self.assertEqual(self.fixture.writes, [])
        body = result["plan"]["operations"][0]["body"]
        self.assertEqual(body["signature_time"], "2026-10-02T10:30")
        self.assertEqual(body["evaluation_time"], "2026-10-02T11:45")
        self.assertEqual(body["first_start_time"], "09:00")
        self.assertIn("请核对下方填写项", result["answer"])

    async def test_explicit_change_cannot_be_replaced_by_unchanged_form_or_prose(self):
        operation = {"api_id": "PUT /api/drills/{drill_id}/execution", "path_params": {"drill_id": self.definition["drill_id"]}, "params": {"scope": "A"}}
        async def stream(messages, info):
            results = self.fixture.tools(messages)
            retried = any(getattr(part, "part_kind", "") == "retry-prompt" for message in messages for part in message.parts)
            if not results:
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {**operation, "api_id": "GET /api/drills/{drill_id}/execution"}}))}
            elif len(results) == 1:
                yield {0: ref.DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "未应用的新时间", "operations": [operation]}))}
            elif not retried:
                self.assertFalse(results[-1]["ok"])
                self.assertIn("新值", results[-1]["error"])
                yield "审核人签名时间已修改到10:30，表单已准备好。"
            else:
                yield {0: ref.DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "更改时间", "operations": [{**operation, "body": {"signature_time": "2026-10-02T10:30"}}]}))}
        result = await self.fixture.answer(stream, "读取A楼演练，保留其他填写，只把审核人签名时间改成2026-10-02T10:30，不实际提交。")
        self.assertEqual(result["plan"]["operations"][0]["body"]["signature_time"], "2026-10-02T10:30")
        self.assertNotIn("已修改到", result["answer"])
        self.assertEqual(len(self.reads), 1)
        self.assertEqual(self.fixture.writes, [])

    async def test_query_reference_target_keeps_native_form_and_version(self):
        async def stream(messages, info):
            results = self.fixture.tools(messages)
            if not results:
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/drills/{drill_id}/execution", "path_params": {"drill_id": self.definition["drill_id"]}, "params": {"scope": "A"}}}))}
            elif len(results) == 1:
                query = results[0]["query_ref"]
                self.assertEqual(results[0]["editable_form"]["operation"]["api_id"], "PUT /api/drills/{drill_id}/execution")
                yield {0: ref.DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "填写演练", "operations": [{
                    "api_id": "PUT /api/drills/{drill_id}/execution", "params": {"scope": "A"},
                    "path_params": {"drill_id": {"$query": {"ref": query, "path": "drill.drill_id"}}},
                    "body": {"$query": {"ref": query, "path": "execution"}, "signature_time": "2026-10-01T12:34"}}]}))}
            else:
                self.assertTrue(results[-1]["ok"], results[-1])
                yield "表单已准备。"
        result = await self.fixture.answer(stream, "重新填写A楼演练的全部内容")
        self.assertTrue(any(field.get("native_drill") for field in result["plan"]["fields"]))
        body = result["plan"]["operations"][0]["body"]
        self.assertEqual(body["expected_version"], 12)
        self.assertEqual(body["first_start_time"], "09:00")
        self.assertEqual(body["signature_time"], "2026-10-01T12:34")
        self.assertEqual(self.fixture.writes, [])

    async def test_concrete_registered_read_path_keeps_original_target_and_form(self):
        async def stream(messages, info):
            results = self.fixture.tools(messages)
            if not results:
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": f"GET /api/drills/{self.definition['drill_id']}/execution", "params": {"scope": "A"}}}))}
            else:
                self.assertTrue(results[-1]["ok"], results[-1])
                yield "请在表单选择人员。"
        result = await self.fixture.answer(stream, "重新填写A楼演练的全部内容")
        self.assertEqual(result["plan"]["operations"][0]["path_params"]["drill_id"], self.definition["drill_id"])
        self.assertTrue(result["plan"]["fields"][0]["native_drill"])
        self.assertEqual(self.fixture.writes, [])

    def test_concrete_path_adapter_cannot_infer_writes_or_conflicting_ids(self):
        catalog = ref.PortalAPICatalog(self.fixture.app)
        identity = self.definition["drill_id"]
        with self.assertRaises(ref.AssistantError):
            catalog.read_operation({"api_id": f"PUT /api/drills/{identity}/execution"})
        with self.assertRaises(ref.AssistantError):
            catalog.read_operation({"api_id": f"GET /api/drills/{identity}/execution", "path_params": {"drill_id": "another"}})
        with self.assertRaises(ref.AssistantError):
            catalog.read_operation({"api_id": "GET /api/polling-work-orders/private-role/session"})

    async def test_discovery_does_not_search_api_names_repeatedly_with_a_record_title(self):
        async def stream(messages, info):
            results = self.fixture.tools(messages)
            if not results:
                yield {0: ref.DeltaToolCall(name="discover", json_args=json.dumps({"keyword": "交互演练记录的名字"}))}
            elif len(results) == 1:
                self.assertIn("GET /api/drills/{drill_id}/execution", [item["id"] for item in results[0]["items"]])
                self.assertIn("不是记录搜索", results[0]["note"])
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/drills/{drill_id}/execution", "path_params": {"drill_id": self.definition["drill_id"]}, "params": {"scope": "A"}}}))}
            else:
                yield "请填写表单。"
        result = await self.fixture.answer(stream, "重新填写A楼交互演练记录的全部内容")
        self.assertTrue(result["plan"]["fields"][0]["native_drill"])
        self.assertEqual(len(self.reads), 1)

    async def test_drill_without_complete_snapshot_cannot_skip_native_form(self):
        portal = ref.PortalAgent(self.fixture.assistant, ref.PortalAPICatalog(self.fixture.app), self.fixture.files)
        operation = {"api_id": "PUT /api/drills/{drill_id}/execution", "path_params": {"drill_id": self.definition["drill_id"]},
                     "params": {"scope": "A"}, "body": {"expected_version": 12, "signature_time": "2026-10-01T12:34"}}
        for queries in ({}, {"partial": {"execution": self.execution}}):
            with self.subTest(queries=bool(queries)), self.assertRaises(ref.AssistantError):
                portal.prepare(ref.ACTOR, {"operations": [operation]}, "incomplete-drill", [], queries=queries)
        self.assertEqual(self.fixture.writes, [])

    async def test_drill_copied_readonly_metadata_is_not_submitted_and_version_is_frozen(self):
        portal = ref.PortalAgent(self.fixture.assistant, ref.PortalAPICatalog(self.fixture.app), self.fixture.files)
        body = {**self.execution, "expected_version": {"$result": {"step": 0, "path": "version"}}, "signature_time": "2026-10-01T12:34"}
        plan = portal.prepare(ref.ACTOR, {"operations": [{"api_id": "PUT /api/drills/{drill_id}/execution", "path_params": {"drill_id": self.definition["drill_id"]},
            "params": {"scope": "A"}, "body": body}]}, "copied-drill", [], queries={"original": {"drill": self.definition, "execution": self.execution}})
        saved = plan["operations"][0]["body"]
        self.assertEqual(saved["expected_version"], 12)
        self.assertEqual(saved["signature_time"], "2026-10-01T12:34")
        self.assertNotIn("scope", saved)
        self.assertNotIn("version", saved)
        self.assertNotIn("$result", json.dumps(saved))
        self.assertTrue(plan["fields"][0]["native_drill"])

    async def test_new_cabinet_todo_request_is_not_routed_to_pending_summary(self):
        @self.fixture.app.post("/api/cabinet-power/batches")
        async def create(request: ref.Request):
            raise AssertionError("Prepare must not create a batch")
        async def stream(messages, info):
            yield {0: ref.DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "文本批次", "operations": [{
                "api_id": "POST /api/cabinet-power/batches", "body": {"source": "text"}}]}))}
        result = await self.fixture.answer(stream, "我要新建A楼粘贴文本机柜待办，先打开粘贴识别表单。")
        self.assertTrue(result["plan"]["fields"][0]["native_cabinet_text_create"])
        self.assertEqual(result["sources"], [])
        self.assertEqual(self.fixture.writes, [])

    async def test_how_to_question_does_not_prepare_a_write(self):
        result = await self.fixture.answer(self.stream(), "如何重新填写A楼演练？")
        self.assertNotIn("plan", result)
        self.assertEqual(self.fixture.writes, [])

    async def test_read_only_question_does_not_prepare_a_write(self):
        result = await self.fixture.answer(self.stream(), "只查询A楼演练当前信息，不修改")
        self.assertNotIn("plan", result)
        self.assertEqual(self.fixture.writes, [])

    async def test_ambiguous_target_can_ask_which_record_without_preparing_a_write(self):
        @self.fixture.app.get("/api/drills")
        async def records(scope: str = "A"):
            return {"ok": True, "data": {"items": [{"drill_id": "one", "name": "演练一", "scope": scope},
                                                   {"drill_id": "two", "name": "演练二", "scope": scope}]}}
        async def stream(messages, info):
            if not self.fixture.tools(messages):
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/drills", "params": {"scope": "A"}}}))}
            else:
                yield "有两份演练：演练一、演练二。需要重新填写哪一份？"
        result = await self.fixture.answer(stream, "重新填写A楼演练")
        self.assertNotIn("plan", result)
        self.assertIn("哪一份", result["answer"])
        self.assertEqual(self.fixture.writes, [])

    async def test_work_order_execution_stays_outside_the_assistant(self):
        async def stream(messages, info):
            yield "请提供工单操作人和步骤。"
        result = await self.fixture.answer(stream, "填写这个SOP工单并确认第一步")
        self.assertNotIn("plan", result)
        self.assertIn("原工单入口", result["answer"])
        self.assertEqual(self.reads, [])
        self.assertEqual(self.fixture.writes, [])

    async def test_event_upload_wrong_channel_returns_limit_instead_of_retry_loop(self):
        from clipflow_backend.api_models import WorkbenchActionRequest
        @self.fixture.app.post("/api/workbench-actions")
        async def send(body: WorkbenchActionRequest):
            raise AssertionError("Event must not reach maintenance handler")
        calls = []
        async def stream(messages, info):
            calls.append(1)
            if not self.fixture.tools(messages):
                yield {0: ref.DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "更新事件", "operations": [{
                    "api_id": "POST /api/workbench-actions", "body": {"work_type": "event", "command_format": "notice_command", "scope": "A", "action": "update", "target_record_id": "rec-event", "patch": {"progress": "隔离测试"}}}]}))}
            else:
                yield "事件已更新。"
        result = await self.fixture.answer(stream, "更新A楼事件通告的进展为隔离测试")
        self.assertNotIn("plan", result)
        self.assertIn("Qt 专用链路", result["answer"])
        self.assertIn("未发送", result["answer"])
        self.assertNotIn("事件已更新", result["answer"])
        self.assertEqual(len(calls), 2)
        self.assertEqual(self.fixture.writes, [])

    async def test_screenshot_association_opens_proof_form_not_generic_row_edit(self):
        await self._proof_intent(editable=True)

    async def test_locked_proof_records_report_conflict_without_model_retry_loop(self):
        await self._proof_intent(editable=False)

    async def test_screenshot_missing_rack_opens_correction_not_existing_row_association(self):
        @self.fixture.app.get("/api/cabinet-power/batches/{batch_id}")
        async def batch(batch_id: str):
            return {"ok": True, "data": {"batch_id": batch_id, "version": 1, "scopes": ["A"], "owner_id": ref.ACTOR["id"],
                "source": "image", "rows": [], "images": [{"image_id": "proof-a", "name": "确认图.png", "status": "done", "suggestions": []}]}}
        @self.fixture.app.post("/api/cabinet-power/batches/{batch_id}/images/{image_id}/correct")
        async def correct(batch_id: str, image_id: str, request: ref.Request):
            raise AssertionError("Preparing must not create a cabinet row")
        async def stream(messages, info):
            if not self.fixture.tools(messages):
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/cabinet-power/batches/{batch_id}", "path_params": {"batch_id": "batch-a"}}}))}
            else:
                yield "请告诉我图片编号和要补全的机柜编号。"
        result = await self.fixture.answer(stream, "帮我补全A楼这个图片批次漏识别的机柜")
        field = result["plan"]["fields"][0]
        self.assertTrue(field["native_cabinet_correct"])
        self.assertEqual(field["options_source"], "cabinet_racks")
        self.assertEqual(field["scopes"], ["A"])
        self.assertEqual(field["rows"], [])
        self.assertEqual(result["plan"]["operations"][0]["body"]["version"], 1)
        self.assertNotIn("请告诉", result["answer"])

    async def test_large_form_stays_in_ui_not_model_tool_or_history(self):
        @self.fixture.app.get("/api/cabinet-power/batches/{batch_id}")
        async def batch(batch_id: str):
            return {"ok": True, "data": {"batch_id": batch_id, "version": 1, "scopes": ["A"], "source": "manual", "rows": [
                {"row_id": f"row-{index}", "scope": "A", "room": "201", "rack": f"A{index:03}",
                 "editable": True, "status": "ready", "result": "成功"} for index in range(500)]}}
        @self.fixture.app.patch("/api/cabinet-power/batches/{batch_id}")
        async def save(batch_id: str, request: ref.Request):
            raise AssertionError("Opening form must not write")
        async def stream(messages, info):
            tools = self.fixture.tools(messages)
            if not tools:
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/cabinet-power/batches/{batch_id}", "path_params": {"batch_id": "large-batch"}}}))}
            elif len(tools) == 1:
                yield {0: ref.DeltaToolCall(name="prepare_business", json_args=json.dumps({"title": "修改机柜批次", "operations": [{
                    "api_id": "PATCH /api/cabinet-power/batches/{batch_id}", "path_params": {"batch_id": "large-batch"}}]}))}
            else:
                tool = tools[-1]
                self.assertTrue(tool["ok"], tool)
                self.assertFalse(tool["business_written"])
                self.assertLess(len(json.dumps(tool, ensure_ascii=False)), 5000)
                self.assertEqual(tool["operations"][0]["target"]["batch_id"], "large-batch")
                self.assertNotIn("children", json.dumps(tool))
                yield "表单已准备。"
        result = await self.fixture.answer(stream, "修改A楼这个机柜批次")
        from .lan_bitable_template_portal.lighthouse_model import plan_context
        context = plan_context(result["plan"])
        self.assertLess(len(json.dumps(context, ensure_ascii=False)), 5000)
        self.assertEqual(context["operations"][0]["target"]["batch_id"], "large-batch")
        self.assertNotIn("children", json.dumps(context))
        field = next(field for field in result["plan"]["fields"] if field.get("native_cabinet_edit"))
        self.assertEqual(len(field["children"]), 500)
        self.assertEqual(self.fixture.writes, [])
        history = [{"question": "修改机柜批次", "answer": "请核对表单", "scopes": ["A"], "plan": result["plan"]}]
        async def followup(messages, info):
            history_parts = [part.content for message in messages for part in message.parts
                             if isinstance(getattr(part, "content", None), str) and part.content.startswith("上次操作结果")]
            self.assertEqual(len(history_parts), 1)
            self.assertLess(len(history_parts[0]), 5000)
            self.assertIn("large-batch", history_parts[0])
            self.assertNotIn("children", history_parts[0])
            yield "上次只是准备表单，未保存。"
        await self.fixture.answer(followup, "谢谢", history=history)

    async def _proof_intent(self, *, editable):
        calls = []
        @self.fixture.app.get("/api/cabinet-power/batches/{batch_id}")
        async def batch(batch_id: str):
            calls.append(batch_id)
            return {"ok": True, "data": {"batch_id": batch_id, "version": 1, "scopes": ["A"], "rows": [
                {"row_id": "row-a", "scope": "A", "room": "201", "rack": "A01", "editable": editable, "status": "ready"}],
                "images": [{"image_id": "proof-a", "name": "测试截图.png", "status": "done", "suggestions": []}]}}
        @self.fixture.app.post("/api/cabinet-power/batches/{batch_id}/images/{image_id}/apply")
        async def associate(batch_id: str, image_id: str, request: ref.Request):
            raise AssertionError("Preparing proof form must never write")
        async def stream(messages, info):
            if not self.fixture.tools(messages):
                yield {0: ref.DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/cabinet-power/batches/{batch_id}", "path_params": {"batch_id": "batch-a"}}}))}
            else:
                yield "请提供截图编号和机柜编号，我再关联。"
        result = await self.fixture.answer(stream, "把截图关联到A楼这个批次的机柜")
        self.assertEqual(calls, ["batch-a"])
        if editable:
            self.assertTrue(any(field.get("native_cabinet_proof") for field in result["plan"]["fields"]))
            self.assertNotIn("请提供", result["answer"])
        else:
            self.assertNotIn("plan", result)
            self.assertIn("没有可关联", result["answer"])
            self.assertIn("尚未提交业务修改", result["answer"])


if __name__ == "__main__":
    unittest.main()
