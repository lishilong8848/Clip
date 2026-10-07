"""Secure-navigation and metadata-catalog regressions for the lighthouse changes.

Verifies that requests for passwords, permission changes, personal-signature
collection and the homepage AdminTools/settings (首页设置、MOP配置、历史记忆导入、
签名管理合并) resolve to the *original protected page link* (handover /
permissions / signature management) through the secure_page shortcut, without
producing a business plan, without calling model_factory, and without touching
native business routes -- while still re-checking the actor's current
authorization.

It also guards the opposite boundary: native business signing (generating a
maintenance form, picking an evaluator, sending a signature-use confirmation,
uploading an already-signed form) must remain NON-security intent (i.e. it must
still reach the model and be able to produce a normal business plan instead of
being captured by the secure regex).

Finally it locks the metadata catalog: POST /api/handover-links stays excluded,
GET /api/handover-links stays included, auth + personal-signature write routes
stay excluded, and POST /api/signatures/usage-confirmations/send stays included.
"""

import copy
import json
import sys
import tempfile
import unittest
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI, Request  # noqa: E402
from pydantic_ai.models.function import DeltaToolCall, FunctionModel  # noqa: E402

from lan_bitable_template_portal.drill_management import DrillManagementService  # noqa: E402
from lan_bitable_template_portal.lighthouse_agent import PortalAgent  # noqa: E402
from lan_bitable_template_portal.lighthouse_ai import AssistantError, LighthouseAssistant  # noqa: E402
from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog  # noqa: E402
from lan_bitable_template_portal.lighthouse_files import LighthouseFiles  # noqa: E402
from lan_bitable_template_portal.lighthouse_model import LighthouseModel  # noqa: E402
from test_drill_management import _fixture_xlsx  # noqa: E402
from test_lighthouse_api import _native_route_catalog  # noqa: E402
from test_lighthouse_stream import Store  # noqa: E402


ACTOR = {"id": "secure-actor", "scopes": ["A"], "is_admin": False, "learning_scopes": ["A"]}

SECURE_LINKS = ("/?admin=handover", "/?admin=permissions", "/signature-management", "/?admin=status")


class SecureNavigationTests(unittest.IsolatedAsyncioTestCase):
    """Password / permission-change / personal-signature-collection requests are
    redirected to the original protected pages without model or business work."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = Store(Path(self.tmp.name) / "state.sqlite3")
        model = Mock()
        model.settings.return_value = {
            "configured": True, "enabled": True, "active_model_id": "default",
            "models": [{"id": "default", "name": "fixture", "model": "fixture-model", "configured": True}],
        }
        self.assistant = LighthouseAssistant(self.store, lambda *_: ([], []), model=model)
        self.files = LighthouseFiles(self.store)
        self.app = FastAPI()

        @self.app.get("/api/learning/papers")
        async def learning_papers(scope: str, today: str = "0"):
            return {"ok": True, "data": {"items": [{"id": "paper-d-1001", "scope": scope}], "total": 1}}

        @self.app.get("/api/learning/review")
        async def learning_review(scope: str):
            return {"ok": True, "data": {"items": [{"id": "review-d-1", "scope": scope}]}}

        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        self.request = Request({
            "type": "http", "scheme": "http", "server": ("testserver", 80),
            "client": ("127.0.0.1", 1), "path": "/api/assistant/messages", "root_path": "",
            "query_string": b"", "headers": [(b"origin", b"http://testserver")],
        })
        self.actor = copy.deepcopy(ACTOR)
        self.auth_calls = 0
        self.authorize_calls = []

    @asynccontextmanager
    async def forbidden_factory(self, *args):
        """Passed to LighthouseModel so ANY model_factory contact fails the test."""
        self.fail("Secure navigation must never call model_factory")
        yield  # pragma: no cover

    async def authorize(self):
        self.auth_calls += 1
        self.authorize_calls.append(("auth", copy.deepcopy(self.actor)))
        return copy.deepcopy(self.actor)

    async def _answer(self, question, *, authorize=None):
        engine = LighthouseModel(self.portal, model_factory=self.forbidden_factory)
        async def emit(*_):
            pass
        return await engine.answer(
            self.actor,
            {"question": question, "operation_id": "secure_nav_%04d" % (self.auth_calls + 1),
             "file_ids": [], "_profile": {"id": "default", "name": "fixture", "model": "fixture-model"}},
            [], self.request, emit, authorize or self.authorize, {},
        )

    async def _assert_secure_page(self, question, expected_link):
        result = await self._answer(question)
        self.assertIn(expected_link, result["answer"])
        self.assertIn("请在原[", result["answer"])
        self.assertNotIn("plan", result, "secure navigation must not prepare a business plan")
        # Auth is re-checked before publishing the protected link, and the model
        # must never be contacted (so no native business route can run either).
        self.assertGreaterEqual(self.auth_calls, 1, "auth must be rechecked before a protected link")
        self.assertGreaterEqual(len(self.authorize_calls), 1)
        return result

    async def test_password_request_returns_handover_page_link(self):
        await self._assert_secure_page("修改交接链接密码", "/?admin=handover")

    async def test_learning_workflows_only_return_original_page(self):
        # Learning WRITE (publish) stays excluded -> original /learning page only.
        result = await self._answer("发布今日学练题单")
        self.assertIn("/learning", result["answer"])
        self.assertNotIn("plan", result)
        self.assertGreater(self.auth_calls, 0)

        # Learning QUERIES are native GET reads under the original account scope.
        async def learning_stream(messages, info):
            has_return = any(getattr(part, "part_kind", "") == "tool-return"
                             for message in messages for part in message.parts)
            if has_return:
                yield "学练未答题与题单已核对（paper-d-1001），画像学练见 /learning 学习页。"
            else:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/learning/papers", "params": {"scope": "A", "today": "0"}}}))}

        @asynccontextmanager
        async def query_factory(*_):
            yield FunctionModel(stream_function=learning_stream)

        async def emit(*_):
            pass

        # "今天学练还有几道未答题" resolves through the native pending shortcut to
        # the original /learning page; the other queries reach the model's query tool.
        expectations = {
            "今天学练还有几道未答题": ["/learning", "今日学练"],
            "答题进度": ["/learning", "画像学练", "paper-d-1001"],
            "查看错题": ["/learning", "画像学练", "paper-d-1001"],
        }
        for question, want in expectations.items():
            with self.subTest(question=question):
                engine = LighthouseModel(self.portal, model_factory=query_factory)
                result = await engine.answer(
                    self.actor,
                    {"question": question, "operation_id": "secure_nav_query_%04d" % (self.auth_calls + 1),
                     "file_ids": [], "_profile": {"id": "default", "name": "fixture", "model": "fixture-model"}},
                    [], self.request, emit, self.authorize, {},
                )
                for expect in want:
                    self.assertIn(expect, result["answer"])
                self.assertNotIn("plan", result)
                self.assertGreater(self.auth_calls, 0)

    async def test_permission_change_returns_permissions_page_link(self):
        await self._assert_secure_page("调整权限管理范围", "/?admin=permissions")

    async def test_personal_signature_collection_returns_signature_management_link(self):
        await self._assert_secure_page("采集我的签名图片", "/signature-management")

    async def test_view_and_change_my_signature_return_signature_management_link(self):
        # Viewing/replacing the current actor's own signature is still the
        # protected personal-signature page, not a maintenance/drill business flow.
        for question in ("查看我的签名", "我的签名怎么更换"):
            with self.subTest(question=question):
                await self._assert_secure_page(question, "/signature-management")

    async def test_admin_note_only_applies_to_non_signature_secure_pages(self):
        # Signature management never demands admin (it is the current actor's own page).
        signature = await self._assert_secure_page("本人签名采集", "/signature-management")
        self.assertNotIn("管理员权限", signature["answer"])

        # Permission change for a non-admin adds the admin note.
        self.actor = {**self.actor, "is_admin": False}
        result = await self._answer("调整权限管理范围")
        self.assertIn("该页面仍要求管理员权限。", result["answer"])

        # An admin sees no such note.
        self.actor = {**ACTOR, "is_admin": True}
        result = await self._answer("调整权限管理范围")
        self.assertIn("/?admin=permissions", result["answer"])
        self.assertNotIn("管理员权限", result["answer"])

    async def test_homepage_setting_returns_status_admin_link(self):
        # The homepage AdminTools/settings page is no longer assistant-managed;
        # any phrase asking for 首页设置 / 管理员设置 resolves to the original /?admin=status.
        for question in ("首页设置", "管理员工具", "管理员设置", "主页面设置"):
            with self.subTest(question=question):
                await self._assert_secure_page(question, "/?admin=status")

    async def test_mop_configuration_returns_status_admin_link(self):
        # MOP 配置 lives inside the excluded homepage AdminTools page.
        for question in ("MOP配置", "维护单配置", "维护单MOP配置"):
            with self.subTest(question=question):
                await self._assert_secure_page(question, "/?admin=status")

    async def test_history_memory_import_returns_status_admin_link(self):
        # 历史记忆导入/通告记忆扫描 is an AdminTools page, not a business import.
        for question in ("历史记忆导入", "导入通告记忆", "扫描通告记忆", "管理通告历史记忆"):
            with self.subTest(question=question):
                await self._assert_secure_page(question, "/?admin=status")

    async def test_signature_management_merge_returns_signature_management_link(self):
        # Signature-management page sub-flows (merge/migrate) belong to the
        # protected /signature-management page, not to a business write intent.
        for question in ("合并重复人员", "迁移已关联签名", "合并签名重复人员"):
            with self.subTest(question=question):
                await self._assert_secure_page(question, "/signature-management")

    async def test_reauthorized_scope_change_blocks_secure_page_before_reply(self):
        async def narrowing():
            self.auth_calls += 1
            return {**self.actor, "scopes": []}
        with self.assertRaises(AssistantError) as error:
            await self._answer("修改交接链接密码", authorize=narrowing)
        self.assertEqual(error.exception.status, 403)
        self.assertGreaterEqual(self.auth_calls, 1)

    async def test_pre_upgrade_mixed_plan_is_blocked_before_any_native_write(self):
        from lan_bitable_template_portal.lighthouse_agent import PLAN_NAMESPACE
        native_calls = []

        @self.app.post("/api/business/fixture")
        async def allowed():
            native_calls.append("allowed")
            return {"ok": True}

        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)
        actor = {**self.actor, "is_admin": True}
        plan = self.portal.prepare(actor, {"title": "旧混合计划", "operations": [{"api_id": "POST /api/business/fixture"}]}, "old_plan", [])
        for excluded in ("POST /api/admin/mop-settings", "POST /api/learning/papers/{id}/answer", "POST /api/learning/questions/{id}/status"):
            plan["operations"] = [{"api_id": "POST /api/business/fixture"}, {"api_id": excluded, "body": {}}]
            for status, stage in (("awaiting_confirmation", "review"), ("awaiting_second_confirmation", "execute")):
                plan["status"] = status
                self.store.put_document(PLAN_NAMESPACE, plan["id"], plan)
                with self.assertRaises(AssistantError):
                    await self.portal.confirm(actor, plan["id"], {"version": plan["version"], "stage": stage}, self.request)
                self.assertEqual(native_calls, [])
                self.assertFalse(self.portal.tasks)

    async def test_native_business_signing_questions_reach_the_model_not_secure_page(self):
        """生成维护单 / 选择评估人 / 发送签名使用确认 / 在维护单或演练中
        使用我的签名 are NON-security intents:
        they must reach model_factory (never short-circuit into a protected link)."""
        for question in ("生成A楼维护单", "请选择评估人", "给A楼发送签名使用确认",
                         "在维护单中使用我的签名", "在演练中使用我的签名"):
            with self.subTest(question=question):
                rounds = []

                async def stream(messages, info):
                    rounds.append(len(messages))
                    yield "业务表单已准备，请核对。"

                @asynccontextmanager
                async def factory(*_):
                    yield FunctionModel(stream_function=stream)

                engine = LighthouseModel(self.portal, model_factory=factory)
                async def emit(*_):
                    pass
                result = await engine.answer(
                    self.actor,
                    {"question": question, "operation_id": "business_%04d" % (self.auth_calls + 1),
                     "file_ids": [], "_profile": {"id": "default", "name": "fixture", "model": "fixture-model"}},
                    [], self.request, emit, self.authorize, {},
                )
                self.assertTrue(rounds, "business intent must reach model_factory (not secure_page)")
                for link in SECURE_LINKS:
                    self.assertNotIn(link, result["answer"], question)

    async def test_upload_signed_reaches_model_not_secure_page_via_sentinel(self):
        """'上传并回填已签维护单' must reach model_factory -- no large fixture is
        needed: a factory that raises a unique sentinel proves the model was
        contacted, and the exact sentinel must propagate unchanged.  A secure
        page (signature collection etc.) never calls model_factory (forbidden_factory
        guards that direction in _assert_secure_page)."""
        class UploadSignedReachedModel(Exception):
            pass

        async def emit(*_):
            pass

        async def authorize():
            return copy.deepcopy(self.actor)

        @asynccontextmanager
        async def raising_factory(*_):
            raise UploadSignedReachedModel("upload-signed reached model_factory")
            yield  # pragma: no cover

        engine = LighthouseModel(self.portal, model_factory=raising_factory)
        for question in ("上传已签名的维护单", "上传并回填A楼已签维护单", "为A楼维护单配置签名人员和时间"):
            with self.subTest(question=question), self.assertRaises(UploadSignedReachedModel) as caught:
                await engine.answer(
                    self.actor,
                    {"question": question, "operation_id": "upload_signed",
                     "file_ids": [], "_profile": {"id": "default", "name": "fixture", "model": "fixture-model"}},
                    [], self.request, emit, authorize, {},
                )
            self.assertEqual(str(caught.exception), "upload-signed reached model_factory")

    async def test_business_signing_still_prepares_a_plan_through_the_model(self):
        """A real business-signing edit (drill execution: commander/evaluator/signers)
        must produce a normal needs_input plan, not be treated as security."""
        root = Path(self.tmp.name)
        service = DrillManagementService(self.store, data_root=root / "forms")
        definition = service.create_definition(
            name="安全演练", year=2026, month=10, file_name="fixture.xlsx",
            source=_fixture_xlsx(with_evaluator=True),
        )
        definition = service.publish(definition["drill_id"], expected_version=definition["version"])
        execution = service.get_execution(definition["drill_id"], "A", create=True)
        execution.update(drill_date="2026-09-29", first_start_time="09:00",
                         version=12, execution_version=2, status="synced")

        reads = []

        @self.app.get("/api/drills/{drill_id}/execution")
        async def state(drill_id: str, scope: str):
            reads.append(drill_id)
            return {"ok": True, "data": {"drill": definition, "execution": copy.deepcopy(execution)}}

        @self.app.put("/api/drills/{drill_id}/execution")
        async def save(drill_id: str, body, scope: str):
            raise AssertionError("preparing a signing form must not write the drill")

        # Rebuild the portal so the catalog now knows the drill routes.
        self.portal = PortalAgent(self.assistant, PortalAPICatalog(self.app), self.files)

        drill_id = definition["drill_id"]

        def tools(messages):
            return [part.content for message in messages for part in message.parts
                    if getattr(part, "part_kind", "") == "tool-return"]

        async def stream(messages, info):
            results = tools(messages)
            if not results:
                yield {0: DeltaToolCall(name="query", json_args=json.dumps({"operation": {
                    "api_id": "GET /api/drills/{drill_id}/execution",
                    "path_params": {"drill_id": drill_id}, "params": {"scope": "A"}}}))}
            elif len(results) == 1:
                yield {0: DeltaToolCall(name="prepare_business", json_args=json.dumps({
                    "title": "填写演练", "operations": [{
                        "api_id": "PUT /api/drills/{drill_id}/execution",
                        "path_params": {"drill_id": drill_id}, "params": {"scope": "A"}}]}))}
            else:
                yield "表单已准备。"

        @asynccontextmanager
        async def factory(*_):
            yield FunctionModel(stream_function=stream)

        engine = LighthouseModel(self.portal, model_factory=factory)
        async def emit(*_):
            pass
        result = await engine.answer(
            self.actor,
            {"question": "重新填写A楼演练的全部内容", "operation_id": "drill_plan",
             "file_ids": [], "_profile": {"id": "default", "name": "fixture", "model": "fixture-model"}},
            [], self.request, emit, self.authorize, {},
        )
        self.assertTrue(reads, "business intent queried the native read route")
        self.assertIn("plan", result, "business signing must produce a business plan")
        self.assertEqual(result["plan"]["status"], "needs_input")
        self.assertTrue(any(field.get("native_drill") for field in result["plan"]["fields"]))
        for link in SECURE_LINKS:
            self.assertNotIn(link, result["answer"])


class SecureMetadataCatalogTests(unittest.TestCase):
    """Metadata catalog keeps GET handover links / signature-use-confirmation,
    but excludes the write/private handover, auth and personal-signature routes."""

    @classmethod
    def setUpClass(cls):
        cls.catalog = _native_route_catalog()

    def test_handover_links_write_is_excluded_but_read_is_retained(self):
        desc = self.catalog.get("GET /api/handover-links")
        self.assertEqual(desc["method"], "GET")
        self.assertTrue(desc["read_only"])
        self.assertIn("交接", desc["name"])
        with self.assertRaises(AssistantError):
            self.catalog.get("POST /api/handover-links")
        # Older write endpoint under _EXCLUDED_PREFIXES is hidden too.
        with self.assertRaises(AssistantError):
            self.catalog.get("POST /api/handover-links-auth")

    def test_auth_routes_stay_excluded(self):
        for cid in ("GET /api/auth/login", "GET /api/auth/status",
                    "POST /api/auth/permission-requests", "POST /api/auth/permissions",
                    "POST /api/auth/permission-directory/grant", "POST /api/auth/permissions/remove"):
            with self.assertRaises(AssistantError, msg=cid):
                self.catalog.get(cid)

    def test_all_home_settings_and_signature_management_operations_are_excluded(self):
        for api_id in ("POST /api/notice-memory/import", "GET /api/admin/mop-settings", "POST /api/admin/mop-settings",
                       "POST /api/admin/notice-memory/history-scan", "POST /api/admin/notice-memory/history-save",
                       "GET /api/signatures/management/people", "GET /api/signatures/management/duplicates",
                       "POST /api/signatures/management/associate", "POST /api/signatures/management/merge",
                       "POST /api/signatures/management/migrate", "POST /api/signatures/management/refresh"):
            with self.subTest(api_id=api_id), self.assertRaises(AssistantError):
                self.catalog.get(api_id)

    def test_personal_signature_write_routes_stay_excluded(self):
        for cid in ("POST /api/signatures/save",
                    "POST /api/signatures/management/submit",
                    "POST /api/signatures/management/temporary",
                    "POST /api/signatures/management/request",
                    "GET /api/signatures/image"):
            with self.assertRaises(AssistantError, msg=cid):
                self.catalog.get(cid)

    def test_signature_usage_confirmation_stays_included(self):
        usage = self.catalog.get("POST /api/signatures/usage-confirmations/send")
        self.assertEqual(usage["id"], "POST /api/signatures/usage-confirmations/send")
        self.assertFalse(usage["read_only"], "signature usage-confirmation is a business write")
        self.assertIn("usage-confirmations/send", usage["id"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
