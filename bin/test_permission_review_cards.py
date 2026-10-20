"""Isolated permission approval tests. No real messages, grants or directory writes."""
import asyncio
import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from fastapi.testclient import TestClient
from . import test_account_levels as levels
from .test_feishu_delivery import qt_credentials_fixture

from clipflow_backend.main import FastAPIPortalController
from lan_bitable_template_portal.server import PortalRuntime
from lan_bitable_template_portal.portal_auth import AUTH_COOKIE_NAME

from lan_bitable_template_portal.permission_review_cards import PermissionCards, card, parse_event
from lan_bitable_template_portal.portal_auth import LI_SHILONG_OPEN_ID, BUILDING_OPEN_ID_MAP
from lan_bitable_template_portal.portal_service import PortalError
from openclaw_service.assistant.lighthouse_ai import AssistantError


class PermissionReviewTests(levels.AccountLevelTests):
    def setUp(self):
        self.enterContext(qt_credentials_fixture())
        super().setUp()
        # Migration preserves existing grants; empty test stores no longer bootstrap them.
        auth = self.h.auth
        payload = auth._load_permissions_locked()
        for home, oid in BUILDING_OPEN_ID_MAP.items():
            payload['users'][oid] = {'enabled': True, 'role': 'admin' if home == 'H' else 'building',
                'scopes': auth._admin_scopes() if home == 'H' else [home]}
        for oid in (LI_SHILONG_OPEN_ID, 'ou_fixtureHM1', 'ou_fixtureHM2'):
            payload['users'][oid] = {**payload['users'].get(oid, {}),
                'enabled': True, 'role': 'admin', 'scopes': auth._admin_scopes()}
        auth._save_permissions_locked(payload)
        self.controller = SimpleNamespace(_clear_read_cache=Mock(), _notify_permission_review_result=Mock())
        self.bot = SimpleNamespace(runtime=SimpleNamespace(auth_manager=self.h.auth, state_store=self.h.auth._state_store, service=self.h.service),
            controller=self.controller, messenger=None,
            _resolve_user=AsyncMock(return_value={"open_id": LI_SHILONG_OPEN_ID, "name": "审批管理员"}))
        self.cards = PermissionCards(self.bot)

    def application(self):
        return self.h.auth.create_permission_request(open_id="ou_fixtureAO", name="测试AO", scopes=["B"], reason="隔离测试", approver_open_id=LI_SHILONG_OPEN_ID)

    def message(self, data, action="approve", reason=""):
        private = self.cards.request(data["request_id"])
        return {"union_id": "on_Review", "reply_to_message_id": "om_Approval", "review": {
            "request_id": data["request_id"], "token": private["review_card_token"], "action": action, "reason": reason}}

    def test_selected_approver_validation_and_token_redaction(self):
        with self.assertRaises(PortalError):
            self.h.auth.create_permission_request(open_id="ou_fixtureAO", scopes=["B"], approver_open_id="ou_fixtureAE")
        data = self.application()
        self.assertEqual(data["approver_open_id"], LI_SHILONG_OPEN_ID)
        self.assertNotIn("review_card_token", data)
        self.assertNotIn("review_card_token", self.h.auth.get_current_permission_request("ou_fixtureAO", data["request_id"]))
        self.assertEqual(self.h.auth.get_current_permission_request("ou_fixtureBE", data["request_id"]), {})

    def test_card_review_adds_scope_without_changing_level_modules_or_duty(self):
        data = self.application()
        before = self.h.auth.account_profile("ou_fixtureAO")
        duty_before = {oid:self.h.auth.account_profile(oid) for oid in BUILDING_OPEN_ID_MAP.values()}
        result = asyncio.run(self.cards.handle(self.message(data)))
        self.assertIn("已通过", result["text"])
        after = self.h.auth.account_profile("ou_fixtureAO")
        self.assertEqual(set(after["scope_rules"]), {"A", "B"})
        for key in ("level", "category", "role", "modules"):
            self.assertEqual(before[key], after[key])
        self.assertEqual(duty_before, {oid:self.h.auth.account_profile(oid) for oid in BUILDING_OPEN_ID_MAP.values()})
        asyncio.run(self.cards.handle(self.message(data)))
        self.assertEqual(self.controller._notify_permission_review_result.call_count, 1, "duplicate clicks must not repeat result notifications")

    def test_another_web_admin_can_decide_then_card_cannot_reverse(self):
        data = self.application()
        self.h.auth.approve_permission_request(data["request_id"], updated_by="ou_fixtureHM1")
        result = asyncio.run(self.cards.handle(self.message(data, "reject", "拒绝")))
        self.assertIn("已通过", result["text"])
        self.assertEqual(self.cards.request(data["request_id"])["reviewed_by"], "ou_fixtureHM1")
        self.assertFalse(self.controller._notify_permission_review_result.called)

    def test_reject_needs_reason_and_does_not_grant(self):
        data = self.application()
        with self.assertRaises(AssistantError):
            asyncio.run(self.cards.handle(self.message(data, "reject")))
        asyncio.run(self.cards.handle(self.message(data, "reject", "无需跨楼")))
        self.assertEqual(self.h.auth.scopes_for_open_id("ou_fixtureAO"), ["A"])
        self.assertEqual(self.cards.request(data["request_id"])["status"], "rejected")

    def test_expired_or_revoked_review_cannot_grant(self):
        data = self.application()
        self.cards.update(data["request_id"], expires_at_ts=1)
        with self.assertRaisesRegex(AssistantError, "过期"):
            asyncio.run(self.cards.handle(self.message(data)))
        data = self.application()
        with patch.object(self.h.auth, "can_review_permission_request", return_value=False), self.assertRaises(AssistantError):
            asyncio.run(self.cards.handle(self.message(data)))
        self.assertEqual(self.h.auth.scopes_for_open_id("ou_fixtureAO"), ["A"])

    def test_simultaneous_approval_only_notifies_once(self):
        data = self.application()
        async def both():
            return await asyncio.gather(self.cards.handle(self.message(data)), self.cards.handle(self.message(data)))
        results = asyncio.run(both())
        self.assertTrue(all("已通过" in r["text"] for r in results))
        self.assertEqual(self.controller._notify_permission_review_result.call_count, 1)
        self.h.auth.sync_directory_permissions(self.h.login.snapshot["people"].values())
        self.assertEqual(set(self.h.auth.scopes_for_open_id("ou_fixtureAO")), {"A", "B"})

    def test_forwarded_forged_and_superseded_cards_cannot_grant(self):
        data = self.application()
        message = self.message(data)
        self.bot._resolve_user = AsyncMock(return_value={"open_id":"ou_fixtureHM1", "name":"另一管理员"})
        with self.assertRaises(AssistantError):
            asyncio.run(self.cards.handle(message))
        self.bot._resolve_user = AsyncMock(return_value={"open_id":LI_SHILONG_OPEN_ID, "name":"管理员"})
        forged = copy.deepcopy(message);forged["review"]["token"] = "x" * 43
        with self.assertRaises(AssistantError):
            asyncio.run(self.cards.handle(forged))
        self.application()
        self.assertIn("替代", asyncio.run(self.cards.handle(message))["text"])
        self.assertEqual(self.h.auth.scopes_for_open_id("ou_fixtureAO"), ["A"])

    def test_notification_failure_keeps_web_application_pending(self):
        data = self.application()
        with patch("lan_bitable_template_portal.portal_service.external_real_write_guard", return_value={"real_write_allowed":True}):
            asyncio.run(self.cards.notify(data["request_id"]))
        saved = self.cards.request(data["request_id"])
        self.assertEqual(saved["status"], "pending")
        self.assertEqual(saved["notification_status"], "failed")
        self.h.auth.approve_permission_request(data["request_id"], updated_by=LI_SHILONG_OPEN_ID)
        self.assertEqual(self.h.auth.scopes_for_open_id("ou_fixtureAO"), ["A", "B"])

    def test_parser_and_terminal_card_remove_action_buttons(self):
        data = self.cards.request(self.application()["request_id"])
        value = {"kind":"permission_review", "request_id":data["request_id"], "token":data["review_card_token"], "action":"approve"}
        payload = {"header":{"app_id":"cliFixture", "event_type":"card.action.trigger", "event_id":"event1"},"event":{
            "operator":{"union_id":"on_Review", "open_id":"ou_BotScope"},"context":{"open_chat_id":"oc_Private", "open_message_id":"om_Approval"},
            "action":{"value":value, "form_value":{"reason":"同意"}}}}
        parsed = parse_event(payload, "cliFixture")
        self.assertEqual(parsed["kind"], "permission_review")
        self.assertIsNone(parse_event(payload, "wrongApp"))
        bad = copy.deepcopy(payload);bad["event"]["action"]["form_value"]["scopes"] = ["ALL"]
        self.assertIsNone(parse_event(bad, "cliFixture"))
        self.assertTrue(any(e["tag"] == "form" for e in card(data)["body"]["elements"]))
        data["status"] = "approved"
        self.assertFalse(any(e["tag"] == "form" for e in card(data)["body"]["elements"]))

    def test_notification_uses_unified_sender_and_stable_uuid(self):
        data = self.application()
        self.bot.messenger = SimpleNamespace()
        sender = AsyncMock(return_value={'message_id': 'om_Approval', 'sender_app_id': 'cli_fixture'})
        with patch("lan_bitable_template_portal.portal_service.external_real_write_guard", return_value={"real_write_allowed":True}), \
                patch('lan_bitable_template_portal.feishu_delivery.send_agent_message', sender):
            asyncio.run(self.cards.notify(data["request_id"]))
            self.cards.update(data["request_id"], notification_status="failed")
            asyncio.run(self.cards.notify(data["request_id"]))
        first, second = sender.call_args_list
        self.assertIs(first.args[0], self.bot.runtime)
        self.assertEqual(first.args[1], LI_SHILONG_OPEN_ID)
        self.assertEqual(first.kwargs['message_uuid'], second.kwargs['message_uuid'])
        self.assertEqual(self.cards.request(data['request_id'])['review_sender_app_id'], 'cli_fixture')

    def test_supervisor_card_applies_exact_self_access_request(self):
        data=self.h.auth.create_permission_request(open_id='ou_fixtureAO',name='测试AO',reason='隔离权限申请',approver_open_id='ou_fixtureAS',
            requested_access={'level':'L4','category':'operator','scopes':['A'],'modules':['learning','maintenance']})
        self.bot._resolve_user=AsyncMock(return_value={'open_id':'ou_fixtureAS','name':'A楼主管'})
        rendered=str(card(self.cards.request(data['request_id'])))
        self.assertIn('目标级别：L4',rendered);self.assertIn('申请模块',rendered)
        result=asyncio.run(self.cards.handle(self.message(data)))
        self.assertIn('已通过',result['text'])
        self.assertEqual(self.h.auth.account_profile('ou_fixtureAO')['modules'],['learning','maintenance'])

    def test_rich_request_rejects_scope_tampering_and_disabled_applicant(self):
        auth=self.h.auth
        data=auth.create_permission_request(open_id='ou_fixtureAO',name='测试AO',approver_open_id=LI_SHILONG_OPEN_ID,
            requested_access={'level':'L4','category':'operator','scopes':['A'],'modules':['learning']})
        before=copy.deepcopy(auth._load_permissions_locked())
        with self.assertRaisesRegex(PortalError,'完整权限'):
            auth.approve_permission_request(data['request_id'],scopes=['B'],updated_by=LI_SHILONG_OPEN_ID)
        self.assertEqual(auth._load_permissions_locked(),before)
        before['users']['ou_fixtureAO']['enabled']=False
        auth._save_permissions_locked(before)
        with self.assertRaises(PortalError):
            auth.approve_permission_request(data['request_id'],updated_by=LI_SHILONG_OPEN_ID)

    def test_expired_request_is_not_presented_as_pending(self):
        data=self.application()
        self.cards.update(data['request_id'],expires_at_ts=1)
        current=self.h.auth.get_current_permission_request('ou_fixtureAO',data['request_id'])
        self.assertEqual(current['status'],'expired')
        self.assertEqual(self.h.auth.list_permission_requests_for_admin(status='pending',reviewer_id=LI_SHILONG_OPEN_ID),[])

    def test_supervisor_web_review_is_scoped_and_audited(self):
        auth=self.h.auth
        auth._sessions['supervisor-test']={'user':{'open_id':'ou_fixtureAS'},'role':'building','allowed_scopes':['A'],'expires_at':9999999999}
        own=auth.create_permission_request(open_id='ou_fixtureAO',name='测试AO',approver_open_id='ou_fixtureAS',
            requested_access={'level':'L4','category':'operator','scopes':['A'],'modules':['learning']})
        other=auth.create_permission_request(open_id='ou_fixtureBE',scopes=['A'],approver_open_id=LI_SHILONG_OPEN_ID)
        with patch.object(PortalRuntime,'auth_manager',auth),patch.object(PortalRuntime,'state_store',auth._state_store):
            controller=FastAPIPortalController(host='127.0.0.1',port=18766)
            client=TestClient(controller._build_app())
            self.addCleanup(client.close)
            with patch.object(controller._permission_cards,'enqueue_update'):
                headers={'Cookie':f'{AUTH_COOKIE_NAME}=supervisor-test'}
                listing=client.get('/api/auth/permission-requests/admin?limit=1',headers=headers)
                self.assertEqual(listing.status_code,200,listing.text)
                self.assertEqual([item['request_id'] for item in listing.json()['data']['items']],[own['request_id']])
                denied=client.post('/api/auth/permission-requests/'+other['request_id']+'/approve',headers=headers,json={})
                self.assertGreaterEqual(denied.status_code,400)
                with patch.object(controller,'_queue_permission_review_result',return_value={'ok':True,'queued':True}):
                    approved=client.post('/api/auth/permission-requests/'+own['request_id']+'/approve',headers=headers,json={})
                self.assertEqual(approved.status_code,200,approved.text)
                self.assertNotIn('permissions',approved.json()['data'])
        self.assertEqual(auth.account_profile('ou_fixtureAO')['modules'],['learning'])
        audit=auth._state_store.list_business_operation_audits(domain='accounts',limit=20)
        self.assertTrue(any(row.get('metadata',{}).get('request_id')==own['request_id'] for row in audit))

    def test_actual_request_and_web_review_routes_share_state(self):
        auth = self.h.auth
        for token, oid in (("requester-test", "ou_fixtureAO"), ("reviewer-test", "ou_fixtureHM1")):
            auth._sessions[token] = {"user":{"open_id":oid,"name":oid}, "role":auth.role_for_open_id(oid), "allowed_scopes":auth.scopes_for_open_id(oid), "expires_at":9999999999}
        with patch.object(PortalRuntime, "auth_manager", auth), patch.object(PortalRuntime, "state_store", auth._state_store):
            controller = FastAPIPortalController(host="127.0.0.1", port=18766)
            client = TestClient(controller._build_app())
            self.addCleanup(client.close)
            requester = {"Cookie":f"{AUTH_COOKIE_NAME}=requester-test"}
            reviewer = {"Cookie":f"{AUTH_COOKIE_NAME}=reviewer-test"}
            options = client.get("/api/auth/permission-approvers", headers=requester)
            self.assertEqual(options.status_code, 200, options.text)
            self.assertIn(LI_SHILONG_OPEN_ID, [row["open_id"] for row in options.json()["data"]["items"]])
            with patch.object(controller._permission_cards, "enqueue") as enqueue:
                response = client.post("/api/auth/permission-requests", headers=requester, json={"scopes":["B"],"approver_open_id":LI_SHILONG_OPEN_ID})
            self.assertEqual(response.status_code, 200, response.text)
            data = response.json()["data"]["request"]
            enqueue.assert_called_once_with(data["request_id"])
            self.assertNotIn("review_card_token", data)
            peer = auth.create_permission_request(open_id="ou_fixtureHM2", name="同级经理", approver_open_id=LI_SHILONG_OPEN_ID,
                requested_access={"level":"L1", "category":"manager", "scopes":["ALL"], "modules":["maintenance"]})
            listing = client.get("/api/auth/permission-requests/admin", headers=reviewer)
            self.assertEqual(listing.status_code, 200, listing.text)
            visible_ids = [item["request_id"] for item in listing.json()["data"]["items"]]
            self.assertIn(data["request_id"], visible_ids)
            self.assertNotIn(peer["request_id"], visible_ids)
            legacy=client.get('/api/auth/permissions',headers=reviewer)
            self.assertEqual(legacy.status_code,200,legacy.text)
            visible_accounts=[item['open_id'] for item in legacy.json()['data']['users']]
            self.assertIn('ou_fixtureAO',visible_accounts)
            self.assertNotIn('ou_fixtureHM2',visible_accounts)
            self.assertNotIn(LI_SHILONG_OPEN_ID,visible_accounts)
            path = "/api/auth/permission-requests/" + data["request_id"] + "/approve"
            self.assertEqual(client.post(path, headers=requester, json={}).status_code, 403)
            cross_origin=client.post(path,headers={**reviewer,'Origin':'https://other.invalid'},json={})
            self.assertEqual(cross_origin.status_code,403)
            self.assertEqual(auth.get_current_permission_request('ou_fixtureAO',data['request_id'])['status'],'pending')
            with patch.object(controller, "_queue_permission_review_result", return_value={"ok":True,"queued":True}):
                approved = client.post(path, headers=reviewer, json={})
            self.assertEqual(approved.status_code, 200, approved.text)
            current = client.get("/api/auth/permission-requests/current", headers=requester, params={"request_id":data["request_id"]})
            self.assertEqual(current.json()["data"]["request"]["status"], "approved")
            self.assertEqual(set(auth.scopes_for_open_id("ou_fixtureAO")), {"A","B"})
            requested={'level':'L2','category':'engineer','scopes':['A'],'modules':['maintenance']}
            with patch.object(controller._permission_cards, 'enqueue'):
                response=client.post('/api/auth/permission-requests',headers=requester,json={'scopes':['A'],'reason':'申请变更权限','approver_open_id':LI_SHILONG_OPEN_ID,'requested_access':requested})
            self.assertEqual(response.status_code,200,response.text)
            application=response.json()['data']['request']
            self.assertEqual(application['requested_access']['modules'],['maintenance'])
            with patch.object(controller,'_queue_permission_review_result',return_value={'ok':True,'queued':True}):
                approved=client.post('/api/auth/permission-requests/'+application['request_id']+'/approve',headers=reviewer,json={})
            self.assertEqual(approved.status_code,200,approved.text)
            self.assertEqual(auth.account_profile('ou_fixtureAO')['level'],'L2')

    def test_result_notification_is_queued_without_blocking_approval(self):
        controller=FastAPIPortalController.__new__(FastAPIPortalController)
        controller._permission_cards=SimpleNamespace(enqueue_update=Mock())
        controller._submit_background=Mock(return_value=True)
        controller._notify_permission_review_result=Mock()
        result=controller._queue_permission_review_result({'request_id':'fixture'},approved=True,actor_name='隔离测试')
        self.assertTrue(result['queued'])
        controller._notify_permission_review_result.assert_not_called()
        controller._permission_cards.enqueue_update.assert_called_once_with('fixture')
