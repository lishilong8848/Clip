"""Isolated tests for per-account Lighthouse model bindings.

Covers the contract that ``CustomModel.for_actor(actor_id)`` returns an
account-bound model snapshot stored under the ``lighthouse_ai`` namespace at
``model:<actor_id>`` and that ``LighthouseAssistant.model_for(actor)`` uses the
account-bound model for conversation, model switching and completion requests.

Only in-memory stores and an isolated temporary SQLite file are used; no real
network requests or production tables are touched. ``protect``/``unprotect`` are
simple reversible test functions and HTTP transport is an ``httpx.MockTransport``.
"""
import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import httpx

from lan_bitable_template_portal.lighthouse_ai import (
    AssistantError,
    CustomModel,
    ENDPOINT,
    MODEL,
    NAMESPACE,
    LighthouseAssistant,
    model_capabilities,
)
from upload_event_module.services.http_client import FeishuHttpClient


def protect(value):
    return "encrypted:" + value


def unprotect(value):
    return value.split(":", 1)[1]


class MemoryStore:
    """Deep-copying memory store using a key namespace tuple."""

    def __init__(self):
        self.docs = {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, data):
        self.docs[namespace, key] = copy.deepcopy(data)

    def list_documents(self, namespace, *, key_prefix=''):
        return [{'key': key, 'payload': copy.deepcopy(value)} for (ns, key), value in self.docs.items()
                if ns == namespace and key.startswith(key_prefix)]


def chat_endpoint_path(model_id):
    return "https://model." + model_id + ".example/v1/chat/completions"


def make_client(handler):
    client = FeishuHttpClient(timeout=2, retries=0, transport=httpx.MockTransport(handler))
    return client


def account_bound_model(store, actor_id, *, client=None):
    """Construct a CustomModel and return (model, client). """
    if client is None:
        client = make_client(lambda request: httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}]}))
    model = CustomModel(store, client=client, protect=protect, unprotect=unprotect)
    return model, client


class ForActorModelTests(unittest.TestCase):
    """Tests for ``CustomModel.for_actor(actor_id)``."""

    def setUp(self):
        self.store = MemoryStore()
        self.client = make_client(lambda request: httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}]}))
        self.addCleanup(self.client.close)

    def test_advanced_configuration_roundtrip_is_private_and_reaches_request(self):
        requests = []
        client = make_client(lambda request: requests.append(request) or httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}]}))
        self.addCleanup(client.close)
        base = CustomModel(self.store, client=client, protect=protect, unprotect=unprotect)
        first, second = base.for_actor('first'), base.for_actor('second')
        fields = {'id': 'custom', 'name': '我的模型', 'model': 'custom-model',
                  'endpoint': 'https://provider.example/custom/infer', 'api_key': 'test-key',
                  'custom_protocol': True, 'tool_calls': True, 'image_input': True,
                  'reasoning': True, 'reasoning_only': True, 'allow_reasoning_off': True,
                  'reasoning_efforts': ['high', 'xhigh'], 'reasoning_effort': 'xhigh'}
        settings = first.configure({'action': 'upsert', 'profile': fields})
        first.configure({'action': 'select', 'id': 'custom'})
        first.complete([{'role': 'user', 'content': '你好'}])
        self.assertEqual(str(requests[-1].url), fields['endpoint'])
        self.assertEqual(json.loads(requests[-1].content)['reasoning_effort'], 'xhigh')
        self.assertEqual(model_capabilities(settings['models'][0]), model_capabilities(fields))
        self.assertNotIn('test-key', json.dumps(settings))
        self.assertFalse(second.settings()['models'])
        first.configure({'action': 'upsert', 'profile': {**fields, 'api_key': '', 'reasoning_only': False,
            'reasoning_effort': 'off', 'tool_calls': False, 'image_input': False}})
        first.complete([])
        self.assertEqual(json.loads(requests[-1].content)['reasoning_effort'], 'none')
        first.configure({'action': 'upsert', 'profile': {'id': 'custom', 'name': '改名', 'model': 'custom-model',
            'endpoint': fields['endpoint']}})
        self.assertEqual(first.profile()['reasoning_effort'], 'off')
        self.assertFalse(first.profile()['image_input'])

    def test_advanced_configuration_rejects_invalid_values_without_overwriting(self):
        model, client = account_bound_model(self.store, 'first')
        self.addCleanup(client.close)
        valid = {'id': 't', 'name': '测试', 'model': 'fixture', 'endpoint': ENDPOINT, 'api_key': 'fixture-key'}
        model.configure({'action': 'upsert', 'profile': valid})
        before = model.settings()
        for change in ({'image_input': 'yes'}, {'tool_calls': 1}, {'reasoning': True, 'reasoning_efforts': []},
                       {'reasoning': True, 'reasoning_efforts': ['unsupported']},
                       {'reasoning': True, 'reasoning_only': True, 'allow_reasoning_off': True, 'reasoning_effort': 'off'},
                       {'reasoning': True, 'reasoning_efforts': ['low'], 'reasoning_effort': 'xhigh'}):
            with self.subTest(change=change), self.assertRaises(AssistantError):
                model.configure({'action': 'upsert', 'profile': {**valid, **change}})
            self.assertEqual(model.settings(), before)
        for endpoint in ('http://public.example/custom', 'https://127.0.0.1/custom',
                         'https://user:pass@provider.example/custom', 'https://provider.example/custom?token=secret'):
            with self.subTest(endpoint=endpoint), self.assertRaises(AssistantError):
                model.configure({'action': 'upsert', 'profile': {**valid, 'custom_protocol': True, 'endpoint': endpoint}})
            self.assertEqual(model.settings(), before)

    def test_legacy_picture_support_does_not_require_probe_and_explicit_off_wins(self):
        model, client = account_bound_model(self.store, 'first')
        self.addCleanup(client.close)
        fields = {'id': 't', 'name': 'MiniMax', 'model': 'WanWu/MiniMax-Auto', 'endpoint': ENDPOINT, 'api_key': 'fixture-key'}
        model.configure({'action': 'upsert', 'profile': fields})
        self.assertTrue(model.profile()['image_input'])
        self.assertNotIn('vision_verified', model.profile())
        model.configure({'action': 'upsert', 'profile': {**fields, 'api_key': '', 'image_input': False}})
        self.assertFalse(model.profile()['image_input'])

    def test_for_actor_returns_instance_bound_to_account(self):
        """for_actor returns an account-bound model with shared client/lock/store."""
        base = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        model_a = base.for_actor("actor_a")
        model_b = base.for_actor("actor_b")
        self.assertIsNot(model_a, model_b)
        self.assertIs(model_a.client, self.client)
        self.assertIs(model_b.client, self.client)
        self.assertIs(model_a.store, self.store)
        self.assertIs(model_b.store, self.store)
        self.assertEqual(model_a.protect("x"), "encrypted:x")
        self.assertEqual(model_b.unprotect("encrypted:x"), "x")

    def test_account_key_uses_lighthouse_model_namespace(self):
        """Account data lives under the lighthouse namespace at model:<actor_id>."""
        base = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        base.settings()  # Initialize the legacy global default template model.
        global_before = copy.deepcopy(self.store.get_document(NAMESPACE, "model"))
        model = base.for_actor("actor_a")
        model.configure({"api_key": "key-a"})
        stored = self.store.get_document(NAMESPACE, "model:actor_a")
        self.assertIsNotNone(stored)
        self.assertEqual(self.store.get_document(NAMESPACE, "model"), global_before)

    def test_new_account_copies_independent_snapshot_from_global(self):
        """A new account can use shared defaults without owning or changing them."""
        self.store.put_document(NAMESPACE, "model", {
            "enabled": True,
            "active_model_id": "default",
            "models": [{
                "id": "default", "name": "旧默认模型", "endpoint": ENDPOINT, "model": MODEL,
                "key_cipher": "encrypted:global-key",
            }],
        })
        base = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        base.settings()  # Initialize the legacy global default template model.
        global_before = copy.deepcopy(self.store.get_document(NAMESPACE, "model"))
        model = base.for_actor("actor_a")
        self.assertEqual(model.settings()["models"][0]["name"], "旧默认模型")
        # Independent: mutating the account does not touch the global document.
        model.configure({"action": "upsert", "profile": {
            "id": "default", "name": "我的新名称", "endpoint": ENDPOINT, "model": MODEL}})
        self.assertEqual(self.store.get_document(NAMESPACE, "model"), global_before)
        self.assertEqual(self.store.get_document(NAMESPACE, "model:actor_a")["models"][0]["name"], "我的新名称")

    def test_two_accounts_add_edit_delete_toggle_and_default_are_isolated(self):
        """Registered APIs: configure/settings/profile/complete only touch their account."""
        base = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        model_a = base.for_actor("actor_a")
        model_b = base.for_actor("actor_b")

        model_a.configure({"api_key": "key-a"})
        model_b.configure({"api_key": "key-b"})

        # Add a second model only to account A.
        model_a.configure({"action": "upsert", "profile": {
            "id": "a2", "name": "A专属模型", "endpoint": chat_endpoint_path("a2"),
            "model": "model-a2", "api_key": "key-a2"}})

        self.assertIn("a2", [p["id"] for p in model_a.settings()["models"]])
        self.assertNotIn("a2", [p["id"] for p in model_b.settings()["models"]])

        # Toggle only account A off; account B stays on.
        model_a.configure({"action": "toggle", "enabled": False})
        self.assertFalse(model_a.settings()["enabled"])
        self.assertTrue(model_b.settings()["enabled"])

        # Edit only account B's default.
        model_b.configure({"action": "upsert", "profile": {
            "id": "default", "name": "B改名", "endpoint": ENDPOINT, "model": MODEL}})
        self.assertEqual(next(p for p in model_b.settings()["models"] if p["id"] == "default")["name"], "B改名")
        self.assertEqual(next(p for p in model_a.settings()["models"] if p["id"] == "default")["name"], "灯塔默认模型")

        # Select default differs per account.
        model_a.configure({"action": "toggle", "enabled": True})
        model_a.configure({"action": "select", "id": "a2"})
        self.assertEqual(model_a.settings()["active_model_id"], "a2")
        self.assertEqual(model_b.settings()["active_model_id"], "default")

        # Delete only account A's extra model.
        model_a.configure({"action": "delete", "id": "a2"})
        self.assertNotIn("a2", [p["id"] for p in model_a.settings()["models"]])
        self.assertNotIn("a2", [p["id"] for p in model_b.settings()["models"]])

    def test_same_model_id_different_endpoint_and_key(self):
        """Accounts may reuse a model id with distinct endpoints and ciphertext."""
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(200, json={"choices": [{"message": {"content": "ok"}}]})

        client = make_client(handler)
        self.addCleanup(client.close)
        base = CustomModel(self.store, client=client, protect=protect, unprotect=unprotect)
        model_a = base.for_actor("actor_a")
        model_b = base.for_actor("actor_b")
        model_a.configure({"api_key": "key-a"})
        model_b.configure({"api_key": "key-b"})
        model_a.configure({"action": "upsert", "profile": {
            "id": "shared", "name": "甲端模型", "endpoint": chat_endpoint_path("shared-a"),
            "model": "model-x", "api_key": "endpoint-key-a"}})
        model_a.configure({"action": "select", "id": "shared"})
        model_b.configure({"action": "upsert", "profile": {
            "id": "shared", "name": "乙端模型", "endpoint": chat_endpoint_path("shared-b"),
            "model": "model-y", "api_key": "endpoint-key-b"}})
        model_b.configure({"action": "select", "id": "shared"})

        calls.clear()
        model_a.complete([{"role": "user", "content": "hi"}])
        self.assertEqual(str(calls[-1].url), chat_endpoint_path("shared-a"))
        self.assertEqual(calls[-1].headers["authorization"], "Bearer endpoint-key-a")
        self.assertEqual(json.loads(calls[-1].content)["model"], "model-x")

        calls.clear()
        model_b.complete([{"role": "user", "content": "hi"}])
        self.assertEqual(str(calls[-1].url), chat_endpoint_path("shared-b"))
        self.assertEqual(calls[-1].headers["authorization"], "Bearer endpoint-key-b")
        self.assertEqual(json.loads(calls[-1].content)["model"], "model-y")

    def test_settings_never_leak_key_cipher_or_api_key(self):
        """settings must not expose key_cipher or api_key for any account."""
        base = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        model = base.for_actor("actor_a")
        model.configure({"action": "upsert", "profile": {
            "id": "m1", "name": "保密模型", "endpoint": chat_endpoint_path("m1"),
            "model": "model-m", "api_key": "secret-value"}})
        text = json.dumps(model.settings(), ensure_ascii=False)
        self.assertNotIn("secret-value", text)
        self.assertNotIn("key_cipher", text)
        self.assertNotIn("encrypted:", text)

    def test_invalid_actor_rejected(self):
        """for_actor rejects missing or invalid actor identifiers."""
        base = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        for bad in (None, "", "   ", [], {}, 123):
            with self.assertRaises(AssistantError):
                base.for_actor(bad)

    def test_own_saves_do_not_change_global_config(self):
        """Persisting an account model never rewrites the legacy global model doc."""
        base = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        base.settings()  # Initialize the legacy global default template model.
        global_before = copy.deepcopy(self.store.get_document(NAMESPACE, "model"))
        model = base.for_actor("actor_a")
        model.configure({"api_key": "key-a"})
        model.configure({"action": "toggle", "enabled": False})
        model.configure({"action": "select", "id": "default"})
        self.assertEqual(self.store.get_document(NAMESPACE, "model"), global_before)
        # A global config that already exists stays byte-for-byte untouched.
        global_doc = {"enabled": True, "active_model_id": "default",
                      "models": [{"id": "default", "name": "只读全局", "endpoint": ENDPOINT, "model": MODEL}]}
        self.store.put_document(NAMESPACE, "model", copy.deepcopy(global_doc))
        base_b = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        base_b.for_actor("actor_b").configure({"api_key": "key-b"})
        self.assertEqual(self.store.get_document(NAMESPACE, "model"), global_doc)


class AccountSqliteRestartTests(unittest.TestCase):
    """New-instance restart independence using an isolated temporary SQLite file."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "accounts.sqlite3"

    def _new_store(self):
        from lan_bitable_template_portal.state_store import LanPortalStateStore
        return LanPortalStateStore(db_path=self.path)

    def test_save_then_restart_preserves_per_account_isolated_state(self):
        first_client = make_client(lambda request: httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}]}))
        self.addCleanup(first_client.close)
        store1 = self._new_store()
        base1 = CustomModel(store1, client=first_client, protect=protect, unprotect=unprotect)
        model_a1 = base1.for_actor("actor_a")
        model_b1 = base1.for_actor("actor_b")
        model_a1.configure({"api_key": "key-a"})
        model_a1.configure({"action": "upsert", "profile": {
            "id": "a2", "name": "A重启模型", "endpoint": chat_endpoint_path("a2"),
            "model": "model-a2", "api_key": "key-a2"}})
        model_a1.configure({"action": "select", "id": "a2"})
        model_b1.configure({"api_key": "key-b"})
        model_b1.configure({"action": "toggle", "enabled": False})

        # A brand new store instance from the same DB file must see only its own account state.
        store2 = self._new_store()
        client2 = make_client(lambda request: httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}]}))
        self.addCleanup(client2.close)
        base2 = CustomModel(store2, client=client2, protect=protect, unprotect=unprotect)
        model_a2 = base2.for_actor("actor_a")
        model_b2 = base2.for_actor("actor_b")
        self.assertEqual(model_a2.settings()["active_model_id"], "a2")
        self.assertIn("a2", [p["id"] for p in model_a2.settings()["models"]])
        self.assertFalse(model_b2.settings()["enabled"])
        self.assertNotIn("a2", [p["id"] for p in model_b2.settings()["models"]])


class LighthouseAssistantModelForTests(unittest.TestCase):
    """``LighthouseAssistant.model_for(actor)`` feeds account-bound conversation and model switching."""

    def setUp(self):
        self.store = MemoryStore()
        self.search = lambda query, actor: ([], [])
        self.client = make_client(lambda request: httpx.Response(200, json={
            "choices": [{"message": {"content": "ok"}}]}))
        self.addCleanup(self.client.close)

    def _assistant(self):
        base = CustomModel(self.store, client=self.client, protect=protect, unprotect=unprotect)
        return LighthouseAssistant(self.store, self.search, model=base)

    def test_model_for_returns_account_bound_instance(self):
        assistant = self._assistant()
        model_a = assistant.model_for({"id": "actor_a", "scopes": ["A"]})
        model_b = assistant.model_for({"id": "actor_b", "scopes": ["A"]})
        self.assertIsNot(model_a, model_b)
        self.assertTrue(hasattr(model_a, "configure"))
        self.assertTrue(hasattr(model_a, "settings"))
        self.assertTrue(hasattr(model_a, "profile"))
        self.assertTrue(hasattr(model_a, "complete"))

    def test_model_for_is_used_by_all_conversation_entry_points(self):
        """Chat, selection, and refresh must all route through the account model."""
        assistant = self._assistant()

        actor_a = {"id": "actor_a", "scopes": ["A"]}
        actor_b = {"id": "actor_b", "scopes": ["A"]}

        # Each account gets its own fully independent binding.
        model_a = assistant.model_for(actor_a)
        model_b = assistant.model_for(actor_b)
        model_a.configure({"api_key": "key-a"})
        model_b.configure({"api_key": "key-b"})
        model_a.configure({"action": "upsert", "profile": {
            "id": "m1", "name": "甲模型", "endpoint": chat_endpoint_path("m1"),
            "model": "model-a1", "api_key": "key-a1"}})
        model_b.configure({"action": "upsert", "profile": {
            "id": "m1", "name": "乙模型", "endpoint": chat_endpoint_path("m1-b"),
            "model": "model-b1", "api_key": "key-b1"}})
        model_a.configure({"action": "select", "id": "m1"})
        model_b.configure({"action": "select", "id": "m1"})

        # The assistant conversation must reflect account A's own model settings.
        convo_a = assistant.conversation(actor_a)
        self.assertEqual(convo_a["model_id"], "m1")
        self.assertEqual(convo_a["model_name"], "甲模型")

        # Account B's conversation is entirely independent.
        convo_b = assistant.conversation(actor_b)
        self.assertEqual(convo_b["model_name"], "乙模型")

        # Placing an existing completed turn must not be dropped by a model change.
        assistant.select_model(actor_a, {"conversation_id": convo_a["conversation_id"], "model_id": "m1"})
        stored = self.store.get_document(NAMESPACE, "conversation:actor_a")
        self.assertEqual(stored["id"], convo_a["conversation_id"])

    def test_model_switch_keeps_turns_and_context(self):
        """Model switching must not discard conversation turns/context."""
        assistant = self._assistant()
        actor = {"id": "actor_a", "scopes": ["A"]}
        model = assistant.model_for(actor)
        model.configure({"api_key": "key-a"})
        model.configure({"action": "upsert", "profile": {
            "id": "m1", "name": "一号模型", "endpoint": chat_endpoint_path("m1"),
            "model": "model-1", "api_key": "key-1"}})
        model.configure({"action": "upsert", "profile": {
            "id": "m2", "name": "二号模型", "endpoint": chat_endpoint_path("m2"),
            "model": "model-2", "api_key": "key-2"}})
        model.configure({"action": "select", "id": "m1"})

        # Place one completed turn and a compressed summary in the account conversation store.
        convo = {"id": "convo_1", "turns": [{
            "operation_id": "turn_0000000000001", "question": "旧问题",
            "answer": "旧答案", "scopes": ["A"], "status": "completed", "model_name": "一号模型"}],
            "context": {"summary": "旧摘要", "compressed_turns": 3, "last_compressed_at": 1}}
        self.store.put_document(NAMESPACE, "conversation:actor_a", copy.deepcopy(convo))

        # Switching to model 2 through the assistant keeps the same id, turns and context.
        switched = assistant.select_model(actor, {"conversation_id": "convo_1", "model_id": "m2"})
        self.assertEqual(switched["conversation_id"], "convo_1")
        self.assertEqual(len(switched["turns"]), 1)
        self.assertEqual(switched["turns"][0]["operation_id"], "turn_0000000000001")
        self.assertEqual(switched["context"]["compressed_turns"], 3)
        self.assertEqual(switched["model_id"], "m2")

    def test_chat_and_compression_use_only_the_owner_configuration(self):
        calls = []
        client = make_client(lambda request: calls.append(request) or httpx.Response(200, json={
            "choices": [{"message": {"content": "测试回答"}}]}))
        self.addCleanup(client.close)
        model = CustomModel(self.store, client=client, protect=protect, unprotect=unprotect)
        assistant = LighthouseAssistant(self.store, self.search, model=model)
        actor_a, actor_b = {"id": "actor_a", "scopes": ["A"]}, {"id": "actor_b", "scopes": ["B"]}
        assistant.model_for(actor_a).configure({"api_key": "key-a"})
        assistant.model_for(actor_b).configure({"api_key": "key-b"})
        assistant.model_for(actor_b).configure({"action": "toggle", "enabled": False})
        model.configure({"action": "toggle", "enabled": False})
        conversation = assistant.conversation(actor_a)
        result = assistant.chat(actor_a, {"question": "你好", "operation_id": "isolated_account_chat",
            "conversation_id": conversation["conversation_id"]})
        self.assertEqual(result["turns"][-1]["status"], "completed")
        self.assertEqual(calls[-1].headers["authorization"], "Bearer key-a")
        previous = [{"operation_id": "older_" + str(n), "question": "旧问题" * 200,
            "answer": "旧回答" * 800, "scopes": ["A"]} for n in range(8)]
        state = assistant._state(actor_a)
        context = assistant._compress(state, previous, actor_a, assistant.model_for(actor_a).profile())
        self.assertGreater(context["compressed_turns"], 0)
        self.assertEqual(calls[-1].headers["authorization"], "Bearer key-a")
        self.assertEqual(len(calls), 2)

    def test_stream_accept_freezes_each_owners_profile_not_global_or_other_account(self):
        from types import SimpleNamespace
        from lan_bitable_template_portal.lighthouse_stream import LighthouseStream
        assistant = self._assistant()
        actors = [{"id": "actor_a", "scopes": ["A"]}, {"id": "actor_b", "scopes": ["B"]}]
        stream = LighthouseStream(SimpleNamespace(assistant=assistant, files=None), engine=object())
        for actor in actors:
            assistant.model_for(actor).configure({"api_key": "key-" + actor["id"]})
            run, public = stream._accept(actor, {"question": "你好", "operation_id": "isolated_stream_" + actor["id"],
                "conversation_id": assistant._state(actor)["id"]})
            self.assertEqual(run["_turn"]["_profile"]["key_cipher"], "encrypted:key-" + actor["id"])
            self.assertNotIn("key_cipher", json.dumps(public))
            self.assertNotIn("_profile", json.dumps(self.store.get_document("lighthouse_runs", run["id"])))


class SharedDefaultModelsTests(unittest.TestCase):
    def setUp(self):
        self.store = MemoryStore()
        model, client = account_bound_model(self.store, 'fixture')
        self.addCleanup(client.close)
        self.service = LighthouseAssistant(self.store, lambda *_: [], model=model)
        self.admin = {'id': 'admin', 'is_admin': True, 'scopes': ['A', 'B']}
        self.user = {'id': 'user', 'is_admin': False, 'scopes': ['A']}

    def shared(self, identity='first', **changes):
        return self.service.model_settings(self.admin, {'scope': 'shared', 'action': 'upsert', 'profile': {
            'id': identity, 'name': identity, 'endpoint': ENDPOINT, 'model': MODEL,
            'api_key': 'fixture-shared-key', **changes}})

    def test_administrator_add_edit_delete_is_live_for_every_account(self):
        for name in ('MiniMax', 'GLM', 'Qwen', 'Deepseek'):
            self.shared(name, model='WanWu/' + name + '-Auto')
        settings = self.service.model_settings(self.user)
        self.assertEqual(len(settings['models']), 4)
        self.assertTrue(all(row['shared'] and row['configured'] for row in settings['models']))
        self.assertFalse(settings['can_manage_shared'])
        self.assertNotIn('fixture-shared-key', json.dumps(settings))
        self.assertNotIn('key_cipher', json.dumps(settings))
        self.assertEqual(self.store.get_document(NAMESPACE, 'model:user')['models'], [])
        self.shared('Deepseek', name='Updated', api_key='replacement-key')
        profile = self.service.model_for(self.user).profile('shared_Deepseek')
        self.assertEqual(profile['name'], 'Updated')
        self.assertEqual(unprotect(profile['key_cipher']), 'replacement-key')
        self.service.model_settings(self.admin, {'scope': 'shared', 'action': 'delete', 'id': 'shared_Deepseek'})
        self.assertEqual(len(self.service.model_settings(self.user)['models']), 3)
        with self.assertRaises(AssistantError):
            self.service.model_for(self.user).profile('shared_Deepseek')

    def test_non_admin_cannot_change_shared_models_even_through_personal_endpoint(self):
        self.shared()
        before = copy.deepcopy(self.store.docs)
        for payload in (
            {'scope': 'shared', 'action': 'delete', 'id': 'shared_first'},
            {'action': 'delete', 'id': 'shared_first'},
            {'action': 'upsert', 'profile': {'id': 'shared_first'}},
            {'scope': 'shared', 'action': 'upsert', 'profile': {'id': 'new'}},
        ):
            with self.subTest(payload=payload), self.assertRaises(AssistantError) as failed:
                self.service.model_settings(self.user, payload)
            self.assertEqual(failed.exception.status, 403)
        self.assertEqual(self.store.get_document(NAMESPACE, 'model'), before[NAMESPACE, 'model'])

    def test_dormant_legacy_copies_retire_but_personal_edits_and_selection_survive(self):
        self.shared()
        legacy = self.store.get_document(NAMESPACE, 'model')
        legacy['active_model_id'] = 'first'
        self.store.put_document(NAMESPACE, 'model:dormant', legacy)
        edited = copy.deepcopy(legacy)
        edited['models'][0]['name'] = 'Personal copy'
        self.store.put_document(NAMESPACE, 'model:user', edited)
        self.service.model_settings(self.admin, {'scope': 'shared', 'action': 'delete', 'id': 'shared_first'})
        self.assertEqual(self.service.model_settings({'id': 'dormant'})['models'], [])
        own = self.service.model_settings(self.user)
        self.assertEqual([m['name'] for m in own['models']], ['Personal copy'])
        self.assertEqual(own['active_model_id'], 'first')
        self.assertFalse(own['models'][0]['shared'])
        self.store.put_document(NAMESPACE, 'model:legacy_single', {'enabled': True, 'key_cipher': 'encrypted:personal-legacy-key'})
        legacy_model = self.service.model_for({'id': 'legacy_single'})
        self.assertEqual(legacy_model.profile()['key_cipher'], 'encrypted:personal-legacy-key')
        self.assertFalse(legacy_model.settings()['models'][0]['shared'])

    def test_unassigned_formal_account_can_chat_but_cannot_query_business(self):
        from types import SimpleNamespace
        from lan_bitable_template_portal.lighthouse_stream import LighthouseStream
        self.shared()
        actor = {**self.user, 'scopes': []}
        stream = LighthouseStream(SimpleNamespace(assistant=self.service, files=None), engine=object())
        conversation = self.service._state(actor)['id']
        run, _ = stream._accept(actor, {'question': '你好', 'conversation_id': conversation,
                                       'operation_id': 'scope_free_general_question'})
        self.assertEqual(run['scopes'], [])
        for question in ('今天E楼有几条事件', '有几条进行中的通告'):
            with self.subTest(question=question), self.assertRaises(AssistantError):
                stream._accept(actor, {'question': question, 'conversation_id': conversation,
                                      'operation_id': 'scope_free_business_question'})


class DirectModelProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_custom_endpoint_image_and_reasoning_reach_openai_sdk(self):
        from pydantic_ai import Agent, BinaryContent
        from lan_bitable_template_portal.lighthouse_model import configured_model
        from unittest.mock import patch
        store = MemoryStore()
        model, sync_client = account_bound_model(store, 'fixture')
        self.addCleanup(sync_client.close)
        model.configure({'action': 'upsert', 'profile': {'id': 't', 'name': '测试', 'model': 'fixture',
            'endpoint': 'https://provider.example/custom/infer', 'custom_protocol': True, 'api_key': 'test-key',
            'reasoning': True}})
        captured = []
        async def provider(request):
            captured.append(request)
            return httpx.Response(200, json={'id': 'test', 'object': 'chat.completion', 'created': 1, 'model': 'fixture',
                'choices': [{'index': 0, 'finish_reason': 'stop', 'message': {'role': 'assistant', 'content': '红色正方形'}}]})
        real_client = httpx.AsyncClient
        class FixtureClient(real_client):
            def __init__(self, **kwargs):
                super().__init__(**kwargs, transport=httpx.MockTransport(provider))
        with patch.object(httpx, 'AsyncClient', FixtureClient):
            async with configured_model(model, model.profile()) as configured:
                result = await Agent(configured, model_settings={'extra_body': {'reasoning_effort': 'xhigh'}}).run(
                    ['图片是什么', BinaryContent(data=b'synthetic-image', media_type='image/png')])
        self.assertEqual(result.output, '红色正方形')
        self.assertEqual(str(captured[0].url), 'https://provider.example/custom/infer')
        body = json.loads(captured[0].content)
        self.assertEqual(body['reasoning_effort'], 'xhigh')
        self.assertEqual(body['messages'][0]['content'][1]['type'], 'image_url')


if __name__ == "__main__":
    unittest.main()
