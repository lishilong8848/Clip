"""Isolated checks for the document-library link preview and Wiki save path.

Every collaborator is synthetic: the token manager is the shared RemoteTestCase
fake, the HTTP client is that test's recording fake, and the service reuses
LibraryFixture's temporary data root. Only the fixed Feishu endpoints are
answered, no configuration or credential file is read and no real provider
call is made. The Wiki/Base tokens below are synthetic fixtures.
"""
import copy
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import Mock, patch

# Production packages live next to this module. Fixtures are imported as bin.* so
# they stay the *same* module objects the other suites use.
sys.path.insert(0, str(Path(__file__).resolve().parent))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from lan_bitable_template_portal import document_library_remote as remote_module
from lan_bitable_template_portal.document_library_portal import install_document_library_portal
from lan_bitable_template_portal.document_library import DocumentLibraryService
from lan_bitable_template_portal.portal_auth import PortalAuthManager
from bin.test_document_library import LibraryFixture, actor, configuration
from bin.test_document_library import AssistantError as ServiceError
from bin.test_document_library_remote import (
    AssistantError, FakeHttpClient, FakeTokenManager, ROTATED_CREDENTIALS, RemoteTestCase)

# Two error classes are involved and both are checked explicitly:
# - AssistantError: raised by the mocked Bitable transport (RemoteTestCase._error);
# - ServiceError:   raised by the real DocumentLibraryService (same class the real
#                   app raises, i.e. openclaw_service.assistant.lighthouse_ai).

BASE = "https://team.feishu.cn"
BASE_TOKEN = "bascnPreviewBase01"
WIKI_TOKEN = "wikcnPreviewNode01"
TABLES_URL = "https://open.feishu.cn/open-apis/bitable/v1/apps/{0}/tables".format(BASE_TOKEN)
WIKI_URL = "https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node"


def base_link(table=""):
    return BASE + "/base/" + BASE_TOKEN + ("?table=" + table if table else "")


def wiki_link(table=""):
    return BASE + "/wiki/" + WIKI_TOKEN + ("?table=" + table if table else "")


def link_key(base, table):
    return "table_" + hashlib.sha256((base + ":" + table).encode()).hexdigest()[:20]


def tables_page(items, has_more=False, page_token=""):
    return {"code": 0, "data": {"items": items, "has_more": has_more, "page_token": page_token}}


def node_page(base=BASE_TOKEN):
    return {"code": 0, "data": {"node": {"obj_type": "bitable", "obj_token": base}}}


def fields_page(items, has_more=False, page_token=""):
    return {"code": 0, "data": {"items": items, "has_more": has_more, "page_token": page_token}}


def table(table_id, name):
    return {"table_id": table_id, "name": name}


def field(field_id, name, type=1):
    return {"field_id": field_id, "field_name": name, "type": type}


class PreviewRemoteTests(RemoteTestCase):
    """Resolution and preview contracts through the mocked Bitable transport."""

    def setUp(self):
        super().setUp()
        self._script = []
        self._after_call = None
        # Own the transport patches. RemoteTestCase registers its own with
        # cleanup providers, so they are only live while its suite runs last;
        # this class must hold them for its whole lifetime instead.
        self.patches = [
            patch.object(remote_module, '_error',
                         side_effect=lambda message, status=400, category='':
                         AssistantError(message, status, category=category)),
            patch.object(remote_module.DocumentLibraryRemote, '_token_manager', staticmethod(lambda: self.manager)),
            patch.object(remote_module.DocumentLibraryRemote, '_http_client',
                         lambda adapter: self._fake_client(adapter)),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        super().tearDown()

    def resolve(self, url, table_id="", script=()):
        # inspect_link builds its own adapter and fake client. Each call is
        # answered from the recorded script and stays a read-only GET.
        self._script = list(script)
        self.http = None
        return remote_module.DocumentLibraryRemote.inspect_link(url, table_id)

    def _queue(self):
        self.http.before_call = None
        if callable(self._after_call):
            self._after_call()
        if self._script:
            self.http.json_responses.append(self._script.pop(0))
        self.http.before_call = self._queue

    def _fake_client(self, adapter):
        client = super()._fake_client(adapter)
        client.before_call = self._queue
        return client

    def test_base_link_with_table_resolves_to_canonical_source(self):
        fields = [field("fldTitle", "标题"), field("fldFiles", "附件", 17)]
        result = self.resolve(base_link("tblManual"), script=[
            tables_page([table("tblManual", " Manuals ")]), fields_page(fields)])
        self.assertEqual([call["method"] for call in self.calls], ["GET", "GET"])
        self.assertEqual(
            [(call["url"], call["retries"]) for call in self.calls],
            [(TABLES_URL, 0), (TABLES_URL + "/tblManual/fields", 0)])
        self.assertEqual(self.calls[0]["params"], {"page_size": 100})
        self.assertEqual(result["tables"], [table("tblManual", "Manuals")])
        self.assertEqual(result["fields"], fields)
        source = result["source"]
        self.assertEqual((source["base_token"], source["table_id"]), (BASE_TOKEN, "tblManual"))
        self.assertEqual(source["name"], "Manuals")
        self.assertEqual(source["id"], link_key(BASE_TOKEN, "tblManual"))
        self.assertEqual(result["url"], BASE + "/base/" + BASE_TOKEN + "?table=tblManual")
        self.assertEqual(source["url"], result["url"])
        self.assertEqual(self.http.closed, 1)

    def test_wiki_link_with_table_uses_the_wiki_node_resolved_base(self):
        fields = [field("fldTitle", "标题")]
        result = self.resolve(wiki_link("tblKnowledge"), script=[
            node_page(), tables_page([table("tblKnowledge", "Knowledge")]), fields_page(fields)])
        endpoints = [(call["url"], call["params"]) for call in self.calls]
        self.assertEqual(endpoints, [
            (WIKI_URL, {"token": WIKI_TOKEN}),
            (TABLES_URL, {"page_size": 100}),
            (TABLES_URL + "/tblKnowledge/fields", {"page_size": 100})])
        source = result["source"]
        self.assertEqual((source["base_token"], source["table_id"]), (BASE_TOKEN, "tblKnowledge"))
        self.assertEqual(source["id"], link_key(BASE_TOKEN, "tblKnowledge"))
        self.assertEqual(result["url"], BASE + "/base/" + BASE_TOKEN + "?table=tblKnowledge")
        self.assertEqual(result["fields"], fields)

    def test_wiki_link_without_table_selects_the_only_table(self):
        fields = [field("fldTitle", "标题")]
        result = self.resolve(wiki_link(), script=[node_page(), tables_page([table("tblOnly", "Only")]),
                                                   fields_page(fields)])
        self.assertEqual(result["source"]["table_id"], "tblOnly")
        self.assertEqual(self.calls[-1]["url"], TABLES_URL + "/tblOnly/fields")

    def test_wiki_link_without_table_returns_choices_when_many(self):
        many = [table("tblA", "A"), table("tblB", "B")]
        result = self.resolve(wiki_link(), script=[node_page(), tables_page(many)])
        self.assertEqual(len(self.calls), 2)
        self.assertIsNone(result["source"])
        self.assertEqual(result["tables"], many)
        self.assertEqual(result["fields"], [])
        self.assertEqual(result["url"], wiki_link())

    def test_selecting_a_table_reads_only_that_table_fields(self):
        many = [table("tblA", "A"), table("tblB", "B")]
        fields = [field("fldOwner", "Owner", 11)]
        result = self.resolve(wiki_link("tblB"), script=[node_page(), tables_page(many), fields_page(fields)])
        self.assertEqual(result["source"]["table_id"], "tblB")
        self.assertEqual(result["source"]["name"], "B")
        self.assertEqual([call["url"] for call in self.calls],
                         [WIKI_URL, TABLES_URL, TABLES_URL + "/tblB/fields"])
        # A table id chosen by the operator has the same effect as ?table=.
        self.assertEqual(self.resolve(wiki_link(), "tblB",
                                     script=[node_page(), tables_page(many), fields_page(fields)]
                                     )["source"]["table_id"], "tblB")

    def test_fields_are_read_across_pages(self):
        first, second = field("fldOne", "One"), field("fldTwo", "Two")
        result = self.resolve(base_link("tblManual"), script=[
            tables_page([table("tblManual", "Manuals")]),
            fields_page([first], has_more=True, page_token="f2"),
            fields_page([second])])
        self.assertEqual(result["fields"], [first, second])
        self.assertEqual(self.calls[-1]["params"], {"page_size": 100, "page_token": "f2"})

    def test_rejected_links_never_reach_the_transport(self):
        for url, table_id in ((base_link("tblManual") + "&table=tblOther", ""),
                              (base_link("tblManual"), "tblOther"),
                              (base_link("tblManual"), "tbl/../x"),
                              ("https://evil.example/base/" + BASE_TOKEN + "?table=tblManual", ""),
                              ("http://team.feishu.cn/base/" + BASE_TOKEN + "?table=tblManual", ""),
                              ("https://team.feishu.cn:8443/base/" + BASE_TOKEN + "?table=tblManual", ""),
                              ("https://team.feishu.cn/base/" + BASE_TOKEN + "?table=not-a-table", ""),
                              ("https://team.feishu.cn/base/" + BASE_TOKEN + "?table=", ""),
                              ("", ""), (None, "")):
            with self.subTest(url=url, table_id=table_id):
                with self.assertRaises(AssistantError) as raised:
                    self.resolve(url, table_id)
                self.assertEqual(raised.exception.status, 400)
                self.assertNotIn(BASE_TOKEN, str(raised.exception))
                # The transport was never touched: no fake client was created.
                self.assertIsNone(self.http)

    def test_a_well_formed_link_does_reach_the_transport(self):
        # The guard in test_rejected_links_never_reach_the_transport is not vacuous.
        self.resolve(base_link("tblManual"), script=[
            tables_page([table("tblManual", "Manuals")]), fields_page([field("fldA", "A")])])
        self.assertEqual(len(self.calls), 2)

    def test_denied_and_unreadable_tables_are_reported_without_leaking_ids(self):
        cases = (
            ("denied", [{"code": 1254302, "msg": "secret provider text"}], "provider_error", 502),
            # No readable table: reported as a permission problem, not a 502.
            ("unreadable", [{"code": 0, "data": {"items": [], "has_more": False}}], "", 403),
            ("unknown table", [tables_page([table("tblOther", "Other")])], "", 403))
        for label, script, category, status in cases:
            with self.subTest(label):
                with self.assertRaises(AssistantError) as raised:
                    self.resolve(base_link("tblManual"), script=script)
                self.assertEqual(raised.exception.category, category)
                self.assertEqual(raised.exception.status, status)
                self.assertNotIn("secret provider text", str(raised.exception))
                self.assertNotIn("tblOther", str(raised.exception))
                self.assertNotIn(BASE_TOKEN, str(raised.exception))

    def test_non_bitable_wiki_node_is_refused_before_any_table_read(self):
        # A Wiki node that is not a bitable is a clear, user-facing rejection;
        # a malformed node payload is a bad provider response. Neither reads tables.
        for node, status in (({"obj_type": "docx", "obj_token": BASE_TOKEN}, 400),
                             ({"obj_type": "sheet", "obj_token": BASE_TOKEN}, 400),
                             ({}, 502), ({"obj_type": "bitable"}, 502)):
            with self.subTest(node=node):
                with self.assertRaises(AssistantError) as raised:
                    self.resolve(wiki_link("tblKnowledge"), script=[{"code": 0, "data": {"node": node}}])
                self.assertEqual(raised.exception.status, status)
                self.assertEqual(len(self.calls), 1)
                self.assertEqual(self.calls[0]["url"], WIKI_URL)

    def test_malformed_tables_payloads_fail_closed(self):
        bad_pages = ({"code": 0, "data": {}},
                     {"code": 0, "data": {"items": [], "has_more": 0}},
                     {"code": 0, "data": {"items": [{"table_id": "bad", "name": "X"}], "has_more": False}},
                     {"code": 0, "data": {"items": [{"table_id": "tblA", "name": " "}], "has_more": False}},
                     {"code": 0, "data": {"items": [table("tblA", "A"), table("tblA", "A2")], "has_more": False}})
        for page in bad_pages:
            with self.subTest(page=page):
                with self.assertRaises(AssistantError) as raised:
                    self.resolve(base_link("tblA"), script=[page])
                self.assertEqual(raised.exception.status, 502)

    def test_malformed_table_pagination_fails_closed(self):
        # A repeating cursor loops with empty pages until the MAX_PAGES budget is
        # spent, so it surfaces as an incomplete read (503), not a page error.
        for page, status in (({"code": 0, "data": {"items": [], "has_more": True}}, 502),
                             ({"code": 0, "data": {"items": [], "has_more": True, "page_token": "loop"}}, 503),
                             ({"code": 0, "data": {"items": [], "has_more": True, "page_token": 7}}, 502)):
            with self.subTest(page=page):
                script = [page] * 2 if page.get("page_token") == "loop" else [page]
                with self.assertRaises(AssistantError) as raised:
                    self.resolve(base_link("tblA"), script=script)
                self.assertEqual(raised.exception.status, status)

    def test_malformed_fields_payloads_fail_closed(self):
        pages = tables_page([table("tblManual", "Manuals")])
        for fields in ({}, {"code": 0, "data": {}}, {"code": 0, "data": {"items": [], "has_more": False}},
                       fields_page([{"field_id": "bad", "field_name": "X", "type": 1}]),
                       fields_page([{"field_id": "fldA", "field_name": " ", "type": 1}]),
                       fields_page([{"field_id": "fldA", "field_name": "X", "type": "1"}]),
                       fields_page([field("fldA", "A"), field("fldA", "B")]),
                       fields_page([field("fldA", "A")], has_more=True, page_token="loop")):
            with self.subTest(fields=fields):
                script = [pages, fields] + ([fields] if "loop" in json.dumps(fields) else [])
                with self.assertRaises(AssistantError) as raised:
                    self.resolve(base_link("tblManual"), script=script)
                self.assertEqual(raised.exception.status, 502)

    def test_auto_id_is_stable_for_the_same_base_or_wiki(self):
        fields = [field("fldTitle", "标题")]
        base_ids = [self.resolve(base_link("tblManual"), script=[tables_page([table("tblManual", "Manuals")]),
                                                                 fields_page(fields)])["source"]["id"]
                    for _ in range(2)]
        wiki_ids = [self.resolve(wiki_link(), script=[node_page(), tables_page([table("tblManual", "M")]),
                                                      fields_page(fields)])["source"]["id"]
                    for _ in range(2)]
        other = self.resolve(base_link("tblOther"), script=[tables_page([table("tblOther", "Other")]),
                                                            fields_page(fields)])["source"]["id"]
        self.assertEqual(base_ids[0], base_ids[1])
        self.assertEqual(wiki_ids[0], wiki_ids[1])
        # The Wiki link and its resolved Base link address the same source.
        self.assertEqual(base_ids[0], wiki_ids[0])
        self.assertNotEqual(base_ids[0], other)
        self.assertRegex(base_ids[0], r"^table_[0-9a-f]{20}$")

    def test_preview_sends_reads_only(self):
        self.resolve(wiki_link("tblKnowledge"), script=[node_page(), tables_page([table("tblKnowledge", "K")]),
                                                        fields_page([field("fldA", "A")])])
        self.assertTrue(self.calls)
        self.assertEqual({call["method"] for call in self.calls}, {"GET"})
        self.assertEqual({call["kind"] for call in self.calls}, {"json"})
        self.assert_no_retry()
        self.assertNotIn("client_token", json.dumps(self.calls))
        # Reads never touch the external write guard.
        self.assertEqual(self.guard_calls, 0)

    def test_credential_change_between_reads_stops_without_a_mixed_response(self):
        """Base tables read, then credentials rotate, then fields would be read."""
        rotated = []

        def rotate():
            # Runs after the first read is recorded and before the second call.
            if self.http.calls and not rotated:
                rotated.append(True)
                self.manager.credentials = ROTATED_CREDENTIALS

        self._after_call = rotate
        with self.assertRaises(AssistantError) as raised:
            self.resolve(base_link("tblManual"),
                         script=[tables_page([table("tblManual", "Manuals")]), fields_page([field("fldA", "A")])])
        self.assertEqual(raised.exception.category, "credential_changed")
        self.assertEqual(raised.exception.status, 401)
        # The first read happened; the second was never sent, so no mixed result.
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.http.json_responses, [])


class PreviewServiceTests(LibraryFixture):
    """Preview through the real service; only the Bitable transport is synthetic."""

    def setUp(self):
        super().setUp()
        self.transport = FakeHttpClient()
        self.tokens = FakeTokenManager()
        self.patches = [
            patch.object(remote_module, '_error',
                         side_effect=lambda message, status=400, category='':
                         ServiceError(message, status, category=category)),
            patch.object(remote_module.DocumentLibraryRemote, '_token_manager', staticmethod(lambda: self.tokens)),
            patch.object(remote_module.DocumentLibraryRemote, '_http_client',
                         lambda adapter: self.transport if adapter._http is None else adapter._http),
        ]
        for item in self.patches:
            item.start()

    def tearDown(self):
        # Always stop: a leftover transport patch would follow the next suite
        # around and serve its requests from this fixture.
        for item in reversed(self.patches):
            item.stop()
        super().tearDown()

    def test_preview_is_rejected_for_non_admin(self):
        # (actor, expected status): unauthenticated/guest -> 401, module or admin -> 403.
        for denied, status in ((actor(), 403), ({**actor(), 'is_guest': True}, 401),
                               ({**actor(admin=True), 'modules': []}, 403),
                               ({**actor(admin=True), 'read_only': True}, 403)):
            with self.subTest(actor=denied):
                with self.assertRaises(ServiceError) as raised:
                    self.service.preview_table(denied, {'url': base_link('tblX')})
                self.assertEqual(raised.exception.status, status)
        self.assertEqual(self.transport.calls, [])

    def test_preview_payload_is_strict(self):
        good = {'url': base_link('tblManual')}
        for payload in ({**good, 'base_token': BASE_TOKEN}, {**good, 'name': 'X'}, {'table_id': 'tblX'}, []):
            with self.subTest(payload=payload):
                with self.assertRaises(ServiceError):
                    self.service.preview_table(self.admin, payload)
        self.assertEqual(self.transport.calls, [])

    def test_preview_returns_the_resolution_and_rechecks_admin(self):
        resolved = {'url': base_link('tblManual'), 'source': None, 'tables': [table('tblManual', 'M')], 'fields': []}
        with patch.object(remote_module.DocumentLibraryRemote, 'inspect_link', return_value=resolved) as inspect:
            self.assertEqual(self.service.preview_table(self.admin, {'url': base_link('tblManual'), 'table_id': 'tblManual'}),
                             resolved)
        inspect.assert_called_once_with(base_link('tblManual'), 'tblManual')

        calls = []
        def refresh(current):
            calls.append(current)
            return {**current, 'is_admin': False} if len(calls) == 2 else dict(current)
        self.service.refresh_actor = refresh
        with patch.object(remote_module.DocumentLibraryRemote, 'inspect_link', return_value=resolved):
            with self.assertRaises(ServiceError) as raised:
                self.service.preview_table(self.admin, {'url': base_link('tblManual')})
        self.assertEqual(raised.exception.status, 403)
        self.assertEqual(len(calls), 2)

    def test_preview_rejects_a_non_bitable_wiki_node_without_reading_tables(self):
        """The whole preview path runs really; only the HTTP transport is the fixture."""
        self.transport.json_responses = [{'code': 0, 'data': {'node': {'obj_type': 'docx', 'obj_token': BASE_TOKEN}}}]
        # The transport's rejection is re-raised by the service unchanged.
        with self.assertRaises(ServiceError) as raised:
            self.service.preview_table(self.admin, {'url': wiki_link('tblKnowledge')})
        self.assertEqual(raised.exception.status, 400)
        self.assertEqual([call['url'] for call in self.transport.calls], [WIKI_URL])
        self.assertEqual(self.remote.creates + self.remote.updates + self.remote.uploads, 0)

    def test_preview_does_not_persist_anything(self):
        before = [item['id'] for item in self.service.tables(self.admin)['items']]
        payloads = [{'url': base_link('tblManual')}, {'url': wiki_link()}, {'url': base_link('tblNew')}]
        self.transport.json_responses = [
            tables_page([table('tblManual', 'Manuals')]), fields_page([field('fldA', 'A')]),
            node_page(), tables_page([table('tblA', 'A'), table('tblB', 'B')]),
            tables_page([table('tblNew', 'New')]), fields_page([field('fldB', 'B')])]
        results = [self.service.preview_table(self.admin, payload) for payload in payloads]
        self.assertEqual([result['source'] and result['source']['id'] for result in results],
                         [link_key(BASE_TOKEN, 'tblManual'), None, link_key(BASE_TOKEN, 'tblNew')])
        # The saved configuration is untouched and the record store is never written.
        self.assertEqual([item['id'] for item in self.service.tables(self.admin)['items']], before)
        self.assertEqual((self.remote.creates, self.remote.updates, self.remote.uploads), (0, 0, 0))
        self.assertEqual({call['method'] for call in self.transport.calls}, {'GET'})

    def test_save_rejects_an_explicitly_mismatched_wiki_base(self):
        resolved = {'url': base_link('tblKnowledge'),
                    'source': {'id': 'table_' + '0' * 20, 'name': 'Knowledge', 'base_token': BASE_TOKEN,
                               'table_id': 'tblKnowledge', 'url': base_link('tblKnowledge')},
                    'tables': [table('tblKnowledge', 'Knowledge')], 'fields': [field('fldA', 'A')]}
        with patch.object(remote_module.DocumentLibraryRemote, 'inspect_link', return_value=resolved):
            with self.assertRaises(ServiceError) as raised:
                self.service.save_table(self.admin, configuration(id='wiki', url=wiki_link('tblKnowledge'),
                                                                  base_token='bascnOtherBase02', table_id='tblKnowledge'))
        self.assertEqual(str(raised.exception), 'Wiki 链接与多维表标识不一致。')
        self.assertEqual([item['id'] for item in self.service.tables(self.admin)['items']], ['manuals'])

    def test_save_requires_a_selected_wiki_table(self):
        many = {'url': wiki_link(), 'source': None, 'tables': [table('tblA', 'A'), table('tblB', 'B')], 'fields': []}
        with patch.object(remote_module.DocumentLibraryRemote, 'inspect_link', return_value=many):
            with self.assertRaises(ServiceError) as raised:
                self.service.save_table(self.admin, configuration(id='wiki', url=wiki_link()))
        self.assertEqual(str(raised.exception), '请先选择具体的数据表。')


class PreviewRouteTests(LibraryFixture):
    """POST /preview through the real portal bridge authorization."""

    def setUp(self):
        super().setUp()
        self.profile = {'role': 'admin', 'modules': ['knowledge'], 'scopes': ['A']}
        self.session = {'user': {'open_id': 'ouFixtureAdmin', 'name': 'Fixture admin'}, 'is_admin': True}
        auth = SimpleNamespace(
            account_profile=lambda identity: copy.deepcopy(self.profile),
            scopes_for_open_id=lambda identity: list(self.profile['scopes']),
            _scopes_from_raw=lambda values: values or [])
        auth.is_admin = lambda session: PortalAuthManager.is_admin(auth, session)
        auth.session_scopes = lambda session: PortalAuthManager.session_scopes(auth, session)
        self.session_reads = Mock(side_effect=lambda request: copy.deepcopy(self.session))
        self.controller = SimpleNamespace(
            _current_session=self.session_reads,
            _request_base_url=lambda request: str(request.base_url).rstrip('/'))
        runtime = SimpleNamespace(auth_manager=auth,
                                  state_store=SimpleNamespace(db_path=Path(self.temp) / 'portal.sqlite3'))

        def build(root, **kwargs):
            return DocumentLibraryService(root, remote_factory=lambda _: self.remote,
                                          knowledge=self.kb, clock=lambda: self.now, **kwargs)

        self.enterContext(patch(
            'lan_bitable_template_portal.document_library_portal.DocumentLibraryService', side_effect=build))
        # The preview's Bitable reads go to a synthetic transport, never the network.
        self.transport = FakeHttpClient()
        self.tokens = FakeTokenManager()
        self.patches = [
            patch.object(remote_module, '_error',
                         side_effect=lambda message, status=400, category='':
                         ServiceError(message, status, category=category)),
            patch.object(remote_module.DocumentLibraryRemote, '_token_manager', staticmethod(lambda: self.tokens)),
            patch.object(remote_module.DocumentLibraryRemote, '_http_client',
                         lambda adapter: adapter._http or self.transport),
        ]
        for item in self.patches:
            item.start()
        app = FastAPI()
        install_document_library_portal(app, self.controller, runtime)
        self.client = self.enterContext(TestClient(app, base_url='https://portal.example'))
        self.origin = {'origin': 'https://portal.example'}

    def tearDown(self):
        # Stop before the client closes so no request can see a half-torn state.
        for item in reversed(self.patches):
            item.stop()
        super().tearDown()

    def post(self, payload, headers=None):
        return self.client.post('/api/document-library/preview', json=payload,
                                headers=self.origin if headers is None else headers)

    def assert_nothing_saved(self):
        # The fixture saved one 'manuals' entry; preview must not add, edit or upload.
        self.assertEqual([item['id'] for item in self.service.tables(self.admin)['items']], ['manuals'])
        self.assertEqual((self.remote.creates, self.remote.updates, self.remote.uploads), (0, 0, 0))

    def test_preview_is_read_only_and_saves_nothing(self):
        self.transport.json_responses = [tables_page([table('tblManual', 'Manuals')]),
                                         fields_page([field('fldTitle', '标题')])]
        response = self.post({'url': base_link('tblManual'), 'table_id': 'tblManual'})
        self.assertEqual(response.status_code, 200, response.text)
        self.assertEqual(response.headers['cache-control'], 'no-store')
        self.assertTrue(response.json()['ok'])
        data = response.json()['data']
        self.assertEqual(data['source']['id'], link_key(BASE_TOKEN, 'tblManual'))
        self.assertEqual(data['url'], base_link('tblManual'))
        self.assertEqual([(call['method'], call['url']) for call in self.transport.calls],
                         [('GET', TABLES_URL), ('GET', TABLES_URL + '/tblManual/fields')])
        self.assert_nothing_saved()

    def test_wiki_with_many_tables_returns_choices_without_saving(self):
        self.transport.json_responses = [
            node_page(), tables_page([table('tblA', 'A'), table('tblB', 'B')]),
            node_page(), tables_page([table('tblA', 'A'), table('tblB', 'B')]),
            fields_page([field('fldA', 'A')])]
        chosen = self.post({'url': wiki_link()})
        selected = self.post({'url': wiki_link(), 'table_id': 'tblB'})
        self.assertEqual(chosen.status_code, 200, chosen.text)
        self.assertIsNone(chosen.json()['data']['source'])
        self.assertEqual([item['table_id'] for item in chosen.json()['data']['tables']], ['tblA', 'tblB'])
        self.assertEqual(selected.status_code, 200, selected.text)
        self.assertEqual(selected.json()['data']['source']['table_id'], 'tblB')
        self.assertEqual(self.transport.calls[-1]['url'], TABLES_URL + '/tblB/fields')
        self.assert_nothing_saved()

    def test_preview_requires_login_admin_and_knowledge_module(self):
        for session, profile, status in (
                (None, self.profile, 401),
                ({'user': {'open_id': 'ouGuest'}, 'is_guest': True}, self.profile, 403),
                (self.session, {**self.profile, 'role': 'building'}, 403),
                (self.session, {**self.profile, 'modules': ['assistant']}, 403),
                ({**self.session, 'source': 'admin_impersonation'}, self.profile, 403)):
            with self.subTest(session=session, profile=profile):
                self.session, self.profile = session, profile
                response = self.post({'url': base_link('tblManual')})
                self.assertEqual(response.status_code, status, response.text)
                self.assertFalse(response.json()['ok'])
        self.assertEqual(self.transport.calls, [])
        self.assert_nothing_saved()

    def test_preview_requires_same_origin_and_rechecks_current_session(self):
        for headers in ({}, {'origin': 'https://other.example'}, {'origin': 'null'},
                        {**self.origin, 'sec-fetch-site': 'cross-site'}):
            with self.subTest(headers=headers):
                response = self.post({'url': base_link('tblManual')}, headers=headers)
                self.assertEqual(response.status_code, 403)
                self.assertEqual(response.json()['error'], '不允许跨来源提交资料。')
        self.assertEqual(self.transport.calls, [])
        # The session is re-read after the preview; a revoked grant is not served.
        self.transport.json_responses = [tables_page([table('tblManual', 'Manuals')]),
                                         fields_page([field('fldTitle', '标题')])]
        original = self.transport.request_json

        def revoke_after_first_read(method, url, **kwargs):
            # The read already happened: drop the module before the recheck.
            self.profile['modules'] = []
            return original(method, url, **kwargs)

        self.transport.request_json = revoke_after_first_read
        response = self.post({'url': base_link('tblManual')})
        self.assertEqual(response.status_code, 403, response.text)
        self.assertNotIn('source', response.text)
        self.assertGreaterEqual(self.session_reads.call_count, 2)
        self.assert_nothing_saved()


if __name__ == '__main__':
    unittest.main()
