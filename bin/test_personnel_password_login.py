# -*- coding: utf-8 -*-
"""Offline verification for personnel-table password login.

Covers the current implementations of:
  * lan_bitable_template_portal.personnel_password_login
  * lan_bitable_template_portal.portal_auth.PortalAuthManager (personnel sessions)
  * lan_bitable_template_portal.portal_service.MaintenancePortalService._load_signature_people
     (password hash stripping, without network)
  * PortalAuthManager.get_session/public_status for normal Feishu sessions
     (no PersonnelPasswordLogin/DIRECTORY_NS lookup, admin/building privileges
     preserved)
  * AST-extracted clipflow_backend.main.py life_guide_page / workbench_lite_page
     unauth redirects (clipflow-login-method=password preference, next
     preservation, and forging the preference alone never authenticates)

All fake personnel rows carry a synthetic, unique real Feishu user field
(员工姓名) with an ``ou_fixture*`` open_id; all passwords and identity numbers
are synthetic. The session principal is the real open_id from the selected HR
row (员工姓名 user-field, falling back to the 飞书 open_id field only when the
user field is absent) - never a fabricated ``personnel_`` portal account.

Everything runs against a temp SQLite/data directory with the real
PortalAuthManager (get_data_file_path patched to temp) and a mocked Feishu
service. The main.py route tests use an AST-extracted copy of the real async
route bodies with the FastAPI decorators removed, so no production controller
is started. No real credentials, cloud requests/writes or production service are
ever touched, and the real 人员表 is never written.
"""
from __future__ import annotations

import ast
import asyncio
import concurrent.futures
import copy
import gc
import hashlib
import json
import os
import secrets
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlencode, urlsplit

import httpx
from fastapi import FastAPI, Request, Response
from httpx import ASGITransport

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal import personnel_password_login as ppl  # noqa: E402
from lan_bitable_template_portal.personnel_password_login import (  # noqa: E402
    PASSWORD_FIELD,
    PersonnelPasswordLogin,
    PasswordLoginError,
    hash_password,
    verify_password,
)
from lan_bitable_template_portal.portal_auth import (  # noqa: E402
    AUTH_SESSION_TTL_SECONDS,
    PortalAuthManager,
)
from lan_bitable_template_portal.portal_service import MaintenancePortalService  # noqa: E402


def _rec(row_id: str):
    return {"record_id": row_id, "fields": {}}


# Synthetic national-ID fixture. It is impossible (not a real person) and is the
# only identity datum this suite may use; its last six characters become the
# one-time bootstrap for a blank-password (needs_setup) account. The legacy
# all-123 defaults and any real identity data are never used here.
IDENTITY_NUMBER = "00000020000101234X"
# Last six characters of the synthetic national ID; the case-insensitive
# initial password used only for a blank-密码 (needs_setup) account.
BOOTSTRAP = IDENTITY_NUMBER[-6:]

# A same-shaped synthetic ID whose final check digit differs only in case; used
# to prove the bootstrap comparison is case insensitive without real data.
IDENTITY_NUMBER_LOWER_X = IDENTITY_NUMBER[:-1] + "x"
BOOTSTRAP_LOWER_X = IDENTITY_NUMBER_LOWER_X[-6:]


def _user(name: str, open_id: str):
    """Synthetic 员工姓名 user-field payload carrying a real-style Feishu open_id."""
    return [{"name": name, "open_id": open_id}]


class FakePortalService:
    """Feishu service stand-in used only by the real PersonnelPasswordLogin code."""

    def __init__(self):
        self._records = {}          # rid -> fields dict (password direction preserved)
        self._pages = []            # queue of directory pages
        self._request_payload_calls = 0
        self._request_json_calls = 0
        self._patch_calls = 0
        self._last_patch_fields = None
        self._last_patch_table_id = None
        self._last_patch_app_token = None
        self._last_request_json_table_ids = []
        self._last_search_urls = []
        self._last_search_field_names = None
        self._forged_record_id = None
        self._write_response_loss = False
        self._write_fail_no_apply = False

    # Bind the real portal normalizers so tests exercise the production
    # building/text/inactive normalization instead of a hand-copied stand-in.
    # Pure static helpers are bound verbatim; classmethod-based ones delegate
    # to the real class so their internal sibling-helper calls stay on the
    # production MaintenancePortalService.
    _mop_field_text = staticmethod(MaintenancePortalService._mop_field_text)
    _signature_person_inactive = staticmethod(
        MaintenancePortalService._signature_person_inactive
    )

    @staticmethod
    def _signature_user_info(value):
        return MaintenancePortalService._signature_user_info(value)

    @staticmethod
    def _building_codes_from_value(value):
        return MaintenancePortalService._building_codes_from_value(value)

    @staticmethod
    def _building_label_from_codes(codes):
        return MaintenancePortalService._building_label_from_codes(codes)

    def _auth_headers(self):
        return {"Authorization": "Bearer mock-token"}

    def _request_payload(self, method, url, *, context, headers, params, json_payload):
        self._request_payload_calls += 1
        self._last_search_urls.append(url)
        if not self._pages:
            raise AssertionError("Unexpected directory fetch: no pages queued")
        # The directory search must target the real 人员表 (never a legacy old
        # table), must NOT request 身份证号, and must read the COMPLETE table
        # (records/search on the table itself; no view filter).
        expected_table = (
            f"https://open.feishu.cn/open-apis/bitable/v1/apps/"
            f"{ppl.APP_TOKEN}/tables/{ppl.TABLE_ID}/records/search"
        )
        if url != expected_table:
            raise AssertionError(f"Directory search URL must use current 人员表: {url!r}")
        if "view_id" in params:
            raise AssertionError("Directory search must read the complete table (no view filter)")
        field_names = (json_payload or {}).get("field_names") or []
        if ppl.IDENTITY_FIELD in field_names:
            raise AssertionError("Directory field_names must not request 身份证号")
        self._last_search_field_names = list(field_names)
        return self._pages.pop(0)

    def _request_json(self, path, *, params=None, app_token=None, table_id=None, http_client=None):
        self._request_json_calls += 1
        # Single-row reads must target the real 人员表 (never a legacy table).
        if (app_token, table_id) != (ppl.APP_TOKEN, ppl.TABLE_ID):
            raise AssertionError(f"Single-row read must use current 人员表: app={app_token!r} table={table_id!r}")
        self._last_request_json_table_ids.append(table_id)
        rid = None
        if path.startswith("records/") and path.count("/") == 1:
            rid = path.split("/", 1)[1]
        if not rid:
            raise AssertionError(f"Unexpected request_json path: {path}")
        if self._forged_record_id is not None:
            return {"code": 0, "data": {"record": {"record_id": self._forged_record_id, "fields": {}}}}
        row = self._records.get(rid)
        if row is None:
            raise AssertionError(f"No mock record for {rid}: {self._records}")
        return {"code": 0, "data": {"record": {"record_id": rid, "fields": {"账号性质": "外部账号", **row}}}}

    def _patch_record_fields_exact(self, *, app_token, table_id, record_id, fields):
        self._patch_calls += 1
        # Password writes must target the real 人员表 and only the 密码 field.
        if (app_token, table_id) != (ppl.APP_TOKEN, ppl.TABLE_ID):
            raise AssertionError(f"Password write must use current 人员表: app={app_token!r} table={table_id!r}")
        self._last_patch_app_token = app_token
        self._last_patch_table_id = table_id
        self._last_patch_fields = dict(fields)
        apply = not self._write_fail_no_apply
        if apply:
            current = self._records.get(record_id) or {}
            self._records[record_id] = {**current, **fields}
        if self._write_response_loss or self._write_fail_no_apply:
            raise RuntimeError("simulated write failure/lost response")
        return {"code": 0, "data": {}}


def _page(rows, has_more=False, token=""):
    data = {"items": rows, "has_more": has_more}
    if token:
        data["page_token"] = token
    return {"code": 0, "data": data}


def _allow_real_writes():
    return patch(
        "lan_bitable_template_portal.portal_service.external_real_write_guard",
        return_value={
            "real_write_allowed": True,
            "mock_external": False,
            "require_confirm": False,
            "confirmed": True,
            "reason": "",
        },
    )


def _sqlite_write_sync_env():
    """Keep SQLite writes synchronous for this fixture only (no global mutation)."""
    return patch.dict(os.environ, {"CLIPFLOW_DISABLE_SQLITE_WRITE_WORKER": "1"})


def _shutdown_manager(mgr):
    """Fully stop a PortalAuthManager including its SQLite write worker.

    Errors are propagated so a failed worker shutdown surfaces as a test
    failure instead of being silently hidden.
    """
    store = getattr(mgr, "_state_store", None)
    if store is not None:
        store.shutdown_write_worker()
    mgr.shutdown()


class _TempDirTestCase(unittest.TestCase):
    """Shared isolated-temp-dir + synchronous-SQLite fixture for this module."""

    _tmp_prefix = "test_"

    def setUp(self):
        super().setUp()
        self.enterContext(_sqlite_write_sync_env())
        self._tmp = tempfile.TemporaryDirectory(
            prefix=self._tmp_prefix, ignore_cleanup_errors=True
        )
        self.root = Path(self._tmp.name)
        self.addCleanup(self._cleanup_temp_dir)

    def _cleanup_temp_dir(self):
        gc.collect()
        try:
            self._tmp.cleanup()
        except PermissionError:
            # Windows may momentarily lock the sqlite file; leftover temp dir is harmless.
            pass


class PersonnelLoginHarness:
    """Builds a real PortalAuthManager + real PersonnelPasswordLogin + fake Feishu."""

    def __init__(self, root: Path):
        self.root = Path(root)
        self.patcher = patch(
            "lan_bitable_template_portal.portal_auth.get_data_file_path",
            side_effect=lambda name: str(self.root / name),
        )
        self.patcher.start()
        self.auth = PortalAuthManager()
        self.service = FakePortalService()
        self.login = PersonnelPasswordLogin(
            lambda: self.service,
            self.auth._state_store,
            self.auth,
        )

    def close(self):
        # Stop the auth manager (and its SQLite write worker) fully; errors
        # propagate so worker shutdown problems are not silently hidden.
        _shutdown_manager(self.auth)
        self.patcher.stop()


class PersonnelPasswordLoginTests(_TempDirTestCase):
    _tmp_prefix = "personnel_login_"

    def setUp(self):
        super().setUp()
        self.harness = PersonnelLoginHarness(self.root)
        self.addCleanup(self._cleanup_harness)

    def _cleanup_harness(self):
        self.harness.close()

    def _open(self, rid, fields):
        self.harness.service._records[rid] = fields

    # ---- public directory -------------------------------------------------
    def test_vnet_cannot_password_login_or_reset(self):
        fields = {'姓名': '内部测试', '机楼/专业': 'A楼', '账号性质': 'VNET',
                  '员工姓名': _user('内部测试', 'ou_fixtureVnet'), '身份证号': IDENTITY_NUMBER}
        self._open('recVnet', fields)
        self.harness.service._pages = [_page([{'record_id': 'recVnet', 'fields': fields}])]
        with self.assertRaisesRegex(PasswordLoginError, '飞书登录'):
            self.harness.login.login({'person_id': 'recVnet', 'password': BOOTSTRAP}, 'fixture')
        with self.assertRaisesRegex(PasswordLoginError, '外部人员'):
            self.harness.login.reset({'person_id': 'recVnet', 'identity_number': IDENTITY_NUMBER, 'new_password': 'NotUsed123'}, 'fixture')
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_expired_directory_detects_new_external_and_departure(self):
        rows = [{'record_id': 'recExternal', 'fields': {'姓名': '外部测试', '账号性质': '外部账号',
                  '机楼/专业': 'C楼', '员工姓名': _user('外部测试', 'ou_fixtureExt')}}]
        self.harness.service._pages = [_page([]), _page(rows), _page([])]
        with patch.object(ppl.time, 'time', return_value=1000):
            self.assertEqual(self.harness.login.people()['items'], [])
        with patch.object(ppl.time, 'time', return_value=1301):
            self.assertEqual(self.harness.login.people()['items'][0]['account_nature'], '外部账号')
        with patch.object(ppl.time, 'time', return_value=1602):
            self.assertEqual(self.harness.login.people()['items'], [])

    def test_public_directory_is_paginated_cached_and_strips_password_fields(self):
        rows = [
            {"record_id": "recA", "fields": {"姓名": "张三", "机楼/专业": "A楼",
                                             PASSWORD_FIELD: BOOTSTRAP,
                                             "员工姓名": _user("张三", "ou_fixture1")}},
            {"record_id": "recB", "fields": {"姓名": "李四", "机楼/专业": "H楼",
                                             PASSWORD_FIELD: "pbkdf2_sha256$600000$fakesalt$fakehash",
                                             "员工姓名": _user("李四", "ou_fixture2")}},
        ]
        self.harness.service._pages = [_page([rows[0]], has_more=True, token="t1"), _page([rows[1]])]
        first = self.harness.login.people()
        self.assertEqual(self.harness.service._request_payload_calls, 2)
        # The directory search must never request the national-ID field.
        self.assertEqual(
            self.harness.service._last_search_field_names,
            ["姓名", "员工姓名", "员工工号", "机楼/专业", "离职/异动情况", "飞书 open_id", "账号性质", PASSWORD_FIELD],
        )
        self.assertNotIn(ppl.IDENTITY_FIELD, self.harness.service._last_search_field_names)
        self.assertEqual(len(first["items"]), 2)
        for item in first["items"]:
            self.assertEqual(
                set(item),
                {"id", "name", "employee_no", "building", "selectable", "disabled_reason", "needs_setup", "account_nature"},
            )
            for forbidden in ("password", PASSWORD_FIELD, "password_revision", "open_id", "openid",
                              "stored", "scopes", "login_ids", ppl.IDENTITY_FIELD, "identity"):
                self.assertNotIn(forbidden, item)
        self.assertEqual(first["items"][0]["id"], "recA")
        self.assertEqual(first["items"][1]["id"], "recB")
        self.assertTrue(first["items"][0]["selectable"])
        self.assertTrue(first["items"][1]["selectable"])
        # cached: a second call must NOT hit the (now empty) directory page queue.
        second = self.harness.login.people()
        self.assertEqual(self.harness.service._request_payload_calls, 2)
        self.assertEqual(second["items"], first["items"])

    def test_duplicate_record_id_across_pages_is_rejected(self):
        row = {"record_id": "recDup", "fields": {"姓名": "重复", "机楼/专业": "E楼"}}
        self.harness.service._pages = [_page([row], has_more=True, token="x"), _page([row])]
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.people()
        self.assertEqual(caught.exception.status, 503)
        self.assertIn("重复", str(caught.exception))

    def test_departed_excluded_and_blank_or_multifloor_pending_no_autogrant(self):
        rows = [
            {"record_id": "recGone", "fields": {"姓名": "离职", "机楼/专业": "A楼", "离职/异动情况": "已离职",
                                               "员工姓名": _user("离职", "ou_fixture1")}},
            {"record_id": "recBlank", "fields": {"姓名": "无楼", "机楼/专业": "",
                                                "员工姓名": _user("无楼", "ou_fixture2")}},
            {"record_id": "recMulti", "fields": {"姓名": "多楼", "机楼/专业": "A楼H楼",
                                                "员工姓名": _user("多楼", "ou_fixture3")}},
            {"record_id": "recOk", "fields": {"姓名": "正常", "机楼/专业": "C楼", PASSWORD_FIELD: BOOTSTRAP,
                                             "员工姓名": _user("正常", "ou_fixture4")}},
        ]
        self.harness.service._pages = [_page(rows)]
        result = self.harness.login.people()
        ids = [item["id"] for item in result["items"]]
        # Departed person is excluded entirely from the public directory.
        self.assertNotIn("recGone", ids)
        self.assertIn("recOk", ids)
        # blank / multi-floor people stay listed and remain selectable: they can
        # authenticate (pending session) but never get an auto-chosen floor.
        self.assertIn("recBlank", ids)
        self.assertIn("recMulti", ids)
        listed = {item["id"]: item for item in result["items"]}
        self.assertTrue(listed["recBlank"]["selectable"])
        self.assertTrue(listed["recMulti"]["selectable"])
        self.assertTrue(listed["recOk"]["selectable"])
        details = {}
        for row in rows:
            item = self.harness.login.person(row)
            details[row["record_id"]] = item
        self.assertTrue(details["recGone"]["inactive"])
        self.assertFalse(details["recGone"]["selectable"])
        self.assertEqual(details["recBlank"]["scopes"], [])
        self.assertEqual(details["recMulti"]["scopes"], ["A", "H"])
        self.assertTrue(details["recBlank"]["selectable"])
        self.assertTrue(details["recMulti"]["selectable"])
        self.assertEqual(details["recOk"]["scopes"], ["C"])
        self.assertTrue(details["recOk"]["selectable"])

    def test_blank_password_is_setup_and_legacy_plaintext_never_usable(self):
        # Blank密码 means the account needs initial setup: the bootstrap is the
        # last six characters of the real row's 身份证号 (never the legacy "123"
        # default, never a fixed six-digit value).
        rows = [
            {"record_id": "recNoPwd", "fields": {"姓名": "无密码", "机楼/专业": "C楼",
                                                 "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                                                 "员工姓名": _user("无密码", "ou_fixture1")}},
            {"record_id": "recLegacy", "fields": {"姓名": "旧默认", "机楼/专业": "C楼", PASSWORD_FIELD: "123",
                                                 "员工姓名": _user("旧默认", "ou_fixture2")}},
            {"record_id": "recSix", "fields": {"姓名": "六位", "机楼/专业": "C楼", PASSWORD_FIELD: "123456",
                                              "员工姓名": _user("六位", "ou_fixture3")}},
        ]
        self.harness.service._pages = [_page(rows)]
        result = self.harness.login.people()
        listed = {item["id"]: item for item in result["items"]}
        self.assertIn("recNoPwd", listed)
        self.assertIn("recLegacy", listed)
        self.assertIn("recSix", listed)
        # Blank is an unprovisioned setup account: needs_setup and selectable.
        self.assertTrue(listed["recNoPwd"]["needs_setup"])
        self.assertTrue(listed["recNoPwd"]["selectable"])
        # Non-empty legacy plaintext values are selectable but never a valid
        # credential (verify_password requires the pbkdf2 hash format).
        self.assertFalse(listed["recLegacy"]["needs_setup"])
        self.assertTrue(listed["recLegacy"]["selectable"])
        self.assertFalse(listed["recSix"]["needs_setup"])
        self.assertTrue(listed["recSix"]["selectable"])

        # Blank account: the synthetic-ID last-six bootstrap unlocks setup.
        self._open("recNoPwd", {"姓名": "无密码", "机楼/专业": "C楼",
                                "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                                "员工姓名": _user("无密码", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recNoPwd")])]
        with _allow_real_writes():
            data, session_id = self.harness.login.login(
                {"person_id": "recNoPwd", "password": BOOTSTRAP}, "127.0.0.1"
            )
        self.assertEqual(data, {"requires_password_change": True})
        self.assertIsNone(session_id)
        # The wrong/lazy six-digit plaintext is not accepted for the blank account.
        self.harness.service._pages = [_page([_rec("recNoPwd")])]
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.login(
                {"person_id": "recNoPwd", "password": "123456"}, "127.0.0.1"
            )
        self.assertEqual(caught.exception.status, 401)

        # Legacy plaintext stored passwords ("123" and "123456") are never
        # accepted even when typed verbatim.
        for rid, stored, oid in (("recLegacy", "123", "ou_fixture2"), ("recSix", "123456", "ou_fixture3")):
            self._open(rid, {"姓名": "旧账号", "机楼/专业": "C楼", PASSWORD_FIELD: stored,
                             "员工姓名": _user("旧账号", oid)})
            self.harness.service._pages = [_page([_rec(rid)])]
            with self.assertRaises(PasswordLoginError) as caught:
                self.harness.login.login(
                    {"person_id": rid, "password": stored}, "127.0.0.1"
                )
            self.assertEqual(caught.exception.status, 401, stored)

    # ---- first login ------------------------------------------------------
    def test_initial_bootstrap_returns_requires_password_change_and_no_session_or_write(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        with _allow_real_writes():
            data, session_id = self.harness.login.login(
                {"person_id": "recA", "password": BOOTSTRAP},
                "127.0.0.1",
            )
        self.assertEqual(data, {"requires_password_change": True})
        self.assertIsNone(session_id)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_bootstrap_x_is_compared_case_insensitively(self):
        # National-ID suffix may carry a trailing X/x; both spellings unlock the
        # bootstrap exactly once and no session is emitted without new_password.
        for stored_identity, typed_value in (
            (IDENTITY_NUMBER, "01234x"),
            (IDENTITY_NUMBER_LOWER_X, "01234X"),
        ):
            self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                                "身份证号": stored_identity, PASSWORD_FIELD: "",
                                "员工姓名": _user("张三", "ou_fixture1")})
            self.harness.service._pages = [_page([_rec("recA")])]
            with _allow_real_writes():
                data, session_id = self.harness.login.login(
                    {"person_id": "recA", "password": typed_value}, "127.0.0.1"
                )
            self.assertEqual(data, {"requires_password_change": True})
            self.assertIsNone(session_id)
            self.assertEqual(self.harness.service._patch_calls, 0)

    def test_wrong_password_rejected_on_initial_and_hash(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.login({"person_id": "recA", "password": "bad"}, "127.0.0.1")
        self.assertEqual(caught.exception.status, 401)

        stored = hash_password("GoodPass1")
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼", PASSWORD_FIELD: stored,
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        with self.assertRaises(PasswordLoginError) as caught2:
            self.harness.login.login({"person_id": "recA", "password": BOOTSTRAP}, "127.0.0.1")
        self.assertEqual(caught2.exception.status, 401)

    def test_new_password_min_length_and_whitespace_only_rejected(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        for new_pwd in ("short7", " " * 8, "\t" * 8, "123"):
            with self.assertRaises(PasswordLoginError) as caught:
                self.harness.login.login(
                    {"person_id": "recA", "password": BOOTSTRAP, "new_password": new_pwd},
                    "127.0.0.1",
                )
            self.assertEqual(caught.exception.status, 400, new_pwd)
        # the six-digit bootstrap itself remains unusable as a new password
        # (it equals the initial/last-six of this synthetic identity).
        with self.assertRaises(PasswordLoginError):
            self.harness.login.login(
                {"person_id": "recA", "password": BOOTSTRAP, "new_password": BOOTSTRAP},
                "127.0.0.1",
            )
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_first_valid_change_writes_only_pbkdf2_hash_then_reads_back_before_session(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        with _allow_real_writes():
            data, session_id = self.harness.login.login(
                {"person_id": "recA", "password": BOOTSTRAP, "new_password": "NewPass123"},
                "127.0.0.1",
            )
        self.assertEqual(self.harness.service._patch_calls, 1)
        self.assertEqual(set(self.harness.service._last_patch_fields), {PASSWORD_FIELD})
        # Writes target the current 人员表 (never a legacy table).
        self.assertEqual(self.harness.service._last_patch_table_id, ppl.TABLE_ID)
        self.assertEqual(self.harness.service._last_patch_app_token, ppl.APP_TOKEN)
        written = self.harness.service._last_patch_fields[PASSWORD_FIELD]
        self.assertTrue(written.startswith("pbkdf2_sha256$"))
        self.assertTrue(verify_password("NewPass123", written))
        # Session issued belongs to the personnel principal (the real open_id).
        self.assertTrue(session_id)
        self.assertIsNone(data.get("requires_password_change"))
        self.assertTrue(data.get("redirect_url"))
        # readback happened: record was re-fetched after the patch.
        self.assertEqual(self.harness.service._request_json_calls, 2)

    def test_old_bootstrap_stops_working_after_change_and_new_password_logs_in(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        with _allow_real_writes():
            _, first_session = self.harness.login.login(
                {"person_id": "recA", "password": BOOTSTRAP, "new_password": "NewPass123"},
                "127.0.0.1",
            )
        self.assertTrue(first_session)
        # old bootstrap no longer works
        self.harness.service._pages = [_page([_rec("recA")])]
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.login({"person_id": "recA", "password": BOOTSTRAP}, "127.0.0.1")
        self.assertEqual(caught.exception.status, 401)
        # saved/new password logs in
        self.harness.service._pages = [_page([_rec("recA")])]
        data, session_id = self.harness.login.login(
            {"person_id": "recA", "password": "NewPass123"}, "127.0.0.1"
        )
        self.assertTrue(session_id)
        self.assertTrue(data.get("redirect_url"))
        self.assertIn("scope=A", data["redirect_url"])

    def test_write_response_loss_is_verified_without_second_write(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        self.harness.service._write_response_loss = True
        with _allow_real_writes():
            data, session_id = self.harness.login.login(
                {"person_id": "recA", "password": BOOTSTRAP, "new_password": "NewPass123"},
                "127.0.0.1",
            )
        # response lost but the write was applied server-side; exactly one write.
        self.assertEqual(self.harness.service._patch_calls, 1)
        self.assertTrue(session_id)
        self.assertTrue(data.get("redirect_url"))

    def test_failed_write_issues_no_session(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        self.harness.service._write_fail_no_apply = True
        with _allow_real_writes():
            with self.assertRaises(PasswordLoginError) as caught:
                self.harness.login.login(
                    {"person_id": "recA", "password": BOOTSTRAP, "new_password": "NewPass123"},
                    "127.0.0.1",
                )
        self.assertEqual(caught.exception.status, 503)
        self.assertEqual(self.harness.service._patch_calls, 1)

    # ---- password reset ---------------------------------------------------
    def test_reset_without_new_password_verifies_identity_no_session_no_write(self):
        self._open("recR", {"姓名": "重置", "机楼/专业": "D楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("重置", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recR")])]
        with _allow_real_writes():
            data, session_id = self.harness.login.reset(
                {"person_id": "recR", "identity_number": IDENTITY_NUMBER}, "127.0.0.1"
            )
        self.assertEqual(data, {"requires_password_change": True})
        self.assertIsNone(session_id)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_reset_with_new_password_writes_only_password_hash_no_session(self):
        self._open("recR", {"姓名": "重置", "机楼/专业": "D楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("重置", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recR")])]
        with _allow_real_writes():
            data, session_id = self.harness.login.reset(
                {"person_id": "recR", "identity_number": IDENTITY_NUMBER,
                 "new_password": "StrongPass9"}, "127.0.0.1"
            )
        self.assertEqual(data, {"password_changed": True})
        self.assertIsNone(session_id)
        self.assertEqual(self.harness.service._patch_calls, 1)
        # Write targets ONLY the 密码 field of the current 人员表 (never a legacy table).
        self.assertEqual(set(self.harness.service._last_patch_fields), {PASSWORD_FIELD})
        self.assertEqual(self.harness.service._last_patch_table_id, ppl.TABLE_ID)
        self.assertEqual(self.harness.service._last_patch_app_token, ppl.APP_TOKEN)
        written = self.harness.service._last_patch_fields[PASSWORD_FIELD]
        self.assertTrue(written.startswith("pbkdf2_sha256$"))
        self.assertTrue(verify_password("StrongPass9", written))
        # readback re-used (patch write then single-row verify read).
        self.assertEqual(self.harness.service._request_json_calls, 2)

    def test_reset_rejects_wrong_identity(self):
        self._open("recR", {"姓名": "重置", "机楼/专业": "D楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("重置", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recR")])]
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.reset(
                {"person_id": "recR", "identity_number": "00000020000109999X"}, "127.0.0.1"
            )
        self.assertEqual(caught.exception.status, 401)

    def test_reset_rejects_blank_or_malformed_identity(self):
        self._open("recR", {"姓名": "重置", "机楼/专业": "D楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("重置", "ou_fixture1")})
        for bad_identity in ("", "   ", "12345", "00000020000101234", "abcdefghijklmnop"):
            self.harness.service._pages = [_page([_rec("recR")])]
            with self.assertRaises(PasswordLoginError) as caught:
                self.harness.login.reset(
                    {"person_id": "recR", "identity_number": bad_identity}, "127.0.0.1"
                )
            self.assertIn(caught.exception.status, (400, 401), bad_identity)

    def test_reset_rejects_new_password_equal_full_id_or_initial_suffix(self):
        self._open("recR", {"姓名": "重置", "机楼/专业": "D楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("重置", "ou_fixture1")})
        for bad_new in (IDENTITY_NUMBER, BOOTSTRAP):
            self.harness.service._pages = [_page([_rec("recR")])]
            with _allow_real_writes():
                with self.assertRaises(PasswordLoginError) as caught:
                    self.harness.login.reset(
                        {"person_id": "recR", "identity_number": IDENTITY_NUMBER,
                         "new_password": bad_new}, "127.0.0.1"
                    )
            self.assertEqual(caught.exception.status, 400)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_reset_inactive_or_disabled_denied(self):
        self._open("recR", {"姓名": "重置", "机楼/专业": "D楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("重置", "ou_fixture1"),
                            "离职/异动情况": "已离职"})
        self.harness.service._pages = [_page([_rec("recR")])]
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.reset(
                {"person_id": "recR", "identity_number": IDENTITY_NUMBER}, "127.0.0.1"
            )
        self.assertEqual(caught.exception.status, 403)

    def test_reset_rate_limits_same_person_and_ip(self):
        self._open("recR", {"姓名": "重置", "机楼/专业": "D楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("重置", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recR")])]
        with self.assertRaises(PasswordLoginError) as caught:
            for _ in range(12):
                try:
                    self.harness.login.reset(
                        {"person_id": "recR", "identity_number": IDENTITY_NUMBER}, "10.9.8.7"
                    )
                except PasswordLoginError as exc:
                    if exc.status == 429:
                        raise
        self.assertEqual(caught.exception.status, 429)

    # ---- session revocation ------------------------------------------------
    def test_save_password_invalidates_old_personnel_session(self):
        self._open("recRev", {"姓名": "撤销", "机楼/专业": "E楼",
                              "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                              "员工姓名": _user("撤销", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recRev")])]
        with _allow_real_writes():
            _, session_id = self.harness.login.login(
                {"person_id": "recRev", "password": BOOTSTRAP, "new_password": "FirstPass1"},
                "127.0.0.1",
            )
        self.assertTrue(session_id)
        self.assertIsNotNone(self.harness.auth.get_session(session_id))
        # A further password change must revoke the previously issued session on
        # the next read (save_password remembers a new revision and invalidates
        # cached personnel checks).
        self.harness.service._pages = [_page([_rec("recRev")])]
        with _allow_real_writes():
            self.harness.login.save_password("recRev", "SecondPass2")
        self.assertIsNone(self.harness.auth.get_session(session_id))

    # ---- invalid identities ----------------------------------------------
    def test_outside_view_record_rejected(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.login(
                {"person_id": "recFORGED", "password": BOOTSTRAP}, "127.0.0.1"
            )
        self.assertEqual(caught.exception.status, 403)

    def test_forged_record_rejected(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        self.harness.service._forged_record_id = "recEVIL"
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.login(
                {"person_id": "recA", "password": BOOTSTRAP, "new_password": "NewPass123"},
                "127.0.0.1",
            )
        self.assertEqual(caught.exception.status, 503)

    def test_login_payload_cannot_supply_open_id_role_or_scopes(self):
        # The API payload may only carry person_id/password/new_password/next.
        # Forging open_id/role/scopes must be rejected before any identity/session
        # handling and before any write.
        stored = hash_password("Pass1234")
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼",
                            PASSWORD_FIELD: stored,
                            "员工姓名": _user("张三", "ou_fixture1")})
        for extra_payload in (
            {"person_id": "recA", "password": "Pass1234", "open_id": "ou_attacker"},
            {"person_id": "recA", "password": "Pass1234", "role": "admin"},
            {"person_id": "recA", "password": "Pass1234", "scopes": ["ALL"]},
            {"person_id": "recA", "password": "Pass1234", "new_password": "NewPass123", "person": "x"},
        ):
            self.harness.service._pages = [_page([_rec("recA")])]
            with self.assertRaises(PasswordLoginError) as caught:
                self.harness.login.login(extra_payload, "127.0.0.1")
            self.assertIn(caught.exception.status, (400, 401), extra_payload)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_inactive_person_rejected_at_login(self):
        self._open("recA", {"姓名": "张三", "机楼/专业": "A楼", "离职/异动情况": "已离职",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recA")])]
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.login({"person_id": "recA", "password": BOOTSTRAP}, "127.0.0.1")
        self.assertEqual(caught.exception.status, 403)

    # ---- missing / duplicated OpenID --------------------------------------
    def test_missing_open_id_person_cannot_login(self):
        # No 员工姓名 user field and no 飞书 open_id -> cannot authenticate.
        self._open("recMiss", {"姓名": "无OpenID", "机楼/专业": "A楼",
                               "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: hash_password("Pass1234")})
        self.harness.service._pages = [_page([_rec("recMiss")])]
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.login(
                {"person_id": "recMiss", "password": "Pass1234"}, "127.0.0.1"
            )
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_fallback_open_id_text_field_used_when_user_field_missing(self):
        # When the 员工姓名 user field is absent, the row-level 飞书 open_id text
        # field is the identity fallback (both are synthetic in this fixture).
        stored = hash_password("Pass1234")
        rid = "recFallback"
        self._open(rid, {"姓名": "回退", "机楼/专业": "A楼",
                         PASSWORD_FIELD: stored,
                         "飞书 open_id": "ou_fixtureF1"})
        self.harness.service._pages = [_page([_rec(rid)])]
        data, session_id = self.harness.login.login(
            {"person_id": rid, "password": "Pass1234"}, "127.0.0.1"
        )
        self.assertTrue(session_id)
        session = self.harness.auth.get_session(session_id)
        self.assertEqual((session["user"] or {}).get("open_id"), "ou_fixtureF1")
        self.assertIn("scope=A", data["redirect_url"])

    def test_multifloor_and_nofloor_login_authenticate_but_no_autogrant(self):
        # Blank-floor and multi-floor people CAN authenticate (pending session),
        # but NEVER get an auto-chosen floor: the redirect stays "/" (permission
        # UI) and no permission entry is created for them.
        stored = hash_password("Pass1234")
        fields = {
            "recMulti": {"姓名": "多楼", "机楼/专业": "A楼H楼",
                         PASSWORD_FIELD: stored,
                         "员工姓名": _user("多楼", "ou_fixture3")},
            "recBlank": {"姓名": "无楼", "机楼/专业": "",
                         PASSWORD_FIELD: stored,
                         "员工姓名": _user("无楼", "ou_fixture4")},
        }
        # Build the real directory page once so BOTH people are known to login.
        rows = [{"record_id": rid, "fields": dict(f)} for rid, f in fields.items()]
        self.harness.service._pages = [_page(rows)]
        # Ensure people() populates the authoritative directory snapshot.
        self.harness.login.people()
        for rid, f in fields.items():
            self.harness.service._records[rid] = f
            self.harness.service._pages = [_page([_rec(rid)])]
            data, session_id = self.harness.login.login(
                {"person_id": rid, "password": "Pass1234"}, "127.0.0.1"
            )
            self.assertTrue(session_id, rid)
            self.assertEqual(data["redirect_url"], "/", rid)
            session = self.harness.auth.get_session(session_id)
            self.assertEqual(session["allowed_scopes"], [], rid)
            users = {u["open_id"]: u for u in self.harness.auth.get_permissions_payload()["users"]}
            self.assertNotIn(f["员工姓名"][0]["open_id"], users, rid)

    def test_duplicate_open_id_denied_before_write_including_attempted_remember_cache(self):
        # Two active people share the same open_id. Both are marked
        # non-selectable in the directory, and a login attempt for one of them
        # is refused BEFORE any password write. The failed login must not leave
        # a synthetic "rememberable" entry behind either (remember only runs
        # after identity verification passes).
        stored = hash_password("Pass1234")
        rows = [
            {"record_id": "recOne", "fields": {"姓名": "甲", "机楼/专业": "A楼",
                                               PASSWORD_FIELD: stored,
                                               "员工姓名": _user("甲", "ou_fixture1")}},
            {"record_id": "recTwo", "fields": {"姓名": "乙", "机楼/专业": "B楼",
                                               PASSWORD_FIELD: stored,
                                               "员工姓名": _user("乙", "ou_fixture1")}},
        ]
        self.harness.service._pages = [_page(rows)]
        result = self.harness.login.people()
        listed = {item["id"]: item for item in result["items"]}
        self.assertFalse(listed["recOne"]["selectable"])
        self.assertFalse(listed["recTwo"]["selectable"])
        self.assertIn("多人使用同一OpenID", listed["recOne"]["disabled_reason"])

        # Attempt a login for the duplicated row: even though a stale cache may
        # attempt to remember it, it must be denied before any write.
        self.harness.service._records["recOne"] = rows[0]["fields"]
        self.harness.service._pages = []  # directory stays cached; no re-fetch needed
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.login(
                {"person_id": "recOne", "password": "Pass1234", "new_password": "NewPass999"},
                "127.0.0.1",
            )
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.harness.service._patch_calls, 0)
        # The failed login must not have poisoned the cache with a selectable
        # single-owner entry for recOne.
        cached = self.harness.auth._state_store.get_document(
            ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY
        ) or {}
        people = cached.get("people") or {}
        self.assertIs(people["recOne"]["selectable"], False)
        self.assertIn("多人使用同一OpenID", people["recOne"]["disabled_reason"])
        self.assertIn("recTwo", people)

    # ---- rate limits ------------------------------------------------------
    def test_rate_limits_person_and_ip(self):
        self._open("recRate", {"姓名": "张三", "机楼/专业": "A楼",
                               "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                               "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recRate")])]
        with _allow_real_writes():
            with self.assertRaises(PasswordLoginError) as caught:
                for _ in range(12):
                    try:
                        self.harness.login.login(
                            {"person_id": "recRate", "password": BOOTSTRAP}, "10.0.0.1"
                        )
                    except PasswordLoginError as exc:
                        if exc.status == 429:
                            raise
            self.assertEqual(caught.exception.status, 429)

    def test_same_person_concurrent_first_login_has_single_winner(self):
        self._open("recCon", {"姓名": "并发", "机楼/专业": "A楼",
                              "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                              "员工姓名": _user("并发", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recCon")])]
        outcomes = []

        def attempt():
            try:
                with _allow_real_writes():
                    data, session_id = self.harness.login.login(
                        {"person_id": "recCon", "password": BOOTSTRAP, "new_password": "SharedNew1"},
                        "10.0.0.2",
                    )
                outcomes.append(("ok", bool(session_id)))
            except PasswordLoginError as exc:
                outcomes.append(("error", exc.status))

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda _: attempt(), range(2)))
        ok_count = sum(1 for kind, _ in outcomes if kind == "ok")
        self.assertEqual(ok_count, 1, outcomes)
        self.assertEqual(sum(1 for kind, status in outcomes if kind == "error"), 1)

    # ---- private cache ----------------------------------------------------
    def test_private_cache_never_contains_raw_password_or_raw_hash(self):
        stored = hash_password("SecretPass1")
        row_fields = {"姓名": "王五", "机楼/专业": "H楼", PASSWORD_FIELD: stored,
                      "员工姓名": _user("王五", "ou_fixtureP")}
        self._open("recP", row_fields)
        self.harness.service._pages = [_page([{"record_id": "recP", "fields": dict(row_fields)}])]
        trimmed = self.harness.login.people()
        cached = self.harness.auth._state_store.get_document(
            ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY
        ) or {}
        people = cached.get("people") or {}
        self.assertIn("recP", people)
        for key in (PASSWORD_FIELD, "password", "stored", "open_id", "openid"):
            self.assertNotIn(key, people["recP"])
        serialized = json.dumps(people["recP"])
        self.assertNotIn(stored, serialized)
        self.assertNotIn("SecretPass1", serialized)
        # password_revision is a digest, not the hash itself.
        self.assertEqual(
            people["recP"]["password_revision"],
            hashlib.sha256(stored.encode("utf-8")).hexdigest(),
        )
        self.assertEqual(len(trimmed["items"]), 1)

    def test_bootstrap_metadata_contains_neither_suffix_nor_digest(self):
        # A blank-密码 (setup) account is never provisioned into the cached
        # metadata: password_revision stays empty and neither the synthetic
        # identity, its trailing bootstrap suffix, nor any digest of them
        # appears in the stored directory document.
        rec_fields = {"姓名": "初始", "机楼/专业": "B楼",
                      "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                      "员工姓名": _user("初始", "ou_fixtureB")}
        self._open("recBoot", rec_fields)
        self.harness.service._pages = [_page([{"record_id": "recBoot", "fields": dict(rec_fields)}])]
        self.harness.login.people()
        cached = self.harness.auth._state_store.get_document(
            ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY
        ) or {}
        people = cached.get("people") or {}
        self.assertIn("recBoot", people)
        self.assertEqual(people["recBoot"]["needs_setup"], True)
        self.assertEqual(people["recBoot"]["password_revision"], "")
        serialized = json.dumps(people["recBoot"])
        for secret in (IDENTITY_NUMBER, BOOTSTRAP,
                       hashlib.sha256(IDENTITY_NUMBER.encode("utf-8")).hexdigest(),
                       hashlib.sha256(BOOTSTRAP.encode("utf-8")).hexdigest()):
            self.assertNotIn(secret, serialized)

    def test_external_real_write_guard_blocks_password_write(self):
        self._open("recG", {"姓名": "张三", "机楼/专业": "A楼",
                            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                            "员工姓名": _user("张三", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec("recG")])]
        blocked_guard = patch(
            "lan_bitable_template_portal.portal_service.external_real_write_guard",
            return_value={
                "real_write_allowed": False,
                "mock_external": True,
                "require_confirm": True,
                "confirmed": False,
                "reason": "blocked for test",
            },
        )
        with blocked_guard:
            with self.assertRaises(PasswordLoginError) as caught:
                self.harness.login.login(
                    {"person_id": "recG", "password": BOOTSTRAP, "new_password": "NewPass123"},
                    "127.0.0.1",
                )
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.harness.service._patch_calls, 0)

    # ---- authenticated change (原密码 + 两次新密码) ---------------------------
    def _login_personnel_session(self, rid, pwd):
        """Log in through the real PersonnelPasswordLogin and return (session_id, session)."""
        self.harness.service._pages = [_page([_rec(rid)])]
        with _allow_real_writes():
            _, session_id = self.harness.login.login(
                {"person_id": rid, "password": pwd}, "127.0.0.1"
            )
        session = self.harness.auth.get_session(session_id)
        self.assertIsNotNone(session)
        return session_id, session

    def _stored_person(self, rid, pwd):
        return {"姓名": "改密", "机楼/专业": "A楼", "身份证号": IDENTITY_NUMBER,
                PASSWORD_FIELD: hash_password(pwd),
                "员工姓名": _user("改密", "ou_fixtureA")}

    def test_change_success_only_password_field_and_revokes_old_session(self):
        rid = "recChg"
        self._open(rid, self._stored_person(rid, "OldPass1"))
        session_id, session = self._login_personnel_session(rid, "OldPass1")
        self.assertEqual((session["user"] or {}).get("open_id"), "ou_fixtureA")
        self.assertNotIn(ppl.PRINCIPAL_PREFIX, (session["user"] or {}).get("open_id", ""))
        self.assertIsNotNone(self.harness.auth.get_session(session_id))
        with _allow_real_writes():
            data, new_session_id = self.harness.login.change(
                {"current_password": "OldPass1", "new_password": "NewPass2"},
                "127.0.0.1", session,
            )
        self.assertEqual(data, {"password_changed": True})
        self.assertIsNone(new_session_id)
        # ONLY the 密码 field is written on the live 人员表.
        self.assertEqual(set(self.harness.service._last_patch_fields), {PASSWORD_FIELD})
        self.assertEqual(self.harness.service._last_patch_table_id, ppl.TABLE_ID)
        self.assertEqual(self.harness.service._last_patch_app_token, ppl.APP_TOKEN)
        written = self.harness.service._last_patch_fields[PASSWORD_FIELD]
        self.assertTrue(written.startswith("pbkdf2_sha256$"))
        self.assertTrue(verify_password("NewPass2", written))
        # The old personnel session is invalidated immediately after success.
        self.assertIsNone(self.harness.auth.get_session(session_id))
        # The saved password is now the live credential.
        _, new_session = self.harness.login.login(
            {"person_id": rid, "password": "NewPass2"}, "127.0.0.1"
        )
        self.assertTrue(new_session)

    def test_change_wrong_current_password_401(self):
        rid = "recWrong"
        self._open(rid, self._stored_person(rid, "OldPass1"))
        _, session = self._login_personnel_session(rid, "OldPass1")
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.change(
                {"current_password": "WrongPass", "new_password": "NewPass2"},
                "127.0.0.1", session,
            )
        self.assertEqual(caught.exception.status, 401)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_change_missing_or_non_personnel_session_rejected(self):
        rid = "recNS"
        self._open(rid, self._stored_person(rid, "OldPass1"))
        payload = {"current_password": "OldPass1", "new_password": "NewPass2"}
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.change(payload, "127.0.0.1", None)
        self.assertEqual(caught.exception.status, 403)
        # A normal Feishu session is not allowed on this password-only path.
        feishu_session = {"user": {"open_id": "ou_fixtureF"}, "role": "building", "allowed_scopes": ["A"]}
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.change(payload, "127.0.0.1", feishu_session)
        self.assertEqual(caught.exception.status, 403)
        # Guest sessions are likewise rejected.
        guest_session = {"user": {"open_id": "guest_tmp"}, "role": "guest", "allowed_scopes": []}
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.change(payload, "127.0.0.1", guest_session)
        self.assertEqual(caught.exception.status, 403)

    def test_change_payload_whitelist_rejects_spoof_person_id(self):
        rid = "recSpoof"
        self._open(rid, self._stored_person(rid, "OldPass1"))
        _, session = self._login_personnel_session(rid, "OldPass1")
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.change(
                {"person_id": "recEVIL", "current_password": "OldPass1", "new_password": "NewPass2"},
                "127.0.0.1", session,
            )
        self.assertEqual(caught.exception.status, 400)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_change_rejects_same_current_and_identity_values(self):
        rid = "recSame"
        self._open(rid, self._stored_person(rid, "OldPass1"))
        _, session = self._login_personnel_session(rid, "OldPass1")
        for new_pwd in ("OldPass1", IDENTITY_NUMBER, BOOTSTRAP):
            with self.assertRaises(PasswordLoginError) as caught:
                self.harness.login.change(
                    {"current_password": "OldPass1", "new_password": new_pwd},
                    "127.0.0.1", session,
                )
            self.assertEqual(caught.exception.status, 400, new_pwd)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_change_rejects_blank_or_too_short_new_password(self):
        rid = "recShort"
        self._open(rid, self._stored_person(rid, "OldPass1"))
        _, session = self._login_personnel_session(rid, "OldPass1")
        for new_pwd in ("", "   ", "short7", "123"):
            with self.assertRaises(PasswordLoginError) as caught:
                self.harness.login.change(
                    {"current_password": "OldPass1", "new_password": new_pwd},
                    "127.0.0.1", session,
                )
            self.assertEqual(caught.exception.status, 400, repr(new_pwd))
        self.assertEqual(self.harness.service._patch_calls, 0)

    def _seed_change_meta(self, rid, *, selectable=True, disabled=False, open_id="ou_fixtureA"):
        person = {
            "id": rid, "name": "改密", "employee_no": "E1", "building": "A楼",
            "scopes": ["A"], "selectable": selectable, "inactive": not selectable,
            "disabled_reason": "" if selectable else "已停用",
            "needs_setup": False, "password_revision": "rev-1", "account_nature": "外部账号",
            "login_ids": [open_id],
        }
        snapshot = {"people": {rid: person}, "loaded_at": time.time(), "schema": 2}
        self.harness.auth._state_store.put_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY, snapshot)
        self.harness.login.snapshot = snapshot
        if disabled:
            self.harness.auth.upsert_permission_user(
                open_id=open_id, name="改密", role="building",
                scopes=["A"], enabled=False, updated_by="test",
            )

    def test_change_out_of_view_removed_inactive_or_disabled_fail(self):
        # out-of-view / removed: no directory entry for this person.
        self.harness.auth._state_store.put_document(
            ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY, {"people": {}, "loaded_at": time.time(), "schema": 2}
        )
        self.harness.login.snapshot = {"people": {}, "loaded_at": time.time(), "schema": 2}
        session = {"source": "personnel_password", "user": {"personnel_record_id": "recNope"}}
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.change(
                {"current_password": "x", "new_password": "NewPass2"}, "127.0.0.1", session
            )
        self.assertEqual(caught.exception.status, 403)

        # inactive account.
        rid = "recInAct"
        self._seed_change_meta(rid, selectable=False, open_id="ou_fixtureI")
        self.harness.service._records[rid] = {
            "姓名": "改密", "机楼/专业": "A楼", "离职/异动情况": "已离职",
            "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: hash_password("OldPass1"),
            "员工姓名": _user("改密", "ou_fixtureI"),
        }
        session2 = {"source": "personnel_password", "user": {"personnel_record_id": rid, "open_id": "ou_fixtureI"}}
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.change(
                {"current_password": "OldPass1", "new_password": "NewPass2"}, "127.0.0.1", session2
            )
        self.assertEqual(caught.exception.status, 403)

        # explicitly disabled account.
        rid2 = "recDisab"
        self._seed_change_meta(rid2, selectable=True, disabled=True, open_id="ou_fixtureD")
        self.harness.service._records[rid2] = {
            "姓名": "改密", "机楼/专业": "A楼", PASSWORD_FIELD: hash_password("OldPass1"),
            "员工姓名": _user("改密", "ou_fixtureD"),
        }
        session3 = {"source": "personnel_password", "user": {"personnel_record_id": rid2, "open_id": "ou_fixtureD"}}
        with self.assertRaises(PasswordLoginError) as caught:
            self.harness.login.change(
                {"current_password": "OldPass1", "new_password": "NewPass2"}, "127.0.0.1", session3
            )
        self.assertEqual(caught.exception.status, 403)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_reset_final_submit_rechecks_identity_after_earlier_verification(self):
        rid = "recRV"
        self._open(rid, {"姓名": "重置", "机楼/专业": "D楼", "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                         "员工姓名": _user("重置", "ou_fixture1")})
        self.harness.service._pages = [_page([_rec(rid)])]
        data, session_id = self.harness.login.reset(
            {"person_id": rid, "identity_number": IDENTITY_NUMBER}, "127.0.0.1"
        )
        self.assertEqual(data, {"requires_password_change": True})
        self.assertIsNone(session_id)
        # The earlier verification grants no bypass: the final submit rechecks
        # the identity and rejects a wrong value before any write.
        self.harness.service._pages = [_page([_rec(rid)])]
        with _allow_real_writes():
            with self.assertRaises(PasswordLoginError) as caught:
                self.harness.login.reset(
                    {"person_id": rid, "identity_number": "00000020000109999X",
                     "new_password": "StrongPass9"}, "127.0.0.1"
                )
        self.assertEqual(caught.exception.status, 401)
        self.assertEqual(self.harness.service._patch_calls, 0)

    def test_change_and_public_flow_identity_and_suffix_absent_from_surfaces(self):
        rid = "recPriv"
        stored = hash_password("OldPass1")
        row_fields = {"姓名": "私密", "机楼/专业": "A楼", "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: stored,
                      "员工姓名": _user("私密", "ou_fixture1")}
        self._open(rid, row_fields)
        self.harness.service._pages = [_page([{"record_id": rid, "fields": dict(row_fields)}])]
        self.harness.login.people()
        # public metadata
        items = self.harness.login.people()["items"]
        serialized_items = json.dumps(items, ensure_ascii=False)
        self.assertNotIn(IDENTITY_NUMBER, serialized_items)
        self.assertNotIn(BOOTSTRAP, serialized_items)
        # cache
        cached = self.harness.auth._state_store.get_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY) or {}
        serialized_cache = json.dumps(cached, ensure_ascii=False)
        self.assertNotIn(IDENTITY_NUMBER, serialized_cache)
        self.assertNotIn(BOOTSTRAP, serialized_cache)
        # logs / search field list must never request 身份证号
        self.assertNotIn(ppl.IDENTITY_FIELD, ppl.FIELDS)
        self.assertNotIn(ppl.IDENTITY_FIELD, self.harness.service._last_search_field_names or [])
        # read-body requests list (directory + single-row reads) carries no identity/suffix
        for blob in (self.harness.service._last_search_urls, self.harness.service._last_request_json_table_ids):
            serialized = json.dumps(blob, ensure_ascii=False)
            self.assertNotIn(IDENTITY_NUMBER, serialized)
            self.assertNotIn(BOOTSTRAP, serialized)


class PersonnelAuthSessionTests(_TempDirTestCase):
    """Real PortalAuthManager personnel-session semantics with temp stores."""

    _tmp_prefix = "personnel_auth_"

    def _build_manager(self, root=None):
        mgr_root = Path(root or self.root)
        with patch(
            "lan_bitable_template_portal.portal_auth.get_data_file_path",
            side_effect=lambda name: str(mgr_root / name),
        ):
            mgr = PortalAuthManager()
        # Fully stop the manager (executor + SQLite write worker) on teardown.
        self.addCleanup(_shutdown_manager, mgr)
        return mgr

    def _person(self, rid="recS", scopes=("A",), open_id="ou_fixture1"):
        return {
            "id": rid,
            "name": "张三",
            "employee_no": "E001",
            "building": "A楼",
            "scopes": list(scopes),
            "selectable": True,
            "inactive": False,
            "disabled_reason": "",
            "needs_setup": False,
            "password_revision": "rev-1",
            "login_ids": [open_id],
        }

    def _seed_directory(self, mgr, person):
        store = mgr._state_store
        payload = {"people": {person["id"]: dict(person)}, "loaded_at": time.time(), "schema": 2}
        store.put_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY, payload)

    def test_creates_building_principal_real_open_id_exact_floor_absolute_ttl_and_restart(self):
        person = self._person(scopes=("E",), open_id="ou_fixtureE")
        mgr1 = self._build_manager()
        self._seed_directory(mgr1, person)
        session_id = mgr1.create_personnel_session(person)
        self.assertTrue(session_id)
        self.assertFalse(session_id.startswith(ppl.PRINCIPAL_PREFIX))  # session id itself is opaque token
        identity = person["login_ids"][0]

        fresh = self._build_manager()
        session = fresh.get_session(session_id)
        self.assertIsNotNone(session)
        self.assertEqual(session["user"]["open_id"], identity)
        self.assertEqual(session["user"]["personnel_record_id"], person["id"])
        self.assertEqual(session["role"], "building")
        self.assertEqual(session["allowed_scopes"], ["E"])
        self.assertEqual(session["source"], "personnel_password")
        self.assertEqual(session["expires_at"], session["created_at_ts"] + AUTH_SESSION_TTL_SECONDS)

        # absolute TTL: forcing an old created_at should expire it on restart.
        manager2 = self._build_manager()
        store = manager2._state_store
        payload = store.get_auth_session(fresh._secret_hash(session_id)) or {}
        payload["created_at_ts"] = time.time() - AUTH_SESSION_TTL_SECONDS - 60
        store.put_auth_session(fresh._secret_hash(session_id), payload)
        restarted = self._build_manager()
        self.assertIsNone(restarted.get_session(session_id))

    def test_disabled_identity_and_revised_hash_and_removed_record_revoke(self):
        # --- disabled identity ---
        person = self._person("recR", scopes=("B",), open_id="ou_fixtureB")
        mgr = self._build_manager()
        self._seed_directory(mgr, person)
        session_id = mgr.create_personnel_session(person)
        identity = person["login_ids"][0]
        mgr.upsert_permission_user(open_id=identity, name="张三", scopes=["B"], enabled=False, updated_by="test")
        self.assertIsNone(mgr.get_session(session_id))

        # --- revised hash (distinct identity) ---
        person2 = self._person("recH", scopes=("H",), open_id="ou_fixtureH")
        self._seed_directory(mgr, person2)
        session_id2 = self._build_manager().create_personnel_session(person2)
        restarted = self._build_manager()
        snapshot = {**(restarted._state_store.get_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY) or {})}
        snapshot.setdefault("people", {})["recH"] = {**person2, "password_revision": "rev-changed"}
        restarted._state_store.put_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY, snapshot)
        self.assertIsNone(restarted.get_session(session_id2))

        # --- removed record (distinct identity) ---
        person3 = self._person("recM", scopes=("C",), open_id="ou_fixtureC")
        self._seed_directory(mgr, person3)
        session_id3 = self._build_manager().create_personnel_session(person3)
        restarted2 = self._build_manager()
        snapshot2 = {**(restarted2._state_store.get_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY) or {})}
        snapshot2["people"] = {}
        restarted2._state_store.put_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY, snapshot2)
        self.assertIsNone(restarted2.get_session(session_id3))

    def test_admin_role_preserved_not_inferred_from_name(self):
        person = self._person("recX", scopes=("C",), open_id="ou_fixtureX")
        mgr = self._build_manager()
        self._seed_directory(mgr, person)
        session_id = mgr.create_personnel_session(person)
        identity = person["login_ids"][0]
        mgr.upsert_permission_user(open_id=identity, name="管理员", role="admin", scopes=["ALL"],
                                   enabled=True, updated_by="test")
        fresh = self._build_manager()
        session = fresh.get_session(session_id)
        self.assertIsNotNone(session)
        # Existing admin permission on the real open_id is preserved; it is not
        # downgraded to building simply because the directory name is 管理员 or
        # because the HR row only lists a building.
        self.assertEqual(session["role"], "admin")
        self.assertIn("ALL", session["allowed_scopes"])
        self.assertTrue(fresh.is_admin(session))

    def test_session_survives_manager_restart(self):
        person = self._person(open_id="ou_fixture1")
        mgr = self._build_manager()
        self._seed_directory(mgr, person)
        session_id = mgr.create_personnel_session(person)
        restarted = self._build_manager()
        session = restarted.get_session(session_id)
        self.assertIsNotNone(session)
        self.assertEqual(session["user"]["open_id"], person["login_ids"][0])

    def test_create_personnel_session_refuses_needs_setup(self):
        # A bootstrap account has needs_setup=True and empty password_revision;
        # it must never be issued a session even if some caller mislabels it.
        bootstrap_person = {
            **self._person("recBoot", scopes=("A",), open_id="ou_fixtureB"),
            "needs_setup": True,
            "password_revision": "",
            "selectable": True,
        }
        mgr = self._build_manager()
        with self.assertRaises(ppl.PortalError) as caught:
            mgr.create_personnel_session(bootstrap_person)
        self.assertIn("无法登录", str(caught.exception))

    # New requirement: unknown open_id auto-floor only persisted permission.
    def test_unknown_ou_auto_floor_only_persisted_permission(self):
        person = self._person("recU", scopes=("E",), open_id="ou_fixtureU")
        mgr = self._build_manager()
        self._seed_directory(mgr, person)
        session_id = mgr.create_personnel_session(person)
        # Auto-grant persists exactly one unambiguous building for a brand-new
        # (unknown) real open_id, without inventing an admin role.
        users = {u["open_id"]: u for u in mgr.get_permissions_payload()["users"]}
        perm = users.get("ou_fixtureU")
        self.assertIsNotNone(perm)
        self.assertEqual(perm["role"], "building")
        self.assertEqual(perm["scopes"], ["E"])
        session = mgr.get_session(session_id)
        self.assertEqual(session["allowed_scopes"], ["E"])
        self.assertEqual(session["user"]["open_id"], "ou_fixtureU")

    # New requirement: existing other floor preserved.
    def test_existing_other_floor_preserved_on_auto_grant(self):
        person = self._person("recM", scopes=("E",), open_id="ou_fixtureM")
        mgr = self._build_manager()
        mgr.upsert_permission_user(open_id="ou_fixtureM", name="李四", role="building",
                                   scopes=["B"], enabled=True, updated_by="test")
        self._seed_directory(mgr, person)
        session_id = mgr.create_personnel_session(person)
        users = {u["open_id"]: u for u in mgr.get_permissions_payload()["users"]}
        perm = users.get("ou_fixtureM")
        self.assertEqual(set(perm["scopes"]), {"B", "E"})
        self.assertEqual(perm["role"], "building")
        session = mgr.get_session(session_id)
        self.assertEqual(set(session["allowed_scopes"]), {"B", "E"})

    # New requirement: explicitly disabled permission user is not reactivated.
    def test_explicit_disabled_not_reactivated(self):
        person = self._person("recD", scopes=("A",), open_id="ou_fixtureD")
        mgr = self._build_manager()
        mgr.upsert_permission_user(open_id="ou_fixtureD", name="停用", role="building",
                                   scopes=["A"], enabled=False, updated_by="test")
        self._seed_directory(mgr, person)
        with self.assertRaises(ppl.PortalError) as caught:
            mgr.create_personnel_session(person)
        self.assertIn("无法登录", str(caught.exception))
        # The disabled record remains disabled; the auto-grant must not re-enable it.
        users = {u["open_id"]: u for u in mgr.get_permissions_payload()["users"]}
        self.assertIs(users.get("ou_fixtureD")["enabled"], False)

    # New requirement: ambiguous (multi-floor) grants no auto-floor.
    def test_ambiguous_floor_no_autogrant_pending_session(self):
        person = self._person("recA", scopes=("A", "H"), open_id="ou_fixtureA")
        mgr = self._build_manager()
        self._seed_directory(mgr, person)
        session_id = mgr.create_personnel_session(person)
        # No permission is auto-created for an ambiguous multi-floor row.
        users = {u["open_id"]: u for u in mgr.get_permissions_payload()["users"]}
        self.assertNotIn("ou_fixtureA", users)
        session = mgr.get_session(session_id)
        self.assertEqual(session["allowed_scopes"], [])

    # New requirement: no fake synthetic personnel_ portal accounts are created.
    def test_no_fake_personnel_account_created(self):
        person = self._person("recN", scopes=("A",), open_id="ou_fixtureN")
        mgr = self._build_manager()
        self._seed_directory(mgr, person)
        session_id = mgr.create_personnel_session(person)
        session = mgr.get_session(session_id)
        # The session principal is the real open_id, not a fabricated personnel_ id.
        self.assertEqual(session["user"]["open_id"], "ou_fixtureN")
        self.assertFalse(session["user"]["open_id"].startswith(ppl.PRINCIPAL_PREFIX))
        users = {u["open_id"]: u for u in mgr.get_permissions_payload()["users"]}
        fake = ppl.PRINCIPAL_PREFIX + person["id"]
        self.assertNotIn(fake, session["user"]["open_id"])
        self.assertNotIn(fake, users)

    # New requirement: password revision + OpenID remap revocation.
    def test_cookie_password_revision_and_openid_remap_revocation(self):
        def _force_next_check(mgr, session_id_):
            # get_session re-validates personnel sessions at most every 30s;
            # force the very next look-up on the same manager to re-check the
            # directory by resetting its runtime personnel-check marker.
            in_memory = mgr._sessions.get(session_id_)
            if in_memory is not None:
                in_memory["_last_personnel_check"] = 0

        person = self._person("recS", scopes=("A",), open_id="ou_fixture1")
        mgr = self._build_manager()
        self._seed_directory(mgr, person)
        session_id = mgr.create_personnel_session(person)
        self.assertIsNotNone(mgr.get_session(session_id))

        # Password revision change revokes the old session.
        snapshot = {**(mgr._state_store.get_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY) or {})}
        snapshot.setdefault("people", {})["recS"] = {**person, "password_revision": "rev-changed"}
        mgr._state_store.put_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY, snapshot)
        _force_next_check(mgr, session_id)
        self.assertIsNone(mgr.get_session(session_id))

        # Recreate with a fresh (now-valid) revision, then remap the open_id: an
        # old session must be revoked when the directory row's login_ids no
        # longer matches the session's principal.
        snapshot2 = {**(mgr._state_store.get_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY) or {})}
        snapshot2.setdefault("people", {})["recS"] = dict(person)
        mgr._state_store.put_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY, snapshot2)
        session_id2 = mgr.create_personnel_session(person)
        self.assertIsNotNone(mgr.get_session(session_id2))
        remapped = {**(mgr._state_store.get_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY) or {})}
        remapped.setdefault("people", {})["recS"] = {
            **person, "password_revision": "rev2", "login_ids": ["ou_fixture99"],
        }
        mgr._state_store.put_document(ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY, remapped)
        _force_next_check(mgr, session_id2)
        self.assertIsNone(mgr.get_session(session_id2))

    # New requirement: permission application request owner equals selected real open_id.
    def test_permission_application_request_owner_is_selected_real_openid(self):
        person = self._person("recP", scopes=("A",), open_id="ou_fixtureP")
        mgr = self._build_manager()
        self._seed_directory(mgr, person)
        session_id = mgr.create_personnel_session(person)
        session = mgr.get_session(session_id)
        real_open_id = session["user"]["open_id"]
        self.assertEqual(real_open_id, "ou_fixtureP")
        request = mgr.create_permission_request(
            open_id=real_open_id, name="张三", scopes=["H"], reason="需要查看H楼"
        )
        self.assertEqual(request["open_id"], real_open_id)
        current = mgr.get_current_permission_request(real_open_id)
        self.assertEqual(current["open_id"], real_open_id)


class PortalFeishuSessionMappingTests(_TempDirTestCase):
    """Real PortalAuthManager.get_session/public_status for normal Feishu sessions.

    A valid pre-existing (persisted) Feishu session must be restored without any
    PersonnelPasswordLogin or DIRECTORY_NS lookup and without any password
    requirement; existing admin/building privileges must remain unchanged.
    """

    _tmp_prefix = "portal_feishu_session_"

    def _build_manager(self, root=None):
        mgr_root = Path(root or self.root)
        with patch(
            "lan_bitable_template_portal.portal_auth.get_data_file_path",
            side_effect=lambda name: str(mgr_root / name),
        ):
            mgr = PortalAuthManager()
        self.addCleanup(_shutdown_manager, mgr)
        return mgr

    @staticmethod
    def _feishu_session(open_id, role, scopes):
        now = time.time()
        return {
            "user": {"open_id": open_id, "name": "飞书用户", "union_id": "u_fixture_union"},
            "role": role,
            "allowed_scopes": list(scopes),
            "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "created_at_ts": now,
            "expires_at": now + AUTH_SESSION_TTL_SECONDS,
        }

    def _persist_feishu_session(self, mgr, session):
        # Mirror complete_login: the session lives only in the persistent store
        # (and the in-memory map of the original manager is never warmed), so
        # get_session exercises the real persisted-restore path.
        session_id = secrets.token_urlsafe(32)
        mgr._state_store.put_auth_session(mgr._secret_hash(session_id), session)
        return session_id

    def _assert_no_directory_lookup(self, mgr, callable_fn):
        """Fail the test if the personnel password table (DIRECTORY_NS) is read."""
        seen = {"directories": []}
        original = mgr._state_store.get_document

        def guarded(ns, key, *args, **kwargs):
            seen["directories"].append(ns)
            if ns == ppl.DIRECTORY_NS:
                raise AssertionError(
                    f"DIRECTORY_NS unexpectedly accessed during Feishu session path: {ns}/{key}"
                )
            return original(ns, key, *args, **kwargs)

        with patch.object(mgr._state_store, "get_document", side_effect=guarded):
            result = callable_fn()
        self.assertNotIn(ppl.DIRECTORY_NS, seen["directories"])
        return result

    def test_feishu_admin_session_restored_without_password_table(self):
        mgr = self._build_manager()
        open_id = "ou_fixture_admin"
        mgr.upsert_permission_user(
            open_id=open_id, name="管理员", role="admin", scopes=[],
            enabled=True, updated_by="test",
        )
        session_id = self._persist_feishu_session(
            mgr, self._feishu_session(open_id, "admin", ["ALL", "CAMPUS", "H"])
        )
        session = self._assert_no_directory_lookup(
            mgr, lambda: mgr.get_session(session_id)
        )
        self.assertIsNotNone(session)
        self.assertEqual(session["user"]["open_id"], open_id)
        self.assertEqual(session["role"], "admin")
        # get_session re-derives admin scopes from permissions and keeps them.
        self.assertIn("ALL", session["allowed_scopes"])
        # A normal Feishu session carries no password material at all.
        self.assertNotIn("source", session)
        self.assertNotIn("password_revision", session)

        status = self._assert_no_directory_lookup(
            mgr, lambda: mgr.public_status(session)
        )
        self.assertIs(status["logged_in"], True)
        self.assertEqual(status["user"]["role"], "admin")
        self.assertEqual(status["user"]["login_method"], "feishu")

    def test_feishu_building_session_role_and_scopes_unchanged(self):
        mgr = self._build_manager()
        open_id = "ou_fixture_building"
        mgr.upsert_permission_user(
            open_id=open_id, name="楼栋用户", role="building", scopes=["E"],
            enabled=True, updated_by="test",
        )
        session_id = self._persist_feishu_session(
            mgr, self._feishu_session(open_id, "building", ["E"])
        )
        session = self._assert_no_directory_lookup(
            mgr, lambda: mgr.get_session(session_id)
        )
        self.assertIsNotNone(session)
        self.assertEqual(session["role"], "building")
        self.assertEqual(session["allowed_scopes"], ["E"])
        self.assertNotIn("password_revision", session)

        status = self._assert_no_directory_lookup(
            mgr, lambda: mgr.public_status(session)
        )
        self.assertIs(status["logged_in"], True)
        self.assertEqual(status["user"]["role"], "building")
        self.assertEqual(status["allowed_scopes"], ["E"])
        self.assertEqual(status["default_scope"], "E")
        self.assertEqual(status["user"]["login_method"], "feishu")


class PersonnelPasswordRouteTests(_TempDirTestCase):
    _tmp_prefix = "personnel_route_"

    def _build(self):
        with patch(
            "lan_bitable_template_portal.portal_auth.get_data_file_path",
            side_effect=lambda name: str(self.root / name),
        ):
            self.auth = PortalAuthManager()
        self.addCleanup(_shutdown_manager, self.auth)
        self.service = FakePortalService()
        runtime = SimpleNamespace(
            service=self.service,
            state_store=self.auth._state_store,
            auth_manager=self.auth,
        )
        def _current_session(request):
            sid = request.cookies.get("lan_portal_session", "")
            return self.auth.get_session(sid) if sid else None

        controller = SimpleNamespace(
            _request_base_url=lambda request: str(request.base_url),
            _current_session=_current_session,
        )
        self.app = FastAPI()
        ppl.install_personnel_password_login(self.app, controller, runtime)
        return self.app

    def _run(self, coro):
        return asyncio.run(coro)

    def _client(self):
        return httpx.AsyncClient(
            transport=ASGITransport(app=self.app),
            base_url="http://testserver",
        )

    def _make_personnel_session(self, rid="recChg", pwd="OldPass1"):
        stored = hash_password(pwd)
        self.service._records[rid] = {"姓名": "改密", "机楼/专业": "A楼", PASSWORD_FIELD: stored,
                                      "员工姓名": _user("改密", "ou_fixtureA")}
        person = {
            "id": rid, "name": "改密", "employee_no": "E1", "building": "A楼",
            "scopes": ["A"], "selectable": True, "inactive": False,
            "disabled_reason": "",
            "needs_setup": False,
            "password_revision": hashlib.sha256(stored.encode("utf-8")).hexdigest(), "account_nature": "外部账号",
            "login_ids": ["ou_fixtureA"],
        }
        self.auth._state_store.put_document(
            ppl.DIRECTORY_NS, ppl.DIRECTORY_KEY,
            {"people": {rid: person}, "loaded_at": time.time(), "schema": 2},
        )
        return self.auth.create_personnel_session(person)

    def _feishu_session(self, open_id, role, scopes):
        now = time.time()
        return {
            "user": {"open_id": open_id, "name": "飞书用户"},
            "role": role,
            "allowed_scopes": list(scopes),
            "created_at_ts": now,
            "created_at": "2026-10-10 00:00:00",
            "expires_at": now + AUTH_SESSION_TTL_SECONDS,
        }

    def _persist_feishu_session(self, session):
        session_id = secrets.token_urlsafe(32)
        self.auth._state_store.put_auth_session(
            self.auth._secret_hash(session_id), session
        )
        return session_id

    def test_bootstrap_returns_no_set_cookie(self):
        self._build()
        self.service._pages = [_page([_rec("recA")])]
        self.service._records["recA"] = {"姓名": "张三", "机楼/专业": "A楼",
                                         "员工姓名": _user("张三", "ou_fixture1")}

        async def run():
            async with self._client() as client:
                response = await client.get("/api/auth/password/people")
                return response

        response = self._run(run())
        self.assertEqual(response.status_code, 200, response.text)
        self.assertIs(response.json()["ok"], True)
        self.assertNotIn("set-cookie", {k.lower(): v for k, v in response.headers.items()})

    def test_csrf_origin_guard(self):
        self._build()
        cases = [
            ({"Origin": "http://other.test"}, 403),
            ({"Origin": "null"}, 403),
            ({"Referer": "http://other.test/x"}, 403),
            ({"Sec-Fetch-Site": "cross-site"}, 403),
            ({"Origin": "http://testserver"}, 400),  # same-origin but bad body -> payload error first? no -> origin ok
        ]

        async def run():
            async with self._client() as client:
                results = []
                for headers, expected in cases:
                    response = await client.post(
                        "/api/auth/password/login",
                        json={"person_id": "recA", "password": BOOTSTRAP},
                        headers=headers,
                    )
                    results.append((response.status_code, expected, headers))
                return results

        for status, expected, headers in self._run(run()):
            if headers == {"Origin": "http://testserver"}:
                # same-origin passes origin check; it fails later with payload/identity -> 400-ish
                continue
            self.assertEqual(status, expected, headers)

    def test_malformed_payload_and_size_limit(self):
        self._build()

        async def run():
            async with self._client() as client:
                malformed = await client.post(
                    "/api/auth/password/login",
                    content=b"{bad json",
                    headers={"Origin": "http://testserver"},
                )
                too_large = await client.post(
                    "/api/auth/password/login",
                    content=b'{"x": "' + b"a" * 9000 + b'"}',
                    headers={"Origin": "http://testserver"},
                )
                return malformed, too_large

        malformed, too_large = self._run(run())
        self.assertEqual(malformed.status_code, 400, malformed.text)
        self.assertEqual(too_large.status_code, 413, too_large.text)

    def test_successful_login_sets_http_only_cookie(self):
        self._build()
        self.service._pages = [_page([_rec("recA")])]
        self.service._records["recA"] = {"姓名": "张三", "机楼/专业": "A楼",
                                         "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                                         "员工姓名": _user("张三", "ou_fixture1")}

        async def run():
            async with self._client() as client:
                response = await client.post(
                    "/api/auth/password/login",
                    json={"person_id": "recA", "password": BOOTSTRAP, "new_password": "NewPass123"},
                    headers={"Origin": "http://testserver"},
                )
                return response

        with _allow_real_writes():
            response = self._run(run())
        self.assertEqual(response.status_code, 200, response.text)
        set_cookie = response.headers.get("set-cookie", "")
        self.assertIn("lan_portal_session=", set_cookie)
        self.assertIn("HttpOnly", set_cookie)
        self.assertIn("SameSite=Lax", set_cookie)
        self.assertIn("Path=/", set_cookie)

    def test_reset_requires_password_change_returns_no_cookie(self):
        self._build()
        self.service._pages = [_page([_rec("recR")])]
        self.service._records["recR"] = {"姓名": "重置", "机楼/专业": "D楼",
                                         "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                                         "员工姓名": _user("重置", "ou_fixture1")}

        async def run():
            async with self._client() as client:
                response = await client.post(
                    "/api/auth/password/reset",
                    json={"person_id": "recR", "identity_number": IDENTITY_NUMBER},
                    headers={"Origin": "http://testserver"},
                )
                return response

        response = self._run(run())
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertIs(body["ok"], True)
        self.assertIs(body["data"]["requires_password_change"], True)
        self.assertNotIn("set-cookie", {k.lower(): v for k, v in response.headers.items()})
        self.assertEqual(self.service._patch_calls, 0)

    def test_reset_writes_hash_no_session_cookie(self):
        self._build()
        self.service._pages = [_page([_rec("recR")])]
        self.service._records["recR"] = {"姓名": "重置", "机楼/专业": "D楼",
                                         "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                                         "员工姓名": _user("重置", "ou_fixture1")}

        async def run():
            async with self._client() as client:
                response = await client.post(
                    "/api/auth/password/reset",
                    json={"person_id": "recR", "identity_number": IDENTITY_NUMBER,
                          "new_password": "StrongPass9"},
                    headers={"Origin": "http://testserver"},
                )
                return response

        with _allow_real_writes():
            response = self._run(run())
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertIs(body["ok"], True)
        self.assertIs(body["data"]["password_changed"], True)
        # A password change never grants a session; the route clears any
        # existing personnel cookie instead (empty value + Max-Age=0).
        set_cookie = response.headers.get("set-cookie", "")
        self.assertIn("lan_portal_session=;", set_cookie)
        self.assertIn("Max-Age=0", set_cookie)
        self.assertEqual(set(self.service._last_patch_fields), {PASSWORD_FIELD})
        self.assertEqual(self.service._last_patch_table_id, ppl.TABLE_ID)

    def test_reset_wrong_identity_returns_401(self):
        self._build()
        self.service._pages = [_page([_rec("recR")])]
        self.service._records["recR"] = {"姓名": "重置", "机楼/专业": "D楼",
                                         "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                                         "员工姓名": _user("重置", "ou_fixture1")}

        async def run():
            async with self._client() as client:
                response = await client.post(
                    "/api/auth/password/reset",
                    json={"person_id": "recR", "identity_number": "00000020000109999X"},
                    headers={"Origin": "http://testserver"},
                )
                return response

        response = self._run(run())
        self.assertEqual(response.status_code, 401, response.text)
        body = response.json()
        self.assertIs(body["ok"], False)
        self.assertNotIn("auth_required", body)

    def test_reset_rejects_initial_suffix_as_new_password(self):
        self._build()
        self.service._pages = [_page([_rec("recR")])]
        self.service._records["recR"] = {"姓名": "重置", "机楼/专业": "D楼",
                                         "身份证号": IDENTITY_NUMBER, PASSWORD_FIELD: "",
                                         "员工姓名": _user("重置", "ou_fixture1")}

        async def run():
            async with self._client() as client:
                response = await client.post(
                    "/api/auth/password/reset",
                    json={"person_id": "recR", "identity_number": IDENTITY_NUMBER,
                          "new_password": BOOTSTRAP},
                    headers={"Origin": "http://testserver"},
                )
                return response

        with _allow_real_writes():
            response = self._run(run())
        self.assertEqual(response.status_code, 400, response.text)
        self.assertIs(response.json()["ok"], False)
        self.assertEqual(self.service._patch_calls, 0)

    def test_reset_cross_origin_denied(self):
        self._build()

        async def run():
            async with self._client() as client:
                return await client.post(
                    "/api/auth/password/reset",
                    json={"person_id": "recR", "identity_number": IDENTITY_NUMBER},
                    headers={"Origin": "http://evil.example"},
                )

        response = self._run(run())
        self.assertEqual(response.status_code, 403, response.text)

    def test_wrong_password_401_has_no_auth_required(self):
        self._build()
        self.service._pages = [_page([_rec("recA")])]
        self.service._records["recA"] = {
            "姓名": "张三", "机楼/专业": "A楼", PASSWORD_FIELD: hash_password("GoodPass1"),
            "员工姓名": _user("张三", "ou_fixture1"),
        }

        async def run():
            async with self._client() as client:
                response = await client.post(
                    "/api/auth/password/login",
                    json={"person_id": "recA", "password": "wrong"},
                    headers={"Origin": "http://testserver"},
                )
                return response

        response = self._run(run())
        self.assertEqual(response.status_code, 401, response.text)
        body = response.json()
        self.assertIs(body["ok"], False)
        self.assertNotIn("auth_required", body)

    def test_people_list_rate_limited_after_90_requests(self):
        self._build()
        self.service._pages = [_page([_rec("recA")])]
        self.service._records["recA"] = {"姓名": "张三", "机楼/专业": "A楼",
                                         "员工姓名": _user("张三", "ou_fixture1")}

        async def run():
            async with self._client() as client:
                statuses = []
                for _ in range(92):
                    response = await client.get("/api/auth/password/people")
                    statuses.append(response.status_code)
                return statuses

        statuses = self._run(run())
        self.assertEqual(statuses[0], 200)
        self.assertTrue(all(s == 200 for s in statuses[:90]), statuses[:90])
        # 91st and 92nd requests exceed the 90-request list window -> 429.
        self.assertEqual(statuses[90], 429, statuses)
        self.assertEqual(statuses[91], 429, statuses)

    # ---- authenticated change route (POST /api/auth/password/change) -------
    def test_change_success_clears_cookie_and_revokes_session_no_new(self):
        self._build()
        session_id = self._make_personnel_session()

        async def run():
            async with self._client() as client:
                return await client.post(
                    "/api/auth/password/change",
                    json={"current_password": "OldPass1", "new_password": "NewPass2"},
                    headers={"Origin": "http://testserver"},
                    cookies={"lan_portal_session": session_id},
                )

        with _allow_real_writes():
            response = self._run(run())
        self.assertEqual(response.status_code, 200, response.text)
        body = response.json()
        self.assertIs(body["ok"], True)
        self.assertIs(body["data"]["password_changed"], True)
        # Route clears the existing personnel cookie and issues NO new session.
        set_cookie = response.headers.get("set-cookie", "")
        self.assertIn("lan_portal_session=;", set_cookie)
        self.assertIn("Max-Age=0", set_cookie)
        self.assertIsNone(self.auth.get_session(session_id))
        # Only the 密码 target field is patched on the live 人员表.
        self.assertEqual(set(self.service._last_patch_fields), {PASSWORD_FIELD})
        self.assertEqual(self.service._last_patch_table_id, ppl.TABLE_ID)
        self.assertEqual(self.service._last_patch_app_token, ppl.APP_TOKEN)

    def test_change_missing_session_401_auth_required(self):
        self._build()

        async def run():
            async with self._client() as client:
                return await client.post(
                    "/api/auth/password/change",
                    json={"current_password": "OldPass1", "new_password": "NewPass2"},
                    headers={"Origin": "http://testserver"},
                )

        response = self._run(run())
        self.assertEqual(response.status_code, 401, response.text)
        body = response.json()
        self.assertIs(body["ok"], False)
        self.assertIs(body["auth_required"], True)

    def test_change_wrong_current_password_401_no_auth_required(self):
        self._build()
        session_id = self._make_personnel_session()

        async def run():
            async with self._client() as client:
                return await client.post(
                    "/api/auth/password/change",
                    json={"current_password": "WrongPass", "new_password": "NewPass2"},
                    headers={"Origin": "http://testserver"},
                    cookies={"lan_portal_session": session_id},
                )

        response = self._run(run())
        self.assertEqual(response.status_code, 401, response.text)
        body = response.json()
        self.assertIs(body["ok"], False)
        self.assertNotIn("auth_required", body)
        self.assertEqual(self.service._patch_calls, 0)

    def test_change_feishu_and_guest_sessions_rejected_403(self):
        self._build()
        feishu_id = self._persist_feishu_session(
            self._feishu_session("ou_fixtureF", "building", ["A"])
        )
        guest_id = self._persist_feishu_session(
            self._feishu_session("guest_tmp", "guest", [])
        )

        async def run():
            async with self._client() as client:
                feishu_resp = await client.post(
                    "/api/auth/password/change",
                    json={"current_password": "OldPass1", "new_password": "NewPass2"},
                    headers={"Origin": "http://testserver"},
                    cookies={"lan_portal_session": feishu_id},
                )
                guest_resp = await client.post(
                    "/api/auth/password/change",
                    json={"current_password": "OldPass1", "new_password": "NewPass2"},
                    headers={"Origin": "http://testserver"},
                    cookies={"lan_portal_session": guest_id},
                )
                return feishu_resp, guest_resp

        feishu_resp, guest_resp = self._run(run())
        self.assertEqual(feishu_resp.status_code, 403, feishu_resp.text)
        self.assertEqual(guest_resp.status_code, 403, guest_resp.text)

    def test_change_spoof_person_id_rejected_400(self):
        self._build()
        session_id = self._make_personnel_session()

        async def run():
            async with self._client() as client:
                return await client.post(
                    "/api/auth/password/change",
                    json={"person_id": "recEVIL", "current_password": "OldPass1", "new_password": "NewPass2"},
                    headers={"Origin": "http://testserver"},
                    cookies={"lan_portal_session": session_id},
                )

        response = self._run(run())
        self.assertEqual(response.status_code, 400, response.text)
        self.assertIs(response.json()["ok"], False)
        self.assertEqual(self.service._patch_calls, 0)


class RouteLoginMethodPreferenceRegressionTests(unittest.TestCase):
    """AST-level regression for life_guide_page / workbench_lite_page redirects.

    The real async route bodies are extracted from clipflow_backend/main.py with
    the FastAPI decorators removed, so the production controller never starts.
    These tests verify the unauth redirect honours the harmless
    ``clipflow-login-method=password`` preference cookie (redirecting to
    ``/?login=password&next=...``) while the default OAuth route is preserved,
    that the query ``next`` is kept exactly, and that a forged preference cookie
    alone never authenticates (unauthenticated sessions always stay redirects).
    """

    MAIN_PY = Path(__file__).resolve().parent / "clipflow_backend" / "main.py"

    @classmethod
    def _route_fn(cls, route_name: str, controller):
        tree = ast.parse(cls.MAIN_PY.read_text(encoding="utf-8-sig"))
        node = copy.deepcopy(
            next(
                node
                for node in ast.walk(tree)
                if isinstance(node, ast.AsyncFunctionDef) and node.name == route_name
            )
        )
        node.decorator_list = []
        namespace = {
            "self": controller,
            "Request": Request,
            "Response": Response,
            "asyncio": asyncio,
            "urlencode": urlencode,
            "portal_index_file": lambda: Path("fixture-index.html"),
            "NOTICE_TYPE_BY_WORK_TYPE": {"maintenance": "维保通告"},
            "SCOPE_OPTIONS": [{"value": "E", "label": "E楼"}],
            "PENDING_PAGE_SIZE": 24,
            "ONGOING_PAGE_SIZE": 18,
            "_current_month_label": lambda: "10月",
        }
        exec(
            compile(
                ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])),
                f"<{route_name}-route>",
                "exec",
            ),
            namespace,
        )
        return namespace[route_name]

    @staticmethod
    def _request(path, query="", password_cookie=False):
        headers = []
        if password_cookie:
            headers.append((b"cookie", b"clipflow-login-method=password"))
        return Request(
            {
                "type": "http",
                "method": "GET",
                "path": path,
                "scheme": "http",
                "server": ("fixture", 80),
                "query_string": query.encode(),
                "headers": headers,
            }
        )

    def _redirect_location(self, route_name, path, query="", password_cookie=False):
        controller = SimpleNamespace(
            _current_session=lambda _: None,
            _static_file_response=lambda *_: (_ for _ in ()).throw(
                AssertionError("static render must not be called when unauthenticated")
            ),
        )
        route = self._route_fn(route_name, controller)
        response = asyncio.run(route(self._request(path, query, password_cookie)))
        self.assertEqual(response.status_code, 302)
        return response.headers["location"]

    @staticmethod
    def _assert_default_oauth_login(location, expected_next):
        assert location.startswith("/api/auth/login?"), location
        params = parse_qs(urlsplit(location).query)
        if params["next"] != [expected_next]:
            raise AssertionError(f"next mismatch: {params['next']} != {[expected_next]}")

    @staticmethod
    def _assert_password_login(location, expected_next):
        assert location.startswith("/?login=password&"), location
        params = parse_qs(urlsplit(location).query)
        if params.get("login") != ["password"] or params["next"] != [expected_next]:
            raise AssertionError(
                f"password login mismatch: login={params.get('login')} next={params.get('next')}"
            )

    def test_life_guide_password_preference_redirects_to_password_login(self):
        for path in ("/life-guide", "/link-directory", "/knowledge-base"):
            with self.subTest(path=path):
                location = self._redirect_location(
                    "life_guide_page", path, query="a=1&b=2", password_cookie=True
                )
                self._assert_password_login(location, f"{path}?a=1&b=2")

    def test_life_guide_default_oauth_redirect_preserves_query_next(self):
        location = self._redirect_location("life_guide_page", "/life-guide", query="month=10")
        self._assert_default_oauth_login(location, "/life-guide?month=10")

    def test_workbench_lite_password_preference_drops_transient_params(self):
        location = self._redirect_location(
            "workbench_lite_page",
            "/workbench-lite",
            query="scope=E&month=10&_assistant_frame=1",
            password_cookie=True,
        )
        # _assistant_frame / _frame_retry are transient and dropped from next.
        self._assert_password_login(location, "/workbench-lite?scope=E&month=10")

    def test_workbench_lite_default_oauth_redirect_preserves_query_next(self):
        location = self._redirect_location("workbench_lite_page", "/workbench-lite", query="scope=E")
        self._assert_default_oauth_login(location, "/workbench-lite?scope=E")

    def test_workbench_lite_default_oauth_no_query_next_is_bare_path(self):
        location = self._redirect_location("workbench_lite_page", "/workbench-lite")
        self._assert_default_oauth_login(location, "/workbench-lite")

    def test_forged_password_preference_alone_never_authenticates(self):
        # The presence of clipflow-login-method=password must NOT grant access:
        # unauthenticated requests still get a 302 redirect (never a rendered
        # page), and the session resolver is still asked and returns None.
        for route_name in ("life_guide_page", "workbench_lite_page"):
            with self.subTest(route=route_name):
                session_resolver = Mock(return_value=None)
                controller = SimpleNamespace(
                    _current_session=session_resolver,
                    _static_file_response=Mock(),
                )
                route = self._route_fn(route_name, controller)
                path = "/life-guide" if route_name == "life_guide_page" else "/workbench-lite"
                response = asyncio.run(
                    route(self._request(path, password_cookie=True))
                )
                self.assertEqual(response.status_code, 302)
                self.assertTrue(response.headers["location"].startswith("/?login=password&"))
                session_resolver.assert_called_once()
                controller._static_file_response.assert_not_called()


class SignaturePeopleHashStrippingTests(unittest.TestCase):
    def test_load_signature_people_strips_password_field_without_network(self):
        service = object.__new__(MaintenancePortalService)
        service._signature_people_cache_lock = threading.RLock()
        service._signature_people_cache = None
        service._signature_crypto = SimpleNamespace(
            metadata_from_field=lambda value: {},
            is_portable_metadata=lambda metadata: False,
        )
        fields = {
            "姓名": "张三",
            "员工姓名": [{"open_id": "ou_fixtureSig", "name": "张三"}],
            "楼栋": "A楼",
            "离职/异动情况": "否",
            PASSWORD_FIELD: "pbkdf2_sha256$600000$somesalt$somehash",
        }
        payload = {"code": 0, "data": {"items": [{"record_id": "recSig1", "fields": fields}], "has_more": False}}
        service._request_json = lambda *args, **kwargs: payload

        people = service._load_signature_people(force=True)
        self.assertEqual(len(people), 1)
        raw = people[0].get("raw_fields") or {}
        self.assertNotIn(PASSWORD_FIELD, raw)
        serialized = json.dumps(people[0], ensure_ascii=False)
        self.assertNotIn("pbkdf2_sha256", serialized)
        self.assertNotIn("somehash", serialized)
        self.assertNotIn("somesalt", serialized)

    def test_load_signature_people_excludes_neither_openid_but_strips_password(self):
        # Regression: the signature people loader keeps the real open_id but
        # never leaks a password hash, and a malformed password value is treated
        # as a recovery signal (no crash, no write).
        fields = {
            "姓名": "李四",
            "员工姓名": [{"open_id": "ou_fixtureSig2", "name": "李四"}],
            PASSWORD_FIELD: "not-a-valid-hash",
        }
        payload = {"code": 0, "data": {"items": [{"record_id": "recSig2", "fields": fields}], "has_more": False}}
        service = object.__new__(MaintenancePortalService)
        service._signature_people_cache_lock = threading.RLock()
        service._signature_people_cache = None
        service._signature_crypto = SimpleNamespace(
            metadata_from_field=lambda value: {},
            is_portable_metadata=lambda metadata: False,
        )
        service._request_json = lambda *args, **kwargs: payload
        people = service._load_signature_people(force=True)
        self.assertEqual(len(people), 1)
        raw = people[0].get("raw_fields") or {}
        self.assertNotIn(PASSWORD_FIELD, raw)


if __name__ == "__main__":
    unittest.main()
