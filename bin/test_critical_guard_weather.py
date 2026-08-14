# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.critical_guard_weather import (
    build_weather_guard_card,
    normalize_weather_snapshot,
    progress_summary,
    weather_cells_patch,
)
from lan_bitable_template_portal.portal_service import (
    BUILDING_OPEN_ID_MAP,
    CRITICAL_GUARD_WEATHER_MEMORY_KEY,
    CRITICAL_GUARD_WEATHER_SCOPES,
    CRITICAL_GUARD_WEATHER_URL,
    MaintenancePortalService,
    PortalError,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore


class CriticalGuardWeatherParserTests(unittest.TestCase):
    @staticmethod
    def _payload() -> dict[str, object]:
        return {
            "schemaVersion": "2.0",
            "generatedAt": "2026-08-11T05:26:01.407Z",
            "pollIntervalSeconds": 600,
            "hasWarning": True,
            "hasGuard": True,
            "status": "active",
            "sourceState": "fresh",
            "guardDecisionState": "fresh",
            "lastSuccessAt": "2026-08-11T05:24:10.112Z",
            "guardLevel": "三级戒备",
            "primaryWarningId": "warning-one",
            "warnings": [
                {
                    "id": "warning-one",
                    "title": "南通市气象台发布暴雨蓝色预警",
                    "type": "暴雨",
                    "color": "blue",
                    "guardRequired": True,
                    "guardLevel": "三级戒备",
                    "publishTime": "2026-08-11T08:37+08:00",
                    "startTime": "2026-08-11T08:37+08:00",
                    "endTime": None,
                    "status": "发布",
                }
            ],
            "weather": {
                "temperature": 30,
                "feelsLikeTemperature": 31,
                "relativeHumidity": 92,
                "windDirection": "东风",
                "windLevel": 3,
                "windLevelText": "3级",
                "observedAt": "2026-08-11T12:31:18+08:00",
            },
            "units": {
                "temperature": "℃",
                "relativeHumidity": "%",
            },
            "guard": {
                "required": True,
                "level": "三级戒备",
                "actions": ["现场团队进行内部预警通报；"],
                "initialNotificationEnabled": True,
                "progressNotificationEnabled": False,
                "reminderIntervalMinutes": None,
            },
            "registrationRequirements": {
                "required": True,
                "enabled": False,
                "handlingMode": "external_application",
                "summary": "由其他应用承接",
                "applicableBuildings": ["A楼", "B楼", "C楼", "D楼", "E楼"],
                "requiredForms": [
                    "《重保戒备检查表-设备安全检查页》",
                    "《重保戒备检查表-灾害专项检查页》",
                    "《重保戒备检查表-物资检查清单》",
                    "《重保戒备检查表-重保联络清单》",
                ],
                "requiredContents": ["设备安全：填写正常或异常"],
                "attachmentRequirements": ["设备安全：提交1份检查附件"],
            },
        }

    def test_v2_final_contract_maps_long_required_form_names(self) -> None:
        snapshot = normalize_weather_snapshot(self._payload())
        self.assertEqual(CRITICAL_GUARD_WEATHER_URL, "https://www.sm.sjhl.online:3001/api/weather/warning")
        self.assertTrue(snapshot["fresh_for_publish"])
        self.assertTrue(snapshot["authoritative_for_state"])
        self.assertEqual(
            snapshot["warnings"][0]["sheet_types"],
            ["设备安全", "灾害专项", "物资检查清单", "重保联络清单"],
        )
        self.assertNotIn("环境安全", snapshot["sheet_types"])
        self.assertEqual(snapshot["weather"]["feels_like"], 31)
        self.assertEqual(snapshot["weather"]["wind_scale"], "3级")
        self.assertEqual(snapshot["registration"]["handling_mode"], "external_application")
        self.assertFalse(snapshot["registration"]["enabled"])

    def test_v2_required_forms_use_contains_matching_and_deduplicate(self) -> None:
        payload = self._payload()
        payload["guard"] = {**payload["guard"], "level": "一级戒备"}
        payload["guardLevel"] = "一级戒备"
        payload["warnings"] = [
            {
                **payload["warnings"][0],
                "id": "warning-two",
                "title": "台风红色预警",
                "guardLevel": "一级戒备",
            }
        ]
        requirements = dict(payload["registrationRequirements"])
        requirements["requiredForms"] = [
            "设备安全",
            "《重保戒备检查表-设备安全检查页》",
            "某某环境安全检查页",
            "《重保戒备检查表-客户重保检查页》",
            "灾害专项检查页",
        ]
        payload["registrationRequirements"] = requirements
        snapshot = normalize_weather_snapshot(payload)
        self.assertEqual(
            snapshot["sheet_types"],
            ["设备安全", "环境安全", "客户重保", "灾害专项"],
        )
        self.assertEqual(
            weather_cells_patch(snapshot["warnings"][0]),
            {
                "weather": {
                    "level1": "台风红色预警",
                    "level2": "",
                    "current": "台风红色预警",
                }
            },
        )

    def test_identity_changes_when_level_or_sheet_set_changes(self) -> None:
        base = self._payload()
        base["warnings"] = [{**base["warnings"][0], "id": "same-warning", "title": "暴雨预警"}]
        first = normalize_weather_snapshot(base)["warnings"][0]["weather_key"]
        changed = self._payload()
        changed["guardLevel"] = "二级戒备"
        changed["guard"] = {**changed["guard"], "level": "二级戒备"}
        changed["warnings"] = [
            {
                **changed["warnings"][0],
                "id": "same-warning",
                "title": "暴雨预警",
                "guardLevel": "二级戒备",
            }
        ]
        second = normalize_weather_snapshot(changed)["warnings"][0]["weather_key"]
        self.assertNotEqual(first, second)

    def test_all_guard_warnings_are_processed_by_stable_id(self) -> None:
        payload = self._payload()
        payload["warnings"] = [
            payload["warnings"][0],
            {
                **payload["warnings"][0],
                "id": "warning-three",
                "title": "南通市气象台发布雷暴大风黄色预警",
                "type": "雷暴大风",
                "color": "yellow",
            },
            {
                **payload["warnings"][0],
                "id": "warning-watch-only",
                "title": "仅关注天气提示",
                "guardRequired": False,
                "guardLevel": None,
            },
        ]
        snapshot = normalize_weather_snapshot(payload)
        self.assertEqual(
            [warning["id"] for warning in snapshot["warnings"]],
            ["warning-one", "warning-three"],
        )
        self.assertEqual(snapshot["unmatched_warning_count"], 1)

    def test_v2_explicit_empty_required_set_does_not_fallback(self) -> None:
        payload = self._payload()
        requirements = dict(payload["registrationRequirements"])
        requirements["required"] = False
        requirements["requiredForms"] = []
        payload["registrationRequirements"] = requirements
        snapshot = normalize_weather_snapshot(payload)
        self.assertEqual(snapshot["sheet_types"], [])
        self.assertEqual(len(snapshot["warnings"]), 1)
        self.assertFalse(snapshot["fresh_for_publish"])

    def test_v2_guard_not_required_does_not_publish(self) -> None:
        payload = self._payload()
        payload["hasGuard"] = False
        payload["guardLevel"] = None
        payload["guard"] = {**payload["guard"], "required": False, "level": None}
        payload["warnings"] = [
            {
                **payload["warnings"][0],
                "id": "warning-watch-only",
                "title": "仅关注天气提示",
                "guardRequired": False,
                "guardLevel": None,
            }
        ]
        requirements = dict(payload["registrationRequirements"])
        requirements["required"] = False
        requirements["requiredForms"] = []
        payload["registrationRequirements"] = requirements
        snapshot = normalize_weather_snapshot(payload)
        self.assertEqual(snapshot["sheet_types"], [])
        self.assertEqual(snapshot["warnings"], [])
        self.assertFalse(snapshot["fresh_for_publish"])

    def test_unsupported_schema_is_rejected(self) -> None:
        payload = self._payload()
        payload["schemaVersion"] = "1.9"
        with self.assertRaisesRegex(ValueError, "需要 2.x"):
            normalize_weather_snapshot(payload)

    def test_unknown_required_form_is_rejected_instead_of_partially_publishing(self) -> None:
        payload = self._payload()
        requirements = dict(payload["registrationRequirements"])
        requirements["requiredForms"] = ["《未知检查表》"]
        payload["registrationRequirements"] = requirements
        with self.assertRaisesRegex(ValueError, "尚未支持的必填检查表"):
            normalize_weather_snapshot(payload)

    def test_degraded_snapshot_is_not_authoritative(self) -> None:
        payload = self._payload()
        payload["status"] = "degraded"
        payload["sourceState"] = "degraded"
        payload["guardDecisionState"] = "stale"
        snapshot = normalize_weather_snapshot(payload)
        self.assertFalse(snapshot["authoritative_for_state"])
        self.assertFalse(snapshot["fresh_for_publish"])

    def test_fresh_clear_snapshot_is_authoritative(self) -> None:
        payload = self._payload()
        payload.update(
            {
                "hasWarning": False,
                "hasGuard": False,
                "status": "clear",
                "guardLevel": None,
                "primaryWarningId": None,
                "warnings": [],
                "guard": {
                    "required": False,
                    "level": None,
                    "actions": [],
                    "initialNotificationEnabled": True,
                    "progressNotificationEnabled": False,
                    "reminderIntervalMinutes": None,
                },
                "registrationRequirements": {
                    "required": False,
                    "enabled": False,
                    "handlingMode": "external_application",
                    "applicableBuildings": [],
                    "requiredForms": [],
                    "requiredContents": [],
                    "attachmentRequirements": [],
                },
            }
        )
        snapshot = normalize_weather_snapshot(payload)
        self.assertTrue(snapshot["authoritative_for_state"])
        self.assertFalse(snapshot["fresh_for_publish"])
        self.assertEqual(snapshot["warnings"], [])

    def test_progress_counts_building_completion_and_abnormal_items(self) -> None:
        progress = progress_summary(
            {
                "responses": [
                    {
                        "scope": "A",
                        "status": "submitted",
                        "cells": {
                            "template_items": [
                                {
                                    "key": "1",
                                    "category": "环境",
                                    "content": "检查漏水",
                                }
                            ],
                            "checks": {
                                "1": {"status": "abnormal", "note": "渗水"}
                            }
                        },
                    },
                    {"scope": "B", "status": "pending", "cells": {}},
                ]
            },
            ("A", "B"),
        )
        self.assertEqual(progress["registered_scopes"], 1)
        self.assertEqual(progress["completed_scopes"], 1)
        self.assertEqual(progress["abnormal"], 1)
        self.assertEqual(
            progress["scopes"][0]["abnormal_items"],
            ["检查项 · 检查漏水：渗水"],
        )
        self.assertFalse(progress["complete"])


class CriticalGuardWeatherStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = LanPortalStateStore(Path(self.temp_dir.name) / "state.sqlite3")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_weather_task_upsert_preserves_delivery_and_archive_state(self) -> None:
        created = self.store.put_critical_guard_weather_task(
            weather_key="weather-key",
            warning_id="warning-id",
            warning_title="暴雨蓝色预警",
            warning_type="暴雨",
            warning_color="blue",
            guard_level="三级戒备",
            sheet_types=["设备安全"],
            task_id="task-id",
            source_payload={"snapshot": {"status": "active"}},
        )
        self.store.update_critical_guard_weather_task(
            "weather-key",
            scope_state={"A": {"initial_sent_at": 10}},
            archive_status="failed",
            archive_error="timeout",
        )
        updated = self.store.put_critical_guard_weather_task(
            weather_key="weather-key",
            warning_id="warning-id",
            warning_title="暴雨蓝色预警",
            warning_type="暴雨",
            warning_color="blue",
            guard_level="三级戒备",
            sheet_types=["设备安全"],
            task_id="task-id",
            source_payload={"snapshot": {"status": "active", "new": True}},
        )
        self.assertEqual(created["archive_status"], "pending")
        self.assertEqual(updated["scope_state"]["A"]["initial_sent_at"], 10)
        self.assertEqual(updated["archive_status"], "failed")
        self.assertEqual(updated["archive_error"], "timeout")

    def test_existing_weather_task_refreshes_snapshot_and_keeps_download_token(self) -> None:
        service = self._weather_service()
        self.store.put_critical_guard_weather_task(
            weather_key="weather-refresh-key",
            warning_id="warning-id",
            warning_title="暴雨蓝色预警",
            warning_type="暴雨",
            warning_color="blue",
            guard_level="三级戒备",
            sheet_types=["设备安全"],
            task_id="weather-task-id",
            source_payload={
                "template_download_token": "stable-token",
                "snapshot": {"weather": {"temperature": "28"}},
            },
        )
        refreshed, created = service._ensure_critical_guard_weather_task(
            snapshot={
                "weather": {"temperature": "31"},
                "snapshot_at": "2026-08-11T12:00:00+08:00",
                "actions": [],
            },
            warning={
                "weather_key": "weather-refresh-key",
                "id": "warning-id",
                "title": "暴雨蓝色预警",
                "type": "暴雨",
                "color": "blue",
                "guard_level": "三级戒备",
                "sheet_types": ["设备安全"],
            },
            operator_open_id="",
            operator_name="",
        )
        self.assertFalse(created)
        self.assertEqual(
            refreshed["source_payload"]["snapshot"]["weather"]["temperature"],
            "31",
        )
        self.assertEqual(
            refreshed["source_payload"]["template_download_token"],
            "stable-token",
        )

    def test_new_weather_task_reuses_latest_saved_checks_per_scope(self) -> None:
        service = self._weather_service()
        previous = service.create_critical_guard_task(
            name="旧暴雨预警任务",
            sheet_types=["设备安全"],
            target_scopes=["A"],
            operation_id="legacy-weather-memory-task",
            operator_open_id="operator-open-id",
            operator_name="管理员",
        )
        response = previous["responses"][0]
        cells = dict(response["cells"])
        checks = {key: dict(value) for key, value in cells["checks"].items()}
        first_key = next(iter(checks))
        checks[first_key] = {"status": "abnormal", "note": "沿用上次检查结果"}
        cells["checks"] = checks
        self.store.update_critical_guard_response(
            response["response_id"],
            cells=cells,
            signatures=[],
            signature_source="",
            signature_record_id="",
            signature_name="",
            generated=False,
            generated_image=None,
            expected_version=response["version"],
            actor_open_id="operator-open-id",
            actor_name="填写人",
        )

        weather_task, created = service._ensure_critical_guard_weather_task(
            snapshot={"weather": {}, "actions": []},
            warning={
                "weather_key": "new-weather-memory-key",
                "id": "new-warning-id",
                "title": "新的台风预警",
                "type": "台风",
                "color": "orange",
                "guard_level": "二级戒备",
                "sheet_types": ["设备安全"],
            },
            operator_open_id="operator-open-id",
            operator_name="管理员",
        )

        self.assertTrue(created)
        task = self.store.get_critical_guard_task(
            weather_task["task_id"], include_all_responses=True
        ) or {}
        self.assertEqual(task["memory_key"], CRITICAL_GUARD_WEATHER_MEMORY_KEY)
        by_scope = {item["scope"]: item for item in task["responses"]}
        self.assertEqual(
            by_scope["A"]["cells"]["checks"][first_key],
            {"status": "abnormal", "note": "沿用上次检查结果"},
        )
        self.assertEqual(
            by_scope["B"]["cells"]["checks"][first_key],
            {"status": "normal", "note": ""},
        )

    def test_weather_job_cleanup_keeps_recent_terminal_jobs(self) -> None:
        for index in range(5):
            job_id = f"job-{index}"
            self.store.create_critical_guard_weather_job(
                job_id=job_id,
                trigger_type="manual",
            )
            self.store.update_critical_guard_weather_job(
                job_id,
                phase="completed",
                status="completed",
                finished=True,
            )
            time.sleep(0.002)
        removed = self.store.cleanup_critical_guard_weather_jobs(keep_count=2)
        # The production method enforces a conservative minimum of ten records.
        self.assertEqual(removed, 0)
        for index in range(5, 15):
            job_id = f"job-{index}"
            self.store.create_critical_guard_weather_job(
                job_id=job_id,
                trigger_type="manual",
            )
            self.store.update_critical_guard_weather_job(
                job_id,
                phase="completed",
                status="completed",
                finished=True,
            )
            time.sleep(0.002)
        removed = self.store.cleanup_critical_guard_weather_jobs(keep_count=10)
        self.assertEqual(removed, 5)
        self.assertIsNone(self.store.get_critical_guard_weather_job("job-0"))
        self.assertIsNotNone(self.store.get_critical_guard_weather_job("job-14"))

    def test_failed_query_keeps_last_successful_snapshot(self) -> None:
        service = MaintenancePortalService.__new__(MaintenancePortalService)
        service._state_store = self.store
        service._critical_guard_weather_job_lock = threading.RLock()
        service._critical_guard_weather_running_job_id = "job-id"
        service._fetch_critical_guard_weather_snapshot = lambda: (_ for _ in ()).throw(
            PortalError("network timeout")
        )
        self.store.put_critical_guard_weather_state(
            {"snapshot": {"status": "active", "marker": "keep"}}
        )
        self.store.create_critical_guard_weather_job(
            job_id="job-id",
            trigger_type="manual",
        )
        service._run_critical_guard_weather_job(job_id="job-id")
        state = self.store.get_critical_guard_weather_state()
        job = self.store.get_critical_guard_weather_job("job-id") or {}
        self.assertEqual(state["snapshot"]["marker"], "keep")
        self.assertIn("network timeout", state["last_error"])
        self.assertEqual(job["status"], "failed")

    def test_degraded_response_keeps_last_authoritative_warning_state(self) -> None:
        service = self._weather_service()
        service._fetch_critical_guard_weather_snapshot = lambda: {
            "schema_version": "2.0",
            "status": "degraded",
            "source_state": "degraded",
            "guard_decision_state": "stale",
            "authoritative_for_state": False,
            "fresh_for_publish": False,
            "warnings": [],
            "guard_level": "",
            "poll_interval_seconds": 600,
        }
        self.store.put_critical_guard_weather_state(
            {
                "current_warnings": [{"id": "keep-warning", "title": "保留预警"}],
                "current_guard_level": "三级戒备",
            }
        )
        job = self.store.create_critical_guard_weather_job(
            job_id="degraded-job",
            trigger_type="manual",
        )
        service._critical_guard_weather_running_job_id = job["job_id"]
        service._run_critical_guard_weather_job(job_id=job["job_id"])
        state = self.store.get_critical_guard_weather_state()
        self.assertEqual(state["current_warnings"][0]["id"], "keep-warning")
        self.assertEqual(state["current_guard_level"], "三级戒备")
        self.assertEqual(state["snapshot"]["source_state"], "degraded")

    def test_fresh_clear_response_clears_last_warning_state(self) -> None:
        service = self._weather_service()
        service._fetch_critical_guard_weather_snapshot = lambda: {
            "schema_version": "2.0",
            "status": "clear",
            "source_state": "fresh",
            "guard_decision_state": "fresh",
            "authoritative_for_state": True,
            "fresh_for_publish": False,
            "warnings": [],
            "guard_level": "",
            "poll_interval_seconds": 600,
        }
        self.store.put_critical_guard_weather_state(
            {
                "current_warnings": [{"id": "old-warning", "title": "旧预警"}],
                "current_guard_level": "三级戒备",
            }
        )
        job = self.store.create_critical_guard_weather_job(
            job_id="clear-job",
            trigger_type="manual",
        )
        service._critical_guard_weather_running_job_id = job["job_id"]
        service._run_critical_guard_weather_job(job_id=job["job_id"])
        state = self.store.get_critical_guard_weather_state()
        self.assertEqual(state["current_warnings"], [])
        self.assertEqual(state["current_guard_level"], "")

    def test_template_download_token_is_scoped_to_weather_task(self) -> None:
        service = MaintenancePortalService.__new__(MaintenancePortalService)
        service._state_store = self.store
        self.store.put_critical_guard_weather_task(
            weather_key="weather-token-key",
            warning_id="warning-id",
            warning_title="暴雨蓝色预警",
            warning_type="暴雨",
            warning_color="blue",
            guard_level="三级戒备",
            sheet_types=["设备安全"],
            task_id="weather-task-id",
            source_payload={"template_download_token": "secret-token"},
        )
        self.assertTrue(
            service.can_download_critical_guard_template(
                task_id="weather-task-id",
                token="secret-token",
            )
        )
        self.assertFalse(
            service.can_download_critical_guard_template(
                task_id="weather-task-id",
                token="wrong-token",
            )
        )

    @staticmethod
    def _empty_snapshot() -> dict[str, object]:
        return {
            "fresh_for_publish": True,
            "warnings": [],
            "guard_level": "",
        }

    def _weather_service(self) -> MaintenancePortalService:
        service = MaintenancePortalService.__new__(MaintenancePortalService)
        service._state_store = self.store
        service._critical_guard_response_locks_guard = threading.RLock()
        service._critical_guard_response_locks = {}
        service._critical_guard_weather_job_lock = threading.RLock()
        service._critical_guard_weather_running_job_id = ""
        return service

    def test_h_observer_card_has_no_action_buttons(self) -> None:
        card = build_weather_guard_card(
            weather_task={
                "warning_title": "暴雨蓝色预警",
                "guard_level": "三级戒备",
                "source_payload": {},
            },
            progress={"scopes": []},
            registration_url="http://example.test/register",
            template_url="http://example.test/template",
            include_actions=False,
        )
        self.assertFalse(
            any(item.get("tag") == "action" for item in card.get("elements") or [])
        )
        self.assertNotIn("H", CRITICAL_GUARD_WEATHER_SCOPES)

    def test_scope_and_all_completion_cards_use_distinct_messages(self) -> None:
        weather_task = {
            "warning_title": "暴雨蓝色预警",
            "guard_level": "三级戒备",
            "source_payload": {},
        }
        partial_progress = {
            "registered_scopes": 1,
            "completed_scopes": 1,
            "scope_count": 5,
            "submitted": 2,
            "total": 10,
            "abnormal": 0,
            "complete": False,
            "scopes": [
                {
                    "scope": "A",
                    "submitted": 2,
                    "total": 2,
                    "complete": True,
                    "abnormal_items": ["设备安全 · 水泵状态：渗水"],
                },
                {"scope": "B", "submitted": 0, "total": 2, "complete": False},
            ],
        }
        scope_card = build_weather_guard_card(
            weather_task=weather_task,
            progress=partial_progress,
            registration_url="http://example.test/register",
            template_url="http://example.test/template",
            message_kind="completed",
            recipient_scope="A",
        )
        self.assertEqual(
            scope_card["header"]["title"]["content"],
            "A楼重保检查 · 本楼已完成",
        )
        scope_content = scope_card["elements"][0]["text"]["content"]
        self.assertIn("A楼已完成本次 2/2 项检查", scope_content)
        self.assertIn("　异常：设备安全 · 水泵状态：渗水", scope_content)
        self.assertFalse(
            any(item.get("tag") == "action" for item in scope_card["elements"])
        )

        all_progress = {
            **partial_progress,
            "registered_scopes": 5,
            "completed_scopes": 5,
            "submitted": 10,
            "complete": True,
        }
        all_card = build_weather_guard_card(
            weather_task=weather_task,
            progress=all_progress,
            registration_url="http://example.test/register",
            template_url="http://example.test/template",
            message_kind="completed",
        )
        self.assertEqual(
            all_card["header"]["title"]["content"],
            "南通天气重保 · 全部楼栋已完成",
        )
        all_content = all_card["elements"][0]["text"]["content"]
        self.assertIn("全部 5 个楼栋已完成本次重保检查", all_content)
        self.assertIn("戒备要求 · 5项", all_content)
        self.assertIn("5. 应急储备物资清点", all_content)
        self.assertNotEqual(scope_content, all_content)

    def test_h_observer_receives_one_persisted_message_per_weather_task(self) -> None:
        service = self._weather_service()
        task = service.create_critical_guard_task(
            name="H楼单次通知测试",
            sheet_types=["设备安全"],
            target_scopes=list(CRITICAL_GUARD_WEATHER_SCOPES),
            operation_id="weather-h-observer-task",
            operator_open_id="operator-open-id",
            operator_name="管理员",
        )
        weather_task = self.store.put_critical_guard_weather_task(
            weather_key="weather-h-observer-key",
            warning_id="weather-h-observer-warning",
            warning_title="暴雨蓝色预警",
            warning_type="暴雨",
            warning_color="blue",
            guard_level="三级戒备",
            sheet_types=["设备安全"],
            task_id=task["task_id"],
            source_payload={},
        )
        already_sent = {
            scope: {"initial_sent_at": 1}
            for scope in CRITICAL_GUARD_WEATHER_SCOPES
        }
        weather_task = self.store.update_critical_guard_weather_task(
            "weather-h-observer-key",
            scope_state=already_sent,
        )
        calls: list[dict[str, object]] = []

        def capture_send(**kwargs):
            calls.append(dict(kwargs))
            return True, "ok"

        service._send_critical_guard_weather_scope_card = capture_send
        first = service._reconcile_critical_guard_weather_task(weather_task, now=100)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0].get("scope"), "H")
        self.assertEqual(calls[0].get("recipient_open_id"), BUILDING_OPEN_ID_MAP["H"])
        self.assertIs(calls[0].get("include_actions"), False)
        self.assertEqual(first["notifications_sent"], 1)
        self.assertEqual(first["notifications_failed"], 0)

        persisted = self.store.get_critical_guard_weather_task(
            weather_key="weather-h-observer-key"
        ) or {}
        self.assertEqual(
            persisted.get("scope_state", {}).get("_h_observer", {}).get("sent_at"),
            100,
        )

        restarted_service = self._weather_service()
        calls.clear()
        restarted_service._send_critical_guard_weather_scope_card = capture_send
        second = restarted_service._reconcile_critical_guard_weather_task(
            persisted,
            now=101,
        )
        self.assertEqual(calls, [])
        self.assertEqual(second["notifications_sent"], 0)
        self.assertEqual(second["notifications_failed"], 0)

        calls.clear()
        restarted_service._reconcile_critical_guard_weather_task(persisted, now=1400)
        self.assertEqual(len(calls), len(CRITICAL_GUARD_WEATHER_SCOPES))
        self.assertNotIn(BUILDING_OPEN_ID_MAP["H"], {
            str(item.get("recipient_open_id") or "") for item in calls
        })
        self.assertTrue(all(item.get("include_actions", True) for item in calls))

    def test_all_completed_card_is_sent_to_group_once(self) -> None:
        service = self._weather_service()
        weather_task = self.store.put_critical_guard_weather_task(
            weather_key="weather-completion-group-key",
            warning_id="weather-completion-group-warning",
            warning_title="暴雨蓝色预警",
            warning_type="暴雨",
            warning_color="blue",
            guard_level="三级戒备",
            sheet_types=["设备安全"],
            task_id="weather-completion-group-task",
            source_payload={},
        )
        scope_state = {
            scope: {
                "initial_sent_at": 1,
                "completed_notified_at": 1,
                "group_completion_ready_at": 1,
            }
            for scope in CRITICAL_GUARD_WEATHER_SCOPES
        }
        scope_state["_h_observer"] = {"sent_at": 1}
        weather_task = self.store.update_critical_guard_weather_task(
            "weather-completion-group-key",
            scope_state=scope_state,
        )
        complete_progress = {
            "registered_scopes": 5,
            "completed_scopes": 5,
            "scope_count": 5,
            "submitted": 5,
            "total": 5,
            "abnormal": 0,
            "scopes": [
                {
                    "scope": scope,
                    "submitted": 1,
                    "total": 1,
                    "complete": True,
                    "registered": True,
                }
                for scope in CRITICAL_GUARD_WEATHER_SCOPES
            ],
            "complete": True,
        }
        service._state_store.get_critical_guard_task = lambda *_args, **_kwargs: {
            "task_id": "weather-completion-group-task",
            "responses": [],
        }
        service._archive_critical_guard_weather_task = (
            lambda *_args, **_kwargs: {
                "status": "archived",
                "record_id": "archive-record-id",
            }
        )
        cards: list[dict[str, object]] = []

        def capture_group(card, chat_id):
            cards.append({"card": card, "chat_id": chat_id})
            return True, "ok"

        with (
            patch(
                "lan_bitable_template_portal.portal_service.critical_guard_weather_progress",
                return_value=complete_progress,
            ),
            patch(
                "lan_bitable_template_portal.portal_service.send_interactive_to_chat_id",
                side_effect=capture_group,
            ),
        ):
            first = service._reconcile_critical_guard_weather_task(
                weather_task,
                now=100,
            )
            persisted = self.store.get_critical_guard_weather_task(
                weather_key="weather-completion-group-key"
            ) or {}
            second = service._reconcile_critical_guard_weather_task(
                persisted,
                now=101,
            )

        self.assertEqual(len(cards), 6)
        self.assertTrue(
            all(
                item["chat_id"] == "oc_afb27caf36b3bfeea2de20bd6f955d21"
                for item in cards
            )
        )
        self.assertEqual(
            [item["card"]["header"]["title"]["content"] for item in cards[:5]],
            [f"{scope}楼重保检查 · 本楼已完成" for scope in CRITICAL_GUARD_WEATHER_SCOPES],
        )
        self.assertEqual(
            cards[-1]["card"]["header"]["title"]["content"],
            "南通天气重保 · 全部楼栋已完成",
        )
        self.assertEqual(
            persisted["scope_state"]["_archive_completion_group"]["sent_at"],
            100,
        )
        self.assertEqual(first["notifications_sent"], 6)
        self.assertEqual(second["notifications_sent"], 0)

    def test_restart_does_not_send_historical_completion_without_pending_marker(self) -> None:
        service = self._weather_service()
        weather_task = self.store.put_critical_guard_weather_task(
            weather_key="historical-completion-key",
            warning_id="historical-warning",
            warning_title="历史已完成任务",
            warning_type="暴雨",
            warning_color="blue",
            guard_level="三级戒备",
            sheet_types=["设备安全"],
            task_id="historical-completion-task",
            source_payload={},
        )
        scope_state = {
            scope: {"initial_sent_at": 1, "completed_notified_at": 1}
            for scope in CRITICAL_GUARD_WEATHER_SCOPES
        }
        scope_state["_h_observer"] = {"sent_at": 1}
        weather_task = self.store.update_critical_guard_weather_task(
            "historical-completion-key",
            scope_state=scope_state,
        )
        service._state_store.get_critical_guard_task = lambda *_args, **_kwargs: {
            "task_id": "historical-completion-task",
            "responses": [],
        }
        service._archive_critical_guard_weather_task = (
            lambda *_args, **_kwargs: {"status": "archived", "reused": True}
        )
        complete_progress = {
            "registered_scopes": 5,
            "completed_scopes": 5,
            "scope_count": 5,
            "submitted": 5,
            "total": 5,
            "abnormal": 0,
            "scopes": [
                {
                    "scope": scope,
                    "submitted": 1,
                    "total": 1,
                    "complete": True,
                    "registered": True,
                }
                for scope in CRITICAL_GUARD_WEATHER_SCOPES
            ],
            "complete": True,
        }
        cards: list[dict[str, object]] = []
        with (
            patch(
                "lan_bitable_template_portal.portal_service.critical_guard_weather_progress",
                return_value=complete_progress,
            ),
            patch(
                "lan_bitable_template_portal.portal_service.send_interactive_to_chat_id",
                side_effect=lambda card, chat_id: (
                    cards.append({"card": card, "chat_id": chat_id}) or True,
                    "ok",
                ),
            ),
        ):
            result = service._reconcile_critical_guard_weather_task(
                weather_task,
                now=100,
            )

        self.assertEqual(cards, [])
        self.assertEqual(result["notifications_sent"], 0)

    def test_manual_trigger_reuses_running_scheduled_job(self) -> None:
        service = self._weather_service()
        fetch_started = threading.Event()
        release_fetch = threading.Event()

        def fetch() -> dict[str, object]:
            fetch_started.set()
            release_fetch.wait(timeout=3)
            return self._empty_snapshot()

        service._fetch_critical_guard_weather_snapshot = fetch
        scheduled = service.start_critical_guard_weather_job(
            trigger_type="scheduled"
        )
        self.assertTrue(fetch_started.wait(timeout=2))
        manual = service.start_critical_guard_weather_job(trigger_type="manual")
        self.assertEqual(manual["job_id"], scheduled["job_id"])
        self.assertTrue(manual["reused"])
        release_fetch.set()
        deadline = time.time() + 3
        while time.time() < deadline:
            job = self.store.get_critical_guard_weather_job(scheduled["job_id"]) or {}
            if (
                job.get("status") == "completed"
                and not service._critical_guard_weather_running_job_id
            ):
                break
            time.sleep(0.01)
        self.assertEqual(job.get("status"), "completed")
        self.assertFalse(service._critical_guard_weather_running_job_id)

    def test_pause_blocks_manual_and_scheduled_weather_queries(self) -> None:
        service = self._weather_service()
        status = service.set_critical_guard_weather_query_paused(
            paused=True,
            operator_open_id="operator-id",
            operator_name="管理员",
        )

        self.assertTrue(status["paused"])
        self.assertEqual(status["next_query_at"], 0)
        self.assertEqual(status["pause_updated_by_name"], "管理员")
        with self.assertRaisesRegex(PortalError, "已暂停"):
            service.start_critical_guard_weather_job(trigger_type="manual")
        scheduled = service.start_critical_guard_weather_job(
            trigger_type="scheduled"
        )
        self.assertTrue(scheduled["paused"])
        self.assertFalse(scheduled["running"])

        resumed = service.set_critical_guard_weather_query_paused(paused=False)
        self.assertFalse(resumed["paused"])
        self.assertGreater(resumed["next_query_at"], time.time())

    def test_job_queued_before_pause_does_not_call_weather_interface(self) -> None:
        service = self._weather_service()
        fetch_calls = 0

        def fetch() -> dict[str, object]:
            nonlocal fetch_calls
            fetch_calls += 1
            return self._empty_snapshot()

        service._fetch_critical_guard_weather_snapshot = fetch
        job = self.store.create_critical_guard_weather_job(
            job_id="paused-before-fetch",
            trigger_type="scheduled",
        )
        service._critical_guard_weather_running_job_id = job["job_id"]
        self.store.put_critical_guard_weather_state({"paused": True})

        service._run_critical_guard_weather_job(job_id=job["job_id"])

        completed = self.store.get_critical_guard_weather_job(job["job_id"]) or {}
        self.assertEqual(fetch_calls, 0)
        self.assertEqual(completed.get("status"), "completed")
        self.assertEqual(completed.get("phase"), "paused")
        self.assertTrue((completed.get("result") or {}).get("skipped"))

    def test_manual_query_does_not_shift_next_scheduled_query(self) -> None:
        service = self._weather_service()
        service._fetch_critical_guard_weather_snapshot = self._empty_snapshot

        scheduled_job = self.store.create_critical_guard_weather_job(
            job_id="scheduled-job",
            trigger_type="scheduled",
        )
        service._critical_guard_weather_running_job_id = scheduled_job["job_id"]
        service._run_critical_guard_weather_job(job_id=scheduled_job["job_id"])
        scheduled_anchor = float(
            self.store.get_critical_guard_weather_state().get(
                "last_scheduled_query_at"
            )
            or 0
        )
        self.assertGreater(scheduled_anchor, 0)

        manual_job = self.store.create_critical_guard_weather_job(
            job_id="manual-job",
            trigger_type="manual",
        )
        service._critical_guard_weather_running_job_id = manual_job["job_id"]
        service._run_critical_guard_weather_job(job_id=manual_job["job_id"])
        status = service.critical_guard_weather_status()
        self.assertAlmostEqual(
            status["next_query_at"],
            scheduled_anchor + 10 * 60,
            places=3,
        )

    def test_archive_rejects_missing_generated_images_before_remote_calls(self) -> None:
        service = self._weather_service()
        with self.assertRaisesRegex(PortalError, "缺少已生成图片"):
            service._critical_guard_archive_fields(
                {
                    "weather_key": "missing-image-key",
                    "warning_title": "暴雨蓝色预警",
                    "sheet_types": ["设备安全"],
                },
                {
                    "responses": [
                        {
                            "scope": "A",
                            "sheet_type": "设备安全",
                            "status": "submitted",
                            "generated_image_path": "",
                        }
                    ]
                },
            )

    def test_archive_sets_both_managers_without_confirmation(self) -> None:
        service = self._weather_service()
        image_path = Path(self.temp_dir.name) / "device.png"
        image_path.write_bytes(b"image")
        field_names = {
            "日期",
            "检查人",
            "重保标签",
            "重保戒备等级",
            "机房经理",
            "机房经理确认",
            "检查设备安全项",
            "设备安全检查纸质图片",
            "设备异常描述",
        }
        meta_by_name = {
            name: SimpleNamespace(
                field_type=3 if name == "机房经理确认" else 1,
                ui_type="SingleSelect" if name == "机房经理确认" else "Text",
                option_names=["已确认"] if name == "机房经理确认" else [],
            )
            for name in field_names
        }
        service._load_table_fields = lambda **_kwargs: ([], meta_by_name)
        fields, attachments = service._critical_guard_archive_fields(
            {
                "weather_key": "archive-fields-key",
                "warning_title": "暴雨蓝色预警",
                "warning_type": "暴雨",
                "warning_color": "blue",
                "guard_level": "三级戒备",
                "sheet_types": ["设备安全"],
            },
            {
                "responses": [
                    {
                        "scope": "A",
                        "sheet_type": "设备安全",
                        "status": "submitted",
                        "generated_image_path": str(image_path),
                        "cells": {"checks": {}},
                    }
                ]
            },
        )
        self.assertEqual(
            fields["机房经理"],
            [
                {"id": "ou_a6644e62a43b916c6bc26148cf74f208"},
                {"id": "ou_6e607320c167d816366acba893b339b1"},
            ],
        )
        self.assertEqual(fields["重保标签"], "暴雨蓝色")
        self.assertNotIn("机房经理确认", fields)
        self.assertEqual(
            attachments["设备安全检查纸质图片"],
            [str(image_path)],
        )

    def test_archive_record_lookup_requires_matching_tag_and_date(self) -> None:
        service = self._weather_service()
        service._request_json = lambda *_args, **_kwargs: {
            "data": {
                "items": [
                    {
                        "record_id": "old-record",
                        "fields": {"重保标签": "暴雨蓝色", "日期": 1_700_000_000_000},
                    },
                    {
                        "record_id": "current-record",
                        "fields": {"重保标签": "暴雨蓝色", "日期": 1_800_000_010_000},
                    },
                    {
                        "record_id": "wrong-tag",
                        "fields": {"重保标签": "高温橙色", "日期": 1_800_000_000_000},
                    },
                ],
                "has_more": False,
            }
        }
        self.assertEqual(
            service._find_critical_guard_archive_record(
                "暴雨蓝色",
                expected_date_ms=1_800_000_000_000,
            ),
            "current-record",
        )
        self.assertEqual(
            service._find_critical_guard_archive_record(
                "暴雨蓝色",
                expected_date_ms=1_900_000_000_000,
            ),
            "",
        )

    def test_one_bad_task_does_not_abort_other_weather_tasks(self) -> None:
        service = self._weather_service()
        service._fetch_critical_guard_weather_snapshot = self._empty_snapshot
        for index in range(2):
            self.store.put_critical_guard_weather_task(
                weather_key=f"weather-task-{index}",
                warning_id=f"warning-{index}",
                warning_title=f"预警 {index}",
                warning_type="暴雨",
                warning_color="blue",
                guard_level="三级戒备",
                sheet_types=["设备安全"],
                task_id=f"guard-task-{index}",
                source_payload={},
            )
        calls = 0

        def reconcile(weather_task, *, now):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise PortalError("corrupt task")
            return {
                "task_id": weather_task["task_id"],
                "notifications_sent": 1,
                "notifications_failed": 0,
                "archive": {"status": "pending"},
            }

        service._reconcile_critical_guard_weather_task = reconcile
        job = self.store.create_critical_guard_weather_job(
            job_id="isolated-job",
            trigger_type="manual",
        )
        service._critical_guard_weather_running_job_id = job["job_id"]
        service._run_critical_guard_weather_job(job_id=job["job_id"])
        completed = self.store.get_critical_guard_weather_job(job["job_id"]) or {}
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["result"]["processed_tasks"], 2)
        self.assertEqual(completed["result"]["task_failed"], 1)
        self.assertEqual(completed["result"]["notifications_sent"], 1)


if __name__ == "__main__":
    unittest.main()
