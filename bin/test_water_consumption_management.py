# -*- coding: utf-8 -*-
from __future__ import annotations

import io
import tempfile
import sys
import threading
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

from PIL import Image

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal import portal_service as portal_service_module
from lan_bitable_template_portal.portal_service import (
    FieldMeta,
    MaintenancePortalService,
    PortalError,
)
from lan_bitable_template_portal.state_store import LanPortalStateStore


def _record(
    record_id: str,
    scope: str,
    date_key: str,
    *,
    meter: str = "东区水表-总",
    usage: float = 1.0,
) -> dict:
    date_ms = int(
        __import__("datetime").datetime.strptime(date_key, "%Y-%m-%d").timestamp()
        * 1000
    )
    token = f"token_{record_id}"
    return {
        "record_id": record_id,
        "scope_code": scope,
        "building": f"{scope}楼",
        "title": f"{scope}楼水耗",
        "auto_number": record_id,
        "meter": meter,
        "frequency": "日",
        "shift": "白",
        "statistic_date_ms": date_ms,
        "statistic_date_key": date_key,
        "statistic_date": date_key,
        "meter_value": 100.0,
        "corrected_usage": None,
        "computed_usage": usage,
        "previous_date_text": "",
        "previous_value_text": "",
        "previous_usage": None,
        "yoy_ratio": 0.1,
        "created_time": f"{date_key} 08:00",
        "version": "1",
        "source_updated_at": 1.0,
        "formula_pending": False,
        "photos": [
            {
                "image_id": f"image_{record_id}",
                "file_token": token,
                "name": f"{record_id}.jpg",
                "mime_type": "image/jpeg",
                "size": 10,
                "download_url": f"https://example.test/{record_id}.jpg",
            }
        ],
        "search_text": f"{scope}楼\n{meter}\n{date_key}",
    }


class WaterConsumptionStateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = LanPortalStateStore(
            Path(self.tempdir.name) / "water_state.sqlite3"
        )

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_snapshot_query_summary_and_private_attachment_token(self) -> None:
        result = self.store.replace_water_consumption_snapshot(
            app_token="app",
            table_id="table",
            fields=[],
            options={"水表": ["东区水表-总", "西区水表-总"]},
            records=[
                _record("rec_a1", "A", "2026-07-25", usage=3.5),
                _record("rec_a2", "A", "2026-07-27", usage=4.5),
                _record("rec_b1", "B", "2026-07-27", usage=8.0),
            ],
        )
        self.assertEqual(result["record_count"], 3)

        page = self.store.query_water_consumption_records(
            scope="A",
            start_date="2026-07-26",
            end_date="2026-07-27",
            limit=50,
        )
        self.assertEqual(page["total"], 1)
        self.assertEqual(page["records"][0]["record_id"], "rec_a2")
        self.assertNotIn("file_token", page["records"][0]["photos"][0])

        summary = self.store.water_consumption_summary(
            scopes=["A", "B"],
            month="2026-07",
        )
        self.assertEqual(summary["aggregate"]["record_count"], 3)
        self.assertEqual(summary["aggregate"]["total_usage"], 16.0)

        image = self.store.get_water_consumption_image("image_rec_a2")
        self.assertEqual(image["file_token"], "token_rec_a2")
        self.assertEqual(image["scope_code"], "A")
        self.assertEqual(
            image["download_url"],
            "https://example.test/rec_a2.jpg",
        )
        self.assertNotIn(
            "download_url",
            page["records"][0]["photos"][0],
        )

    def test_operation_checkpoint_is_replayed(self) -> None:
        started = self.store.begin_water_consumption_operation(
            operation_id="water-op-1",
            operation_type="create",
            request_hash="hash-1",
        )
        self.assertEqual(started["state"], "started")
        self.store.checkpoint_water_consumption_operation(
            "water-op-1",
            result={"saved": True, "record_id": "rec_created"},
        )
        replay = self.store.begin_water_consumption_operation(
            operation_id="water-op-1",
            operation_type="create",
            request_hash="hash-1",
        )
        self.assertEqual(replay["state"], "replay")
        self.assertEqual(replay["result"]["record_id"], "rec_created")


class WaterConsumptionValidationTests(unittest.TestCase):
    @staticmethod
    def _meta(name: str, options: list[str]) -> FieldMeta:
        return FieldMeta(
            field_id=f"fld_{name}",
            field_name=name,
            ui_type="SingleSelect",
            field_type=3,
            is_primary=False,
            options_map={value: value for value in options},
            option_names=options,
            has_formula=False,
        )

    def test_write_fields_only_contains_writable_values(self) -> None:
        metas = {
            "水表": self._meta("水表", ["东区水表-总"]),
            "统计频次": self._meta("统计频次", ["日"]),
            "班次": self._meta("班次", ["白"]),
        }
        fields = MaintenancePortalService._water_write_fields(
            scope="A",
            meter="东区水表-总",
            frequency="日",
            shift="白",
            statistic_date="2026-07-27",
            meter_value="123.5",
            corrected_usage="2.5",
            attachment_tokens=["token-1"],
            meta_by_name=metas,
        )
        self.assertEqual(fields["楼栋"], "A楼")
        self.assertEqual(fields["水表数值"], 123.5)
        self.assertEqual(fields["水表照片"], [{"file_token": "token-1"}])
        self.assertNotIn("当期耗水量（t）", fields)
        self.assertNotIn("耗水量同比", fields)

    def test_invalid_select_value_is_rejected(self) -> None:
        metas = {
            "水表": self._meta("水表", ["东区水表-总"]),
            "统计频次": self._meta("统计频次", ["日"]),
            "班次": self._meta("班次", ["白"]),
        }
        with self.assertRaisesRegex(PortalError, "不在多维表选项中"):
            MaintenancePortalService._water_write_fields(
                scope="A",
                meter="未知水表",
                frequency="日",
                shift="白",
                statistic_date="2026-07-27",
                meter_value=1,
                corrected_usage=None,
                attachment_tokens=["token-1"],
                meta_by_name=metas,
            )

    def test_hash_attachment_name_is_replaced_with_readable_name(self) -> None:
        photos = MaintenancePortalService._water_photos(
            "rec_hash",
            [
                {
                    "file_token": "token-1",
                    "name": "ed5e4b9266d4a7380265b16664b8240d.jpg",
                    "type": "image/jpeg",
                    "url": "https://example.test/download",
                }
            ],
        )
        self.assertEqual(photos[0]["name"], "水表照片 1.jpg")
        self.assertEqual(
            photos[0]["download_url"],
            "https://example.test/download",
        )


class WaterConsumptionWriteFlowTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = LanPortalStateStore(
            Path(self.tempdir.name) / "water_write.sqlite3"
        )
        self.service = MaintenancePortalService.__new__(MaintenancePortalService)
        self.service._state_store = self.store
        self.service._water_consumption_fields = Mock(
            return_value=([], {}, {})
        )
        self.service.start_water_consumption_refresh_async = Mock()

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_create_writes_uploaded_photo_token_once_and_replays(self) -> None:
        self.service._water_upload_staged_images = Mock(
            return_value=(["photo-token-1"], ["upload-1"])
        )
        self.service._create_record_fields = Mock(
            return_value={"data": {"record": {"record_id": "rec_created"}}}
        )
        self.service._water_remote_record = Mock(
            return_value=_record("rec_created", "A", "2026-07-28")
        )

        kwargs = {
            "scope": "A",
            "meter": "东区水表-总",
            "frequency": "日",
            "shift": "白",
            "statistic_date": "2026-07-28",
            "meter_value": "101.5",
            "corrected_usage": None,
            "upload_ids": ["upload-1"],
            "operation_id": "water-create-op",
            "operator_open_id": "ou_test",
        }
        first = self.service.create_water_consumption_record(**kwargs)
        second = self.service.create_water_consumption_record(**kwargs)

        self.assertTrue(first["saved"])
        self.assertEqual(second["record_id"], "rec_created")
        self.assertEqual(self.service._create_record_fields.call_count, 1)
        written = self.service._create_record_fields.call_args.kwargs["fields"]
        self.assertEqual(
            written["水表照片"],
            [{"file_token": "photo-token-1"}],
        )

    def test_staged_image_is_uploaded_as_bitable_image(self) -> None:
        image = Image.new("RGB", (24, 24), "#1e63b7")
        buffer = io.BytesIO()
        image.save(buffer, "PNG")
        staged = self.store.put_notice_upload_attachment(
            open_id="ou_test",
            file_name="meter.png",
            mime_type="image/png",
            content=buffer.getvalue(),
            ttl_seconds=3600,
        )
        self.service._upload_drive_media_file = Mock(
            return_value="photo-token-uploaded"
        )

        tokens, used_ids = self.service._water_upload_staged_images(
            upload_ids=[staged["upload_id"]],
            open_id="ou_test",
        )

        self.assertEqual(tokens, ["photo-token-uploaded"])
        self.assertEqual(used_ids, [staged["upload_id"]])
        upload_call = self.service._upload_drive_media_file.call_args.kwargs
        self.assertEqual(upload_call["parent_type"], "bitable_image")
        self.assertEqual(
            upload_call["parent_node"],
            portal_service_module.WATER_CONSUMPTION_APP_TOKEN,
        )
        self.assertEqual(upload_call["file_name"], "meter.png")

    def test_update_retains_existing_photo_and_appends_uploaded_photo(self) -> None:
        existing = _record("rec_update", "A", "2026-07-28")
        self.store.replace_water_consumption_snapshot(
            app_token="app",
            table_id="table",
            fields=[],
            options={},
            records=[existing],
        )
        updated = {
            **existing,
            "version": "2",
            "meter_value": 202.0,
            "photos": [
                *existing["photos"],
                {
                    "image_id": "image_new",
                    "file_token": "photo-token-new",
                    "name": "new.jpg",
                    "mime_type": "image/jpeg",
                    "size": 20,
                },
            ],
        }
        self.service._water_remote_record = Mock(
            side_effect=[existing, updated]
        )
        self.service._water_upload_staged_images = Mock(
            return_value=(["photo-token-new"], ["upload-new"])
        )
        self.service._patch_record_fields_exact = Mock()

        result = self.service._update_water_consumption_record_unlocked(
            "rec_update",
            scope="A",
            meter="东区水表-总",
            frequency="日",
            shift="白",
            statistic_date="2026-07-28",
            meter_value="202",
            corrected_usage=None,
            retained_image_ids=["image_rec_update"],
            upload_ids=["upload-new"],
            expected_version="1",
            operation_id="water-update-op",
            operator_open_id="ou_test",
        )

        self.assertTrue(result["saved"])
        written = self.service._patch_record_fields_exact.call_args.kwargs["fields"]
        self.assertEqual(
            written["水表照片"],
            [
                {"file_token": "token_rec_update"},
                {"file_token": "photo-token-new"},
            ],
        )


class WaterConsumptionImageProxyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.store = LanPortalStateStore(
            Path(self.tempdir.name) / "water_image.sqlite3"
        )
        self.store.replace_water_consumption_snapshot(
            app_token="app",
            table_id="table",
            fields=[],
            options={},
            records=[_record("rec_photo", "A", "2026-07-28")],
        )
        image = Image.new("RGB", (40, 60), "#1e63b7")
        buffer = io.BytesIO()
        image.save(buffer, "JPEG")
        self.image_bytes = buffer.getvalue()
        self.service = MaintenancePortalService.__new__(MaintenancePortalService)
        self.service._state_store = self.store
        self.service._water_consumption_image_cache_lock = threading.RLock()
        self.service._water_consumption_image_http_client = Mock()
        self.service._water_consumption_image_http_client.request_bytes.return_value = (
            self.image_bytes,
            "image/jpeg",
        )
        self.service._auth_headers = Mock(return_value={"Authorization": "Bearer test"})

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def test_image_proxy_uses_bitable_download_url_and_builds_thumbnail(self) -> None:
        cache_root = Path(self.tempdir.name) / "runtime"
        with patch.object(
            portal_service_module,
            "get_data_file_path",
            side_effect=lambda name: str(cache_root / name),
        ):
            thumb, content_type, file_name = (
                self.service.get_water_consumption_image_bytes(
                    "image_rec_photo",
                    scope="A",
                    variant="thumb",
                )
            )
            original, original_type, _ = (
                self.service.get_water_consumption_image_bytes(
                    "image_rec_photo",
                    scope="A",
                    variant="original",
                )
            )

        request = (
            self.service._water_consumption_image_http_client.request_bytes.call_args
        )
        self.assertEqual(
            request.args[1],
            "https://example.test/rec_photo.jpg",
        )
        self.assertEqual(content_type, "image/jpeg")
        self.assertEqual(original_type, "image/jpeg")
        self.assertEqual(original, self.image_bytes)
        self.assertTrue(file_name.endswith("_缩略图.jpg"))
        with Image.open(io.BytesIO(thumb)) as loaded:
            self.assertEqual(loaded.size, (360, 240))

    def test_old_snapshot_resolves_download_url_from_remote_record(self) -> None:
        self.service._request_json = Mock(
            return_value={
                "data": {
                    "record": {
                        "fields": {
                            "水表照片": [
                                {
                                    "file_token": "old-token",
                                    "url": "https://example.test/bitable-download",
                                }
                            ]
                        }
                    }
                }
            }
        )
        url = self.service._water_image_download_url(
            {
                "record_id": "rec_old",
                "file_token": "old-token",
            }
        )
        self.assertEqual(url, "https://example.test/bitable-download")


if __name__ == "__main__":
    unittest.main()
