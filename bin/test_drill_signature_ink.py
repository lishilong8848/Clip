# -*- coding: utf-8 -*-
"""Focused regression tests for the drill signature natural-ink change.

Synthetic-only: no real signatures, network, or cloud access. Reuses the
ready-drill helpers already defined in ``test_drill_management`` without
modifying that module.
"""

from __future__ import annotations

import io
import sys
import tempfile
import unittest
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from unittest.mock import Mock, patch

from PIL import Image, ImageDraw

BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from lan_bitable_template_portal.drill_management import (  # noqa: E402
    DrillManagementService,
    _EMU_PER_PIXEL,
    _XDR_NS,
    _is_drill_signature_anchor,
    _marker_pixels,
    _parse_workbook,
    _row_col_pixels,
    normalize_drill_signature_png,
)
from lan_bitable_template_portal.portal_service import (  # noqa: E402
    MaintenancePortalService,
    PortalError,
    SIGNATURE_KEY_FIELD,
    SIGNATURE_TABLE_ID,
    TEMP_SIGNATURE_KEY_FIELD,
    TEMP_SIGNATURE_TABLE_ID,
)
from lan_bitable_template_portal.signature_crypto import SignatureCryptoManager  # noqa: E402
from lan_bitable_template_portal.state_store import LanPortalStateStore  # noqa: E402

import test_drill_management as tdm  # noqa: E402


def _synthetic_natural_signature() -> bytes:
    """Signed white canvas with an opaque blue and a translucent gray stroke."""
    image = Image.new("RGBA", (200, 60), (255, 255, 255, 255))
    draw = ImageDraw.Draw(image)
    draw.rectangle((10, 10, 40, 50), fill=(30, 90, 200, 255))
    draw.rectangle((100, 10, 130, 50), fill=(140, 140, 140, 160))
    output = io.BytesIO()
    image.save(output, "PNG")
    return output.getvalue()


def _open_rgba(content: bytes) -> Image.Image:
    with Image.open(io.BytesIO(content)) as loaded:
        return loaded.convert("RGBA")


def _cell_origin_px(sheet: dict, row: int, col: int) -> tuple[float, float]:
    """Top-left pixel position of the cell at 1-based (row, col)."""
    marker = ET.Element(f"{{{_XDR_NS}}}from")
    for tag, value in (
        ("col", col - 1),
        ("colOff", 0),
        ("row", row - 1),
        ("rowOff", 0),
    ):
        ET.SubElement(marker, f"{{{_XDR_NS}}}{tag}").text = str(value)
    _row_zero, _col_zero, x, y = _marker_pixels(sheet, marker)
    return x, y


class DrillInkTests(unittest.TestCase):
    def _service(self, root: Path) -> DrillManagementService:
        return DrillManagementService(
            LanPortalStateStore(root / "state.sqlite3"),
            data_root=root / "drills",
        )

    def _assert_ink_preserved(self, content: bytes) -> None:
        rgba = _open_rgba(content)
        self.assertEqual(rgba.size, (200, 60))
        # Opaque white background preserved exactly.
        self.assertEqual(rgba.getpixel((4, 4)), (255, 255, 255, 255))
        # Blue and translucent gray strokes keep RGB and intermediate alpha.
        self.assertEqual(rgba.getpixel((25, 30)), (30, 90, 200, 255))
        self.assertEqual(rgba.getpixel((115, 30)), (140, 140, 140, 160))
        alpha_hist = rgba.getchannel("A").histogram()
        self.assertTrue(
            any(count > 0 for count in alpha_hist[1:255]),
            "natural ink must keep intermediate alpha values",
        )

    def _assert_anchors_within_cells(
        self, output: Path, definition: dict, execution: dict
    ) -> None:
        book = _parse_workbook(output)
        record_sheet = next(
            sheet for sheet in book["sheets"] if sheet["name"] == "本月记录"
        )
        placements = [
            item
            for item in tdm._derived_values(definition, execution).get(
                "signature_cells"
            )
            or []
            if str(item.get("sheet_type") or "record") == "record"
        ]
        self.assertTrue(placements)
        with zipfile.ZipFile(output) as archive:
            drawing_root = ET.fromstring(archive.read(record_sheet["drawing_path"]))
            anchors = [
                anchor
                for anchor in drawing_root
                if _is_drill_signature_anchor(anchor)
            ]
        self.assertTrue(anchors)

        cells = []
        for placement in placements:
            row, col, _r2, _c2, area_width, area_height = _row_col_pixels(
                record_sheet, str(placement["range"])
            )
            origin_x, origin_y = _cell_origin_px(record_sheet, row, col)
            cells.append(
                {
                    "name": placement["range"],
                    "left": origin_x,
                    "top": origin_y,
                    "right": origin_x + area_width,
                    "bottom": origin_y + area_height,
                }
            )

        for anchor in anchors:
            marker = anchor.find(f"{{{_XDR_NS}}}from")
            _row_zero, _col_zero, left, top = _marker_pixels(record_sheet, marker)
            ext = anchor.find(f"{{{_XDR_NS}}}ext")
            width_px = int(ext.attrib.get("cx") or 0) / _EMU_PER_PIXEL
            height_px = int(ext.attrib.get("cy") or 0) / _EMU_PER_PIXEL
            containers = [
                cell
                for cell in cells
                if cell["left"] - 0.01 <= left < cell["right"]
                and cell["top"] - 0.01 <= top < cell["bottom"]
            ]
            self.assertTrue(
                containers,
                f"anchor at ({left:.2f},{top:.2f}) is outside every signature cell",
            )
            cell = containers[0]
            with self.subTest(cell=cell["name"]):
                self.assertLessEqual(left + width_px, cell["right"] + 0.01)
                self.assertLessEqual(top + height_px, cell["bottom"] + 0.01)

    def test_generated_archive_preserves_natural_ink_within_signature_cells(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            service = self._service(Path(temporary))
            test_case = tdm.DrillManagementTests()
            definition, execution, _default = test_case._ready_drill(
                service, tdm._fixture_xlsx()
            )
            raw = _synthetic_natural_signature()
            normalized = normalize_drill_signature_png(raw)
            signatures = {f"person-{index}": raw for index in range(1, 5)}
            generated = service.generate(
                definition["drill_id"], "A", signatures=signatures
            )
            output = Path(generated["generated"]["path"])

            with zipfile.ZipFile(output) as archive:
                media_names = [
                    name
                    for name in archive.namelist()
                    if "drill_signature_" in name and name.endswith(".png")
                ]
                self.assertTrue(media_names, "expected embedded drill signatures")
                for media_name in media_names:
                    with self.subTest(media=media_name):
                        self.assertEqual(archive.read(media_name), normalized)
                        self._assert_ink_preserved(normalized)

            self._assert_anchors_within_cells(output, definition, execution)


class PortalInkForwardingTests(unittest.TestCase):
    def test_drill_signature_image_bytes_forwards_preserve_ink_for_staff(self) -> None:
        svc = object.__new__(MaintenancePortalService)
        with patch.object(
            svc, "signature_image_bytes", return_value=(b"staff-png", "image/png")
        ) as staff_mock:
            self.assertEqual(
                svc.drill_signature_image_bytes(record_id="staff-rec"),
                (b"staff-png", "image/png"),
            )
        staff_mock.assert_called_once_with(record_id="staff-rec", preserve_ink=True)

    def test_drill_signature_image_bytes_forwards_preserve_ink_for_external(self) -> None:
        svc = object.__new__(MaintenancePortalService)
        with patch.object(
            svc,
            "external_signature_image_bytes",
            return_value=(b"ext-png", "image/png"),
        ) as external_mock:
            self.assertEqual(
                svc.drill_signature_image_bytes(record_id="external:ext-rec"),
                (b"ext-png", "image/png"),
            )
        external_mock.assert_called_once_with(record_id="ext-rec", preserve_ink=True)

    def test_linked_staff_fallback_propagates_preserve_ink_to_external(self) -> None:
        svc = object.__new__(MaintenancePortalService)
        person = {"record_id": "staff-1", "has_signature": False, "raw_fields": {}}
        svc._load_signature_people = Mock(return_value=[person])
        fake_management = Mock()
        fake_management.directory.return_value = {
            "resolved": {
                "staff:staff-1": {
                    "source": "external",
                    "record_id": "ext-9",
                    "has_signature": True,
                }
            }
        }
        svc._signature_management = fake_management
        with patch.object(
            svc, "external_signature_image_bytes", return_value=(b"ext-png", "image/png")
        ) as external_mock:
            self.assertEqual(
                svc.signature_image_bytes(record_id="staff-1", preserve_ink=True),
                (b"ext-png", "image/png"),
            )
        external_mock.assert_called_once_with(record_id="ext-9", preserve_ink=True)


class DecoderInkTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self._temporary.cleanup)
        root = Path(self._temporary.name)
        self.plain = _synthetic_natural_signature()
        self.svc = object.__new__(MaintenancePortalService)
        self.svc._signature_crypto = SignatureCryptoManager(
            master_key_path=root / "secure" / "signature_master.key",
            cache_root=root / "signature_cache",
        )
        self.svc._mark_signature_crypto_migration = Mock()
        self.svc._maybe_migrate_plain_signature_async = Mock()
        self._transparent_patcher = patch.object(
            MaintenancePortalService,
            "_transparent_signature_png",
            return_value=b"PROCESSED-PNG",
        )
        self.transparent_mock = self._transparent_patcher.start()
        self.addCleanup(self._transparent_patcher.stop)

        self.encrypted, self.metadata = self.svc._signature_crypto.encrypt_signature(
            self.plain,
            self.svc._signature_crypto.build_aad(
                app_token="app-test",
                table_id=SIGNATURE_TABLE_ID,
                record_id="rec-ink",
                source="staff",
            ),
        )
        self.fields = {
            SIGNATURE_KEY_FIELD: SignatureCryptoManager.metadata_to_text(self.metadata)
        }
        self.kwargs = dict(
            record_id="rec-ink",
            fields=self.fields,
            attachment={},
            table_id=SIGNATURE_TABLE_ID,
            source="staff",
        )

    def test_preserve_ink_reads_download_once_and_keeps_processed_cache(self) -> None:
        sha = self.metadata["signature_sha256"]
        self.svc._signature_crypto.write_cache("rec-ink", sha, b"pre-seeded-processed")
        download = Mock(return_value=(self.encrypted, "image/png"))
        self.svc._download_mop_attachment = download
        read_kwargs = dict(self.kwargs, preserve_ink=True)

        first = self.svc._decode_signature_attachment_bytes(**read_kwargs)
        second = self.svc._decode_signature_attachment_bytes(**read_kwargs)

        self.assertEqual(first, self.plain)
        self.assertEqual(second, self.plain)
        download.assert_called_once()
        self.transparent_mock.assert_not_called()
        self.svc._maybe_migrate_plain_signature_async.assert_not_called()
        # Raw ink cached under the original_ prefix only.
        self.assertEqual(
            self.svc._signature_crypto.read_cache("original_rec-ink", sha), self.plain
        )
        # The pre-seeded processed cache is independent and left untouched.
        self.assertEqual(
            self.svc._signature_crypto.read_cache("rec-ink", sha),
            b"pre-seeded-processed",
        )

    def test_corrupt_raw_raises_instead_of_using_stale_processed_cache(self) -> None:
        sha = self.metadata["signature_sha256"]
        self.svc._signature_crypto.write_cache("rec-ink", sha, b"stale-processed-png")
        corrupt = self.encrypted[:-1] + bytes([self.encrypted[-1] ^ 1])
        self.svc._download_mop_attachment = Mock(return_value=(corrupt, "image/png"))

        with self.assertRaises(PortalError):
            self.svc._decode_signature_attachment_bytes(
                **self.kwargs, preserve_ink=True
            )
        self.transparent_mock.assert_not_called()
        self.assertEqual(
            self.svc._signature_crypto.read_cache("rec-ink", sha),
            b"stale-processed-png",
        )

    def test_default_processed_path_omits_preserve_ink_and_uses_transparent_normalizer(
        self,
    ) -> None:
        sha = self.metadata["signature_sha256"]
        self.svc._download_mop_attachment = Mock(return_value=(self.encrypted, "image/png"))
        result = self.svc._decode_signature_attachment_bytes(
            record_id="rec-processed",
            fields={
                TEMP_SIGNATURE_KEY_FIELD: SignatureCryptoManager.metadata_to_text(
                    self.metadata
                )
            },
            attachment={},
            table_id=TEMP_SIGNATURE_TABLE_ID,
            source="external",
        )

        self.assertEqual(result, b"PROCESSED-PNG")
        self.transparent_mock.assert_called_once_with(self.plain)
        self.assertEqual(
            self.svc._signature_crypto.read_cache("rec-processed", sha),
            b"PROCESSED-PNG",
        )
        self.assertIsNone(
            self.svc._signature_crypto.read_cache("original_rec-processed", sha)
        )


if __name__ == "__main__":
    unittest.main()