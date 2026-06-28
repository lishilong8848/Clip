# -*- coding: utf-8 -*-
from __future__ import annotations

import sys
from pathlib import Path


BIN_DIR = Path(__file__).resolve().parents[1]
PROJECT_ROOT = BIN_DIR.parent


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def _require_marker(path: Path, marker: str, label: str, failures: list[str]) -> None:
    text = _read(path)
    if marker not in text:
        failures.append(f"{label}: 缺少 {marker!r} ({path.relative_to(PROJECT_ROOT)})")


def main() -> int:
    failures: list[str] = []
    main_py = BIN_DIR / "clipflow_backend" / "main.py"
    package_portable_py = PROJECT_ROOT / "package_portable.py"
    state_store_py = BIN_DIR / "lan_bitable_template_portal" / "state_store.py"
    portal_service_py = BIN_DIR / "lan_bitable_template_portal" / "portal_service.py"
    workbench_lite_py = BIN_DIR / "lan_bitable_template_portal" / "workbench_lite.py"
    admin_status_vue = (
        BIN_DIR
        / "lan_bitable_template_portal"
        / "frontend"
        / "src"
        / "components"
        / "AdminStatusPane.vue"
    )
    engineer_mop_vue = (
        BIN_DIR
        / "lan_bitable_template_portal"
        / "frontend"
        / "src"
        / "components"
        / "EngineerMopPage.vue"
    )

    checks = [
        (main_py, '@app.get("/api/backend/consistency")', "管理员一致性诊断接口"),
        (main_py, '@app.get("/api/backend/notice-diagnostic")', "管理员通告链路自检接口"),
        (main_py, '@app.post("/api/backend/notice-projection-repair")', "管理员本地投影修复接口"),
        (main_py, "def _backend_consistency_snapshot", "Qt/网页一致性快照"),
        (main_py, "def _sync_qt_active_item_upload_result", "Qt 上传成功后回填目标记录"),
        (main_py, "record_id_aliases", "Qt 目标记录回填保留旧 ID 别名"),
        (state_store_py, "def cleanup_mop_temporary_signature_sessions", "MOP 临时签名清理"),
        (portal_service_py, "def _verify_mop_source_record_update", "MOP 上传后源表校验"),
        (portal_service_py, '"verification": verification', "MOP 上传接口返回校验结果"),
        (workbench_lite_py, "setFormSubmitBusy", "工作台提交按钮整体锁定"),
        (workbench_lite_py, "appliedOngoingLocally = isRow && navLink.matches('.ongoing-row')", "进行中通告本地切换"),
        (workbench_lite_py, "现场照片", "通告结束现场照片提示"),
        (workbench_lite_py, "mop-action-panel", "维保 MOP 后续动作入口"),
        (admin_status_vue, "Qt/网页一致性", "管理员页一致性展示"),
        (admin_status_vue, "通告链路自检", "管理员页通告链路自检展示"),
        (admin_status_vue, "修复本地映射", "管理员页本地映射修复按钮"),
        (engineer_mop_vue, "uploadSignedMop", "MOP 已签名上传接口"),
        (package_portable_py, "notice_flow_smoke.py", "打包前通告链路烟测"),
    ]
    for path, marker, label in checks:
        if not path.is_file():
            failures.append(f"{label}: 文件不存在 {path.relative_to(PROJECT_ROOT)}")
            continue
        _require_marker(path, marker, label, failures)

    if failures:
        print("[NoticeFlowSmoke] FAIL")
        for item in failures:
            print(f"- {item}")
        return 1
    print("[NoticeFlowSmoke] OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
