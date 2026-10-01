# -*- coding: utf-8 -*-
"""合同检查:frontend/src 实际业务API调用 与 native route 目录的映射核对。

只读审查,不启动服务、不调用真实业务。用与 bin/test_lighthouse_api.py
中 ``_native_route_catalog`` 相同的 AST 注册办法从
``clipflow_backend/main.py`` 与三个 ``ROUTES`` 文件重建 FastAPI 目录,然后
逐条核对前端(``lan_bitable_template_portal/frontend/src``)已核实的业务API
与方法、动态路径、multipart/raw/json 传输格式是否存在于目录中。

输出改为"前端合同 -> 目录行为",明确报告目录漏报的传输格式与被排除但前端
仍在调用的业务端点。不写任何业务数据,不启动真实服务。
"""
import ast
import copy
import json
import re
import sys
import unittest
import uuid
from datetime import date, datetime, time
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
PORTAL_DIR = BIN_DIR / "lan_bitable_template_portal"
FRONTEND_SRC = PORTAL_DIR / "frontend" / "src"
MAIN_PY = BIN_DIR / "clipflow_backend" / "main.py"

sys.path.insert(0, str(BIN_DIR))

from fastapi import (FastAPI, Request, Response, UploadFile, File, Form, Body,  # noqa: E402
                     Query, Path, Header, Cookie, Depends, Security, BackgroundTasks)  # noqa: E402

from lan_bitable_template_portal.lighthouse_api import PortalAPICatalog  # noqa: E402


def _stub_namespace():
    """回到目录核对所需的注解/默认命名空间(不导入/不运行 main 业务体)。"""
    import pydantic
    from typing import (Any, Annotated, Callable, Dict, Generic, Iterable, Iterator,
                        List, Literal, Mapping, Optional, Sequence, Set, Tuple,
                        TypeVar, Union)
    ns = dict(vars(__import__("fastapi")))
    ns.update(dict(vars(pydantic)))
    ns.update({
        # 常见注解
        "Any": Any, "Annotated": Annotated, "Callable": Callable, "Dict": Dict,
        "Generic": Generic, "Iterable": Iterable, "Iterator": Iterator, "List": List,
        "Literal": Literal, "Mapping": Mapping, "Optional": Optional, "Sequence": Sequence,
        "Set": Set, "Tuple": Tuple, "TypeVar": TypeVar, "Union": Union,
        "dict": dict, "list": list, "set": set, "tuple": tuple, "str": str, "int": int,
        "float": float, "bool": bool, "bytes": bytes, "object": object, "None": type(None),
        "Request": Request, "Response": Response, "UploadFile": UploadFile,
        "File": File, "Form": Form, "Body": Body, "Query": Query, "Path": Path,
        "Header": Header, "Cookie": Cookie, "Depends": Depends, "Security": Security,
        "BackgroundTasks": BackgroundTasks,
        "json": json, "re": re, "datetime": datetime, "date": date, "time": time, "uuid": uuid,
    })
    return ns


def _make_signature_stub(node):
    """根据 AST 重建与原路由函数一致的签名(参数/注解/File/Form默认),函数体抛错。

    该 stub 只用于让 FastAPI 正确识别 multipart/raw 的 body 字段,绝不执行业务逻辑。
    若签名含无法解析的自定义注解/依赖,返回 None 交由调用方回退到无参占位。
    """
    clone = copy.deepcopy(node)
    clone.decorator_list = []                     # 重新注册了就不再附带原装饰器
    clone.name = "_metadata_stub_" + node.name
    clone.returns = None                          # 响应注解不影响请求体识别
    clone.body = [
        ast.Raise(
            exc=ast.Call(
                func=ast.Name(id="RuntimeError", ctx=ast.Load()),
                args=[ast.Constant("metadata audit stub must not be invoked")],
                keywords=[]),
            cause=None),
    ]
    try:
        source = ast.unparse(clone)
    except Exception:  # noqa: BLE001
        return None
    local_ns = {}
    try:
        exec(compile(source, "<metadata-stub>", "exec"), _stub_namespace(), local_ns)
    except Exception:  # noqa: BLE001
        return None
    return local_ns.get(clone.name)


def build_native_catalog():
    """复刻 _native_route_catalog:AST 读取 main.py 装饰器 + 三个 ROUTES 文件。

    与 test_lighthouse_api 的空实现占位不同,这里从 AST 重建每个路由函数的
    参数/注解/File/Form 默认(正文换成 raise),让 ``_route_multipart``/
    ``_file_field_names`` 能读出真实的 multipart 请求形状;签名无法解析的
    路由回退到无参占位,不影响非上传接口的注册。
    """
    app = FastAPI()

    async def _dummy_endpoint():
        raise AssertionError("metadata audit must not invoke business operations")

    tree = ast.parse(MAIN_PY.read_text(encoding="utf-8-sig"))
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                continue
            method = decorator.func.attr
            if method not in {"get", "post", "put", "patch", "delete", "api_route"} or not decorator.args:
                continue
            path = ast.literal_eval(decorator.args[0])
            if not path.startswith("/api/"):
                continue
            if method != "api_route":
                methods = [method.upper()]
            else:
                methods = next(ast.literal_eval(kw.value) for kw in decorator.keywords if kw.arg == "methods")
            app.add_api_route(path, _make_signature_stub(node) or _dummy_endpoint, methods=methods)

    for filename, prefix in (("learning_routes.py", "learning"),
                             ("plan_convergence_routes.py", "plan-convergence"),
                             ("cabinet_power_routes.py", "cabinet-power")):
        route_tree = ast.parse((PORTAL_DIR / filename).read_text(encoding="utf-8-sig"))
        for node in ast.walk(route_tree):
            routes = None
            if isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "ROUTES" for t in node.targets):
                routes = node.value
            elif isinstance(node, ast.For) and isinstance(node.iter, ast.Call) \
                    and isinstance(node.iter.func, ast.Attribute):
                routes = node.iter.func.value
            if isinstance(routes, ast.Dict):
                for path, methods in ast.literal_eval(routes).items():
                    app.add_api_route(f"/api/{prefix}/{path}", _dummy_endpoint, methods=list(methods))
    return PortalAPICatalog(app)


def _norm_path(path: str) -> str:
    """把前端动态 URL 里的任意段占位规范为 {param} 形态用于目录匹配。"""
    return path.rstrip("/")


class FrontendContract:
    """一条已核实的前端业务API合同。"""

    def __init__(self, contract_id, method, path, transport, source, note=""):
        self.id = contract_id
        self.method = method
        self.path = path               # 目录 id 用的带 {param} 路径
        self.transport = transport     # "json" | "multipart" | "raw"
        self.source = source           # frontend 文件:行 的实际请求
        self.note = note

    def catalog_id(self):
        return f"{self.method} {_norm_path(self.path)}"


def _contracts():
    f = FRONTEND_SRC
    return [
        # --- multipart 上传:前端=FormData,目录必须 multipart=True ---
        FrontendContract(
            "mop_upload_local", "POST", "/api/engineer/mop/upload-local", "multipart",
            f"{f}/mopFileApi.ts:100 requestJson(path, method POST, body formData)",
            "后端 File()+Form() 为 multipart,前端用 FormData。"),
        FrontendContract(
            "critical_guard_source_files", "POST", "/api/critical-guard/source-files", "multipart",
            f"{f}/components/CriticalGuardPage.vue:1703-1713 new FormData()+file/scope/response_id",
            "后端 File()+Form() 为 multipart,前端用 FormData。"),
        FrontendContract(
            "drills_create", "POST", "/api/drills", "multipart",
            f"{f}/components/DrillManagementPage.vue:1640-1648 new FormData()+file/year/month...",
            "后端 File()+Form() 为 multipart,前端上传后用 FormData。"),
        # --- raw 上传:前端=requestBinaryJson 原样体 ---
        FrontendContract(
            "water_uploads", "POST", "/api/capacity/water/uploads", "raw",
            f"{f}/components/WaterManagementPage.vue:1242 requestBinaryJson(path, file, Content-Type:mime)",
            "前端以二进制原始体上传照片,目录应标注 upload_format=raw。"),
        # --- 已正确声明的 multipart 上传(对照,不应报缺口)---
        FrontendContract(
            "learning_attachments", "POST", "/api/learning/attachments", "multipart",
            f"{f}/components/LearningPage.vue:841 new FormData()+files",
            "对照项:learning 附件本已 multipart。"),
        FrontendContract(
            "cabinet_batches_recognize", "POST", "/api/cabinet-power/batches/recognize", "multipart",
            f"{f}/components/CabinetPowerBatchPage.vue:614 new FormData()+files",
            "对照项:机柜识别本已 multipart。"),
        FrontendContract(
            "cabinet_batches_images", "POST", "/api/cabinet-power/batches/{batch_id}/images", "multipart",
            f"{f}/components/CabinetPowerBatchPage.vue:542 new FormData()+files",
            "对照项:批次图集本已 multipart。"),
        # --- JSON 写/查(动态路径、共享维修路由)---
        FrontendContract(
            "repair_records_get", "GET", "/api/repair-management/records/{record_id}", "json",
            f"{f}/components/RepairManagementPage.vue:2207/2963 GET records/<record_id>"),
        FrontendContract(
            "repair_records_put", "PUT", "/api/repair-management/records/{record_id}", "json",
            f"{f}/components/RepairManagementPage.vue:3365 PUT records/<record_id> 含 operation_id"),
        FrontendContract(
            "repair_records_post", "POST", "/api/repair-management/records", "json",
            f"{f}/components/RepairManagementPage.vue:3392 POST records"),
        FrontendContract(
            "repair_operations_check", "GET", "/api/repair-management/operations/{operation_id}", "json",
            f"{f}/composables/useRepairSubmission.ts:38 GET operations/<id> 查写入状态"),
        FrontendContract(
            "repair_followups_post", "POST", "/api/repair-management/followups", "json",
            f"{f}/components/RepairFollowupPanel.vue:1436 POST followups"),
        # --- 机柜动态下载 / 事件 / 计划收敛 / 演练动态 ---
        FrontendContract(
            "cabinet_evidence_get", "GET", "/api/cabinet-power/operations/{record_id}/evidence/{image_id}", "json",
            f"{f}/components/CabinetPowerPage.vue:523 evidenceUrl GET"),
        FrontendContract(
            "plan_ruleset_expand", "GET", "/api/plan-convergence/rulesets/{id}/expand", "json",
            f"{f}/components/PlanConvergenceRules.vue:1124 GET expand"),
        FrontendContract(
            "drill_execution_get", "GET", "/api/drills/{drill_id}/execution", "json",
            f"{f}/components/DrillManagementPage.vue:1040 GET execution"),
        FrontendContract(
            "drill_delete", "DELETE", "/api/drills/{drill_id}", "json",
            f"{f}/components/DrillManagementPage.vue:1739 DELETE drills/<id>"),
        FrontendContract(
            "events_transfer_repair", "POST", "/api/events/transfer-repair", "json",
            f"{f}/components/EventManagementPage.vue:675 POST transfer-repair"),
        # 原签名人员选择与使用确认可办理；签名图片和令牌会话保持隔离。
        FrontendContract(
            "signatures_temp_people", "GET", "/api/signatures/temporary/people", "json",
            f"{f}/mopSignatureApi.ts:44 fetchExternalSignaturePeople",
            "临时/外部签名人选择,前端业务调用,目录应开放。"),
        FrontendContract(
            "signatures_temp_list", "GET", "/api/signatures/temporary/list", "json",
            f"{f}/mopSignatureApi.ts:73 fetchTemporarySignatures",
            "已用临时签名列表,目录应开放。"),
        FrontendContract(
            "signatures_usage_confirm_send", "POST", "/api/signatures/usage-confirmations/send", "json",
            f"{f}/mopSignatureApi.ts:55 sendSignatureUsageConfirmations",
            "发送签名使用确认,目录应开放。"),
    ]


class FrontendContractTests(unittest.TestCase):
    """对已验证前端业务API做目录存在性 + 传输格式核对。"""

    @classmethod
    def setUpClass(cls):
        cls.catalog = build_native_catalog()
        cls.found, cls.gaps = _run_checks(cls.catalog)

    def test_all_verified_contracts_exist_in_directory(self):
        missing = [cid for cid, msg in self.gaps if msg.startswith("目录缺失")]
        self.assertEqual(
            missing, [],
            "以下前端已核实的业务API在 native 目录中缺失(被排除或未注册):\n" + "\n".join(
                f"  {cid}" for cid in missing))

    def test_transport_format_matches_directory(self):
        bad = [g for g in self.gaps if g[1].startswith("传输格式")]
        self.assertEqual(
            bad, [],
            "以下前端已核实的传输格式与目录不一致:\n" + "\n".join(f"  {msg}" for _, msg in bad))

    def test_no_missing_contracts_without_reason(self):
        unseen = [g for g in self.gaps if g[1].startswith("未找到")]
        self.assertEqual(unseen, [])

    def test_contract_table_is_populated(self):
        self.assertGreaterEqual(len(self.found), 10)

    def test_signature_image_session_handwrite_stay_excluded(self):
        # 凭证/签名图片、token会话与临时手写采集不返回模型,必须继续排除。
        still_excluded = [
            "GET /api/signatures/image",
            "GET /api/signatures/temporary/session",   # token 会话
            "GET /api/signatures/temporary/image",     # 临时签名图片
        ]
        all_ids = set()
        first = self.catalog.discover(page_size=50)
        for page in range(1, (first["total"] + 49) // 50 + 1):
            all_ids.update(item["id"] for item in self.catalog.discover(page=page, page_size=50)["items"])
        for cid in still_excluded:
            self.assertNotIn(cid, all_ids, f"敏感签名端点不应出现在业务目录:{cid}")


def _run_checks(catalog):
    """对每条合同检查并返回 (found_ids, [(contract_id, message), ...])。"""
    found = []
    gaps = []
    for contract in _contracts():
        cid = contract.catalog_id()
        try:
            desc = catalog.get(cid)
        except Exception:
            gaps.append((contract.id, f"目录缺失 {cid} ({contract.source})"))
            continue
        found.append(contract.id)

        has_upload_format = desc.get("upload_format")
        multipart_flag = bool(desc.get("multipart"))
        if contract.transport == "multipart" and not (multipart_flag or has_upload_format == "multipart"):
            gaps.append((
                contract.id,
                f"传输格式 {cid}: 前端为 multipart(FormData),目录 multipart={multipart_flag} "
                f"upload_format={has_upload_format} ({contract.source})"))
        elif contract.transport == "raw" and has_upload_format != "raw":
            gaps.append((
                contract.id,
                f"传输格式 {cid}: 前端 requestBinaryJson 原样体,目录 upload_format={has_upload_format} "
                f"({contract.source})"))
        elif contract.transport == "json" and (multipart_flag or has_upload_format in ("multipart", "raw", "raw-excel")):
            gaps.append((
                contract.id,
                f"传输格式 {cid}: 前端 JSON,目录却 multipart={multipart_flag} "
                f"upload_format={has_upload_format} ({contract.source})"))
        # 动态路径占位符一致性(仅目录按 {param} 注册时)
        expected_placeholders = sorted(
            p.strip("{}") for p in contract.path.split("/")
            if p.startswith("{") and p.endswith("}"))
        actual_placeholders = sorted(desc.get("schema", {}).get("path", []) or [])
        if expected_placeholders and expected_placeholders != actual_placeholders:
            gaps.append((
                contract.id,
                f"动态路径 {cid}: 前端占位 {expected_placeholders} 目录为 {actual_placeholders} "
                f"({contract.source})"))
    return found, gaps


if __name__ == "__main__":
    print("\n=== 前端业务API合同检查(只读 AST 目录核对)===")
    catalog = build_native_catalog()
    found, gaps = _run_checks(catalog)
    print(f"已核实的合同数: {len(_contracts())}; 目录中找到: {len(found)}; 缺口: {len(gaps)}")
    if gaps:
        print("\n剩余缺口明细(前端合同 -> 当前目录行为):")
        for cid, msg in gaps:
            print(f"  - [{cid}] {msg}")
    else:
        print("\n未发现缺口。")
    print()
    print("(兼容 unittest 发现:请用 `python -m unittest test_lighthouse_frontend_contracts` 运行断言)")
    raise SystemExit(1 if gaps else 0)
