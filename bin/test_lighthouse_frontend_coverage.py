# -*- coding: utf-8 -*-
"""全量只读前端API覆盖证据:frontend/src *.vue/*.ts + Python渲染页面。

目标
----
对 ``bin/lan_bitable_template_portal/frontend/src`` 下除助手自身与静态资源外的
全部 ``.vue``/``.ts`` 文件,以及 Python 渲染的工作台/签名/轮巡页面
(``workbench_lite.py``、``server.py``),抽取实际调用的 ``/api/...`` 路径与方法
(requestJson/requestBinaryJson/downloadFile/requestLearning/fetch/form action),
留存文件+行号证据与不能解析的动态表达式,再与 native route 目录
(复用 ``test_lighthouse_frontend_contracts.build_native_catalog`` 的 AST 重建目录)
按“方法 + 路径段/占位符形态”比对。

分类
----
- covered  : 前端实际调用在目录中存在(方法+路径形态一致)
- missing  : 前端调用在目录中缺失(业务接口或目录漏注册,需人工复核)
- excluded : 目录有意排除的系统/登录/Qt/原始签名图片/工单执行轮巡等端点
              前端若调用它们,不计入“覆盖”,单列说明
- unresolved: 路径为变量/三元/拼接且本静态审计无法可靠解析,留待人工复核

约束
----
- 只读审计,不启动服务、不写业务数据、不读取凭据。
- CODE 只修改 ``bin/test_lighthouse_frontend_coverage.py``。
- 不导入/不运行生产业务模块,仅用 AST 目录夹具。
"""
import hashlib
import json
import re
import sys
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
PORTAL_DIR = BIN_DIR / "lan_bitable_template_portal"
FRONTEND_SRC = PORTAL_DIR / "frontend" / "src"

sys.path.insert(0, str(BIN_DIR))

from test_lighthouse_frontend_contracts import build_native_catalog  # noqa: E402

#: 前端的传输函数调用(不含 transport 层内部定义)。requestLearning 为学练专用封装。
TRANSPORT_CALLS = ("requestJson", "requestBinaryJson", "downloadFile", "requestLearning", "fetch")

#: 目录有意排除而前端/页面仍会调用的端点前缀(不应计入“缺失”)。
#: 与 lighthouse_api._EXCLUDED_PREFIXES/_SIGNATURE_EXCLUDED_PREFIXES 对齐:
#: 登录、系统后端、Qt 投影流、浏览器事件流、轮巡工单执行、计划收敛的
#: settings/browser-login(浏览器登录凭证管理),以及原始签名/令牌会话/保存/使用确认
#: 等保留给原生界面的流程。
EXCLUDED_PREFIXES = (
    "/api/polling-work-orders",          # 用户明确:工单执行保持原接口,助手目录不暴露
    "/api/auth",                         # 登录/登出
    "/api/qt-active-items",              # Qt 投影流
    "/api/repair-management/stream",     # 浏览器事件流(非业务可调目录)
    "/api/plan-convergence/settings",    # 浏览器登录/凭证管理(目录排除)
    "/api/signatures/save",              # 原始签名保存流程
    "/api/signatures/usage-confirm",     # 原生使用确认页
    "/api/signatures/usage-confirmations",  # 发送/确认流程(Codex 待开 send)
    "/api/signatures/send-link",         # 临时发送链接
    "/api/signatures/management/request",
    "/api/signatures/management/submit",
    "/api/signatures/management/temporary",
    "/api/signatures/external",          # 外部跨上下文写入
    "/api/signatures/temporary/",        # 临时签名/令牌会话(management/原始/保存等)
    "/api/signatures/temp",
)
# 原始签名图片/令牌会话/手写采集仍不返回模型(目录 _SIGNATURE_EXCLUDED_PREFIXES)。
EXCLUDED_EXACT = (
    "/api/signatures/image",
    "/api/signatures/temporary/session",
    "/api/signatures/temporary/image",
    "/api/signatures/temporary/handwrite",
)

#: 目录当前排除、但 Codex 已明确要开放给助手模型的 3 个签名业务端点
#: (见 lighthouse_api._SIGNATURE_BUSINESS_PATHS)。审计单独列 reserved,不当作缺失,
#: 也不静默当已覆盖。
PENDING_OPEN_CODEX = (
    "/api/signatures/temporary/people",
    "/api/signatures/temporary/list",
    "/api/signatures/usage-confirmations/send",
)

#: Python 渲染的页面来源(工作台 + 签名使用确认页面)。这些文件里的嵌入 JS/表单
#: 是浏览器真实请求,属于“Python 渲染页面”审计范围。
PY_PAGE_SOURCES = (
    (PORTAL_DIR / "workbench_lite.py", "workbench-lite"),
    (PORTAL_DIR / "server.py", "server-signature-usage"),
)

#: 传输层封装文件(定义 requestJson/requestLearning/downloadFile 等),内部路径为参数
#: 占位,不代表前端业务端点,不纳入端点抽取(避免噪声)。这类文件的固定 /api/auth/login
#: 属登录/系统排除类,不影响业务覆盖结论。
TRANSPORT_SKIPS = {
    "api/client.ts",
    "api/learning.ts",
}


# ---------------------------------------------------------------------------
# 低层文本工具
# ---------------------------------------------------------------------------
def _scan_call_spans(text: str, fn_name: str):
    """扫描 ``fn_name(`` 的所有调用,返回 (start, open_idx, end_idx) 列表。

    end_idx 是匹配的右括号下标;调用体为 text[open_idx+1:end_idx]。
    """
    out = []
    for m in re.finditer(r"(?<![A-Za-z0-9_])%s\s*\(" % re.escape(fn_name), text):
        start = m.start()
        open_idx = m.end() - 1
        depth = 0
        i = open_idx
        n = len(text)
        while i < n:
            ch = text[i]
            if ch in "\"'`":
                close = {'"': '"', "'": "'", "`": "`"}[ch]
                # 跳过字符串字面量与模板,含转义
                while i < n:
                    i += 1
                    if i >= n:
                        break
                    if text[i] == "\\":
                        i += 1
                        continue
                    if text[i] == close:
                        break
                i += 1
                continue
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth == 0:
                    out.append((start, open_idx, i))
                    break
            i += 1
    return out


def _top_level_split(s: str, sep=","):
    parts, depth, out = [], 0, []

    def flush():
        cur = "".join(parts)
        if cur.strip():
            out.append(cur)
        parts.clear()

    i = 0
    n = len(s)
    while i < n:
        ch = s[i]
        if ch in "\"'`":
            close = {'"': '"', "'": "'", "`": "`"}[ch]
            parts.append(ch)
            i += 1
            while i < n:
                if s[i] == "\\":
                    parts.append(s[i])
                    if i + 1 < n:
                        parts.append(s[i + 1])
                    i += 2
                    continue
                parts.append(s[i])
                if s[i] == close:
                    i += 1
                    break
                i += 1
            continue
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == sep and depth == 0:
            flush()
        else:
            parts.append(ch)
        i += 1
    flush()
    return out


def _strip_quotes(s: str) -> str:
    s = s.strip()
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "\"'`":
        body = s[1:-1]
        # 简单反斜杠转义(仅常见,不追求完美)
        body = body.replace("\\\\", "\\").replace('\\"', '"').replace("\\'", "'")
        return body
    return s


def _strip_query(path: str) -> str:
    """去掉 ? 后面的查询串(目录 id 不含查询;source-refresh 之类以路径形态注册)。"""
    if "?" in path:
        path = path.split("?", 1)[0]
    return path


def _template_to_shape(path: str) -> str:
    """把 JS 模板里的 ${...} / Jinja {{...}} 统一成 {param};再去查询串。"""
    path = path.strip()
    # 覆盖 bash/JS 模板占位 ${...} 与 Python 字符串里的 ${{...}}(Jinja 转义)
    path = re.sub(r"\$\{([^{}]*)\}", "{param}", path)
    path = re.sub(r"\$\{\{([^{}]*)\}\}", "{param}", path)
    # 类 Jinja {{ var }} (不经 $) 也可能出现在内嵌 JS
    path = re.sub(r"\{\{[^{}]*\}\}", "{param}", path)
    path = _strip_query(path)
    return path.replace("//", "/")


def _normalize_frontend_path(path: str, base: str = "") -> str:
    """规范化前端路径为 `/(api)/...` 且不含查询串、占位符统一为 {param}。"""
    if base:
        path = base.rstrip("/") + "/" + path.lstrip("/")
    path = path.strip()
    if not path.startswith("/api/"):
        path = "/api/" + path.lstrip("/")
    return _template_to_shape(path)


def _segments(path: str):
    return [s for s in path.rstrip("/").split("/") if s]


def _shape_equal(left: str, right: str) -> bool:
    """按“方法+路径段/占位符形态”比较:静态段相等,{x} 任意占位段互配。"""
    lsegs, rsegs = _segments(left), _segments(right)
    if len(lsegs) != len(rsegs):
        return False
    for a, b in zip(lsegs, rsegs):
        adyn = a.startswith("{") and a.endswith("}")
        bdyn = b.startswith("{") and b.endswith("}")
        if adyn or bdyn:
            if not (adyn or bdyn):
                return False
            continue
        if a != b:
            return False
    return True


# ---------------------------------------------------------------------------
# 前端调用抽取 (Vue / TS)
# ---------------------------------------------------------------------------
def _is_quoted_literal(expr: str) -> bool:
    expr = expr.strip()
    return (len(expr) >= 2 and expr[0] in "\"'`" and expr[-1] == expr[0]
            and (expr[0] != "`" or "`" not in expr[1:-1].replace("\\`", "")))


def _resolve_concat(expr: str, file_text: str, base: str = ""):
    """解析简单 ``base + '/sub' + var`` 拼接路径:首项若是可解析常量则继续拼字面量,
    变量/函数调用段归一为 ``{param}``。无法可靠静态拼时返回 (None, reason)。"""
    parts = _top_level_split(expr, sep="+")
    if not parts:
        return None, f"拼接为空:{expr[:60]}"
    resolved_segments = []
    first = parts[0].strip()
    # 首段必须是含 /api/ 的字面量或可解析标识符
    if first[:1] in "\"'`":
        seg = _strip_quotes(first)
    else:
        assigned = _find_assign_of(first, file_text)
        if not assigned:
            return None, f"拼接首项 {first} 无法解析"
        seg = _strip_quotes(assigned)
    if "/api/" not in seg:
        return None, f"拼接无 /api/ 前缀:{expr[:60]}"
    resolved_segments.append(seg)
    for operand in parts[1:]:
        op = operand.strip()
        if op[:1] in "\"'`":
            resolved_segments.append(_strip_quotes(op))
        elif re.fullmatch(r"[A-Za-z_$][\w$]*", op):
            assigned = _find_assign_of(op, file_text)
            if assigned and assigned[:1] in "\"'`":
                resolved_segments.append(_strip_quotes(assigned))
            else:
                # 变量(如 op, id)视作占位
                resolved_segments.append("{param}")
        else:
            # encodeURIComponent(...) 之类 → 占位
            resolved_segments.append("{param}")
    joined = "".join(resolved_segments)
    norm = _normalize_frontend_path(joined, base)
    if not norm or "/api/" not in norm:
        return None, f"拼接结果不可用:{expr[:60]}"
    return norm, ""


def _resolve_ternary(expr: str, file_text: str, base: str = ""):
    """二元/三元 两分支若解析到同一路径则收敛;否则 unresolved。"""
    branches = re.split(r"\s*\?\s*|\s*:\s*", expr)
    norms = []
    for b in branches:
        b = b.strip()
        if not b:
            continue
        res = _resolve_simple_path(b, file_text, base)
        if res[0]:
            norms.append(res[0])
    if len(norms) >= 2 and len(set(norms)) == 1:
        return norms[0], ""
    if len(branches) == 1:
        return None, "无法解析的三元片段"
    return None, "三元两分支未收敛到同一路径"


def _resolve_simple_path(expr: str, file_text: str, base: str = ""):
    """尽力解析常见路径表达式。返回 (norm_path, evidence_extra) 或 (None, reason)。

    支持:
      - 字符串/模板字面量
      - 简单字符串拼接 `base + '/sub' + var`(base/var 可解析或归为占位)
      - 简单标识符 `const path = <expr>`(全文件唯一含 /api/ 的赋值)
      - 简单函数 `buildRecordsUrl()` -> `function buildRecordsUrl() {...}`
      - 两个分支收敛到同一路径的三元
    其余 -> 无法解析(undefined/unresolved)。
    """
    expr = expr.strip()
    # 完整字面量
    if _is_quoted_literal(expr):
        literal = _strip_quotes(expr)
        if "${" in literal:
            # 模板内 ${var} 若能解析到唯一字面量则代入,否则保持 {param}。
            literal = _substitute_template_vars(literal, file_text)
        return _normalize_frontend_path(literal, base), ""
    # 简单拼接
    if "+" in expr:
        return _resolve_concat(expr, file_text, base)
    # 三元:两分支同路则收敛
    if "?" in expr or ":" in expr:
        if re.search(r"\?\s*['\"`]|\?\s*.{0,3}['\"`]", expr):
            return _resolve_ternary(expr, file_text, base)
        return None, f"无法解析的三元表达式:{expr[:90]}"
    # 简单标识符
    if re.fullmatch(r"[A-Za-z_$][\w$]*", expr):
        candidates = _find_assign_of(expr, file_text)
        if candidates:
            return _resolve_simple_path(candidates, file_text, base) if candidate_is_literal(candidates) else (
                None, f"变量 {expr} 的赋值不是字面量"
            )
        return None, f"变量 {expr} 未找到字面量赋值"
    # 简单函数 buildRecordsUrl()
    m = re.fullmatch(r"([A-Za-z_$][\w$]*)\s*\(\s*\)", expr)
    if m:
        fn_body = _find_function_return(m.group(1), file_text)
        if fn_body:
            return _resolve_simple_path(fn_body, file_text, base) if candidate_is_literal(fn_body) else (
                None, f"函数 {m.group(1)} 返回值不是字面量"
            )
        return None, f"函数 {m.group(1)} 未找到"
    # 复杂表达式 -> unresolved
    return None, f"无法解析的表达式:{expr[:90]}"


def _find_assign_of(name: str, file_text: str) -> str:
    """在文件文本中找 `name = <expr>`(const/let/var),若唯一含 /api/ 则返回表达式。"""
    found = []
    for m in re.finditer(r"\b(?:const|let|var)\s+%s\s*=\s*([^;]+)" % re.escape(name),
                         file_text):
        val = m.group(1).strip()
        if "/api/" in val:
            found.append(val)
    if len(found) == 1:
        return found[0].strip()
    return ""


def candidate_is_literal(expr: str) -> bool:
    expr = expr.strip()
    return expr[:1] in "\"'`" or "+" in expr or "?" in expr


def _substitute_template_vars(literal: str, file_text: str) -> str:
    """模板字面量里 ${var} 若可解析到唯一字面量则代入,否则保留原样交给 {param} 归一。"""
    def _sub(m):
        name = m.group(1).strip()
        if not re.fullmatch(r"[A-Za-z_$][\w$]*", name):
            return m.group(0)
        assigned = _find_assign_of(name, file_text)
        if assigned and assigned[:1] in "\"'`":
            return _strip_quotes(assigned)
        return m.group(0)
    return re.sub(r"\$\{([^{}]*)\}", _sub, literal)


def _find_function_return(name: str, file_text: str) -> str:
    """找 `function name(...) [...]) { ... return <expr>; ... }` 中的首个 /api/ 返回表达式。"""
    m = re.search(r"\bfunction\s+%s\s*\([^)]*\)[^{}]*\{" % re.escape(name), file_text)
    if not m:
        return ""
    start = m.end()
    # m.end() 已在函数体的左花括号之后,故起始深度为 1。
    depth = 1
    i = start
    n = len(file_text)
    while i < n:
        ch = file_text[i]
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                break
        i += 1
    body = file_text[start:i]
    for ret in re.finditer(r"\breturn\s+([^;]+)", body):
        val = ret.group(1).strip()
        if "/api/" in val and (val[:1] in "\"'`" or "+" in val or "?" in val):
            return val
    return ""


def _find_balanced_brace(seg: str) -> str:
    """返回从首字符到配对右花括号前的内容(seg 起点已在左花括号之后,故起始深度为 1)。"""
    depth = 1
    for i, ch in enumerate(seg):
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return seg[:i]
    return ""


def _method_from_options(rest: str, default: str, file_text: str = "") -> str:
    m = re.search(r"\bmethod\s*:\s*['\"]([A-Za-z]+)['\"]", rest)
    if m:
        return m.group(1).upper()
    # 三元方法 id? "PUT":"POST" -> unresolved
    if re.search(r"\bmethod\s*:\s*[^,]{0,40}\?", rest):
        return "unresolved"
    # 未内联方法:尝试解析尾随的 options 变量(如 const options={method:'DELETE'})。
    if file_text:
        tail = rest.rstrip()
        mm = re.search(r",\s*([A-Za-z_$][\w$]*)\s*$", tail)
        if mm:
            var = mm.group(1)
            am = re.search(
                r"\b(?:const|let|var)\s+" + var + r"\s*=\s*\{",
                file_text)
            if am:
                # 只在该对象字面量的右花括号内找 method,避免泄漏到后面无关的 options。
                seg = file_text[am.end():am.end() + 4000]
                close = _find_balanced_brace(seg)
                vm = re.search(r"\bmethod\s*:\s*['\"]([A-Za-z]+)['\"]",
                               close)
                if vm:
                    return vm.group(1).upper()
                # 对象可解析但无 method 键 → JavaScript fetch/requestJson 默认方法即生效。
                return default
            # options 以变量传入但无法定位来源 → 未知,不得臆断默认方法。
            return "unresolved"
    return default


def _transport_from_body(body_expr: str, fn: str, file_text: str = "") -> str:
    if fn == "requestBinaryJson":
        return "raw"
    if fn == "downloadFile":
        return "download"
    if "new FormData(" in body_expr or re.search(r"\bFormData\b", body_expr):
        return "multipart"
    if file_text:
        # body: form 形如 `body: form`(form 变量在前面 `new FormData()` 构造)。
        bm = re.search(r"\bbody\s*:\s*([A-Za-z_$][\w$]*)", body_expr)
        if bm:
            nm = re.search(
                r"\b(?:const|let|var)\s+" + bm.group(1) + r"\s*=\s*new FormData\s*\(",
                file_text)
            if nm:
                return "multipart"
    return "json"


def _extract_frontend_calls(file_text: str, file_path: Path, base="", module_label=""):
    """从单个 Vue/TS 文件抽取全部业务调用。"""
    records = []
    lines = file_text.splitlines()
    for fn in TRANSPORT_CALLS:
        for start, open_idx, end_idx in _scan_call_spans(file_text, fn):
            lineno = file_text.count("\n", 0, start) + 1
            call_body = file_text[open_idx + 1:end_idx]
            parts = _top_level_split(call_body)
            if not parts:
                continue
            if fn == "requestLearning":
                # requestLearning(options, path, method, body)
                if len(parts) < 2:
                    continue
                path_expr = parts[1]
                method_default = "GET"
                method = "GET"
                if len(parts) >= 3:
                    m = re.fullmatch(r"['\"]([A-Za-z]+)['\"]", parts[2].strip())
                    method = m.group(1).upper() if m else (
                        "unresolved" if "?" in parts[2] else parts[2].strip().upper())
                body_idx = 3
            else:
                path_expr = parts[0]
                method_default = "POST" if fn == "requestBinaryJson" else "GET"
                method = _method_from_options(call_body, method_default, file_text)
                body_idx = 1
            body_expr = "".join(parts[body_idx:]) if body_idx < len(parts) else ""
            path_base = "/api/learning" if fn == "requestLearning" else base

            norm, why = _resolve_simple_path(path_expr, file_text, path_base)
            records.append({
                "module": module_label or file_path.stem,
                "source": str(file_path.relative_to(BIN_DIR)).replace("\\", "/"),
                "line": lineno,
                "fn": fn,
                "raw_path": path_expr.strip()[:160],
                "method": method,
                "transport": _transport_from_body(body_expr, fn, file_text) if norm else None,
                "norm_path": norm,
                "resolved": bool(norm),
                "reason": why,
                "raw_call": call_body.strip()[:120],
            })
    return records


# ---------------------------------------------------------------------------
# Python 渲染页面调用抽取(工作台 / 签名页面)
# ---------------------------------------------------------------------------
_PY_FORM_RE = re.compile(
    r"<form\b[^>]*?\bmethod\s*=\s*['\"]([a-z]+)['\"][^>]*?\baction\s*=\s*['\"](/api/[^'\">]+)['\"]",
    re.IGNORECASE)
_PY_FORM_RE2 = re.compile(
    r"<form\b[^>]*?\baction\s*=\s*['\"](/api/[^'\">]+)['\"][^>]*?\bmethod\s*=\s*['\"]([a-z]+)['\"]",
    re.IGNORECASE)


def _extract_python_page_calls(file_text: str, file_path: Path, module_label=""):
    """抽取 Python 渲染页面里的浏览器调用(fetch/requestJson/表单 action)。

    只抽取嵌入页面 JS 的 ``fetch('/api/...')``/``requestJson`` 与表单
    ``<form action="/api/...">``;不做整文件 /api/ 字面量兜底(那会把后端请求路由
    字符串误当前端调用)。跨行/模板/拼接用 ``_resolve_simple_path`` 解析,无法解析
    的留作 unresolved 证据。
    """
    records = []
    src = str(file_path.relative_to(BIN_DIR)).replace("\\", "/")

    def push(line, fn, raw, method, transport, norm, resolved, reason, raw_call):
        records.append({
            "module": module_label,
            "source": src, "line": line, "fn": fn, "raw_path": raw[:160],
            "method": method, "transport": transport, "norm_path": norm,
            "resolved": resolved, "reason": reason, "raw_call": (raw_call or raw)[:120],
        })

    # fetch / requestJson 浏览器调用:用调用括号扫描 + 解析器(单行/多行/模板/拼接一致处理)。
    for fn in ("fetch", "requestJson", "requestBinaryJson"):
        for start, _o, end in _scan_call_spans(file_text, fn):
            lineno = file_text.count("\n", 0, start) + 1
            body = file_text[_o + 1:end]
            parts = _top_level_split(body)
            if not parts:
                continue
            expr = parts[0].strip()
            if expr[:1] in "\"'`" or "+" in expr or "?" in expr:
                norm, why = _resolve_simple_path(expr, file_text)
                if norm:
                    default = "POST" if fn == "requestBinaryJson" else "GET"
                    method = _method_from_options(body, default, file_text)
                    push(lineno, "fetch", expr, method, "json", norm, True, "", expr)
                    continue
                push(lineno, "fetch/unresolved", expr, "unresolved", None, None, False,
                     why or "未能解析", expr)
    # 表单 action->/api/...(签名使用确认页面按 method 决定请求方法)
    for m in list(_PY_FORM_RE.finditer(file_text)) + list(_PY_FORM_RE2.finditer(file_text)):
        lineno = file_text.count("\n", 0, m.start()) + 1
        if _PY_FORM_RE.match(m.group(0)):
            method = m.group(1).upper()
            raw = m.group(2)
        else:
            raw = m.group(1)
            method = m.group(2).upper()
        push(lineno, "form", raw, method, "form", _normalize_frontend_path(raw), True, "", raw)
    return records


def _gather_all_sources():
    """(label, source_text, Path, is_py_page) 列表。"""
    sources = []
    for p in sorted(FRONTEND_SRC.rglob("*")):
        if not p.is_file() or p.suffix.lower() not in (".vue", ".ts"):
            continue
        rel = str(p.relative_to(FRONTEND_SRC)).replace("\\", "/")
        low = rel.lower()
        if "assets" in low or "assistant" in low or rel in TRANSPORT_SKIPS:
            continue
        src = p.read_text(encoding="utf-8-sig", errors="replace")
        sources.append((rel, src, p, False))
    for fp, label in PY_PAGE_SOURCES:
        if fp.is_file():
            sources.append((fp.name + " [py-page]", fp.read_text(encoding="utf-8-sig", errors="replace"),
                            fp, True))
    return sources


def _is_excluded(norm_path: str) -> bool:
    path = norm_path.rstrip("/")
    if path in EXCLUDED_EXACT:
        return True
    for prefix in EXCLUDED_PREFIXES:
        stem = prefix.rstrip("/")
        if path == stem or path.startswith(stem + "/"):
            return True
    return False


# ---------------------------------------------------------------------------
# 审计主流程
# ---------------------------------------------------------------------------
def build_native_ids(catalog) -> list:
    first = catalog.discover(page_size=50)
    total = first["total"]
    ids = []
    for page in range(1, (total + 49) // 50 + 1):
        ids.extend(item["id"] for item in catalog.discover(page=page, page_size=50)["items"])
    return ids


def classify(call, native_ids) -> dict:
    """返回带 status 的分类:covered/missing/excluded/unresolved。"""
    out = dict(call)
    norm = call.get("norm_path")
    if not call.get("resolved") or not norm or call.get("method") == "unresolved":
        out["status"] = "unresolved"
        out["note"] = call.get("reason") or ("" if call.get("resolved") else "路径未解析")
        return out
    method = (call.get("method") or "GET").upper()
    # Codex 明确要开放给助手的签名业务端点:先于排除判定,单独列 reserved,
    # 不误报为缺失也不假覆盖(即使当前临时签名异常被目录排除)。
    if norm.rstrip("/") in PENDING_OPEN_CODEX:
        for cid in native_ids:
            if cid.startswith(method + " ") and _shape_equal(norm, cid.split(" ", 1)[1]):
                out["status"] = "covered"
                out["note"] = "目录已覆盖(Codex 已开放)"
                return out
        out["status"] = "reserved"
        out["note"] = "Codex 待开放的签名业务端点(当前目录排除,不应缺省)"
        return out
    if _is_excluded(norm):
        out["status"] = "excluded"
        out["note"] = "目录有意排除(系统/登录/Qt/原始签名/工单执行轮巡/计划收敛设置)"
        return out
    # 裸占位符(前无 /)不是合法路由形态:多半是 base + 动态变量拼接,归为待人工复核。
    if re.search(r"[^/]\{param\}", norm):
        out["status"] = "unresolved"
        out["note"] = f"动态变量直接拼在末段,具体子路径未知:{norm}"
        return out
    for cid in native_ids:
        if cid.startswith(method + " ") and _shape_equal(norm, cid.split(" ", 1)[1]):
            out["status"] = "covered"
            out["note"] = f"匹配目录 {cid}"
            return out
    out["status"] = "missing"
    out["note"] = "目录中未找到该方法+路径形态"
    return out


def run_audit():
    catalog = build_native_catalog()
    native_ids = build_native_ids(catalog)
    all_records = []
    for rel, src, p, is_py in _gather_all_sources():
        if is_py:
            recs = _extract_python_page_calls(src, p, module_label=rel)
        else:
            recs = _extract_frontend_calls(src, p, module_label=rel)
        all_records.extend(recs)
    # 去重:同一 源文件+行+规范化ID 保留一条(合成 evidence)
    seen = {}
    for r in all_records:
        classified = classify(r, native_ids)
        key = (classified["source"], classified["line"], classified["norm_path"], classified["method"])
        if key not in seen:
            classified["evidence_lines"] = [classified["line"]]
            seen[key] = classified
        else:
            seen[key]["evidence_lines"].append(classified["line"])
            seen[key]["evidence_lines"].sort()
    ordered = sorted(seen.values(), key=lambda r: (r["source"], r["line"], r["norm_path"] or r["raw_path"]))
    # 汇总
    summary = {"total_unique": len(ordered), "covered": 0, "missing": 0,
                   "excluded": 0, "unresolved": 0, "reserved": 0}
    by_module = {}
    for r in ordered:
        summary[r["status"]] += 1
        by_module.setdefault(r["module"], []).append(r)
    return {
        "native_total": len(native_ids),
        "summary": summary,
        "records": ordered,
        "by_module": by_module,
    }


# ---------------------------------------------------------------------------
# 输出
# ---------------------------------------------------------------------------
def _group_label(norm_path: str) -> str:
    if not norm_path:
        return "unresolved"
    parts = _segments(norm_path)
    if len(parts) >= 2 and parts[0] == "api":
        return parts[1]
    return "other"


def _render_text(result):
    lines = []
    lines.append("=== 前端API覆盖证据:文件+调用 级别 ===")
    lines.append(f"native 目录路由总数: {result['native_total']}")
    s = result["summary"]
    lines.append(
        f"前端唯一调用形态: {s['total_unique']}  (covered={s['covered']}, "
        f"missing={s['missing']}, excluded={s['excluded']}, "
        f"reserved={s.get('reserved', 0)}, unresolved={s['unresolved']})")
    lines.append("")
    lines.append("--- 按业务模块覆盖 ---")
    mod = {}
    for r in result["records"]:
        g = _group_label(r["norm_path"]) if r["norm_path"] else r["source"]
        mod.setdefault(g, {"covered": 0, "missing": 0, "excluded": 0, "unresolved": 0,
                           "reserved": 0, "sources": set()})
        mod[g][r["status"]] += 1
        mod[g]["sources"].add(r["source"])
    for g in sorted(mod):
        m = mod[g]
        lines.append(f"  {g:<24} covered={m['covered']}  missing={m['missing']}  "
                     f"excluded={m['excluded']}  reserved={m.get('reserved', 0)}  "
                     f"unresolved={m['unresolved']}  (文件:{len(m['sources'])})")
    lines.append("")
    lines.append("--- MISSING(前端调用在目录缺失)---")
    for r in sorted([x for x in result["records"] if x["status"] == "missing"],
                    key=lambda x: (x["source"], x["line"])):
        lines.append(f"  [{r['method']}] {r['norm_path'] or r['raw_path']}  <- {r['source']}:{r['line']}  "
                     f"({r['raw_path']})")
    lines.append("")
    lines.append("--- RESERVED(Codex 明确待开放的签名业务端点,当前目录排除)---")
    for r in sorted([x for x in result["records"] if x["status"] == "reserved"],
                    key=lambda x: (x["source"], x["line"])):
        lines.append(f"  [{r['method']}] {r['norm_path']}  <- {r['source']}:{r['line']}")
    lines.append("")
    lines.append("--- UNRESOLVED(动态路径未解析,需人工复核)---")
    for r in sorted([x for x in result["records"] if x["status"] == "unresolved"],
                    key=lambda x: (x["source"], x["line"])):
        lines.append(f"  {r['source']}:{r['line']}  {r['method']}  {r['raw_path'][:100]}  "
                     f"REASON: {r.get('note') or r.get('reason')}")
    lines.append("")
    lines.append("--- EXCLUDED(目录有意排除,不计覆盖,仅记录证据)---")
    for r in sorted([x for x in result["records"] if x["status"] == "excluded"],
                    key=lambda x: (x["source"], x["line"])):
        lines.append(f"  [{r['method']}] {r['norm_path']}  <- {r['source']}:{r['line']}")
    lines.append("")
    lines.append("--- 高价值覆盖样例(covered 代表性)---")
    covered_probe = ["/api/learning", "/api/critical-guard", "/api/cabinet-power",
                     "/api/repair-management", "/api/drills", "/api/capacity/water",
                     "/api/engineer/mop", "/api/signatures", "/api/workbench", "/api/polling-sops"]
    shown = set()
    for probe in covered_probe:
        for r in sorted([x for x in result["records"] if x["status"] == "covered"
                         and (x["norm_path"] or "").startswith(probe)],
                        key=lambda x: (x["source"], x["line"])):
            k = (r["norm_path"], r["method"])
            if k in shown:
                continue
            shown.add(k)
            lines.append(f"  [{r['method']}] {r['norm_path']}  <- {r['source']}:{r['line']}")
            if len(shown) > 40:
                break
        if len(shown) > 40:
            break
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# unittest:保护代表性抽取 + 清单确定性
# ---------------------------------------------------------------------------
class FrontendCoverageAuditTests(unittest.TestCase):
    def setUp(self):
        self.result = run_audit()

    def test_representative_frontend_calls_extracted(self):
        recs = self.result["records"]
        by_key = {(r["norm_path"], r["method"], r["source"]) for r in recs}
        # 学练答题 POST(模板路径归一为 {param})
        self.assertTrue(
            any(r["norm_path"] == "/api/learning/papers/{param}/answer" and r["method"] == "POST"
                and "LearningPage" in r["source"] for r in recs),
            "LearningPage POST /api/learning/papers/{id}/answer 未抽取")
        # 维保任务详情 GET(模板路径含 ?admin=1,应剥离查询到路径形态)
        self.assertTrue(
            any(r["norm_path"] == "/api/critical-guard/tasks/{param}" and r["method"] == "GET"
                and "CriticalGuard" in r["source"] for r in recs),
            "CriticalGuard GET /api/critical-guard/tasks/{id} 未抽取")
        # 机柜识别 multipart 上传
        self.assertTrue(
            any(r["norm_path"] == "/api/cabinet-power/batches/recognize" and r["transport"] == "multipart"
                for r in recs))


    def test_inventory_is_deterministic(self):
        a = _serialize_for_hash(run_audit())
        b = _serialize_for_hash(run_audit())
        self.assertEqual(hashlib.sha256(a).hexdigest(), hashlib.sha256(b).hexdigest())

    def test_excluded_workorder_routes_classified_excluded(self):
        any_excl = any(r["status"] == "excluded" for r in self.result["records"])
        # workbench 工作台必然包含 polling-work-orders 执行端点
        wo = [r for r in self.result["records"]
              if r["status"] == "excluded" and (r["norm_path"] or "").startswith("/api/polling-work-orders")]
        self.assertTrue(wo, "Python 工作台页的轮巡执行端点应被分类为 excluded(而非 missing)")
        for r in wo:
            self.assertEqual(r["status"], "excluded")

    def test_missing_and_unresolved_are_visible_not_hidden(self):
        s = self.result["summary"]
        # 不强制断言数量(避免人为忽略);但保证它们被如实记录,不被任意忽略列表掩盖。
        self.assertIn("missing", s)
        self.assertIn("unresolved", s)
        self.assertGreaterEqual(s["total_unique"], 40)
        self.assertGreaterEqual(s["covered"], 10)


def _serialize_for_hash(result):
    return json.dumps({
        "native_total": result["native_total"],
        "summary": result["summary"],
        "records": [{k: r[k] for k in ("source", "line", "fn", "method", "norm_path",
                                       "raw_path", "status", "transport", "reason", "note")
                     if k in r} for r in result["records"]],
    }, ensure_ascii=False, sort_keys=True).encode("utf-8")


# ---------------------------------------------------------------------------
def main():
    result = run_audit()
    print(_render_text(result))
    print("\n故障排查:如需 JSON 全量清单,调用 run_audit() 取 result['records'](按 source/status 已排序)。")
    return result


if __name__ == "__main__":
    main()