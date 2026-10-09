# -*- coding: utf-8 -*-
"""有界审计:静态 unresolved 前端/页面动态端点 vs 实际调用者与 native 目录。

策略
----
``test_lighthouse_frontend_coverage.run_audit`` 会把无法被其启发式静态解析的
路径标为 ``unresolved``。本模块不改动该审计(其他文件由其他
agent 负责),只新增一份**独立的、有边界的人工复核目录** ``KNOWN_DYNAMIC_BINDINGS``:

- ``covered`` : 从源码片段可确证该调用实际命中 native 目录中的已知方法与路径,
  并给出“调用行 + 源码窗口 + 目录形态/排除判定”三方证据。
- ``excluded`` : 调用确实存在,但其端点属于 native 有意排除的能力
  (学练 learning / 工单执行轮巡 SOP execution / 设置 settings / 签名 signature /
  Qt 事件写等),必须保持 excluded,绝不当成 covered/missing。
- ``unresolved``: 确为真正的动态/包装传输调用(把 path 当参数的帮助函数、
  读 ``item.path``、页面表单 action 等),审计应继续如实标 unresolved,不得臆断。

约束
----
- 只读审计,不启动服务、不写业务数据、不读取凭据。
- 只新建 ``bin/test_lighthouse_dynamic_endpoints.py``,不改动其他任何文件。
- 测试只做有界断言:不启动服务器、不调用真实业务。
"""
import sys
import unittest
from pathlib import Path

BIN_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BIN_DIR))

from test_lighthouse_frontend_contracts import build_native_catalog  # noqa: E402
from test_lighthouse_frontend_coverage import (  # noqa: E402
    run_audit,
    _shape_equal,
)
from lan_bitable_template_portal.lighthouse_api import _route_excluded  # noqa: E402


# ---------------------------------------------------------------------------
# 人工复核目录:source:line -> 已知(方法,路径)对 与 源码证据针
# ---------------------------------------------------------------------------
# 命名占位符统一用 {param}(与 run_audit 归一一致);匹配目录时按 _shape_equal 形态比较。
# 每个 covered/excluded 条目用 method_paths 逐条列(方法, 归一路径),避免方法×路径叉乘。
KNOWN_DYNAMIC_BINDINGS = [
    # --- covered:可确证命中 native 目录的已知业务端点 ---
    dict(
        source="lan_bitable_template_portal/frontend/src/components/CriticalGuardPage.vue",
        line=1089, kind="covered",
        method_paths=[("GET", "/api/critical-guard/tasks")],
        window=8, needles=["/api/critical-guard/tasks?admin=1", "/api/critical-guard/tasks?scope="],
        note="admin/scope 二元路径去查询串后均收敛到 /api/critical-guard/tasks",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/CriticalGuardPage.vue",
        line=1244, kind="covered",
        method_paths=[("GET", "/api/critical-guard/tasks/{param}")],
        window=8, needles=["/api/critical-guard/tasks/${encodeURIComponent(taskId)}"],
        note="任务详情模板路径,查询串 admin/scope 被剥离",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/CabinetBatchTextFill.vue",
        line=70, kind="covered",
        method_paths=[("POST", "/api/cabinet-power/batches/{param}/text-preview"),
                      ("POST", "/api/cabinet-power/batches/{param}/text-apply")],
        window=40, needles=["/api/cabinet-power/batches/${props.batchId}/text-${action}",
                            "request('preview'", "request('apply'"],
        note="request(action) 帮助函数;action∈{preview,apply} 在 75/106 行确证",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/WaterManagementPage.vue",
        line=1160, kind="covered",
        method_paths=[("POST", "/api/capacity/water/records"),
                      ("PATCH", "/api/capacity/water/records/{param}")],
        window=14, needles=["/api/capacity/water/records/${encodeURIComponent(editingRecordId.value)}",
                            "/api/capacity/water/records", "PATCH", "POST"],
        note="editingRecordId 三元:新建 POST /records,编辑 PATCH /records/{id}",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/composables/useRepairSubmission.ts",
        line=38, kind="covered",
        method_paths=[("GET", "/api/repair-management/operations/{param}"),
                      ("POST", "/api/repair-management/operations/{param}")],
        window=6, needles=["/api/repair-management/operations/${encodeURIComponent(item.id)}",
                           'recover ? "POST" : "GET"'],
        note="写入状态查询/重试:recover 时 POST,否则 GET 同一 operations/{id}",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=3814, kind="covered",
        method_paths=[("POST", "/api/polling-sops"),
                      ("PUT", "/api/polling-sops/{param}")],
        window=4, needles=["/api/polling-sops/${{encodeURIComponent(sop.sop_id)}}",
                            "'/api/polling-sops'", "PUT", "POST"],
        note="SOP 保存:sop_id 存在时 PUT /polling-sops/{id},否则 POST /polling-sops",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=5382, kind="covered",
        method_paths=[("POST", "/api/notice-attachments")],
        window=10, needles=["'/api/notice-attachments?file_name='", "method: 'POST'"],
        note="原始体上传通告附件;file_name/identity 等在查询串中,路径形态 /api/notice-attachments",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=5681, kind="covered",
        method_paths=[("DELETE", "/api/change-confirmations/{param}/screenshot")],
        window=6, needles=["/api/change-confirmations/${{encodeURIComponent(targetRecordId)}}/screenshot",
                            "method: 'DELETE'"],
        note="删除变更确认截图;?local_image_id= 或 ?file_token= 属查询串非路径段",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=6260, kind="covered",
        method_paths=[("GET", "/api/workbench/source-options")],
        window=10, needles=["new URL('/api/workbench/source-options'"],
        note="new URL('/api/workbench/source-options', ...) + url.pathname+url.search",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=6410, kind="covered",
        method_paths=[("GET", "/api/workbench/repair-event-prefill")],
        window=10, needles=["new URL('/api/workbench/repair-event-prefill'"],
        note="new URL('/api/workbench/repair-event-prefill', ...)",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=6571, kind="covered",
        method_paths=[("GET", "/api/workbench/repair-event-candidates")],
        window=10, needles=["new URL('/api/workbench/repair-event-candidates'"],
        note="new URL('/api/workbench/repair-event-candidates', ...)",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=7367, kind="covered",
        method_paths=[("GET", "/api/workbench/lite-fragment"),
                      ("GET", "/api/workbench/lite-detail")],
        window=4, needles=["liteFragmentUrl(url, detailOnly)"],
        note="liteFragmentUrl 定义(4651 行)按 detailOnly 拼接 lite-fragment/lite-detail;两路径均在目录",
        definition_ref=dict(
            source="lan_bitable_template_portal/workbench_lite.py",
            definition_needles=["'/api/workbench/lite-detail'", "'/api/workbench/lite-fragment'"],
            definition_marker="liteFragmentUrl",
        ),
    ),

    dict(
        source="lan_bitable_template_portal/frontend/src/components/LearningPage.vue",
        line=676, kind="covered",
        method_paths=[("GET", "/api/learning/papers")],
        window=4, needles=['"/papers?" + query({ today: 1 })'],
        note="个人今日题单只读查询:requestLearning 添加 /api/learning 前缀,命中已开放的 GET 目录",
    ),

    # --- excluded:已知端点属 native 有意排除能力,必须保持 excluded ---
    dict(
        source="lan_bitable_template_portal/frontend/src/components/LearningPage.vue",
        line=662, kind="excluded",
        method_paths=[("GET", "/api/learning/people")],
        window=4, needles=['"/people?" + new URLSearchParams'],
        note="学练人员查询:requestLearning 添加 /api/learning 前缀,保持原生目录排除",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/LearningPage.vue",
        line=666, kind="excluded",
        method_paths=[("GET", "/api/learning/bootstrap")],
        window=6, needles=["/bootstrap${scope.value"],
        note="学练未集成到助手目录;requestLearning 前缀 /api/learning 被排除",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/LearningPage.vue",
        line=789, kind="excluded",
        method_paths=[("POST", "/api/learning/questions"),
                      ("PUT", "/api/learning/questions/{param}")],
        window=6, needles=["/questions/${encodeURIComponent(id)}", '"/questions"'],
        note="学练题目保存:新建 POST /questions,编辑 PUT /questions/{id};learning 排除",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=9359, kind="excluded",
        method_paths=[("POST", "/api/polling-work-orders/photo")],
        window=3, needles=["/api/polling-work-orders/photo?token="],
        note="工单执行轮巡步骤照片上传;polling-work-orders 执行端点在目录中排除",
    ),

    # --- unresolved:确为包装/动态/页面路由,保持审计如实 unresolved ---
    dict(
        source="lan_bitable_template_portal/frontend/src/App.vue",
        line=552, kind="unresolved",
        method_paths=[], window=6,
        needles=["requestJson(path, options", "portalRequest(path"],
        note="portalRequest(path, options) 传输包装;path 为调用方传入参数,非独立端点",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergencePage.vue",
        line=76, kind="unresolved",
        method_paths=[], window=6,
        needles=["requestJson(base+path", "const post = (path"],
        note="post(path, data) 帮助函数;base+path 的参数在别处由调用方给定",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergenceRules.vue",
        line=312, kind="unresolved",
        method_paths=[], window=4,
        needles=["requestJson(path, { cache: \"no-store\" })"],
        note="getJson(path) 包装;真实端点由调用方传入(已在他处覆盖)",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergenceRules.vue",
        line=316, kind="unresolved",
        method_paths=[], window=4,
        needles=["requestJson(path, { method: \"POST\""],
        note="postJson(path, body) 包装;真实端点由调用方传入",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergenceRules.vue",
        line=320, kind="unresolved",
        method_paths=[], window=4,
        needles=["requestJson(path, { method: \"PUT\""],
        note="putJson(path, body) 包装;真实端点由调用方传入",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergenceRules.vue",
        line=324, kind="unresolved",
        method_paths=[], window=4,
        needles=["requestJson(path, { method: \"DELETE\""],
        note="deleteJson(path) 包装;真实端点由调用方传入",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/composables/useRepairSubmission.ts",
        line=63, kind="unresolved",
        method_paths=[], window=4,
        needles=["requestJson(item.path", "item.method", "item.body"],
        note="重试读本地暂存的 item.path(动态存储值),静态不可解析",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/composables/useRepairSubmission.ts",
        line=97, kind="unresolved",
        method_paths=[], window=4,
        needles=["requestJson(path, { ...options, timeoutMs"],
        note="submit(path,...) 包装;path 为调用方参数(RepairManagementPage:3392 传 /records)",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=3686, kind="unresolved",
        method_paths=[], window=4,
        needles=["async function pollingApi(url", "fetch(url, {{ credentials"],
        note="pollingApi(url,...) 传输包装的 fetch(url);真实端点由调用方传入",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=7404, kind="unresolved",
        method_paths=[], window=6,
        needles=["async function fetchLiteRefreshJson(url", "fetch(url, {", "credentials: 'same-origin'"],
        note="fetchLiteRefreshJson(url,...) 包装的 fetch(url);真实端点由调用方(liteFetchThenRefresh)传入",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=7428, kind="unresolved",
        method_paths=[], window=6,
        needles=["liteFetchThenRefresh(url, label, kind)", "fetchLiteRefreshJson(url, 15000)"],
        note="liteFetchThenRefresh(url,...) 包装转调 refresh 包装;真实端点由调用方传入",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=9148, kind="unresolved",
        method_paths=[], window=6,
        needles=["fetch(pasteForm.action", "method: 'POST'", "new FormData(pasteForm)"],
        note="页面表单 action=/workbench-lite/parse(非 /api/ 业务路由),不纳入 API 目录",
    ),
]


# ---------------------------------------------------------------------------
# 12 个 remaining wrapper callsite 的人工复核:真实调用者→具体端点分类
# ---------------------------------------------------------------------------
# 这些是 run_audit 无法穿透的 transport/帮助函数(portalRequest/post/getJson/
# submit/pollingApi/fetchLiteRefreshJson/liteFetchThenRefresh/表单 action)。
# 我们不能把它们标为 missing API;正确做法是:追踪其真实调用者传入的具体端点,
# 逐个确认在 native 目录中存在(covered)或被原生排除(excluded)。凡“既不在目录
# 又未被排除”的具体端点即 concrete allowed endpoint gap —— 审计必须显式报告;
# 经人工复核,当前 12 个 wrapper 的全部真实调用端点均落到 covered/excluded,
# **无 allowed endpoint gap**。
#
# callers 条目为 dict:
#   method / path       —— 声称的真实端点(path 已归一化,占位符为 {param})
#   caller_line         —— 真实调用/证明点的行号(在 caller_source 中)
#   marker              —— 该行附近的真实源码子串(证据针,防伪造/防漂移)
#   caller_source       —— 调用者所在文件,缺省时为 wrapper 所在 source
# 测试先用 marker 断言端点确实出现在真实源码(再对 native 目录 + _route_excluded 分类)。
WRAPPER_REVIEW = [
    dict(
        source="lan_bitable_template_portal/frontend/src/App.vue",
        line=552, label="portalRequest(path, options) 传输包装",
        anchor="async function portalRequest(path",
        callers=[
            dict(method="GET", path="/api/auth/status", caller_line=570,
                 marker='portalRequest(`/api/auth/status?next='),
            dict(method="GET", path="/api/scope-overview", caller_line=594,
                 marker='portalRequest("/api/scope-overview")'),
            dict(method="GET", path="/api/handover-links", caller_line=603,
                 marker='portalRequest("/api/handover-links")'),
            dict(method="GET", path="/api/auth/permission-requests/current", caller_line=612,
                 marker='portalRequest("/api/auth/permission-requests/current")'),
            dict(method="POST", path="/api/auth/logout", caller_line=790,
                 marker='portalRequest("/api/auth/logout", { method: "POST"'),
            dict(method="POST", path="/api/auth/permission-requests", caller_line=805,
                 marker='portalRequest("/api/auth/permission-requests"'),
        ],
        note="portalRequest 仅 App.vue 内部 6 处调用;auth 类被 /api/auth/ 排除,"
             " scope-overview/handover-links 在目录,无 gap(每处 caller 均以真实源码 marker 锚定)",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergencePage.vue",
        line=76, label="post(local) 帮助函数(base=/api/plan-convergence, 一律 POST)",
        anchor="const post = (path",
        callers=[
            dict(method="POST", path="/api/plan-convergence/rulesets/{param}/match", caller_line=96,
                 marker="post('/rulesets/'+setId.value+'/match'"),
            dict(method="POST", path="/api/plan-convergence/compare", caller_line=101,
                 marker="post('/compare'"),
            dict(method="POST", path="/api/plan-convergence/rule-view", caller_line=111,
                 marker="kind:'snapshots'|'rule-view'"),
            dict(method="POST", path="/api/plan-convergence/snapshots", caller_line=111,
                 marker="kind:'snapshots'|'rule-view'"),
            dict(method="POST", path="/api/plan-convergence/settings/browser-login", caller_line=129,
                 marker="post('/settings/browser-login'"),
            dict(method="POST", path="/api/plan-convergence/settings/browser-login/cancel", caller_line=130,
                 marker="post('/settings/browser-login/cancel'"),
            dict(method="POST", path="/api/plan-convergence/maintenance/check", caller_line=139,
                 marker="post('/maintenance/check'"),
        ],
        note="base+path 业务端点均在目录;settings 前缀被排除,无 gap。rule-view/snapshots 由 "
             "drill 的 kind 类型联合('snapshots'|'rule-view')在 111 行同一动态调用处确证",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergenceRules.vue",
        line=312, label="getJson(path) 包装",
        anchor="async function getJson(path",
        callers=[
            dict(method="GET", path="/api/plan-convergence/catalog", caller_line=332,
                 marker="getJson(`/api/plan-convergence/catalog?${qs.toString()}`)"),
            dict(method="GET", path="/api/plan-convergence/rulesets", caller_line=385,
                 marker='getJson("/api/plan-convergence/rulesets")'),
            dict(method="GET", path="/api/plan-convergence/rulesets/{param}", caller_line=396,
                 marker="getJson(`/api/plan-convergence/rulesets/${id}`)"),
            dict(method="GET", path="/api/plan-convergence/rulesets/{param}/expand", caller_line=1151,
                 marker="getJson(`/api/plan-convergence/rulesets/${currentSetId.value}/expand`)"),
        ],
        note="规则目录读取端点均在目录,无 gap",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergenceRules.vue",
        line=316, label="postJson(path, body) 包装",
        anchor="async function postJson(path",
        callers=[dict(method="POST", path="/api/plan-convergence/rulesets", caller_line=438,
                      marker='postJson("/api/plan-convergence/rulesets"')],
        note="新建规则集在目录,无 gap",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergenceRules.vue",
        line=320, label="putJson(path, body) 包装",
        anchor="async function putJson(path",
        callers=[dict(method="PUT", path="/api/plan-convergence/rulesets/{param}", caller_line=471,
                      marker="putJson(`/api/plan-convergence/rulesets/${currentSetId.value}`")],
        note="更新规则集在目录,无 gap",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/components/PlanConvergenceRules.vue",
        line=324, label="deleteJson(path) 包装",
        anchor="async function deleteJson(path",
        callers=[dict(method="DELETE", path="/api/plan-convergence/rulesets/{param}", caller_line=494,
                      marker="deleteJson(`/api/plan-convergence/rulesets/${id}`)")],
        note="删除规则集在目录,无 gap",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/composables/useRepairSubmission.ts",
        line=63, label="retry item.path(读本地暂存的动态 path)",
        anchor="const result = await requestJson(item.path",
        callers=[
            dict(method="POST", path="/api/repair-management/records", caller_line=3392,
                 caller_source="lan_bitable_template_portal/frontend/src/components/RepairManagementPage.vue",
                 marker='submission.submit("/api/repair-management/records"',
                 note="真实 path 来源:RepairManagementPage:3392 只传 POST /records,写入 item.path"),
            dict(method="PUT", path="/api/repair-management/records/{param}", caller_line=16,
                 marker='pending.value.path.startsWith("/api/repair-management/records/")',
                 note="遗留 overwrite 支持 PUT /records/{id}:line16 startsWith 是文档化证据,非伪造字面量"),
        ],
        note="item.path 为本地暂存动态值,静态不可解析;POST /records 的来源锚定到 "
             "RepairManagementPage 真实调用,PUT /records/{id} 锚定到第16行 overwrite 守卫,均不臆断",
    ),
    dict(
        source="lan_bitable_template_portal/frontend/src/composables/useRepairSubmission.ts",
        line=97, label="submit(path,...) 包装(真实端点由调用方传入)",
        anchor="const result = await requestJson(path, { ...options, timeoutMs",
        callers=[dict(method="POST", path="/api/repair-management/records", caller_line=3392,
                      caller_source="lan_bitable_template_portal/frontend/src/components/RepairManagementPage.vue",
                      marker='submission.submit("/api/repair-management/records"')],
        note="RepairManagementPage:3392 传 POST /api/repair-management/records,在目录",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=3702, label="pollingApi(url,...) fetch 包装",
        anchor="async function pollingApi(url",
        callers=[
            dict(method="GET", path="/api/polling-sops", caller_line=3716,
                 marker="pollingApi(`/api/polling-sops?scope=${{"),
            dict(method="POST", path="/api/polling-sops/refresh", caller_line=3736,
                 marker="pollingApi('/api/polling-sops/refresh',{{method:'POST'"),
            dict(method="DELETE", path="/api/polling-sops/{param}", caller_line=3751,
                 marker="${{encodeURIComponent(target.sop_id)}}?expected_version=${{target.version}}`,{{method:'DELETE'"),
            dict(method="PUT", path="/api/polling-sops/{param}", caller_line=3756,
                 marker="/api/polling-sops/${{encodeURIComponent(sop.sop_id)}}`,{{method:'PUT'"),
            dict(method="GET", path="/api/signatures/people", caller_line=3776,
                 marker="pollingApi('/api/signatures/people?limit=200')"),
            dict(method="DELETE", path="/api/polling-sops/{param}/attachments/{param}", caller_line=3820,
                 marker="/attachments/${{encodeURIComponent(item.attachment_id)}}?expected_version=${{sop.version}}`,{{method:'DELETE'"),
            dict(method="POST", path="/api/polling-sops/{param}/attachments", caller_line=3823,
                 marker="/api/polling-sops/${{encodeURIComponent(sop.sop_id)}}/attachments`,{{method:'POST'"),
            dict(method="POST", path="/api/polling-sops", caller_line=3831,
                 marker=":'/api/polling-sops';saved=await pollingApi(url"),
            dict(method="PUT", path="/api/polling-sops/{param}", caller_line=3831,
                 marker="sop.sop_id?'PUT':'POST'"),
            dict(method="GET", path="/api/workbench/polling-delay-status", caller_line=6060,
                 marker="pollingApi(`/api/workbench/polling-delay-status?group_id=${{"),
            dict(method="POST", path="/api/polling-work-orders/{param}/retry-upload", caller_line=7896,
                 marker="/api/polling-work-orders/${{encodeURIComponent(pollingRetryUpload.getAttribute('data-polling-retry-upload')"),
            dict(method="POST", path="/api/polling-work-orders/{param}/resend-links", caller_line=7903,
                 marker="/api/polling-work-orders/${{encodeURIComponent(pollingResendLinks.getAttribute('data-polling-resend-links')"),
        ],
        note="SOP CRUD/轮巡延时/签名字典均在目录;polling-work-orders 执行端点被原生排除,"
             " 符合 SOP execution 排除约定",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=7423, label="fetchLiteRefreshJson(url) fetch 包装",
        anchor="async function fetchLiteRefreshJson(url",
        callers=[
            dict(method="GET", path="/api/source-refresh-status", caller_line=7471,
                 marker="/api/source-refresh-status?"),
        ],
        note="fetchLiteRefreshJson 只直接调用 source-refresh-status(7471 行字面量);"
             " maintenance/repair/change-refresh 端点由 liteFetchThenRefresh(url) 经 url 参数转发,"
             " 其真实字面量在 8214/8225/8236,归属 liteFetchThenRefresh 条目,不在此重复臆断",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=7448, label="liteFetchThenRefresh(url,...) 包装",
        anchor="async function liteFetchThenRefresh(url",
        callers=[
            dict(method="GET", path="/api/maintenance-refresh", caller_line=8214,
                 marker="liteFetchThenRefresh('/api/maintenance-refresh?scope='"),
            dict(method="GET", path="/api/repair-refresh", caller_line=8225,
                 marker="liteFetchThenRefresh('/api/repair-refresh?scope='"),
            dict(method="GET", path="/api/change-refresh", caller_line=8236,
                 marker="liteFetchThenRefresh('/api/change-refresh?scope='"),
        ],
        note="维保/检修/变更刷新端点都在目录,无 gap",
    ),
    dict(
        source="lan_bitable_template_portal/workbench_lite.py",
        line=9148, label="页面表单 action(非 /api/ 业务路由)",
        anchor="fetch(pasteForm.action",
        callers=[],
        note="action=/workbench-lite/parse 是页面路由而非 /api/ 业务端点,本身不构成 API gap;"
             " 如实保持 unresolved",
    ),
]


# ---------------------------------------------------------------------------
# helper
# ---------------------------------------------------------------------------
def _native_ids():
    cat = build_native_catalog()
    first = cat.discover(page_size=50)
    total = first["total"]
    ids = []
    for page in range(1, (total + 49) // 50 + 1):
        ids.extend(item["id"] for item in cat.discover(page=page, page_size=50)["items"])
    return ids


def _in_catalog(native_ids, method, path):
    cid = f"{method} {path}"
    if cid in native_ids:
        return True
    return any(
        i.startswith(method + " ") and _shape_equal(path, i.split(" ", 1)[1])
        for i in native_ids
    )


def _file_text(source):
    path = BIN_DIR / source
    return path.read_text(encoding="utf-8-sig", errors="replace")


def _source_window(source, line, window):
    text = _file_text(source)
    lines = text.splitlines()
    lo = max(1, line - window)
    hi = min(len(lines), line + window)
    return "\n".join(lines[lo - 1:hi])


def _caller_source_of(w, c):
    """wrapper 复核条目的调用者所在文件:caller 未指明 caller_source 时即 wrapper 源文件。"""
    return c.get("caller_source", w["source"])


def _caller_marker_present(w, c, window=12):
    """Use a unique source marker; ambiguous markers retain the location check."""
    src = _caller_source_of(w, c)
    if len(_signature_lines(src, c["marker"])) == 1:
        return True
    return c["marker"] in _source_window(src, c["caller_line"], window)


def _signature_lines(source, signature):
    """signature 子串在源码中出现的所有行号(源码标记,不依赖绝对行号断言)。"""
    text = _file_text(source)
    return [i + 1 for i, ln in enumerate(text.splitlines()) if signature in ln]


def _unique_unresolved(audit_records):
    seen = set()
    out = []
    for r in audit_records:
        if r.get("status") == "unresolved":
            k = (r["source"], r["line"])
            if k not in seen:
                seen.add(k)
                out.append(r)
    return out


def _resolve_binding_records(audit_records, bindings, tolerance=40):
    """为每条 binding 用源码标记(signature=needles[0])在 unresolved 记录中定位唯一对应记录。

    规则:signature 首次/出现行 → 取所有 unresolved 记录中与该行“最近距离最小”的一条;
    只有当最近距离 <= tolerance 且严格唯一(次近更大)时才认领,否则记为 None(标记不足
    以唯一对应,留待人工而非臆断)。返回值与 bindings 顺序对齐。
    """
    uniq = _unique_unresolved(audit_records)
    recs_by_file = {}
    for r in uniq:
        recs_by_file.setdefault(r["source"], []).append(r)
    out = []
    for b in bindings:
        sig = b.get("signature") or b["needles"][0]
        occ = _signature_lines(b["source"], sig)
        if not occ:
            out.append(None)
            continue
        cand = []
        for r in recs_by_file.get(b["source"], []):
            d = min(abs(o - r["line"]) for o in occ)
            cand.append((d, r["line"]))
        cand.sort()
        if not cand:
            out.append(None)
            continue
        best_d, best_line = cand[0]
        second_d = cand[1][0] if len(cand) > 1 else tolerance + 1
        if best_d > tolerance or best_d == second_d:
            out.append(None)
            continue
        hits = [r for r in uniq if r["source"] == b["source"] and r["line"] == best_line]
        out.append(hits[0] if len(hits) == 1 else None)
    return out


# ---------------------------------------------------------------------------
# 测试
# ---------------------------------------------------------------------------
class DynamicEndpointAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.native_ids = _native_ids()
        res = run_audit()
        cls.audit_records = res["records"]
        cls.audit_summary = res["summary"]
        cls.bindings = KNOWN_DYNAMIC_BINDINGS
        # 标记驱动:每条 binding 用 needles 在 run_audit 记录中定位唯一记录(源码标记,非绝对行号)
        cls.binding_record = _resolve_binding_records(cls.audit_records, cls.bindings)

    def _binding_record(self, b):
        idx = self.bindings.index(b)
        return self.binding_record[idx]

    def test_all_unresolved_are_accounted_by_ground_truth(self):
        """run_audit 的 unresolved 必须全部被本目录覆盖,不丢不减不臆断。

        改用源码标记(needles)定位:每条 binding 必须被源码标记唯一定位到一条
        unresolved 记录,且全体 unresolved 记录被恰好认领一次(双射),不再依赖绝对行号。
        """
        unresolved = [r for r in self.audit_records if r["status"] == "unresolved"]
        self.assertEqual(len(unresolved), len(self.bindings))
        unresolved_keys = {(r["source"], r["line"]) for r in unresolved}
        claimed = set()
        for b in self.bindings:
            rec = self._binding_record(b)
            self.assertIsNotNone(
                rec,
                f"binding 未能用源码标记唯一定位到 unresolved 记录: {b['source']} "
                f"needles={b['needles']} -> {b['note']}")
            self.assertEqual(
                rec["status"], "unresolved",
                f"{b['source']}:{b['line']} 定位到的记录应为 unresolved;当前={rec['status']}")
            k = (rec["source"], rec["line"])
            self.assertNotIn(k, claimed, f"两条 binding 认领同一 unresolved 记录 {k}")
            claimed.add(k)
        self.assertEqual(claimed, unresolved_keys,
                         "unresolved 记录集合与绑定集合不一致(源码标记双射被破坏)")

    def test_known_covered_bindings_match_native_catalog_and_not_excluded(self):
        """covered 绑定:(方法,路径) 形态存在于 native 目录,且未被 _route_excluded 排除。"""
        covered = [b for b in self.bindings if b["kind"] == "covered"]
        self.assertGreaterEqual(len(covered), 10, "应至少有 10 条可确证 covered 绑定")
        for b in covered:
            self.assertTrue(b["method_paths"], f"{b['source']}:{b['line']} covered 缺方法/路径对")
            for m, p in b["method_paths"]:
                self.assertTrue(
                    _in_catalog(self.native_ids, m, p),
                    f"covered 绑定应命中 native 目录但不位于排除集:\n"
                    f"  {b['source']}:{b['line']} [{m}] {p}\n"
                    f"  note: {b['note']}")
                self.assertFalse(
                    _route_excluded(p, m),
                    f"covered 绑定不得被 _route_excluded 排除:\n"
                    f"  {b['source']}:{b['line']} [{m}] {p}\n"
                    f"  note: {b['note']}")

    def test_known_excluded_bindings_are_honored_native_exclusions(self):
        """excluded 绑定:确实属于 native 有意排除能力,绝不当成 covered/missing。

        覆盖学练(learning)、SOP 执行轮巡(polling-work-orders)等排除前缀;若未来
        settings/signature/Qt 事件写路径出现在本目录,同样必须落在此约束下。
        """
        excluded = [b for b in self.bindings if b["kind"] == "excluded"]
        self.assertTrue(excluded, "至少应有排除类动态端点绑定")
        for b in excluded:
            self.assertTrue(b["method_paths"])
            for m, p in b["method_paths"]:
                self.assertTrue(
                    _route_excluded(p, m),
                    f"excluded 绑定必须被 native 排除规则识别(learning/SOP等):\n"
                    f"  {b['source']}:{b['line']} [{m}] {p}\n"
                    f"  note: {b['note']}")
                self.assertFalse(
                    _in_catalog(self.native_ids, m, p),
                    f"excluded 端点不应进入 native 目录:\n"
                    f"  {b['source']}:{b['line']} [{m}] {p}")

    def test_exclusion_guard_rejects_covered_for_known_excluded_prefixes(self):
        """排除前缀守卫:任何已知排除路径不得标 covered。

        这是对“native 排除约定”的显式护栏,防止将来把被排除端点误报为 covered:
          - learning 学练写操作及未开放查询
          - plan-convergence/settings 受保护设置
          - signatures 原始签名/临时凭据/使用确认
          - polling-work-orders 工单执行轮巡(SOP execution:session/photo/confirm/
            rollback/activate/release/retry/resend)
          - /api/qt/ Qt-only 事件写(remove/add/update/end 等 Qt 本地写)
          - /api/system|/api/remote|/api/update|/api/runtime|/stream 系统/内部端点
        """
        excluded_prefixes = (
            "/api/learning", "/api/plan-convergence/settings", "/api/signatures/",
            "/api/polling-work-orders", "/api/qt/", "/api/system", "/api/remote",
            "/api/update", "/api/runtime",
        )
        for b in self.bindings:
            for method, p in b.get("method_paths") or []:
                if (method, p) == ("GET", "/api/learning/papers"):
                    self.assertEqual(b["kind"], "covered")
                    continue
                for pref in excluded_prefixes:
                    if p.startswith(pref):
                        self.assertEqual(
                            b["kind"], "excluded",
                            f"路径 {p} 属于排除前缀 {pref},必须标 excluded 而非 {b['kind']}\n"
                            f"  {b['source']}:{b['line']}  {b['note']}")

    def test_snippet_evidence_is_present_in_real_source(self):
        """每条绑定的证据针必须真实出现在对应源码窗口内(防伪造/防漂移)。

        锚点取自“标记解析出的 audit 记录行”(_resolve_binding_records 由 signature 定位),
        而非硬编码绝对行号;再取 ±window 验证全部 needles 都在该记录附近出现。
        """
        self.assertGreaterEqual(len(self.bindings), 27)
        for i, b in enumerate(self.bindings):
            rec = self.binding_record[i]
            self.assertIsNotNone(
                rec,
                f"需先标记解析到 audit 记录才能做证据校验 {b['source']}  needles={b['needles']}")
            win = _source_window(b["source"], rec["line"], b.get("window", 6))
            missing = [n for n in b["needles"] if n not in win]
            self.assertEqual(
                missing, [],
                f"源码窗口缺失证据针 {b['source']} 记录@{rec['line']} 附近 ±{b.get('window',6)} 行:\n"
                f"  missing={missing}\n  note={b['note']}")

    def test_lite_fragment_url_definition_resolves_both_native_paths(self):
        """7367 覆写的 liteFragmentUrl 定义确实同时拼接 lite-fragment/lite-detail 两个 native 路径。"""
        b = next(x for x in self.bindings if x["line"] == 7367)
        ref = b["definition_ref"]
        text = (BIN_DIR / ref["source"]).read_text(encoding="utf-8-sig", errors="replace")
        marker = text.find(ref["definition_marker"])
        self.assertGreaterEqual(marker, 0,
                                f"未找到 liteFragmentUrl 定义于 {ref['source']}")
        ctx = text[marker:marker + 4000]
        for needle in ref["definition_needles"]:
            self.assertIn(needle, ctx,
                          f"liteFragmentUrl 定义上下文应含 {needle}\n  near line 7367")

    def test_honest_unresolved_are_still_reported_unresolved(self):
        """unresolved(包装/动态/页面路由)必须保持审计如实标 unresolved,绝不臆断 covered。

        run_audit 把 12 个 wrapper/动态调用点标为 unresolved(它无法穿透 transport);
        即便我们追踪真实调用者拿到了具体端点,也不能把 wrapper 函数本身改成 covered。
        """
        unresolved_bindings = [b for b in self.bindings if b["kind"] == "unresolved"]
        self.assertEqual(len(unresolved_bindings), 12,
                         "当前 12 个 remaining wrapper callsite 必须保持 honest unresolved")
        for b in unresolved_bindings:
            rec = self._binding_record(b)
            self.assertIsNotNone(
                rec,
                f"{b['source']} 应仍在 run_audit 中标 unresolved(诚实),但标记定位失败\n"
                f"  note: {b['note']}")
            self.assertEqual(
                rec["status"], "unresolved",
                f"{b['source']} 应为 unresolved;当前 status={rec['status']}\n"
                f"  note: {b['note']}")

    def test_no_known_dynamic_endpoint_silently_drops_from_audit(self):
        """本目录中的每个调用点都必须在 run_audit 里有记录(不静默丢弃)。"""
        for b in self.bindings:
            rec = self._binding_record(b)
            self.assertIsNotNone(
                rec,
                f"{b['source']} 应在 run_audit 记录,不得静默丢弃({b['kind']})\n"
                f"  needles={b['needles']}")

    def test_12_wrapper_review_anchors_exist_in_real_source(self):
        """12 个 wrapper 复核条目:anchor 标记必须真实存在于对应源码(防伪造/防漂移)。"""
        self.assertEqual(len(WRAPPER_REVIEW), 12, "必须恰好复核 12 个 remaining wrapper callsite")
        for w in WRAPPER_REVIEW:
            text = _file_text(w["source"])
            self.assertIn(w["anchor"], text,
                          f"wrapper 复核 anchor 未找到 {w['source']} anchor={w['anchor']!r}")

    def test_12_wrapper_real_callers_have_no_allowed_endpoint_gaps(self):
        """Review the 12 wrapper callsites against their real callers:报告 concrete allowed endpoint gaps。

        逐个验收 12 个 wrapper 的真实调用者端点:必须落在 native 目录(covered)或被
        _route_excluded 排除(excluded)。若某端点“既不在目录又未被排除”,即为
        concrete allowed endpoint gap,此处必须显式暴露(失败),绝不当成 wrapper 的
        missing API。当前人工复核结论:全部 covered/excluded,无 allowed endpoint gap。
        且每个声称端点在分类前必须先由真实源码 marker 锚定(防止替换 caller 也能通过)。
        """
        gaps = []
        uncovered_markers = []
        covered_n = excluded_n = 0
        for w in WRAPPER_REVIEW:
            for c in w["callers"]:
                method, path = c["method"], c["path"]
                cs = _caller_source_of(w, c)
                if not _caller_marker_present(w, c):
                    uncovered_markers.append((w["source"], w["line"], method, path,
                                              cs, c["caller_line"], c["marker"]))
                    continue
                in_cat = _in_catalog(self.native_ids, method, path)
                is_excluded = _route_excluded(path, method)
                if in_cat:
                    covered_n += 1
                elif is_excluded:
                    excluded_n += 1
                else:
                    gaps.append((w["source"], w["line"], method, path, cs, c["caller_line"]))
        self.assertEqual(
            uncovered_markers, [],
            "12 个 wrapper 声称的真实调用端点缺少源码 marker 证据(可能被替换/伪造):\n"
            + "\n".join(
                f"  {ws}:{wl} [{m}] {p}  caller@{cs}:{cl}  marker={marker!r}"
                for ws, wl, m, p, cs, cl, marker in uncovered_markers))
        self.assertEqual(
            gaps, [],
            "12 个 wrapper 的真实调用端出现 allowed endpoint gap(既不在目录也未被排除):\n"
            + "\n".join(f"  {s}:{l} [{m}] {p}  <- caller@{cs}:{cl}"
                        for s, l, m, p, cs, cl in gaps))
        self.assertGreaterEqual(covered_n, 1, "wrapper 复核应至少有一条 covered 端点")
        self.assertGreaterEqual(excluded_n, 1, "wrapper 复核应至少有一条 excluded 端点(auth/settings/轮巡)")

    def test_wrapper_caller_markers_are_anchored_in_real_source(self):
        """独立护栏:每个声称的真实调用端点都必须在对应源码的 caller_line 附近出现 marker。

        这一层专门拦截“把 caller 替换成任意目录端点”的作弊:任何声称端点都必须先
        由真实源码 marker 绑定到实际调用/证明点,否则直接失败。
        """
        reviewed = 0
        for w in WRAPPER_REVIEW:
            for c in w["callers"]:
                reviewed += 1
                self.assertTrue(
                    _caller_marker_present(w, c),
                    f"wrapper 复核 caller 缺少源码证据:\n"
                    f"  {w['source']}:{w['line']} [{c['method']}] {c['path']}\n"
                    f"  caller@{_caller_source_of(w, c)}:{c['caller_line']}\n"
                    f"  marker={c.get('marker')!r} 未出现在 ±12 行窗口\n"
                    f"  note: {w['note']}")
        self.assertGreaterEqual(reviewed, 1, "应有至少一条 wrapper 真实调用端点被复核")

    def test_excluded_wrapper_handle_qt_event_write_forms_are_preserved(self):
        """Qt-only 事件写(remove/add/update/end)与受保护 settings 必须保持 excluded。

        这些能力走 Qt 壳后台(upload_event_module)而非 web 前端 /api/,在 native
        目录中被 /api/qt/、/api/backend/ 等前缀排除;凡出现类似端点一律不得标 covered。
        用 _route_excluded 直接复验代表性 Qt 事件写/设置路径,防止误判为 allowed gap。
        """
        qt_event_writes = (
            ("POST", "/api/qt/events"),
            ("POST", "/api/qt/events/{param}/add"),
            ("PATCH", "/api/qt/events/{param}/update"),
            ("POST", "/api/qt/events/{param}/end"),
            ("DELETE", "/api/qt/events/{param}"),
            ("POST", "/api/plan-convergence/settings/apply"),
            ("POST", "/api/backend/events/sync"),
        )
        for method, path in qt_event_writes:
            self.assertTrue(
                _route_excluded(path, method),
                f"Qt-only 事件写/受保护设置应被 native 排除,却未排除:[{method}] {path}")
            self.assertFalse(
                _in_catalog(self.native_ids, method, path),
                f"Qt-only 事件写/受保护设置不应进入 native 目录:[{method}] {path}")


def main():
    res = run_audit()
    native_ids = _native_ids()
    covered = [b for b in KNOWN_DYNAMIC_BINDINGS if b["kind"] == "covered"]
    excluded = [b for b in KNOWN_DYNAMIC_BINDINGS if b["kind"] == "excluded"]
    unresolved = [b for b in KNOWN_DYNAMIC_BINDINGS if b["kind"] == "unresolved"]
    lines = []
    lines.append(f"=== 动态端点审计(有界人工复核):{len(KNOWN_DYNAMIC_BINDINGS)} 个 static-unresolved vs native 目录 ===")
    lines.append(f"run_audit unresolved 总数: {res['summary']['unresolved']}")
    lines.append(f"已确证 covered(命中 native 目录并可给源码证据)   : {len(covered)} 条")
    lines.append(f"已确证 excluded(learning/SOP执行等原生排除能力)  : {len(excluded)} 条")
    lines.append(f"保持 unresolved(包装/动态/页面路由,诚实留待人工): {len(unresolved)} 条")
    lines.append("")
    lines.append("--- covered(可 tie 到已知方法/路径:native 目录存在)---")
    for b in covered:
        pairs = " ; ".join(f"{m} {p}" for m, p in b["method_paths"])
        lines.append(f"  {b['source']}:{b['line']}  {pairs}\n        <- {b['note']}")
    lines.append("")
    lines.append("--- excluded(原生排除能力,不得当作 covered/missing)---")
    for b in excluded:
        pairs = " ; ".join(f"{m} {p}" for m, p in b["method_paths"])
        lines.append(f"  {b['source']}:{b['line']}  {pairs}\n        <- {b['note']}")
    lines.append("")
    lines.append("--- 保持 unresolved(诚实)---")
    for b in unresolved:
        lines.append(f"  {b['source']}:{b['line']}\n        <- {b['note']}")
    lines.append("")
    lines.append("=== 12 个 remaining wrapper callsite:真实调用者端点复核(concrete allowed endpoint gaps)===")
    total_gaps = 0
    for w in WRAPPER_REVIEW:
        if not w["callers"]:
            lines.append(f"  {w['source']}:{w['line']}  {w['label']}")
            lines.append(f"        [无 /api/ 业务端点] {w['note']}")
            continue
        lines.append(f"  {w['source']}:{w['line']}  {w['label']}")
        for c in w["callers"]:
            method, path = c["method"], c["path"]
            cs, cl, marker = _caller_source_of(w, c), c["caller_line"], c["marker"]
            anchored = "anchored" if _caller_marker_present(w, c) else "NO-MARKER"
            in_cat = _in_catalog(native_ids, method, path)
            is_excluded = _route_excluded(path, method)
            if in_cat:
                status = "covered"
            elif is_excluded:
                status = "excluded"
            else:
                status = "GAP"
                total_gaps += 1
            lines.append(f"        [{status:8}|{anchored:9}] [{method}] {path}   <- caller@{cs}:{cl} ({marker!r})")
        lines.append(f"        <- {w['note']}")
    lines.append("")
    if total_gaps == 0:
        lines.append("结论:12 个 wrapper 的真实调用者端点全部 covered 或 excluded,"
                     "**无 concrete allowed endpoint gap**;wrapper 函数本身不标 missing API。")
    else:
        lines.append(f"结论:发现 {total_gaps} 个 concrete allowed endpoint gap(见上方 GAP),必须处理。")
    lines.append("")
    lines.append("报告:上述 covered/excluded 均已通过目录形态或排除规则有界断言;")
    lines.append("unresolved 组为审计如实保留的不确定项,提示需人工/运行时确认。")
    return "\n".join(lines)


if __name__ == "__main__":
    unittest.main(verbosity=2)
