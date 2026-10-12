# -*- coding: utf-8 -*-
from __future__ import annotations

import re
import shutil
import subprocess
import sys
import unittest
from pathlib import Path

BIN = Path(__file__).resolve().parent
if str(BIN) not in sys.path:
    sys.path.insert(0, str(BIN))

from lan_bitable_template_portal.workbench_lite import (
    _WORK_ORDER_DOM_HELPERS,
    render_polling_work_order_page,
    render_polling_work_order_steps_page,
)


def _extract_function(page: str, pattern: str) -> str:
    match = re.search(pattern, page)
    if not match:
        raise AssertionError(f"function not found: {pattern!r}")
    return match.group(0)


_MOCK_JS = r"""
function makeNode(modes){
  const node={
    childNodes:[], textContent:'', className:'', hidden:false, disabled:false,
    type:'', value:'', dataset:{}, title:'',
    setAttribute(){}, setAttributeNS(){}, focus(){}, select(){}, remove(){},
    appendChild(child){this.childNodes.push(child);return child;},
    removeChild(child){const i=this.childNodes.indexOf(child);if(i<0)throw new Error('not a child');this.childNodes.splice(i,1);return child;},
  };
  Object.defineProperty(node,'firstChild',{get(){return this.childNodes[0]||null;}});
  if(modes&&modes.native){node.replaceChildren=(...children)=>{node.childNodes.splice(0,node.childNodes.length,...children);};}
  if(modes&&modes.append){node.append=(...children)=>{node.childNodes.push(...children);};}
  return node;
}
function scenario(modes){
  const ids={
    role:makeNode(), title:makeNode(), summary:makeNode(), status:makeNode(),
    retry:makeNode(), 'cancel-selection':makeNode(), back:makeNode(),
    overview:makeNode(modes), steps:makeNode(modes),
  };
  return {
    ids,
    q:id=>ids[id],
    document:{title:'',getElementById:id=>ids[id],createElement:()=>makeNode()},
  };
}
"""


class WorkOrderDomCompatibilityTests(unittest.TestCase):
    """Regression coverage for both polling work-order pages in older
    embedded Feishu browsers.

    The two pages share one injected JS helper source (_WORK_ORDER_DOM_HELPERS)
    that provides appendChildrenSafe() (appendChild-only, never .append()) and
    replaceChildrenSafe() (native replaceChildren when available, otherwise
    clear-then-appendChild). The tests load that same shared constant as
    production does and exercise the real overview/steps initial render and
    re-render with native replaceChildren present and with append missing.
    """

    def _require_node(self):
        node = shutil.which("node")
        if not node:
            self.skipTest("node is unavailable")
        return node

    def test_helpers_are_shared_and_injected_before_page_js(self) -> None:
        overview = render_polling_work_order_page()
        steps = render_polling_work_order_steps_page()
        for page in (overview, steps):
            self.assertIn(_WORK_ORDER_DOM_HELPERS, page)
            self.assertEqual(page.count("function appendChildrenSafe"), 1)
            self.assertEqual(page.count("function replaceChildrenSafe"), 1)
            first_helper = page.index("function appendChildrenSafe")
            marker = "function card(" if "card(" in page else "function workOrderCard("
            self.assertLess(first_helper, page.index(marker))
        self.assertNotIn("globalThis", overview)
        self.assertNotIn("globalThis", steps)

    def test_no_element_append_or_replaceChildren_directly(self) -> None:
        overview = render_polling_work_order_page()
        steps = render_polling_work_order_steps_page()
        for page in (overview, steps):
            self.assertNotIn(".append(", page)
            self.assertNotIn("q('overview').replaceChildren(", page)
            self.assertNotIn("q('steps').replaceChildren(", page)

    def test_page_functions_preserved(self) -> None:
        overview = render_polling_work_order_page()
        steps = render_polling_work_order_steps_page()
        self.assertIn("token", overview)
        self.assertIn("timer=setInterval(load,3000)", overview)
        self.assertIn("/polling-work-order/steps?token=", overview)
        for fragment in (
            "timer=setInterval(load,3000)",
            "clock=setInterval(refreshCountdown,1000)",
            "/api/polling-work-orders/confirm",
            "/api/polling-work-orders/photo",
            "/api/polling-work-orders/rollback",
            "function uploadStepPhoto",
            "function confirmStep",
            "function rollbackPrevious",
        ):
            self.assertIn(fragment, steps)

    def test_overview_initial_and_rerender_native_and_fallback(self) -> None:
        node = self._require_node()
        overview = render_polling_work_order_page()
        work_order_card = _extract_function(overview, r"function workOrderCard\(order\).*")
        render_fn = _extract_function(overview, r"function render\(data\).*")
        js = _build_overview_script(_WORK_ORDER_DOM_HELPERS, work_order_card, render_fn)
        result = subprocess.run(
            [node, "-e", js], capture_output=True, text=True, encoding="utf-8"
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_steps_initial_and_rerender_native_and_fallback(self) -> None:
        node = self._require_node()
        steps = render_polling_work_order_steps_page()
        card_fn = _extract_function(steps, r"function card\(step\).*")
        render_fn = _extract_function(steps, r"function render\(data\).*")
        js = _build_steps_script(_WORK_ORDER_DOM_HELPERS, card_fn, render_fn)
        result = subprocess.run(
            [node, "-e", js], capture_output=True, text=True, encoding="utf-8"
        )
        self.assertEqual(result.returncode, 0, result.stderr)


def _build_base_js(helpers: str) -> str:
    return "\n".join(
        [
            "const assert=require('node:assert/strict');",
            helpers,
            "let q, document, current=null, timer=0, busy=false;",
            "const clearInterval=()=>{};",
            "const status=(text)=>{return message=text;};",
            "let message='';",
            _MOCK_JS,
        ]
    )


def _build_overview_script(helpers: str, work_order_card: str, render_fn: str) -> str:
    return "\n".join(
        [
            _build_base_js(helpers),
            "const openWorkOrder=()=>{};",
            work_order_card,
            render_fn,
            r"""
function run(mode){
  const sc=scenario({native:'native'===mode||'both'===mode,append:'both'===mode});
  q=sc.q; document=sc.document;
  current=null; timer=0; busy=false;
  let nativeCalls=0;
  if(mode==='native'||mode==='both'){
    sc.ids.overview.replaceChildren=(...children)=>{nativeCalls++;sc.ids.overview.childNodes.splice(0,sc.ids.overview.childNodes.length,...children);};
  }
  render({
    work_type:'maintenance', role_label:'操作员', assigned_person:{name:'张三'},
    title:'测试总览', sop_name:'SOP', state:'active',
    work_orders:[
      {run_index:1,label:'工单A',completed_steps:1,step_count:3,state:'available',selectable:true},
      {run_index:2,label:'工单B',completed_steps:0,step_count:4,state:'available',selectable:true},
    ],
  });
  assert.equal(sc.ids.overview.childNodes.length,2,'initial card count');
  assert.equal(sc.ids.overview.childNodes[0].childNodes[0].textContent,'工单 1');
  assert.equal(sc.ids.title.textContent,'测试总览');
  assert.equal(sc.ids.retry.hidden,true,'retry hidden while active');
  if(mode==='native'){assert.equal(nativeCalls,1,'native path used once on first render');}
  render({
    work_type:'maintenance', role_label:'操作员', assigned_person:{name:'张三'},
    title:'测试总览', sop_name:'SOP', state:'completed',
    work_orders:[
      {run_index:3,label:'工单C',completed_steps:2,step_count:2,state:'completed',selectable:false},
    ],
  });
  assert.equal(sc.ids.overview.childNodes.length,1,'rerender card count');
  assert.equal(sc.ids.overview.childNodes[0].childNodes[0].textContent,'工单 3');
  assert.equal(sc.ids.retry.hidden,true,'retry hidden when completed');
  if(mode==='native'){assert.equal(nativeCalls,2,'native path used on rerender');}
  if(mode==='native'){assert.ok(nativeCalls>=2,'replaceChildren was the fallback-proof path only on native');}
}
run('native');
run('fallback');
// fallback explicitly also lacks .append on created nodes: appendChildrenSafe uses appendChild
console.log('OVERVIEW-OK');
""",
        ]
    )


def _build_steps_script(helpers: str, card_fn: str, render_fn: str) -> str:
    return "\n".join(
        [
            _build_base_js(helpers),
            "let runIndex=1;",
            "const goOverview=()=>{};",
            "const goSelected=()=>{};",
            "const remaining=()=>0;",
            "const confirmStep=()=>{};",
            "const uploadStepPhoto=()=>{};",
            "const rollbackPrevious=()=>{};",
            card_fn,
            render_fn,
            r"""
function run(mode){
  const sc=scenario({native:'native'===mode||'both'===mode,append:'both'===mode});
  q=sc.q; document=sc.document;
  current={role:'operator',role_label:'操作员',can_rollback_previous:false};
  runIndex=1; timer=0; busy=false;
  let nativeCalls=0;
  if(mode!=='fallback'){
    sc.ids.steps.replaceChildren=(...children)=>{nativeCalls++;sc.ids.steps.childNodes.splice(0,sc.ids.steps.childNodes.length,...children);};
  }
  const step1={run_index:1,step_index:1,position:'current',content:'检查设备',run_label:'作业',
    operator_required:true,reviewer_required:false,operator_confirmed:false,reviewer_confirmed:false,
    photo_required:false,photos:[],remaining_seconds:30,step_key:'1:1'};
  const step2={run_index:1,step_index:2,position:'next',content:'复核',run_label:'作业',
    operator_required:true,reviewer_required:false,operator_confirmed:false,reviewer_confirmed:false,
    photo_required:false,photos:[],remaining_seconds:0,step_key:'1:2'};
  render({
    current_run_index:1, work_type:'maintenance', role_label:'操作员',
    assigned_person:{name:'李四'}, title:'步骤页', sop_name:'SOP',
    work_orders:[{run_index:1,label:'工单1'}],
    steps:[step1,step2],
  });
  assert.equal(sc.ids.steps.childNodes.length,2,'initial step count');
  assert.equal(sc.ids.steps.childNodes[0].className,'step current');
  assert.equal(sc.ids.title.textContent,'步骤页');
  assert.equal(sc.ids.steps.childNodes[0].childNodes.length>=5,true,'current step includes header/run/content/checks/confirm');
  if(mode!=='fallback'){assert.equal(nativeCalls,1,'native path used once on first render');}
  render({
    current_run_index:1, work_type:'maintenance', role_label:'操作员',
    assigned_person:{name:'李四'}, title:'步骤页', sop_name:'SOP',
    work_orders:[{run_index:1,label:'工单1'}],
    steps:[step2],
  });
  assert.equal(sc.ids.steps.childNodes.length,1,'rerender step count');
  assert.equal(sc.ids.steps.childNodes[0].className,'step next');
  if(mode!=='fallback'){assert.equal(nativeCalls,2,'native path used on rerender');}
}
run('native');
run('fallback');
console.log('STEPS-OK');
""",
        ]
    )


if __name__ == "__main__":
    unittest.main()