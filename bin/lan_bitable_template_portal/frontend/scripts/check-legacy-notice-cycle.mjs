import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const repo = path.resolve(root, '../../..');
const generated = spawnSync(path.join(repo, 'bin/.venv/Scripts/python.exe'), ['-c', `
import json,sys
sys.path.insert(0,'bin')
from lan_bitable_template_portal.workbench_lite import render_workbench_lite
from test_lighthouse_notice_command_regression import VALID_STARTS
pages={}
for kind,draft in VALID_STARTS.items():
    pages[kind]=render_workbench_lite(payload={'records':[],'ongoing':[]},session={'role':'building','allowed_modules':list(VALID_STARTS)},scope='A',work_type=kind,manual=True,prefill_draft={**draft,'maintenance_cycle':'每月','content':'季度保养','reason':'按半年度计划维护'})
print(json.dumps(pages,ensure_ascii=False))
`], { cwd: repo, encoding: 'utf8', maxBuffer: 20 * 1024 * 1024, env: { ...process.env, PYTHONIOENCODING: 'utf-8' } });
assert.equal(generated.status, 0, generated.stderr);
const pages = JSON.parse(generated.stdout);
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  await page.route('**/*', route => route.abort());
  for (const [kind, html] of Object.entries(pages)) {
    const logic = html.match(/function updateMaintenanceCycleWarnings\(form\) \{[\s\S]*?(?=\s*function updateNoticePreview)/)?.[0];
    assert(logic);
    await page.setContent(html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/gi, ''));
    await page.addScriptTag({ content: `function previewValue(form,key){return String(form.querySelector('[name="'+key+'"]')?.value||'');}\n${logic}` });
    const result = await page.evaluate(kind => {
      const form = document.querySelector('#lite-notice-form');
      if (!form) throw new Error('Fixture notice form missing');
      const warnings = window.updateMaintenanceCycleWarnings(form);
      const controls = ['content', 'reason'].map(name => form.querySelector(`[name="${name}"]`));
      const initial = controls.map(control => !!control?.classList.contains('cycle-mismatch'));
      const text = controls.map(control => control?.value);
      const disabled = [...form.querySelectorAll('[name="submit_action"]')].map(button => button.disabled);
      if (kind !== 'maintenance') return { warnings, initial, rules: form.hasAttribute('data-maintenance-cycle-rules') };
      const actionResults = ['start', 'update', 'end'].map(action => { form.dataset.action = action; return window.updateMaintenanceCycleWarnings(form).length; });
      controls.forEach(control => { control.value = '按月度计划维护'; });
      const fixed = window.updateMaintenanceCycleWarnings(form);
      const fixedRed = controls.some(control => control.classList.contains('cycle-mismatch'));
      controls[0].value = '半年度维护'; form.querySelector('[name="maintenance_cycle"]').value = '半年';
      controls[1].value = '每半年维护'; const half = window.updateMaintenanceCycleWarnings(form);
      return { warnings, initial, text, actionResults, fixed, fixedRed, half,
        disabledUnchanged: JSON.stringify(disabled) === JSON.stringify([...form.querySelectorAll('[name="submit_action"]')].map(button => button.disabled)),
        validityMessages: controls.map(control => control.validationMessage),
        hidden: form.querySelector('#lite-cycle-warning').hidden };
    }, kind);
    if (kind === 'maintenance') {
      assert.equal(result.warnings.length, 2); assert.deepEqual(result.initial, [true, true]);
      assert.deepEqual(result.text, ['季度保养', '按半年度计划维护']); assert.deepEqual(result.actionResults, [2, 2, 2]);
      assert.deepEqual(result.fixed, []); assert.equal(result.fixedRed, false); assert.deepEqual(result.half, []);
      assert.equal(result.hidden, true); assert(result.disabledUnchanged); assert.deepEqual(result.validityMessages, ['', '']);
    } else { assert.deepEqual(result.warnings, []); assert(result.initial.every(value => !value)); assert.equal(result.rules, false); }
  }
  console.log('Legacy maintenance cycle warnings: rendered forms, all actions, reversible red borders, untouched text/buttons, other types excluded passed');
} finally { await browser.close(); }
