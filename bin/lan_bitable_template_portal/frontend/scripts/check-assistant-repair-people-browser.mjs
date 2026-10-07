import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const python = path.join(root, 'bin/.venv', process.platform === 'win32' ? 'Scripts/python.exe' : 'bin/python');
const synthesized = spawnSync(python, ['-c', `import sys,json
sys.path.insert(0,'bin')
from lan_bitable_template_portal.lighthouse_api import _repair_frontend_fields
metas=[
  {'field_name':'维修负责人','field_type':11,'editable':True,'options':[]},
  {'field_name':'维修链接','field_type':15,'editable':True,'options':[]},
  {'field_name':'只读文本','field_type':1,'editable':False,'options':[]},
  {'field_name':'所属专业','field_type':3,'editable':True,'options':['电气','暖通']},
]
print(json.dumps(_repair_frontend_fields(metas, unlinked=True, scope='D'), ensure_ascii=False))
`], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8', PYTHONWARNINGS: 'ignore' } });
if (synthesized.status !== 0) throw new Error(synthesized.stderr || `python exited ${synthesized.status}`);
const control = JSON.parse(synthesized.stdout);
assert.equal(control.native_repair, true);
assert.equal(control.type, 'object');
const children = Object.fromEntries(control.children.map(child => [child.path, child]));
assert.equal(children['维修负责人'].native_repair_people, true);
assert.equal(children['维修负责人'].scope, 'D');
assert.equal(children['维修链接'].repair_field?.field_type, 15);
assert.equal(children['维修链接'].native_repair_people, undefined);
assert.equal('只读文本' in children, false);

const output = path.join(root, 'output/playwright/assistant-repair-people');
await mkdir(output, { recursive: true });
const base = 'http://127.0.0.1:19003';
const browser = await chromium.launch({ headless: true });

// Readonly original field lives in the initial plan value (NOT a rendered editable
// child) and must be preserved verbatim on every submit.
const READONLY_TEXT = '原始只读内容';

function buildPlan(people, version = 1) {
  const value = {
    '维修负责人': people || [{ id: 'ou_1', name: '张三', employee_no: 'E001' }],
    '维修链接': { link: 'https://example.com/a', text: '验收链接' },
    '所属专业': '电气',
    '只读文本': READONLY_TEXT,
  };
  return {
    id: 'repair-people',
    title: '维修单信息',
    status: 'needs_input',
    version,
    fields: [{
      name: 'step0.fields',
      path: 'fields',
      label: '维修单信息',
      section: 'body',
      operation_index: 0,
      required: false,
      value,
      ...control,
    }],
    operations: [],
    results: [],
  };
}

try {
  for (const width of [1440, 390]) {
    const context = await browser.newContext({ viewport: { width, height: 1000 } });
    assert.equal((await (await context.request.get(base + '/api/health')).json()).instance_id, 'isolated-lighthouse-stream');
    const page = await context.newPage();
    const errors = [];
    const saved = [];
    const scopes = [];
    page.on('pageerror', error => errors.push(error.message));

    let plan = buildPlan();
    let selection = []; // latest submitted structured people

    await page.route('**/api/assistant/conversation', route => route.fulfill({ json: {
      ok: true,
      data: { conversation_id: 'repair-people', configured: true, enabled: true, busy: false,
        turns: [{ operation_id: 'repair-people-op', question: '登记维修单', answer: '请补充以下维修单信息。', status: 'completed', plan }] },
    } }));

    // Native people contract carries user_id, not a fabricated id key. The initially
    // pre-selected baseline still uses the legacy id key (existing baseline id).
    const PEOPLE = [
      { user_id: 'ou_1', name: '张三', employee_no: 'E001', building: 'A栋', position: '主管', selectable: true },
      { user_id: 'ou_2', name: '李四', employee_no: 'E002', building: 'B栋', position: '技术员', selectable: true },
      { name: '资料不全用户', employee_no: '', building: '', position: '', selectable: false },
    ];
    const RAW_IDS = ['ou_1', 'ou_2', 'ou_3'];

    await page.route('**/api/repair-management/people?*', route => {
      const url = new URL(route.request().url());
      const scope = url.searchParams.get('scope');
      const q = url.searchParams.get('q');
      assert.equal(scope, 'D', `people scope must be D, got ${scope}`);
      assert.notEqual(q, null, 'people request must carry q');
      scopes.push(scope);
      const query = String(q || '').trim().toLowerCase();
      const people = query
        ? PEOPLE.filter(p => String(p.name || '').toLowerCase().includes(query))
        : PEOPLE;
      return route.fulfill({ json: { ok: true, data: { people } } });
    });

    await page.route('**/api/assistant/plans/repair-people', route => {
      assert.equal(route.request().method(), 'PATCH');
      const body = route.request().postDataJSON();
      if (body.action === 'edit') {
        // Return-edit must send the current plan.version and must NOT reset it to 1.
        assert.equal(body.version, plan.version, 'edit body.version must match current plan.version');
        plan = buildPlan(selection, plan.version + 1);
        return route.fulfill({ json: { ok: true, data: plan } });
      }
      // Submit
      const values = body.values['step0.fields'];
      assert.ok(values && typeof values === 'object' && !Array.isArray(values), 'step0.fields must be an object');
      assert.equal(values['只读文本'], READONLY_TEXT, 'readonly original field must be preserved on submit');
      const picked = values['维修负责人'];
      assert.ok(Array.isArray(picked), '维修负责人 must be an array, got ' + JSON.stringify(picked));
      for (const p of picked) {
        assert.equal(typeof p.id, 'string', 'picked person missing id');
        assert.equal(typeof p.name, 'string', 'picked person missing name');
        assert.equal(typeof p.employee_no, 'string', 'picked person missing employee_no');
        assert.notEqual(typeof p, 'string', 'person must not be a raw JSON string');
      }
      assert.deepEqual(values['维修链接'], { link: 'https://example.com/a', text: '验收链接' });
      assert.equal(body.version, plan.version, 'submit body.version must match current plan.version');
      saved.push(body);
      selection = picked;
      plan = {
        ...plan,
        status: 'awaiting_confirmation',
        version: plan.version + 1,
        can_edit: true,
        fields: [],
        operations: [{
          api_id: 'POST /api/repair-management/records',
          name: '保存维修单',
          body: { scope: 'D', fields: { '维修负责人': picked.map(p => ({ id: p.id })) } },
          selected_labels: { '维修负责人': picked.map(p => p.name).join('、'), scope: 'D' },
        }],
      };
      return route.fulfill({ json: { ok: true, data: plan } });
    });

    await page.goto(base);
    await page.getByRole('button', { name: '打开灯塔助手', exact: true }).click();
    const form = page.locator('.plan-form');
    await form.waitFor();

    const picker = form.locator('.repair-people-picker');
    assert.equal(await picker.count(), 1, 'exactly one native repair people picker expected');
    const search = page.getByLabel('维修负责人', { exact: true });
    assert.equal(await search.getAttribute('type'), 'search', 'people picker search must be type=search');

    // type-15 URL stays a text field, not a picker.
    const urlField = page.getByLabel('维修链接', { exact: true });
    assert.equal(await urlField.getAttribute('data-repair-control'), '', 'URL must render as repair text field');
    assert.equal(await urlField.getAttribute('type'), 'text');
    assert.equal(await urlField.inputValue(), 'https://example.com/a');

    // Default selected person has id/name.
    const chips = form.locator('.repair-person-chip');
    assert.equal(await chips.count(), 1);
    assert.equal(await chips.first().innerText(), '张三');
    assert.ok(await form.getByRole('button', { name: '移除张三', exact: true }).isVisible());

    // Open picker -> fetch scope=D, q=''.
    await search.focus();
    const results = form.locator('.repair-people-results');
    await results.waitFor();
    await page.waitForFunction(() => document.querySelectorAll('.repair-people-results [role=option]').length === 3);
    assert.deepEqual(scopes, ['D']);
    for (const selector of ['.repair-people-search', '.repair-people-popover']) {
      assert.equal(await picker.locator(selector).evaluate(el => getComputedStyle(el).backgroundColor), 'rgb(41, 50, 44)');
    }
    assert.equal(await urlField.evaluate(el => getComputedStyle(el).backgroundColor), 'rgb(41, 50, 44)');
    assert.equal(await results.locator('.repair-person-info b').first().evaluate(el => getComputedStyle(el).color), 'rgb(226, 232, 226)');
    assert.ok(await form.getByRole('option', { name: /李四/ }).isEnabled());
    assert.ok(await form.getByRole('option', { name: /资料不全/ }).isDisabled());
    assert.equal(await form.getByText('资料不完整', { exact: true }).count(), 1);

    // Multiselect: click a result option. Focus stays on that option.
    await form.getByRole('option', { name: /李四/ }).click();
    assert.equal(await chips.count(), 2);
    assert.ok(await form.getByRole('button', { name: '移除张三', exact: true }).isVisible());
    assert.ok(await form.getByRole('button', { name: '移除李四', exact: true }).isVisible());

    // Escape DIRECTLY on the focused option (NOT on the search input): the shared
    // root Escape must close only the open picker, keep the assistant open, and
    // retain the selected values (no search clearing involved).
    await page.keyboard.press('Escape');
    assert.equal(await form.locator('.repair-people-popover').count(), 0, 'Escape on focused option must close only the picker');
    assert.equal(await page.locator('.assistant-panel').count(), 1);
    assert.ok(await page.locator('.assistant-panel').isVisible());
    assert.equal(await chips.count(), 2, 'selected values retained after Escape on option');
    assert.ok(await form.getByRole('button', { name: '移除张三', exact: true }).isVisible());
    assert.ok(await form.getByRole('button', { name: '移除李四', exact: true }).isVisible());

    // Search still filters and selections persist.
    await search.focus();
    await page.waitForFunction(() => document.querySelectorAll('.repair-people-results [role=option]').length === 3);
    await search.fill('李');
    await page.waitForFunction(() => {
      const opts = [...document.querySelectorAll('.repair-people-results [role=option]')];
      return opts.length === 1 && opts[0].textContent.includes('李四');
    });
    assert.deepEqual(scopes.at(-1), 'D');
    assert.equal(await chips.count(), 2);
    assert.ok(await form.getByRole('button', { name: '移除张三', exact: true }).isVisible());
    assert.equal(await form.getByRole('option', { name: /资料不全/ }).count(), 0);

    // Search Escape closes the picker WITHOUT clearing the query.
    await page.keyboard.press('Escape');
    assert.equal(await form.locator('.repair-people-popover').count(), 0, 'Escape on search must close picker');
    assert.equal(await page.locator('.assistant-panel').count(), 1);
    assert.ok(await page.locator('.assistant-panel').isVisible());
    assert.equal(await search.inputValue(), '李', 'Escape on search must preserve query');

    // Reopen via ArrowDown (search still focused) then clear the query for the
    // removal/re-add cycle.
    await page.keyboard.press('ArrowDown');
    await search.fill('');
    await page.waitForFunction(() => document.querySelectorAll('.repair-people-results [role=option]').length === 3);

    // Removal then re-add.
    await form.getByRole('button', { name: '移除张三', exact: true }).click();
    assert.equal(await chips.count(), 1);
    assert.equal(await chips.first().innerText(), '李四');
    await form.getByRole('option', { name: /张三/ }).click();
    assert.equal(await chips.count(), 2);
    assert.ok(await form.getByRole('button', { name: '移除李四', exact: true }).isVisible());
    assert.ok(await form.getByRole('button', { name: '移除张三', exact: true }).isVisible());

    // No raw ids anywhere in visible text or in any JSON textarea.
    const visibleText = await page.evaluate(() => document.body.innerText || '');
    for (const id of RAW_IDS) assert.ok(!visibleText.includes(id), `raw id ${id} leaked into visible text`);
    const jsonTexts = await page.$$eval('textarea', els => els.map(el => el.value || ''));
    for (const id of RAW_IDS) for (const txt of jsonTexts) assert.ok(!txt.includes(id), `raw id ${id} leaked into JSON textarea`);

    // Nested overflow check: document, landing plan form AND the picker itself.
    const overflow = await page.evaluate(() => {
      const inView = (el) => {
        const r = el.getBoundingClientRect();
        return el.scrollWidth <= el.clientWidth && r.right <= innerWidth && r.left >= 0;
      };
      const formEl = document.querySelector('.plan-form');
      const pickerEl = document.querySelector('.repair-people-picker');
      return {
        document: document.documentElement.scrollWidth <= innerWidth,
        form: !formEl || inView(formEl),
        picker: !pickerEl || inView(pickerEl),
      };
    });
    assert.equal(overflow.document, true, `no document horizontal overflow at ${width}px`);
    assert.equal(overflow.form, true, `no landing form horizontal overflow at ${width}px`);
    assert.equal(overflow.picker, true, `no repair people picker horizontal overflow at ${width}px`);

    await page.screenshot({ path: path.join(output, `repair-people-${width}.png`), fullPage: true });

    // Close any open picker before submitting, then submit structured people
    // (id/name/employee_no, not raw JSON typing).
    await page.keyboard.press('Escape');
    assert.equal(await form.locator('.repair-people-popover').count(), 0, 'picker must close before submit');
    assert.equal(await page.locator('.assistant-panel').count(), 1);
    assert.ok(await page.locator('.assistant-panel').isVisible());
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 1);
    assert.equal(saved[0].values['step0.fields']['只读文本'], READONLY_TEXT, 'readonly field preserved on first submit');
    assert.deepEqual(saved[0].values['step0.fields']['维修负责人'], [
      { id: 'ou_2', name: '李四', employee_no: 'E002' },
      { id: 'ou_1', name: '张三', employee_no: 'E001' },
    ]);
    assert.deepEqual(selection.map(p => p.id), ['ou_2', 'ou_1']);

    // Return-edit must keep a monotonic plan.version (never reset to 1); the edit
    // request version was already asserted inside the route handler.
    await page.getByRole('button', { name: '返回修改', exact: true }).click();
    await form.waitFor();
    assert.equal(plan.version, 3, 'return-edit advances the version like the native backend');
    assert.equal(await chips.count(), 2);
    assert.ok(await form.getByRole('button', { name: '移除李四', exact: true }).isVisible());
    assert.ok(await form.getByRole('button', { name: '移除张三', exact: true }).isVisible());

    // Reopen edit after reload: names persisted, still edit-mode (needs_input).
    await page.reload();
    await form.waitFor();
    assert.equal(await chips.count(), 2);
    assert.ok(await form.getByRole('button', { name: '移除李四', exact: true }).isVisible());
    assert.ok(await form.getByRole('button', { name: '移除张三', exact: true }).isVisible());
    assert.equal(await chips.first().innerText(), '李四');

    // Second submit: version continues monotonic (now 3, matching the current
    // plan.version) and the readonly original field is preserved again.
    await form.getByRole('button', { name: '补充并继续', exact: true }).click();
    await page.getByRole('button', { name: '确认操作清单', exact: true }).waitFor();
    assert.equal(saved.length, 2);
    assert.equal(saved[1].values['step0.fields']['只读文本'], READONLY_TEXT, 'readonly field preserved on second submit');
    assert.deepEqual(saved[1].values['step0.fields']['维修负责人'], [
      { id: 'ou_2', name: '李四', employee_no: 'E002' },
      { id: 'ou_1', name: '张三', employee_no: 'E001' },
    ]);
    assert.equal(plan.version, 4, 'plan version must increment monotonically after second submit');

    assert.deepEqual(errors, []);
    await context.close();
    console.log(`RepairPeoplePicker ${width}px: names/scope/search/multiselect/removal/persist/Escape-on-option/Escape-preserves-query/readonly-preserved/monotonic-version/no-raw-id/nested-overflow/reopen-reload OK`);
  }
} finally {
  await browser.close();
}
