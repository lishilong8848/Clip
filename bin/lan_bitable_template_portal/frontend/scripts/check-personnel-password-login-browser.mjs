import assert from 'node:assert/strict';
import { mkdir } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { chromium } from 'playwright';
import { preview } from 'vite';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const output = path.resolve(root, '../../../output/playwright/personnel-password-login');
await mkdir(output, { recursive: true });
const server = await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0 } });
const origin = `http://127.0.0.1:${server.httpServer.address().port}`;
const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
page.setDefaultTimeout(12000);
const errors = [];
const loginPosts = [];
const peoplePosts = [];
const resetPosts = [];
const changePosts = [];
const resetAccounts = new Set();
let feishuAuthStatus = null;
let changeAuthUser = null;
let failFirstLogin = false;
let gateLogin = null;
let gateReset = null;
let gateChange = null;
let unsafeRedirectNext = false;
let authStatusLoggedInAsPassword = false;
let authStatusCallCount = 0;
let moduleAuthExpired = false;
let malformedResetVerify = false;
let resetServerError503 = false;

const people = [
  { id: 'recZhangsanA', name: '张三', employee_no: 'EMP-1001', building: 'A楼', selectable: true, needs_setup: true },
  { id: 'recZhangsanB', name: '张三', employee_no: 'EMP-1002', building: 'B楼', selectable: true, needs_setup: false },
  { id: 'recLisi', name: '李四', employee_no: 'EMP-2001', building: 'A楼', selectable: false, disabled_reason: '已离职', needs_setup: false },
  { id: 'recWangwu', name: '王五', employee_no: 'EMP-3001', building: 'C楼', selectable: true, needs_setup: true },
];
// Mutable server-side view (used to reflect needs_setup=false after a successful reset).
const peopleDb = people.map(p => ({ account_nature: '外部账号', ...p }));
peopleDb.push({ id: 'recInternal', name: '内部人员', employee_no: 'EMP-4001', building: 'D楼', selectable: true, needs_setup: false, account_nature: 'VNET' });

const loginJson = ({ person_id, password: pass, new_password, next }) => {
  const body = { person_id, password: pass, next };
  if (new_password !== undefined) body.new_password = new_password;
  return body;
};
// Synthetic six-character initial password (national-ID-last6 shape, no real identity data).
const initialPwd = '12345X';
// Synthetic full national-ID for identity verification (18 chars, no real identity data).
const validIdentity = '110101199001010000';
// A syntactically complete but wrong 18-char national ID for auto-verify mismatch.
const wrongFullId = '999999999999999999';

page.on('pageerror', error => errors.push(error.message));
await page.route('**/api/**', async route => {
  const req = route.request(), url = new URL(req.url());
  const ok = data => route.fulfill({ json: { ok: true, data } });
  if (url.pathname === '/api/auth/status') {
    authStatusCallCount++;
    if (feishuAuthStatus) {
      return ok({
        logged_in: true,
        user: feishuAuthStatus.user,
        scope_options: feishuAuthStatus.scopeOptions,
        login_url: '/api/auth/login',
      });
    }
    if (changeAuthUser) {
      return ok({
        logged_in: true,
        user: changeAuthUser,
        scope_options: [{ value: 'A', label: 'A楼' }],
        login_url: '/api/auth/login',
      });
    }
    if (authStatusLoggedInAsPassword && authStatusCallCount === 1) {
      return ok({
        logged_in: true,
        user: { open_id: 'ou_sessionexpiry', name: '张三', login_method: 'password' },
        scope_options: [{ value: 'A', label: 'A楼' }],
        login_url: '/api/auth/login',
      });
    }
    return ok({ logged_in: false });
  }
  if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-personnel-password' } });
  if (moduleAuthExpired && (url.pathname === '/api/scope-overview' || url.pathname === '/api/handover-links')) {
    return route.fulfill({ status: 401, json: { error: '登录已过期，请重新扫码登录。', auth_required: true } });
  }
  if (url.pathname === '/api/auth/password/people') {
    peoplePosts.push({ method: req.method(), path: url.pathname });
    return ok({ items: peopleDb, loaded_at: 1791510000 });
  }
  if (url.pathname === '/api/auth/password/login') {
    const body = req.postDataJSON();
    loginPosts.push({ method: req.method(), path: url.pathname, body });
    try {
      if (gateLogin) await gateLogin;
      if (failFirstLogin && !('new_password' in body)) {
        return await route.fulfill({ status: 401, json: { ok: false, error: '密码错误，请重试。' } });
      }
      const person = peopleDb.find(p => p.id === body.person_id);
      const setupDone = resetAccounts.has(body.person_id) || person?.needs_setup === false;
      if (!person || setupDone || 'new_password' in body) {
        return await route.fulfill({ json: { ok: true, data: { redirect_url: unsafeRedirectNext ? '/\\\\evil.example' : '/?scope=A' } } });
      }
      return await route.fulfill({ json: { ok: true, data: { requires_password_change: true } } });
    } catch {
      // Client aborted the in-flight request (e.g. component unmounted) before fulfill.
    }
  }
  if (url.pathname === '/api/auth/password/reset') {
    const body = req.postDataJSON();
    resetPosts.push({ method: req.method(), path: url.pathname, body });
    try {
      if (gateReset) await gateReset;
      const person = peopleDb.find(p => p.id === body.person_id);
      if (!person || String(body.identity_number || '').trim().toUpperCase() !== validIdentity) {
        return await route.fulfill({ status: 401, json: { ok: false, error: '身份证号与所选人员不匹配，请重新核验。' } });
      }
      if (resetServerError503) {
        return await route.fulfill({ status: 503, json: { ok: false, error: '服务繁忙，请稍后重试。' } });
      }
      if ('new_password' in body) {
        resetAccounts.add(body.person_id);
        const target = peopleDb.find(p => p.id === body.person_id);
        if (target) target.needs_setup = false;
        return await route.fulfill({ json: { ok: true, data: { password_changed: true } } });
      }
      // HTTP 200 but no requires_password_change:true must NOT mark identity as passed.
      if (malformedResetVerify) {
        return await route.fulfill({ json: { ok: true, data: {} } });
      }
      return await route.fulfill({ json: { ok: true, data: { requires_password_change: true } } });
    } catch {
      // Client aborted the in-flight reset (e.g. automatic verification superseded) before fulfill.
    }
  }
  if (url.pathname === '/api/auth/password/change') {
    const body = req.postDataJSON();
    changePosts.push({ method: req.method(), path: url.pathname, body });
    try {
      if (gateChange) await gateChange;
      if (!body.current_password || body.current_password !== 'correctOld') {
        return await route.fulfill({ status: 401, json: { ok: false, error: '原密码错误，请重试。' } });
      }
      return await route.fulfill({ json: { ok: true, data: { password_changed: true } } });
    } catch {
      // Client aborted the in-flight change (e.g. unmounted) before fulfill.
    }
  }
  return ok({});
});

try {
  const labelFor = id => {
    const p = peopleDb.find(item => item.id === id);
    return p ? [p.name, p.building, p.employee_no].filter(Boolean).join(' · ') : '';
  };
  function personPicker(selector) {
    const field = page.locator(selector);
    field.selectOption = async id => {
      await field.fill(labelFor(id));
      if (id) await page.getByRole('option', { name: labelFor(id), exact: true }).click();
    };
    return field;
  }
  async function openForm() {
    await page.goto(origin + '/');
    const passwordSelect = page.locator('#personnel-name-select');
    await passwordSelect.waitFor();
    await passwordSelect.waitFor();
  }

  async function openResetForm() {
    await openForm();
    await page.getByRole('button', { name: '忘记密码', exact: true }).click();
    await page.locator('#personnel-reset-name').waitFor();
    await page.locator('#personnel-reset-name').waitFor();
  }

  const select = () => personPicker('#personnel-name-select');
  const resetSelect = () => personPicker('#personnel-reset-name');
  const identity = () => page.locator('#personnel-identity-number');

  // --- Block 0: direct form, no toggle to unfold ---
  await openForm();
  assert.equal(await page.locator('#personnel-name-select').count(), 1, '姓名下拉应直接显示');
  assert.equal(await page.locator('#personnel-password').count(), 1, '密码输入应直接显示');
  assert.equal(await page.getByRole('button', { name: '姓名密码登录', exact: true }).count(), 1, '姓名密码登录提交按钮应直接显示');
  assert.equal(await page.getByRole('button', { name: '忘记密码', exact: true }).count(), 1, '登录页应显示“忘记密码”而非“修改密码”');

  // --- Block 1: application link + reminder + dropdown identities / duplicates / disabled ---
  await openForm();
  const link = page.locator('.app-link');
  assert.equal(await link.getAttribute('href'), 'https://applink.feishu.cn/T9aqNt3i8fXN');
  assert.equal(await link.getAttribute('target'), '_blank');
  assert.equal(await link.getAttribute('rel'), 'noopener noreferrer');
  assert.match(String(await link.textContent()), /在登录前需先点击此链接并申请使用飞书应用，如已经申请就无需重复申请。/);
  await select().click();
  const options = page.getByRole('option');
  assert.equal(await options.count(), 4, 'three external people and one internal person');
  assert.equal(await page.getByRole('option', { name: '张三 · A楼 · EMP-1001' }).count(), 1);
  assert.equal(await page.getByRole('option', { name: '张三 · B楼 · EMP-1002' }).count(), 1);
  assert.equal(await page.getByRole('option', { name: '王五 · C楼 · EMP-3001' }).count(), 1);
  assert.equal(await page.getByRole('option', { name: /李四/ }).count(), 0, 'departed people are not selectable');
  await page.screenshot({ path: path.join(output, '01-dropdown.png'), fullPage: true, animations: 'disabled' });

  // --- Block 2: first-time national-ID-last6 hint conditional + empty selection error ---
  await openForm();
  await select().selectOption('recZhangsanA');
  await page.getByText('首次登录密码为本人身份证号后6位，验证后须立即修改', { exact: true }).waitFor();
  await select().selectOption('recZhangsanB');
  assert.equal(await page.getByText('首次登录密码为本人身份证号后6位，验证后须立即修改', { exact: true }).count(), 0,
    'needs_setup=false 的人员不应显示初始密码提示');
  await select().selectOption('');
  await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
  await page.getByText('请先选择姓名。', { exact: true }).waitFor();
  assert.equal(loginPosts.length, 0, '未选择人员不应发起登录请求');
  await page.screenshot({ path: path.join(output, '02-hint-and-empty.png'), fullPage: true, animations: 'disabled' });

  // --- Block 3: valid initial response shows first-change form without navigation; change person clears secrets ---
  await openForm();
  await select().selectOption('recZhangsanA');
  await page.getByText('首次登录密码为本人身份证号后6位，验证后须立即修改', { exact: true }).waitFor();
  await page.locator('#personnel-password').fill(initialPwd);
  const beforeFirst = loginPosts.length;
  await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
  await page.locator('#personnel-new-password').waitFor();
  assert.equal(page.url(), origin + '/', '首个请求返回需改密时不应跳转');
  assert.equal(loginPosts.length, beforeFirst + 1);
  assert.deepEqual(loginJson(loginPosts[loginPosts.length - 1].body),
    { person_id: 'recZhangsanA', password: initialPwd, next: '/' });
  assert.equal(await page.locator('#personnel-password').inputValue(), initialPwd, '原密码保留用于二次提交');
  // change person resets secrets and first-step state
  await select().selectOption('recZhangsanB');
  assert.equal(await page.locator('#personnel-new-password').count(), 0, '切换人员应隐藏改密表单');
  assert.equal(await page.locator('#personnel-password').inputValue(), '');
  assert.equal(await page.getByText('请先选择姓名。', { exact: true }).count(), 0);
  await page.screenshot({ path: path.join(output, '03-first-step-and-clear.png'), fullPage: true, animations: 'disabled' });

  // --- Block 4: min / mismatch fails without extra login posts ---
  await openForm();
  await select().selectOption('recZhangsanA');
  await page.locator('#personnel-password').fill(initialPwd);
  await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
  await page.locator('#personnel-new-password').waitFor();
  const beforeInvalid = loginPosts.length;
  const changeButton = page.getByRole('button', { name: '设置新密码并登录', exact: true });
  await page.locator('#personnel-new-password').fill('short');
  await page.locator('#personnel-confirm-password').fill('short');
  await changeButton.click();
  await page.getByText('新密码长度需为 8-128 位。', { exact: true }).waitFor();
  assert.equal(loginPosts.length, beforeInvalid);
  await page.locator('#personnel-new-password').fill('   ');
  await page.locator('#personnel-confirm-password').fill('   ');
  await changeButton.click();
  await page.getByText('新密码长度需为 8-128 位。', { exact: true }).waitFor();
  assert.equal(loginPosts.length, beforeInvalid);
  await page.locator('#personnel-new-password').fill('password123');
  await page.locator('#personnel-confirm-password').fill('password456');
  await changeButton.click();
  await page.getByText('两次输入的新密码不一致，请重新确认。', { exact: true }).waitFor();
  assert.equal(loginPosts.length, beforeInvalid);
  await page.screenshot({ path: path.join(output, '04-validation-fails.png'), fullPage: true, animations: 'disabled' });

  // --- Block 5: successful new password sends exact person & newpass then redirect ---
  await openForm();
  await select().selectOption('recZhangsanA');
  await page.locator('#personnel-password').fill(initialPwd);
  await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
  await page.locator('#personnel-new-password').waitFor();
  const beforeSecond = loginPosts.length;
  await page.locator('#personnel-new-password').fill('newpass1234');
  await page.locator('#personnel-confirm-password').fill('newpass1234');
  await page.getByRole('button', { name: '设置新密码并登录', exact: true }).click();
  await page.waitForURL(url => url.searchParams.get('scope') === 'A');
  assert.equal(loginPosts.length, beforeSecond + 1);
  assert.deepEqual(loginJson(loginPosts[loginPosts.length - 1].body),
    { person_id: 'recZhangsanA', password: initialPwd, new_password: 'newpass1234', next: '/' });
  await page.screenshot({ path: path.join(output, '05-redirect.png'), fullPage: true, animations: 'disabled' });

  // --- Block 5b: initialized person (needs_setup=false) single-step success redirect ---
  await openForm();
  await select().selectOption('recZhangsanB');
  await page.locator('#personnel-password').fill('secretpass');
  const beforeInit = loginPosts.length;
  await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
  await page.waitForURL(url => url.searchParams.get('scope') === 'A');
  assert.equal(loginPosts.length, beforeInit + 1);
  assert.deepEqual(loginJson(loginPosts[loginPosts.length - 1].body),
    { person_id: 'recZhangsanB', password: 'secretpass', next: '/' });
  await page.screenshot({ path: path.join(output, '05b-initialized-success.png'), fullPage: true, animations: 'disabled' });

  // --- Block 6: failure preserves input and selection; HTTP 401 wrong password must NOT OAuth-redirect ---
  failFirstLogin = true;
  try {
    await openForm();
    await select().selectOption('recZhangsanA');
    await page.locator('#personnel-password').fill('wrongpass1');
    await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
    await page.getByText('密码错误，请重试。', { exact: true }).waitFor();
    assert.equal(await page.locator('#personnel-password').inputValue(), 'wrongpass1', '失败后应保留密码输入');
    assert.equal(await select().inputValue(), labelFor('recZhangsanA'), '失败后应保留所选人员');
    await page.waitForTimeout(300);
    assert.equal(page.url(), origin + '/', '401 密码错误不得触发 OAuth 跳转');
    await page.screenshot({ path: path.join(output, '06-failure-preserves.png'), fullPage: true, animations: 'disabled' });
  } finally {
    failFirstLogin = false;
  }

  // --- Block 7: loading prevents duplicate submissions; no unmasked password or secret storage ---
  let releaseLogin;
  gateLogin = new Promise(resolve => { releaseLogin = resolve; });
  try {
    await openForm();
    await select().selectOption('recZhangsanA');
    await page.locator('#personnel-password').fill(initialPwd);
    const beforeBusy = loginPosts.length;
    await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
    await page.waitForFunction(() => {
      const btn = document.querySelector('.submit');
      return btn && btn.getAttribute('aria-disabled') === null && btn.disabled;
    });
    assert.equal(loginPosts.length, beforeBusy + 1, '首次请求已发出');
    await page.locator('.submit').dispatchEvent('click');
    await page.waitForTimeout(120);
    assert.equal(loginPosts.length, beforeBusy + 1, 'busy 时重复点击不应重复提交');
  } finally {
    releaseLogin?.();
    gateLogin = null;
  }
  await page.locator('#personnel-new-password').waitFor();
  assert.equal(await page.locator('#personnel-password').getAttribute('type'), 'password', '原密码输入不可明文');
  assert.equal(await page.locator('#personnel-new-password').getAttribute('type'), 'password');
  assert.equal(await page.locator('#personnel-confirm-password').getAttribute('type'), 'password');
  const storageLeaks = await page.evaluate(secrets => {
    const found = [];
    for (let i = 0; i < window.localStorage.length; i++) {
      const key = window.localStorage.key(i);
      const value = window.localStorage.getItem(key);
      for (const secret of secrets) {
        if ((key && key.includes(secret)) || (value && value.includes(secret))) found.push({ key, secret });
      }
    }
    return found;
  }, [initialPwd, 'newpass1234', 'secretpass', 'wrongpass1']);
  assert.deepEqual(storageLeaks, [], '登录密码或新密码不得写入 localStorage');
  await page.screenshot({ path: path.join(output, '07-secrets.png'), fullPage: true, animations: 'disabled' });

  // --- Block 7b: rapid double submit produces only one login request ---
  let releaseRapid;
  gateLogin = new Promise(resolve => { releaseRapid = resolve; });
  try {
    await openForm();
    await select().selectOption('recZhangsanA');
    await page.locator('#personnel-password').fill(initialPwd);
    const beforeRapid = loginPosts.length;
    const submitBtn = page.locator('.submit');
    await submitBtn.dispatchEvent('click');
    await submitBtn.dispatchEvent('click');
    await page.waitForTimeout(120);
    assert.equal(loginPosts.length, beforeRapid + 1, '快速重复提交只允许一次登录请求');
  } finally {
    releaseRapid?.();
    gateLogin = null;
  }
  await page.locator('#personnel-new-password').waitFor();

  // --- Block 8: unsafe backslash redirect is rejected (no navigation, error shown) ---
  unsafeRedirectNext = true;
  try {
    await openForm();
    await select().selectOption('recZhangsanA');
    await page.locator('#personnel-password').fill(initialPwd);
    await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
    await page.locator('#personnel-new-password').waitFor();
    await page.locator('#personnel-new-password').fill('newpass1234');
    await page.locator('#personnel-confirm-password').fill('newpass1234');
    await page.getByRole('button', { name: '设置新密码并登录', exact: true }).click();
    await page.getByText('登录成功但跳转地址无效，请刷新后重试。', { exact: true }).waitFor();
    assert.equal(page.url(), origin + '/', '危险的跳转地址不得导致页面跳转');
    await page.screenshot({ path: path.join(output, '08-unsafe-redirect-rejected.png'), fullPage: true, animations: 'disabled' });
  } finally {
    unsafeRedirectNext = false;
  }

  // --- Block 9: stale success after unmount cannot navigate (adapted, no toggles) ---
  let releaseStale;
  gateLogin = new Promise(resolve => { releaseStale = resolve; });
  try {
    await openForm();
    await select().selectOption('recZhangsanB');
    await page.locator('#personnel-password').fill('secretpass');
    const beforeStale = loginPosts.length;
    await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
    await page.waitForTimeout(80);
    assert.equal(loginPosts.length, beforeStale + 1, '待处理登录请求已发出');
    // Navigate to a valid Feishu authenticated main app to unmount the personnel login form.
    feishuAuthStatus = {
      user: { open_id: 'ou_feishu_stale', name: '孙七', login_method: 'feishu', role: 'user' },
      scopeOptions: [{ value: 'A', label: 'A楼' }],
    };
    await page.goto(origin + '/');
    await page.waitForFunction(targetName => {
      const el = document.querySelector('.security-watermark');
      return Boolean(el && el.textContent && el.textContent.includes(targetName));
    }, '孙七');
    releaseStale();
    await page.waitForTimeout(200);
    assert.equal(page.url(), origin + '/', '卸载后的成功响应不得跳转');
    assert.doesNotMatch(page.url(), /scope=A/, '卸载后的成功响应不得跳转至业务页');
  } finally {
    releaseStale?.();
    gateLogin = null;
    feishuAuthStatus = null;
  }

  // --- Block A: /?login=password shows form directly (no toggle) ---
  await page.goto(origin + '/');
  await page.evaluate(() => sessionStorage.removeItem('clipflow-login-method'));
  await page.goto(origin + '/?login=password&next=' + encodeURIComponent('/?scope=A'));
  await page.locator('#personnel-name-select').waitFor();
  await page.locator('#personnel-name-select').waitFor();
  assert.equal(await page.getByRole('button', { name: '姓名密码登录', exact: true }).count(), 1, '密码表单应直接显示');
  assert.match(page.url(), /(^|[?&])login=password/);
  await page.waitForTimeout(800);
  await page.screenshot({ path: path.join(output, '09-login-link-autopen.png'), fullPage: true, animations: 'disabled' });

  // --- Block B: password account session expiry returns password entry, not /api/auth/login ---
  await page.goto(origin + '/');
  await page.evaluate(() => sessionStorage.removeItem('clipflow-login-method'));
  authStatusLoggedInAsPassword = true;
  moduleAuthExpired = true;
  authStatusCallCount = 0;
  try {
    await page.goto(origin + '/');
    await page.waitForURL(url => url.searchParams.get('login') === 'password');
    await page.locator('#personnel-name-select').waitFor();
    await page.locator('#personnel-name-select').waitFor();
    assert.doesNotMatch(page.url(), /\/api\/auth\/login/, '密码账号过期应回密码登录表单而非 OAuth /api/auth/login');
    assert.match(page.url(), /(^|[?&])login=password/);
    await page.waitForTimeout(800);
    await page.screenshot({ path: path.join(output, '10-session-expiry-password-entry.png'), fullPage: true, animations: 'disabled' });
  } finally {
    authStatusLoggedInAsPassword = false;
    authStatusCallCount = 0;
    moduleAuthExpired = false;
  }

  // --- Block C: valid Feishu ordinary building user enters normally, no password form or personnel-password API ---
  feishuAuthStatus = {
    user: { open_id: 'ou_feishu_ordinary', name: '王五', login_method: 'feishu', role: 'user' },
    scopeOptions: [{ value: 'C', label: 'C楼' }, { value: 'A', label: 'A楼' }],
  };
  peoplePosts.length = 0;
  const loginPostsFeishuBase = loginPosts.length;
  try {
    await page.goto(origin + '/');
    await page.waitForFunction(targetName => {
      const el = document.querySelector('.security-watermark');
      return Boolean(el && el.textContent && el.textContent.includes(targetName));
    }, '王五');
    await page.waitForTimeout(300);
    assert.equal(await page.locator('#personnel-name-select').count(), 0, '有效飞书普通用户不得出现姓名密码选择框');
    assert.equal(await page.locator('.app-link').count(), 0, '有效飞书普通用户不得出现人员密码登录链接');
    assert.equal(await page.getByRole('button', { name: '姓名密码登录', exact: true }).count(), 0, '有效飞书普通用户不得出现姓名密码登录按钮');
    assert.equal(await page.locator('#personnel-new-password').count(), 0, '有效飞书普通用户不得出现初始密码修改表单');
    assert.equal(peoplePosts.length, 0, '有效飞书普通用户不得请求 /api/auth/password/people');
    assert.equal(loginPosts.length, loginPostsFeishuBase, '有效飞书普通用户不得请求 /api/auth/password/login');
    assert.match(String(await page.locator('.security-watermark').textContent()), /王五/, '应显示飞书登录用户身份水印');
    assert.equal(page.url(), origin + '/', '有效飞书普通用户不应被强制重定向');
    await page.screenshot({ path: path.join(output, '11-feishu-ordinary-authenticated.png'), fullPage: true, animations: 'disabled' });
  } finally {
    feishuAuthStatus = null;
    peoplePosts.length = 0;
  }

  // --- Block D: valid Feishu admin enters normally, no password form or personnel-password API ---
  feishuAuthStatus = {
    user: { open_id: 'ou_feishu_admin', name: '李四', login_method: 'feishu', role: 'admin' },
    scopeOptions: [{ value: 'A', label: 'A楼' }],
  };
  peoplePosts.length = 0;
  const loginPostsAdminBase = loginPosts.length;
  try {
    await page.goto(origin + '/');
    await page.waitForFunction(targetName => {
      const el = document.querySelector('.security-watermark');
      return Boolean(el && el.textContent && el.textContent.includes(targetName));
    }, '李四');
    await page.waitForTimeout(300);
    assert.equal(await page.locator('#personnel-name-select').count(), 0, '有效飞书管理员不得出现姓名密码选择框');
    assert.equal(await page.locator('.app-link').count(), 0, '有效飞书管理员不得出现人员密码登录链接');
    assert.equal(await page.getByRole('button', { name: '姓名密码登录', exact: true }).count(), 0, '有效飞书管理员不得出现姓名密码登录按钮');
    assert.equal(await page.locator('#personnel-new-password').count(), 0, '有效飞书管理员不得出现初始密码修改表单');
    assert.equal(peoplePosts.length, 0, '有效飞书管理员不得请求 /api/auth/password/people');
    assert.equal(loginPosts.length, loginPostsAdminBase, '有效飞书管理员不得请求 /api/auth/password/login');
    assert.equal(await page.getByRole('button', { name: '打开管理员诊断和权限管理' }).count(), 1, '管理员应显示管理员入口');
    assert.match(String(await page.locator('.security-watermark').textContent()), /李四/, '应显示飞书管理员用户身份水印');
    assert.equal(page.url(), origin + '/', '有效飞书管理员不应被强制重定向');
    await page.screenshot({ path: path.join(output, '12-feishu-admin-authenticated.png'), fullPage: true, animations: 'disabled' });
  } finally {
    feishuAuthStatus = null;
    peoplePosts.length = 0;
  }

  // --- Block E: URL ?login=password + old sessionStorage preference=password yet valid Feishu auth status wins ---
  feishuAuthStatus = {
    user: { open_id: 'ou_feishu_precedence', name: '赵六', login_method: 'feishu', role: 'user' },
    scopeOptions: [{ value: 'D', label: 'D楼' }],
  };
  peoplePosts.length = 0;
  const loginPostsPrecedenceBase = loginPosts.length;
  try {
    const precedencePage = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    precedencePage.setDefaultTimeout(12000);
    precedencePage.on('pageerror', error => errors.push(error.message));
    await precedencePage.addInitScript(() => window.sessionStorage.setItem('clipflow-login-method', 'password'));
    await precedencePage.route('**/api/**', async route => {
      const req = route.request(), url = new URL(req.url());
      const ok = data => route.fulfill({ json: { ok: true, data } });
      if (url.pathname === '/api/auth/status') {
        return ok({ logged_in: true, user: feishuAuthStatus.user, scope_options: feishuAuthStatus.scopeOptions, login_url: '/api/auth/login' });
      }
      if (url.pathname === '/api/auth/password/people') {
        peoplePosts.push({ method: req.method(), path: url.pathname });
        return ok({ items: peopleDb, loaded_at: 1791510000 });
      }
      if (url.pathname === '/api/auth/password/login') {
        loginPosts.push({ method: req.method(), path: url.pathname });
        return ok({});
      }
      if (url.pathname === '/api/auth/password/reset') {
        resetPosts.push({ method: req.method(), path: url.pathname });
        return ok({});
      }
      if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-feishu-precedence' } });
      return ok({});
    });
    await precedencePage.goto(origin + '/?login=password');
    await precedencePage.waitForFunction(targetName => {
      const el = document.querySelector('.security-watermark');
      return Boolean(el && el.textContent && el.textContent.includes(targetName));
    }, '赵六');
    await precedencePage.waitForTimeout(400);
    assert.equal(await precedencePage.locator('#personnel-name-select').count(), 0, '有效飞书会话优先于 password 偏好，不得出现姓名密码表单');
    assert.equal(await precedencePage.locator('.app-link').count(), 0, '有效飞书会话优先不得出现人员密码登录链接');
    assert.equal(await precedencePage.getByRole('button', { name: '姓名密码登录', exact: true }).count(), 0, '有效飞书会话优先不得出现姓名密码登录按钮');
    assert.equal(await precedencePage.locator('#personnel-new-password').count(), 0, '有效飞书会话优先不得出现初始密码修改表单');
    assert.equal(peoplePosts.length, 0, '有效飞书会话优先不得请求 /api/auth/password/people');
    assert.equal(loginPosts.length, loginPostsPrecedenceBase, '有效飞书会话优先不得请求 /api/auth/password/login');
    assert.match(String(await precedencePage.locator('.security-watermark').textContent()), /赵六/, '应显示飞书用户身份水印');
    const prefAfter = await precedencePage.evaluate(() => window.sessionStorage.getItem('clipflow-login-method'));
    assert.equal(prefAfter, 'feishu', '有效飞书会话应将登录偏好纠正为 feishu');
    assert.match(String(precedencePage.url()), /(^|[?&])login=password/, '带 login=password 的地址不应被强制重定向到密码登录或 /api/auth/login');
    await precedencePage.screenshot({ path: path.join(output, '13-feishu-wins-over-password-preference.png'), fullPage: true, animations: 'disabled' });
    await precedencePage.close();
  } finally {
    feishuAuthStatus = null;
    peoplePosts.length = 0;
  }

  // --- Block F: clicking Feishu link remembers feishu preference ---
  {
    const feishuClickPage = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
    feishuClickPage.setDefaultTimeout(12000);
    feishuClickPage.on('pageerror', error => errors.push(error.message));
    await feishuClickPage.route('**/api/**', async route => {
      const req = route.request(), url = new URL(req.url());
      const ok = data => route.fulfill({ json: { ok: true, data } });
      if (url.pathname === '/api/auth/login') return route.fulfill({ status: 200, body: 'ok', contentType: 'text/plain' });
      if (url.pathname === '/api/auth/status') return ok({ logged_in: false });
      if (url.pathname === '/api/auth/password/people') return ok({ items: peopleDb, loaded_at: 1791510000 });
      if (url.pathname === '/api/health') return route.fulfill({ json: { ok: true, service: 'clipflow_backend', instance_id: 'isolated-feishu-click' } });
      return ok({});
    });
    await feishuClickPage.goto(origin + '/');
    await feishuClickPage.locator('#personnel-name-select').waitFor();
    await feishuClickPage.getByRole('link', { name: '飞书登录', exact: true }).click();
    await feishuClickPage.waitForTimeout(150);
    const pref = await feishuClickPage.evaluate(() => window.sessionStorage.getItem('clipflow-login-method'));
    assert.equal(pref, 'feishu', '点击飞书登录应记住 feishu 偏好');
    await feishuClickPage.close();
  }

  // ============ CHANGE MODE (authenticated password user) ============

  // --- Block G1: /?account=password renders change form, no people fetch/login link ---
  changeAuthUser = { open_id: 'ou_pwd_change', name: '张三', login_method: 'password', personnel_record_id: 'recZhangsanA' };
  peoplePosts.length = 0;
  try {
    await page.goto(origin + '/?account=password');
    await page.locator('#personnel-current-password').waitFor();
    assert.equal(await page.locator('.current-account strong').textContent(), '张三', '修改密码应显示当前账号');
    assert.equal(await page.locator('#personnel-name-select').count(), 0, '修改密码模式不得出现姓名下拉');
    assert.equal(await page.locator('.app-link').count(), 0, '修改密码模式不得出现飞书申请链接');
    assert.equal(await page.getByRole('button', { name: '忘记密码', exact: true }).count(), 1, '修改密码模式应提供忘记密码入口');
    assert.equal(await page.locator('#personnel-current-password').getAttribute('type'), 'password');
    assert.equal(await page.locator('#personnel-change-new-password').getAttribute('type'), 'password');
    assert.equal(await page.locator('#personnel-change-confirm-password').getAttribute('type'), 'password');
    assert.equal(peoplePosts.length, 0, '修改密码模式不得请求 people 列表');
    assert.equal(changePosts.length, 0, '进入修改密码不得发起改密请求');
    await page.waitForTimeout(300);
    assert.equal(page.url(), origin + '/?account=password', '修改密码模式应停留在原页面');
    await page.screenshot({ path: path.join(output, '20-change-mode.png'), fullPage: true, animations: 'disabled' });
  } finally {
    peoplePosts.length = 0;
  }

  // --- Block G2a: wrong old password 401 must NOT OAuth-redirect; request has no person_id ---
  await page.goto(origin + '/?account=password');
  await page.locator('#personnel-current-password').waitFor();
  await page.locator('#personnel-current-password').fill('wrongOld');
  await page.locator('#personnel-change-new-password').fill('newpass9876');
  await page.locator('#personnel-change-confirm-password').fill('newpass9876');
  const beforeWrong = changePosts.length;
  await page.getByRole('button', { name: '确认修改密码', exact: true }).click();
  await page.getByText('原密码错误，请重试。', { exact: true }).waitFor();
  assert.equal(changePosts.length, beforeWrong + 1);
  const wrongBody = changePosts[changePosts.length - 1].body;
  assert.equal('person_id' in wrongBody, false, '修改密码请求不得携带 person_id');
  assert.equal(wrongBody.current_password, 'wrongOld');
  assert.equal(wrongBody.new_password, 'newpass9876');
  await page.waitForTimeout(200);
  assert.equal(page.url(), origin + '/?account=password', '原密码错误 401 不得触发 OAuth/跳转');
  assert.equal(await page.locator('#personnel-current-password').inputValue(), 'wrongOld', '失败后应保留原密码输入');
  await page.screenshot({ path: path.join(output, '21-change-wrong-old.png'), fullPage: true, animations: 'disabled' });

  // --- Block G2b: duplicate click while busy sends one change request; success navigates to /?login=password ---
  changeAuthUser = null;
  let releaseChange;
  gateChange = new Promise(resolve => { releaseChange = resolve; });
  try {
    await page.locator('#personnel-current-password').fill('correctOld');
    await page.locator('#personnel-change-new-password').fill('newpass9876');
    await page.locator('#personnel-change-confirm-password').fill('newpass9876');
    const beforeDup = changePosts.length;
    const submitBtn = page.locator('.submit');
    await submitBtn.dispatchEvent('click');
    await page.waitForFunction(() => {
      const btn = document.querySelector('.submit');
      return btn && btn.disabled;
    });
    await submitBtn.dispatchEvent('click');
    await page.waitForTimeout(120);
    assert.equal(changePosts.length, beforeDup + 1, '修改密码 busy 时重复提交不得重复发起');
  } finally {
    releaseChange?.();
    gateChange = null;
  }
  await page.waitForURL(url => (url.searchParams.get('login') ?? '') === 'password');
  const successBody = changePosts[changePosts.length - 1].body;
  assert.deepEqual(successBody, { current_password: 'correctOld', new_password: 'newpass9876' },
    '成功提交只应携带 current_password 与 new_password');
  assert.equal('person_id' in successBody, false, '成功提交也不得携带 person_id');
  assert.doesNotMatch(page.url(), /scope=A|account=password/, '修改成功后不得跳转业务页');
  const changeStorageLeaks = await page.evaluate(secrets => {
    const found = [];
    const buckets = [window.localStorage, window.sessionStorage];
    for (const bucket of buckets) {
      for (let i = 0; i < bucket.length; i++) {
        const key = bucket.key(i);
        const value = bucket.getItem(key);
        for (const secret of secrets) {
          if ((key && key.includes(secret)) || (value && value.includes(secret))) found.push({ key, secret });
        }
      }
    }
    return found;
  }, ['correctOld', 'newpass9876', 'wrongOld']);
  assert.deepEqual(changeStorageLeaks, [], '原密码/新密码不得写入任何存储');
  assert.doesNotMatch(page.url(), /(correctOld|newpass9876|wrongOld)/, '密码不得出现在地址栏');
  await page.screenshot({ path: path.join(output, '22-change-success.png'), fullPage: true, animations: 'disabled' });

  // --- Block G3: reauth after successful change (server cleared session, must login again) ---
  await page.locator('#personnel-name-select').waitFor();
  await page.locator('#personnel-name-select').waitFor();
  await select().selectOption('recZhangsanB');
  await page.locator('#personnel-password').fill('ignoredpass');
  const loginChangeBase = loginPosts.length;
  await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
  await page.waitForURL(url => url.searchParams.get('scope') === 'A');
  assert.equal(loginPosts.length, loginChangeBase + 1, '修改密码成功后需重新登录');
  await page.screenshot({ path: path.join(output, '23-change-reauth.png'), fullPage: true, animations: 'disabled' });

  // --- Block G4: 忘记密码 from change mode switches to forgot-flow (name + ID) ---
  changeAuthUser = { open_id: 'ou_pwd_change', name: '张三', login_method: 'password', personnel_record_id: 'recZhangsanA' };
  peoplePosts.length = 0;
  try {
    await page.goto(origin + '/?account=password');
    await page.locator('#personnel-current-password').waitFor();
    await page.getByRole('button', { name: '忘记密码', exact: true }).click();
    await page.locator('#personnel-reset-name').waitFor();
    await page.locator('#personnel-reset-name').waitFor();
    assert.equal(peoplePosts.length > 0, true, '从修改密码进入忘记密码应加载人员列表');
    assert.equal(await page.getByRole('button', { name: '返回修改密码', exact: true }).count(), 1, '从修改密码进入忘记密码应提供返回修改密码');
    assert.equal(await page.locator('#personnel-identity-number').count(), 1, '忘记密码表单应显示身份证号');
    await page.getByRole('button', { name: '返回修改密码', exact: true }).click();
    await page.locator('#personnel-current-password').waitFor();
    assert.equal(await page.getByRole('button', { name: '忘记密码', exact: true }).count(), 1, '返回后应回到修改密码模式');
    await page.screenshot({ path: path.join(output, '24-change-to-forgot.png'), fullPage: true, animations: 'disabled' });
  } finally {
    changeAuthUser = null;
    peoplePosts.length = 0;
  }

  // --- Block G5: mode switches clear old password and all secrets (change & login) ---
  changeAuthUser = { open_id: 'ou_pwd_change', name: '张三', login_method: 'password', personnel_record_id: 'recZhangsanA' };
  peoplePosts.length = 0;
  try {
    // Part A: change-mode secrets (old + new + confirm) must be cleared when leaving
    // for 忘记密码 and again when returning from reset mode.
    await page.goto(origin + '/?account=password');
    await page.locator('#personnel-current-password').waitFor();
    await page.locator('#personnel-current-password').fill('correctOld');
    await page.locator('#personnel-change-new-password').fill('newpass9876');
    await page.locator('#personnel-change-confirm-password').fill('newpass9876');
    await page.getByRole('button', { name: '忘记密码', exact: true }).click();
    await page.locator('#personnel-reset-name').waitFor();
    assert.equal(await page.locator('#personnel-current-password').count(), 0, '忘记密码后应离开修改密码表单');
    assert.equal(await page.locator('#personnel-change-new-password').count(), 0, '忘记密码后应清除修改密码新密码输入');
    assert.equal(await page.locator('#personnel-change-confirm-password').count(), 0, '忘记密码后应清除修改密码确认输入');
    await resetSelect().selectOption('recZhangsanA');
    await identity().fill(validIdentity);
    await page.locator('[data-verify-status="passed"]').waitFor();
    await page.locator('#personnel-reset-new-password').fill('resetSecret99');
    await page.locator('#personnel-reset-confirm-password').fill('resetSecret99');
    await page.getByRole('button', { name: '返回修改密码', exact: true }).click();
    await page.locator('#personnel-current-password').waitFor();
    assert.equal(await page.locator('#personnel-current-password').inputValue(), '', '返回修改密码后原密码应被清空');
    assert.equal(await page.locator('#personnel-change-new-password').inputValue(), '', '返回修改密码后新密码应被清空');
    assert.equal(await page.locator('#personnel-change-confirm-password').inputValue(), '', '返回修改密码后确认密码应被清空');
    assert.equal(await page.locator('#personnel-reset-new-password').count(), 0, '返回后不得残留重置新密码输入');
  } finally {
    changeAuthUser = null;
    peoplePosts.length = 0;
  }
  // Part B: login-mode old password must be cleared when entering 忘记密码 and on return.
  await openForm();
  await select().selectOption('recZhangsanA');
  await page.locator('#personnel-password').fill('oldLoginPass');
  await page.getByRole('button', { name: '忘记密码', exact: true }).click();
  await page.locator('#personnel-reset-name').waitFor();
  assert.equal(await page.locator('#personnel-password').count(), 0, '忘记密码后应离开密码登录表单');
  await page.getByRole('button', { name: '返回密码登录', exact: true }).click();
  await page.locator('#personnel-password').waitFor();
  assert.equal(await page.locator('#personnel-password').inputValue(), '', '返回密码登录后旧密码应被清空');
  assert.equal(await page.locator('#personnel-reset-new-password').count(), 0, '返回登录后不得残留重置新密码输入');
  await page.screenshot({ path: path.join(output, '25-mode-switch-clears-secrets.png'), fullPage: true, animations: 'disabled' });

  // ============ RESET / 忘记密码 flows (automatic identity verification) ============

  // --- Block R1: reset wrong full ID auto-verifies to mismatch; 401 must NOT OAuth-redirect ---
  await openResetForm();
  await resetSelect().selectOption('recZhangsanA');
  assert.equal(await identity().getAttribute('type'), 'password', '身份证号输入必须遮蔽');
  assert.equal(await identity().getAttribute('maxlength'), '18', '身份证号应限制 maxlength=18');
  const beforeWrongReset = resetPosts.length;
  await identity().fill(wrongFullId);
  await page.locator('[data-verify-status="failed"]').waitFor();
  assert.equal(resetPosts.length, beforeWrongReset + 1);
  assert.deepEqual(resetPosts[resetPosts.length - 1].body,
    { person_id: 'recZhangsanA', identity_number: wrongFullId }, '自动核验只提交 person_id 与完整身份证号');
  assert.equal(await identity().inputValue(), wrongFullId, '失败后应保留身份证号输入');
  assert.equal(await resetSelect().inputValue(), labelFor('recZhangsanA'), '失败后应保留所选人员');
  assert.equal(await page.locator('#personnel-reset-new-password').isDisabled(), true, '未通过核验时新密码必须禁用');
  await page.waitForTimeout(300);
  assert.equal(page.url(), origin + '/', '重置错误身份证号不得触发 OAuth 跳转');
  await page.screenshot({ path: path.join(output, 'r1-reset-wrong-id.png'), fullPage: true, animations: 'disabled' });

  // --- Block R2: reset success (no-session) navigates hard, then must login again; no PII ---
  await openResetForm();
  await resetSelect().selectOption('recZhangsanA');
  const resetBase = resetPosts.length;
  const loginBeforeReset = loginPosts.length;
  await identity().fill(validIdentity);
  await page.locator('[data-verify-status="passed"]').waitFor();
  assert.equal(resetPosts.length, resetBase + 1);
  assert.deepEqual(resetPosts[resetPosts.length - 1].body,
    { person_id: 'recZhangsanA', identity_number: validIdentity }, '自动核验只提交 person_id 与完整身份证号');
  assert.equal(await page.locator('#personnel-reset-new-password').getAttribute('type'), 'password', '新密码输入必须遮蔽');
  assert.equal(await page.locator('#personnel-reset-confirm-password').getAttribute('type'), 'password', '确认新密码输入必须遮蔽');
  // mode switch resets verification + new secrets
  await page.getByRole('button', { name: '返回密码登录', exact: true }).click();
  assert.equal(await page.locator('#personnel-reset-new-password').count(), 0, '切换模式应重置核验状态');
  assert.equal(await page.locator('#personnel-identity-number').count(), 0, '切换模式应回到密码登录表单');
  await page.getByRole('button', { name: '忘记密码', exact: true }).click();
  await page.locator('#personnel-reset-name').waitFor();
  await resetSelect().selectOption('recZhangsanA');
  await identity().fill(validIdentity);
  await page.locator('[data-verify-status="passed"]').waitFor();
  assert.equal(resetPosts.length, resetBase + 2);
  await page.locator('#personnel-reset-new-password').fill('newpass4321');
  await page.locator('#personnel-reset-confirm-password').fill('newpass4321');
  await page.getByRole('button', { name: '确认重置密码', exact: true }).click();
  await page.waitForURL(url => (url.searchParams.get('login') ?? '') === 'password');
  assert.equal(resetPosts.length, resetBase + 3);
  assert.deepEqual(resetPosts[resetPosts.length - 1].body,
    { person_id: 'recZhangsanA', identity_number: validIdentity, new_password: 'newpass4321' }, '最终提交应重新发送完整身份证号');
  assert.doesNotMatch(page.url(), /scope=A|account=password/, '重置成功不得自动登录跳转');
  assert.equal(loginPosts.length, loginBeforeReset, '重置流程不得调用登录接口');
  // no PII storage (full ID + last-6 derived suffix + new password)
  const resetStorageLeaks = await page.evaluate(secrets => {
    const found = [];
    const buckets = [window.localStorage, window.sessionStorage];
    for (const bucket of buckets) {
      for (let i = 0; i < bucket.length; i++) {
        const key = bucket.key(i);
        const value = bucket.getItem(key);
        for (const secret of secrets) {
          if ((key && key.includes(secret)) || (value && value.includes(secret))) found.push({ key, secret });
        }
      }
    }
    return found;
  }, [validIdentity, validIdentity.slice(-6), 'newpass4321', wrongFullId]);
  assert.deepEqual(resetStorageLeaks, [], '身份证号/派生后缀/新密码不得写入任何存储');
  assert.doesNotMatch(page.url(), /(newpass4321|110101199001010000)/, '新密码/身份证号不得出现在地址栏');
  await page.screenshot({ path: path.join(output, 'r2-reset-success.png'), fullPage: true, animations: 'disabled' });

  // --- Block R2b: after reset, login with new password works (needs login) ---
  await openForm();
  await select().selectOption('recZhangsanA');
  await page.locator('#personnel-password').fill('newpass4321');
  const loginResetBase = loginPosts.length;
  await page.getByRole('button', { name: '姓名密码登录', exact: true }).click();
  await page.waitForURL(url => url.searchParams.get('scope') === 'A');
  assert.equal(loginPosts.length, loginResetBase + 1, '重置后需用新密码登录');
  assert.deepEqual(loginJson(loginPosts[loginPosts.length - 1].body),
    { person_id: 'recZhangsanA', password: 'newpass4321', next: '/' });
  const prefAfterLogin = await page.evaluate(() => window.sessionStorage.getItem('clipflow-login-method'));
  assert.equal(prefAfterLogin, 'password', '实际密码提交应记住 password 偏好');
  assert.equal(await page.locator('#personnel-new-password').count(), 0, '重置成功后不应残留改密表单');
  assert.equal(await page.getByText('首次登录密码为本人身份证号后6位，验证后须立即修改', { exact: true }).count(), 0,
    '成功重置后应隐藏首次登录提示（needs_setup 为 false）');
  await page.screenshot({ path: path.join(output, 'r2b-reset-then-login.png'), fullPage: true, animations: 'disabled' });

  // --- Block R1c: malformed HTTP 200 (no requires_password_change:true) must NOT pass verification ---
  malformedResetVerify = true;
  try {
    await openResetForm();
    await resetSelect().selectOption('recZhangsanA');
    const beforeMalformed = resetPosts.length;
    await identity().fill(validIdentity);
    await page.locator('[data-verify-status="failed"]').waitFor();
    assert.equal(resetPosts.length, beforeMalformed + 1, '格式异常的成功响应仍应发起自动核验');
    assert.equal(await page.locator('[data-verify-status="passed"]').count(), 0, '缺少 requires_password_change:true 不得显示核验通过');
    assert.equal(await page.locator('#personnel-reset-new-password').isDisabled(), true, '未通过核验时新密码必须禁用');
    assert.match(String(await page.locator('[data-verify-status="failed"]').textContent()),
      /身份核验未完成，请重试。/, '应显示核验失败信息');
  } finally {
    malformedResetVerify = false;
  }
  await page.screenshot({ path: path.join(output, 'r1c-malformed-200.png'), fullPage: true, animations: 'disabled' });

  // --- Block R1d: network 503 during auto-verification shows real retry label, not false ID mismatch ---
  resetServerError503 = true;
  try {
    await openResetForm();
    await resetSelect().selectOption('recZhangsanA');
    await identity().fill(validIdentity);
    await page.locator('[data-verify-status="failed"]').waitFor();
    assert.equal(await page.locator('[data-verify-status="passed"]').count(), 0, '网络 503 不得显示核验通过');
    const failed503Text = String(await page.locator('[data-verify-status="failed"]').textContent());
    assert.match(failed503Text, /服务繁忙|稍后重试/, '503 应显示服务异常/重试信息');
    assert.doesNotMatch(failed503Text, /身份证号与所选人员不匹配/, '503 不得误报为身份证号不匹配');
    assert.equal(await page.getByRole('button', { name: '重新核验', exact: true }).count(), 1, '网络失败后应提供重新核验入口');
  } finally {
    resetServerError503 = false;
  }
  await page.screenshot({ path: path.join(output, 'r1d-503-retry.png'), fullPage: true, animations: 'disabled' });

  // --- Block R3: input change resets verification + clears new secrets ---
  await openResetForm();
  await resetSelect().selectOption('recZhangsanA');
  await identity().fill(validIdentity);
  await page.locator('[data-verify-status="passed"]').waitFor();
  assert.equal(await page.locator('#personnel-reset-new-password').isEnabled(), true, '核验通过后新密码输入应可用');
  await page.locator('#personnel-reset-new-password').fill('brandNewPass');
  await page.locator('#personnel-reset-confirm-password').fill('brandNewPass');
  await identity().fill(validIdentity.replace(/.$/, '1'));
  assert.equal(await page.locator('[data-verify-status="passed"]').count(), 0, '修改身份证号应清除核验通过状态');
  assert.equal(await page.locator('#personnel-reset-new-password').isDisabled(), true, '核验重置后新密码输入应禁用');
  assert.equal(await page.locator('#personnel-reset-new-password').inputValue(), '', '核验重置后应清除新密码');
  assert.equal(await page.locator('#personnel-reset-confirm-password').inputValue(), '', '核验重置后应清除确认密码');
  await page.screenshot({ path: path.join(output, 'r3-input-change-resets.png'), fullPage: true, animations: 'disabled' });

  // --- Block R4: reset final-submit busy prevention + in-flight disables identity/mode change ---
  let releaseReset;
  // NOTE: gateReset must NOT be armed before the automatic identity verification
  // completes, because the reset mock awaits gateReset on every reset request.
  // Arming it up-front would block the auto-verify and the "[data-verify-status=passed]"
  // wait below would never resolve (deadlock). We arm it only for the final submit.
  try {
    await openResetForm();
    await resetSelect().selectOption('recZhangsanA');
    await identity().fill(validIdentity);
    await page.locator('[data-verify-status="passed"]').waitFor();
    gateReset = new Promise(resolve => { releaseReset = resolve; });
    await page.locator('#personnel-reset-new-password').fill('newpass5555');
    await page.locator('#personnel-reset-confirm-password').fill('newpass5555');
    const beforeResetBusy = resetPosts.length;
    const submitBtn = page.locator('.submit');
    await submitBtn.dispatchEvent('click');
    await page.waitForFunction(() => {
      const btn = document.querySelector('.submit');
      return btn && btn.disabled;
    });
    await submitBtn.dispatchEvent('click');
    await page.waitForTimeout(120);
    assert.equal(resetPosts.length, beforeResetBusy + 1, 'busy 时重复核验不得重复提交');
    assert.equal(await resetSelect().isDisabled(), true, 'in-flight 时应禁用姓名切换');
    assert.equal(await identity().isDisabled(), true, 'in-flight 时应禁用身份证号输入');
    const backBtn = page.getByRole('button', { name: '返回密码登录', exact: true });
    assert.equal(await backBtn.isDisabled(), true, 'in-flight 时应禁用切换模式');
  } finally {
    releaseReset?.();
    gateReset = null;
  }
  await page.waitForURL(url => (url.searchParams.get('login') ?? '') === 'password');
  await page.screenshot({ path: path.join(output, 'r4-reset-busy.png'), fullPage: true, animations: 'disabled' });

  // --- Block R5: debounced automatic verification stale-race — old in-flight success must never approve changed input ---
  await openResetForm();
  await resetSelect().selectOption('recZhangsanA');
  let releaseRace;
  gateReset = new Promise(resolve => { releaseRace = resolve; });
  try {
    await identity().fill(validIdentity);
    await page.locator('[data-verify-status="checking"]').waitFor();
    // Wait past the 500ms debounce so the first automatic verify request is sent (blocked on gate).
    const beforeFirstRace = resetPosts.length;
    await page.waitForTimeout(700);
    assert.equal(resetPosts.length, beforeFirstRace + 1, '第一笔自动核验请求应已发出并阻塞');
    // Change to a different complete ID while the first request is in flight.
    await identity().fill(validIdentity.replace(/.$/, '8'));
    await page.waitForTimeout(150);
    assert.equal(await page.locator('[data-verify-status="passed"]').count(), 0, '在途的旧核验结果不得批准已变更的输入');
    assert.equal(await page.locator('[data-verify-status="failed"]').count(), 0, '第二次核验尚未完成');
  } finally {
    releaseRace?.();
    gateReset = null;
  }
  await page.locator('[data-verify-status="failed"]').waitFor();
  assert.equal(await page.locator('[data-verify-status="passed"]').count(), 0, '竞态结束后不得显示通过');
  assert.equal(await page.locator('#personnel-reset-new-password').isDisabled(), true, '竞态失败后新密码输入应禁用');
  await page.waitForTimeout(150);
  assert.equal(page.url(), origin + '/', '竞态核验失败不得跳转');
  await page.screenshot({ path: path.join(output, 'r5-stale-race.png'), fullPage: true, animations: 'disabled' });

  // "Transition was skipped" is a Chromium View-Transitions API rejection emitted when a
  // same-document history navigation supersedes a pending browser view transition. The
  // production navigation handler already consumes these for pageswap/pagereveal
  // (src/navigation.ts), but occasionally one still surfaces as a transient pageerror.
  // It is not an app-logic error, so filter it out of the fatal pageerror assertion.
  changeAuthUser = null; feishuAuthStatus = null; authStatusLoggedInAsPassword = false;
  await openResetForm();
  await resetSelect().click();
  assert.equal(await page.getByRole('option', { name: /内部人员/ }).count(), 0, 'forgot password excludes VNET');
  await openForm();
  await page.route('**/api/auth/login?*', route => route.fulfill({ contentType: 'text/html', body: '<p>模拟飞书授权</p>' }));
  const beforeInternal = loginPosts.length;
  await select().fill(labelFor('recInternal'));
  await page.waitForURL(url => url.pathname === '/api/auth/login');
  assert.equal(loginPosts.length, beforeInternal, 'VNET selection never submits a password');
  await page.goBack();
  await select().waitFor();
  await page.clock.install();
  await openForm();
  peopleDb.push({ id: 'recNew', name: '新增外部', employee_no: 'EMP-5001', building: 'A楼', account_nature: '外部账号', selectable: true });
  await page.clock.runFor(61000);
  await select().fill('新增外部');
  await page.getByRole('option', { name: /新增外部/ }).waitFor();
  const fatalErrors = errors.filter(message => message !== 'Transition was skipped');
  assert.deepEqual(fatalErrors, []);
  console.log(JSON.stringify({
    ok: true,
    screenshots: output,
    checks: 'direct no-toggle form, 忘记密码 label, app link/reminder, dropdown duplicate-name disambiguation + disabled option, national-ID-last6 hint conditional, empty selection error, first-step form without navigation, change-person clears secrets, min/mismatch/whitespace validation without extra posts, new-password redirect scope, initialized-person success, HTTP-401 wrong-password failure preserves input and stays (no OAuth redirect), busy + rapid-duplicate blocked, masked inputs + no secret storage, unsafe backslash redirect rejected, stale success after unmount does not navigate, /?login=password shows form directly, password-account session expiry returns password entry not /api/auth/login, valid-Feishu ordinary user enters normally, valid-Feishu admin enters normally, ?login=password + sessionStorage preference=password still yields valid-Feishu entry, clicking Feishu remembers feishu, change-mode renders with current account + no people fetch, wrong-old-password 401 stays inline and change request has no person_id, change busy duplicate blocked, change success navigates to /?login=password + reauth + no secret in URL/storage, 忘记密码 from change switches to forgot form, change-mode switch clears old password and all secrets in both change and login modes, reset wrong-full-ID auto-verifies to mismatch with no OAuth, malformed HTTP 200 (missing requires_password_change) cannot pass verification, network 503 auto-verify shows real retry label without false ID mismatch, reset auto-verify debounce + final same-endpoint submit + hard navigation, no PII storage, reset reauth with new password, reset input-change resets verification, reset final busy prevents duplicates + in-flight disables, debounced ID verification stale-race never approves changed input',
  }));
} catch (error) {
  await page.screenshot({ path: path.join(output, 'failure.png'), fullPage: true });
  throw error;
} finally {
  await browser.close();
  await new Promise(resolve => server.httpServer.close(resolve));
}
