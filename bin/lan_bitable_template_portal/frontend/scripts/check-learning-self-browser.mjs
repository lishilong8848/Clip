import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createServer, preview } from "vite";
import { chromium } from "playwright";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const width = Number(process.env.LEARNING_VIEWPORT || 1440);
const output = path.resolve(root, `../../../output/playwright/learning-self-${width}`);
await mkdir(output, { recursive: true });
const scopesValue = "ABCDEH".split("").map(value => ({ value, label: `${value}楼` }));
const counterparty = { id: "p2", name: "测试人员乙", employee_no: "1002", scopes: ["A"], active: true };
const otherBuilding = { id: 'p3', name: 'B楼测试人员', employee_no: '1003', scopes: ['B'], active: true };
const unansweredPerson = { id: 'p4', name: '尚未学练人员', employee_no: '1004', scopes: ['H'], active: true };
const actors = {
  admin: { is_admin: true, can_answer: true, can_view_buildings: true, self_person: { id: "admin-self", name: "管理员本人", employee_no: "9000", scopes: ["A"], active: true }, self_scope: "A", scopes: scopesValue, identity_issue: "" },
  personal: { is_admin: false, can_answer: true, can_view_buildings: false, self_person: { id: "pers-self", name: "普通人员", employee_no: "2001", scopes: ["B"], active: true }, self_scope: "B", scopes: [], identity_issue: "" },
  duty: { is_admin: false, can_answer: false, can_view_buildings: true, self_person: null, self_scope: "", scopes: [{ value: "A", label: "A楼" }], identity_issue: "" },
};
let currentActor = actors.admin;
const today = "2026-10-09";
const makePaper = person_id => ({
  id: `paper-${person_id}`,
  person_id,
  person: [actors.admin.self_person, actors.personal.self_person, counterparty].find(p => p?.id === person_id) || { id: person_id, name: `人员${person_id}`, employee_no: "", scopes: ["A"], active: true },
  scope: "A",
  date: today,
  version: 0,
  mode: 'daily',
  created_at: today + 'T08:00:00',
  status: "pending",
  shortage: {},
  questions: Array.from({ length: 15 }, (_, n) => ({
    id: `q-${person_id}-${n}`,
    version: "v1",
    stem: `第${n + 1}题：进行设备维护前，应先核对哪项安全条件？涉及多个设备时，应如何逐一核实现场安全条件与作业范围？`,
    bank: "written",
    type: n === 1 ? 'multiple' : n === 2 ? 'interview' : 'single',
    options: n === 1 ? 'abcd'.split('').map(id => ({id: `${person_id}-${id}`, text: `核对设备与作业条件${id}`})) : [{ id: `${person_id}-a`, text: "现场安全条件" }, { id: `${person_id}-b`, text: "直接开始操作" }],
    correct_option_ids: (n === 1 ? 'abcd'.split('') : ['a']).map(id => `${person_id}-${id}`),
    has_hint: true,
    attachments: [],
  })),
});
function publicPaper(p) { return { ...structuredClone(p), stats: { total: p.questions.length, answered: p.questions.filter(q => q.attempt).length, shortage: 0 } }; }
const papers = new Map();
const practicePapers = new Map();
let published = true;
const allPapers = () => [...papers.values(), ...practicePapers.values()];
function paperFor(person_id) { if (!papers.has(person_id)) papers.set(person_id, makePaper(person_id)); return papers.get(person_id); }
const requests = [];
const unexpected = [];
const pageErrors = [];
const claims = [];
let answerGate = null;

const production = process.env.LEARNING_PRODUCTION === '1';
const server = production
  ? await preview({ root, logLevel: 'error', preview: { host: '127.0.0.1', port: 0, strictPort: false } })
  : await createServer({ root, server: { host: "127.0.0.1", port: 0, strictPort: false, hmr: false }, logLevel: "error" });
if (!production) await server.listen();
const base = `http://127.0.0.1:${server.httpServer.address().port}`;

const summary = person_id => {
  const p = papers.get(person_id);
  const attempts = (p?.questions || []).filter(q => q.attempt);
  const correct = attempts.filter(q => q.attempt.correct).length;
  return {
    assigned: (p?.questions.length || 0), answered: attempts.length, accuracy: attempts.length ? 100 * correct / attempts.length : null,
    independent_correct: correct, wrong: attempts.length - correct, independent_answered: attempts.length,
    completed_people: 0, papers: papers.size, learning_days: attempts.length ? 1 : 0, interview_total: 0, interview_ratings: {},
  };
};
const peopleFor = () => {
  const list = new Set();
  if (currentActor.self_person) list.add(JSON.stringify(currentActor.self_person));
  list.add(JSON.stringify(counterparty));
  if (currentActor.is_admin) [otherBuilding, unansweredPerson].forEach(p => list.add(JSON.stringify(p)));
  return [...list].map(s => JSON.parse(s));
};

let browser, page;
try {
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width, height: width < 700 ? 844 : 1100 }, serviceWorkers: "block", hasTouch: width < 700 });
  await context.route("**/*", async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== base) { unexpected.push(req.url()); return route.abort(); }
    if (url.pathname === "/api/health") return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, service: "clipflow_backend", instance_id: "learning-self-fixture" }) });
    if (!url.pathname.startsWith("/api/")) return route.continue();
    const p = url.pathname, method = req.method();
    const body = req.headers()["content-type"]?.includes("json") ? req.postDataJSON() : null;
    requests.push({ p, method, body, query: Object.fromEntries(url.searchParams) });
    const ok = data => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, data }) });
    if (p === "/api/auth/status") return ok({ logged_in: true, user: { open_id: "self-user", name: "测试用户", role: "user" }, scope_options: scopesValue.slice(0, 1) });
    if (p === "/api/scope-overview") return ok({ scopes: {} });
    if (p === "/api/assistant/appearance") return ok({ enabled: false });
    if (p === "/api/handover-links") return ok({ links: {} });
    if (p === "/api/auth/permission-requests/current") return ok({});
    if (p === "/api/learning/people") return ok({ items: peopleFor().filter(q => `${q.name}${q.employee_no}`.includes(url.searchParams.get("q") || "")), total: peopleFor().length, ready: true, issues: [] });
    if (p === "/api/learning/bootstrap") return ok({ ...currentActor, scope: url.searchParams.get("scope") || "A", settings: { enabled: true, publish_time: "08:00", reminder_enabled: false }, sync: { status: "ready", pending: 0 }, today, question_problem_count: currentActor.is_admin ? 1 : 0 });
    if (p === "/api/learning/papers/claim") {
      claims.push(body);
      if (body.mode === 'practice') {
        const key = `${body.person_id}:${body.operation_id}`;
        if (!practicePapers.has(key)) {
          const paper = makePaper(body.person_id);
          paper.id = `paper-practice-${body.person_id}-${practicePapers.size + 1}`;
          paper.mode = 'practice'; paper.created_at = `${today}T10:00:${String(practicePapers.size).padStart(2, '0')}`;
          paper.questions.forEach(q => { q.id = `${paper.id}:${q.id}`; });
          practicePapers.set(key, paper);
        }
        return ok(publicPaper(practicePapers.get(key)));
      }
      if (!papers.has(body.person_id)) papers.set(body.person_id, makePaper(body.person_id));
      return ok(publicPaper(papers.get(body.person_id)));
    }
    const paperAnswer = p.match(/^\/api\/learning\/papers\/paper-[^/]+\/answer$/);
    if (paperAnswer && method === "POST") {
      if (answerGate) await answerGate;
      const paper = allPapers().find(paper => paper.id === p.split('/').at(-2));
      const q = paper.questions.find(q => q.id === body.question_id);
      if (q) {
        const attempt = { correct: q.type === 'interview' ? null : JSON.stringify([...(body.option_ids || [])].sort()) === JSON.stringify([...q.correct_option_ids].sort()), option_ids: body.option_ids, answer_text: body.answer_text, submitted_at: today + "T09:00:00", self_rating: body.self_rating, assisted: false };
        if (body.practice) { q.practice = q.practice || []; q.practice.push(attempt); }
        else { q.attempt = attempt; }
      }
      return ok(publicPaper(paper));
    }
    const paperGet = p.match(/^\/api\/learning\/papers\/paper-[^/]+$/);
    if (paperGet && method === "GET") {
      const id = p.split("/").at(-1);
      return ok(publicPaper(allPapers().find(p => p.id === id)));
    }
    if (p === "/api/learning/papers" || p === "/api/learning/history") {
      const pid = url.searchParams.get("person_id");
      const mode = url.searchParams.get('mode') || (url.searchParams.get('today') === '1' ? 'daily' : '');
      const items = allPapers().filter(q => (!pid || q.person_id === pid) && (!mode || q.mode === mode)).sort((a, b) => b.created_at.localeCompare(a.created_at)).map(publicPaper);
      return ok({ items, total: items.length, page: 1, page_size: 20, today, published });
    }
    if (p === "/api/learning/profile") {
      const id = url.searchParams.get("person_id");
      const s = { assigned: 15, answered: 10, task_answered: 10, choice_answered: 8, correct: 6, wrong: 2, accuracy: 75, independent_answered: 4, independent_correct: 3, learning_days: 3, interview_total: 2, interview_ratings: { '部分掌握': 1, '需复习': 1 }, papers: 1, answered_people: 1, received_people: 1 };
      const allPeople = url.searchParams.get('all_people') === '1';
      const people = [{person_id:counterparty.id, name:counterparty.name, scope:'A', employee_no:counterparty.employee_no, summary:s, last_answered_at:today+'T08:00:00', today:{answered:10,total:15}}];
      if (allPeople) {
        people.push({person_id:otherBuilding.id, name:otherBuilding.name, scope:'B', employee_no:otherBuilding.employee_no, summary:s, last_answered_at:today+'T09:00:00', today:{answered:10,total:15}});
        people.push({person_id:unansweredPerson.id, name:unansweredPerson.name, scope:'H', employee_no:unansweredPerson.employee_no, summary:{answered:0,attempt_count:0,wrong:0,accuracy:null}, last_answered_at:'',today:null});
      }
      return ok({ person: peopleFor().find(q => q.id === id), published, summary: s, today_summary: s,
        trend: Array.from({length:7}, (_,n) => ({date:`2026-10-0${n+1}`, answered:n+1, people:n+1, accuracy:n*10+30})),
        banks:[{label:'新增机电综合题库', wrong:2}], topics:[],
        distribution:[{label:'80-99%', count:1}], people:id ? [] : people });
    }
    if (p === "/api/learning/settings") return ok({ enabled: true });
    if (p === '/api/learning/questions' || p === '/api/learning/review' || p === '/api/learning/issues') return ok({ items: [], total: 0, page: 1 });
    if (p === "/api/learning/export") return route.fulfill({ status: 200, contentType: "text/csv; charset=utf-8", body: "date,scope\n" });
    unexpected.push(`${method} ${p}`);
    return route.fulfill({ status: 500, contentType: "application/json", body: JSON.stringify({ ok: false, error: "unexpected" }) });
  });
  page = await context.newPage();
  page.setDefaultTimeout(20000);
  page.on("pageerror", error => pageErrors.push(error.message));
  page.on("console", message => { if (message.type() === "error") pageErrors.push(message.text()); });
  const backButton = () => page.locator("#page-back-slot .vnet-back-button, .vnet-back-button");

  // ---- S1 picker view readonly + no claim, back direct + selector works (admin overview) ----
  currentActor = actors.admin;
  await page.goto(`${base}/learning?view=overview&scope=A`);
  await page.getByRole("button", { name: "选择人员", exact: true }).waitFor();

  // All overview includes unassigned people and preserves its scope through back/reload.
  await page.locator('.building-tabs').getByRole('button', {name: /全部画像/}).click();
  await page.getByRole('heading', {name:'全员学习汇总', exact:true}).waitFor();
  await page.waitForFunction(() => document.querySelectorAll('.learning-page').length === 1);
  assert.equal(new URL(page.url()).searchParams.get('scope'), '');
  assert.equal(await page.locator('.building-tabs button').count(), 7);
  await page.locator('.learning-dashboard tbody tr').filter({hasText:'尚未学练人员'}).waitFor();
  assert.equal(await page.locator('.learning-dashboard table').getByRole('columnheader', {name:'楼栋', exact:true}).count(), 1);
  const unassigned = page.locator('.learning-dashboard tbody tr').filter({hasText:'尚未学练人员'});
  assert.match(await unassigned.textContent(), /数据不足.*未领取.*尚未作答/);
  await page.locator('.learning-dashboard tbody tr').filter({hasText:'B楼测试人员'}).getByRole('button', {name:'个人画像'}).click();
  await page.locator('.learner-strip').getByText('B楼测试人员', {exact:true}).waitFor();
  await page.waitForFunction(() => document.querySelectorAll('.learning-page').length === 1);
  await backButton().click();
  await page.getByRole('heading', {name:'全员学习汇总', exact:true}).waitFor();
  await page.reload();
  await page.getByRole('heading', {name:'全员学习汇总', exact:true}).waitFor();
  await page.screenshot({path:path.join(output, 'all-portrait.png'), fullPage:true});
  assert.ok(requests.some(r => r.p === '/api/learning/profile' && r.query.all_people === '1' && !r.query.scope && !r.query.person_id));
  assert.equal(claims.length, 0, 'all overview never claims a paper');
  await page.locator('.building-tabs').getByRole('button', {name:/^A楼/}).click();
  await page.getByRole('heading', {name:'人员学习汇总', exact:true}).waitFor();
  await page.waitForFunction(() => document.querySelectorAll('.learning-page').length === 1);
  assert.equal(await page.locator('.learning-dashboard tbody tr').filter({hasText:'B楼测试人员'}).count(), 0);
  await page.getByRole("button", { name: "选择人员", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: /测试人员乙/ }).click();
  await page.waitForURL(/\/learning\?.*(?:view=overview.*person_id=p2|person_id=p2.*view=overview)/);
  await page.locator('.learning-page:not([inert]) .tabs').getByRole("button", { name: "个人画像", exact: true }).waitFor();
  assert.equal(claims.length, 0, "picker overview profile must not claim");
  assert.equal(await page.getByRole("button", { name: "今日学练", exact: true }).count(), 0, "overview hides 今日学练");
  assert.equal(await page.getByRole("button", { name: "错题与复习", exact: true }).count(), 0, "overview hides 错题与复习");
  assert.equal(await page.getByRole("button", { name: "确认作答", exact: true }).count(), 0, "overview has no answer control");
  assert.equal(await page.getByRole('button', { name: /领取今日题单|继续今日学练/ }).count(), 0, 'overview renders no answer entry');
  await backButton().click();
  await page.waitForURL(/\/learning\?.*view=overview/);
  assert.equal(new URL(page.url()).searchParams.get("view"), "overview", "profile back keeps overview");
  await page.getByRole("button", { name: "选择人员", exact: true }).waitFor();
  // selector works again inside overview
  await page.getByRole("button", { name: "选择人员", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: /测试人员乙/ }).click();
  await page.waitForURL(/\/learning\?.*(?:view=overview.*person_id=p2|person_id=p2.*view=overview)/);
  await page.screenshot({ path: path.join(output, "overview-picker.png"), fullPage: true });

  // ---- S2 admin profile others cannot answer (overview stays readonly) ----
  assert.equal(await page.getByRole("button", { name: "确认作答", exact: true }).count(), 0, "admin viewing others cannot answer");
  assert.equal(await page.getByRole("button", { name: "重新练习与自评", exact: true }).count(), 0, "admin viewing others has no practice re-answer");
  await page.goto(`${base}/learning?view=overview&scope=A`);
  await page.getByRole("button", { name: "选择人员", exact: true }).waitFor();

  // ---- S3 practice automatically self, no picker (admin) ----
  await page.goto(`${base}/learning?view=practice`);
  await page.getByRole("button", { name: "我的答题", exact: true }).waitFor();
  await page.getByText("管理员本人").first().waitFor();
  assert.equal(await page.getByRole("button", { name: "选择人员", exact: true }).count(), 0, "practice has no people picker");
  assert.equal(await page.getByRole("button", { name: "切换人员", exact: true }).count(), 0, "practice has no switch-person");
  assert.ok(claims.some(c => c.person_id === "admin-self"), "practice claims only self, no spoof");
  assert.ok(!claims.some(c => c.person_id === "p2"), "practice must never claim a counterparty");
  await page.screenshot({ path: path.join(output, "practice-self.png"), fullPage: true });

  // ---- S4 personal account default /learning goes practice, no picker ----
  currentActor = actors.personal;
  await page.goto(`${base}/learning`);
  await page.getByText("普通人员").first().waitFor();
  assert.equal(await page.getByRole("button", { name: "选择人员", exact: true }).count(), 0, "personal practice no picker");
  assert.equal(await page.getByRole("button", { name: "楼栋画像", exact: true }).count(), 0, "personal has no building overview switch");
  await page.screenshot({ path: path.join(output, "personal-practice.png"), fullPage: true });

  // ---- S5 duty default /learning goes overview readonly own scope ----
  currentActor = actors.duty;
  const beforeDuty = claims.length;
  await page.goto(`${base}/learning`);
  await page.getByRole("button", { name: "选择人员", exact: true }).waitFor();
  assert.equal(await page.locator(".building-tabs button").count(), 1, "duty sees only own building tab");
  assert.equal(await page.getByRole("button", { name: "我的答题", exact: true }).count(), 0, "duty has no practice switch");
  await page.getByRole("button", { name: "选择人员", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: /测试人员乙/ }).click();
  await page.waitForURL(/\/learning\?.*(?:view=overview.*person_id=p2|person_id=p2.*view=overview)/);
  await page.locator('.learning-page:not([inert]) .tabs').getByRole("button", { name: "个人画像", exact: true }).waitFor();
  assert.equal(await page.getByRole("button", { name: "确认作答", exact: true }).count(), 0, "duty cannot answer");
  assert.equal(claims.length, beforeDuty, "duty overview never claims");
  await page.screenshot({ path: path.join(output, "duty-overview.png"), fullPage: true });

  // ---- S6 cannot bypass real answer-save back: back blocked while /answer busy ----
  currentActor = actors.admin;
  paperFor("admin-self");
  await page.goto(`${base}/learning?view=practice`);
  const answerBtn = page.getByRole("button", { name: "确认作答", exact: true });
  await answerBtn.waitFor();
  await page.locator(".options label").first().click();
  let release;
  answerGate = new Promise(resolve => { release = resolve; });
  await answerBtn.click();
  await page.waitForTimeout(150);
  assert.equal(await backButton().isDisabled(), true, "back disabled during a real answer save");
  const urlBefore = page.url();
  await backButton().click({ force: true }).catch(() => {});
  assert.equal(page.url(), urlBefore, "back must not navigate during real answer save");
  release();
  answerGate = null;
  await page.getByRole('button', { name: '再次练习', exact: true }).waitFor();
  await page.screenshot({ path: path.join(output, "busy-answer-block.png"), fullPage: true });

  // ---- S7 repeated practice history shown (first attempt + practice attempts) ----
  const histPaper = paperFor("admin-self");
  const q0 = histPaper.questions[0];
  q0.attempt = { correct: false, option_ids: ["admin-self-b"], wrong: ["admin-self-b"], missed: ["admin-self-a"], submitted_at: today + "T08:00:00" };
  q0.practice = [
    { correct: false, option_ids: ["admin-self-b"], submitted_at: today + "T08:30:00" },
    { correct: true, option_ids: ["admin-self-a"], submitted_at: today + "T09:00:00" },
  ];
  currentActor = actors.admin;
  await page.goto(`${base}/learning?view=practice`);
  const records = page.locator(".attempt-records");
  await records.first().waitFor();
  const text = await records.first().textContent();
  assert.match(text, /首次作答/, "attempt records include first attempt");
  assert.match(text, /复习 1/, "attempt records include practice #1");
  assert.match(text, /复习 2/, "attempt records include practice #2");
  assert.match(text, /082?0|08:00/, "attempt records include first timestamp");
  assert.match(await page.locator('.answer-actions .result').textContent(), /最近复习：回答正确/);
  assert.match(await page.locator('.first-result').textContent(), /首次作答：回答错误/);
  assert.equal(await page.locator('.options input:checked').inputValue(), 'admin-self-a');
  await page.screenshot({ path: path.join(output, "attempt-records.png"), fullPage: true });

  // A new practice result is visible immediately, while the first score stays wrong.
  for (const [option, result] of [[1, '回答错误'], [0, '回答正确']]) {
    await page.getByRole('button', { name: '再次练习', exact: true }).click();
    await page.locator('.options label').nth(option).click();
    await page.getByRole('button', { name: '提交本次复习', exact: true }).click();
    await page.getByRole('button', { name: '再次练习', exact: true }).waitFor();
    assert.ok((await page.locator('.answer-actions .result').textContent()).includes(`最近复习：${result}`));
    assert.match(await page.locator('.first-result').textContent(), /首次作答：回答错误/);
    assert.equal(paperFor('admin-self').questions[0].attempt.correct, false);
  }
  await page.screenshot({ path: path.join(output, 'practice-latest-result.png'), fullPage: true, animations: 'disabled' });

  if (width < 700) {
    const fits = async name => {
      await page.waitForFunction(() => document.querySelectorAll('.learning-page').length === 1);
      await page.waitForTimeout(350);
      const size = await page.evaluate(() => ({ full: document.documentElement.scrollWidth, width: innerWidth }));
      assert.ok(size.full <= size.width + 1, `${name}: page overflow ${JSON.stringify(size)}`);
      const content = await page.locator('.learning-page').boundingBox();
      assert.ok(content.width >= width - 32, `${name}: collapsed page width ${content.width}`);
      await page.screenshot({ path: path.join(output, `${name}.png`), fullPage: true });
    };
    await fits('mobile-answer');
    assert.ok((await page.locator('.question-numbers button').first().boundingBox()).height >= 44);
    await page.getByRole('button', { name: '再次练习', exact: true }).click();
    await page.locator('.options label').last().click();
    await page.getByRole('button', { name: '提交本次复习', exact: true }).click();
    await page.getByRole('button', { name: '再次练习', exact: true }).waitFor();
    assert.equal(paperFor('admin-self').questions[0].practice.length, 5);
    assert.equal(paperFor('admin-self').questions[0].attempt.correct, false, 'practice never overwrites first score');
    await page.getByRole('button', {name:'下一题', exact:true}).click();
    for (const index of [3, 2, 1, 0]) await page.locator('.options label').nth(index).click();
    await page.getByRole('button', {name:'确认作答', exact:true}).click();
    await page.getByRole('button', {name:'再次练习', exact:true}).waitFor();
    assert.equal(paperFor('admin-self').questions[1].attempt.correct, true);
    await fits('mobile-multiple');
    await page.getByRole('button', {name:'下一题', exact:true}).click();
    await page.locator('.interview-answer textarea').fill('核对操作票、设备编号与停送电范围，确认安全措施，向负责人报告。');
    await page.locator('#learning-select-2').click();
    await page.getByRole('option', {name:'部分掌握', exact:true}).click();
    await page.getByRole('button', {name:'确认作答', exact:true}).click();
    await page.getByRole('button', {name:'重新练习与自评', exact:true}).waitFor();
    assert.equal(paperFor('admin-self').questions[2].attempt.self_rating, '部分掌握');
    await fits('mobile-interview');
    await page.getByRole('button', { name: '题目有疑问', exact: true }).click();
    await page.getByRole('dialog').waitFor();
    await fits('mobile-issue');
    await page.getByRole('button', { name: '关闭窗口', exact: true }).click();
    await page.getByRole('dialog').waitFor({ state: 'hidden' });
    await page.getByRole('button', { name: '楼栋画像', exact: true }).click();
    await page.locator('.building-tabs button').first().waitFor();
    await fits('mobile-building');
    await page.getByRole('button', { name: '选择人员', exact: true }).click();
    await page.getByRole('dialog').waitFor();
    await fits('mobile-people');
    await page.getByRole('dialog').getByRole('button', { name: /测试人员乙/ }).click();
    await page.locator('.learning-page:not([inert]) .tabs').getByRole('button', { name: '个人画像', exact: true }).waitFor();
    await fits('mobile-profile');
    const axisSizes = await page.locator('.trend-plot text').evaluateAll(els => els.map(el => parseFloat(getComputedStyle(el).fontSize) * el.getScreenCTM().a));
    assert.ok(axisSizes.every(size => size >= 12), `mobile chart text must stay legible: ${axisSizes}`);
    const dateLabels = await page.locator('.trend-plot text[y="179"]').evaluateAll(els => els.map(el => { const box = el.getBoundingClientRect(); return { left: box.left, right: box.right }; }));
    assert.ok(dateLabels.every((label, i) => i === 0 || label.left >= dateLabels[i - 1].right), 'mobile chart date labels must not overlap');
    await backButton().click();
    await page.waitForFunction(() => document.querySelectorAll('.learning-page').length === 1);
    await page.getByRole('button', { name: '发布设置', exact: true }).click();
    await page.getByRole('button', { name: '手动发布题单', exact: true }).waitFor();
    await fits('mobile-settings');
    await page.getByRole('button', { name: '题库管理', exact: true }).click();
    await page.getByRole('button', { name: '新增题目', exact: true }).click();
    await page.getByRole('dialog').waitFor();
    await fits('mobile-question-editor');
  }

  // Personal autonomous rounds do not depend on daily publication or replace it.
  currentActor = { ...actors.personal, self_person: {...actors.personal.self_person, id:'autonomous-user', name:'自主学练人员'} };
  published = false;
  const claimsBefore = claims.length;
  await page.goto(`${base}/learning?view=practice`);
  await page.getByText('今日暂无可学习题目', {exact:true}).waitFor();
  assert.equal(claims.length, claimsBefore, 'unpublished daily paper is not claimed');
  await page.getByRole('button', {name:'开始学练',exact:true}).click();
  await page.getByRole('button', {name:'确认作答',exact:true}).waitFor();
  assert.equal(await page.locator('.question-numbers button').count(), 15);
  const firstRound = allPapers().find(p => p.person_id === 'autonomous-user');
  assert.equal(firstRound.mode, 'practice');
  assert.equal(claims.at(-1).mode, 'practice');
  await page.locator('.options label').first().click();
  await page.getByRole('button', {name:'确认作答',exact:true}).click();
  await page.getByRole('button', {name:'再次练习',exact:true}).waitFor();
  await page.getByRole('group', {name:'学练方式'}).getByRole('button', {name:'每日题单',exact:true}).click();
  await page.getByText('今日暂无可学习题目', {exact:true}).waitFor();
  await page.getByRole('group', {name:'学练方式'}).getByRole('button', {name:'自主练习',exact:true}).click();
  await page.getByRole('button', {name:'再次练习',exact:true}).waitFor();
  assert.equal(claims.length, claimsBefore + 1, 'mode switch continues the same practice');
  await page.reload();
  await page.getByText('今日暂无可学习题目', {exact:true}).waitFor();
  await page.getByRole('group', {name:'学练方式'}).getByRole('button', {name:'自主练习',exact:true}).click();
  await page.getByRole('button', {name:'再次练习',exact:true}).waitFor();
  assert.equal(claims.length, claimsBefore + 1, 'reload does not generate new questions');
  await page.screenshot({path:path.join(output, 'autonomous-practice.png'), fullPage:true, animations:'disabled'});
  assert.ok(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1), 'autonomous toolbar fits viewport');
  await page.getByRole('button', {name:'开始学练',exact:true}).click();
  await page.getByRole('button', {name:'确认作答',exact:true}).waitFor();
  assert.equal(claims.length, claimsBefore + 2);
  assert.notEqual(claims.at(-1).operation_id, claims.at(-2).operation_id);
  assert.equal(firstRound.questions[0].attempt.correct, true, 'earlier round remains intact');
  published = true;
  await page.getByRole('group', {name:'学练方式'}).getByRole('button', {name:'每日题单',exact:true}).click();
  await page.getByRole('button', {name:'确认作答',exact:true}).waitFor();
  assert.equal(papers.get('autonomous-user').mode, 'daily');
  await page.getByRole('button', {name:'学习历史',exact:true}).click();
  await page.locator('.table-wrap tbody tr').first().waitFor();
  assert.equal(await page.locator('.table-wrap tbody tr').count(), 3);
  assert.equal(await page.locator('.table-wrap tbody tr').filter({hasText:'自主练习'}).count(), 2);
  assert.equal(await page.locator('.table-wrap tbody tr').filter({hasText:'每日题单'}).count(), 1);

  assert.deepEqual(pageErrors, [], `browser errors: ${pageErrors.join("\n")}`);
  assert.deepEqual(unexpected, [], `unexpected requests: ${unexpected.join(", ")}`);
  console.log("[LearningSelfBrowser] OK", { production, width, requests: requests.length, claims: claims.length, output });
} catch (error) {
  console.error("[BrowserErrors]", pageErrors, unexpected);
  await page?.screenshot({ path: path.join(output, "failure.png"), fullPage: true });
  throw error;
} finally {
  await browser?.close();
  if (production) await new Promise(resolve => server.httpServer.close(resolve));
  else await server.close();
}
