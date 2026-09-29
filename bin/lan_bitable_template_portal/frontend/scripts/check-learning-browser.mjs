import assert from "node:assert/strict";
import { readFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { chromium } from "playwright";
import { parse } from "@vue/compiler-sfc";
import ts from "typescript";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const output = path.resolve(root, "../../../output/playwright/learning");
await mkdir(output, { recursive: true });
const source = await readFile(path.join(root, "src/components/LearningPage.vue"), "utf8");
const { descriptor } = parse(source);
const compiled = ts.transpileModule(descriptor.script.content, { compilerOptions: { module: ts.ModuleKind.ES2022 } }).outputText;
const { parseLearningImport, learningQuestionProblems } = await import(`data:text/javascript;base64,${Buffer.from(compiled).toString("base64")}`);
const question = (n = 1) => ({ id: `q${n}`, question_id: `q${n}`, bank: "written", stem: n === 1 ? "检修前发现供电切换异常，应如何确认现场条件？\n" + "核对设备运行状态、操作票和影响范围，确认安全条件后进行下一步。".repeat(12) : `测试题目 ${n}`, type: "single", type_label: "单选", year: "2026", options: [{ id: `q${n}-a`, text: "核对运行方式与隔离点，并进行双人复核。" }, { id: `q${n}-b`, text: "直接切换设备。" }, { id: `q${n}-c`, text: "仅以远程指示作为依据。" }], correct_option_ids: [`q${n}-a`], answer_text: "核对运行方式与隔离点，并进行双人复核。", analysis: "先核对现场条件，再执行经批准的操作。", hint: "注意隔离点与现场复核。", topic: "供电安全", specialty: "电气", difficulty: "中等", version: "v1", status: "published", attachments: [], problems: [] });
const sample = question();
assert.deepEqual(learningQuestionProblems(sample), []);
for (const year of ["", "1900", "2026", "2025年", "2100年"]) assert.deepEqual(learningQuestionProblems({ ...sample, year }), []);
for (const year of ["1899", "2101年", "20a6", "单选"]) assert(learningQuestionProblems({ ...sample, year }).some(error => error.includes("年度")));
const sorted = structuredClone(sample); sorted.options.reverse();
assert.deepEqual(sorted.correct_option_ids, ["q1-a"]);
assert.deepEqual(learningQuestionProblems(sorted), []);
sorted.options = sorted.options.filter(o => o.id !== "q1-a");
assert(learningQuestionProblems(sorted).length > 0, "deleting a correct option must invalidate the answer");
const csvCell = value => '"' + String(value).replaceAll('"', '""') + '"';
const csv = ["bank,type,stem,options,correct_option_ids", ["written", "single", '含逗号,与换行\n以及"引号"', JSON.stringify(sample.options), JSON.stringify(sample.correct_option_ids)].map(csvCell).join(",")].join("\r\n");
assert.equal(parseLearningImport(csv, true)[0].question.stem, '含逗号,与换行\n以及"引号"');
assert.throws(() => parseLearningImport('bank,stem\nwritten,"未闭合', true), /未闭合/);
assert.throws(() => parseLearningImport('{"questions":[]}'), /非空/);
assert(parseLearningImport(JSON.stringify({ questions: [{ ...sample, correct_option_ids: ["missing"] }] }))[0].errors.length);

const server = await createServer({ root, configFile: path.join(root, "vite.config.ts"), server: { host: "127.0.0.1", port: 0, strictPort: false, hmr: false }, logLevel: "error" });
let browser, activePage;
await server.listen();
const base = `http://127.0.0.1:${server.httpServer.address().port}`;
const calls = [], unexpected = [], pageErrors = [];
let admin = false, lostAnswer = false, staleAnswer = false, publishingPolls = 0, staleAttachment = false, paperDeleted = false;
let profileGate;
let silentPublishing = true;
const settings = { enabled: true, publish_time: "08:00", reminder_enabled: false, reminder_time: "17:00", portal_url: "https://portal.example.test:8443" };
const bank = Array.from({ length: 23 }, (_, n) => question(n + 1));
const paper = { id: "paper-H", date: "2026-09-29", scope: "H", version: 0, status: "pending", shortage: { written: 1, duty: 0, professional: 0 }, questions: bank.slice(0, 10).map(q => ({ ...structuredClone(q), attempt: null, note: "", favorite: false, practice: [] })) };
paper.questions[1].type = "multiple"; paper.questions[1].type_label = "不定项"; paper.questions[1].correct_option_ids = ["q2-a", "q2-c"];
paper.questions[2].type = "interview"; paper.questions[2].bank = "duty"; paper.questions[2].options = []; paper.questions[2].correct_option_ids = [];
paper.questions[3].invalid = true;
const issues = [];
const scopeOptions = "ABCDEH".split("").map(value => ({ value, label: `${value}楼` }));
function publicPaper() {
  const result = structuredClone(paper);
  result.questions = result.questions.map(q => {
    const answer = { correct_option_ids: q.correct_option_ids, answer_text: q.answer_text, analysis: q.analysis, hint: q.hint, attachments: [] };
    delete q.correct_option_ids; delete q.answer_text; delete q.analysis; delete q.hint;
    if (q.attempt || q.revealed) q.answer = answer;
    else if (q.hinted) q.answer = { hint: answer.hint };
    return q;
  });
  result.stats = { total: result.questions.length, answered: result.questions.filter(q => q.attempt).length };
  return result;
}
const summary = { assigned: 9, answered: 3, completion_rate: 33.3, accuracy: 50, independent_accuracy: 100, hint_rate: 33.3, review_total: 1, interview_total: 1 };
async function fixture(route) {
  const request = route.request(), url = new URL(request.url());
  if (url.origin !== base) { unexpected.push(request.url()); return route.abort(); }
  if (!url.pathname.startsWith("/api/")) return route.continue();
  const p = url.pathname, method = request.method();
  const body = request.headers()["content-type"]?.includes("json") ? request.postDataJSON() : null;
  calls.push({ p, method, body, query: Object.fromEntries(url.searchParams) });
  const ok = data => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, data }) });
  const fail = (message, status = 400) => route.fulfill({ status, contentType: "application/json", body: JSON.stringify({ ok: false, error: message }) });
  if (p === "/api/health") return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, service: "clipflow_backend", instance_id: "learning-fixture" }) });
  if (p === "/api/auth/status") return ok({ logged_in: true, user: { open_id: admin ? "admin-user" : "H-user", name: admin ? "管理员" : "H楼值班", role: admin ? "admin" : "user" }, scope_options: admin ? scopeOptions : [scopeOptions[5]] });
  if (p === "/api/scope-overview") return ok({ scopes: {} });
  if (p === "/api/handover-links") return ok({ links: {} });
  if (p === "/api/auth/permission-requests/current") return ok({});
  if (p === "/api/learning/bootstrap") return ok({ is_admin: admin, can_answer: true, silent_manual_publish: silentPublishing, scope: admin ? url.searchParams.get("scope") || "" : "H", scopes: admin ? [{ value: "", label: "全部楼栋" }, ...scopeOptions] : [scopeOptions[5]], settings, sync: { status: publishingPolls-- > 0 ? "publishing" : "ready", pending: 0 }, today: "2026-09-29", question_problem_count: 4, summary });
  if (p === "/api/learning/papers" || p === "/api/learning/history") return ok({ items: paperDeleted ? [] : [publicPaper()], total: paperDeleted ? 0 : 1, page: 1, page_size: 20, today: "2026-09-29" });
  if (p === "/api/learning/papers/paper-H" && method === "GET") return paperDeleted ? fail("题单已删除", 404) : ok(publicPaper());
  if (p === "/api/learning/papers/paper-H" && method === "DELETE") { assert(admin); paperDeleted = true; return ok({ id: "paper-H", deleted: true }); }
  if (p.startsWith("/api/learning/papers/paper-H/")) {
    const q = paper.questions.find(v => v.id === body.question_id);
    if (p.endsWith("/answer")) {
      assert.equal(typeof body.operation_id, "string");
      if (staleAnswer) { staleAnswer = false; q.attempt = { option_ids: [q.options[1].id], correct: false, submitted_at: "2026-09-29T10:00:00+08:00" }; paper.version++; return fail("该账号记录已更新", 409); }
      const previous = [q.attempt, ...q.practice].filter(Boolean).find(a => a.operation_id === body.operation_id);
      if (!previous) {
        assert.equal(body.version, paper.version);
        if (q.type === "interview") assert(["部分掌握", "需复习"].includes(body.self_rating));
        const attempt = { ...body, correct: q.type === "interview" ? null : JSON.stringify([...body.option_ids].sort()) === JSON.stringify([...q.correct_option_ids].sort()), submitted_at: "2026-09-29T09:00:00+08:00", assisted: !!q.hinted };
        if (body.practice) q.practice.push(attempt); else q.attempt = attempt;
        paper.version++;
      }
      if (lostAnswer) { lostAnswer = false; return route.abort("connectionreset"); }
    } else if (p.endsWith("/reveal")) { q.hinted = true; if (body.kind !== "hint") q.revealed = true; paper.version++; }
    else if (p.endsWith("/notes")) { Object.assign(q, body); paper.version++; }
    else { unexpected.push(p); return fail("unexpected mock endpoint"); }
    return ok(publicPaper());
  }
  if (p === "/api/learning/review") return ok({ items: [{ paper_id: paper.id, date: paper.date, scope: "H", question: publicPaper().questions[1] }], total: 1 });
  if (p === "/api/learning/issues" && method === "GET") return ok({ items: issues, total: issues.length, page: 1 });
  if (p === "/api/learning/issues" && method === "POST") {
    assert.equal(body.paper_id, paper.id); assert.equal(body.category, "答案");
    const item = { ...body, id: "issue-1", question: structuredClone(paper.questions.find(q => q.id === body.question_id)), version: 1, status: "pending", comments: [], attachments: [] }; issues.push(item); return ok(item);
  }
  if (p === "/api/learning/issues/issue-1" && method === "PATCH") {
    assert.equal(body.version, issues[0].version); assert(body.remark);
    Object.assign(issues[0], { status: body.status, version: issues[0].version + 1 }); issues[0].comments.push({ name: admin ? "管理员" : "H楼值班", text: body.remark }); return ok(issues[0]);
  }
  if (p === "/api/learning/attachments" && method === "POST") {
    const owner = url.searchParams.has("question_id") ? bank.find(q => q.id === url.searchParams.get("question_id")) : issues[0];
    assert.equal(url.searchParams.get("version"), String(owner.version));
    assert(request.postData().includes('name="version"'), "multipart must carry the version too");
    if (staleAttachment) { staleAttachment = false; owner.stem = "其他管理员的最新题干"; owner.version += "-concurrent"; return fail("题目已被其他管理员修改，请读取最新版本", 409); }
    const file = { id: `file-${owner.id}`, name: "证据.txt", kind: url.searchParams.get("kind"), size: 8, url: `/api/learning/attachments/file-${owner.id}` };
    owner.attachments.push(file); owner.version = typeof owner.version === "number" ? owner.version + 1 : owner.version + "-file";
    return ok({ items: [file], attachments: owner.attachments, version: owner.version });
  }
  if (p.startsWith("/api/learning/attachments/") && method === "DELETE") {
    const id = p.split("/").at(-1), owner = [...bank, ...issues].find(q => q.attachments.some(a => a.id === id));
    assert.equal(url.searchParams.get("version"), String(owner.version));
    owner.attachments = owner.attachments.filter(a => a.id !== id); owner.version += "-deleted";
    return ok({ deleted: true, version: owner.version });
  }
  if (p === "/api/learning/questions" && method === "GET") {
    const filtered = bank.filter(q => (!url.searchParams.get("status") || q.status === url.searchParams.get("status")) && (!url.searchParams.get("search") || q.stem.includes(url.searchParams.get("search"))));
    const page = Number(url.searchParams.get("page") || 1);
    assert.equal(url.searchParams.get("page_size"), "20");
    return ok({ items: filtered.slice((page - 1) * 20, page * 20), total: filtered.length, page, page_size: 20 });
  }
  if (p === "/api/learning/questions" && method === "POST") {
    assert(body.new_id.startsWith("q_"));
    const item = { ...structuredClone(body), id: body.new_id, version: "v1", problems: [] };
    bank.push(item);
    return ok(item);
  }
  if (/\/questions\/q\d+$/.test(p)) {
    const item = bank.find(q => q.id === p.split("/").at(-1));
    if (method === "GET") return ok({ ...item, audit: [{ at: "2026-09-28", actor: "管理员", reason: "答案更正", before: { stem: "原题干" }, after: { stem: item.stem } }] });
    if (method === "PUT") { assert.equal(body.version, item.version); assert(!("audit" in body)); Object.assign(item, body, { version: item.version + "-saved" }); return ok(item); }
  }
  if (p.endsWith("/status")) { const item = bank.find(q => q.id === p.split("/").at(-2)); assert.equal(body.version, item.version); item.status = body.status; item.version += "-status"; return ok(item); }
  if (p === "/api/learning/settings") {
    if (method === "PUT") Object.assign(settings, body);
    return ok(settings);
  }
  if (p === "/api/learning/import") { assert(Array.isArray(body.questions)); return ok(body.preview ? { items: body.questions, errors: [] } : { imported: body.questions.length, status: "draft" }); }
  if (p === "/api/learning/export") return url.searchParams.get("kind") === "questions"
    ? route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ questions: bank }) })
    : route.fulfill({ status: 200, contentType: "text/csv; charset=utf-8", headers: { "Content-Disposition": "attachment; filename=learning.csv" }, body: "date,scope\n2026-09-29,H\n" });
  if (p === "/api/learning/publish") { assert.equal(body.notify, false); publishingPolls = 2; return ok({ queued: true }); }
  if (p === "/api/learning/refresh") return ok({ queued: true });
  if (p === "/api/learning/profile") {
    if (profileGate) await profileGate;
    return ok({ summary, topics: [{ topic: "供电安全", answered: 3, accuracy: 33.3 }], buildings: [{ scope: "H", assigned: 9, answered: 3 }], questions: [{ id: "q1", stem: sample.stem, answered: 2, wrong: 1, issue_count: 1 }], inventory: { written: { available: 326, remaining: 14 }, duty: { available: 80, remaining: 42 }, professional: { available: 42, remaining: 14 } } });
  }
  unexpected.push(`${method} ${p}`); return fail("Unexpected API request in isolated fixture");
}

try {
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, serviceWorkers: "block" });
  await context.route("**/*", fixture);
  const page = await context.newPage();
  activePage = page;
  page.setDefaultTimeout(15000);
  page.on("pageerror", error => pageErrors.push(error.message));
  const select = async (id, value) => { await page.locator(`#${id}`).click(); await page.getByRole("option", { name: value, exact: true }).click(); };
  await page.goto(`${base}/learning?scope=H`);
  await page.getByRole("heading", { name: "画像学练", exact: true }).waitFor();
  await page.locator(".stem").waitFor();
  assert.equal(calls.filter(c => c.p === "/api/learning/papers" && c.method === "GET").at(-1).query.today, "1");
  assert.equal(await page.getByLabel("题单日期", { exact: true }).count(), 0);
  assert(await page.getByText("笔试题库缺 1 题", { exact: true }).isVisible());
  assert.equal(await page.getByRole("button", { name: "题库管理", exact: true }).count(), 0);
  await page.locator(".options input").first().check();
  await page.locator(".notes summary").click();
  await page.getByLabel("学习笔记", { exact: true }).fill("待同步的笔记");
  await page.reload();
  await page.getByText("已恢复本机草稿，尚未正式提交。", { exact: true }).waitFor();
  await page.locator(".notes summary").click();
  assert(await page.locator(".options input").first().isChecked());
  assert.equal(await page.getByLabel("学习笔记", { exact: true }).inputValue(), "待同步的笔记");
  await page.screenshot({ path: path.join(output, "learning-long-question.png"), fullPage: true });
  lostAnswer = true;
  await page.getByRole("button", { name: "确认作答", exact: true }).click();
  await page.getByRole("alert").waitFor();
  await page.getByRole("button", { name: "确认作答", exact: true }).click();
  await page.getByText("作答已确认。", { exact: true }).waitFor();
  const answers = calls.filter(c => c.p.endsWith("/answer"));
  assert.equal(answers.length, 2); assert.equal(answers[0].body.operation_id, answers[1].body.operation_id, "retry must reuse operation id");
  assert.equal(await page.getByLabel("学习笔记", { exact: true }).inputValue(), "待同步的笔记", "submitting must preserve unsaved notes");
  await page.getByRole("button", { name: "保存笔记", exact: true }).click();
  await page.getByText("学习记录已保存。", { exact: true }).waitFor();
  await page.getByRole("button", { name: "收藏题目", exact: true }).click();
  await page.getByRole("button", { name: "取消收藏", exact: true }).waitFor();
  const originalAttempt = structuredClone(paper.questions[0].attempt);
  await page.getByRole("button", { name: "再次练习", exact: true }).click();
  await page.locator(".options input").nth(1).check();
  await page.reload();
  await page.getByRole("button", { name: "提交本次复习", exact: true }).waitFor();
  assert(await page.locator(".options input").nth(1).isChecked(), "practice draft must also recover");
  await page.getByRole("button", { name: "提交本次复习", exact: true }).click();
  await page.getByText("复习记录（1 次）", { exact: true }).waitFor();
  assert.deepEqual(paper.questions[0].attempt, originalAttempt, "practice cannot overwrite first answer");
  await page.getByRole("button", { name: /^第 2 题/ }).click();
  await page.locator(".options input").first().check();
  await page.getByRole("button", { name: "思路提示", exact: true }).click();
  await page.getByRole("heading", { name: "提示", exact: true }).waitFor();
  assert(await page.locator(".options input").first().isChecked(), "reveal must preserve draft");
  staleAnswer = true;
  await page.getByRole("button", { name: "确认作答", exact: true }).click();
  await page.getByText("该题已有已确认作答，已显示最新记录。", { exact: true }).waitFor();
  assert(await page.locator(".options input").nth(1).isChecked());
  assert(await page.locator(".options input").first().isDisabled());
  await page.getByRole("button", { name: "题目有疑问", exact: true }).click();
  await page.getByLabel("问题说明", { exact: false }).fill("答案与现场条件存在差异，请核对。 ");
  await page.getByLabel("建议答案或依据", { exact: true }).fill("建议补充适用前提。");
  await page.screenshot({ path: path.join(output, "learning-new-issue.png"), fullPage: true });
  await page.getByRole("button", { name: "提交质疑", exact: true }).click();
  await page.getByRole("heading", { name: "质疑处理记录", exact: true }).waitFor();
  await page.locator('.learning-modal input[type="file"]').setInputFiles({ name: "证据.txt", mimeType: "text/plain", buffer: Buffer.from("evidence") });
  await page.getByRole("button", { name: "上传所选附件", exact: true }).click();
  await page.getByText("附件已上传。", { exact: true }).waitFor();
  await page.getByLabel("补充说明 / 处理结论", { exact: false }).fill("已补充证据附件。");
  await page.getByRole("button", { name: "提交补充与处理", exact: true }).click();
  await page.getByText("质疑记录已保存。", { exact: true }).waitFor();
  assert.equal(issues[0].version, 3, "issue upload version must flow into the next PATCH");
  await page.getByRole("button", { name: "关闭", exact: true }).click();
  await page.getByRole("button", { name: /^第 3 题/ }).click();
  await page.getByLabel("我的回答", { exact: true }).fill("先确认现场运行方式，复核操作条件，再执行切换。");
  await page.locator("#learning-select-2").click();
  assert.equal(await page.getByRole("option", { name: "已掌握", exact: true }).count(), 0);
  await page.getByRole("option", { name: "部分掌握", exact: true }).click();
  await page.getByRole("button", { name: "确认作答", exact: true }).click();
  await page.getByText("作答已确认。", { exact: true }).waitFor();
  await page.getByRole("button", { name: /^第 4 题/ }).click();
  assert(await page.getByText("此题已失效，已从评价分母剔除，原作答保留。", { exact: true }).isVisible());
  assert.equal(await page.getByRole("button", { name: "确认作答", exact: true }).count(), 0);
  await page.getByRole("button", { name: "学习历史", exact: true }).click();
  await page.getByRole("button", { name: "查看题单", exact: true }).waitFor();
  assert.equal(await page.getByRole("button", { name: "导出学习记录", exact: true }).count(), 0);
  await page.getByRole("button", { name: "错题与复习", exact: true }).click();
  await select("learning-select-3", "收藏");
  assert(calls.some(c => c.p.endsWith("/review") && c.query.kind === "favorites"));
  await page.getByRole("button", { name: "学习画像", exact: true }).click();
  await page.getByText("33.3%", { exact: true }).first().waitFor();
  assert.equal(await page.locator(".completion-ring").count(), 1);
  assert.equal(await page.locator(".topic-bar").count(), 1);
  assert.equal(await page.getByText("3330.0%", { exact: true }).count(), 0);
  assert.equal(await page.getByRole("button", { name: "题目统计", exact: true }).count(), 0);
  await page.screenshot({ path: path.join(output, "learning-profile.png"), fullPage: true });
  await page.getByRole("button", { name: "今日学练", exact: true }).click();
  await page.locator(".stem").waitFor();
  await page.setViewportSize({ width: 390, height: 844 });
  await page.screenshot({ path: path.join(output, "learning-mobile.png"), fullPage: true });
  assert(await page.locator(".learning-page").evaluate(e => e.scrollWidth <= e.clientWidth + 1), "learning page should fit a phone viewport");

  admin = true;
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.reload();
  await page.getByText("管理员作答计入H楼进度", { exact: true }).waitFor();
  await page.getByRole("button", { name: /^第 5 题/ }).click();
  assert.equal(await page.locator(".answer-reference").count(), 0);
  await page.locator(".options input").first().check();
  await page.getByRole("button", { name: "确认作答", exact: true }).click();
  assert(admin && paper.questions[4].attempt, "admin answer must update the selected building paper");
  await page.getByText("管理员作答计入H楼进度", { exact: true }).waitFor();
  await page.getByRole("button", { name: "学习历史", exact: true }).click();
  await select("learning-select-1", "全部楼栋");
  await page.getByRole("button", { name: "查看题单", exact: true }).click();
  await page.getByRole("button", { name: /^第 6 题/ }).click();
  assert(await page.getByRole("button", { name: "确认作答", exact: true }).isEnabled(), "admin may answer a paper opened from all-building history");
  await select("learning-select-1", "H楼");
  await page.getByRole("button", { name: "题库管理", exact: true }).click();
  await page.locator("table tbody tr").first().waitFor();
  assert.equal(await page.locator("table tbody tr").count(), 20);
  await page.screenshot({ path: path.join(output, "learning-admin-bank.png"), fullPage: true });
  assert.equal(await page.locator("table tbody tr").first().getByText("版本 v1", { exact: true }).count(), 0);
  await page.getByRole("button", { name: "最后一页", exact: true }).click();
  await page.getByText("2 / 2", { exact: true }).waitFor();
  await page.locator("table tbody tr").filter({ hasText: "测试题目 21" }).waitFor();
  assert.equal(await page.locator("table tbody tr").count(), 3);
  await page.getByRole("spinbutton", { name: "跳转页码", exact: true }).fill("99");
  await page.getByRole("button", { name: "跳转", exact: true }).click();
  assert.equal(await page.getByRole("spinbutton", { name: "跳转页码", exact: true }).inputValue(), "2");
  await page.getByRole("spinbutton", { name: "跳转页码", exact: true }).fill("1");
  await page.getByRole("spinbutton", { name: "跳转页码", exact: true }).press("Enter");
  await page.getByText("1 / 2", { exact: true }).waitFor();
  await page.getByRole("button", { name: "编辑题目", exact: true }).first().click();
  await page.getByRole("heading", { name: "编辑题目", exact: true }).waitFor();
  assert.equal(await page.getByLabel("解析", { exact: true }).isVisible(), false);
  assert.equal(await page.getByLabel("年度", { exact: true }).isVisible(), false);
  assert.equal(await page.getByLabel("参考答案", { exact: true }).count(), 0);
  await page.getByRole("button", { name: "下移选项", exact: true }).first().click();
  assert.equal(await page.locator('.option-edit-row input:checked').getAttribute("aria-label"), "选项 B 为正确答案");
  await page.getByRole("textbox", { name: "题干 *", exact: true }).fill("管理员修订后的题干");
  await page.locator('.learning-modal input[type="file"]').setInputFiles({ name: "证据.txt", mimeType: "text/plain", buffer: Buffer.from("evidence") });
  await page.getByRole("button", { name: "上传所选附件", exact: true }).click();
  await page.getByText("附件已上传。", { exact: true }).waitFor();
  assert.equal(await page.getByRole("textbox", { name: "题干 *", exact: true }).inputValue(), "管理员修订后的题干", "successful upload must preserve unsaved text");
  await page.getByRole("button", { name: "删除附件", exact: true }).click();
  await page.getByRole("button", { name: "确认", exact: true }).click();
  await page.getByText("附件已删除。", { exact: true }).waitFor();
  await page.screenshot({ path: path.join(output, "learning-admin-editor.png"), fullPage: true });
  await page.getByRole("button", { name: "保存题目", exact: true }).click();
  await page.getByText("题目已保存。", { exact: true }).waitFor();
  assert.equal(bank[0].options[1].id, "q1-a");
  assert.deepEqual(bank[0].correct_option_ids, ["q1-a"]);
  assert.equal(bank[0].analysis, "先核对现场条件，再执行经批准的操作。");
  await page.getByRole("textbox", { name: "题干 *", exact: true }).fill("旧窗口未保存的题干");
  await page.locator('.learning-modal input[type="file"]').setInputFiles({ name: "证据.txt", mimeType: "text/plain", buffer: Buffer.from("new evidence") });
  staleAttachment = true;
  await page.getByRole("button", { name: "上传所选附件", exact: true }).click();
  await page.getByText("题目已被其他管理员修改，请读取最新版本", { exact: true }).waitFor();
  await page.waitForFunction(() => document.querySelector('.learning-modal .modal-scroll').scrollTop === 0);
  assert.equal(await page.getByRole("textbox", { name: "题干 *", exact: true }).inputValue(), "旧窗口未保存的题干");
  assert.equal(await page.locator(".pending-file").count(), 1);
  await page.getByRole("button", { name: "读取最新版本", exact: true }).click();
  await page.getByRole("button", { name: "确认", exact: true }).click();
  await page.waitForFunction(() => document.querySelector('#learning-question-form textarea').value === "其他管理员的最新题干");
  await page.getByRole("button", { name: "关闭", exact: true }).click();
  await page.getByRole("button", { name: "新增题目", exact: true }).click();
  await page.getByRole("heading", { name: "新增题目", exact: true }).waitFor();
  assert.equal(await page.getByLabel("知识点", { exact: true }).isVisible(), false);
  assert.equal(await page.locator('.attachment-editor').getAttribute("open"), null);
  await select("learning-select-9", "不定项");
  assert.equal(await page.locator('.option-edit-row input[type="checkbox"]').count(), 2);
  await select("learning-select-9", "单选题");
  await page.getByRole("textbox", { name: "题干 *", exact: true }).fill("只填写核心信息的新题目");
  await page.getByLabel("选项 A 内容", { exact: true }).fill("正确选项");
  await page.getByLabel("选项 B 内容", { exact: true }).fill("其他选项");
  await page.getByLabel("选项 A 为正确答案", { exact: true }).check();
  await select("learning-select-10", "已发布");
  await page.screenshot({ path: path.join(output, "learning-simple-create.png"), fullPage: false });
  await page.getByRole("button", { name: "保存题目", exact: true }).click();
  await page.getByText("题目已保存。", { exact: true }).waitFor();
  const created = calls.filter(c => c.p === "/api/learning/questions" && c.method === "POST").at(-1).body;
  assert.equal(created.analysis, "");
  assert.equal(created.specialty, "");
  assert.equal(created.correct_option_ids[0], created.options[0].id);
  await page.getByRole("button", { name: "关闭", exact: true }).click();
  await page.getByRole("button", { name: "新增题目", exact: true }).click();
  await select("learning-select-8", "值班面试");
  assert(await page.getByLabel("参考答案", { exact: true }).isVisible());
  assert.equal(await page.locator(".option-edit-row").count(), 0);
  await page.getByRole("button", { name: "关闭", exact: true }).click();
  await page.getByRole("button", { name: "确认", exact: true }).click();
  await select("learning-select-4", "笔试题库");
  const download = page.waitForEvent("download");
  await page.getByRole("button", { name: "导出", exact: true }).click();
  assert.equal((await download).suggestedFilename(), "学练题库.json");
  assert.equal(calls.filter(c => c.p.endsWith("/export")).at(-1).query.bank, "written");
  await page.getByRole("button", { name: "错题与复习", exact: true }).click();
  await page.locator("table tbody tr").first().waitFor();
  assert.equal(calls.filter(c => c.p.endsWith("/review")).at(-1).query.bank, undefined);
  const reviewResponse = page.waitForResponse(r => new URL(r.url()).pathname.endsWith("/review") && new URL(r.url()).searchParams.get("bank") === "duty");
  await select("learning-select-3b", "值班面试");
  await reviewResponse;
  await page.getByRole("button", { name: "题库管理", exact: true }).click();
  await page.locator("table tbody tr").first().waitFor();
  await page.getByRole("button", { name: "清空筛选", exact: true }).click();
  await page.locator("#learning-select-4").filter({ hasText: "全部题库" }).waitFor();
  assert.equal(calls.filter(c => c.p.endsWith("/questions") && c.method === "GET").at(-1).query.bank, undefined);
  assert.equal(calls.filter(c => c.p.endsWith("/questions") && c.method === "GET").at(-1).query.scope, "H");
  await page.getByRole("button", { name: "导入", exact: true }).click();
  await page.locator('.learning-modal input[type="file"]').setInputFiles({ name: "questions.json", mimeType: "application/json", buffer: Buffer.from(JSON.stringify({ questions: [question(30)] })) });
  await page.getByText("通过", { exact: true }).waitFor();
  await page.getByRole("button", { name: /^确认导入/ }).click();
  await page.getByRole("button", { name: "确认", exact: true }).click();
  await page.getByText("导入完成。", { exact: true }).waitFor();
  assert(calls.some(c => c.p.endsWith("/import") && c.body.preview === true));
  settings.enabled = false;
  await page.getByRole("button", { name: "发布设置", exact: true }).click();
  assert.equal(await page.getByRole("textbox", { name: "学习入口", exact: true }).inputValue(), settings.portal_url + "/learning");
  assert.equal(await page.getByRole("textbox", { name: "学习入口", exact: true }).getAttribute("readonly"), "");
  assert.equal(await page.getByRole("textbox", { name: "门户地址", exact: true }).count(), 0);
  await page.getByLabel("每日自动发布", { exact: true }).check();
  await page.getByLabel("提醒未完成的楼栋", { exact: true }).check();
  await page.getByLabel("提醒时间", { exact: true }).fill("17:30");
  await page.screenshot({ path: path.join(output, "learning-admin-settings.png"), fullPage: true });
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 1000 });
    const inputBox = await page.getByRole("textbox", { name: "学习入口", exact: true }).boundingBox();
    const linkBox = await page.getByRole("link", { name: "打开学习入口", exact: true }).boundingBox();
    assert(Math.abs(inputBox.y + inputBox.height / 2 - linkBox.y - linkBox.height / 2) < 2, "portal link must remain beside the address");
    assert(await page.locator(".learning-page").evaluate(e => e.scrollWidth <= e.clientWidth + 1));
  }
  await page.screenshot({ path: path.join(output, "learning-settings-mobile.png"), fullPage: true });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.getByRole("button", { name: "保存设置", exact: true }).click();
  // The bootstrap originally said enabled, so reload the page to test first enable explicitly.
  await page.getByText("设置已保存。", { exact: true }).waitFor();
  assert.equal(settings.reminder_time, "17:30");
  assert.deepEqual(Object.keys(calls.filter(c => c.p.endsWith("/settings") && c.method === "PUT").at(-1).body).sort(), ["enabled", "publish_time", "reminder_enabled", "reminder_time"]);
  settings.enabled = false;
  await page.reload();
  await page.getByRole("button", { name: "手动发布题单", exact: true }).waitFor();
  assert(await page.getByRole("button", { name: "手动发布题单", exact: true }).isEnabled(), "manual publish must work with automatic publishing off");
  await page.getByRole("button", { name: "发布设置", exact: true }).first().click();
  await page.getByLabel("每日自动发布", { exact: true }).check();
  await page.getByRole("button", { name: "保存设置", exact: true }).click();
  await page.getByRole("dialog", { name: "启用每日自动发布", exact: true }).waitFor();
  await page.getByRole("button", { name: "确认", exact: true }).click();
  await page.getByText("设置已保存。", { exact: true }).waitFor();
  await page.getByRole("button", { name: "学习画像", exact: true }).click();
  await select("learning-select-1", "全部楼栋");
  await page.getByRole("button", { name: "六楼进度", exact: true }).click();
  assert.equal(await page.locator("table tbody tr").count(), 6);
  assert(calls.some(c => c.p.endsWith("/profile") && !c.query.scope));
  await page.locator(".inventory summary").click();
  assert(await page.locator(".inventory").innerText().then(text => text.includes("本轮剩余") && text.includes("326")));
  await select("learning-select-1", "E楼");
  await page.locator('.toolbar input[type="date"]').first().fill("2026-09-10");
  await page.locator('.toolbar input[type="date"]').last().fill("2026-09-20");
  let releaseProfile;
  profileGate = new Promise(resolve => { releaseProfile = resolve; });
  await page.getByRole("button", { name: "查询", exact: true }).click();
  await page.getByText("正在读取统计…", { exact: true }).waitFor();
  assert.equal(await page.locator('.table-wrap').getAttribute("inert"), "");
  releaseProfile(); profileGate = undefined;
  await page.getByText("正在读取统计…", { exact: true }).waitFor({ state: "hidden" });
  await page.getByRole("button", { name: "题库管理", exact: true }).click();
  await page.locator("table tbody tr").first().waitFor();
  await page.getByRole("button", { name: "学习画像", exact: true }).click();
  await page.getByText("33.3%", { exact: true }).first().waitFor();
  assert.equal(await page.locator('.toolbar input[type="date"]').first().inputValue(), "2026-09-10");
  assert.equal(await page.locator('.toolbar input[type="date"]').last().inputValue(), "2026-09-20");
  assert.equal(calls.filter(c => c.p.endsWith("/profile")).at(-1).query.scope, "E");
  const profileDownload = page.waitForEvent("download");
  await page.getByRole("button", { name: "导出报表", exact: true }).click();
  await profileDownload;
  assert.equal(calls.filter(c => c.p.endsWith("/export")).at(-1).query.from, "2026-09-10");
  assert.equal(calls.filter(c => c.p.endsWith("/export")).at(-1).query.period, "month");
  await page.getByRole("button", { name: "学习历史", exact: true }).click();
  await page.getByRole("button", { name: "查看题单", exact: true }).waitFor();
  await page.getByLabel("开始日期", { exact: true }).fill("");
  await page.getByLabel("结束日期", { exact: true }).fill("");
  const historyDownload = page.waitForEvent("download");
  await page.getByRole("button", { name: "导出学习记录", exact: true }).click();
  await historyDownload;
  const historyQuery = calls.filter(c => c.p.endsWith("/export")).at(-1).query;
  assert.equal(historyQuery.period, undefined);
  assert.equal(historyQuery.bank, undefined);
  await page.getByRole("button", { name: "问题中心", exact: true }).click();
  await page.getByRole("button", { name: "题库待核对 4 题", exact: true }).click();
  assert(await page.getByLabel("仅问题题目", { exact: true }).isChecked());
  await page.getByRole("button", { name: "今日学练", exact: true }).click();
  await page.getByRole("button", { name: "手动发布题单", exact: true }).click();
  await page.getByRole("dialog", { name: "手动发布今日题单", exact: true }).waitFor();
  assert(await page.getByRole("dialog").innerText().then(value => value.includes("不发送飞书消息")));
  await page.getByRole("button", { name: "确认", exact: true }).click();
  await page.getByText("手动发布请求已提交，不发送飞书消息。", { exact: true }).waitFor();
  await page.locator(".sync-label").filter({ hasText: "发布中" }).waitFor();
  await page.locator(".sync-label").filter({ hasText: "已同步" }).waitFor();
  silentPublishing = false;
  await page.reload();
  await page.getByText("管理员作答计入H楼进度", { exact: true }).waitFor();
  assert(await page.getByRole("button", { name: "手动发布题单", exact: true }).isDisabled());
  assert.equal(await page.getByRole("button", { name: "手动发布题单", exact: true }).getAttribute("title"), "请重启主程序后使用静默发布");
  paperDeleted = true;
  await page.evaluate(() => document.dispatchEvent(new Event("visibilitychange")));
  await page.getByText("今日暂无可学习题目", { exact: true }).waitFor();
  paperDeleted = false;
  await page.getByRole("button", { name: "刷新当前页面", exact: true }).click();
  await page.locator(".stem").waitFor();
  await page.screenshot({ path: path.join(output, "learning-admin-daily-paper.png"), fullPage: true });
  await page.setViewportSize({ width: 390, height: 844 });
  assert(await page.locator(".learning-page").evaluate(e => e.scrollWidth <= e.clientWidth + 1));
  await page.screenshot({ path: path.join(output, "learning-admin-daily-mobile.png"), fullPage: true });
  await page.setViewportSize({ width: 1440, height: 1000 });
  await page.getByRole("button", { name: "删除本楼题单", exact: true }).click();
  await page.getByRole("dialog", { name: "删除已发布题单", exact: true }).waitFor();
  await page.getByRole("button", { name: "确认", exact: true }).click();
  await page.getByText("今日暂无可学习题目", { exact: true }).waitFor();
  assert(paperDeleted);
  admin = false;
  await page.reload();
  await page.getByText("今日暂无可学习题目", { exact: true }).waitFor();
  assert.equal(await page.getByRole("button", { name: "删除本楼题单", exact: true }).count(), 0);
  await page.getByRole("button", { name: "学习历史", exact: true }).click();
  await page.getByText("暂无符合条件的记录", { exact: true }).waitFor();
  await page.getByRole("button", { name: "返回", exact: true }).click();
  await page.waitForURL(`${base}/?entry=tools`);
  await page.getByText("画像学练", { exact: true }).first().waitFor();
  assert.deepEqual(unexpected, [], "all API traffic must be handled by isolated mocks");
  assert.deepEqual(pageErrors, [], "browser must have no uncaught errors");
  await context.close();
  console.log(`Learning UI checks passed: import parsing, draft recovery, lost-response retry, stale-answer protection, notes, favorites, issue+attachment versions, interview, history, profile, admin pagination/editor/settings. Screenshots: ${output}`);
} catch (error) {
  console.error({ pageErrors, unexpected, lastCalls: calls.slice(-8), body: (await activePage?.locator('body').innerText().catch(() => '') || '').slice(-5000) });
  await activePage?.screenshot({ path: path.join(output, "failure.png"), fullPage: true }).catch(() => {});
  throw error;
} finally {
  await browser?.close();
  await server.close();
}
