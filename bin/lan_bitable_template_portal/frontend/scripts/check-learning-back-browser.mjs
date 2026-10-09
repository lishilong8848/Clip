import assert from "node:assert/strict";
import { mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createServer } from "vite";
import { chromium } from "playwright";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const output = path.resolve(root, "../../../output/playwright/learning-back");
await mkdir(output, { recursive: true });
const scopesValue = "ABCDEH".split("").map(value => ({ value, label: `${value}楼` }));
const selfPerson = { id: "admin-self", name: "管理员本人", employee_no: "9000", scopes: ["A"], active: true };
const people = [
  selfPerson,
  { id: "p1", name: "测试人员甲", employee_no: "1001", scopes: ["A"], active: true },
  { id: "p2", name: "测试人员乙", employee_no: "1002", scopes: ["A"], active: true },
];
const today = "2026-10-09";
const makePaper = person_id => ({
  id: `paper-${person_id}`,
  person_id,
  person: people.find(p => p.id === person_id),
  scope: "A",
  date: today,
  version: 0,
  status: "pending",
  shortage: {},
  questions: Array.from({ length: 3 }, (_, n) => ({
    id: `q-${person_id}-${n}`,
    version: "v1",
    stem: `人员${person_id}第${n + 1}题：应先核对哪项安全条件？`,
    bank: "written",
    type: "single",
    options: [{ id: `${person_id}-a`, text: "现场安全条件" }, { id: `${person_id}-b`, text: "直接开始操作" }],
    correct_option_ids: [`${person_id}-a`],
    has_hint: true,
    attachments: [],
  })),
});
const publicPaper = p => ({ ...structuredClone(p), stats: { total: p.questions.length, answered: p.questions.filter(q => q.attempt).length, shortage: 0 } });
const papers = new Map();
const requests = [];
const unexpected = [];
const pageErrors = [];
const claims = [];
let claimGate = null;
let answerGate = null;

const server = await createServer({
  root,
  server: { host: "127.0.0.1", port: 0, strictPort: false, hmr: false },
  logLevel: "error",
});
await server.listen();
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

let browser, page;
try {
  browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({ viewport: { width: 1440, height: 1000 }, serviceWorkers: "block" });
  await context.route("**/*", async route => {
    const req = route.request(), url = new URL(req.url());
    if (url.origin !== base) { unexpected.push(req.url()); return route.abort(); }
    if (url.pathname === "/api/health") return route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, service: "clipflow_backend", instance_id: "learning-back-fixture" }) });
    if (!url.pathname.startsWith("/api/")) return route.continue();
    const p = url.pathname, method = req.method();
    const body = req.headers()["content-type"]?.includes("json") ? req.postDataJSON() : null;
    requests.push({ p, method, body, query: Object.fromEntries(url.searchParams) });
    const ok = data => route.fulfill({ status: 200, contentType: "application/json", body: JSON.stringify({ ok: true, data }) });
    if (p === "/api/auth/status") return ok({ logged_in: true, user: { open_id: "back-user", name: "测试用户", role: "user" }, scope_options: scopesValue.slice(0, 1) });
    if (p === "/api/scope-overview") return ok({ scopes: {} });
    if (p === "/api/assistant/appearance") return ok({ enabled: false });
    if (p === "/api/handover-links") return ok({ links: {} });
    if (p === "/api/auth/permission-requests/current") return ok({});
    if (p === "/api/learning/people") return ok({ items: people.filter(q => `${q.name}${q.employee_no}`.includes(url.searchParams.get("q") || "")), total: people.length, ready: true, issues: [] });
    if (p === "/api/learning/bootstrap") return ok({
      is_admin: true, can_answer: true, can_view_buildings: true,
      self_person: selfPerson, self_scope: "A", identity_issue: "",
      scope: url.searchParams.get("scope") || "A", scopes: scopesValue,
      settings: { enabled: true, publish_time: "08:00", reminder_enabled: false },
      sync: { status: "ready", pending: 0 }, today, question_problem_count: 0,
    });
    if (p === "/api/learning/papers/claim") {
      claims.push(body);
      if (claimGate) await claimGate;
      if (!papers.has(body.person_id)) papers.set(body.person_id, makePaper(body.person_id));
      try { return await ok(publicPaper(papers.get(body.person_id))); } catch { /* request aborted by nav while gated */ }
    }
    const paperAnswer = p.match(/^\/api\/learning\/papers\/paper-[^/]+\/answer$/);
    if (paperAnswer && method === "POST") {
      if (answerGate) await answerGate;
      const paper = papers.get(body.person_id) || makePaper(body.person_id);
      papers.set(body.person_id, paper);
      const q = paper.questions.find(q => q.id === body.question_id);
      if (q) q.attempt = { correct: body.option_ids?.includes(q.correct_option_ids[0]), option_ids: body.option_ids, answer_text: body.answer_text, submitted_at: today + "T09:00:00", self_rating: body.self_rating };
      return ok(publicPaper(paper));
    }
    if (/\/papers\/paper-[^/]+$/.test(p) && method === "GET") {
      const id = p.split("/").at(-1);
      return papers.has(id) ? ok(publicPaper(papers.get(id))) : ok({ id, person_id: id.slice("paper-".length), questions: [] });
    }
    if (p === "/api/learning/papers" || p === "/api/learning/history") {
      const pid = url.searchParams.get("person_id");
      const items = [...papers.values()].filter(q => !pid || q.person_id === pid).map(publicPaper);
      return ok({ items, total: items.length, page: 1, page_size: 20, today, published: true });
    }
    if (p === "/api/learning/profile") {
      const id = url.searchParams.get("person_id");
      if (!id) return ok({ person: null, published: true, summary: {}, topics: [], questions: [], buildings: scopesValue.filter(s => s.value === "A").map(s => ({ scope: s.value, assigned: 0, answered: 0 })) });
      const s = summary(id);
      return ok({ person: people.find(q => q.id === id), published: true, summary: s, today_summary: s, trend: [], banks: [], topics: [], people: [] });
    }
    if (p === "/api/learning/settings") return ok({ enabled: true });
    if (p === "/api/learning/export") return route.fulfill({ status: 200, contentType: "text/csv; charset=utf-8", body: "date,scope\n" });
    unexpected.push(`${method} ${p}`);
    return route.fulfill({ status: 500, contentType: "application/json", body: JSON.stringify({ ok: false, error: "unexpected" }) });
  });
  page = await context.newPage();
  page.setDefaultTimeout(20000);
  page.on("pageerror", error => pageErrors.push(error.message));
  page.on("console", message => { if (message.type() === "error") pageErrors.push(message.text()); });

  const backButton = () => page.locator("#page-back-slot .vnet-back-button, .vnet-back-button");

  // Scenario A: overview picker view is readonly (no claim), back direct + selector keeps overview
  await page.goto(`${base}/learning?view=overview&scope=A`);
  await page.getByRole("heading", { name: "画像学练", exact: true }).waitFor();
  await page.getByRole("button", { name: "选择人员", exact: true }).waitFor();
  assert.equal(await page.locator(".building-tabs button").count(), 6, "overview must render building tabs");
  await page.getByRole("button", { name: "选择人员", exact: true }).click();
  await page.getByRole("dialog").getByRole("button", { name: /测试人员乙/ }).click();
  await page.waitForURL(/\/learning\?.*(?:view=overview.*person_id=p2|person_id=p2.*view=overview)/);
  await page.getByRole("button", { name: "个人画像", exact: true }).waitFor();
  // overview person pulling a profile must never claim a paper
  assert.equal(claims.length, 0, "overview person profile must never POST /papers/claim");
  // overview must not expose the practice answer tabs
  assert.equal(await page.getByRole("button", { name: "今日学练", exact: true }).count(), 0, "overview must not show 今日学练 tab");
  assert.equal(await page.getByRole("button", { name: "错题与复习", exact: true }).count(), 0, "overview must not show 错题与复习 tab");
  // back removes the person but preserves the overview view
  await backButton().click();
  await page.waitForURL(/\/learning\?.*view=overview/);
  assert.equal(new URL(page.url()).searchParams.get("person_id"), null, "person_id removed on back");
  assert.equal(new URL(page.url()).searchParams.get("view"), "overview", "overview view preserved on back");
  await page.getByRole("button", { name: "选择人员", exact: true }).waitFor();
  assert.equal(await page.locator(".building-tabs button").count(), 6, "building overview must render after overview back");
  await page.screenshot({ path: path.join(output, "overview-picker-back.png"), fullPage: true });

  // Scenario B: direct URL person profile then back preserves overview
  await page.goto(`${base}/learning?view=overview&scope=A&person_id=p1`);
  await page.getByRole("button", { name: "个人画像", exact: true }).waitFor();
  await backButton().click();
  await page.waitForURL(/\/learning\?.*view=overview/);
  assert.equal(new URL(page.url()).searchParams.get("view"), "overview", "direct-url back keeps overview");
  assert.equal(new URL(page.url()).searchParams.get("person_id"), null, "direct-url back drops person");
  await page.getByRole("button", { name: "选择人员", exact: true }).waitFor();
  await page.screenshot({ path: path.join(output, "overview-direct-back.png"), fullPage: true });

  // Scenario C: practice real answer save in flight (busy) must BLOCK back, no bypass.
  papers.set(selfPerson.id, makePaper(selfPerson.id));
  await page.goto(`${base}/learning?view=practice`);
  const answerBtnC = page.getByRole("button", { name: "确认作答", exact: true });
  await answerBtnC.waitFor();
  await page.locator(".options label").first().click();
  let releaseC;
  answerGate = new Promise(resolve => { releaseC = resolve; });
  await answerBtnC.click();
  await page.waitForTimeout(150);
  await backButton().waitFor();
  assert.equal(await backButton().isDisabled(), true, "back must be disabled while a real answer save (busy) is in flight");
  const urlBeforeC = page.url();
  await backButton().click({ force: true }).catch(() => {});
  assert.equal(page.url(), urlBeforeC, "back must not navigate during a real answer save");
  releaseC();
  answerGate = null;
  await page.screenshot({ path: path.join(output, "busy-blocks-back.png"), fullPage: true });

  // Scenario D: building overview (no person) back leaves to tools
  await page.goto(`${base}/learning?view=overview&scope=A`);
  await page.getByRole("button", { name: "选择人员", exact: true }).waitFor();
  await backButton().click();
  await page.waitForURL("**/?entry=tools");
  await page.screenshot({ path: path.join(output, "overview-back-tools.png"), fullPage: true });

  assert.deepEqual(pageErrors, [], `browser errors: ${pageErrors.join("\n")}`);
  assert.deepEqual(unexpected, [], `unexpected requests: ${unexpected.join(", ")}`);
  console.log("[LearningBackBrowser] OK", { requests: requests.length, claims: claims.length, output });
} catch (error) {
  console.error("[BrowserErrors]", pageErrors, unexpected);
  await page?.screenshot({ path: path.join(output, "failure.png"), fullPage: true });
  throw error;
} finally {
  await browser?.close();
  await server.close();
}
