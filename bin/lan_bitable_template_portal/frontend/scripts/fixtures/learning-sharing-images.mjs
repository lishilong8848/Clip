import assert from 'node:assert/strict';

export const batchId = '1234567890abcdef1234567890abcdef';
export const deniedId = 'fedcba0987654321fedcba0987654321';
export const generationNote = '图片识别说明：图1需对照原图核对，图3未能识别。';
export const sourceImages = ['vision', 'ocr', 'unreadable', 'ocr'].map((mode, index) => {
  const id = 'abcd'[index].repeat(64);
  return { id, url: `/api/learning/sharing/${batchId}/images/${id}`, label: `材料图${index + 1}：设备流程核对图`, mode,
    text: mode === 'unreadable' ? '' : `仅管理员识别文本${index + 1}：供回路图核对使用。` };
});
export const imageSvg = `<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="1000" viewBox="0 0 1600 1000">
<rect width="1600" height="1000" fill="#f6f8fb"/><text x="80" y="100" font-size="44" fill="#26364a">Equipment Flow / Fixture</text>
<path d="M220 450H1320M520 450V700H1060V450" fill="none" stroke="#236cad" stroke-width="18"/>
<g fill="#fff" stroke="#3d6875" stroke-width="8"><rect x="120" y="320" width="280" height="260"/><rect x="650" y="320" width="280" height="260"/><rect x="1180" y="320" width="280" height="260"/></g>
<g font-size="48" fill="#26364a"><text x="180" y="470">Unit A</text><text x="710" y="470">Unit B</text><text x="1240" y="470">Unit C</text><text x="80" y="900">Check flow direction and labels.</text></g></svg>`;

export function fixtureHtml(v2) {
  return `<!doctype html><html lang="zh-CN"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
  <style>body{margin:0;padding:16px;font:14px Arial,'Microsoft YaHei',sans-serif;background:#f7f9fc}#app{max-width:1280px;margin:auto}*{box-sizing:border-box}</style>
  <div id="app" class="${v2 ? 'workspace-v2' : ''}"></div><script type="module">
  import {createApp,h} from 'vue';import {NConfigProvider,NDialogProvider} from 'naive-ui';
  import Sharing from '/src/components/LearningSharing.vue';import UiTransition from '/src/components/UiTransition.vue';
  const admin=new URL(location.href).searchParams.get('role')!=='member';
  createApp({render:()=>h(NConfigProvider,{},()=>h(NDialogProvider,{},()=>h(Sharing,{isAdmin:admin,userId:'ou_fixture'})))}).component('UiTransition',UiTransition).mount('#app');
  </script></html>`;
}

export function createFixture() {
  const bound = ids => ids.map(id => {
    const { text, ...image } = sourceImages.find(image => image.id === id);
    return image;
  });
  const questions = Array.from({ length: 7 }, (_, index) => ({
    id: `q${index + 1}`, origin: index === 2 ? 'manual' : 'ai', type: index === 3 ? 'interview' : 'single',
    stem: `设备流程知识点${index + 1}：应先核对哪项条件？`, options: index === 3 ? [] : ['核对现场运行条件', '直接开始操作'],
    correct: index === 3 ? [] : [0], reference_answer: index === 3 ? '仅提交后参考答案：先核对条件。' : '',
    explanation: `仅提交后解析${index + 1}`, evidence: `仅管理员题目依据${index + 1}`,
    ...([0, 1, 3, 5].includes(index) ? { image_ids: [sourceImages[index === 1 ? 1 : 0].id], image_confirmed: index === 3 } : {}),
  }));
  const batch = { id: batchId, title: '分享会题图隔离验收', status: 'draft', version: 1, question_count: questions.length,
    question_types: ['single', 'interview'], url: '', total: 1, completed: 0, questions,
    generation_note: generationNote, source_images: structuredClone(sourceImages),
    participants: [{ name: '测试参会人', open_id: 'ou_fixture', person_id: 'p1', scope: 'A', exempted: false }],
  };
  const fixture = { role: 'admin', batch, saves: [], answerPosts: [], publishes: 0, requests: [], images: [], unexpected: [], denyImage: false };
  const paper = { id: 'fixture-paper', open_id: 'ou_fixture', question_ids: questions.map(q => q.id), draft_answers: {}, progress_version: 0, submitted_at: '' };
  function detail() {
    const value = structuredClone(batch);
    value.questions = value.questions.map(q => ({ ...q, images: bound(q.image_ids || []) }));
    if (fixture.role === 'member') {
      delete value.source_images; delete value.generation_note;
      value.paper = structuredClone(paper);
      value.questions = value.questions.filter(q => paper.question_ids.includes(q.id)).map(q => {
        const { evidence, image_confirmed, correct, explanation, reference_answer, ...visible } = q;
        return { ...visible, ...(paper.submitted_at ? { correct, explanation, reference_answer, selected: paper.draft_answers[q.id] } : {}) };
      });
    }
    return value;
  }
  fixture.handle = (pathname, method = 'GET', body = {}) => {
    fixture.requests.push(`${method} ${pathname}`);
    const ok = data => ({ status: 200, json: { ok: true, data } });
    const fail = (status, error) => ({ status, json: { ok: false, error } });
    if (pathname === '/api/health') return { status: 200, json: { ok: true, service: 'clipflow_backend', instance_id: 'images-fixture' } };
    if (pathname === '/api/auth/status') return ok({ logged_in: true, user: { open_id: 'ou_fixture', name: '测试参会人', role: fixture.role === 'admin' ? 'admin' : 'user', modules: ['learning'] }, scope_options: [{ value: 'A', label: 'A楼' }] });
    if (pathname === '/api/assistant/appearance') return ok({ enabled: false });
    if (pathname === '/api/assistant/icon-weather') return ok({ enabled: false });
    if (pathname === '/api/handover-links') return ok({ links: {} });
    if (pathname === '/api/auth/permission-requests/current') return ok({});
    if (pathname === '/api/scope-overview') return ok({ scopes: {} });
    if (pathname === '/api/learning/bootstrap') return ok({ is_admin: fixture.role === 'admin', can_answer: true, can_view_buildings: fixture.role === 'admin', self_person: { id: 'p1', name: '测试参会人', scopes: ['A'] }, self_scope: 'A', scopes: [{ value: 'A', label: 'A楼' }], settings: { enabled: true }, sync: { status: 'ready', pending: 0 }, today: '2026-10-22' });
    if (pathname === '/api/learning/sharing' && method === 'GET') return ok({ items: [], total: 0, page: 1 });
    if (pathname === `/api/learning/sharing/${deniedId}`) return fail(403, '无权查看此分享会批次');
    if (pathname.startsWith(`/api/learning/sharing/${batchId}/images/`)) {
      const id = pathname.split('/').at(-1);
      fixture.images.push(id);
      if (fixture.denyImage || !sourceImages.some(image => image.id === id)
        || (fixture.role === 'member' && !batch.questions.some(q => paper.question_ids.includes(q.id) && q.image_ids?.includes(id)))) return fail(403, '无权查看此图片');
      return { status: 200, contentType: 'image/svg+xml', body: imageSvg };
    }
    if (pathname === `/api/learning/sharing/${batchId}`) {
      if (method === 'PUT') {
        assert.equal(fixture.role, 'admin'); assert.equal(batch.status, 'draft');
        assert.equal(body.version, batch.version);
        for (const q of body.questions) {
          assert(!('images' in q), 'save IDs, not image descriptors');
          assert(Array.isArray(q.image_ids)); assert(q.image_ids.length <= 3);
          assert.equal(new Set(q.image_ids).size, q.image_ids.length);
          assert(q.image_ids.every(id => sourceImages.some(image => image.id === id)));
          assert.equal(typeof q.image_confirmed, 'boolean');
        }
        fixture.saves.push(structuredClone(body)); batch.questions = structuredClone(body.questions); batch.version++;
      }
      return ok(detail());
    }
    if (pathname === `/api/learning/sharing/${batchId}/publish`) {
      assert.equal(fixture.role, 'admin'); assert.equal(body.version, batch.version);
      if (batch.questions.some(q => bound(q.image_ids || []).some(image => image.mode === 'vision') && !q.image_confirmed)) return fail(400, '请核对原图');
      fixture.publishes++; batch.status = 'published'; batch.version++; return ok({ id: batchId });
    }
    if (pathname === `/api/learning/sharing/${batchId}/answers`) {
      assert.equal(fixture.role, 'member'); assert.equal(body.version, paper.progress_version);
      assert.deepEqual(Object.keys(body.answers), paper.question_ids.slice(body.page * 5, body.page * 5 + 5));
      assert.equal(typeof body.operation_id, 'string');
      assert(!JSON.stringify(body).includes('image_'), 'answer payload remains independent of images');
      Object.assign(paper.draft_answers, Object.fromEntries(Object.entries(body.answers).map(([id, value]) => [id, Array.isArray(value) ? value.map(Number) : value])));
      fixture.answerPosts.push(structuredClone(body)); paper.progress_version++;
      if (body.submit) { assert.equal(body.page, 1); paper.submitted_at = '2026-10-22T12:00:00+08:00'; batch.completed = 1; }
      return ok({ id: batchId });
    }
    fixture.unexpected.push(`${method} ${pathname}`);
    return fail(500, 'Unexpected isolated fixture endpoint');
  };
  return fixture;
}
