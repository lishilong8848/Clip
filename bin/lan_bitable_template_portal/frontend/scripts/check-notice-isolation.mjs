import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { chromium } from 'playwright';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../../..');
const html = execFileSync(path.join(root, 'bin/.venv/Scripts/python.exe'), ['-s', '-c',
  "import sys;sys.path.insert(0,'bin');from lan_bitable_template_portal.workbench_lite import render_workbench_lite;print(render_workbench_lite(payload={'records':[],'ongoing':[],'daily_summary':{'stats':{}}},session={'user':{'name':'Test'}},scope='C',work_type='maintenance'))"
], { cwd: root, encoding: 'utf8', env: { ...process.env, PYTHONIOENCODING: 'utf-8' }, maxBuffer: 4 * 1024 * 1024 });
const section = (start, end) => {
  const from = html.indexOf(start), to = html.indexOf(end, from + start.length);
  assert(from > 0 && to > from, start);
  return html.slice(from, to);
};
const code = [
  section('    function rowIdentityCandidates(', '    function clearCompletedCurrentNotice('),
  section('    let noticeTagsTimer =', '    function setSubmitButtons('),
  section('    let latestSubmittedJobId =', "    document.addEventListener('submit'"),
  section('    function setButtonBusy(', '    function setPickerOpen('),
  section('    function promoteSubmittedStartToOngoing(', '    function keepUpdatedCurrentNoticeActive('),
].join('\n');
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  await page.setContent('<div class="notice-detail-overlay open"></div><form id="lite-notice-form"><input name="work_type" value="maintenance"><input name="target_record_id" value="rec-ba"><input name="source_record_id" value="rec-plan"><button name="submit_action">Send</button></form><section id="lite-alert-tags"></section>');
  const result = await page.evaluate(async source => {
    const previewValue = (form, name) => form?.querySelector(`[name="${name}"]`)?.value || '';
    const noticeDrawerOverlay = () => document.querySelector('.notice-detail-overlay');
    const isMissingTargetRecordId = value => !value;
    let litePageSuspended = false, status = '', errorText = '', patches = 0;
    const setLiteStatus = text => { status = text; };
    const showLiteError = text => { errorText = text; };
    const sleep = () => Promise.resolve();
    const handleLiteAuthRequired = () => false;
    const applyJobPatch = () => { patches++; };
    const friendlyLiteMessage = text => text;
    const jobPhaseText = phase => phase;
    const updateActionAvailability = () => {};
    const form = document.getElementById('lite-notice-form');
    const target = form.querySelector('[name="target_record_id"]');
    // Run unchanged production functions with isolated network/side-effect boundaries.
    return await eval(`(async () => { ${source}
      const checks = [];
      checks.push(!currentNoticeMatchesDraft(form, {target_record_id:'rec-svg',source_record_id:'rec-plan'}));
      let finishTags;
      window.fetch = () => new Promise(resolve => { finishTags = resolve; });
      const loading = refreshNoticeTags();
      target.value = 'rec-svg';
      finishTags({ok:true,json:async()=>({ok:true,data:{status:'ready',title:'BA maintenance',tags:[{label:'maintenance',content:'BA'}]}})});
      await loading;
      checks.push(!document.getElementById('lite-alert-tags').textContent.includes('BA'));

      latestSubmittedJobId = 'job-ba';
      window.fetch = async () => ({ok:true,json:async()=>({ok:true,data:{phase:'failed',error:'BA failure'}})});
      await pollSubmittedJob('job-ba', '', {target_record_id:'rec-ba',source_record_id:'rec-plan'});
      checks.push(!status && !errorText && patches === 1);

      form.dataset.pendingActionJobId = 'job-ba';
      setFormSubmitBusy(form, true);
      schedulePostSubmitRefresh('', 'job-ba', {target_record_id:'rec-ba'}, form);
      form.dataset.pendingActionJobId = 'job-svg';
      await new Promise(resolve => setTimeout(resolve, 20));
      checks.push(form.dataset.pendingActionJobId === 'job-svg' && form.classList.contains('is-submitting'));
      delete form.dataset.pendingActionJobId;
      setFormSubmitBusy(form, false);
      checks.push(!form.classList.contains('is-submitting') && !form.querySelector('button').disabled);
      promoteSubmittedStartToOngoing({action:'start',target_record_id:'rec-ba',source_record_id:'rec-plan'});
      checks.push(target.value === 'rec-svg');
      return checks;
    })()`);
  }, code);
  assert.deepEqual(result, [true, true, true, true, true, true]);
  console.log('[pass] distinct target identity, stale tags/status isolation, new task ownership, busy reset, late start cannot replace another notice');
} finally { await browser.close(); }
