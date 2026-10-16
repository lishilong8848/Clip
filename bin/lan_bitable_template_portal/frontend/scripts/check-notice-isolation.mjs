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
  await page.setContent('<div class="notice-detail-overlay open"></div><form id="lite-notice-form"><input name="work_type" value="maintenance"><input name="target_record_id" value="rec-ba"><input name="source_record_id" value="rec-plan"><button name="submit_action">Send</button></form><section id="lite-alert-tags"></section><div id="lite-event-wait-dialog" hidden><p id="lite-event-wait-copy"></p><button id="lite-event-wait-close">知道了</button></div>');
  const result = await page.evaluate(async source => {
    const previewValue = (form, name) => form?.querySelector(`[name="${name}"]`)?.value || '';
    const noticeDrawerOverlay = () => document.querySelector('.notice-detail-overlay');
    const isMissingTargetRecordId = value => !value;
    let litePageSuspended = false, status = '', errorText = '', patches = 0;
    const setLiteStatus = text => { status = text; };
    const showLiteError = text => { errorText = text; };
    let simulatedTime = Date.now();
    const sleep = milliseconds => { simulatedTime += milliseconds === 15000 ? 720000 : milliseconds; return Promise.resolve(); };
    const handleLiteAuthRequired = () => false;
    const applyJobPatch = () => { patches++; };
    const friendlyLiteMessage = text => text;
    const jobPhaseText = phase => phase;
    const deleteSubmittedLiteDraft = async () => {};
    const successfulNoticeActionText = () => 'success';
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
      target.value = 'rec-ba';latestSubmittedJobId = 'job-wait';form.dataset.pendingActionJobId = 'job-wait';setFormSubmitBusy(form, true);
      let reads = 0, unlocked = false, popupOnce = false;
      const realNow = Date.now;Date.now = () => simulatedTime;
      window.fetch = async () => {
        reads++;
        if (reads === 2) {
          unlocked = !form.classList.contains('is-submitting') && !form.querySelector('button').hasAttribute('aria-busy');
          document.getElementById('lite-event-wait-close').click();
        }
        if (reads === 3) popupOnce = document.getElementById('lite-event-wait-dialog').hidden;
        return {ok:true,json:async()=>({ok:true,data:{job_id:'job-wait',phase:reads<3?'event_waiting':'success',event_wait:{message:'A楼事件，等待结束后发送'}}})};
      };
      const finished = await pollSubmittedJob('job-wait', '', {action:'start',target_record_id:'rec-ba',source_record_id:'rec-plan'});
      Date.now = realNow;
      if (!finished || reads < 3 || !unlocked || !popupOnce) throw new Error(JSON.stringify({finished,reads,unlocked,popupOnce,status,errorText}));
      checks.push(finished && reads >= 3 && unlocked && popupOnce);
      checks.push(document.getElementById('lite-event-wait-copy').textContent === 'A楼事件，等待结束后发送');
      return checks;
    })()`);
  }, code);
  assert.deepEqual(result, [true, true, true, true, true, true, true, true]);
  console.log('[pass] notice identity isolation, event-wait popup once, unlocked copy/edit and waiting over 10 minutes resumes without timeout');
} finally { await browser.close(); }
