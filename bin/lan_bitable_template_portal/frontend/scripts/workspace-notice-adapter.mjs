// Only the independent V2 build consumes the original parser's HTML response as data.
export function adaptNoticeParserClient(code) {
  const marker='const parsed: unknown = await response.json();';
  if(code.split(marker).length!==2)throw new Error('Notice parser transport contract changed');
  return code.replace(marker, `const parsed: unknown = response.ok && path.split('?')[0] === '/workbench-lite/parse' && response.headers.get('Content-Type')?.includes('text/html')
          ? {fragments:{detail:await response.text()}} : await response.json();`);
}

// Change controls in the isolated V2 build, retaining all shared SOP selection rules.
export function adaptNoticeSopControls(code) {
  const nativeCount=(code.match(/<select\b/g)||[]).length;
  if(nativeCount!==7||(code.match(/<VnetSelect\b/g)||[]).length!==2)throw new Error('Notice SOP control contract changed');
  let result=code.replaceAll('<select ', '<NoticeSopSelect ').replaceAll('</select>', '</NoticeSopSelect>')
    .replaceAll('<VnetSelect ', '<NoticeSopSelect ').replaceAll('class="lhs-input"', '')
    .replace('import VnetSelect from "./VnetSelect.vue";', 'import NoticeSopSelect from "../workspace-v2/NoticeSopSelect.vue";\nimport "../workspace-v2/noticeSop.css";');
  const exempt='<label class="lhs-exempt"><input type="checkbox" :checked="local.exempt" :disabled="disabled" :aria-label="exemptLabel" @change="onExempt" /><span>{{ exemptLabel }}</span></label>';
  if(!result.includes(exempt))throw new Error('Notice SOP exemption control changed');
  // Keep the browser's native label/checkbox behaviour and the original change
  // handler. In particular, do not manufacture a DOM Event for a component event.
  return result.replace(exempt,`<div class="v2-sop-choice">
    <label class="lhs-exempt" :class="{'is-selected':local.exempt}"><input type="checkbox" :checked="local.exempt" :disabled="disabled" :aria-label="exemptLabel" @change="onExempt" /><span>{{ exemptLabel }}</span></label>
    <span v-if="local.exempt" class="v2-sop-exempt-status" role="status">已选择 · 本次不创建工单</span>
  </div>`)
    .replace('class="lhs"', 'class="lhs v2-notice-sop"')
    .replace('placeholder="添加操作人（可多选）"', 'placeholder="添加操作人"')
    .replace('placeholder="请选择现场审核人"', 'placeholder="选择审核人"');
}
