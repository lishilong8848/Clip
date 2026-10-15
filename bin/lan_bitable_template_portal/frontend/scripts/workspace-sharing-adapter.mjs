import { parse } from '@vue/compiler-sfc';
import { modernizeControls } from './workspace-learning-adapter.mjs';

// Keep the original requests, mutations and unsaved-change handling in both versions.
export function adaptLearningSharing(source) {
  let code = source.replaceAll('\r\n', '\n');
  const { descriptor } = parse(code), edits = [];
  const has = (node, name) => node.props?.some(p => p.type === 6 && p.name === 'class' && p.value?.content.split(' ').includes(name));
  let bank = '';
  function visit(node) {
    if (node.type === 1) {
      const original = node.loc.source;
      let replacement;
      if (node.tag === 'header' && has(node, 'sharing-toolbar')) {
        replacement = original.replace('</header>', `<NButton v-if="isAdmin && detail" type="error" quaternary :disabled="busy || loading" @click="v2DeleteBatch"><Trash2 :size="16" />删除批次</NButton></header>`);
      }
      if (has(node, 'sharing-bank')) { bank = original; replacement = ''; }
      if (node.tag === 'section' && has(node, 'sharing-section')) {
        if (original.includes('<h3>参会人答题情况')) {
          replacement = original.replace('v-if="detail.participants.length"', 'v-if="v2Section === \'people\' && detail.participants.length"')
            .replace('v-for="p in participants"', 'v-for="p in v2Participants"')
            .replace('</section>', '<NPagination v-if="participants.length > 12" v-model:page="v2PersonPage" :item-count="participants.length" :page-size="12" :page-slot="5" :disabled="busy" aria-label="参会人分页" /></section>');
        } else if (original.includes('v-if="resultPaper"')) {
          replacement = `<NModal :show="!!resultPaper" preset="card" class="v2-sharing-paper" :title="resultPaper?.name + ' · 本次答卷'" :style="{width:'min(760px,calc(100vw - 24px))'}" @update:show="value => { if (!value) selectedPerson = ''; }">${original}</NModal>`;
        } else if (original.includes('detail.weekly_reports')) {
          replacement = `<section v-if="isAdmin && v2Section === 'delivery'" class="v2-sharing-delivery">${bank}${original}<NEmpty v-if="!detail.weekly_reports?.length" description="暂无群汇总记录" /></section>`;
        } else if (original.includes('v-if="detail.questions?.length"')) {
          replacement = original.replace('v-if="detail.questions?.length"', 'v-if="v2Section === \'questions\' && detail.questions?.length"')
            .replace('class="sharing-section"', 'class="sharing-section v2-sharing-questions"')
            .replace('<article v-for="(q, i) in questions"', '<article v-for="(q, i) in questions" :data-type="q.type"')
            .replaceAll('(questionPage - 1) * 5 + i + 1', 'questionPage + i')
            .replaceAll('Math.ceil(detail.questions.length / 5)', 'detail.questions.length')
            .replaceAll('questionPage * 5 >= detail.questions.length', 'questionPage >= detail.questions.length')
            .replace('<span>每页5题</span>', '<span>逐题核对</span>')
            .replace('<article v-for=', '<NPagination v-model:page="questionPage" :item-count="detail.questions.length" :page-size="1" :page-slot="5" :disabled="busy" aria-label="选择题目" /><article v-for=');
        }
      }
      if (has(node, 'sharing-metrics')) {
        replacement = original + `<NTabs v-model:value="v2Section" type="line" animated class="v2-sharing-tabs" aria-label="分享会内容" data-v2-navigation>
          <NTab name="questions" :disabled="!detail.questions?.length">{{isAdmin ? '题目核对' : '我的题目'}} · {{detail.questions?.length || 0}}</NTab>
          <NTab name="people" :disabled="!detail.participants.length">参会人 · {{detail.participants.length}}</NTab>
          <NTab v-if="isAdmin" name="delivery">通知与同步</NTab>
        </NTabs>`;
      }
      if (replacement !== undefined) { edits.push([node.loc.start.offset, node.loc.end.offset, replacement]); return; }
    }
    for (const child of node.children || []) visit(child);
  }
  visit(descriptor.template.ast);
  if (edits.length !== 7) throw new Error('Sharing V2 sections changed');
  for (const [start, end, text] of edits.sort((a,b) => b[0]-a[0])) code = code.slice(0,start)+text+code.slice(end);
  code = code.replace('.slice((questionPage.value - 1) * 5, questionPage.value * 5)', '.slice(questionPage.value - 1, questionPage.value)');
  code = code.replace('<script setup lang="ts">', `<script setup lang="ts">
import { NButton, NEmpty, NModal, NPagination, NTab, NTabs, useDialog } from 'naive-ui';
import { Trash2 } from 'lucide-vue-next';
import '../workspace-v2/sharing.css';`);
  code = code.replace('</script>', `
const v2Section = ref('questions'), v2PersonPage = ref(1);
const v2Dialog = useDialog();
function v2DeleteBatch() {
  if (!props.isAdmin || !detail.value || busy.value) return;
  const batch = {id:detail.value.id, version:detail.value.version, title:detail.value.title};
  v2Dialog.warning({title:'删除分享会批次？', content:batch.title + '。删除后停止本批答题、提醒和后续同步；已上传到多维题库的题目不会删除。', positiveText:'删除批次', negativeText:'取消', onPositiveClick:async () => {
    if (busy.value) return false;
    busy.value = true; error.value = '';
    try {
      await requestLearning({}, '/sharing/' + batch.id + '/delete', 'POST', {version:batch.version});
      clean(); detail.value = null; selectedPerson.value = ''; message.value = '批次已删除，已上传题目保留。';
      await load();
      if (!rows.value.length && page.value > 1) { page.value--; await load(); }
    } catch (e) { error.value = (e as Error).message; return false; }
    finally { busy.value = false; }
  }});
}
const v2Participants = computed(() => participants.value.slice((v2PersonPage.value - 1) * 12, v2PersonPage.value * 12));
watch(() => detail.value?.id, () => {
  v2Section.value = detail.value?.status === 'draft' || !detail.value?.participants.length ? 'questions' : 'people';
  questionPage.value = 1; v2PersonPage.value = 1;
});
watch(personSearch, () => { v2PersonPage.value = 1; });
watch(() => participants.value.length, length => { v2PersonPage.value = Math.min(v2PersonPage.value, Math.max(1, Math.ceil(length / 12))); });
</script>`);
  return modernizeControls(code);
}
