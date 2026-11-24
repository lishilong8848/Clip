<script setup lang="ts">
import { computed, onBeforeUnmount, ref, watch } from 'vue';
import { NButton, NModal, useDialog } from 'naive-ui';
import { ArrowLeft, ChevronLeft, ChevronRight, Pencil } from 'lucide-vue-next';
import LearningQuestionFields from '../../components/LearningQuestionFields.vue';
import { requestLearning } from '../../api/learning';
import type { Dict } from '../client';
import { registerNavigationGuard } from '../navigation';

const props=defineProps<{show:boolean;queue:Dict}>();
const emit=defineEmits<{close:[];refresh:[page:number];saved:[]}>();
const dialog=useDialog();
const issueId=ref(''), detail=ref<Dict>(), question=ref<Dict>(), snapshot=ref('');
const reading=ref(false), saving=ref(false), error=ref(''), result=ref(''), conflict=ref(false);
let controller:AbortController|undefined;
const path=computed(()=>'/issues/'+encodeURIComponent(issueId.value)+'/question?workbench=1');
const dirty=computed(()=>!!question.value&&JSON.stringify(question.value)!==snapshot.value);
const otherReports=computed<Dict[]>(()=>Array.isArray(detail.value?.feedback)
  ?detail.value.feedback.filter((row:Dict)=>row.id!==issueId.value):[]);

function resetEditor(){
  controller?.abort();issueId.value='';detail.value=question.value=undefined;
  snapshot.value='';error.value='';conflict.value=false;reading.value=false;
}
function leave(proceed:()=>void){
  if(saving.value)return;
  if(dirty.value)dialog.warning({title:'放弃本次题目修改？',positiveText:'放弃修改',negativeText:'继续编辑',onPositiveClick:proceed});
  else proceed();
}
function close(){leave(()=>{resetEditor();emit('close');});}
async function read(id:string) {
  if(!id||saving.value)return;
  resetEditor();issueId.value=id;result.value='';reading.value=true;
  const request=new AbortController();controller=request;
  try{
    const data=await requestLearning({signal:request.signal},path.value);
    if(request.signal.aborted)return;
    if(data.editable!==true||!data.question||typeof data.issues_version!=='string'){
      error.value='该题已处理或当前无法编辑。';return;
    }
    detail.value=data;question.value=data.question;snapshot.value=JSON.stringify(data.question);
  }catch(e){if(!request.signal.aborted)error.value=(e as Error).message||'原题读取失败';}
  finally{if(!request.signal.aborted)reading.value=false;}
}
async function save(){
  if(saving.value||!question.value||!detail.value||conflict.value)return;
  const {version,stem,options,correct_option_ids,answer_text,analysis}=question.value;
  const request=new AbortController();controller=request;
  saving.value=true;error.value='';
  try{
    const saved=await requestLearning({signal:request.signal},path.value,'PUT',{
      version,stem,options,correct_option_ids,answer_text,analysis,issues_version:detail.value.issues_version,
    });
    if(request.signal.aborted)return;
    if(saved.resolved!==true||saved.question?.id!==question.value.id)throw new Error('保存结果待核对，请重新读取题目。');
    result.value=saved.sync_pending?'题目已保存，等待同步题库。':'题目已保存，反馈已处理。';
    resetEditor();emit('saved');
  }catch(e){
    if(!request.signal.aborted){
      error.value=(e as Error).message||'保存失败';
      conflict.value=[401,403,409].includes((e as {status?:number}).status||0);
    }
  }finally{saving.value=false;}
}
watch(()=>props.show,show=>{if(!show){resetEditor();result.value='';}});
const stopGuard=registerNavigationGuard((_target,proceed)=>{
  if(saving.value)return false;
  if(!dirty.value)return true;
  leave(()=>{resetEditor();emit('close');proceed();});
  return false;
});
onBeforeUnmount(()=>{stopGuard();controller?.abort();});
</script>

<template>
  <NModal :show="show" preset="card" title="待处理题目" :mask-closable="false" :close-on-esc="!saving"
    :closable="!saving" class="desk-review-dialog" style="width:min(680px,calc(100vw - 24px))" @update:show="value=>!value&&close()">
    <div class="desk-review" :aria-busy="reading||saving" data-v2-inline-feedback>
      <p v-if="result" class="review-result" role="status">{{ result }}</p>
      <template v-if="issueId">
        <NButton size="small" :disabled="saving" @click="leave(resetEditor)"><template #icon><ArrowLeft :size="15"/></template>返回待处理列表</NButton>
        <p v-if="reading" role="status">正在读取题目…</p>
        <p v-if="error" class="review-error" role="alert">{{ error }}</p>
        <NButton v-if="error&&(conflict||!question)" :disabled="reading||saving" @click="leave(()=>read(issueId))">重新读取题目</NButton>
        <form v-if="question" class="review-form" @submit.prevent="save">
          <LearningQuestionFields :question="question" :disabled="saving||conflict" :reason="detail?.reason"/>
          <ul v-if="otherReports.length" class="review-reports" aria-label="同题反馈">
            <li v-for="row in otherReports" :key="row.id">{{ row.scope }}楼 · {{ row.description }}</li>
          </ul>
          <footer><NButton attr-type="submit" type="primary" :loading="saving" :disabled="saving||conflict">保存并处理反馈</NButton></footer>
        </form>
      </template>
      <template v-else>
        <ul v-if="queue.items?.length" class="review-list">
          <li v-for="item in queue.items" :key="item.question_id">
            <button type="button" @click="read(item.issue_id)">
              <Pencil :size="16" aria-hidden="true"/><span><strong>{{ item.stem }}</strong><small>{{ item.feedback_count }} 条反馈</small></span>
              <ChevronRight :size="16" aria-hidden="true"/>
            </button>
          </li>
        </ul>
        <p v-else role="status">暂无待处理题目</p>
        <nav v-if="queue.total>queue.page_size" aria-label="待处理题目分页">
          <NButton size="small" aria-label="上一页" :disabled="queue.page<=1" @click="emit('refresh',queue.page-1)"><ChevronLeft :size="16"/></NButton>
          <span>{{ queue.page }} / {{ Math.ceil(queue.total/queue.page_size) }}</span>
          <NButton size="small" aria-label="下一页" :disabled="queue.page*queue.page_size>=queue.total" @click="emit('refresh',queue.page+1)"><ChevronRight :size="16"/></NButton>
        </nav>
      </template>
    </div>
  </NModal>
</template>

<style scoped>
.desk-review,.review-form,.review-list{display:grid;gap:10px;min-width:0}
.desk-review>.n-button{justify-self:start}
.desk-review p,.review-list,.review-reports{margin:0}
.review-list{list-style:none;padding:0}
.review-list button{display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:center;gap:9px;width:100%;text-align:start;padding:9px;border:1px solid var(--v2-border);border-radius:8px;background:var(--soft-field);color:var(--v2-ink);font:inherit;cursor:pointer}
.review-list button:hover{border-color:var(--v2-palette-accent-text)}
.review-list strong{display:-webkit-box;-webkit-box-orient:vertical;-webkit-line-clamp:2;overflow:hidden;overflow-wrap:anywhere}
.review-list small,.review-reports{display:block;color:var(--v2-muted);font-size:12px}
.review-reports{padding-inline-start:20px;overflow-wrap:anywhere}
.desk-review :is(nav,footer){display:flex;justify-content:flex-end;align-items:center;gap:8px}
.review-error{color:var(--cf-danger,#a43c31)}
.review-result{color:var(--v2-palette-accent-text)}
@media(max-width:600px){.desk-review .n-button{min-height:44px}}
</style>
