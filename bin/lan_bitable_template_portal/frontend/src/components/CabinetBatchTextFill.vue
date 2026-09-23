<template>
  <Teleport to="body">
    <div class="text-fill-backdrop" @click.self="close">
      <section ref="dialog" class="text-fill" role="dialog" aria-modal="true" aria-labelledby="text-fill-title" tabindex="-1">
        <header><h2 id="text-fill-title">粘贴文本识别</h2><button class="icon" aria-label="关闭文本识别" :disabled="busy" @click="close"><X :size="20" /></button></header>
        <div class="paste-area">
          <label for="text-fill-input">机柜上下电记录</label>
          <div class="paste-input"><textarea id="text-fill-input" ref="input" v-model="text" rows="2" :disabled="busy" placeholder="EA118-E2-2  A11  测试电转正式电  2026-09-16 16:02:59  2026-09-14 16:03:16" @paste="paste" /><button :disabled="busy || !text.trim()" @click="append"><ScanText :size="16" />识别</button></div>
          <div v-if="busy" class="feedback" role="status"><Loader2 :size="16" class="spin" />{{ applying ? '正在保存…' : '正在匹配本批次机柜…' }}</div>
          <div v-if="error" class="feedback error" role="alert">{{ error }}<button v-if="stale" :disabled="busy" @click="refresh">重新核对</button></div>
        </div>
        <div class="preview">
          <div v-if="rows.length" class="summary">识别 {{ rows.length }} 条 · 可填入 {{ fillable.length }} 条</div>
          <div v-if="rows.length" class="table-scroll"><table><thead><tr><th>本批次机柜</th><th v-for="field in fields" :key="field.key">{{ field.label }}</th><th>匹配结果</th><th></th></tr></thead>
            <tbody><tr v-for="row in pageRows" :key="keyOf(row)">
              <td><strong>{{ row.room }} / {{ row.rack }}</strong><small>{{ row.scope }}楼</small></td>
              <td v-for="field in fields" :key="field.key" :class="{ changed: row.row_id && row[field.key] !== targetOf(row)?.[field.key] }"><small v-if="targetOf(row)?.[field.key] && row[field.key] !== targetOf(row)?.[field.key]">原：{{ targetOf(row)?.[field.key] }}</small><span>{{ row[field.key] }}</span></td>
              <td class="match-label" :class="{ warning: row.issue || conflicts.has(row.row_id) }">{{ row.issue || (conflicts.has(row.row_id) ? '内容冲突，请移除错误记录' : repeats.has(keyOf(row)) ? '重复，跳过' : !changes(row) ? '内容一致，跳过' : '可填入') }}</td>
              <td><button class="icon" :disabled="busy" :aria-label="'移除识别记录 ' + row.rack" title="移除识别记录" @click="removeRow(row)"><X :size="15" /></button></td>
            </tr></tbody></table></div>
          <p v-else class="empty">暂无粘贴记录</p>
          <div v-if="pages > 1" class="pagination"><span>每页 25 条</span><button class="icon" :disabled="page <= 1" aria-label="上一页" @click="page--"><ChevronLeft :size="16" /></button><span>{{ page }} / {{ pages }}</span><button class="icon" :disabled="page >= pages" aria-label="下一页" @click="page++"><ChevronRight :size="16" /></button></div>
        </div>
        <footer><span>仅补填当前待办，不新增机柜</span><button :disabled="busy" @click="close">取消</button><button class="primary" :disabled="busy || stale || !fillable.length" @click="apply"><Check :size="16" />填入本批次（{{ fillable.length }}）</button></footer>
      </section>
    </div>
    <ConfirmDialog :open="discard" title="放弃本次文本回填？" message="尚未填入的识别内容将被清除，当前批次记录不受影响。" confirm-label="放弃回填" cancel-label="继续核对" @resolve="resolveDiscard" />
  </Teleport>
</template>

<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch } from 'vue';
import { Check, ChevronLeft, ChevronRight, Loader2, ScanText, X } from 'lucide-vue-next';
import { requestJson, type Dict } from '../api/client';
import { randomHexId } from '../browserStorage';
import { acquireModal } from '../modalState';
import ConfirmDialog from './ConfirmDialog.vue';

const props = defineProps<{ batchId: string }>();
const emit = defineEmits<{ close: []; applied: [batch: Dict, count: number] }>();
const fields = [{key:'action',label:'操作类型'},{key:'expected',label:'期望完成时间'},{key:'actual',label:'实际完成时间'}];
const dialog = ref<HTMLElement>(), input = ref<HTMLTextAreaElement>();
const sources = ref<{id:string;text:string}[]>([]), rows = ref<Dict[]>([]), text = ref(''), error = ref('');
const busy = ref(false), applying = ref(false), stale = ref(false), discard = ref(false), page = ref(1), version = ref(0);
const hidden = new Set<string>();
const keyOf = (row:Dict) => `${row.text_id}:${row.text_row}`;
const targetOf = (row:Dict):Dict|undefined => row.targets.length === 1 ? row.targets[0] : undefined;
const changes = (row:Dict):boolean => fields.some(field=>row[field.key] !== (targetOf(row)?.[field.key] || ''));
const overlaps = computed(()=>{
  const seen=new Set<string>(), repeats=new Set<string>(), signatures=new Map<string,Set<string>>();
  for(const row of rows.value){
    if(!row.row_id)continue;
    const signature=JSON.stringify([row.row_id,...fields.map(field=>row[field.key])]);
    if(seen.has(signature))repeats.add(keyOf(row));seen.add(signature);
    if(!signatures.has(row.row_id))signatures.set(row.row_id,new Set());
    signatures.get(row.row_id)!.add(signature);
  }
  return {repeats,conflicts:new Set([...signatures].filter(([,values])=>values.size>1).map(([id])=>id))};
});
const repeats = computed(()=>overlaps.value.repeats), conflicts = computed(()=>overlaps.value.conflicts);
const fillable = computed(()=>rows.value.filter(row=>row.row_id && !row.issue && changes(row) && !repeats.value.has(keyOf(row)) && !conflicts.value.has(row.row_id)));
const pages = computed(()=>Math.max(1,Math.ceil(rows.value.length/25)));
const pageRows = computed(()=>rows.value.slice((page.value-1)*25,page.value*25));
watch(pages,()=>{page.value=Math.min(page.value,pages.value);});
const request = (action:string,body:Dict) => requestJson(`/api/cabinet-power/batches/${props.batchId}/text-${action}`,{method:'POST',body:JSON.stringify(body),timeoutMs:90000});

async function preview(nextSources: typeof sources.value):Promise<void> {
  const result = await request('preview',{sources:nextSources});
  rows.value = result.rows.filter((row:Dict)=>!hidden.has(keyOf(row))).map((row:Dict)=>({
    ...row,row_id:targetOf(row)?.editable ? targetOf(row)?.row_id : '',
    issue:row.issue || (row.targets.length>1 ? '同柜多条，请在待办中核对' : ''),
  }));
  sources.value=nextSources;version.value=result.version;stale.value=false;
}
function paste(event:ClipboardEvent):void {
  if(busy.value)return;
  const value=event.clipboardData?.getData('text/plain');
  if(value?.trim()){event.preventDefault();text.value=value;void append();}
}
async function append():Promise<void> {
  if(busy.value || !text.value.trim())return;
  busy.value=true;error.value='';
  try {await preview([...sources.value,{id:'fill_'+randomHexId(),text:text.value}]);text.value='';page.value=pages.value;}
  catch(exc:any){error.value=exc.message||'文本识别失败，请重新粘贴';}
  finally{busy.value=false;await nextTick();input.value?.focus();}
}
async function refresh():Promise<void> {
  if(busy.value || !sources.value.length)return;
  busy.value=true;error.value='';
  try{await preview(sources.value);}catch(exc:any){error.value=exc.message||'重新核对失败';}
  finally{busy.value=false;}
}
function removeRow(row:Dict):void {hidden.add(keyOf(row));rows.value=rows.value.filter(item=>item!==row);}
async function apply():Promise<void> {
  if(busy.value || stale.value || !fillable.value.length)return;
  busy.value=true;applying.value=true;error.value='';
  const count=fillable.value.length;
  try {
    const batch=await request('apply',{version:version.value,sources:sources.value,rows:fillable.value.map(row=>({text_id:row.text_id,text_row:row.text_row,row_id:row.row_id}))});
    emit('applied',batch,count);
  } catch(exc:any){
    stale.value=true;
    error.value=exc.status===409 ? '批次内容已变化，请重新核对后再填入。' : `${exc.message||'回填保存未完成'}；请重新核对保存结果。`;
  } finally{busy.value=false;applying.value=false;}
}
function close():void {if(busy.value)return;if(rows.value.length||text.value.trim())discard.value=true;else emit('close');}
let pendingNavigation:(()=>void)|undefined;
function requestLeave(proceed:()=>void):void {
  if(busy.value)return;
  if(rows.value.length||text.value.trim()){pendingNavigation=proceed;discard.value=true;}
  else{emit('close');proceed();}
}
function resolveDiscard(value:boolean):void {discard.value=false;const proceed=pendingNavigation;pendingNavigation=undefined;if(value){emit('close');proceed?.();}}
defineExpose({requestLeave});
let modal:ReturnType<typeof acquireModal>|undefined;
let returnFocus:HTMLElement|null=null;
function keydown(event:KeyboardEvent):void {
  if(!modal?.isTop() || !dialog.value)return;
  if(event.key==='Escape'){event.preventDefault();event.stopImmediatePropagation();close();}
  if(event.key==='Tab'){
    const nodes=Array.from(dialog.value.querySelectorAll<HTMLElement>('button:not(:disabled),textarea:not(:disabled)')).filter(node=>node.getClientRects().length);
    const first=nodes[0],last=nodes[nodes.length-1];
    if(!dialog.value.contains(document.activeElement)||(event.shiftKey?document.activeElement===first:document.activeElement===last)){event.preventDefault();(event.shiftKey?last:first)?.focus();}
  }
}
onMounted(async()=>{returnFocus=document.activeElement as HTMLElement;modal=acquireModal();window.addEventListener('keydown',keydown,true);await nextTick();input.value?.focus();});
onBeforeUnmount(()=>{modal?.release();window.removeEventListener('keydown',keydown,true);if(returnFocus?.isConnected)returnFocus.focus();});
</script>

<style scoped>
.text-fill-backdrop{position:fixed;inset:0;z-index:880;display:grid;place-items:center;padding:24px;background:rgba(15,30,48,.38)}
.text-fill{box-sizing:border-box;width:min(1060px,100%);max-height:calc(100dvh - 48px);display:flex;flex-direction:column;border:1px solid #d8e2ed;border-radius:12px;background:#fff;color:#203246;box-shadow:0 20px 65px #14243840;font:14px/1.5 "Microsoft YaHei",sans-serif;letter-spacing:0;overflow:hidden}
.text-fill header,.text-fill footer{display:flex;align-items:center;gap:12px;padding:14px 20px;flex-shrink:0}
.text-fill header{border-bottom:1px solid #e1e7ef}.text-fill h2{flex:1;margin:0;font-size:18px}.text-fill small,.text-fill footer span{color:#697b8d;font-size:12px}.text-fill small{display:block}
.text-fill button,.text-fill textarea{box-sizing:border-box;font:inherit;color:inherit;letter-spacing:0;border:1px solid #cdd9e5;border-radius:6px;background:white;min-width:0}
.text-fill button{display:inline-flex;align-items:center;justify-content:center;gap:6px;min-height:34px;padding:6px 12px;cursor:pointer}.text-fill button:hover{background:#edf5fc}.text-fill button:disabled{opacity:.5;cursor:not-allowed}.text-fill .icon{width:32px;min-height:32px;padding:5px;flex-shrink:0}.text-fill :focus-visible{outline:2px solid #2186c4;outline-offset:2px}
.paste-area{padding:14px 20px;background:#f5f9fc;flex-shrink:0}.paste-area label{font-size:12px;font-weight:600}.paste-input{display:flex;align-items:stretch;gap:8px;margin-top:6px}.paste-input textarea{width:100%;padding:8px 10px;resize:vertical;max-height:110px}.paste-input button{flex-shrink:0}
.feedback{display:flex;align-items:center;gap:8px;margin-top:8px;overflow-wrap:anywhere}.feedback.error{color:#ad3434}.preview{min-height:0;display:flex;flex-direction:column}.summary{padding:10px 20px;color:#527284;font-size:13px}
.table-scroll{min-height:0;overflow:auto;border-top:1px solid #e4eaf1}.text-fill table{border-collapse:collapse;width:100%;font-size:13px;table-layout:fixed;min-width:870px}.text-fill th,.text-fill td{padding:10px 12px;border-bottom:1px solid #e7edf2;text-align:left;vertical-align:middle;overflow-wrap:anywhere}.text-fill th{background:#eff5fa;position:sticky;top:0;font-weight:600;z-index:1}.text-fill th:first-child{width:100px}.text-fill th:nth-child(2){width:130px}.text-fill th:nth-child(5){width:150px}.text-fill th:last-child{width:40px}.text-fill td span{display:block}.changed span{color:#086aa8;font-weight:600}.text-fill td small{font-size:11px;line-height:1.6}.match-label{color:#237459}.match-label.warning{color:#946100}
.pagination{display:flex;gap:10px;align-items:center;justify-content:flex-end;padding:8px 20px;font-size:12px;border-top:1px solid #e7edf2}.pagination>span:first-child{margin-right:auto}.empty{text-align:center;padding:30px;color:#7b8998}
.text-fill footer{border-top:1px solid #dce5ee}.text-fill footer>span{flex:1}.text-fill button.primary{background:#126da8;color:white;border-color:#126da8}.spin{animation:text-fill-spin 1s linear infinite}@keyframes text-fill-spin{to{transform:rotate(360deg)}}
@media(max-width:700px){.text-fill-backdrop{padding:10px}.text-fill{max-height:calc(100dvh - 20px)}.text-fill header,.text-fill footer,.paste-area{padding:12px}.text-fill footer{flex-wrap:wrap}.text-fill footer>span{flex-basis:100%}}
</style>
