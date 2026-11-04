<template>
  <section ref="rootElement" class="inspection-workbook" :class="{'full-height':immersive}" :style="{'--available-height':availableHeight+'px'}" aria-label="巡检原表">
    <div class="book-tools">
      <label><span class="toolbar-label">楼栋</span> <select aria-label="楼栋" :value="scope" data-native-select :disabled="saving || dirty || !!editing || !!abnormal || sortOpen" @change="changeScope"><option v-for="s in scopes" :key="s" :value="s">{{ s }}楼</option></select></label>
      <label><span class="toolbar-label">日期</span> <input aria-label="巡检日期" :value="day" type="date" :disabled="saving || dirty || !!editing || !!abnormal || sortOpen" @change="changeDay"></label>
      <button :disabled="loading || saving || dirty || !!editing" @click="load">刷新</button>
      <button :disabled="loading || saving || dirty || !!editing || !report?.rounds.length" @click="download">导出</button>

    </div>
    <slot name="start" v-if="isCurrentDate && !active" />
    <div class="book-tools">
      <button v-for="mode in ['ALL','电气','暖通']" :key="mode" :class="{selected:professionView===mode}" @click="setProfessionView(mode)">{{ mode==='ALL'?'全部':mode }}</button>
      <button class="book-settings" @click="openSettings">表格设置</button>
      <span v-if="preferencesError" class="book-error">{{ preferencesError }}<button @click="loadPreferences">重读排序</button></span>
    </div>
    <div class="book-status" role="status"><span class="current-key"></span>本次巡检 <span class="history-key"></span>历史巡检
      <span>{{ saving ? '正在保存…' : dirty ? '有未保存修改' : '单元格修改自动保存' }}</span>
      <button v-if="dirty && !saving" @click="flush">重试保存</button><button v-if="dirty && !saving" @click="compareLatest">核对最新数据</button>
    </div>
    <p v-if="error" class="book-error" role="alert">{{ error }}</p>
    <section v-if="comparison" class="book-error" aria-label="核对单元格冲突">
      <p>请核对共享数据，选择保留哪一份；未选择的修改不会自动覆盖。</p>
      <div v-for="job in jobs" :key="job.round.round_id">
        <p v-for="cell in job.cells" :key="cell.template+cell.sheet+cell.cell">{{ cell.sheet.trim() }} {{ cell.cell }}：共享 {{ latestValue(job.round,cell) || '空白' }} / 我的 {{ cell.value || '空白' }}</p>
        <button @click="resolveJob(job,true)">核对后使用我的填写</button><button @click="resolveJob(job,false)">使用共享数据</button>
      </div>
    </section>
    <p v-if="loading && !report" role="status">正在打开巡检表…</p>
    <template v-if="report">
      <nav class="sheet-tabs" aria-label="巡检工作表">
        <button v-for="item in pages" :key="item.key" :class="{selected:pageKey===item.key,seen:visited.has(item.baseKey)}" :aria-pressed="pageKey===item.key" @click="selectPage(item.key)">{{ item.profession }} · {{ item.name.trim() }}<small>{{ item.slot ? item.slot[0]+'班第'+item.slot[1]+'次 · '+(item.saved?savedDayLabel+'已保存':item.slot===activeSlot?'本次待填写':'未开始') : visited.has(item.baseKey)?'已查看':'未查看' }}</small></button>
      </nav>
      <div v-if="currentPage" ref="scrollElement" class="book-scroll" tabindex="0" aria-label="可编辑巡检表格" :key="pageKey+chillerSlot">
        <table :style="{fontSize:zoom+'px'}"><tbody>
          <tr v-for="(row,i) in grid" :key="i"><template v-for="cell in row" :key="cell.address">
            <td v-if="!cell.hidden" v-memo="[cell.text,cell.editable,cell.current,cell.historical,cell.stateCell,cell.round?.version,cell.round?.round_id,editing===cell.address,editing===cell.address?editValue:null]" :rowspan="cell.rowspan" :colspan="cell.colspan" :class="{'current-cell':cell.current,'history-cell':cell.historical,'empty-cell':cell.mapped && !cell.text}"
              :tabindex="cell.editable?0:undefined" :aria-label="cell.address+' '+cell.text" @click="beginEdit(cell)" @keydown.enter.prevent="beginEdit(cell)">
              <select v-if="editing===cell.address && cell.stateCell" v-model="editValue" v-focus data-native-select :aria-label="'编辑 '+cell.address" @click.stop @blur="commit(cell)" @change="commit(cell)"><option value="">待填写</option><option value="√">√ 正常</option><option value="×">× 异常</option><option value="△">△ 未运行</option></select>
              <input v-else-if="editing===cell.address" v-model="editValue" v-focus @click.stop :aria-label="'编辑 '+cell.address" @blur="commit(cell)" @keydown.enter.stop.prevent="commit(cell)" @keydown.esc.stop="cancelEdit">
              <span v-else>{{ cell.text || (cell.mapped?'待填':'\u00a0') }}</span>
            </td>
          </template></tr>
        </tbody></table>
      </div>
      <p v-if="!report.rounds.length">尚无巡检记录；开始本轮后即可填写。原表已显示。</p>
      <footer v-if="currentActive && !readOnly" class="book-submit">
        <span v-if="!allVisited">已查看 {{ requiredPages.filter(p=>visited.has(p.baseKey)).length }} / {{ requiredPages.length }} 个工作表，请逐页查看。</span>
        <button v-if="allVisited || active?.pending" class="primary" :disabled="saving || dirty || loading || parentBusy" @click="confirmSubmit=true">{{ active?.pending?'重试提交归档':'确认提交' }}</button>
      </footer>
    </template>
    <NModal v-model:show="settingsOpen" preset="card" title="表格设置" style="width:min(94vw,460px)">
      <div class="book-settings-content">
      <label>缩放 <select v-model.number="zoom" data-native-select><option :value="12">小</option><option :value="14">标准</option><option :value="18">大</option><option :value="22">更大</option></select></label>      <button :disabled="!preferencesReady || saving || dirty" @click="openSort">工作表排序</button>
      <label v-if="currentActive && !readOnly && currentPage">本班归属 <select :value="currentPage.profession" :disabled="saving || dirty || parentBusy" data-native-select @change="moveProfession"><option>电气</option><option>暖通</option></select></label>
      <p>专业归属只影响本楼本班次；三套排序分别跟随个人账号保存。</p>
      </div>
    </NModal>
    <NModal v-model:show="sortOpen" preset="card" title="个人工作表排序" style="width:min(94vw,560px)" :mask-closable="false">
      <p>{{ professionView==='ALL'?'全部':professionView }}视图 · 排序同步到当前账号，手机和电脑通用</p>
      <div class="sort-list"><div v-for="(key,index) in sortDraft" :key="key" class="sort-row"><span>{{ basePages.find(p=>p.baseKey===key)?.name.trim() }}</span><button :disabled="index===0" @click="shiftSort(index,-1)">上移</button><button :disabled="index===sortDraft.length-1" @click="shiftSort(index,1)">下移</button></div>
      </div><button :disabled="saving" @click="saveSort">保存排序</button>
    </NModal>
    <NModal v-model:show="confirmSubmit" preset="dialog" title="确认提交本轮巡检" content="确认已核对所有工作表。填写会保存到本地并上传归档；上传未确认时保留原任务供重试。" positive-text="确认提交" negative-text="继续查看" @positive-click="submit" />
    <NModal :show="!!abnormal" preset="dialog" title="填写异常说明" positive-text="保存" negative-text="返回修改" @positive-click="saveAbnormal" @negative-click="abnormal=undefined" @update:show="v=>{if(!v)abnormal=undefined}"><textarea v-model="abnormalNote" placeholder="请描述该设备异常，随单元格修改保存" rows="3" /></NModal>
  </section>
</template>
<script setup lang="ts">
import {computed,nextTick,onBeforeUnmount,onMounted,ref,shallowRef,watch} from 'vue';
import {NModal} from 'naive-ui';
import {requestJson,downloadFile} from '../api/client';
import {registerNavigationGuard} from '../navigation';
type CellEdit={template:string;sheet:string;cell:string;value:string;note?:string};
type Reading=CellEdit & {value:any;read_only?:boolean;parameter?:string};
type Round={id?:string;round_id:string;shift:string;round:number;version:string|number;status:string;original_cells:Reading[];extra_cells?:Reading[]};
type Page={key:string;baseKey:string;slot?:string;saved?:boolean;template:string;profession:string;name:string;rows:any[][];merges:string[]};
type Report={draft_key:string;can_edit_history:boolean;scope:string;date:string;rounds:Round[];sheet_groups?:Record<string,string>;original_templates:{name:string;profession:string;sheets:Omit<Page,'key'|'baseKey'|'template'|'profession'>[]}[]};
type RoundMeta=Pick<Round,'id'|'round_id'|'shift'|'round'|'version'|'status'>;
type Job={scope:string;date:string;round:RoundMeta;cells:CellEdit[];attempted?:boolean};
const props=defineProps<{immersive?:boolean;scopes:string[];defaultScope?:string;defaultDate?:string;active?:{id:string;pending:boolean;duty:{scope:string;date:string;shift:string};round:number;review_version:number;professions?:string[]|null};readOnly:boolean;parentBusy:boolean}>();
const emit=defineEmits<{changed:[];scope:[scope:string]}>();
const scope=ref(props.defaultScope||props.scopes[0]||'');
const day=ref(props.defaultDate||new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai',dateStyle:'short'}).format(new Date()));
const report=shallowRef<Report>();const comparison=shallowRef<Report>();const error=ref(''),loading=ref(false),saving=ref(false),zoom=ref(14);
const pageKey=ref(''),visited=ref(new Set<string>()),editing=ref(''),editValue=ref('');
const rootElement=ref<HTMLElement>(),availableHeight=ref(600);
const professionView=ref('ALL'),preferencesReady=ref(false),preferencesError=ref(''),preferencesRevision=ref(0);
const orders=ref<Record<string,string[]>>({ALL:[],电气:[],暖通:[]}),sortOpen=ref(false),settingsOpen=ref(false),sortDraft=ref<string[]>([]);
const savedDayLabel=computed(()=>day.value===new Intl.DateTimeFormat('sv-SE',{timeZone:'Asia/Shanghai',dateStyle:'short'}).format(new Date())?'今日':day.value+' ');
const jobs=ref<Job[]>([]);const savingCells=ref(false);const dirty=computed(()=>jobs.value.length>0);const confirmSubmit=ref(false);
const abnormal=ref<{round:RoundMeta;edit:CellEdit}>(),abnormalNote=ref('');
const slots=['白1','白2','夜1','夜2'];
const activeSlot=computed(()=>props.active?props.active.duty.shift+props.active.round:'');
const isCurrentDate=computed(()=>scope.value===props.defaultScope && day.value===props.defaultDate);
const currentActive=computed(()=>props.active?.duty.scope===scope.value && props.active?.duty.date===day.value?props.active:undefined);
const basePages=computed<Page[]>(()=>report.value?.original_templates.flatMap(t=>t.sheets.map(s=>{const key=JSON.stringify([t.name,s.name]);return {...s,template:t.name,profession:report.value?.sheet_groups?.[key]||t.profession,key,baseKey:key};}))||[]);
const orderedBase=computed(()=>{
 const order=orders.value[professionView.value]||[];
 return basePages.value.filter(p=>professionView.value==='ALL'||p.profession===professionView.value).slice().sort((a,b)=>{
 const x=order.indexOf(a.baseKey),y=order.indexOf(b.baseKey);return (x<0?9999:x)-(y<0?9999:y);});
});
const pages=computed<Page[]>(()=>orderedBase.value.flatMap(p=>p.name.trim()==='暖通冷机系统巡检表'
 ? slots.map(slot=>({...p,key:p.baseKey+':'+slot,slot,saved:report.value?.rounds.some(r=>r.shift+r.round===slot&&r.status==='已归档')})) : [p])
 .sort((a,b)=>Number(!!a.saved)-Number(!!b.saved)));
function preferredPage(){const first=pages.value[0];return (first?.slot ? pages.value.find(p=>p.baseKey===first.baseKey&&p.slot===activeSlot.value)||first : first)?.key||'';}
const currentPage=computed(()=>pages.value.find(p=>p.key===pageKey.value));
const chillerSlot=computed(()=>currentPage.value?.slot||activeSlot.value||'白1');
const requiredPages=computed(()=>basePages.value);
const allVisited=computed(()=>requiredPages.value.length>0 && requiredPages.value.every(p=>visited.value.has(p.baseKey)));
const isChiller=computed(()=>currentPage.value?.name.trim()==='暖通冷机系统巡检表');
const vFocus={mounted:(el:HTMLInputElement)=>{el.focus();if(typeof el.select==='function')el.select();}};
function address(r:number,c:number){let s='';for(c++;c;c=Math.floor((c-1)/26))s=String.fromCharCode(65+(c-1)%26)+s;return s+(r+1);}
function coords(a:string){const m=/^([A-Z]+)(\d+)$/.exec(a)!;let c=0;for(const ch of m[1])c=c*26+ch.charCodeAt(0)-64;return [Number(m[2])-1,c-1];}
const text=(v:any)=>v==null?'':String(v);
const visibleRounds=computed(()=>report.value?.rounds.filter(r=>!isChiller.value || r.shift+r.round===chillerSlot.value)||[]);
type LayoutCell={address:string;raw:any;rowspan:number;colspan:number;hidden:boolean};
const layoutCache=new WeakMap<any[][],LayoutCell[][]>();
const cellLayout=computed(()=>{
 const p=currentPage.value;if(!p)return [];
 const cached=layoutCache.get(p.rows);if(cached)return cached;
 const merges=new Map<string,{rowspan:number;colspan:number}>(),hidden=new Set<string>();
 for(const range of p.merges){const [a,b=a]=range.split(':');const [r,c]=coords(a),[rr,cc]=coords(b);merges.set(a,{rowspan:rr-r+1,colspan:cc-c+1});for(let y=r;y<=rr;y++)for(let x=c;x<=cc;x++)if(y!==r||x!==c)hidden.add(address(y,x));}
 const layout=p.rows.map((row,ri)=>row.map((raw,ci)=>{const a=address(ri,ci);return {address:a,raw,...(merges.get(a)||{rowspan:1,colspan:1}),hidden:hidden.has(a)};}));layoutCache.set(p.rows,layout);return layout;
});
const sheetReadings=computed(()=>{
 const groups=new Map<string,{reading:Reading;round:Round}[]>();
 for(const r of report.value?.rounds||[])for(const c of r.original_cells||[]){const key=JSON.stringify([c.template,c.sheet]);const entries=groups.get(key)||[];entries.push({reading:c,round:r});groups.set(key,entries);}
 return groups;
});
const grid=computed(()=>{
 const p=currentPage.value;if(!p)return [];
 const values=new Map<string,{value:any;round:RoundMeta;readonly?:boolean;mapped:boolean;parameter?:string}>();const occupied=new Set<string>(),collisions=new Set<string>();
 for(const {reading:c,round:r} of sheetReadings.value.get(p.baseKey)||[])if(!isChiller.value||r.shift+r.round===chillerSlot.value){if(occupied.has(c.cell)){collisions.add(c.cell);continue;}occupied.add(c.cell);values.set(c.cell,{value:c.value,round:r,readonly:c.read_only,mapped:true,parameter:c.parameter});}
 for(const r of visibleRounds.value)for(const c of r.extra_cells||[])if(c.template===p.template&&c.sheet===p.name&&!occupied.has(c.cell))values.set(c.cell,{value:c.value,round:r,mapped:false});
 for(const job of jobs.value)for(const c of job.cells)if(c.template===p.template&&c.sheet===p.name&&(!isChiller.value||job.round.shift+job.round.round===chillerSlot.value))values.set(c.cell,{...values.get(c.cell),value:c.value,round:job.round,mapped:occupied.has(c.cell)});
 const fallback=visibleRounds.value.find(r=>r.round_id===currentActive.value?.id)||visibleRounds.value[visibleRounds.value.length-1];
 return cellLayout.value.map(row=>row.map(cell=>{const a=cell.address,raw=cell.raw,v=values.get(a),round=v?.round||fallback;
 const current=round?.round_id===currentActive.value?.id;const editable=!!round && !collisions.has(a) && !v?.readonly && (current?!props.readOnly&&!currentActive.value?.pending:report.value?.can_edit_history&&round.status==='已归档');
 return {...cell,text:collisions.has(a)?'位置冲突，请核对映射':text(v?v.value:raw),round,current:!!v&&current,historical:!!v&&!current,mapped:!!v?.mapped,stateCell:/运行状态|启停状态|开关状态|设备状态|告警状态|有无滴漏/.test(v?.parameter||'')||['√','×','△'].includes(text(v?.value)),editable:!!editable};}));
});
type GridCell=(typeof grid.value)[number][number];
let timer:number|undefined,controller:AbortController|undefined,disposed=false,storageKey='',storageUnverified=false;
function persist(){try{if(storageKey&&!storageUnverified){if(dirty.value)localStorage.setItem(storageKey,JSON.stringify(jobs.value));else localStorage.removeItem(storageKey);}}catch{error.value='浏览器暂存失败，请保持页面打开直到保存完成。';}}
function markViewed(){if(currentPage.value && (!isChiller.value || chillerSlot.value===activeSlot.value || !currentActive.value))visited.value=new Set([...visited.value,currentPage.value.baseKey]);}
function roundMeta(r:RoundMeta):RoundMeta{return {id:r.id,round_id:r.round_id,shift:r.shift,round:r.round,version:r.version,status:r.status};}
function context(){return JSON.stringify([scope.value,day.value]);}
let mutation=0,inFlight:Job|undefined;
let editor:{cell:GridCell;template:string;sheet:string;scope:string;date:string}|undefined;
function finishEdit(){if(editor)commit(editor.cell);}
function selectPage(key:string){finishEdit();rememberScroll();pageKey.value=key;void nextTick(()=>{markViewed();restoreScroll();});}
watch([pageKey,chillerSlot],markViewed);
function beginEdit(cell:GridCell){
 if(editing.value===cell.address)return;
 if(storageUnverified||!cell.editable||(saving.value&&!savingCells.value)||props.parentBusy||!currentPage.value||loading.value)return;
 finishEdit();editing.value=cell.address;editValue.value=cell.text;
 editor={cell,template:currentPage.value.template,sheet:currentPage.value.name,scope:scope.value,date:day.value};
}
function queue(round:RoundMeta,edit:CellEdit){
 mutation++;
 let job=jobs.value.find(j=>j!==inFlight&&!j.attempted&&j.round.round_id===round.round_id);
 if(!job){job={scope:scope.value,date:day.value,round:roundMeta(report.value?.rounds.find(r=>r.round_id===round.round_id)||round),cells:[]};jobs.value.push(job);}
 const existing=job.cells.find(c=>c.template===edit.template&&c.sheet===edit.sheet&&c.cell===edit.cell);
 if(existing)Object.assign(existing,edit);else job.cells.push(edit);
 persist();scheduleSave();
}
function scheduleSave(){window.clearTimeout(timer);timer=window.setTimeout(()=>void flush(),650);}
function commit(cell:GridCell){
 if(editing.value!==cell.address||!editor)return;
 const target=editor;editing.value='';editor=undefined;
 if(editValue.value===target.cell.text||!target.cell.round)return;
 if(target.scope!==scope.value||target.date!==day.value){error.value='填写所属楼栋或日期已变化，请重新核对。';return;}
 const edit={template:target.template,sheet:target.sheet,cell:target.cell.address,value:editValue.value};
 if(edit.value==='×'){abnormal.value={round:target.cell.round,edit};abnormalNote.value='';return;}
 queue(target.cell.round,edit);
}
function cancelEdit(){editing.value='';editor=undefined;if(dirty.value)scheduleSave();}
function saveAbnormal(){if(!abnormal.value||!abnormalNote.value.trim())return false;queue(abnormal.value.round,{...abnormal.value.edit,note:abnormalNote.value.trim()});abnormal.value=undefined;return true;}
async function load(){
 if(disposed||dirty.value||saving.value||editing.value||abnormal.value)return;
 if(loading.value&&controller&&!controller.signal.aborted)return;
 controller?.abort();const ctl=controller=new AbortController(),key=context(),stamp=mutation;
 loading.value=true;error.value='';
 try{
  const value=await requestJson('/api/daily-inspection/day-sheet',{method:'POST',body:JSON.stringify({scope:scope.value,date:day.value,drafts:{},form_only:true}),signal:ctl.signal,timeoutMs:180000}) as Report;
  if(disposed||ctl.signal.aborted||controller!==ctl||key!==context()||stamp!==mutation||dirty.value||editing.value)return;
  if(value.scope!==scope.value||value.date!==day.value||!value.draft_key)throw new Error('巡检表返回的楼栋或日期不一致，未替换当前数据。');
  report.value=value;storageUnverified=false;storageKey='inspection-sheet-edits:'+value.draft_key;
  try{const pending=JSON.parse(localStorage.getItem(storageKey)||'[]');
   if(!Array.isArray(pending))throw new Error();
   jobs.value=pending.map((job:any)=>{
    if(!job.round?.round_id||!Array.isArray(job.cells)||(job.scope&&job.scope!==scope.value)||(job.date&&job.date!==day.value))throw new Error();
    return {scope:scope.value,date:day.value,round:roundMeta(job.round),cells:job.cells,attempted:job.attempted!==false};
   });
  }catch{storageUnverified=true;error.value='暂存修改无法核验，请先核对本机数据；未自动提交。';}
  if(!pages.value.some(p=>p.key===pageKey.value))pageKey.value=preferredPage();
  await nextTick();markViewed();measureHeight();restoreScroll();
 }catch(e){if(!ctl.signal.aborted&&key===context())error.value=e instanceof Error?e.message:'巡检表读取失败';}
 finally{if(controller===ctl)loading.value=false;}
}
function applySaved(job:Job,version:string|number){
 const value=report.value;if(!value)return;
 report.value={...value,rounds:value.rounds.map(r=>{
  if(r.round_id!==job.round.round_id)return r;
  const readings=r.original_cells.map(c=>({...c})),extras=(r.extra_cells||[]).map(c=>({...c}));
  for(const edit of job.cells){const existing=[...readings,...extras].find(c=>c.template===edit.template&&c.sheet===edit.sheet&&c.cell===edit.cell);if(existing)existing.value=edit.value;else extras.push({...edit});}
  return {...r,version,original_cells:readings,extra_cells:extras};
 })};
 for(const queued of jobs.value)if(queued!==job&&queued.round.round_id===job.round.round_id)queued.round.version=version;
}
async function flush(){
 if(saving.value||props.parentBusy||editing.value||abnormal.value||comparison.value||!dirty.value||disposed)return;
 saving.value=true;savingCells.value=true;error.value='';window.clearTimeout(timer);controller?.abort();loading.value=false;
 const key=context();
 try{
  while(jobs.value.length&&!disposed){
   const job=jobs.value[0],r=job.round;
   if(job.scope!==scope.value||job.date!==day.value)throw new Error('暂存修改不属于当前楼栋和日期，已停止发送');
   inFlight=job;job.attempted=true;persist();
   let result:any,version:string|number;
   if(r.round_id===currentActive.value?.id){
    result=await requestJson('/api/daily-inspection/draft?view_scope='+encodeURIComponent(job.scope),{method:'POST',body:JSON.stringify({id:r.round_id,review_version:r.version,sheet_cells:job.cells}),timeoutMs:180000});
    if(result.active?.id!==r.round_id||typeof result.active.review_version!=='number')throw new Error('保存回执未确认原巡检轮次');
    version=result.active.review_version;
   }else{
    if(r.status!=='已归档'||!r.id)throw new Error('原巡检状态已变化，请核对最新数据');
    result=await requestJson('/api/daily-inspection/history-edit?scope='+encodeURIComponent(job.scope),{method:'POST',body:JSON.stringify({id:r.id,version:r.version,cells:job.cells}),timeoutMs:180000});
    if(result.id!==r.id||result.scope!==job.scope||typeof result.version!=='string')throw new Error('历史修改回执未确认原记录');
    version=result.version;
   }
   if(disposed||key!==context())return;
   applySaved(job,version);jobs.value.shift();inFlight=undefined;persist();
   if(editing.value||abnormal.value)break;
  }
  if(!dirty.value)emit('changed');
 }catch(e){if(!disposed&&key===context())error.value=(e instanceof Error?e.message:'保存尚未确认')+'；未确认修改已保留。';}
 finally{inFlight=undefined;saving.value=false;savingCells.value=false;}
}
async function compareLatest(){
 if(saving.value||loading.value)return;finishEdit();controller?.abort();const ctl=controller=new AbortController(),key=context();loading.value=true;
 try{const value=await requestJson('/api/daily-inspection/day-sheet',{method:'POST',body:JSON.stringify({scope:scope.value,date:day.value,drafts:{},form_only:true}),signal:ctl.signal,timeoutMs:180000}) as Report;
 if(!disposed&&!ctl.signal.aborted&&key===context()&&value.scope===scope.value&&value.date===day.value)comparison.value=value;}
 catch(e){if(!disposed&&!ctl.signal.aborted&&key===context())error.value=e instanceof Error?e.message:'最新数据读取失败';}
 finally{if(controller===ctl)loading.value=false;}
}
function latestValue(round:RoundMeta,cell:CellEdit){const r=comparison.value?.rounds.find(r=>r.round_id===round.round_id);const c=[...(r?.original_cells||[]),...(r?.extra_cells||[])].find(c=>c.template===cell.template&&c.sheet===cell.sheet&&c.cell===cell.cell);if(c)return text(c.value);
 const sheet=comparison.value?.original_templates.find(t=>t.name===cell.template)?.sheets.find(s=>s.name===cell.sheet);const [ri,ci]=coords(cell.cell);return text(sheet?.rows[ri]?.[ci]);}
function resolveJob(job:Job,keep:boolean){
 const latest=comparison.value?.rounds.find(r=>r.round_id===job.round.round_id);if(keep&&!latest){error.value='原巡检记录已不存在，请先核对后选择共享数据。';return;}
 if(keep){job.round=roundMeta(latest!);job.attempted=false;}else jobs.value=jobs.value.filter(j=>j!==job);
 const unresolved=jobs.value.some(j=>j!==job&&(!comparison.value?.rounds.some(r=>r.round_id===j.round.round_id)||comparison.value?.rounds.some(r=>r.round_id===j.round.round_id&&r.version!==j.round.version)));
 if(!unresolved){report.value=comparison.value;comparison.value=undefined;error.value='';persist();if(dirty.value)void flush();}else persist();
}
async function submit(){if(!currentActive.value||saving.value||dirty.value||!allVisited.value&&!currentActive.value.pending)return;saving.value=true;error.value='';
 try{await requestJson('/api/daily-inspection/finish?view_scope='+scope.value,{method:'POST',body:JSON.stringify({id:currentActive.value.id,review_version:report.value?.rounds.find(r=>r.round_id===currentActive.value?.id)?.version??currentActive.value.review_version,sheet_workflow:true,visited_sheets:[...visited.value]}),timeoutMs:180000});emit('changed');}
 catch(e){error.value=e instanceof Error?e.message:'提交未确认，请重试原任务。';}finally{saving.value=false;await load();}}
async function download(){try{await downloadFile('/api/daily-inspection/day-export',{method:'POST',body:JSON.stringify({scope:scope.value,date:day.value,drafts:{}}),timeoutMs:180000});}catch(e){error.value=e instanceof Error?e.message:'导出失败';}}
function changeScope(e:Event){if(storageUnverified||dirty.value||saving.value||editing.value||abnormal.value||sortOpen.value)return;scope.value=(e.target as HTMLSelectElement).value;emit('scope',scope.value);void loadPreferences();reset();}
function changeDay(e:Event){if(storageUnverified||dirty.value||saving.value||editing.value||abnormal.value||sortOpen.value)return;day.value=(e.target as HTMLInputElement).value;reset();}
function reset(){settingsOpen.value=false;sortOpen.value=false;sortTarget=undefined;controller?.abort();window.clearTimeout(timer);storageKey='';mutation++;scrollPositions.clear();editor=undefined;editing.value='';comparison.value=undefined;report.value=undefined;visited.value=new Set();pageKey.value='';void load();}
watch(()=>props.defaultScope,v=>{if(v&&v!==scope.value&&!dirty.value&&!saving.value&&!editing.value&&!abnormal.value){scope.value=v;void loadPreferences();reset();}});
watch(()=>props.active?.id,()=>{visited.value=new Set();if(!saving.value&&!dirty.value)void load();});
watch(()=>props.active?.review_version,version=>{if(!saving.value&&!dirty.value&&!editing.value&&report.value?.rounds.find(r=>r.round_id===currentActive.value?.id)?.version!==version)void load();});
watch(()=>props.parentBusy,busy=>{if(!busy&&dirty.value&&!error.value)scheduleSave();});
const scrollElement=ref<HTMLElement>();const scrollPositions=new Map<string,{left:number;top:number}>();
function rememberScroll(){const el=scrollElement.value;if(el)scrollPositions.set(pageKey.value,{left:el.scrollLeft,top:el.scrollTop});}
function restoreScroll(){const el=scrollElement.value,pos=scrollPositions.get(pageKey.value);if(el){el.scrollLeft=pos?.left||0;el.scrollTop=pos?.top||0;}}
function setProfessionView(mode:string){finishEdit();rememberScroll();professionView.value=mode;void nextTick(restoreScroll);}
function measureHeight(){if(rootElement.value)availableHeight.value=Math.max(120,(window.visualViewport?.height||window.innerHeight)-rootElement.value.getBoundingClientRect().top-2);}
onMounted(()=>{measureHeight();window.addEventListener('resize',measureHeight);window.visualViewport?.addEventListener('resize',measureHeight);});
watch(pages,()=>{if(!pages.value.some(p=>p.key===pageKey.value))pageKey.value=preferredPage();});
let preferenceGeneration=0;
function preferencesUrl(){return '/api/workspace-layouts/desktop/inspection-sheets-'+scope.value;}
async function loadPreferences(){const generation=++preferenceGeneration;preferencesReady.value=false;preferencesError.value='';orders.value={ALL:[],电气:[],暖通:[]};
 try{const result=await requestJson(preferencesUrl()) as {revision:number;value:{orders:Record<string,string[]>}|null};if(disposed||generation!==preferenceGeneration)return;preferencesRevision.value=result.revision;orders.value=result.value?.orders||{ALL:[],电气:[],暖通:[]};preferencesReady.value=true;}
 catch(e){if(generation===preferenceGeneration)preferencesError.value=e instanceof Error?e.message:'个人排序读取失败';}}
function openSettings(){finishEdit();settingsOpen.value=true;}
let sortTarget:{scope:string;view:string;revision:number;orders:Record<string,string[]>}|undefined;
function openSort(){settingsOpen.value=false;sortTarget={scope:scope.value,view:professionView.value,revision:preferencesRevision.value,orders:JSON.parse(JSON.stringify(orders.value))};sortDraft.value=orderedBase.value.map(p=>p.baseKey);sortOpen.value=true;}
function shiftSort(index:number,delta:number){const list=sortDraft.value.slice();[list[index],list[index+delta]]=[list[index+delta],list[index]];sortDraft.value=list;}
async function saveSort(){if(!preferencesReady.value||saving.value||!sortTarget||sortTarget.scope!==scope.value)return;const target=sortTarget,key=context();saving.value=true;const value={version:1,orders:{...target.orders,[target.view]:sortDraft.value}};
 try{const result=await requestJson('/api/workspace-layouts/desktop/inspection-sheets-'+target.scope,{method:'PUT',body:JSON.stringify({revision:target.revision,value})}) as {revision:number};if(disposed||key!==context())return;preferencesRevision.value=result.revision;orders.value=value.orders;sortOpen.value=false;preferencesError.value='';}
 catch(e){preferencesError.value=e instanceof Error?e.message:'排序保存失败，调整仍保留';}finally{saving.value=false;}}
async function moveProfession(e:Event){const profession=(e.target as HTMLSelectElement).value;if(!currentPage.value||!currentActive.value||saving.value)return;saving.value=true;error.value='';
 try{await requestJson('/api/daily-inspection/sheet-group?view_scope='+scope.value,{method:'POST',body:JSON.stringify({id:currentActive.value.id,review_version:currentActive.value.review_version,sheet:currentPage.value.baseKey,profession})});emit('changed');}
 catch(e){error.value=e instanceof Error?e.message:'本班分组未保存';}finally{saving.value=false;await load();}}
function allowNavigation(){finishEdit();if(storageUnverified||dirty.value||saving.value||editing.value||abnormal.value||sortOpen.value){error.value='请先保存单元格修改，再离开巡检表。';return false;}return true;}
const unregister=registerNavigationGuard(allowNavigation);
defineExpose({allowNavigation});
function unload(e:BeforeUnloadEvent){if(dirty.value||saving.value||editing.value||abnormal.value)e.preventDefault();}
window.addEventListener('beforeunload',unload);
void load();void loadPreferences();
onBeforeUnmount(()=>{disposed=true;controller?.abort();window.clearTimeout(timer);persist();unregister();window.removeEventListener('beforeunload',unload);window.removeEventListener('resize',measureHeight);window.visualViewport?.removeEventListener('resize',measureHeight);});
</script>
<style scoped>
.inspection-workbook{display:grid;gap:8px;min-width:0}.book-tools,.book-status,.round-tabs{display:flex;align-items:center;gap:6px;flex-wrap:wrap}.book-tools label{display:flex;gap:4px;align-items:center}button,input,select,textarea{font:inherit;color:inherit;border:1px solid var(--inspection-line);background:var(--inspection-surface);padding:6px;min-height:34px;box-sizing:border-box}button{cursor:pointer}button:disabled{opacity:.5;cursor:default}.sheet-tabs{display:flex;overflow-x:auto;gap:4px;padding-bottom:3px}.sheet-tabs button{flex-shrink:0;text-align:left}.sheet-tabs small{display:block;font-size:11px;opacity:.7}.selected{background:var(--v2-palette-accent-text,var(--inspection-primary,#2457d6));color:var(--v2-palette-on-accent-text,#fff)}p{margin:0}.book-scroll{overflow:auto;max-height:calc(100dvh - 230px);min-height:240px;border:1px solid var(--inspection-line);overscroll-behavior:contain}table{border-collapse:collapse;min-width:100%;width:max-content;table-layout:fixed}td{border:1px solid var(--inspection-line);height:30px;min-width:72px;max-width:210px;padding:4px;white-space:pre-wrap;text-align:center;overflow-wrap:anywhere}td[tabindex]{cursor:text}td:focus{outline:2px solid #2457d6;outline-offset:-2px}td input{width:100%;min-width:72px;max-width:210px;padding:2px}.current-cell,.current-key{background:color-mix(in srgb,var(--inspection-surface) 75%,#ffc83d)}.history-cell,.history-key{background:color-mix(in srgb,var(--inspection-surface) 92%,#5783a9)}.current-key,.history-key{display:inline-block;width:14px;height:14px;border:1px solid var(--inspection-line)}.empty-cell{color:var(--cf-warning)}.book-status{font-size:12px}.book-submit{display:flex;align-items:center;justify-content:space-between;gap:8px;position:sticky;bottom:0;padding:8px 0 max(8px,env(safe-area-inset-bottom));background:var(--inspection-surface)}.primary{background:var(--v2-palette-accent-text,#2457d6);color:var(--v2-palette-on-accent-text,#fff);padding:9px 25px;margin-left:auto}.book-error{color:var(--cf-danger)}textarea{width:100%}@media(max-width:760px){.toolbar-label{display:none}input,select{font-size:16px}.book-tools{font-size:13px}.book-tools input[type=date]{width:142px}.sheet-tabs{font-size:13px}.book-scroll{max-height:calc(100dvh - 250px)}}
.book-settings{margin-left:auto}.book-settings-content{display:grid;gap:12px}.book-settings-content label{display:flex;justify-content:space-between;align-items:center;gap:8px}.book-settings-content select{font-size:16px;min-height:40px}.book-error{max-height:20dvh;overflow:auto;flex-shrink:0}.sort-list{max-height:55dvh;overflow:auto}.sort-row{display:flex;align-items:center;gap:5px;margin:5px 0}.sort-row span{flex:1}.full-height{height:var(--available-height);display:flex;flex-direction:column;overflow:hidden}.full-height>.book-tools,.full-height>.book-status,.full-height>.sheet-tabs,.full-height>.book-submit{flex-shrink:0}.full-height>.book-scroll{flex:1;min-height:0;max-height:none;touch-action:pan-x pan-y}.full-height .book-submit{position:static}.full-height .sheet-tabs{max-height:66px}
@media(max-height:500px) and (hover:none){.full-height .book-status{display:none}.full-height .sheet-tabs small{display:none}.full-height .book-tools{gap:3px}.full-height .sheet-tabs{max-height:40px}.full-height button,.full-height input,.full-height select{min-height:30px}.full-height .book-submit{padding:3px 0}.full-height{gap:4px}}
</style>
