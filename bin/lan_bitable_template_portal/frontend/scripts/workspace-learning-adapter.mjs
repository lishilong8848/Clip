import { parse } from '@vue/compiler-sfc';

function replaceNode(source, matches, replacement) {
  const { descriptor } = parse(source), found = [];
  function visit(node) {
    if (node.type === 1 && matches(node)) found.push(node);
    for (const child of node.children || []) visit(child);
  }
  visit(descriptor.template?.ast || {});
  if (found.length !== 1) throw new Error('Learning V2 template contract changed');
  const node = found[0];
  return source.slice(0, node.loc.start.offset) + (typeof replacement === 'function' ? replacement(node.loc.source) : replacement) + source.slice(node.loc.end.offset);
}
const classes = node => node.props.find(p => p.type === 6 && p.name === 'class')?.value?.content?.split(' ') || [];
const has = name => node => classes(node).includes(name);

// Adapt presentation in the V2 build; all existing API, grading and draft code remains the controller.
export function adaptLearningPage(source) {
  let code = source.replaceAll('\r\n', '\n');
  code = code.replace('<section class="learning-page"','<section class="learning-page" :data-v2-learning-tab="tab" :class="{\'v2-learning-answer-scene\':v2AnswerScene,\'v2-learning-card-open\':v2AnswerCard}"');
  code = replaceNode(code, has('page-head'), `<header v-if="!v2AnswerScene && (!isLearningHub || canViewBuildings || isAdmin)" class="page-head v2-learning-head">
    <div class="v2-learning-identity-tabs" data-v2-navigation>
      <VnetBackButton :to="undefined" :disabled="busy" @click="handleBack" />
      <strong v-if="ready">{{learner?learner.name:scopeLabel+'学习汇总'}}</strong>
      <NRadioGroup v-if="ready && canViewBuildings && boot.can_answer" :value="viewMode" size="small" aria-label="答题与画像" :disabled="busy" @update:value="value => value === 'practice' ? goPractice() : goOverview()"><NRadioButton value="practice">个人学习</NRadioButton><NRadioButton value="overview">学习管理</NRadioButton></NRadioGroup>
      <NButton v-if="ready && viewMode==='overview' && canViewBuildings" size="small" text :disabled="busy" @click="openPeople"><Search :size="15" />{{learner?'切换人员':'选择人员'}}</NButton>
    </div>
    <div class="actions" data-v2-navigation>
      <span v-if="ready && isAdmin" class="sync-label" :class="{danger: !!boot.sync?.error, pending: !!boot.sync?.pending}"><Loader2 v-if="syncActive" :size="15" class="spin" />{{syncLabel}}<template v-if="boot.sync?.pending"> · {{boot.sync.pending}} 项待同步</template></span>
      <NDropdown v-if="ready && !isLearningHub && v2AuxOptions.length" :options="v2AuxOptions" @select="v2OpenSection"><NButton secondary size="small" :disabled="busy">{{ v2AuxTitle }}<ChevronDown :size="14" /></NButton></NDropdown>
      <span :data-v2-layout-tools="tab==='profile'?'learning-profile':'learning-practice'" class="v2-toolbar-end"></span>
    </div>
  </header>`);
  code = replaceNode(code, has('building-tabs'), `<div v-if="ready && tab !== 'sharing' && viewMode === 'overview' && !learner && canViewBuildings" class="v2-learning-building" data-v2-navigation>
    <NRadioGroup v-if="!v2Mobile" :value="scope" aria-label="学练楼栋" :disabled="busy" @update:value="v2Building"><NRadioButton v-for="building in overviewScopes" :key="building.value" :value="building.value">{{building.label}}</NRadioButton></NRadioGroup>
    <NSelect v-else :value="scope" :options="overviewScopes" aria-label="学练楼栋" :disabled="busy" @update:value="v2Building" />
  </div>`);
  code = replaceNode(code, has('learner-strip'), '');
  code = replaceNode(code, has('tabs'), `<nav v-if="ready && !isLearningHub && !v2AnswerScene" class="tabs v2-learning-tabs" aria-label="学练页面" data-v2-navigation><NButton v-for="item in v2PrimaryTabs" :key="item.id" :type="tab===item.id?'primary':'default'" :quaternary="tab!==item.id" :aria-current="tab===item.id?'page':undefined" :disabled="busy" @click="switchTab(item.id as Tab)"><component :is="item.icon" :size="16" />{{item.label}}</NButton></nav>`);
  code = replaceNode(code, node => node.tag === 'div' && has('toolbar')(node) && node.loc.source.includes('studyMode'), `<div class="toolbar v2-learning-studybar" data-v2-navigation>
    <template v-if="v2AnswerScene">
      <div class="v2-learning-answer-heading">
        <NButton v-if="completionReturn" secondary :disabled="busy || loading" @click="returnToCompletion"><ChevronLeft :size="16" />返回每日完成</NButton>
        <NButton v-else-if="reading && tab!=='today'" secondary :disabled="busy || loading" @click="saveDraft(); reading=false; loadView()"><ChevronLeft :size="16" />返回{{tab==='review'?'复习列表':'学习历史'}}</NButton>
        <NButton v-else secondary :disabled="busy || loading" @click="switchTab('home')"><ChevronLeft :size="16" />返回学习首页</NButton>
        <strong>{{tab==='review'?'错题复习':tab==='history'?'历史题单':studyMode==='practice'?'自主练习':'每日题单'}}</strong>
      </div>
      <div v-if="paper?.questions?.length" class="v2-learning-answer-progress">
        <span>已答 <b>{{answered}} / {{validCount}}</b> 题</span>
        <NButton v-if="v2Mobile" secondary :disabled="loading" :aria-expanded="v2AnswerCard" @click="v2AnswerCard=!v2AnswerCard">答题卡 · {{index+1}} / {{paper.questions.length}}<ChevronDown :size="16" /></NButton>
        <span v-else>第 {{index+1}} / {{paper.questions.length}} 题</span>
      </div>
      <NButton v-else-if="tab==='today' && boot.can_answer" type="primary" :loading="busy || loading" :disabled="busy || loading" @click="studyMode==='daily'?v2Resume('daily'):v2NewPractice()">{{studyMode==='practice'?'开始自主练习':'开始今日答题'}}</NButton>
    </template>
    <div v-else-if="reading && tab!=='today'" class="actions"><NButton v-if="completionReturn" secondary :disabled="busy" @click="returnToCompletion"><ChevronLeft :size="16" />返回每日完成</NButton><NButton v-else secondary :disabled="busy" @click="saveDraft(); reading=false; loadView()"><ChevronLeft :size="16" />返回{{tab==='review'?'复习列表':'学习历史'}}</NButton><span>{{paper?.date}} · {{paper?.scope}}楼</span></div>
    <template v-else>
      <NRadioGroup v-if="viewMode==='practice' && boot.can_answer" :value="studyMode" size="small" aria-label="学练方式" :disabled="busy || loading" @update:value="value => switchStudyMode(value as 'daily'|'practice')"><NRadioButton value="daily">每日题单</NRadioButton><NRadioButton value="practice">自主练习</NRadioButton></NRadioGroup>
      <span class="v2-learning-progress-label">{{date}}<template v-if="paper"> · 已答 {{answered}} / {{validCount}} 题</template></span>
      <div v-if="viewMode==='practice' && boot.can_answer" class="actions">
        <NButton v-if="studyMode==='daily' && !paper?.questions?.length" class="v2-learning-daily-action" type="primary" :loading="busy || loading" :disabled="busy || loading" @click="v2Resume('daily')">开始今日答题</NButton>
        <NButton v-if="studyMode==='practice'" type="primary" :loading="busy" :disabled="busy || loading" @click="v2NewPractice">新练习15题</NButton>
      </div>
    </template>
  </div>
  <section v-if="tab==='today' && v2Completed && !v2CompletionHidden && !loading" class="v2-learning-completion" aria-label="题单完成结果">
    <CheckCircle2 :size="27" /><div><strong>{{studyMode==='practice'?'本次练习已完成':'今日题单已完成'}}</strong><p>已答 {{answered}} 题 · 首次选择题正确 {{v2PaperStats.correct}} / {{v2PaperStats.choices}}<template v-if="v2PaperStats.interviews"> · 问答 {{v2PaperStats.interviews}} 题</template></p></div>
    <div class="actions" data-v2-navigation><NButton size="small" secondary @click="switchTab('review')">查看错题</NButton><NButton quaternary circle aria-label="收起完成结果" @click="v2CompletionHidden=true"><X :size="17" /></NButton></div>
  </section>`);
  code = replaceNode(code, node => node.tag==='div' && node.loc.source.includes('此处显示本题单进度'), old => `<details v-if="tab==='today' || v2AnswerScene" class="v2-learning-help"><summary>{{tab==='today'?'答题说明与题单选项':'题单信息'}}</summary><div class="v2-learning-help-content"><span>{{paper?.date || date}}<template v-if="paper?.scope"> · {{paper.scope}}楼</template></span>${old}
    <div v-if="v2AnswerScene && tab==='today' && boot.can_answer" class="actions" data-v2-navigation><NRadioGroup :value="studyMode" size="small" aria-label="学练方式" :disabled="busy || loading" @update:value="value => switchStudyMode(value as 'daily'|'practice')"><NRadioButton value="daily">每日题单</NRadioButton><NRadioButton value="practice">自主练习</NRadioButton></NRadioGroup><NButton v-if="studyMode==='practice'" secondary :disabled="busy || loading" @click="v2NewPractice">新练习15题</NButton></div>
    <span v-if="v2AnswerScene" data-v2-layout-tools="learning-practice" class="v2-toolbar-end"></span>
  </div></details>`);
  code = code.replace('v-if="boot.sync?.error && isAdmin"','v-if="boot.sync?.error && isAdmin && !v2AnswerScene"').replace('v-if="ready && !boot.settings?.enabled && isAdmin"','v-if="ready && !boot.settings?.enabled && isAdmin && !v2AnswerScene"');
  code = replaceNode(code, has('question-rail'), old => `<details class="question-rail v2-learning-rail" :open="!v2Mobile || v2AnswerCard"><summary data-v2-navigation @click.prevent="v2AnswerCard=!v2AnswerCard"><span>答题卡</span><strong>{{index+1}} / {{paper.questions.length}}</strong><ChevronDown :size="16" /></summary>${old.slice(old.indexOf('>')+1,old.lastIndexOf('</aside>')).replaceAll('@click="selectQuestion(Number(n))"','@click="v2Question(Number(n))"')}</details>`);
    code = replaceNode(code, has('answer-tools'), `<div class="answer-tools actions" data-v2-navigation><NButton v-if="canAnswer" secondary :disabled="busy" @click="reveal('answer')"><Eye :size="16" />答案与解析</NButton><NDropdown v-if="canAnswer" :options="v2AnswerTools" @select="v2AnswerTool"><NButton quaternary :disabled="busy">更多<ChevronDown :size="15" /></NButton></NDropdown></div>`);
  code = replaceNode(code, has('notes'), old => old.replace('<details ', '<details :open="v2NotesOpen" '));
  code = replaceNode(code, node=>node.tag==='Teleport' && node.loc.source.includes('class="question-footer learning-fixed-bar"'), `<Teleport to="body"><footer class="question-footer v2-learning-answerbar" data-v2-navigation aria-label="答题操作">
    <NButton secondary :disabled="busy || loading || index===0" aria-label="上一题" @click="v2Question(index-1)"><ChevronLeft :size="17" /><span class="v2-learning-prev-text">上一题</span></NButton>
    <NButton v-if="canAnswer" type="primary" :loading="busy" :disabled="busy || loading || locked" @click="submitAnswer">{{current.invalid?'题目已失效':locked?'已确认作答':practice?'提交本次复习':'确认作答'}}</NButton><span v-else>{{index+1}} / {{paper.questions.length}}</span>
    <NButton secondary :disabled="busy || loading || index===paper.questions.length-1" @click="v2Question(index+1)">下一题<ChevronRight :size="17" /></NButton>
  </footer></Teleport>`);
  code = replaceNode(code, has('profile-toolbar'), `<form class="toolbar profile-toolbar v2-learning-profile-filter" @submit.prevent="loadView" data-v2-navigation>
    <NRadioGroup :value="filters.period" size="small" aria-label="统计周期" :disabled="busy || loading" @update:value="v2Period"><NRadioButton value="7">近7天</NRadioButton><NRadioButton value="30">近30天</NRadioButton><NRadioButton value="">自定义</NRadioButton></NRadioGroup>
    <template v-if="!filters.period"><div v-if="v2Mobile" class="v2-learning-range-fields"><LearningDateField v-model="filters.from" type="date" :disabled="busy || loading" aria-label="统计开始日期" placeholder="开始日期" /><LearningDateField v-model="filters.to" type="date" :disabled="busy || loading" aria-label="统计结束日期" placeholder="结束日期" /></div><NDatePicker v-else type="daterange" value-format="yyyy-MM-dd" :formatted-value="filters.from && filters.to ? [filters.from,filters.to] : null" :disabled="busy || loading" clearable aria-label="统计日期区间" start-placeholder="开始日期" end-placeholder="结束日期" @update:formatted-value="v2Range" /><NButton attr-type="submit" :disabled="busy || loading || !filters.from || !filters.to">查询</NButton></template>
    <NButton v-if="isAdmin" secondary :disabled="busy || loading" @click="exportData('results')"><Download :size="16" />导出报表</NButton>
  </form>`);
  code = replaceNode(code, node=>node.tag==='LearningDashboard', old => old.replace('@select="selectLearner"','@select="selectLearner" @drill="v2Drill" :loading="loading" :failed="!!error"').replace('@continue="continueLearning"', '@continue="v2Resume(\'daily\')"'));
  code = replaceNode(code, has('list-pagination'), `<footer class="pagination list-pagination" data-v2-navigation><span>共 {{total}} 条</span><NPagination :page="pageNumber" :item-count="total" :page-size="20" :page-slot="v2Mobile?3:7" :show-quick-jumper="!v2Mobile" :disabled="busy || loading" @update:page="gotoPage" /></footer>`);
  code = replaceNode(code, node=>has('table-wrap')(node)&&node.loc.source.includes('<table v-if="tab === \'history\'">'),old=>old.replace('class="table-wrap"','class="table-wrap v2-learning-old-table"')+mobileRows);
  code = code.replace('import VnetSelect from "./VnetSelect.vue";', 'import VnetSelect from "../workspace-v2/LearningSelect.vue";');
  code = code.replace('import LearningDashboard from "./LearningDashboard.vue";', 'import LearningDashboard from "../workspace-v2/LearningPortrait.vue";');
  code = code.replace('import ConfirmDialog from "./ConfirmDialog.vue";', 'import ConfirmDialog from "../workspace-v2/LearningConfirmation.vue";');
  code = code.replace('paper.value = p; tab.value = "today"; reading.value = true; index.value = 0; hydrateQuestion();','paper.value = p; tab.value = "today"; reading.value = true; index.value = Math.max(0,(p?.questions || []).findIndex((q:Dict)=>!q.invalid && (!q.attempt || q.needs_review))); hydrateQuestion();');
  code = code.replace('scope: scope.value, person_id: person.id', 'scope: overviewScopes.value.some(s=>s.value===person.scope) ? person.scope : scope.value, person_id: person.id');
  code = code.replace('{ id: "history", label: "学习历史", icon: History }, { id: "issues"', '...(learner.value ? [{id:"review",label:"错题记录",icon:Star}] : []), { id: "history", label: "学习历史", icon: History }, { id: "issues"');
  const imports = `import {NButton,NDatePicker,NDropdown,NEmpty,NPagination,NRadioGroup,NRadioButton,NSelect,NTag} from 'naive-ui';\nimport {ChevronDown} from 'lucide-vue-next';\nimport LearningDateField from '../workspace-v2/LearningDateField.vue';\nimport '../workspace-v2/learning.css';\n`;
  code = code.replace('<script setup lang="ts">','<script setup lang="ts">\n'+imports);
  const setup = parse(code).descriptor.scriptSetup;
  if(!setup)throw new Error('Learning V2 requires the existing setup controller');
  code = code.slice(0,setup.loc.end.offset)+learningExtras+'\n'+code.slice(setup.loc.end.offset);
  return modernizeControls(code);
}

export function modernizeControls(source) {
  const { descriptor }=parse(source), edits=[];
  function visit(node,inForm=false){
    if(node.type===1){
      if(node.tag==='button'){
        const attributesEnd=Math.max(node.tag.length+1,...node.props.map(prop=>prop.loc.end.offset-node.loc.start.offset));
        const openingEnd=node.loc.source.indexOf('>',attributesEnd)+1;
        let opening=node.loc.source.slice(0,openingEnd).replace(/^<button/,'<NButton');
        const type=node.props.find(p=>p.type===6&&p.name==='type');
        if(type)opening=opening.replace(type.loc.source,'attr-type="'+type.value.content+'"');
        else if(inForm)opening=opening.replace('<NButton','<NButton attr-type="submit"');
        if(classes(node).includes('primary'))opening=opening.replace('<NButton','<NButton type="primary"');
        else if(classes(node).includes('icon-button'))opening=opening.replace('<NButton','<NButton quaternary circle');
        else opening=opening.replace('<NButton','<NButton secondary');
        const close=node.loc.end.offset-'</button>'.length;
        edits.push({start:node.loc.start.offset,end:node.loc.start.offset+openingEnd,text:opening},{start:close,end:node.loc.end.offset,text:'</NButton>'});
      }
      if(node.tag==='input'){
        const type=node.props.find(p=>p.type===6&&p.name==='type')?.value?.content;
        if(['date','time'].includes(type))edits.push({start:node.loc.start.offset,end:node.loc.end.offset,text:node.loc.source.replace(/^<input/,'<LearningDateField')});
      }
    }
    for(const child of node.children||[])visit(child,inForm||node.tag==='form');
  }
  visit(descriptor.template?.ast||{});
  for(const edit of edits.sort((a,b)=>b.start-a.start))source=source.slice(0,edit.start)+edit.text+source.slice(edit.end);
  return source;
}

const learningExtras = `
const v2Mobile=ref(matchMedia('(max-width:760px)').matches),v2AnswerCard=ref(false),v2NotesOpen=ref(false),v2CompletionHidden=ref(false);
const v2AnswerScene=computed(()=>ready.value && viewMode.value==='practice' && (tab.value==='today' || reading.value && ['review','history'].includes(tab.value)));
const v2Media=matchMedia('(max-width:760px)');
const v2MediaChange=()=>v2Mobile.value=v2Media.matches;
onMounted(()=>v2Media.addEventListener('change',v2MediaChange));
onBeforeUnmount(()=>v2Media.removeEventListener('change',v2MediaChange));
watch(()=>paper.value?.id,()=>{v2CompletionHidden.value=false;v2AnswerCard.value=false;});
const v2PrimaryTabs=computed(()=>['home','today','review','profile','history'].flatMap(id=>tabs.value.filter(item=>item.id===id).map(item=>({...item,label:id==='today'?'答题':id==='review'?'错题':id==='history'?'历史':item.label}))));
const v2AuxOptions=computed(()=>tabs.value.filter(item=>!['home','today','review','profile','history'].includes(item.id)).map(item=>({key:item.id,label:item.label,disabled:busy.value})));
const v2AuxTitle=computed(()=>v2AuxOptions.value.find(item=>item.key===tab.value)?.label|| (isAdmin.value?'学练管理':'更多'));
function v2OpenSection(key:string){if(tabs.value.some(item=>item.id===key))void switchTab(key as Tab);}
function v2Building(value:string){const row=overviewScopes.value.find(item=>item.value===value);if(row)void changeScope(row.label);}
const v2Completed=computed(()=>!!paper.value && validCount.value>0 && answered.value>=validCount.value);
const v2PaperStats=computed(()=>{const qs=(paper.value?.questions||[]).filter((q:Dict)=>!q.invalid);const choices=qs.filter((q:Dict)=>q.type!=='interview'&&q.attempt);return {choices:choices.length,correct:choices.filter((q:Dict)=>q.attempt.correct===true).length,interviews:qs.filter((q:Dict)=>q.type==='interview'&&q.attempt).length};});
async function v2Resume(mode:'daily'|'practice'){
  if(busy.value||loading.value||viewMode.value!=='practice'||!boot.value.can_answer||learner.value?.id!==boot.value.self_person?.id)return;
  if(studyMode.value!==mode)await switchStudyMode(mode);else if(!paper.value)await loadTodayPaper();
  if(!paper.value)return;
  tab.value='today';reading.value=true;
  const next=paper.value.questions.findIndex((q:Dict)=>!q.invalid&&(!q.attempt||q.needs_review));
  v2Question(next<0?0:next);
}
async function v2NewPractice(){
  if(busy.value||loading.value)return;
  if(studyMode.value==='practice'&&paper.value&&!v2Completed.value&&!await ask('开始新练习','当前练习尚未完成，将保留在学习历史中。是否生成新的15题？'))return;
  await startSelfPractice();
}
function v2Question(position:number){
  if(busy.value||loading.value)return;
  selectQuestion(position);v2AnswerCard.value=false;v2NotesOpen.value=false;
  if(v2Mobile.value)void nextTick(()=>document.querySelector('.learning-page .stem')?.scrollIntoView({block:'start',behavior:matchMedia('(prefers-reduced-motion:reduce)').matches?'auto':'smooth'}));
}
const v2AnswerTools=computed(()=>[{key:'hint',label:'思路提示',disabled:busy.value||current.value?.has_hint===false},{key:'notes',label:'学习笔记',disabled:busy.value},{key:'issue',label:'题目有疑问',disabled:busy.value}]);
function v2AnswerTool(key:string){if(busy.value)return;if(key==='hint'&&current.value?.has_hint!==false)void reveal('hint');else if(key==='issue')void openIssue();else if(key==='notes'){v2NotesOpen.value=true;void nextTick(()=>document.querySelector('.learning-page .notes')?.scrollIntoView({block:'nearest',behavior:'smooth'}));}}
function v2Period(value:string){setPeriod(value);if(value)void loadView();}
function v2Range(value:[string,string]|null){filters.from=value?.[0]||'';filters.to=value?.[1]||'';}
async function v2Drill(value:Dict){
  if(busy.value||loading.value)return;
  if(value.date){Object.assign(filters,{from:value.date,to:value.date,period:''});if(viewMode.value==='overview')await loadView();else await switchTab('history');return;}
  if(value.kind==='wrong'&&learner.value){
    filters.review='mistakes';filters.reviewBank=String(value.bank||'');filters.search=String(value.search||'');
    if(tab.value==='review')await loadView();else await switchTab('review');
  }else if(value.kind==='history')await switchTab('history');
}
`;

const mobileRows=`<div v-if="!loading && ['history','review'].includes(tab)" class="v2-learning-mobile-records" role="list">
  <article v-for="row in rows" :key="tab==='history'?row.id:row.paper_id+':'+questionId(row.question)" role="listitem">
    <template v-if="tab==='history'"><header><strong>{{row.date}}</strong><span>{{row.mode==='practice'?'自主练习':'每日题单'}}</span><NTag size="small" :type="row.status==='completed'?'success':'default'">{{row.status==='completed'?'已完成':'待完成'}}</NTag></header><p>{{row.person?.name||'旧楼栋题单'}} · {{row.scope}}楼</p><div class="v2-learning-person-values"><span>已答 <b>{{row.stats?.answered ?? row.answered ?? '—'}} / {{row.stats?.total ?? row.questions?.length ?? row.assigned ?? '—'}}</b></span><span>正确率 <b>{{percent(row.stats?.accuracy)}}</b></span></div><footer data-v2-navigation><NButton secondary :disabled="loading || busy" @click="loadPaper(row.id)">查看题单<ChevronRight :size="14" /></NButton><NButton v-if="isAdmin" text type="error" :disabled="loading || busy" @click="deletePaper(row.id)">删除题单</NButton></footer></template>
    <template v-else><header><NTag size="small">{{typeLabels[row.question.type]}}</NTag><span>{{row.last_wrong_at?timeLabel(row.last_wrong_at):row.date}}</span></header><h3>{{row.question.stem}}</h3><p>{{row.question.topic || bankLabels[row.question.bank]}}</p><footer data-v2-navigation><span v-if="row.mistake_count" class="danger">累计答错 {{row.mistake_count}} 次</span><span v-else>{{resultText(row.question)}}</span><NButton type="primary" secondary :disabled="loading || busy" @click="loadPaper(row.paper_id,questionId(row.question))">{{viewMode==='practice'?'复习':'查看错题'}}<ChevronRight :size="14" /></NButton></footer></template>
  </article><NEmpty v-if="!rows.length" description="暂无匹配的学习记录" />
</div>`;
