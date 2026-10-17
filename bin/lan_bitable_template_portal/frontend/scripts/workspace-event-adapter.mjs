import { parse } from '@vue/compiler-sfc';
import ts from 'typescript';

// V2-only build adaptation preserves the original drawer and all business actions.
export function adaptEventPeriodPage(source) {
  let code = source.replaceAll('\r\n', '\n');
  function replace(before, after) {
    if (code.split(before).length !== 2) throw new Error(`Event V2 contract changed: ${before.slice(0, 90)}`);
    code = code.replace(before, after);
  }
  replace('import EventStatsCards from "./EventStatsCards.vue";', 'import EventStatsCards from "../workspace-v2/EventMetrics.vue";\nimport EventPeriodFilter from "../workspace-v2/EventPeriodFilter.vue";\nimport { NButton, NSkeleton } from "naive-ui";\nimport { beijingDate, occurrenceTimestamp, eventPeriodRange, mergeEventMonths, eventPeriodStats, type EventPeriod } from "../workspace-v2/eventPeriod";');
  replace('const selectedMonth = ref(currentLocalMonth());', `const eventQuery = new URLSearchParams(window.location.search);
const eventPeriod = ref<EventPeriod>(['week','history','custom'].includes(eventQuery.get('period') || '') ? eventQuery.get('period') as EventPeriod : 'month');
const selectedMonth = ref(/^\\d{4}-(0[1-9]|1[0-2])$/.test(eventQuery.get('event_month') || '') ? eventQuery.get('event_month')! : beijingDate().slice(0,7));
const eventNow = ref(Date.now());
const eventDates = ref<[string,string]>([eventQuery.get('event_start') || beijingDate().slice(0,7)+'-01', eventQuery.get('event_end') || beijingDate()]);
const eventRange = computed(() => eventPeriodRange(eventPeriod.value, selectedMonth.value, eventNow.value, eventDates.value));
const chartDate = ref<{start:number;end:number;label:string}|null>(null);
const stateFilter = ref(eventQuery.get('event_state')==='processing'?'processing':'');
const eventDetailsOpen = ref(Boolean(eventQuery.get('detail')==='1' || eventQuery.get('event_state') || eventQuery.get('record_id')));
function showEventRows(){eventDetailsOpen.value=true;requestAnimationFrame(()=>document.getElementById('v2-event-list')?.scrollIntoView({behavior:window.matchMedia('(prefers-reduced-motion: reduce)').matches?'auto':'smooth',block:'start'}));}
function drillEventDate(start:number,end:number,label:string){clearFilters();chartDate.value={start,end,label};showEventRows();}
function drillEventLevel(value:string){clearFilters();levelFilter.value=value;showEventRows();}`);
  replace(`<label class="month-picker">
            <span>月份</span>
            <input v-model="selectedMonth" type="month" :disabled="loading || refreshing" />
          </label>`, '');
  replace('      <MessageBanner v-if="errorText"', '      <EventPeriodFilter v-model="eventPeriod" v-model:month="selectedMonth" v-model:dates="eventDates" :range="eventRange" :disabled="refreshing || eventTransferBusy" />\n      <MessageBanner v-if="errorText"');
  replace('<EventStatsCards :cards="statCards"', '<EventStatsCards :cards="statCards" :loading="loading" :failed="Boolean(errorText) || configMissing"');
  replace('import EventPeriodFilter from "../workspace-v2/EventPeriodFilter.vue";', 'import EventPeriodFilter from "../workspace-v2/EventPeriodFilter.vue";\nimport EventCharts from "../workspace-v2/EventCharts.vue";');
  replace('import EventCharts from "../workspace-v2/EventCharts.vue";', 'import EventCharts from "../workspace-v2/EventCharts.vue";\nimport RepairFlowSteps from "../workspace-v2/RepairFlowSteps.vue";\nimport { canUseModule } from "../api/modulePermissions";');
  replace('<section class="event-repair-actions">', '<RepairFlowSteps context="event" :event="selectedEvent" :busy="eventTransferBusy" :actions="canUseModule(\'repair_management\') ? [\'project\'] : []" @select="openRepairManagementForSelectedEvent" />\n        <section class="event-repair-actions">');
  replace('function clearFilters(): void {', 'function clearFilters(): void {\n  chartDate.value = null; stateFilter.value = "";');
  replace('  return detailEvents.value.filter((item) => {', '  return detailEvents.value.filter((item) => {\n    if(stateFilter.value && statusTone(item.status)!==stateFilter.value)return false;\n    if (chartDate.value) { const time=occurrenceTimestamp(item.occurrence_time); if(time<chartDate.value.start || time>=chartDate.value.end)return false; }');
  replace('return [searchText.value, buildingFilter.value, statusFilter.value, levelFilter.value, sourceFilter.value, specialtyFilter.value]', 'return [stateFilter.value, chartDate.value?.label, searchText.value, buildingFilter.value, statusFilter.value, levelFilter.value, sourceFilter.value, specialtyFilter.value]');
  replace('{ label: "状态", value: statusFilter.value || "全部状态", active: Boolean(statusFilter.value) }', '{ label: "状态", value: statusFilter.value || (stateFilter.value ? "处理中" : "全部状态"), active: Boolean(statusFilter.value || stateFilter.value) }');
  replace('String(item.level || "") !== levelFilter.value', '(String(item.level || "").trim() || "未填写等级") !== levelFilter.value');
  replace('detailEvents.value.map((item) => item.level)', 'detailEvents.value.map((item) => String(item.level || "").trim() || "未填写等级")');
  replace('key: "total",\n      label:', 'key: "total",\n      clickable: true,\n      label:');
  replace('async function openMetricRecords(key: string): Promise<void> {', "async function openMetricRecords(key: string): Promise<void> {\n  if(key==='total'){clearFilters();showEventRows();return;}");
  replace('未计入所选月份。', '未计入当前时间范围。');
  replace('<MessageBanner v-if="unknownTimeCount"', '<MessageBanner data-v2-inline-feedback v-if="unknownTimeCount"');
  replace('<div class="event-work-surface" :class="{ detail: showingDetails }">', '<div v-show="!errorText && !configMissing" class="event-work-surface" :class="{ detail: showingDetails, \'is-period-loading\': loading }" :aria-hidden="loading">');
  replace('<div v-if="showingDetails" class="event-list-panel">', '<EventCharts :records="detailEvents" :range="eventRange" :loading="loading" :failed="Boolean(errorText) || configMissing" @date="drillEventDate" @level="drillEventLevel" />\n    <div class="v2-event-detail-toggle"><button type="button" class="btn quiet" :disabled="loading || Boolean(errorText) || configMissing" :aria-expanded="eventDetailsOpen" aria-controls="v2-event-list" @click="eventDetailsOpen ? eventDetailsOpen=false : showEventRows()">{{eventDetailsOpen ? "收起事件明细" : "展开事件明细"}}</button></div>\n    <div v-if="showingDetails && eventDetailsOpen" id="v2-event-list" class="event-list-panel" role="region" :aria-label="eventRange.label + \'事件列表\'" :aria-busy="loading">');
  replace('{{ detailScopeLabel }}月度事件列表', '{{ detailScopeLabel }}{{ eventRange.label }}事件');
  replace('{{ filteredEvents.length }} / {{ detailEvents.length }} 条', '{{ loading || errorText ? "待读取" : `${filteredEvents.length} / ${detailEvents.length} 条` }}');
  replace(`<div v-if="loading" class="event-empty">
        <strong><LoadingIndicator>正在读取事件数据</LoadingIndicator></strong>
      </div>`, `<div v-if="loading" class="v2-event-list-loading" role="status" aria-label="正在读取事件数据">
        <LoadingIndicator>正在读取{{ eventRange.label }}事件</LoadingIndicator>
        <div class="v2-event-skeletons"><NSkeleton v-for="index in 4" :key="index" height="68px" :sharp="false" /></div>
      </div>
      <div v-else-if="errorText || configMissing" class="event-empty"><strong>事件数据暂不可用，请重试</strong></div>`);
  replace('<strong>本月暂无事件</strong>', '<strong>{{ eventRange.label }}暂无事件</strong>');
  replace('<span class="section-kicker">本月事件</span>', '<span class="section-kicker">{{ eventRange.label }}事件</span>');
  replace('<strong>本月暂无对应记录</strong>', '<strong>{{ eventRange.label }}暂无对应记录</strong>');
  replace('if (!showingDetails.value || !detailScope.value) return events.value;', 'if (!showingDetails.value || !detailScope.value || ["ALL", "CAMPUS"].includes(detailScope.value)) return events.value;');
  replace('const currentStats = showingDetails.value ? stats.value : overviewStats.value;', 'const currentStats = eventPeriodStats(detailEvents.value);');
  replace('label: "本月新增事件"', 'label: `${eventRange.value.label}新增事件`');
  replace('badge: "月度总览"', 'badge: "按发生时间统计"');
  replace('label: "正在检修中"', 'label: "其中检修中"');
  replace('label: "本月I2级及以上"', 'label: `${eventRange.value.label}I2级及以上`');
  replace('label: "本月I3级事件"', 'label: `${eventRange.value.label}I3级事件`');
  replace('{ label: "月份", value: selectedMonth.value || "当前月", active: true }', '{ label: "时间", value: chartDate.value?.label || eventRange.value.caption, active: true }');
  replace('month: selectedMonth.value,\n        record_id: recordId', 'month: beijingDate(occurrenceTimestamp(selectedEvent.value?.occurrence_time)).slice(0,7),\n        record_id: recordId');
  replace('url.searchParams.get(\'month\') === selectedMonth.value', "eventRange.value.months.includes(url.searchParams.get('month') || '')");
  replace('() => [props.scope, selectedMonth.value]', '() => [props.scope, selectedMonth.value, eventPeriod.value, ...eventDates.value]');
  replace('    buildingFilter.value = "";\n    const normalizedScope', `    clearFilters();
    const periodUrl = new URL(window.location.href);
    periodUrl.searchParams.set('period', eventPeriod.value);
    periodUrl.searchParams.set('event_month', selectedMonth.value);
    if(eventPeriod.value==='custom'){periodUrl.searchParams.set('event_start',eventDates.value[0]);periodUrl.searchParams.set('event_end',eventDates.value[1]);}
    else{periodUrl.searchParams.delete('event_start');periodUrl.searchParams.delete('event_end');}
    window.history.replaceState({}, '', periodUrl.toString());
    const normalizedScope`);
  replace('if (detailRequested.value && BUILDING_SCOPE_CODES.includes(normalizedScope))', 'if (detailRequested.value && normalizedScope)');
  replace('    const params = new URLSearchParams({ scope: refreshScope, month: selectedMonth.value });', '    for (const month of eventRange.value.months) {\n    const params = new URLSearchParams({ scope: refreshScope, month });');
  replace('        month: selectedMonth.value,\n        signal: abortController.signal', '        month,\n        signal: abortController.signal');
  replace('    );\n    await loadEvents();', '    );\n    }\n    if (abortController.signal.aborted) return;\n    await loadEvents();');

  // Reuse the same authorized event records for building counts and drilldown.
  const surface = code.indexOf('<div v-show="!errorText && !configMissing" class="event-work-surface"');
  const surfaceEnd = code.indexOf('      </div>', surface) + '      </div>'.length;
  if(surface < 0 || surfaceEnd < surface)throw new Error('Event priority layout changed');
  code = code.slice(0,surface) + `<section class="v2-event-buildings" aria-label="楼栋事件条数" :aria-busy="loading">
        <NButton v-for="card in buildingCards.filter(item=>item.allowed)" :key="card.code" data-v2-navigation
          :disabled="loading || Boolean(errorText) || configMissing" :aria-label="'查看'+card.label+eventRange.label+'事件'"
          :aria-pressed="detailScope===card.code" @click="openBuilding(card.code);showEventRows()">
          <span>{{ card.label }}</span><NSkeleton v-if="loading" width="28px" height="20px" />
          <strong v-else>{{ errorText || configMissing ? '—' : card.total }}</strong><small>件</small>
        </NButton>
      </section>` + code.slice(surfaceEnd);
  const { descriptor } = parse(code);
  const block = descriptor.scriptSetup;
  const tree = ts.createSourceFile('Event.vue.ts', block.content, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const bodies = {
    loadEvents: `{
      if (!props.scope) return;
      const loadVersion = ++eventLoadVersion;
      eventLoadAbortController?.abort();
      const abortController = new AbortController();
      eventLoadAbortController = abortController;
      eventNow.value = Date.now();
      loading.value = true; errorText.value = ''; selectedEvent.value = null;
      closeMetricRecords(); metricRequestVersion += 1;
      try {
        await loadMonthlyEvents(detailScope.value || props.scope, abortController.signal, loadVersion);
        if (loadVersion !== eventLoadVersion || abortController.signal.aborted) return;
        emit('status', eventRange.value.label + '事件已更新');
      } catch (error: unknown) {
        if (abortController.signal.aborted || loadVersion !== eventLoadVersion) return;
        errorText.value = error instanceof Error ? error.message : '事件数据读取失败。';
        events.value = []; stats.value = {}; detailUnknownTimeCount.value = 0;
        emit('status', errorText.value);
      } finally {
        if (eventLoadAbortController === abortController) eventLoadAbortController = null;
        if (loadVersion === eventLoadVersion) loading.value = false;
      }
    }`,
    loadMonthlyEvents: `{
      const range = eventRange.value;
      if(range.error)throw new Error(range.error);
      const payloads: LooseDict[] = [];
      const readScopes = props.scopeOptions.some(item=>normalizeScope(item.value)==='ALL') ? ['ALL'] : [...allowedBuildingCodes.value];
      const queries = range.months.flatMap(month=>readScopes.map(scope=>({scope,month})));
      for(let offset=0;offset<queries.length;offset+=3){
        if(signal?.aborted || loadVersion!==eventLoadVersion)return;
        payloads.push(...await Promise.all(queries.slice(offset,offset+3).map(({scope,month})=>{
          const params = new URLSearchParams({ scope, month, date_field: 'occurrence_time' });
          return requestJson('/api/events/monthly?' + params.toString(), { signal });
        })));
      }
      if (loadVersion !== eventLoadVersion || signal?.aborted) return;
      const payload = mergeEventMonths(payloads, range);
      events.value = payload.records; stats.value = payload.stats;
      detailUnknownTimeCount.value = payload.unknown_time_count;
      lastRefreshedAt.value = payload.last_refreshed_at;
      lastFailed.value = payload.last_failed;
      configMissing.value = false; configError.value = '';
    }`,
  };
  const edits = [];
  for (const node of tree.statements) {
    if (ts.isFunctionDeclaration(node) && bodies[node.name?.text] && node.body) {
      edits.push([block.loc.start.offset + node.body.getStart(tree), block.loc.start.offset + node.body.end, bodies[node.name.text]]);
    }
  }
  if (edits.length !== 2) throw new Error('Event V2 read functions changed');
  for (const [start, end, body] of edits.sort((a, b) => b[0] - a[0])) code = code.slice(0, start) + body + code.slice(end);
  return code;
}
