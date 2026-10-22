import { parse } from '@vue/compiler-sfc';
import ts from 'typescript';

function changeOnce(source, before, after) {
  if (source.split(before).length !== 2) throw new Error(`Repair V2 contract changed: ${before.slice(0, 90)}`);
  return source.replace(before, after);
}

function editFunction(source, name, edit) {
  const tree = ts.createSourceFile('repair.ts', source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TS);
  const node = tree.statements.find(item => ts.isFunctionDeclaration(item) && item.name?.text === name);
  if (!node?.body) throw new Error(`Repair V2 function missing: ${name}`);
  return source.slice(0, node.body.getStart(tree)) + edit(node.body.getText(tree)) + source.slice(node.body.end);
}
const prepend = (body, text) => '{\n' + text + body.slice(1);

const managementExtras = `
const v2Dialog = useDialog(), v2Message = useMessage();
const v2FollowupBusy = ref(false), v2RecordsReady = ref(false), v2RecordError = ref('');
const v2FollowupApi = ref<any>(null);
const v2AppliedState = ref('未完成'), v2Stats = ref<LooseDict | null>(null), v2StatsError = ref('');
const v2StatsLoading = ref(false), v2StatsAt = ref('');
let v2StatsAbort: AbortController | undefined;
const v2Writing = computed(() => saving.value || v2FollowupBusy.value || v2FollowupApi.value?.action.busy || reconcilingRemote.value || integrityRepairSubmitting.value);
const v2StateLabel = computed(() => recordStateOptions.find(item => item.value === recordState.value)?.label || '未完成');
function v2ProgressKnown(record: LooseDict): boolean {
  return record.progress_percent !== null && record.progress_percent !== undefined && record.progress_percent !== '' && Number.isFinite(Number(record.progress_percent));
}
function v2SpecialtyLabel(record: LooseDict): string {
  return typeof record.specialty === 'string' && record.specialty.trim() ? record.specialty.trim() : recordSpecialtyLabel(record);
}
function v2Count(key: string): number | null {
  const value = v2Stats.value?.[key];
  return value === null || value === undefined || value === '' || !Number.isFinite(Number(value)) ? null : Math.max(0, Number(value));
}
async function v2LoadStats(): Promise<void> {
  v2StatsAbort?.abort(); const controller = new AbortController(); v2StatsAbort = controller;
  v2StatsLoading.value = true; v2StatsError.value = '';
  try {
    const data = await requestJson('/api/repair-management/status?' + new URLSearchParams({scope:props.scope || 'ALL',state:'all',period:'all',limit:'1',offset:'0'}), {signal:controller.signal});
    if (controller.signal.aborted) return;
    v2Stats.value = data.stats && typeof data.stats === 'object' ? data.stats : null;
    v2StatsAt.value = new Intl.DateTimeFormat('zh-CN',{timeZone:'Asia/Shanghai',hour:'2-digit',minute:'2-digit'}).format(new Date());
  } catch(error) {
    if (!controller.signal.aborted) v2StatsError.value = error instanceof Error ? error.message : '状态数量暂未读取';
  } finally { if (v2StatsAbort === controller) v2StatsLoading.value = false; }
}
async function v2RefreshView(): Promise<void> {
  if (v2Writing.value || loading.value) return;
  clearRecordResponseCache(); clearRecordDetailCache();
  await Promise.all([loadRecords(true), v2LoadStats()]);
}
function v2SetDetail(element: unknown): void { projectDrawerRef.value = element as HTMLElement | null; }
function v2Discard(): void { hasUnsavedChanges.value = false; followupHasUnsavedChanges.value = false; }
const v2StopGuard = registerNavigationGuard((_target, proceed) => {
  if (v2Writing.value) { v2Message.warning('正在保存，请等待结果。'); return false; }
  if (!hasAnyUnsavedChanges.value) return true;
  v2Dialog.warning({title:'填写尚未保存',content:'离开将放弃当前维修单或跟进的修改。',positiveText:'放弃并离开',negativeText:'继续填写',onPositiveClick:()=>{v2Discard();proceed();}});
  return false;
});
function v2BeforeUnload(event: BeforeUnloadEvent): void {
  if (hasAnyUnsavedChanges.value || v2Writing.value) {event.preventDefault();event.returnValue='';}
}
watch(v2FollowupBusy, value => { if(!value && serverUpdatePending.value && !hasAnyUnsavedChanges.value)scheduleRepairStreamRefresh(); });
onMounted(()=>{void v2LoadStats();window.addEventListener('beforeunload',v2BeforeUnload);});
onBeforeUnmount(()=>{v2StopGuard();v2StatsAbort?.abort();window.removeEventListener('beforeunload',v2BeforeUnload);});
`;

const followupExtras = `
const v2HistoryOpen = ref(true), v2EditingHistory = ref(false), v2EquipmentOpen = ref(false);
const v2ReadError = ref(''), v2Ready = ref(false);
const v2Locked = computed(() => props.readOnly || Boolean(editingRecordId.value) && !v2EditingHistory.value);
const v2Submission = useRepairSubmission(() => 'followup:' + props.userId + ':' + props.scope + ':' + props.summaryRecordId, result => {
  editingRecordId.value = String(result.record_id || ''); createOperationId.value = '';
  applySavedFollowupRecord(editingRecordId.value, result.fields || {}, String(result.record_version || ''));
  followupFocusRecordId.value = editingRecordId.value;refreshFollowupsAfterSave();emit('changed');
  showMessage('跟进已保存，维修项目汇总按原流程同步。','success');
});
watch([saving,binding,v2Submission.checking], values => emit('busy', values.some(Boolean)), {immediate:true});
watch(editingRecordId, ()=>v2EditingHistory.value=false);
function v2EditHistory(): void { if(!props.readOnly&&!saving.value)v2EditingHistory.value=true; }
function v2ToggleSpare(value: boolean): void {
  if(v2Locked.value || saving.value)return;
  involvesSpareParts.value=value;
  if(!value)for(const name of SPARE_PART_FIELD_NAMES){if(Object.prototype.hasOwnProperty.call(draft,name)){draft[name]='';dirtyFieldNames.add(name);}}
  markDirty();
}
function v2RemoveLedger(id: string): void {
  if(v2Locked.value || saving.value)return;
  confirmLedger(ledgerDeviceIds.value.filter(value=>value!==id),ledgerDevices.value.filter(row=>row.record_id!==id));
}
function v2LedgerLabel(id: string): string {
  const row=ledgerDevices.value.find(item=>item.record_id===id);
  return row ? [row['设备编号'],row['设备名称'],row['机楼']].filter(Boolean).join(' · ') || '已关联设备' : '已关联设备（详情待读取）';
}
function v2SectionFields(group: {fields: LooseDict[]}): LooseDict[] {
  return group.fields.filter(field=>v2EquipmentOpen.value||editingRecordId.value||!['设备生产日期','设备使用年限','设备容量KW/AH','是否质保期内'].includes(String(field.field_name)));
}
`;

// Reuse the existing controllers in the V2 build, with independent V2 templates.
// The legacy sources and their build keep the original template and behavior.
export function adaptRepairController(source, kind, exposedNames) {
  const descriptor = parse(source).descriptor;
  if (!descriptor.scriptSetup) throw new Error('Repair controller requires script setup');
  let script = descriptor.scriptSetup.content.replaceAll('\r\n', '\n');
  script = `import { proxyRefs } from 'vue';\n` + script;
  script = changeOnce(script, '  scope: string;', '  scope: string;\n  userId?: string;');
  if (kind === 'management') {
    script = changeOnce(script, 'type WorkspaceTab = "project" | "followups";', 'type WorkspaceTab = "project" | "followups" | "flow" | "complete";');
    script = `import { useDialog, useMessage } from 'naive-ui';\nimport { registerNavigationGuard } from '../workspace-v2/navigation';\n` + script;
    script = changeOnce(script, 'const recordState = ref<RecordStateFilter>("all");', 'const recordState = ref<RecordStateFilter>(props.focusRecordId ? "all" : "active");');
    if(script.split('loadRecordDetailAndOpen(focused);').length!==4)throw new Error('Repair focus entry contract changed');
    script = script.replaceAll('loadRecordDetailAndOpen(focused);','loadRecordDetailAndOpen(focused, new URLSearchParams(window.location.search).get("workspace_tab") === "followups" ? "followups" : new URLSearchParams(window.location.search).get("workspace_tab") === "project" ? "project" : "flow");');
    script = changeOnce(script, '`project:${props.scope}:${editingRecordId.value ? \'direct-update\' : \'new\'}`', '`project:${props.userId}:${props.scope}:${editingRecordId.value ? \'direct-update\' : \'new\'}`');
    script = changeOnce(script, '}, () => `project:${props.scope}`);', '});');
    script = changeOnce(script,'    && hasWritableDraft.value','    && hasWritableDraft.value\n    && !projectConflict.value\n    && (!submission.pending.value || submission.status.value === "failed")');
    script = editFunction(script,'reloadLatestProject',body=>{
      let value=changeOnce(body,'  projectConflict.value = null;\n  hasUnsavedChanges.value = false;\n  dirtyFieldNames.clear();','  const previousRecord=selectedRecord.value;\n  const expectedRequest=recordDetailRequestVersion+1;');
      return changeOnce(value,'  if (editingRecordId.value === recordId && projectDrawerOpen.value) {',`  if(!repairPageActive)return;
  if(!projectDrawerOpen.value){
    if(recordDetailRequestVersion!==expectedRequest)return;
    selectedRecord.value=previousRecord;projectDrawerOpen.value=true;return;
  }
  if (editingRecordId.value === recordId && projectDrawerOpen.value) {`);
    });
    script = editFunction(script, 'closeProjectDrawerNow', body => prepend(body, `  if(v2Writing.value)return;\n  recordDetailRequestVersion++;recordDetailAbortController?.abort();recordDetailAbortController=null;recordDetailLoading.value=false;`));
    script = editFunction(script, 'requestCloseProjectDrawer', body => prepend(body, `  if(v2Writing.value){v2Message.warning('正在保存，请等待结果。');return;}`));
    script = editFunction(script, 'requestSelectRecord', body => prepend(body, `  if(v2Writing.value||loading.value)return;`));
    script = editFunction(script, 'requestDeleteRecord', body => prepend(body, `  if(!props.canDelete)return;`));
    script = editFunction(script, 'deleteRecordNow', body => prepend(body, `  if(!props.canDelete)return;`));
    script = editFunction(script, 'handleProjectDrawerKeydown', () => `{\n  if(event.key==='Escape' && !event.defaultPrevented){event.preventDefault();requestCloseProjectDrawer();}\n}`);
    const watcher = `watch(projectDrawerOpen, (open) => {`;
    const start = script.indexOf(watcher), end = script.indexOf('\n});', start);
    if(start<0||end<start)throw new Error('Repair detail lifecycle changed');
    script = script.slice(0,start) + `watch(projectDrawerOpen, open => {\n  if(open){projectDrawerReturnFocus=document.activeElement instanceof HTMLElement?document.activeElement:null;focusProjectDrawer();}\n  else clearSyncStatusTimer();\n});` + script.slice(end+'\n});'.length);
    script = editFunction(script, 'loadRecords', body => {
      let value=changeOnce(body,'  loading.value = true;',"  loading.value = true;v2RecordError.value='';");
      value=changeOnce(value,'    records.value = incomingRecords.filter(item => !deletedProjectIds.has(String(item.record_id || "")));','    records.value = incomingRecords.filter(item => !deletedProjectIds.has(String(item.record_id || "")));\n    v2RecordsReady.value=true;v2AppliedState.value=v2StateLabel.value;');
      return changeOnce(value,'    if (!silent) {',"    v2RecordError.value=error instanceof Error?error.message:'维修项目读取失败';\n    if (!silent) {");
    });
    script = editFunction(script,'handleFollowupChanged',body=>prepend(body,'  void v2LoadStats();'));
    script = editFunction(script,'saveRecord',body=>{
      const value=changeOnce(body,'      fields: writablePayload(),','      fields: writablePayload(),\n      expected_version: editingRecordId.value ? String(selectedRecord.value?.record_version || "") : "",');
      return changeOnce(value,'    return true;\n  } catch','    void v2LoadStats();\n    return true;\n  } catch');
    });
    script = editFunction(script,'deleteRecordNow',body=>changeOnce(body,'    invalidateRepairStatus();','    invalidateRepairStatus();void v2LoadStats();'));
    script = changeOnce(script,'watch(integrityRepairStatus,',managementExtras+'\nwatch(integrityRepairStatus,');
  } else if (kind === 'followup') {
    script = `import { useRepairSubmission } from '../composables/useRepairSubmission';\n` + script;
    script = changeOnce(script,'  "count-changed": [value: number];','  "count-changed": [value: number];\n  busy: [value: boolean];');
    script = changeOnce(script,`const DEFAULT_FOLLOWUP_FIELD_VALUES: Record<string, string> = {
  [DEVICE_PRODUCTION_DATE_FIELD_NAME]: "2021-03-31T00:00",
  [DEVICE_USAGE_YEARS_FIELD_NAME]: "4",
  [DEVICE_CAPACITY_FIELD_NAME]: "/",
};`,`const DEFAULT_FOLLOWUP_FIELD_VALUES: Record<string, string> = {};`);
    script = editFunction(script,'openFollowupBindPicker',body=>prepend(body,'  if(props.readOnly||saving.value)return;'));
    script = editFunction(script,'confirmFollowupBinding',body=>prepend(body,'  if(props.readOnly||saving.value)return;'));
    script = editFunction(script,'reloadLatestFollowup',body=>{
      let value=changeOnce(body,'  conflictState.value = null;','');
      value=changeOnce(value,'  setDirty(false);','');
      return changeOnce(value,'  await loadRecords(true, true);','  await loadRecords(true, true);\n  if(v2ReadError.value)return;');
    });
    script = editFunction(script,'loadRecords',body=>{
      let value=changeOnce(body,'  loading.value = true;',"  loading.value = true;v2ReadError.value='';");
      value=changeOnce(value,'    records.value = Array.isArray(payload.records) ? payload.records : [];','    records.value = Array.isArray(payload.records) ? payload.records : [];v2Ready.value=true;');
      value=changeOnce(value,`    } else if (!creatingNewFollowup.value && records.value.length) {
      selectRecord(records.value[0]);`, `    } else if (!creatingNewFollowup.value && records.value.length) {
      if(props.readOnly)selectRecord(records.value[0]);else startCreate();`);
      return changeOnce(value,'    if (!silent) {',"    v2ReadError.value=error instanceof Error?error.message:'跟进记录读取失败';\n    if (!silent) {");
    });
    script = editFunction(script,'saveRecord',body=>{
      const value=changeOnce(body,'      fields: buildFields(),','      fields: buildFields(),\n      expected_version: editingRecordId.value ? String(selectedRecord.value?.record_version || "") : "",');
      return changeOnce(value,'await requestJson("/api/repair-management/followups", { method: "POST", body })','await v2Submission.submit("/api/repair-management/followups", { method: "POST", body })');
    });
    script = changeOnce(script,'const editableFields = computed(',followupExtras+'\nconst editableFields = computed(');
  } else throw new Error(`Unknown repair controller: ${kind}`);
  const tree=ts.createSourceFile('repair.ts',script,ts.ScriptTarget.Latest,true,ts.ScriptKind.TS);
  const bindings=new Set(tree.statements.flatMap(node=>ts.isFunctionDeclaration(node)?[node.name?.text]:ts.isVariableStatement(node)?node.declarationList.declarations.filter(d=>ts.isIdentifier(d.name)).map(d=>d.name.text):ts.isImportDeclaration(node)&&node.importClause?.namedBindings&&ts.isNamedImports(node.importClause.namedBindings)?node.importClause.namedBindings.elements.filter(item=>!item.isTypeOnly).map(item=>item.name.text):[]));
  for(const name of exposedNames)if(!bindings.has(name))throw new Error(`Repair V2 binding changed: ${name}`);
  script += `\nconst v2Controller=proxyRefs({${exposedNames.join(',')}});\ndefineExpose({v2Controller});\n`;
  return `<template><slot :controller="v2Controller" /></template>\n<script setup lang="ts">\n${script}\n</script>`;
}

export function controllerBindings(template) {
  return [...new Set([...template.matchAll(/\br\.([A-Za-z_$][\w$]*)/g)].map(match=>match[1]))].sort();
}
