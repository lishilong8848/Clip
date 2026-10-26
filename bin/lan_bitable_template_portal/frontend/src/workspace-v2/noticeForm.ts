import type { Dict } from './client';
import { isMaintenanceNotice, maintenanceCycleRules, noticeCycleWarnings, type MaintenanceCycleRule } from '../noticeCycleWarnings';

export type NoticeField = { key:string; label:string; group:string; type:string; required:boolean; readonly:boolean; options:{label:string;value:string}[]; hint:string; min?:number; max?:number; step?:number };
export type NoticeForm = { values:Dict; cycleRules?:MaintenanceCycleRule[]; contextVersion?:string; fields:NoticeField[]; actions:string[]; initialAction:string; detailMode:string; canDelete:boolean; photoRequired:boolean; photos:Dict[]; aliPhotos:Dict[]; sop:boolean; groupId:string; sopLinks:{label:string;url:string}[]; sourceOptions:{label:string;value:string}[]; repairRelation?:{eventId:string;eventTitle:string;projectId:string;projectTitle:string} };
export const endProgress = '工作已完成，设备运行正常，请知晓！';
export function normalizePastedNotice(value:string):string {return value.replace(/\r\n?/g,'\n').split('\n').filter(line=>line.trim()).join('\n');}
const patchKeys = new Set(`manual manual_id scope source_month action work_type notice_type operation_id manual_binding_choice manual_binding_required active_item_id source_record_id target_record_id record_id repair_management_record_id source_work_type converted_from_work_type converted_to_work_type sync_maintenance_target paired_maintenance_target_record_id paired_maintenance_original_title paired_maintenance_actual_start_time building buildings building_code building_codes title name specialty maintenance_cycle execution_party level start_time end_time actual_action_time response_time location content reason impact progress repair_device repair_fault fault_type repair_mode discovery symptom solution spare_parts fault_time expected_time cabinet quantity device status site_photo_count site_photos extra_images ali_confirmation_images polling_work_order_exempt polling_sop_id polling_sop_version polling_run_count polling_runs polling_operator_record_id polling_operator_record_ids polling_reviewer_record_id mop_status zhihang_record_id lan_zhihang_record_id zhihang_involved`.split(' '));

// The existing read-only detail endpoint remains the source of field metadata.
// Parse inertly: no returned HTML, styles or scripts are mounted into the new page.
export function parseNoticeForm(html:string):NoticeForm {
  const doc=new DOMParser().parseFromString(html,'text/html'),form=doc.querySelector<HTMLFormElement>('#lite-notice-form');
  if(!form)throw new Error('未取得通告填写字段，请重新读取。');
  const values:Dict={},fields:NoticeField[]=[],seen=new Set<string>();
  for(const control of form.querySelectorAll<HTMLInputElement|HTMLSelectElement|HTMLTextAreaElement>('input[name],select[name],textarea[name]')){
    const key=control.name;if(!patchKeys.has(key))continue;
    if(control.type==='checkbox'){
      if(key==='building_codes'){
        values[key]??=[];
        if((control as HTMLInputElement).checked)values[key].push(control.value);
      }else values[key]=(control as HTMLInputElement).checked;
      continue;
    }
    values[key]=control.value;
    if(control.type==='hidden'||seen.has(key)||['source_record_id','polling_work_order_exempt'].includes(key))continue;
    seen.add(key);
    const label=control.closest('label');
    const caption=label?.querySelector('.field-label') || label?.querySelector('span');
    const directText=label?[...label.childNodes].filter(n=>n.nodeType===3).map(n=>n.textContent).join(''):'';
    const text=(caption?[...caption.childNodes].filter(n=>n.nodeType===3).map(n=>n.textContent).join(''):directText||key).replace(/\s*\*\s*$/,'').trim();
    const options=control.tagName==='SELECT'?[...(control as HTMLSelectElement).options].filter(o=>o.value).map(o=>({label:o.label,value:o.value})):[];
    const list=control.getAttribute('list');
    if(list)for(const option of doc.getElementById(list)?.querySelectorAll('option')||[])options.push({label:option.value,value:option.value});
    fields.push({key,label:text,group:control.closest('.form-section')?.querySelector('header strong')?.textContent||'发送信息',
      type:control.tagName==='TEXTAREA'?'textarea':control.type,required:control.required||control.dataset.requiredUpload==='1',
      readonly:control.hasAttribute('readonly')||control.disabled,options,hint:label?.querySelector('small')?.textContent||'',
      ...Object.fromEntries(['min','max','step'].flatMap(key=>{const value=control.getAttribute(key);return value!==null&&value!==''&&Number.isFinite(Number(value))?[[key,Number(value)]]:[];}))});
  }
  const json=(selector:string,attribute:string):Dict[]=>{try{const value=JSON.parse(form.querySelector(selector)?.getAttribute(attribute)||'[]');return Array.isArray(value)?value:[];}catch{return [];}};
  const source=form.querySelector<HTMLSelectElement>('select[name=source_record_id]');
  const eventId=form.querySelector<HTMLInputElement>('[data-repair-event-link] input[name=related_event_record_id]')?.value||'';
  const projectId=values.repair_management_record_id||values.source_record_id||'';
  values.manual_binding_required=['1','true'].includes(String(values.manual_binding_required||''));
  if(form.querySelector('[data-manual-source-binding]'))values.manual_binding_required=true;
  return {values,fields,cycleRules:isMaintenanceNotice(values)?maintenanceCycleRules(form.dataset.maintenanceCycleRules):[],contextVersion:form.dataset.draftContext||'',actions:[...form.querySelectorAll<HTMLButtonElement>('button[name=submit_action]')].filter(b=>!b.disabled).map(b=>b.value),
    ...(values.work_type==='repair'?{repairRelation:{eventId,eventTitle:eventId?form.querySelector('#lite-repair-event-link-status')?.textContent?.trim()||'':'',projectId,projectTitle:source?[...source.options].find(o=>o.value===projectId)?.label||'':''}}:{}),
    initialAction:form.dataset.action||'',detailMode:form.dataset.detailMode||'planned',
    canDelete:!!form.querySelector('[data-ongoing-delete-mode]'),photoRequired:!!form.querySelector('[data-site-photo-panel]'),photos:json('[data-site-photo-panel]','data-existing-images'),aliPhotos:json('[data-ali-confirmation-panel]','data-existing-images'),
    sop:!!form.querySelector('[data-polling-work-order-select]'),groupId:form.querySelector('[data-polling-group-id]')?.getAttribute('data-polling-group-id')||'',
    sopLinks:[...form.querySelectorAll('[data-polling-copy-link]')].flatMap(el=>{
      try{const url=new URL(el.getAttribute('data-polling-copy-link')||'',location.origin);return url.origin===location.origin&&url.pathname==='/polling-work-order'&&url.searchParams.has('token')?[{label:(el.textContent||'查看工单').replace(/^复制/,'打开'),url:url.pathname+url.search}]:[];}catch{return [];}
    }),
    sourceOptions:source?[...source.options].filter(o=>o.value).map(o=>({label:o.label,value:o.value})):[]};
}
export function noticeDraftContext(form:NoticeForm):string {
  return form.contextVersion||JSON.stringify(['scope','work_type','notice_type','source_record_id','target_record_id','active_item_id','source_month','title','maintenance_cycle','building_codes'].map(key=>form.values[key]??''));
}
export function restoreNoticeEdits(form:NoticeForm,cached:Dict,version:number):boolean {
  if(cached.schema!==2||cached.context!==noticeDraftContext(form)||cached.draftVersion!==version||!cached.baseValues||!cached.snapshot?.values)return false;
  // Restore edits, never the automatic defaults captured with an older memory.
  for(const field of form.fields){
    if(field.readonly||!Object.prototype.hasOwnProperty.call(cached.snapshot.values,field.key))continue;
    if(JSON.stringify(cached.snapshot.values[field.key])!==JSON.stringify(cached.baseValues[field.key]))form.values[field.key]=cached.snapshot.values[field.key];
  }
  return true;
}
// Display-only formats mirror the existing web preview; submission remains noticeCommand.
const previewKeys:Record<string,string[]>={
  maintenance:['title','__range','location','content','reason','impact','progress'],
  change:['title','level','__range','location','content','reason','impact','progress'],
  repair:['title','location','level','specialty','end_time','start_time','repair_device','repair_fault','fault_type','repair_mode','impact','discovery','symptom','reason','solution','spare_parts','progress'],
  power:['title','__range','cabinet','quantity','progress'],
  polling:['title','__range','device','content','impact','progress'],
  adjust:['title','__range','location','content','reason','impact','progress'],
};
export function noticePreview(form:NoticeForm,action:string):string {
  const v=form.values, labels:Record<string,string>={title:['polling','repair'].includes(v.work_type)?'标题':'名称',__range:'时间',location:v.work_type==='repair'?'地点':'位置',progress:v.work_type==='repair'?'完成情况':'进度',end_time:'发现故障时间',start_time:'期望完成时间',reason:v.work_type==='repair'?'故障原因（事件发生原因）':'原因',solution:'解决方案（事件解决措施）',discovery:'故障发现方式'};
  const value=(key:string)=>{const text=String(v[key]??'').trim();return ['start_time','end_time','actual_action_time','response_time'].includes(key)?text.replace('T',' '):text;};
  return [`【${v.notice_type}】状态：${{start:'开始',update:'更新',end:'结束'}[action]||''}`,...(previewKeys[v.work_type]||[]).map(key=>`【${labels[key]||form.fields.find(f=>f.key===key)?.label||key}】${key==='__range'?[value('start_time'),value('end_time')].filter(Boolean).join('~'):value(key)}`)].join('\n');
}
export function beijingMinute():string {return new Date(Date.now()+8*3600000).toISOString().slice(0,16).replace('T',' ');}
export function noticeCommand(form:NoticeForm,action:string,operationId:string,draftVersion:number,sop:Dict,photos:Dict[],aliPhotos:Dict[]):Dict {
  const raw={...form.values},values:Dict={};
  for(const key of patchKeys)if(raw[key]!==undefined&&raw[key]!==null&&raw[key]!=='')values[key]=raw[key];
  values.manual=['1','true','yes','on'].includes(String(raw.manual).toLowerCase());
  values.action=action;values.operation_id=operationId;
  values.building_codes=Array.isArray(raw.building_codes)?[...raw.building_codes]:[];
  values.building=values.building_codes.map((s:string)=>s==='110'?'110站':s+'楼').join('、');
  values.actual_action_time=raw.actual_action_time||beijingMinute();values.response_time=values.actual_action_time;
  values.record_id=action==='start'?(raw.source_record_id||raw.manual_id||raw.record_id||''):(raw.target_record_id||raw.record_id||'');
  if(raw.work_type==='repair'){values.repair_management_record_id=raw.repair_management_record_id||(String(raw.source_record_id||'').startsWith('rec')?raw.source_record_id:'');values.fault_time=raw.end_time;values.expected_time=raw.start_time;}
  if(form.sop&&action==='start'){
    values.polling_work_order_exempt=sop.exempt===true;
    if(!sop.exempt){values.polling_sop_id=sop.sop_id;values.polling_sop_version=sop.sop_version;values.polling_operator_record_ids=sop.operator_record_ids??(sop.operator_record_id?[sop.operator_record_id]:[]);values.polling_operator_record_id=values.polling_operator_record_ids[0]||'';values.polling_reviewer_record_id=sop.reviewer_record_id;values.polling_runs=sop.runs||[];values.polling_run_count=sop.runs?.length||1;}
  }
  if(photos.length){values.site_photos=photos;values.extra_images=photos;}
  if(aliPhotos.length&&raw.work_type==='change')values.ali_confirmation_images=aliPhotos;
  return {command_format:'notice_command',action,scope:raw.scope,work_type:raw.work_type,notice_type:raw.notice_type,
    active_item_id:raw.active_item_id||'',source_record_id:raw.source_record_id||'',target_record_id:raw.target_record_id||'',repair_management_record_id:values.repair_management_record_id||'',
    record_id:values.record_id,actual_action_time:values.actual_action_time,operation_id:operationId,draft_version:draftVersion,patch:values};
}
export function validateNotice(form:NoticeForm,action:string,sop:Dict,photos:Dict[]):string {
  if(!form.actions.includes(action))return '该通告当前不支持此操作，请刷新后核对。';
  const cycleIssues=Object.values(noticeCycleWarnings(form.values,form.cycleRules));
  if(cycleIssues.length)return cycleIssues.join('\n');
  const missing=form.fields.filter(f=>f.required&&!String(form.values[f.key]??'').trim());
  if(missing.length)return '请填写：'+missing.map(f=>f.label).join('、');
  if(!form.values.building_codes?.length)return '请选择通告所属楼栋。';
  if(form.values.manual_binding_required&&action==='start'){
    if(!['bind','unbound'].includes(form.values.manual_binding_choice))return '请确认关联计划，或明确选择独立通告。';
    if(form.values.manual_binding_choice==='bind'&&!form.values.source_record_id)return '请选择并确认关联计划。';
  }
  const values=form.values;
  const time=(value:string)=>Date.parse(String(value||'').replace(' ','T')+'+08:00');
  const start=time(values.work_type==='repair'?values.end_time:values.start_time);
  const end=time(values.work_type==='repair'?values.start_time:values.end_time);
  if(Number.isFinite(start)&&Number.isFinite(end)&&end-start<3600000)return '开始与结束时间之间不能少于1小时（检修按故障发现与期望完成时间核对）。';
  if(values.work_type==='repair'&&action==='start'){
    const prefix='EA118_C01机房'+(['A','B','C','D','E','H'].includes(values.scope)?values.scope+'楼':'');
    if(!values.title?.startsWith(prefix)||!values.title.endsWith('检修')||!values.title.slice(prefix.length,-2).trim())return '检修标题格式应为：'+prefix+'故障现象检修。';
  }
  if(action==='end'&&form.photoRequired&&Number(form.values.site_photo_count||0)+photos.length===0)return '结束前至少需要一张现场照片，请先添加。';
  if(form.sop&&action==='start'&&!sop.exempt&&(!sop.sop_id||!(sop.operator_record_ids??(sop.operator_record_id?[sop.operator_record_id]:[])).length||!sop.reviewer_record_id))return '请完成本次工单、操作人及审核人选择，或确认本次不使用工单。';
  if(form.sop&&action==='start'&&!sop.exempt&&(sop.operator_record_ids??[sop.operator_record_id]).includes(sop.reviewer_record_id))return '操作人与现场审核人不能是同一人。';
  return '';
}
