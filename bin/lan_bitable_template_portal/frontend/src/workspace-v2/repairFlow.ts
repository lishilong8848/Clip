import { eventRecordId, eventTransferEnabled } from '../eventManagementUtils';
import type { Dict } from './client';

export type RepairFlowAction = 'event' | 'project' | 'notice' | 'followups' | 'complete';
export type RepairFlowStep = { id:RepairFlowAction; title:string; description:string; status:'wait'|'process'|'finish'|'error'; actionLabel:string };
export function repairFlowSteps(context:'event'|'project', record?:Dict|null, event?:Dict|null, syncStatus=''):RepairFlowStep[] {
  const saved=Boolean(record?.record_id), sourceId=String(record?.source_event_id||'').trim();
  const linkedEvent=context==='event'?event:sourceId&&eventRecordId(event)===sourceId?event:null;
  const transferred=Boolean(linkedEvent&&eventTransferEnabled(linkedEvent));
  const transferKnown=linkedEvent?.transfer_to_overhaul!==undefined&&linkedEvent.transfer_to_overhaul!==null;
  const noticeIds=Array.isArray(record?.source_repair_ids)?record.source_repair_ids.filter(Boolean):[];
  const verified=record?.state_locked!==true&&record?.followup_state_verified!==false;
  const countValue=record?.followup_count;
  const count=verified&&countValue!==null&&countValue!==undefined&&countValue!==''&&Number.isInteger(Number(countValue))&&Number(countValue)>=0?Number(countValue):null;
  const completed=saved&&verified&&record?.is_completed===true;
  const steps:RepairFlowStep[]=[
    {id:'event',title:'事件转检修',description:transferred?'已标记转检修':transferKnown?'尚未标记转检修':sourceId?'已关联事件，转检修待核对':'尚未关联事件',status:transferred?'finish':'wait',actionLabel:'查看事件关联'},
    {id:'project',title:'维修单',description:saved?'已保存维修单':context==='event'?'进入维修单核对':'待保存维修单',status:saved?'finish':'wait',actionLabel:'查看或填写维修单'},
    {id:'notice',title:'检修通告',description:noticeIds.length?`已关联 ${new Set(noticeIds).size} 条；发送状态需核对`:saved?'尚未关联检修通告':'待维修单资料确认',status:noticeIds.length?'finish':'wait',actionLabel:'办理检修通告'},
    {id:'followups',title:'跟进记录',description:count===null?saved?'跟进数量待核验':'待维修单资料确认':count>0?`已保存 ${count} 条跟进`:'尚无跟进记录',status:count!==null&&count>0?'finish':'wait',actionLabel:'查看或填写跟进记录'},
    {id:'complete',title:'维修完成',description:completed?'维修单已确认完成':!verified?'完成状态待核验':record?.is_completed===false?'维修尚未完成':'完成状态待核对',status:completed?'finish':'wait',actionLabel:'查看维修完成情况'},
  ];
  // A linked source or a 100% draft must not invent a successful send or a completed repair.
  const current=context==='event'?(transferred?1:0):!saved||['pending','failed'].includes(syncStatus)?1:!noticeIds.length?2:count===null||count===0?3:4;
  if(steps[current]!.status==='wait')steps[current]!.status='process';
  if(saved&&['pending','failed'].includes(syncStatus)){
    steps[1]!.status=syncStatus==='failed'?'error':'process';
    steps[1]!.description=syncStatus==='failed'?'已保存，飞书同步失败':'已保存，正在同步飞书';
  }
  return steps;
}
