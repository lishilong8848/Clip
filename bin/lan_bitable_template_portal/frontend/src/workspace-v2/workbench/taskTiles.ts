import { Activity, ArrowLeftRight, BookOpen, ClipboardList, Droplets, FileCheck2, KeyRound, Layers3, ListChecks, Package, PenLine, Route, ScanLine, ShieldCheck, Siren, SlidersHorizontal, Wrench, Zap } from 'lucide-vue-next';
import type { WorkItem } from './sources';

export const taskTilePriority:Record<string,number>={events:0,critical_guard:1,'notices:变更':2,risk_inspection:3};

const styles:Record<string,[typeof ClipboardList,number]>={
  events:[Siren,352],critical_guard:[ShieldCheck,32],'notices:变更':[ArrowLeftRight,218],risk_inspection:[ScanLine,278],
  'notices:维保':[Wrench,165],'notices:检修':[FileCheck2,186],'notices:轮巡':[Route,240],
  'notices:设备调整':[SlidersHorizontal,295],'notices:上下电':[Zap,46],
  engineer_mop:[ClipboardList,220],repair_management:[Wrench,172],cabinet:[Layers3,252],drills:[Activity,205],
  water:[Droplets,202],accounts:[KeyRound,38],tool_management:[Package,26],tool_external:[FileCheck2,42],learning:[BookOpen,270],
  inspection:[ListChecks,157],signatures:[PenLine,219],ali_confirmation:[FileCheck2,219],
  pre_shift:[ClipboardList,157],seal:[ShieldCheck,32],
};

export const taskTileStyle=(key:string)=>styles[key]||[ClipboardList,205] as const;
export const noticeTileLabel=(item:WorkItem)=>item.detail==='维保'?'维保通告':item.detail||'通告';
export function taskTileKey(item:WorkItem){
  if(item.kind==='event_notices')return 'events';
  return item.kind==='notices'?'notices:'+(item.detail||'通告'):item.kind;
}
