// These are work modes, independent of drag-and-drop coordinates. A selected
// record and its form always stay mounted together while switching views.
type View = {label:string;blocks:string[]};
export type WorkZone = {id:string;span:number};
type Layout = {zones:WorkZone[];views?:View[]};
const zone=(id:string,span=12):WorkZone=>({id:'block_'+id,span});
export const businessViews:Record<string,Layout>={
  events:{zones:[zone('records'),zone('overview',5),zone('trend',7)],views:[
    {label:'事件台账',blocks:['records']},{label:'运行分析',blocks:['overview','trend']}]},
  daily:{zones:[zone('tasks'),zone('summary')],views:[
    {label:'当班任务',blocks:['tasks']},{label:'任务统计',blocks:['summary']}]},
  cabinet:{zones:[zone('workspace'),zone('summary')],views:[
    {label:'包间与台账',blocks:['workspace']},{label:'上下电统计',blocks:['summary']}]},
  signatures:{zones:[zone('people'),zone('duplicates')],views:[
    {label:'人员签名',blocks:['people']},{label:'重名核对',blocks:['duplicates']}]},
  'learning-profile':{zones:[zone('trend',7),zone('completion',5),zone('method',6),zone('interview',6)]},
  'learning-practice':{zones:[zone('answer',8),zone('questions',4)]},
  'guard-building':{zones:[zone('tasks',5),zone('form',7)]},
  'guard-admin':{zones:[zone('results'),zone('tasks')],views:[
    {label:'执行进度',blocks:['results']},{label:'任务发布',blocks:['tasks']}]},
  mop:{zones:[zone('notices',5),zone('binding',7)]},
  sops:{zones:[zone('templates',4),zone('editor',8)]},
  'plan-review':{zones:[zone('records',5),zone('snapshot',7)]},
  'plan-rules':{zones:[zone('sets'),zone('picker',7),zone('draft',5)]},
};
