<template>
  <div class="original-sheet">
    <div class="toolbar">
      <label>原表 <select v-model="templateIndex" data-native-select><option v-for="(item,index) in templates" :key="item.name" :value="index">{{ item.scope }}楼{{ item.profession }}</option></select></label>
      <label>工作表 <select v-model="sheetIndex" data-native-select><option v-for="(item,index) in template?.sheets || []" :key="item.name" :value="index">{{ item.name.trim() }}</option></select></label>
      <label v-if="isChiller">巡检轮次 <select v-model="chillerRound" data-native-select><option v-for="item in chillerSlots" :key="item.key" :value="item.key">{{ item.shift }}班第{{ item.round }}次{{ rounds.some(r=>r.shift===item.shift && r.round===item.round) ? '' : '（未开始）' }}</option></select></label>
    </div>
    <p>原表中已核实对应的测点按本轮数据显示；没有可靠对应关系的单元格保持原样。冷机表每轮单独查看，表内时间为原模板标注。</p>
    <p v-if="!rounds.length">该日期尚无巡检记录，当前显示空白原表。</p>
    <div v-if="sheet" class="scroll" tabindex="0" aria-label="原巡检表，可横向和纵向滑动">
      <table :style="{fontSize:zoom+'px'}"><tbody>
        <tr v-for="(row,ri) in grid" :key="ri">
          <template v-for="cell in row" :key="cell.address">
            <td v-if="!cell.hidden" :rowspan="cell.rowspan" :colspan="cell.colspan" :class="{reading:!!cell.value,missing:!!cell.value && !display(cell.value.value)}">
              <input v-if="cell.value && editTargets(cell.value).length" :value="currentValue(cell.value)" :placeholder="cell.value.conflict ? '读数不一致，请核对' : '待填'" :aria-label="cell.value.parameter+' '+cell.address"
                @input="update(cell.value,$event)">
              <span v-else-if="cell.value">{{ cell.value.conflict ? '读数不一致' : display(cell.value.value) || '待填' }}</span><span v-else>{{ display(cell.text) }}</span>
            </td>
          </template>
        </tr>
      </tbody></table>
    </div>
    <p v-if="conflictCount" role="status">{{ conflictCount }} 个位置存在重复对应，未自动填入，请核对测点配置。</p>
  </div>
</template>

<script setup lang="ts">
import {computed,ref,watch} from 'vue';
type Value=string|number|boolean|null;
type FormSheet={name:string;rows:Value[][];merges:string[]};
type Template={scope:string;profession:string;name:string;sheets:FormSheet[]};
type Reading={template:string;sheet:string;cell:string;value:Value;device:string;point_id:string;parameter:string;read_only:boolean;kind:'point'|'section'|'paired';location?:string;form?:string;members?:Reading[];conflict?:boolean};
type Round={round_id:string;shift:string;round:number;original_cells:Reading[]};
type Device={id:string;sections:{location:string;form:string}[];points:{id:string;point_id:string}[];parameters:{id:string;reference:Value;read_only?:boolean}[]};
type Active={id:string;pending:boolean;batch:{devices:Device[];form_devices?:Device[]};reviews:Record<string,{values:Record<string,string>}>};
type Draft={values:Record<string,string>};
const props=defineProps<{templates:Template[];rounds:Round[];active?:Active;drafts:Record<string,Draft>;readOnly:boolean;zoom:number}>();
const emit=defineEmits<{updateReading:[device:string,parameter:string,value:string]}>();
const templateIndex=ref(0),sheetIndex=ref(0),chillerRound=ref('白1');
const chillerSlots=[{key:'白1',shift:'白',round:1},{key:'白2',shift:'白',round:2},{key:'夜1',shift:'夜',round:1},{key:'夜2',shift:'夜',round:2}];
const template=computed(()=>props.templates[templateIndex.value]);
const sheet=computed(()=>template.value?.sheets[sheetIndex.value]);
const isChiller=computed(()=>sheet.value?.name.trim()==='暖通冷机系统巡检表');
watch(templateIndex,()=>{sheetIndex.value=0;});
watch([isChiller,()=>props.active?.id],([selected,id])=>{
  if(!selected || !id)return;
  const activeRound=props.rounds.find(item=>item.round_id===id);
  if(activeRound && chillerSlots.some(item=>item.shift===activeRound.shift && item.round===activeRound.round))
    chillerRound.value=`${activeRound.shift}${activeRound.round}`;
});
const display=(v:Value|undefined)=>v==null?'':typeof v==='boolean'?(v?'是':'否'):String(v);
function address(row:number,col:number){let value=col+1,s='';while(value){value--;s=String.fromCharCode(65+value%26)+s;value=Math.floor(value/26);}return s+(row+1);}
function parseAddress(value:string){const m=/^([A-Z]+)([1-9]\d*)$/.exec(value);if(!m)return null;let col=0;for(const c of m[1])col=col*26+c.charCodeAt(0)-64;return {row:Number(m[2])-1,col:col-1};}
const positions=computed(()=>{
  const data=new Map<string,Reading[]>();
  const slot=chillerSlots.find(item=>item.key===chillerRound.value);
  const rounds=isChiller.value ? props.rounds.filter(item=>item.shift===slot?.shift && item.round===slot.round) : props.rounds;
  for(const round of rounds)for(const value of round.original_cells || []){
    if(value.template!==template.value?.name || value.sheet!==sheet.value?.name)continue;
    const entries=data.get(value.cell)||[];entries.push(value);data.set(value.cell,entries);
  }
  return data;
});
const conflictCount=computed(()=>[...positions.value.values()].filter(value=>value.length>1).length);
const grid=computed(()=>{
  if(!sheet.value)return [];
  const merges=new Map<string,{rowspan:number;colspan:number}>(),hidden=new Set<string>();
  for(const range of sheet.value.merges){const [a,b=a]=range.split(':');const start=parseAddress(a),end=parseAddress(b);if(!start||!end)continue;
    merges.set(a,{rowspan:end.row-start.row+1,colspan:end.col-start.col+1});
    for(let r=start.row;r<=end.row;r++)for(let c=start.col;c<=end.col;c++)if(r!==start.row||c!==start.col)hidden.add(address(r,c));
  }
  return sheet.value.rows.map((row,ri)=>row.map((text,ci)=>{const a=address(ri,ci),span=merges.get(a),values=positions.value.get(a);
    return {address:a,text,hidden:hidden.has(a),rowspan:span?.rowspan || 1,colspan:span?.colspan || 1,value:values?.length===1?values[0]:undefined};}));
});
function editTargets(reading:Reading){
  const active=props.active;
  if(!active || active.pending || props.readOnly || reading.read_only)return [];
  const round=props.rounds.find(item=>item.round_id===active.id && item.original_cells?.includes(reading));
  if(!round)return [];
  const members=reading.kind==='paired' ? reading.members || [] : [reading];
  const result:{device:string;parameter:string;reference:Value}[]=[];
  for(const member of members){
    const device=[...active.batch.devices,...(active.batch.form_devices || [])].find(item=>item.id===member.device);
    if(member.kind==='section'){
      const sections=device?.sections.map((item,index)=>({item,index})).filter(({item})=>item.location===member.location && item.form===member.form) || [];
      if(sections.length!==1)return [];
      const parameter=device?.parameters.find(item=>item.id===`s:${sections[0].index}:${member.parameter}` && !item.read_only);
      if(!parameter)return [];
      result.push({device:device!.id,parameter:parameter.id,reference:parameter.reference});
    }else{
      const points=device?.points.filter(point=>point.point_id===member.point_id) || [];
      if(points.length!==1)return [];
      const parameter=device?.parameters.find(item=>item.id==='p:'+points[0].id && !item.read_only);
      if(!parameter)return [];
      result.push({device:device!.id,parameter:parameter.id,reference:parameter.reference});
    }
  }
  return result;
}
function currentValue(reading:Reading){const targets=editTargets(reading);if(!targets.length)return display(reading.value);
  const values=targets.map(target=>props.drafts[target.device]?.values[target.parameter] ?? props.active?.reviews[target.device]?.values[target.parameter] ?? display(target.reference));
  const first=values[0].trim();
  return values.every(value=>{const current=value.trim();return current===first || (!!first && !!current && Number.isFinite(Number(first)) && Number.isFinite(Number(current)) && Number(first)===Number(current));}) ? values[0] : '';}
function update(reading:Reading,event:Event){for(const target of editTargets(reading))emit('updateReading',target.device,target.parameter,(event.target as HTMLInputElement).value);}
</script>

<style scoped>
.original-sheet{display:grid;gap:7px;min-width:0}.toolbar{display:flex;flex-wrap:wrap;gap:6px 10px}.toolbar label{display:flex;align-items:center;gap:4px}.toolbar select{min-height:34px;max-width:100%;color:inherit;background:var(--inspection-surface);border:1px solid var(--inspection-line);font:inherit}
p{margin:0}.scroll{max-width:100%;max-height:65vh;overflow:auto;border:1px solid var(--inspection-line)}table{border-collapse:collapse;width:max-content;min-width:100%;table-layout:fixed}td{border:1px solid var(--inspection-line);min-width:74px;max-width:260px;height:30px;padding:3px 5px;text-align:center;white-space:pre-wrap;overflow-wrap:anywhere}.reading{background:var(--inspection-surface);color:var(--inspection-text)}.missing{color:var(--cf-warning)}td input{width:100%;min-width:65px;padding:3px;color:inherit;background:transparent;border:1px solid var(--inspection-line);font:inherit}@media(max-width:700px){td input{font-size:16px}}
</style>
