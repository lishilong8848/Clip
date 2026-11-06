import assert from 'node:assert/strict';
import {configuredInspectionPositions,manuallyFilledInspectionPositions,inspectionPosition} from '../src/components/inspectionCellVisibility.ts';

const point=(id,cell,enabled=true)=>({id,point_id:id,template:'D.xlsx',sheet:'巡检表',round_cells:{白1:cell,白2:'C9'},enabled});
const active={duty:{shift:'白'},round:1,manual_sheet_cells:[],reviews:{},batch:{
  devices:[{id:'D:device',points:[point('configured','B2'),point('disabled','C2',false),point('unknown','D2',null)],
    sections:[{location:'D-178',form:'巡检表'}],parameters:[{id:'p:configured',point:'configured'},
      {id:'p:disabled',point:'disabled'},{id:'p:unknown',point:'unknown'},{id:'s:0:目录数据',section:0}]}],
  form_devices:[{id:'D:manual',points:[point('original-placeholder','E2')],parameters:[{id:'p:original-placeholder',point:'original-placeholder'}]}]}};
const key=cell=>inspectionPosition('D.xlsx','巡检表',cell);
assert.deepEqual([...configuredInspectionPositions(active)],[key('B2')]);
assert.equal(configuredInspectionPositions({...active,duty:{shift:'夜'}}).size,0);
const reading={template:'D.xlsx',sheet:'巡检表',cell:'F2',kind:'section',device:'D:device',location:'D-178',form:'巡检表',parameter:'目录数据'};
assert.equal(manuallyFilledInspectionPositions(active,[reading]).size,0);
const saved={...active,manual_sheet_cells:[{template:'D.xlsx',sheet:'巡检表',cell:'F2'}]};
assert(manuallyFilledInspectionPositions(saved,[reading]).has(key('F2')));
const legacy={...active,manual_sheet_cells:null,reviews:{'D:device':{values:{'s:0:目录数据':'手填'}}}};
assert(manuallyFilledInspectionPositions(legacy,[reading]).has(key('F2')));
assert(!manuallyFilledInspectionPositions({...active,reviews:legacy.reviews},[reading]).has(key('F2')),'new rounds do not mistake completed source values for manual edits');
console.log('Configured cells, inactive points, supplemental placeholders and manual edits: OK');
