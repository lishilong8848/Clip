// A template/catalogue placeholder is not itself an enabled point configuration.
type Point = {id:string;point_id?:string;enabled?:boolean|null;template?:string;sheet?:string;round_cells?:Record<string,string>;mapping_issue?:string};
type Parameter = {id:string;point?:string;section?:number;read_only?:boolean};
type Device = {id:string;points?:Point[];sections?:{location?:string;form?:string}[];parameters?:Parameter[]};
type Reading = {template:string;sheet:string;cell:string;device?:string;kind?:string;point_id?:string;parameter?:string;location?:string;form?:string;members?:Reading[]};
export type InspectionVisibilityActive = {duty:{shift:string};round:number;batch?:{devices?:Device[];form_devices?:Device[]};reviews?:Record<string,{values?:Record<string,string>}>;manual_sheet_cells?:{template:string;sheet:string;cell:string}[]|null;manual_sheet_cells_legacy?:boolean};

export const inspectionPosition=(template:string,sheet:string,cell:string)=>JSON.stringify([template,sheet.trim(),cell]);

export function configuredInspectionPositions(active:InspectionVisibilityActive|undefined):Set<string>{
  const result=new Set<string>();if(!active)return result;
  const slot=active.duty.shift+active.round;
  // Supplemental form devices represent blank template cells, not live configuration.
  for(const device of active.batch?.devices||[])for(const point of device.points||[]){
    const address=point.round_cells?.[slot];
    if(point.enabled!==true||point.mapping_issue||!point.template||!point.sheet||!address)continue;
    if(!(device.parameters||[]).some(parameter=>parameter.point===point.id&&!parameter.read_only))continue;
    result.add(inspectionPosition(point.template,point.sheet,address));
  }
  return result;
}

export function manuallyFilledInspectionPositions(active:InspectionVisibilityActive|undefined,readings:Reading[]):Set<string>{
  const result=new Set<string>();if(!active)return result;
  for(const cell of active.manual_sheet_cells||[])result.add(inspectionPosition(cell.template,cell.sheet,cell.cell));
  if(Array.isArray(active.manual_sheet_cells)&&!active.manual_sheet_cells_legacy)return result;
  const devices=new Map([...(active.batch?.devices||[]),...(active.batch?.form_devices||[])].map(device=>[device.id,device]));
  for(const reading of readings){
    const members=reading.kind==='paired'?reading.members||[]:[reading];
    for(const member of members){
      const device=devices.get(member.device||''),values=active.reviews?.[member.device||'']?.values;
      if(!device||!values)continue;
      let ids:string[]=[];
      if(member.kind==='point'){
        const points=(device.points||[]).filter(point=>point.point_id===member.point_id);
        if(points.length===1)ids=(device.parameters||[]).filter(parameter=>parameter.point===points[0].id).map(parameter=>parameter.id);
      }else if(member.kind==='section'){
        const sections=(device.sections||[]).map((section,index)=>({section,index})).filter(({section})=>section.location===member.location&&section.form===member.form);
        if(sections.length===1)ids=(device.parameters||[]).filter(parameter=>parameter.section===sections[0].index&&parameter.id===`s:${sections[0].index}:${member.parameter}`).map(parameter=>parameter.id);
      }
      if(ids.some(id=>Object.prototype.hasOwnProperty.call(values,id))){result.add(inspectionPosition(reading.template,reading.sheet,reading.cell));break;}
    }
  }
  return result;
}
