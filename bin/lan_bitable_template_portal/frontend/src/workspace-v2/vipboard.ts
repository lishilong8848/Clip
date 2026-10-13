import type { EChartsCoreOption } from 'echarts/core';

// VIPBoard's bar/area/donut templates, adapted to the existing modular engine.
export type ChartView = 'bar' | 'horizontal' | 'line' | 'area' | 'donut' | 'pie';
export type ChartItem = { label:string; value:number; color?:string; [key:string]:unknown };
export const chartPalette = ['#197f70','#548eb3','#b48135','#8b78ad','#577d56','#ab6874'];
export const chartViews = [
  {value:'bar',label:'柱状图'}, {value:'horizontal',label:'横向条形图'},
  {value:'line',label:'折线图'}, {value:'area',label:'面积趋势图'},
  {value:'donut',label:'环形图'}, {value:'pie',label:'饼图'},
] as const;
export const isTrend = (view:ChartView) => view==='line'||view==='area';
export const isCircular = (view:ChartView) => view==='donut'||view==='pie';
export function chartOption(items:ChartItem[], view:ChartView, unit='条', compact=false, animate=true):EChartsCoreOption {
  const circle=isCircular(view), trend=isTrend(view), horizontal=view==='horizontal';
  const valid=items.map(item=>Number.isFinite(item.value)?item.value:0);
  const maximum=Math.max(0,...valid);
  const spring=(t:number)=>1+2.28*Math.pow(t-1,3)+1.28*Math.pow(t-1,2);
  const color=(item:ChartItem,i:number)=>/^#[0-9a-f]{6}$/i.test(item.color||'')?item.color:chartPalette[i%chartPalette.length];
  const data=items.map((item,i)=>({id:String(item.key??item.label)+':'+i,name:item.label,value:valid[i],itemStyle:{color:circle?color(item,i):item.color&&/^#[0-9a-f]{6}$/i.test(item.color)?item.color:valid[i]===maximum?'#197f70':'#9bb8b4'}}));
  return {
    animation:animate,animationDuration:trend?1000:550,animationDurationUpdate:500,
    animationEasing:'cubicOut',animationEasingUpdate:spring,
    aria:{enabled:true},
    tooltip:{trigger:circle?'item':'axis',renderMode:'richText',confine:true,
      axisPointer:{type:trend?'line':'shadow',snap:true},
      formatter:(input:any)=>{const p=Array.isArray(input)?input[0]:input;const item=items[p?.dataIndex];return item?`${item.label}\n${item.value.toLocaleString('zh-CN')} ${unit}`:'';}},
    grid:{left:horizontal?compact?84:112:12,right:horizontal?40:16,top:28,bottom:items.length>14&&!circle?50:16,containLabel:true},
    xAxis:circle?{show:false}:{type:horizontal?'value':'category',show:true,data:horizontal?undefined:items.map(i=>i.label),minInterval:valid.every(Number.isInteger)?1:undefined,axisTick:{show:false},axisLine:{show:!horizontal,lineStyle:{color:'#cbd6d1'}},axisLabel:{color:'#5b6c63',fontSize:11,hideOverlap:true},splitLine:{show:horizontal,lineStyle:{color:'#e4ebe6',type:'dashed'}}},
    yAxis:circle?{show:false}:{type:horizontal?'category':'value',show:true,inverse:horizontal,data:horizontal?items.map(i=>i.label):undefined,minInterval:valid.every(Number.isInteger)?1:undefined,axisLine:{show:false},axisTick:{show:false},axisLabel:{color:'#5b6c63',fontSize:11,width:horizontal?compact?72:100:undefined,overflow:'truncate'},splitLine:{show:!horizontal,lineStyle:{color:'#e4ebe6',type:'dashed'}}},
    dataZoom:!circle&&items.length>14?[{type:'slider',xAxisIndex:horizontal?undefined:0,yAxisIndex:horizontal?0:undefined,start:0,end:items.length<=60?100:14/items.length*100,height:horizontal?undefined:16,width:horizontal?14:undefined,bottom:horizontal?undefined:3,right:horizontal?0:undefined,showDetail:false,brushSelect:false}]:[],
    series:[circle?{
      id:'records',type:'pie',radius:view==='donut'?['52%','76%']:['0%','76%'],center:['50%','50%'],padAngle:2,stillShowZeroSum:false,
      itemStyle:{borderRadius:4},label:{show:!compact,formatter:'{b}\n{c}',fontSize:11,color:'#53665b'},
      labelLayout:{hideOverlap:true},emphasis:{scaleSize:5},data:data.map(item=>({...item,label:{show:!compact&&item.value>0}})),
      animationType:'expansion',universalTransition:true,
    }:{
      id:'records',type:trend?'line':'bar',data,barMaxWidth:32,barMinHeight:0,
      itemStyle:{borderRadius:horizontal?[0,4,4,0]:[4,4,0,0]},
      label:{show:!trend,position:horizontal?'right':'top',valueAnimation:true,color:'#53665b'},
      showSymbol:false,symbolSize:7,smooth:0.2,lineStyle:{width:2.5,color:'#197f70'},
      areaStyle:view==='area'?{color:{type:'linear',x:0,y:0,x2:0,y2:1,colorStops:[{offset:0,color:'#197f7045'},{offset:1,color:'#197f7004'}]}}:undefined,
      animationDelay:trend?0:(i:number)=>Math.min(i,7)*70,animationEasing:trend?'cubicOut':spring,universalTransition:true,
      emphasis:{focus:'series',scale:true},
    }],
  };
}
