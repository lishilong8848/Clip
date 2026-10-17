import type { EChartsCoreOption } from 'echarts/core';
import { beijingDate } from './eventPeriod';

// VIPBoard's bar/area/donut templates, adapted to the existing modular engine.
export type ChartView = 'bar' | 'horizontal' | 'line' | 'area' | 'donut' | 'pie' | 'calendar';
export type ChartItem = { label:string; value:number; color?:string; [key:string]:unknown };
export const chartPalette = ['#197f70','#548eb3','#b48135','#8b78ad','#577d56','#ab6874'];
export const calendarColors=(dark=false)=>dark?['#25364a','#376eae','#79baff']:['#eaf1ff','#85aaff','#2864e8'];
export const chartViews = [
  {value:'bar',label:'柱状图'}, {value:'horizontal',label:'横向条形图'},
  {value:'line',label:'折线图'}, {value:'area',label:'面积趋势图'},
  {value:'donut',label:'环形图'}, {value:'pie',label:'饼图'},
  {value:'calendar',label:'日历热力图'},
] as const;
export const isTrend = (view:ChartView) => view==='line'||view==='area';
export const isCircular = (view:ChartView) => view==='donut'||view==='pie';
export function chartOption(items:ChartItem[], view:ChartView, unit='条', compact=false, animate=true, dark=false,fit?:{width:number;height:number}):EChartsCoreOption {
  if(view==='calendar'){
    if(items.some(item=>!Number.isFinite(item.start)||!Number.isFinite(item.end)||Number(item.end)-Number(item.start)!==86400000))throw new Error('日历热力图需要逐日数据。');
    const dates=items.map(item=>beijingDate(Number(item.start))),maximum=Math.max(1,...items.map(item=>item.value));
    const long=items.length>62,labelColor=dark?'#c1cbd6':'#5b6c63';
    const tiny=!!fit&&(fit.width<300||fit.height<130),dateLabels=!long&&(!fit||fit.width>240&&fit.height>150);
    // VIPBoard XCalendar: use the actual loaded date range, never fixed sample years or maxima.
    return {
      animation:animate,animationDuration:400,animationDurationUpdate:300,animationEasing:'cubicOut',aria:{enabled:true},
      tooltip:{trigger:'item',renderMode:'richText',confine:true,formatter:(p:any)=>`${dates[p.dataIndex]}\n${items[p.dataIndex]?.value.toLocaleString('zh-CN')} ${unit}`},
      visualMap:{show:false,min:0,max:maximum,inRange:{color:calendarColors(dark)}},
      calendar:{range:[dates[0],dates[dates.length-1]],orient:long?'horizontal':'vertical',top:tiny?4:30,left:tiny?4:long?34:16,right:fit?4:16,bottom:fit?4:10,cellSize:['auto','auto'],splitLine:{show:false},itemStyle:{color:'transparent',borderColor:'transparent',borderWidth:fit&&fit.width<440?1:2},dayLabel:{show:!tiny,firstDay:1,nameMap:['日','一','二','三','四','五','六'],color:labelColor,fontSize:11},monthLabel:{show:long&&!tiny,nameMap:Array.from({length:12},(_,i)=>(i+1)+'月'),color:labelColor,fontSize:11},yearLabel:{show:false}},
      xAxis:[],yAxis:[],dataZoom:[],
      series:[{id:'records',type:'heatmap',coordinateSystem:'calendar',itemStyle:{borderRadius:3},data:items.map((item,i)=>({id:dates[i],value:[dates[i],item.value],label:{show:dateLabels,formatter:String(Number(dates[i]?.slice(-2))),color:dark?(item.value>maximum*.5?'#122437':'#e6f0ff'):(item.value>maximum*.5?'#fff':'#24354a')}})),label:{show:dateLabels,formatter:(p:any)=>String(Number(dates[p.dataIndex]?.slice(-2))),fontSize:11},emphasis:{itemStyle:{borderColor:dark?'#eef6ff':'#1243a5',borderWidth:2}}}],
    };
  }
  const circle=isCircular(view), trend=isTrend(view), horizontal=view==='horizontal';
  const valid=items.map(item=>Number.isFinite(item.value)?item.value:0);
  const maximum=Math.max(0,...valid);
  const labelColor=dark?'#c1cbd6':'#5b6c63',gridColor=dark?'#3b444e':'#e4ebe6';
  const accent=dark?'#65cdb3':'#197f70';
  const spring=(t:number)=>1+2.28*Math.pow(t-1,3)+1.28*Math.pow(t-1,2);
  const color=(item:ChartItem,i:number)=>/^#[0-9a-f]{6}$/i.test(item.color||'')?item.color:chartPalette[i%chartPalette.length];
  const data=items.map((item,i)=>({id:String(item.key??item.label)+':'+i,name:item.label,value:valid[i],itemStyle:{color:circle?color(item,i):item.color&&/^#[0-9a-f]{6}$/i.test(item.color)?item.color:valid[i]===maximum?accent:'#9bb8b4'}}));
  return {
    calendar:[],visualMap:[],
    animation:animate,animationDuration:trend?1000:550,animationDurationUpdate:500,
    animationEasing:'cubicOut',animationEasingUpdate:spring,
    aria:{enabled:true},
    tooltip:{trigger:circle?'item':'axis',renderMode:'richText',confine:true,
      ...(dark?{backgroundColor:'#292f36',borderColor:'#46515e',textStyle:{color:'#edf0f4'}}:{}),
      axisPointer:{type:trend?'line':'shadow',snap:true},
      formatter:(input:any)=>{const p=Array.isArray(input)?input[0]:input;const item=items[p?.dataIndex];return item?`${item.label}\n${item.value.toLocaleString('zh-CN')} ${unit}`:'';}},
    grid:{left:horizontal?compact?84:112:12,right:horizontal?40:16,top:28,bottom:items.length>14&&!circle?50:16,containLabel:true},
    xAxis:circle?[]:{type:horizontal?'value':'category',show:true,data:horizontal?undefined:items.map(i=>i.label),minInterval:valid.every(Number.isInteger)?1:undefined,axisTick:{show:false},axisLine:{show:!horizontal,lineStyle:{color:dark?'#536171':'#cbd6d1'}},axisLabel:{color:labelColor,fontSize:11,hideOverlap:true},splitLine:{show:horizontal,lineStyle:{color:gridColor,type:'dashed'}}},
    yAxis:circle?[]:{type:horizontal?'category':'value',show:true,inverse:horizontal,data:horizontal?items.map(i=>i.label):undefined,minInterval:valid.every(Number.isInteger)?1:undefined,axisLine:{show:false},axisTick:{show:false},axisLabel:{color:labelColor,fontSize:11,width:horizontal?compact?72:100:undefined,overflow:'truncate'},splitLine:{show:!horizontal,lineStyle:{color:gridColor,type:'dashed'}}},
    // Keep zoom/drag inside the plot (touch pan+pinch) with no visible slider
    // bar/track/outline. Plain mouse wheel must keep scrolling the page: zoom
    // only on Ctrl+wheel and never hijack the wheel for panning.
    dataZoom:!circle&&items.length>14?[{type:'inside',xAxisIndex:horizontal?undefined:0,yAxisIndex:horizontal?0:undefined,start:0,end:items.length<=60?100:14/items.length*100,zoomOnMouseWheel:'ctrl',moveOnMouseMove:false,moveOnMouseWheel:false,preventDefaultMouseMove:false}]:[],
    series:[circle?{
      id:'records',type:'pie',radius:view==='donut'?['52%','76%']:['0%','76%'],center:['50%','50%'],padAngle:2,stillShowZeroSum:false,
      itemStyle:{borderRadius:4},label:{show:!compact,formatter:'{b}\n{c}',fontSize:11,color:dark?labelColor:'#53665b'},
      labelLayout:{hideOverlap:true},emphasis:{scaleSize:5},data:data.map(item=>({...item,label:{show:!compact&&item.value>0}})),
      animationType:'expansion',universalTransition:true,
    }:{
      id:'records',type:trend?'line':'bar',data,barMaxWidth:32,barMinHeight:0,
      itemStyle:{borderRadius:horizontal?[0,4,4,0]:[4,4,0,0]},
      label:{show:!trend,position:horizontal?'right':'top',valueAnimation:true,color:dark?labelColor:'#53665b'},
      showSymbol:false,symbolSize:7,smooth:0.2,lineStyle:{width:2.5,color:accent},
      areaStyle:view==='area'?{color:{type:'linear',x:0,y:0,x2:0,y2:1,colorStops:[{offset:0,color:'#197f7045'},{offset:1,color:'#197f7004'}]}}:undefined,
      animationDelay:trend?0:(i:number)=>Math.min(i,7)*70,animationEasing:trend?'cubicOut':spring,universalTransition:true,
      emphasis:{focus:'series',scale:true},
    }],
  };
}
