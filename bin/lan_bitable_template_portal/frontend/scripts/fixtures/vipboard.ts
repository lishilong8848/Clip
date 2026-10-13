import { createApp,h,ref } from 'vue';
import { NConfigProvider } from 'naive-ui';
import MotionChart from '../../src/workspace-v2/MotionChart.vue';
import SidebarNavigation from '../../src/workspace-v2/SidebarNavigation.vue';
import type { ChartView } from '../../src/workspace-v2/vipboard';
import '../../src/workspace-v2/workspace.css';
import '../../src/workspace-v2/dashboard/dashboard.css';
document.documentElement.classList.add('workspace-v2');
const items=ref([{label:'维保',value:12},{label:'变更',value:6},{label:'检修',value:3},{label:'轮巡',value:1}]),view=ref<ChartView>('bar'),failed=ref(false),loading=ref(false),mounted=ref(true);
const fixture=window as any;
const collapsed=ref(false),selected=ref('home'),options=ref([{key:'home',label:'个人工作台'},{key:'run',label:'运行管理',type:'group',children:[{key:'event',label:'事件管理'},{key:'notices',label:'通告办理'}]}]);
fixture.chartFixture={items,view,failed,loading,mounted,selection:[],collapsed,selected,options};
createApp({setup:()=>()=>h(NConfigProvider,{},()=>h('div',{class:['v2-shell',collapsed.value?'is-collapsed':'']},[
  h('header',{class:'v2-header'},'灯塔 · 组件验收'),
  h(SidebarNavigation,{collapsed:collapsed.value,options:options.value,selected:selected.value,name:'组件验收人员',scopeLabel:'A楼','onUpdate:collapsed':(v:boolean)=>collapsed.value=v,onSelect:(v:string)=>selected.value=v}),
  h('main',{class:'v2-main',style:'padding-top:20px'},mounted.value?[h(MotionChart,{title:'业务分布',items:items.value,view:view.value,failed:failed.value,loading:loading.value,onSelect:(item:unknown)=>fixture.chartFixture.selection.push(item)})]:[]),
]))}).mount('#app');
