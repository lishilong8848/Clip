import {createApp,h,ref} from 'vue';
import {installActionMotion,trackButtonRequest} from '../../src/workspace-v2/actionMotion';
import {followBottom,shouldFollow} from '../../src/workspace-v2/chatMotion';
import AnimatedReply from '../../src/workspace-v2/AnimatedReply.vue';
import PasswordStrength from '../../src/workspace-v2/PasswordStrength.vue';
import MobileNavigation from '../../src/workspace-v2/MobileNavigation.vue';
import MotionChart from '../../src/workspace-v2/MotionChart.vue';
import LearningDashboard from '../../src/components/LearningDashboard.vue';
import {usePasswordFeedback} from '../../src/workspace-v2/passwordFeedback';
import {Home,ClipboardList,Grid2X2,UserRound} from 'lucide-vue-next';
import '../../src/workspace-v2/interactions.css';
import '../../src/workspace-v2/workspace.css';
import '../../src/workspace-v2/motionBlocks.css';
document.documentElement.classList.add('workspace-v2');
installActionMotion();
const text=ref('');
const finished=ref(false),saving=ref(false);
const drawer=ref(false);
const password=ref('fixture-only'),newPassword=ref(''),passwordError=ref(''),active=ref('home');
const chartItems=ref([{label:'10/01',value:2},{label:'10/02',value:6},{label:'10/03',value:3}]);
const test=window as any;
test.clicked=0;
test.finishJob=(value:unknown)=>trackButtonRequest(Promise.resolve(value),'GET');
test.text=(value:string)=>text.value=value;
test.changeChart=()=>chartItems.value=[{label:'10/01',value:8},{label:'10/02',value:4},{label:'10/03',value:5},{label:'10/04',value:2}];
createApp({setup:()=>{usePasswordFeedback(passwordError,[password]);return()=>h('div',{},[
  h('button',{id:'save','data-v2-submit':'',style:'width:180px;height:40px;',onClick:()=>{test.clicked++;void trackButtonRequest(new Promise((resolve,reject)=>{test.resolve=resolve;test.reject=reject;})).catch(()=>{});}},'保存变更'),
  h('button',{id:'dynamic-save','data-v2-submit':'',disabled:saving.value||finished.value,style:'width:180px;height:40px;',onClick:()=>{saving.value=true;void trackButtonRequest(new Promise(resolve=>{test.resolveDynamic=()=>{finished.value=true;saving.value=false;resolve({ok:true});};}));}},finished.value?'已完成':saving.value?'正在处理':'完成后禁用'),
  h('button',{id:'open-drawer',style:'width:180px;height:40px;',onClick:()=>{drawer.value=true;void trackButtonRequest(new Promise(resolve=>test.finishDrawerRead=resolve),'GET');}},'打开机柜详情'),
  h('button',{id:'preview',onClick:()=>{void trackButtonRequest(Promise.resolve({ok:true}));}},'生成预览'),
  h('form',{onSubmit:(e:Event)=>{e.preventDefault();void trackButtonRequest(new Promise(resolve=>test.finishSearch=resolve),'GET');}},[h('button',{id:'search',type:'submit'},'查询')]),
  drawer.value?h('section',{id:'drawer-fixture'},'详情已打开，历史加载中'):null,
  h('label',{},[h('input',{id:'toggle',type:'checkbox'}),'勾选项目']),
  h('label',{},[h('input',{id:'binary',type:'checkbox','data-v2-binary':''}),'二选一设置']),
  h('input',{id:'personnel-password',type:'password',value:password.value,onInput:(e:Event)=>password.value=(e.target as HTMLInputElement).value}),
  h('button',{id:'wrong-password',onClick:()=>passwordError.value='姓名或密码不正确'},'模拟密码错误'),
  h('input',{id:'new-password',type:'password',value:newPassword.value,onInput:(e:Event)=>newPassword.value=(e.target as HTMLInputElement).value}),h(PasswordStrength,{value:newPassword.value}),
  h('form',{},[h('input',{required:true,'aria-label':'必填'}),h('button',{type:'submit',id:'invalid'},'提交必填表单')]),
  h('form',{novalidate:true,onSubmit:(e:Event)=>e.preventDefault()},[h('button',{type:'submit',id:'client-invalid'},'未通过业务校验')]),
  h('div',{class:'thread',style:'height:180px;overflow:auto;width:360px;max-width:90vw;margin-top:30px;border:1px solid #aaa'},[
    h('article',{class:'turn is-pending'},[h('div',{class:'answer'},[h('div',{class:'answer-heading'},'灯塔助手'),h(AnimatedReply,{text:text.value})])]),
  ]),
  h('div',{id:'motion-chart',style:'max-width:700px;margin:32px auto'},[h(MotionChart,{title:'图表验收',trend:true,items:chartItems.value,onSelect:(_item:unknown,index:number)=>test.chartSelection=index})]),
  h('div',{id:'learning-chart',style:'max-width:700px;margin:32px auto'},[h(LearningDashboard,{data:{summary:{answered:4,attempt_count:4},today_summary:{},trend:[{date:'2026-10-01',people:2,accuracy:50}],people:[]}})]),
  h(MobileNavigation,{active:active.value,tabs:[{value:'home',label:'首页',icon:Home},{value:'tasks',label:'待办',icon:ClipboardList},{value:'functions',label:'功能',icon:Grid2X2},{value:'my',label:'我的',icon:UserRound}],onSelect:(value:string)=>{active.value=value;}}),
])}}).mount('#app');
test.follow=()=>followBottom(document.querySelector('.thread')!);
test.follows=()=>shouldFollow(document.querySelector('.thread'));
