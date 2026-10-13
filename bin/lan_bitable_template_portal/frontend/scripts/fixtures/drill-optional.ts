import {createApp,h} from 'vue';
import Drill from '../../src/components/DrillManagementPage.vue';
import Transition from '../../src/components/UiTransition.vue';
import Loading from '../../src/components/LoadingIndicator.vue';
import '../../src/global.css';

createApp({render:()=>h(Drill,{scope:'A',scopeOptions:[{value:'A',label:'A楼'}]})})
  .component('UiTransition',Transition).component('LoadingIndicator',Loading).mount('#app');
