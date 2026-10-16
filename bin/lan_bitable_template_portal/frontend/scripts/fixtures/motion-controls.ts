import {createApp} from 'vue';
import Fixture from './motion-controls.vue';
import UiTransition from '../../src/components/UiTransition.vue';
createApp(Fixture).component('UiTransition',UiTransition).mount('#app');
