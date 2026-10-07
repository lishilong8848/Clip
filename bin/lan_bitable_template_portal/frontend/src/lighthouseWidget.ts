import { createApp, defineAsyncComponent } from 'vue';
import LoadingIndicator from './components/LoadingIndicator.vue';
import UiTransition from './components/UiTransition.vue';
import './lighthouseWidget.css';

const host = document.getElementById('clipflow-lighthouse-widget');
if (host?.dataset.userId && !host.hasChildNodes() && !(window.parent !== window && new URLSearchParams(location.search).get('_assistant_frame') === '1')) {
  const Assistant = defineAsyncComponent(() => import('./components/LighthouseAssistant.vue'));
  createApp(Assistant, { userId: host.dataset.userId, userName: host.dataset.userName || '' })
    .component('LoadingIndicator', LoadingIndicator).component('UiTransition', UiTransition).mount(host);
}
