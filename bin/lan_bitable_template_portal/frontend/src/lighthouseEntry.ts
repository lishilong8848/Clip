import { defineAsyncComponent } from 'vue';
import LighthouseLauncherFallback from './components/LighthouseLauncherFallback.vue';

export const LighthouseAssistant = defineAsyncComponent({
  loader: () => import('./components/LighthouseAssistant.vue'),
  loadingComponent: LighthouseLauncherFallback,
  errorComponent: LighthouseLauncherFallback,
  delay: 0,
  timeout: 30000,
  onError(_error, retry, fail, attempts) {
    if (attempts <= 2) window.setTimeout(retry, attempts * 1000);
    else fail();
  },
});
