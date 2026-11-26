import { createApp } from 'vue';
import Workspace from './Workspace.vue';
import LoadingIndicator from '../components/LoadingIndicator.vue';
import UiTransition from '../components/UiTransition.vue';
import '../global.css';
import './workspace.css';
import './interactions.css';
import './events.css';
import './cabinetBatch.css';
import './dashboard/dashboard.css';
import './motionBlocks.css';
import './vipboard.css';
import './feedback.css';
import './feedbackToasts.css';
import './pageFilters.css';
import './pageLayout.css';
import './compactWorkspaces.css';
import './handover.css';
import './header.css';
import './account.css';
import './scrollbars.css';
import './night.css';
import './topNavigation.css';
import './surfaces.css';
import './softInterface.css';
import './businessLayout.css';
import './operationsDesk.css';
import './cabinetDesk.css';
import './drillRiskDesk.css';
import { installFeedbackMotion } from './feedbackMotion';
import { installActionMotion } from './actionMotion';
import { installLinkNavigation } from './navigation';
document.documentElement.classList.add('workspace-v2');
installLinkNavigation();
installActionMotion();
installFeedbackMotion();
// Some mobile WebViews resize only the visual viewport when the keyboard opens.
const viewport = window.visualViewport;
if (viewport && navigator.maxTouchPoints > 0) {
  const root = document.documentElement;
  function syncDialogViewport() {
    const obscured = viewport!.scale === 1 && root.clientHeight - viewport!.height > 100;
    root.toggleAttribute('data-v2-keyboard', obscured);
    if (obscured) {
      root.style.setProperty('--v2-visible-height', viewport!.height + 'px');
      root.style.setProperty('--v2-visible-top', viewport!.offsetTop + 'px');
    }
  }
  viewport.addEventListener('resize', syncDialogViewport);
  viewport.addEventListener('scroll', syncDialogViewport);
  syncDialogViewport();
}
document.addEventListener('click',event=>{const input=event.target instanceof HTMLInputElement?event.target:null;if(input&&!input.disabled&&!input.readOnly&&['date','time','month','datetime-local'].includes(input.type)){try{input.showPicker?.();}catch{/* Native focus remains available on unsupported browsers. */}}});
createApp(Workspace).component('LoadingIndicator', LoadingIndicator).component('UiTransition', UiTransition).mount('#app');
