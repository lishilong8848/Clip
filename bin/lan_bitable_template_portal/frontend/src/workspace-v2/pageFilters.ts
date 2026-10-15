import { currentEmbedded } from './embeddedContext';
export function pageFilterTarget(): HTMLElement | null {
  const embedded=currentEmbedded();if(embedded)return embedded.filterTarget.value;
  return document.getElementById('workspace-page-filters');
}
