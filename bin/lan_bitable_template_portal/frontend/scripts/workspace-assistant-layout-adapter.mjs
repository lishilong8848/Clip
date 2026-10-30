// Reuse the existing overlay observer; only the V2 launcher presentation changes.
export function adaptAssistantLayout(source) {
  let code = source;
  const replace = (before, after) => {
    if (code.split(before).length !== 2) throw new Error('Assistant layout anchor changed: ' + before);
    code = code.replace(before, after);
  };
  replace('v-show="!workspaceOpen" ref="launcherLayer"', 'v-show="!workspaceOpen && (!v2BusinessModal || open)" ref="launcherLayer"');
  replace('let avoidanceFrame = 0, avoidanceHome: FooterRect | null = null;', 'const v2BusinessModal = ref(false);\nlet avoidanceFrame = 0, avoidanceHome: FooterRect | null = null;');
  replace("const BOTTOM_SUBMIT_BAR = '.drill-page .sticky-actions,.v2-learning-answerbar';", "const BOTTOM_SUBMIT_BAR = '.drill-page .sticky-actions,.v2-learning-answerbar,.v2-check-form>footer';");
  replace('const modal = native || modals[modals.length - 1];', `const modal = native || modals[modals.length - 1];
  v2BusinessModal.value = !!modal;
  // Hiding the closed launcher avoids covering fields as well as submit bars.
  // Never dismiss the assistant itself or alter its saved position/conversation.
  if (modal && !open.value) { avoidanceHome = null; footerAvoidance.value = null; return; }`);
  replace('const footer = footers[footers.length - 1];', `const footer = footers[footers.length - 1];
  v2BusinessModal.value = !!modal || !!footer;
  if (footer && !open.value) { avoidanceHome = null; footerAvoidance.value = null; return; }`);
  return code;
}
