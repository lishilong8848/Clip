// Card whitespace opens details; nested controls retain their own action.
export function activateCard(event:MouseEvent):boolean {
  if (event.defaultPrevented || event.button !== 0 || event.ctrlKey || event.metaKey || event.altKey || event.shiftKey) return false;
  const target=event.target, card=event.currentTarget;
  if (!(target instanceof Element) || !(card instanceof Element)) return false;
  if (target.closest('button,a,input,select,textarea,label,summary,[role="button"],[role="combobox"],[contenteditable="true"]')) return false;
  const selection=window.getSelection();
  return !selection?.toString() || !card.contains(selection.anchorNode) && !card.contains(selection.focusNode);
}
