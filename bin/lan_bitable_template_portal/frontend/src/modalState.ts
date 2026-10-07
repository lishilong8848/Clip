const owners = new Set<symbol>();
let originalOverflow = "";

export function acquireModal() {
  const owner = Symbol();
  if (!owners.size) {
    originalOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
  }
  owners.add(owner);
  return {
    isTop: (event?: Event, element?: Element | null) => Array.from(owners)[owners.size - 1] === owner
      && (!event || !isAssistantEvent(event) || !!element?.contains(event.target as Node)),
    release: () => {
      if (owners.delete(owner) && !owners.size) document.body.style.overflow = originalOverflow;
    },
  };
}

export function isAssistantEvent(event: Event): boolean {
  return event.composedPath().some(node => node instanceof Element && node.matches('.lighthouse,.lighthouse-launcher,[data-assistant-control="true"]'));
}
