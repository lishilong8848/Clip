// Teleported menus keep the invoking form's theme without changing the page.
export function inheritedControlTheme(element: HTMLElement | null): Record<string, string> {
  if (!element) return {};
  const style = getComputedStyle(element);
  return Object.fromEntries([
    ...Array.from(style).filter(name => name.startsWith("--lh-") || name.startsWith("--cf-")),
    "color-scheme",
  ].map(name => [name, style.getPropertyValue(name)]));
}
