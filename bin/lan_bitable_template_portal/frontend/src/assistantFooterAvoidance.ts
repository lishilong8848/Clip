export type FooterRect = { left: number; top: number; right: number; bottom: number };
export type LauncherPoint = { x: number; y: number };

export function footerSafePosition(home: FooterRect, footer: FooterRect | null, viewport: FooterRect, gap = 12): LauncherPoint | null {
  if (!footer) return null;
  const width = home.right - home.left, height = home.bottom - home.top;
  const minX = viewport.left + 12, minY = viewport.top + 12;
  const maxX = viewport.right - width - 12, maxY = viewport.bottom - height - 12;
  if (width <= 0 || height <= 0 || maxX < minX || maxY < minY) return null;
  const clamp = (x: number, y: number) => ({ x: Math.min(maxX, Math.max(minX, x)), y: Math.min(maxY, Math.max(minY, y)) });
  const blocked = ({ x, y }: LauncherPoint) => x < footer.right + gap && x + width > footer.left - gap
    && y < footer.bottom + gap && y + height > footer.top - gap;
  const origin = clamp(home.left, home.top);
  if (!blocked(origin)) return null;
  // Pick the nearest free edge; do not shift an entire page or reserve empty footer space.
  return [clamp(origin.x, footer.top - height - gap), clamp(footer.left - width - gap, origin.y),
    clamp(footer.right + gap, origin.y), clamp(origin.x, footer.bottom + gap)]
    .filter(point => !blocked(point))
    .sort((a, b) => Math.hypot(a.x - origin.x, a.y - origin.y) - Math.hypot(b.x - origin.x, b.y - origin.y))[0] || null;
}

export function temporaryLauncherPlacement<T extends { snap_back: boolean }>(appearance: T, position: LauncherPoint | null, temporary: LauncherPoint | null) {
  return { appearance: temporary ? { ...appearance, snap_back: false } : appearance, position: temporary || position };
}
