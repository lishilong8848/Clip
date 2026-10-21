const handled = new WeakSet<Event>();

// Consume file events at the photo field, never at the surrounding AI composer.
export function noticePhotoFiles(event: ClipboardEvent | DragEvent): File[] {
  const transfer = 'clipboardData' in event ? event.clipboardData : event.dataTransfer;
  const files = Array.from(transfer?.files || []);
  if (!files.length) return [];
  const images = files.filter(file => file.type.startsWith('image/'));
  event.stopPropagation();
  if (event.type === 'drop' || images.length) event.preventDefault();
  if (handled.has(event)) return [];
  handled.add(event);
  return images;
}

export function focusNoticePhotoArea(event: MouseEvent, disabled = false): void {
  const target = event.target;
  if (disabled || !(target instanceof Element) || target.closest('button,a,input,label,textarea,select,[contenteditable],.n-image')) return;
  (event.currentTarget as HTMLElement | null)?.focus({ preventScroll: true });
}
