import { ref } from "vue";

function configureSignatureContext(ctx: CanvasRenderingContext2D): CanvasRenderingContext2D {
  ctx.lineWidth = 5.5;
  ctx.lineCap = "round";
  ctx.lineJoin = "round";
  ctx.strokeStyle = "#000000";
  return ctx;
}

export function useMopSignatureCanvas() {
  const canvasRef = ref<HTMLCanvasElement | null>(null);
  const hasInk = ref(false);
  let drawing = false;
  let resizeObserver: ResizeObserver | null = null;

  function context(): CanvasRenderingContext2D | null {
    const canvas = canvasRef.value;
    if (!canvas) return null;
    const ctx = canvas.getContext("2d");
    return ctx ? configureSignatureContext(ctx) : null;
  }

  function resize(): void {
    const canvas = canvasRef.value;
    if (!canvas) return;
    const rect = canvas.getBoundingClientRect();
    const ratio = Math.max(1, Math.min(3, window.devicePixelRatio || 1));
    const width = Math.max(320, Math.floor(rect.width * ratio));
    const height = Math.max(170, Math.floor(rect.height * ratio));
    if (canvas.width === width && canvas.height === height) return;

    const previous = document.createElement("canvas");
    const previousHasInk = hasInk.value && canvas.width > 0 && canvas.height > 0;
    if (previousHasInk) {
      previous.width = canvas.width;
      previous.height = canvas.height;
      previous.getContext("2d")?.drawImage(canvas, 0, 0);
    }

    canvas.width = width;
    canvas.height = height;
    const ctx = canvas.getContext("2d");
    if (!ctx) {
      hasInk.value = false;
      return;
    }
    if (previousHasInk) {
      ctx.drawImage(previous, 0, 0, previous.width, previous.height, 0, 0, width, height);
    }
    ctx.setTransform(ratio, 0, 0, ratio, 0, 0);
    configureSignatureContext(ctx);
    hasInk.value = Boolean(previousHasInk);
  }

  function observe(): void {
    if (!canvasRef.value || !("ResizeObserver" in window)) return;
    if (resizeObserver) return;
    resizeObserver = new ResizeObserver(() => resize());
    resizeObserver.observe(canvasRef.value);
  }

  function disconnect(): void {
    resizeObserver?.disconnect();
    resizeObserver = null;
  }

  function resetInk(): void {
    hasInk.value = false;
  }

  function stopDrawing(): void {
    drawing = false;
  }

  function pointFromEvent(event: PointerEvent): { x: number; y: number } {
    const rect = canvasRef.value?.getBoundingClientRect();
    return {
      x: event.clientX - (rect?.left || 0),
      y: event.clientY - (rect?.top || 0),
    };
  }

  function startDraw(event: PointerEvent, enabled = true): void {
    event.preventDefault();
    if (!enabled) return;
    const canvas = canvasRef.value;
    const ctx = context();
    if (!canvas || !ctx) return;
    canvas.setPointerCapture?.(event.pointerId);
    drawing = true;
    hasInk.value = true;
    const point = pointFromEvent(event);
    ctx.beginPath();
    ctx.moveTo(point.x, point.y);
  }

  function moveDraw(event: PointerEvent): void {
    event.preventDefault();
    if (!drawing) return;
    const ctx = context();
    if (!ctx) return;
    const point = pointFromEvent(event);
    ctx.lineTo(point.x, point.y);
    ctx.stroke();
    hasInk.value = true;
  }

  function endDraw(event: PointerEvent): void {
    event.preventDefault();
    if (!drawing) return;
    drawing = false;
    canvasRef.value?.releasePointerCapture?.(event.pointerId);
  }

  function clear(): void {
    const canvas = canvasRef.value;
    const ctx = context();
    if (canvas && ctx) {
      ctx.clearRect(0, 0, canvas.width, canvas.height);
    }
    hasInk.value = false;
  }

  function dataUrl(): string {
    return canvasRef.value?.toDataURL("image/png") || "";
  }

  return {
    canvasRef,
    hasInk,
    resize,
    observe,
    disconnect,
    resetInk,
    stopDrawing,
    startDraw,
    moveDraw,
    endDraw,
    clear,
    dataUrl,
  };
}
