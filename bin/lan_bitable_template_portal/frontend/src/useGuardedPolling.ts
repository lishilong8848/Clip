export function useGuardedPolling(task: () => Promise<void> | void, intervalMs: number) {
  let timer: ReturnType<typeof setInterval> | null = null;
  let inFlight = false;
  let shouldRun = false;
  let visibilityBound = false;
  let runGeneration = 0;

  function pageVisible(): boolean {
    return typeof document === "undefined" || !document.hidden;
  }

  function tick(): void {
    if (!shouldRun || !pageVisible() || inFlight) return;
    const generation = runGeneration;
    inFlight = true;
    Promise.resolve()
      .then(task)
      .catch(() => undefined)
      .finally(() => {
        if (generation === runGeneration) {
          inFlight = false;
        }
      });
  }

  function onVisibilityChange(): void {
    if (shouldRun && pageVisible()) {
      tick();
    }
  }

  function bindVisibility(): void {
    if (visibilityBound || typeof document === "undefined") return;
    document.addEventListener("visibilitychange", onVisibilityChange);
    visibilityBound = true;
  }

  function unbindVisibility(): void {
    if (!visibilityBound || typeof document === "undefined") return;
    document.removeEventListener("visibilitychange", onVisibilityChange);
    visibilityBound = false;
  }

  function stop(): void {
    shouldRun = false;
    runGeneration += 1;
    if (timer) {
      clearInterval(timer);
      timer = null;
    }
    inFlight = false;
    unbindVisibility();
  }

  function update(active: boolean): void {
    if (active) {
      const starting = !shouldRun;
      shouldRun = true;
      bindVisibility();
      if (starting || !timer) {
        tick();
      }
    }
    if (active && !timer) {
      timer = setInterval(tick, intervalMs);
    } else if (!active) {
      stop();
    }
  }

  return {
    update,
    stop,
  };
}
