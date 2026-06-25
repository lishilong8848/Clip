import { createApp } from "vue";
import App from "./App.vue";
import "./global.css";

function resetLocalRuntimeStateIfRequested(): void {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  if (url.searchParams.get("reset_local") !== "1") return;
  try {
    const keys: string[] = [];
    for (let index = 0; index < window.localStorage.length; index += 1) {
      const key = window.localStorage.key(index);
      if (key && (key.startsWith("clipflow-") || key.startsWith("clipflow:"))) keys.push(key);
    }
    keys.forEach((key) => window.localStorage.removeItem(key));
  } catch {
    // 清理失败时继续加载页面，后端数据仍是唯一业务状态源。
  }
  url.searchParams.delete("reset_local");
  window.history.replaceState({}, "", url);
}

resetLocalRuntimeStateIfRequested();

createApp(App).mount("#app");
