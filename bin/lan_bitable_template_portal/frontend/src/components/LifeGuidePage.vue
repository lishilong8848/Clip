<template>
  <section class="life-guide-page">
    <VnetBackButton to="/" title="返回首页" />
    <iframe ref="guideFrame" title="参考人生指南" :name="storageSeed" :srcdoc="guideHtml" sandbox="allow-scripts allow-popups allow-downloads" referrerpolicy="no-referrer" />
  </section>
</template>

<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref } from "vue";
import VnetBackButton from "./VnetBackButton.vue";
import guideHtml from "../assets/HowToLiveBetter.html?raw";

const keys = ["theme", "htlb_todos_v1", "htlb_rail_open_v1"];
const guideFrame = ref<HTMLIFrameElement | null>(null);
const saved: Record<string, string> = {};
for (const key of keys) {
  try { const value = localStorage.getItem(`clipflow-life-guide:${key}`); if (value !== null) saved[key] = value; } catch { /* Storage may be disabled. */ }
}
const storageSeed = JSON.stringify(saved);

function onGuideMessage(event: MessageEvent): void {
  if (event.source !== guideFrame.value?.contentWindow || event.data?.kind !== "clipflow-life-guide-storage") return;
  const { key, value } = event.data;
  if (!keys.includes(key) || typeof value !== "string" || value.length > 200_000) return;
  try { localStorage.setItem(`clipflow-life-guide:${key}`, value); } catch { /* Keep the current page usable without storage. */ }
}
onMounted(() => window.addEventListener("message", onGuideMessage));
onBeforeUnmount(() => window.removeEventListener("message", onGuideMessage));
</script>

<style scoped>
.life-guide-page{padding:14px 18px 20px}.life-guide-page iframe{display:block;width:100%;height:calc(100dvh - 170px);min-height:560px;border:1px solid #d9e1e8;border-radius:6px;background:#fff}@media(max-width:600px){.life-guide-page{padding:10px}.life-guide-page iframe{height:calc(100dvh - 150px);min-height:480px}}
</style>
