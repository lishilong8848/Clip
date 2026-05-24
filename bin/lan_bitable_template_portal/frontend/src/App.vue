<template>
  <main class="shell">
    <section class="panel">
      <p class="eyebrow">Vue migration workspace</p>
      <h1>南通基地-运维灯塔工作台</h1>
      <p>
        当前生产页面仍由 legacy index.html 提供。此工程用于分阶段迁移到
        Vue 3 + 虚拟列表，避免一次性替换影响现场使用。
      </p>
      <VirtualNoticeList :rows="previewRows" />
    </section>
  </main>
</template>

<script setup lang="ts">
import VirtualNoticeList, { type NoticeRow } from "./components/VirtualNoticeList.vue";

const previewRows: NoticeRow[] = [
  {
    id: "maintenance-preview",
    type: "维保",
    title: "A楼月度维保通告",
    meta: "SQLite 快照 / 待发起事项",
    status: "待发起",
  },
  {
    id: "change-preview",
    type: "变更",
    title: "园区网络设备变更通告",
    meta: "后端 job / SSE 状态",
    status: "排队中",
  },
  {
    id: "repair-preview",
    type: "检修",
    title: "B楼冷源设备检修通告",
    meta: "虚拟列表仅渲染可见行",
    status: "进行中",
  },
];
</script>

<style scoped>
.shell {
  min-height: 100vh;
  display: grid;
  place-items: center;
  background: #f3f4f6;
  color: #111827;
  font-family: "Microsoft YaHei", system-ui, sans-serif;
}

.panel {
  width: min(720px, calc(100vw - 32px));
  padding: 28px;
  border: 1px solid #d1d5db;
  border-radius: 8px;
  background: #ffffff;
}

.eyebrow {
  margin: 0 0 10px;
  color: #2563eb;
  font-size: 13px;
  font-weight: 700;
  letter-spacing: 0;
}

h1 {
  margin: 0 0 14px;
  font-size: 28px;
  line-height: 1.25;
}

p {
  margin: 0 0 18px;
  color: #4b5563;
  line-height: 1.7;
}
</style>
