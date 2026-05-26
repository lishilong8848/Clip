<template>
  <section class="home-shell">
    <div v-if="!activeFeature" class="feature-grid">
      <article class="feature-card">
        <div>
          <span class="feature-kicker">通告工作台</span>
          <strong>维保 / 变更 / 检修</strong>
          <p>进入有权限的楼栋后，发起、更新、结束通告。</p>
        </div>
        <button class="primary" @click="activeFeature = 'workbench'">选择楼栋</button>
      </article>
      <article class="feature-card">
        <div>
          <span class="feature-kicker">外部链接</span>
          <strong>交接班审核页</strong>
          <p>按楼栋打开已配置的交接班审核页面。</p>
        </div>
        <button class="primary" @click="activeFeature = 'handover'">查看链接</button>
      </article>
    </div>

    <section v-else class="feature-section">
      <header class="feature-section__head">
        <div>
          <span class="feature-kicker">{{ activeFeature === "workbench" ? "通告工作台" : "交接班审核页" }}</span>
          <h2>{{ activeFeature === "workbench" ? "选择楼栋进入工作台" : "选择楼栋打开审核页" }}</h2>
        </div>
        <button class="secondary" @click="activeFeature = ''">返回功能选择</button>
      </header>

      <div class="scope-grid">
        <article
          v-for="scope in scopeOptions"
          :key="scope.value"
          class="scope-card"
        >
          <strong>{{ scope.label }}</strong>
          <span v-if="activeFeature === 'workbench'">{{ metricText(scope.value) }}</span>
          <span v-else>{{ handoverLinks[scope.value] ? "审核页已配置" : "暂未配置审核页链接" }}</span>
          <div class="scope-actions">
            <button
              v-if="activeFeature === 'workbench'"
              class="primary"
              @click="$emit('enter', scope.value)"
            >
              进入工作台
            </button>
            <a
              v-else-if="handoverLinks[scope.value]"
              class="primary"
              :href="handoverLinks[scope.value]"
              target="_blank"
              rel="noopener noreferrer"
            >
              打开审核页
            </a>
            <button v-else class="secondary" disabled>未配置</button>
          </div>
        </article>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { ref } from "vue";

type Dict = Record<string, any>;

const props = defineProps<{
  scopeOptions: Array<{ value: string; label: string }>;
  overview: Record<string, Dict>;
  handoverLinks: Record<string, string>;
}>();

defineEmits<{
  enter: [scope: string];
}>();

const activeFeature = ref<"" | "workbench" | "handover">("");

function normalizeScopeValue(value: string, fallback = "ALL"): string {
  const text = String(value || "").trim().toUpperCase();
  if (!text) return fallback;
  if (["ALL", "CAMPUS", "110"].includes(text)) return text;
  const match = text.match(/[ABCDEH]/);
  return match ? match[0] : fallback;
}

function metricText(scope: string): string {
  const item = props.overview[normalizeScopeValue(scope, "ALL")] || {};
  return [
    `维保 ${item.maintenance_pending || 0}`,
    `变更 ${item.change_pending || 0}`,
    `检修 ${item.repair_pending || 0}`,
    `进行中 ${(item.maintenance_ongoing || 0) + (item.change_ongoing || 0) + (item.repair_ongoing || 0)}`,
  ].join(" / ");
}
</script>

<style scoped>
.home-shell {
  padding: 24px;
}

.feature-grid,
.scope-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 14px;
}

.feature-card,
.scope-card {
  display: grid;
  gap: 14px;
  padding: 20px;
  text-align: left;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
  color: #0f172a;
}

.feature-card {
  min-height: 170px;
  align-content: space-between;
}

.feature-card strong,
.scope-card strong {
  display: block;
  margin-top: 6px;
  font-size: 22px;
  line-height: 1.3;
}

.feature-card p,
.scope-card span {
  color: #64748b;
  line-height: 1.55;
}

.feature-kicker {
  color: #2563eb;
  font-size: 13px;
  font-weight: 700;
}

.feature-section {
  display: grid;
  gap: 14px;
}

.feature-section__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 18px 20px;
  border: 1px solid #dbe3ee;
  border-radius: 8px;
  background: #ffffff;
}

.feature-section__head h2 {
  margin: 4px 0 0;
  font-size: 22px;
}

.scope-card {
  min-height: 132px;
}

.scope-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.scope-actions button,
.scope-actions a,
.feature-card button,
.feature-section__head button {
  width: fit-content;
  border: 1px solid #cbd5e1;
  border-radius: 6px;
  padding: 9px 12px;
  color: #0f172a;
  text-decoration: none;
  background: #ffffff;
  cursor: pointer;
}

.primary {
  border-color: #2563eb !important;
  background: #2563eb !important;
  color: #ffffff !important;
}

.secondary:hover:not(:disabled) {
  background: #f8fbff;
}

button:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

@media (max-width: 720px) {
  .home-shell {
    padding: 16px;
  }

  .feature-section__head {
    align-items: flex-start;
    flex-direction: column;
  }
}
</style>
