<template>
  <section class="home-shell">
    <div v-if="!activeFeature" class="home-metrics">
      <article>
        <span class="metric-icon grid"></span>
        <div>
          <strong>功能入口</strong>
          <p>本页可使用功能</p>
        </div>
        <b>3 项</b>
      </article>
      <article>
        <span class="metric-icon work"></span>
        <div>
          <strong>工作台</strong>
          <p>通告管理与执行</p>
        </div>
        <b>进行中</b>
      </article>
      <article>
        <span class="metric-icon link"></span>
        <div>
          <strong>审核链接</strong>
          <p>交接班审核页面</p>
        </div>
        <b>{{ configuredHandoverCount ? "已配置" : "待配置" }}</b>
      </article>
      <article>
        <span class="metric-icon user"></span>
        <div>
          <strong>可访问入口</strong>
          <p>按当前权限显示</p>
        </div>
        <b>{{ scopeOptions.length }} 个</b>
      </article>
    </div>

    <div v-if="!activeFeature" class="feature-grid">
      <article class="feature-card">
        <div class="feature-visual workbench-visual" aria-hidden="true"></div>
        <div>
          <span class="feature-kicker">通告工作台</span>
          <strong>维保 / 变更 / 检修</strong>
          <p>进入有权限的楼栋后，发起、更新、结束通告。</p>
        </div>
        <button class="primary" @click="activeFeature = 'workbench'">选择楼栋</button>
      </article>
      <article class="feature-card">
        <div class="feature-visual link-visual" aria-hidden="true"></div>
        <div>
          <span class="feature-kicker">外部链接</span>
          <strong>交接班审核页</strong>
          <p>按楼栋打开已配置的交接班审核页面。</p>
        </div>
        <button class="primary" @click="activeFeature = 'handover'">查看链接</button>
      </article>
      <article class="feature-card">
        <div class="feature-visual mop-visual" aria-hidden="true"></div>
        <div>
          <span class="feature-kicker">工程师工具</span>
          <strong>维保 MOP 填写</strong>
          <p>选择当天维保通告，绑定 MOP 表格并预览所有 Sheet。</p>
        </div>
        <button class="primary" @click="activeFeature = 'engineer'">选择楼栋</button>
      </article>
    </div>

    <section v-else class="feature-section">
      <header class="feature-section__head">
        <div>
          <span class="feature-kicker">{{ activeFeatureKicker }}</span>
          <h2>{{ activeFeatureTitle }}</h2>
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
          <span v-else-if="activeFeature === 'engineer'">当天维保通告与 MOP 表格对应</span>
          <span v-else>{{ handoverLinks[scope.value] ? "审核页已配置" : "暂未配置审核页链接" }}</span>
          <div class="scope-actions">
            <button
              v-if="activeFeature === 'workbench'"
              class="primary"
              @click="$emit('enter', scope.value)"
            >
              进入工作台
            </button>
            <button
              v-else-if="activeFeature === 'engineer'"
              class="primary"
              @click="$emit('engineer', scope.value)"
            >
              进入 MOP 填写
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
import { computed, ref } from "vue";

type Dict = Record<string, any>;

const props = defineProps<{
  scopeOptions: Array<{ value: string; label: string }>;
  overview: Record<string, Dict>;
  handoverLinks: Record<string, string>;
}>();

defineEmits<{
  enter: [scope: string];
  engineer: [scope: string];
}>();

const activeFeature = ref<"" | "workbench" | "handover" | "engineer">("");
const configuredHandoverCount = computed(() => {
  return Object.values(props.handoverLinks || {}).filter((value) => String(value || "").trim()).length;
});
const activeFeatureKicker = computed(() => {
  if (activeFeature.value === "workbench") return "通告工作台";
  if (activeFeature.value === "engineer") return "工程师工具";
  return "交接班审核页";
});
const activeFeatureTitle = computed(() => {
  if (activeFeature.value === "workbench") return "选择楼栋进入工作台";
  if (activeFeature.value === "engineer") return "选择楼栋进入 MOP 填写";
  return "选择楼栋打开审核页";
});

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
  padding: 34px 38px 42px;
  display: grid;
  gap: 26px;
}

.home-metrics,
.feature-grid,
.scope-grid {
  display: grid;
  gap: 22px;
}

.home-metrics {
  grid-template-columns: repeat(4, minmax(0, 1fr));
}

.home-metrics article {
  position: relative;
  overflow: hidden;
  min-height: 112px;
  display: grid;
  grid-template-columns: 58px minmax(0, 1fr) auto;
  align-items: center;
  gap: 16px;
  padding: 20px 22px;
  border: 1px solid #d8e7f8;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.94);
  box-shadow: 0 18px 42px rgba(22, 78, 151, 0.11);
  transition: border-color 0.18s ease, box-shadow 0.18s ease, transform 0.18s ease;
}

.home-metrics article::after {
  content: "";
  position: absolute;
  right: -38px;
  bottom: -48px;
  width: 128px;
  height: 92px;
  pointer-events: none;
  opacity: 0.42;
  background:
    repeating-linear-gradient(0deg, rgba(22, 120, 255, 0.12) 0 1px, transparent 1px 15px),
    repeating-linear-gradient(90deg, rgba(22, 120, 255, 0.08) 0 1px, transparent 1px 15px);
  transform: rotate(-14deg);
}

.home-metrics article:hover {
  border-color: #9cc7ff;
  box-shadow: 0 22px 54px rgba(22, 78, 151, 0.15);
  transform: translateY(-1px);
}

.home-metrics article > *,
.feature-card > *,
.scope-card > * {
  position: relative;
  z-index: 1;
}

.home-metrics strong {
  display: block;
  color: #09204a;
  font-size: 17px;
}

.home-metrics p {
  margin: 5px 0 0;
  color: #64748b;
  font-size: 13px;
}

.home-metrics b {
  color: #0757d7;
  font-size: 20px;
}

.metric-icon {
  position: relative;
  display: inline-grid;
  place-items: center;
  width: 58px;
  height: 58px;
  border-radius: 14px;
  background: linear-gradient(135deg, #0757d7, #1681ff);
  box-shadow: 0 12px 26px rgba(31, 111, 231, 0.28);
}

.metric-icon::before,
.metric-icon::after {
  content: "";
  position: absolute;
  left: 50%;
  top: 50%;
  box-sizing: border-box;
  transform: translate(-50%, -50%);
}

.metric-icon.grid::before {
  width: 28px;
  height: 28px;
  background:
    linear-gradient(#ffffff, #ffffff) 0 0 / 11px 11px no-repeat,
    linear-gradient(#ffffff, #ffffff) 17px 0 / 11px 11px no-repeat,
    linear-gradient(#ffffff, #ffffff) 0 17px / 11px 11px no-repeat,
    linear-gradient(#ffffff, #ffffff) 17px 17px / 11px 11px no-repeat;
  border-radius: 7px;
}

.metric-icon.work {
  background: linear-gradient(135deg, #13a8c6, #3fd5e7);
}

.metric-icon.work::before {
  width: 30px;
  height: 24px;
  border: 3px solid #ffffff;
  border-radius: 7px;
}

.metric-icon.work::after {
  left: 50%;
  top: 15px;
  width: 16px;
  height: 9px;
  border: 3px solid #ffffff;
  border-bottom: 0;
  border-radius: 7px 7px 0 0;
  transform: translateX(-50%);
}

.metric-icon.link {
  background: linear-gradient(135deg, #2864e8, #4d95ff);
}

.metric-icon.link::before,
.metric-icon.link::after {
  width: 22px;
  height: 12px;
  border: 3px solid #ffffff;
  border-radius: 999px;
}

.metric-icon.link::before {
  transform: translate(calc(-50% - 6px), calc(-50% + 4px)) rotate(-38deg);
}

.metric-icon.link::after {
  transform: translate(calc(-50% + 6px), calc(-50% - 4px)) rotate(-38deg);
}

.metric-icon.user {
  background: linear-gradient(135deg, #1aae67, #42d68e);
}

.metric-icon.user::before {
  top: 14px;
  width: 15px;
  height: 15px;
  border: 3px solid #ffffff;
  border-radius: 999px;
  transform: translateX(-50%);
}

.metric-icon.user::after {
  top: auto;
  bottom: 13px;
  width: 28px;
  height: 16px;
  border: 3px solid #ffffff;
  border-radius: 16px 16px 6px 6px;
  transform: translateX(-50%);
}

.feature-grid {
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 28px;
}

.scope-grid {
  grid-template-columns: repeat(auto-fit, minmax(238px, 1fr));
  gap: 18px;
}

.feature-card,
.scope-card {
  position: relative;
  overflow: hidden;
  display: grid;
  gap: 16px;
  padding: 34px;
  text-align: left;
  border: 1px solid #d8e7f8;
  border-radius: 14px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.98), rgba(250, 253, 255, 0.96)),
    radial-gradient(circle at 10% 20%, rgba(30, 126, 255, 0.12), transparent 32%);
  color: #09204a;
  box-shadow: 0 24px 62px rgba(22, 78, 151, 0.14);
  transition: border-color 0.18s ease, box-shadow 0.18s ease, transform 0.18s ease;
}

.feature-card::before,
.scope-card::before {
  content: "";
  position: absolute;
  inset: auto -52px -78px auto;
  width: 280px;
  height: 170px;
  pointer-events: none;
  opacity: 0.56;
  border-radius: 50%;
  background:
    repeating-radial-gradient(ellipse at center, transparent 0 11px, rgba(22, 120, 255, 0.1) 12px 13px, transparent 14px 24px);
}

.feature-card:hover,
.scope-card:hover {
  border-color: #9cc7ff;
  box-shadow: 0 28px 72px rgba(22, 78, 151, 0.18);
  transform: translateY(-1px);
}

.feature-card {
  min-height: 336px;
  grid-template-columns: 220px minmax(0, 1fr);
  align-content: center;
  align-items: center;
  column-gap: 34px;
}

.feature-card strong,
.scope-card strong {
  display: block;
  margin-top: 14px;
  color: #061432;
  font-size: 34px;
  font-weight: 900;
  line-height: 1.25;
}

.scope-card strong {
  margin-top: 0;
  font-size: 24px;
  line-height: 1.3;
}

.feature-card p,
.scope-card span {
  margin-top: 18px;
  color: #64748b;
  line-height: 1.55;
}

.feature-kicker {
  display: inline-flex;
  align-items: center;
  min-height: 34px;
  padding: 6px 12px;
  border-radius: 8px;
  background: #eaf3ff;
  color: #0757d7;
  font-size: 13px;
  font-weight: 800;
}

.feature-card p::before {
  content: "";
  display: block;
  width: min(420px, 100%);
  height: 1px;
  margin-bottom: 20px;
  border-top: 1px dashed #d3e2f6;
}

.feature-visual {
  position: relative;
  width: 190px;
  height: 190px;
  justify-self: center;
  border-radius: 28px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.65), rgba(182, 216, 255, 0.55)),
    linear-gradient(135deg, #dcecff, #f8fbff);
  box-shadow:
    0 24px 46px rgba(34, 100, 198, 0.18),
    inset 0 1px 0 rgba(255, 255, 255, 0.9);
  transform: perspective(800px) rotateX(58deg) rotateZ(-38deg);
}

.feature-visual::before,
.feature-visual::after {
  content: "";
  position: absolute;
  background: linear-gradient(135deg, #6db3ff, #176bdf);
  box-shadow: 0 16px 30px rgba(22, 100, 217, 0.2);
}

.feature-visual::before {
  left: 54px;
  top: -42px;
  width: 82px;
  height: 118px;
  border-radius: 16px;
  transform: rotateZ(38deg) rotateX(-58deg);
}

.feature-visual::after {
  left: 82px;
  top: -8px;
  width: 34px;
  height: 34px;
  border-radius: 10px;
  background:
    linear-gradient(#fff, #fff) 8px 8px / 8px 8px no-repeat,
    linear-gradient(#fff, #fff) 19px 8px / 8px 8px no-repeat,
    linear-gradient(#fff, #fff) 8px 20px / 8px 8px no-repeat,
    linear-gradient(#fff, #fff) 19px 20px / 8px 8px no-repeat,
    linear-gradient(135deg, #246eea, #66b2ff);
  transform: rotateZ(38deg) rotateX(-58deg);
}

.link-visual::after {
  border-radius: 50%;
  background:
    linear-gradient(45deg, transparent 43%, #fff 44% 56%, transparent 57%),
    linear-gradient(-45deg, transparent 43%, #fff 44% 56%, transparent 57%),
    linear-gradient(135deg, #246eea, #66b2ff);
}

.mop-visual::before {
  background: linear-gradient(135deg, #0891b2, #00b7d7);
}

.mop-visual::after {
  background:
    linear-gradient(#fff, #fff) 8px 10px / 20px 4px no-repeat,
    linear-gradient(#fff, #fff) 8px 19px / 20px 4px no-repeat,
    linear-gradient(#fff, #fff) 8px 28px / 14px 4px no-repeat,
    linear-gradient(135deg, #0891b2, #00b7d7);
}

.feature-section {
  display: grid;
  gap: 18px;
}

.feature-section__head {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 22px 24px;
  border: 1px solid #d8e7f8;
  border-radius: 14px;
  background: rgba(255, 255, 255, 0.95);
  box-shadow: 0 18px 42px rgba(22, 78, 151, 0.1);
}

.feature-section__head h2 {
  margin: 4px 0 0;
  color: #09204a;
  font-size: 24px;
}

.scope-card {
  min-height: 156px;
  padding: 22px;
  align-content: space-between;
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
  min-height: 42px;
  border: 1px solid #c5d9f2;
  border-radius: 10px;
  padding: 10px 16px;
  color: #09204a;
  font-weight: 800;
  text-decoration: none;
  background: #ffffff;
  cursor: pointer;
  box-shadow: 0 8px 20px rgba(22, 78, 151, 0.08);
  transition: border-color 0.16s ease, background 0.16s ease, box-shadow 0.16s ease, transform 0.16s ease;
}

.primary {
  border-color: transparent !important;
  background: linear-gradient(135deg, #0757d7, #1678ff) !important;
  color: #ffffff !important;
  box-shadow: 0 14px 30px rgba(20, 103, 226, 0.28) !important;
}

.primary:hover:not(:disabled) {
  box-shadow: 0 16px 34px rgba(20, 103, 226, 0.34) !important;
  transform: translateY(-1px);
}

.secondary:hover:not(:disabled) {
  border-color: #8dbbfb;
  background: #f6fbff;
  transform: translateY(-1px);
}

button:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

/* Softer rounded VNET surfaces */
.home-metrics article,
.feature-card,
.scope-card,
.feature-section__head {
  border-radius: 22px;
}

.home-metrics strong,
.feature-card strong,
.scope-card strong,
.feature-section__head h2 {
  letter-spacing: 0;
  text-wrap: balance;
}

.home-metrics strong {
  font-weight: 780;
}

.home-metrics p,
.feature-card p,
.scope-card span {
  color: #5f7189;
}

.home-metrics b {
  padding: 5px 10px;
  border: 1px solid #d8e7f8;
  border-radius: 999px;
  background: rgba(234, 243, 255, 0.78);
  color: #0b5ed8;
  font-size: 18px;
  font-weight: 780;
  line-height: 1.2;
  white-space: nowrap;
}

.feature-card strong {
  font-size: clamp(28px, 2.2vw, 32px);
  font-weight: 850;
}

.scope-card strong {
  font-weight: 820;
}

.feature-kicker {
  background: rgba(234, 243, 255, 0.84);
  color: #0b5ed8;
  font-weight: 750;
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.76);
}

.scope-actions button,
.scope-actions a,
.feature-card button,
.feature-section__head button {
  font-weight: 760;
}

.metric-icon {
  border-radius: 17px;
}

.feature-kicker,
.scope-actions button,
.scope-actions a,
.feature-card button,
.feature-section__head button {
  border-radius: 13px;
}

.feature-visual {
  border-radius: 36px;
}

.feature-visual::before {
  border-radius: 20px;
}

.feature-visual::after {
  border-radius: 12px;
}

/* Panorama construction-management polish */
.home-shell {
  padding: 36px 42px 48px;
}

.home-metrics {
  gap: 20px;
}

.home-metrics article {
  min-height: 104px;
  border-color: rgba(207, 224, 255, 0.92);
  border-radius: 24px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(250, 253, 255, 0.96)),
    radial-gradient(circle at 12% 18%, rgba(48, 128, 255, 0.08), transparent 28%);
  box-shadow: 0 14px 34px rgba(20, 70, 138, 0.09);
}

.home-metrics article:hover {
  box-shadow: 0 18px 42px rgba(20, 70, 138, 0.12);
}

.metric-icon {
  border-radius: 20px;
  box-shadow: 0 13px 28px rgba(31, 111, 231, 0.22);
}

.feature-grid {
  gap: 30px;
}

.feature-card,
.scope-card,
.feature-section__head {
  border-color: rgba(207, 224, 255, 0.94);
  border-radius: 26px;
  background:
    linear-gradient(180deg, rgba(255, 255, 255, 0.99), rgba(250, 253, 255, 0.97)),
    radial-gradient(circle at 10% 20%, rgba(48, 128, 255, 0.09), transparent 34%);
  box-shadow: 0 16px 38px rgba(20, 70, 138, 0.1);
}

.feature-card:hover,
.scope-card:hover {
  box-shadow: 0 22px 50px rgba(20, 70, 138, 0.14);
}

.feature-card {
  min-height: 326px;
}

.feature-card strong {
  color: #071634;
  font-size: clamp(28px, 2.1vw, 31px);
  font-weight: 820;
}

.feature-card p::before {
  border-top-color: #dbe6f5;
}

.feature-kicker {
  border: 1px solid #d8e7f8;
  border-radius: 14px;
  background: rgba(239, 246, 255, 0.92);
  color: #155dfc;
}

.feature-visual {
  border-radius: 38px;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.76), rgba(190, 219, 255, 0.58)),
    linear-gradient(135deg, #e7f1ff, #fbfdff);
  box-shadow:
    0 22px 44px rgba(34, 100, 198, 0.16),
    inset 0 1px 0 rgba(255, 255, 255, 0.9);
}

.scope-card {
  min-height: 150px;
}

.scope-card strong {
  color: #071634;
}

.scope-actions button,
.scope-actions a,
.feature-card button,
.feature-section__head button {
  border-radius: 15px;
  box-shadow: 0 10px 22px rgba(15, 86, 228, 0.08);
}

.primary {
  background: linear-gradient(135deg, #155dfc, #3080ff) !important;
  box-shadow: 0 14px 28px rgba(21, 93, 252, 0.24) !important;
}

/* Panorama construction-management command-center skin */
.home-shell {
  width: min(1800px, 100%);
  margin: 0 auto;
  padding: 28px 32px 42px;
}

.home-metrics,
.feature-grid,
.scope-grid {
  gap: 20px;
}

.home-metrics article,
.feature-card,
.scope-card,
.feature-section__head {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.84);
  box-shadow: 0 12px 30px rgba(0, 47, 135, 0.08);
  backdrop-filter: blur(10px);
}

.home-metrics article::after,
.feature-card::before,
.scope-card::before {
  opacity: 0;
}

.home-metrics strong,
.feature-card strong,
.scope-card strong,
.feature-section__head h2 {
  color: #0f172a;
  font-weight: 650;
}

.home-metrics p,
.feature-card p,
.scope-card span {
  color: #64748b;
}

.home-metrics b,
.feature-kicker {
  border-color: #cfe0ff;
  background: rgba(239, 246, 255, 0.86);
  color: #005bff;
}

.metric-icon {
  border-radius: 16px;
  background: linear-gradient(135deg, #1e63ff, #005bff);
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.metric-icon.work {
  background: linear-gradient(135deg, #0891b2, #00b7d7);
}

.metric-icon.user {
  background: linear-gradient(135deg, #059669, #10b981);
}

.feature-card {
  min-height: 224px;
  grid-template-columns: 112px minmax(0, 1fr);
  align-content: center;
  column-gap: 24px;
}

.feature-card strong {
  margin-top: 10px;
  font-size: 28px;
}

.feature-card p {
  margin-top: 12px;
}

.feature-card p::before {
  margin-bottom: 14px;
  border-top-color: #d8e5f7;
}

.feature-visual {
  width: 96px;
  height: 96px;
  border-radius: 24px;
  transform: none;
  background: linear-gradient(135deg, #eff6ff, #ffffff);
  box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.9), 0 14px 28px rgba(0, 47, 135, 0.1);
}

.feature-visual::before {
  left: 25px;
  top: 18px;
  width: 46px;
  height: 56px;
  border-radius: 12px;
  transform: none;
  background: linear-gradient(135deg, #1e63ff, #005bff);
}

.feature-visual::after {
  left: 38px;
  top: 34px;
  transform: none;
}

.scope-actions button,
.scope-actions a,
.feature-card button,
.feature-section__head button {
  border-color: #d8e5f7;
  background: rgba(255, 255, 255, 0.88);
}

.primary {
  background: linear-gradient(135deg, #1e63ff, #1554df) !important;
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.24) !important;
}

.primary:hover:not(:disabled) {
  background: #1554df !important;
}

@media (max-width: 720px) {
  .home-shell {
    padding: 18px;
  }

  .home-metrics,
  .feature-grid {
    grid-template-columns: 1fr;
  }

  .feature-card {
    grid-template-columns: 1fr;
    min-height: auto;
  }

  .feature-visual {
    width: 132px;
    height: 132px;
  }

  .feature-section__head {
    align-items: flex-start;
    flex-direction: column;
  }
}

@media (max-width: 1320px) {
  .feature-grid {
    grid-template-columns: 1fr;
  }
}
</style>
