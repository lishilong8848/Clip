<template>
  <section class="async-page-state" :class="{ failed: Boolean(error) }" role="status">
    <span class="state-mark" aria-hidden="true"></span>
    <div class="state-copy">
      <strong>{{ error ? "页面加载失败" : "正在打开页面" }}</strong>
      <p>{{ error ? (error.message || "当前模块没有加载成功，请重试。") : "正在准备页面内容，请稍候。" }}</p>
      <small v-if="attempts && !error">已尝试 {{ attempts }} 次，仍在准备页面。</small>
    </div>
    <button v-if="error && retry" class="btn blue" type="button" @click="retry">
      重新加载
    </button>
  </section>
</template>

<script setup lang="ts">
defineProps<{
  error?: Error;
  retry?: () => void;
  attempts?: number;
}>();
</script>

<style scoped>
.async-page-state {
  display: grid;
  grid-template-columns: 14px minmax(0, 1fr) auto;
  align-items: center;
  gap: 14px;
  width: min(960px, calc(100% - 48px));
  margin: 28px auto;
  border: 1px solid rgba(171, 195, 231, 0.72);
  border-radius: 22px;
  padding: 18px 20px;
  color: #23476f;
  background:
    linear-gradient(135deg, rgba(255, 255, 255, 0.96), rgba(245, 250, 255, 0.94)),
    #ffffff;
  box-shadow: 0 16px 34px rgba(16, 65, 132, 0.09);
}

.state-mark {
  width: 12px;
  height: 12px;
  border-radius: 999px;
  background: #1e63ff;
  box-shadow: 0 0 0 7px rgba(30, 99, 255, 0.1);
}

.state-copy {
  display: grid;
  gap: 4px;
  min-width: 0;
}

.state-copy strong {
  color: #0c2d63;
  font-size: 15px;
  font-weight: 950;
}

.state-copy p {
  display: -webkit-box;
  overflow: hidden;
  overflow-wrap: anywhere;
  margin: 0;
  color: #61758b;
  font-size: 13px;
  font-weight: 800;
  line-height: 1.45;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 3;
}

.state-copy small {
  color: #7b8da4;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.4;
}

.async-page-state.failed {
  border-color: rgba(252, 165, 165, 0.76);
  color: #9f1239;
  background:
    linear-gradient(135deg, rgba(255, 247, 247, 0.97), rgba(255, 255, 255, 0.95)),
    #ffffff;
}

.async-page-state.failed .state-mark {
  background: #e11d48;
  box-shadow: 0 0 0 7px rgba(225, 29, 72, 0.11);
}

@media (max-width: 720px) {
  .async-page-state {
    grid-template-columns: 12px minmax(0, 1fr);
    width: calc(100% - 24px);
  }

  .async-page-state .btn {
    grid-column: 2;
    justify-self: start;
  }
}
</style>
