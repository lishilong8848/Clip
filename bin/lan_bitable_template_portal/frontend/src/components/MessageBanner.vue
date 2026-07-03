<template>
  <section
    class="message-banner"
    :class="tone"
    :role="tone === 'failed' || tone === 'warning' ? 'alert' : 'status'"
    :aria-live="tone === 'failed' || tone === 'warning' ? 'assertive' : 'polite'"
  >
    <span class="message-dot" aria-hidden="true"></span>
    <div>
      <strong v-if="title">{{ title }}</strong>
      <p v-if="text" :title="text">{{ text }}</p>
      <ul v-if="items.length">
        <li v-for="item in items" :key="item" :title="item">{{ item }}</li>
      </ul>
    </div>
  </section>
</template>

<script setup lang="ts">
withDefaults(defineProps<{
  tone?: "info" | "success" | "warning" | "failed";
  title?: string;
  text?: string;
  items?: string[];
}>(), {
  tone: "info",
  title: "",
  text: "",
  items: () => [],
});
</script>

<style scoped>
.message-banner {
  position: relative;
  overflow: hidden;
  display: grid;
  grid-template-columns: 10px minmax(0, 1fr);
  gap: 10px;
  align-items: start;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  padding: 10px 12px;
  color: #31516f;
  background:
    linear-gradient(135deg, rgba(248, 251, 255, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
  box-shadow: 0 8px 20px rgba(0, 47, 135, 0.06);
}

.message-banner::before {
  content: "";
  position: absolute;
  inset: 0 auto 0 0;
  width: 4px;
  background: #1e63ff;
}

.message-dot {
  position: relative;
  z-index: 1;
  width: 10px;
  height: 10px;
  margin-top: 5px;
  border-radius: 999px;
  background: #1e63ff;
  box-shadow: 0 0 0 5px rgba(30, 99, 255, 0.1);
}

.message-banner strong {
  display: block;
  margin-bottom: 2px;
  color: #0f2f6a;
  font-size: 13px;
  font-weight: 950;
}

.message-banner p,
.message-banner ul {
  margin: 0;
}

.message-banner p,
.message-banner li {
  overflow-wrap: anywhere;
  font-size: 12px;
  font-weight: 800;
  line-height: 1.45;
}

.message-banner p {
  display: -webkit-box;
  overflow: hidden;
  -webkit-box-orient: vertical;
  -webkit-line-clamp: 3;
}

.message-banner ul {
  display: grid;
  gap: 4px;
  max-height: 128px;
  overflow: auto;
  padding-left: 18px;
  padding-right: 4px;
  scrollbar-width: thin;
}

.message-banner.success {
  border-color: #bbf7d0;
  color: #047857;
  background:
    linear-gradient(135deg, rgba(240, 253, 244, 0.96), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.message-banner.success::before {
  background: #059669;
}

.message-banner.success .message-dot {
  background: #059669;
  box-shadow: 0 0 0 5px rgba(5, 150, 105, 0.1);
}

.message-banner.warning {
  border-color: #fde68a;
  color: #92400e;
  background:
    linear-gradient(135deg, rgba(255, 251, 235, 0.97), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.message-banner.warning::before {
  background: #f59e0b;
}

.message-banner.warning .message-dot {
  background: #f59e0b;
  box-shadow: 0 0 0 5px rgba(245, 158, 11, 0.12);
}

.message-banner.failed {
  border-color: #fecaca;
  color: #b91c1c;
  background:
    linear-gradient(135deg, rgba(254, 242, 242, 0.97), rgba(255, 255, 255, 0.92)),
    #ffffff;
}

.message-banner.failed::before {
  background: #e11d48;
}

.message-banner.failed .message-dot {
  background: #e11d48;
  box-shadow: 0 0 0 5px rgba(225, 29, 72, 0.1);
}
</style>
