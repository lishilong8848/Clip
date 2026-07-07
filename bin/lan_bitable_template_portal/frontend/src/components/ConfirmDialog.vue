<template>
  <div v-if="open" class="confirm-backdrop" @click.self="emit('resolve', false)">
    <section
      class="confirm-modal"
      :class="`tone-${tone}`"
      role="dialog"
      aria-modal="true"
      aria-labelledby="confirm-dialog-title"
    >
      <div class="confirm-icon" aria-hidden="true"></div>
      <div class="confirm-content">
        <header>
          <div>
            <span>{{ kicker || "操作确认" }}</span>
            <strong id="confirm-dialog-title">{{ title }}</strong>
          </div>
          <button type="button" class="confirm-close" aria-label="关闭确认弹窗" @click="emit('resolve', false)">×</button>
        </header>
        <p>{{ message }}</p>
        <ul v-if="details.length">
          <li v-for="item in details" :key="item">{{ item }}</li>
        </ul>
        <footer>
          <button type="button" class="btn ghost" @click="emit('resolve', false)">
            {{ cancelLabel || "取消" }}
          </button>
          <button type="button" class="btn" :class="confirmClass || defaultConfirmClass" @click="emit('resolve', true)">
            {{ confirmLabel || "确认" }}
          </button>
        </footer>
      </div>
    </section>
  </div>
</template>

<script setup lang="ts">
import { computed } from "vue";

const props = withDefaults(defineProps<{
  open: boolean;
  tone?: "danger" | "warning" | "primary";
  kicker?: string;
  title: string;
  message: string;
  details?: string[];
  confirmLabel?: string;
  cancelLabel?: string;
  confirmClass?: string;
}>(), {
  tone: "primary",
  kicker: "操作确认",
  details: () => [],
  confirmLabel: "确认",
  cancelLabel: "取消",
  confirmClass: "",
});

const emit = defineEmits<{
  resolve: [confirmed: boolean];
}>();

const defaultConfirmClass = computed(() => (
  props.tone === "danger" ? "danger" : props.tone === "warning" ? "green" : "blue"
));
</script>

<style scoped>
.confirm-backdrop {
  position: fixed;
  inset: 0;
  z-index: var(--cf-z-confirm, 900);
  display: grid;
  place-items: center;
  padding: 24px;
  background: rgba(9, 32, 74, 0.42);
  backdrop-filter: blur(8px);
}

.confirm-modal {
  display: grid;
  grid-template-columns: 48px minmax(0, 1fr);
  gap: 16px;
  width: min(540px, calc(100vw - 40px));
  border: 1px solid #d8e5f7;
  border-radius: 24px;
  background:
    linear-gradient(135deg, rgba(248, 251, 255, 0.98), rgba(255, 255, 255, 0.98)),
    #fff;
  padding: 20px;
  box-shadow: 0 28px 80px rgba(0, 47, 135, 0.24);
}

.confirm-icon {
  position: relative;
  display: grid;
  width: 48px;
  height: 48px;
  place-items: center;
  border-radius: 18px;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  box-shadow: 0 12px 26px rgba(30, 99, 255, 0.22);
}

.confirm-icon::before,
.confirm-icon::after {
  content: "";
  position: absolute;
  border-radius: 999px;
  background: currentColor;
}

.confirm-icon::before {
  width: 5px;
  height: 18px;
  top: 12px;
}

.confirm-icon::after {
  width: 5px;
  height: 5px;
  bottom: 11px;
}

.tone-primary .confirm-icon::before {
  width: 18px;
  height: 5px;
  top: 21px;
}

.tone-primary .confirm-icon::after {
  width: 5px;
  height: 18px;
  bottom: auto;
  top: 14px;
}

.tone-danger .confirm-icon {
  background: linear-gradient(135deg, #ef4444, #be123c);
  box-shadow: 0 12px 26px rgba(225, 29, 72, 0.22);
}

.tone-warning .confirm-icon {
  background: linear-gradient(135deg, #f59e0b, #d97706);
  box-shadow: 0 12px 26px rgba(217, 119, 6, 0.2);
}

.confirm-content {
  min-width: 0;
}

.confirm-content header {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.confirm-content span {
  display: block;
  color: #155dfc;
  font-size: 12px;
  font-weight: 900;
}

.tone-danger .confirm-content span {
  color: #e11d48;
}

.tone-warning .confirm-content span {
  color: #b45309;
}

.confirm-content strong {
  display: block;
  margin-top: 5px;
  color: #09204a;
  font-size: 19px;
  line-height: 1.35;
}

.confirm-content p {
  margin: 12px 0 0;
  color: #334155;
  line-height: 1.65;
}

.confirm-content ul {
  display: grid;
  gap: 8px;
  margin: 12px 0 0;
  padding-left: 18px;
  color: #64748b;
  font-size: 14px;
  line-height: 1.55;
}

.confirm-close {
  display: inline-grid;
  width: 34px;
  height: 34px;
  flex: 0 0 auto;
  place-items: center;
  border: 1px solid #d8e5f7;
  border-radius: 999px;
  background: #ffffff;
  color: #64748b;
  font: inherit;
  font-size: 20px;
  line-height: 1;
  cursor: pointer;
}

.confirm-content footer {
  display: flex;
  flex-wrap: wrap;
  justify-content: flex-end;
  gap: 10px;
  margin-top: 18px;
  padding-top: 14px;
  border-top: 1px solid #e5edf8;
}

.btn {
  min-height: 40px;
  border: 1px solid #d8e5f7;
  border-radius: 15px;
  padding: 0 16px;
  color: #1d4ed8;
  background: #ffffff;
  font: inherit;
  font-weight: 900;
  cursor: pointer;
  transition:
    transform 0.16s ease,
    box-shadow 0.16s ease,
    border-color 0.16s ease,
    background 0.16s ease;
}

.btn:hover {
  transform: translateY(-1px);
  box-shadow: 0 10px 20px rgba(15, 86, 228, 0.1);
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #fff;
}

.btn.green {
  border-color: transparent;
  background: #059669;
  color: #fff;
}

.btn.danger {
  border-color: transparent;
  background: #e11d48;
  color: #fff;
}

.btn.ghost {
  background: #f8fbff;
  color: #33526f;
}

@media (max-width: 560px) {
  .confirm-modal {
    grid-template-columns: 1fr;
  }

  .confirm-icon {
    width: 42px;
    height: 42px;
    border-radius: 15px;
  }
}
</style>
