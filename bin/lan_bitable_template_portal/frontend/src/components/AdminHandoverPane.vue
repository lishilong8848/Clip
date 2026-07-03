<template>
  <section class="pane admin-handover-pane">
    <div class="actions">
      <button class="btn blue" :disabled="busy" @click="$emit('refresh')">刷新链接</button>
      <button class="btn green" :disabled="busy" @click="$emit('save')">保存链接</button>
    </div>
    <label class="password-line">
      设置密码
      <input
        :value="password"
        type="password"
        placeholder="输入交接班链接设置密码"
        @input="$emit('update-password', inputValue($event))"
      />
    </label>
    <div class="handover-grid">
      <label v-for="scope in buildingScopes" :key="scope.value">
        {{ scope.label }}
        <input
          :value="links[scope.value] || ''"
          placeholder="https://..."
          @input="$emit('update-link', scope.value, inputValue($event))"
        />
      </label>
    </div>
  </section>
</template>

<script setup lang="ts">
defineProps<{
  buildingScopes: Array<{ value: string; label: string }>;
  links: Record<string, string>;
  password: string;
  busy: boolean;
}>();

defineEmits<{
  refresh: [];
  save: [];
  "update-password": [value: string];
  "update-link": [scope: string, value: string];
}>();

function inputValue(event: Event): string {
  return String((event.target as HTMLInputElement).value || "");
}
</script>

<style scoped>
.admin-handover-pane {
  display: grid;
  gap: 12px;
}

.actions {
  position: sticky;
  top: 126px;
  z-index: 1;
  display: flex;
  flex-wrap: wrap;
  gap: 7px;
  min-height: 50px;
  padding: 9px;
  border: 1px solid rgba(216, 229, 247, 0.92);
  border-radius: 18px;
  background:
    linear-gradient(135deg, rgba(239, 246, 255, 0.96), rgba(255, 255, 255, 0.9)),
    #ffffff;
  box-shadow: 0 10px 24px rgba(0, 47, 135, 0.07);
}

.btn,
button {
  min-height: 34px;
  border: 1px solid #cbd5e1;
  border-radius: 999px;
  padding: 7px 12px;
  background: #ffffff;
  color: #0f172a;
  font-size: 13px;
  font-weight: 720;
  line-height: 1;
  cursor: pointer;
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

.btn.green {
  border-color: transparent;
  background: #059669;
  color: #ffffff;
}

button:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.handover-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 10px;
  border: 1px solid rgba(216, 229, 247, 0.9);
  border-radius: 20px;
  padding: 12px;
  background: rgba(248, 251, 255, 0.76);
}

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 13px;
}

input {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  padding: 8px 10px;
  background: rgba(255, 255, 255, 0.9);
  color: #0f172a;
  font: inherit;
}

input:focus {
  border-color: #005bff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

.password-line {
  max-width: 360px;
}
</style>
