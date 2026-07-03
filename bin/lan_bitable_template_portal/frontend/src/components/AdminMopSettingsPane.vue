<template>
  <section class="pane admin-mop-settings-pane">
    <div class="actions">
      <button class="btn blue" :disabled="busy" @click="$emit('refresh')">刷新配置</button>
      <button class="btn green" :disabled="busy" @click="$emit('save')">保存配置</button>
    </div>
    <div class="mop-settings-grid">
      <label v-for="field in fields" :key="field.key">
        {{ field.label }}
        <input
          :value="settings[field.key] || ''"
          :placeholder="field.placeholder"
          @input="$emit('update-field', field.key, inputValue($event))"
        />
      </label>
    </div>
  </section>
</template>

<script setup lang="ts">
type MopSettingsKey = "mop_app_token" | "mop_table_id" | "mop_view_id" | "mop_title_field" | "mop_attachment_field";

defineProps<{
  settings: Record<MopSettingsKey, string>;
  busy: boolean;
}>();

defineEmits<{
  refresh: [];
  save: [];
  "update-field": [key: MopSettingsKey, value: string];
}>();

const fields: Array<{ key: MopSettingsKey; label: string; placeholder: string }> = [
  { key: "mop_app_token", label: "MOP app_token", placeholder: "维保MOP所在多维表 app_token" },
  { key: "mop_table_id", label: "MOP table_id", placeholder: "维保MOP所在表 table_id" },
  { key: "mop_view_id", label: "MOP view_id", placeholder: "可选，默认读取 MOP 文件视图" },
  { key: "mop_title_field", label: "标题字段名", placeholder: "文件名" },
  { key: "mop_attachment_field", label: "附件字段名", placeholder: "文件" },
];

function inputValue(event: Event): string {
  return String((event.target as HTMLInputElement).value || "");
}
</script>

<style scoped>
.admin-mop-settings-pane {
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

.mop-settings-grid {
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
</style>
