<template>
  <section class="pane admin-pressure-pane">
    <div class="pressure-form">
      <label>数量 <input :value="pressure.count" type="number" min="1" max="60" @input="updateNumber('count', $event)" /></label>
      <label>并发 <input :value="pressure.concurrency" type="number" min="1" max="10" @input="updateNumber('concurrency', $event)" /></label>
      <label class="inline-check">
        <input :checked="pressure.include_site_photos" type="checkbox" @change="updateBoolean('include_site_photos', $event)" />
        带现场照片
      </label>
      <label>每条照片数 <input :value="pressure.site_photo_count" type="number" min="1" max="3" @input="updateNumber('site_photo_count', $event)" /></label>
      <label>照片大小 KB <input :value="pressure.site_photo_kb" type="number" min="1" max="512" @input="updateNumber('site_photo_kb', $event)" /></label>
      <label>平均提交阈值 ms <input :value="pressure.max_submit_average_ms" type="number" min="1" max="10000" @input="updateNumber('max_submit_average_ms', $event)" /></label>
      <label>总耗时阈值 s <input :value="pressure.max_total_seconds" type="number" min="1" max="600" @input="updateNumber('max_total_seconds', $event)" /></label>
      <label>允许失败数 <input :value="pressure.max_failed" type="number" min="0" max="60" @input="updateNumber('max_failed', $event)" /></label>
      <label>
        场景
        <select :value="pressure.scenario" @change="updateString('scenario', $event)">
          <option value="accepted">全部正常受理</option>
          <option value="mixed">成功和失败混合</option>
          <option value="failed-network">模拟网络失败</option>
          <option value="failed-remote-missing">模拟远端记录缺失</option>
        </select>
      </label>
      <button type="button" class="btn blue" :disabled="busy" @click="$emit('run')">运行离线压测</button>
    </div>
    <section
      v-if="pressureResult.assessment"
      :class="['pressure-assessment', pressureResult.assessment.ok ? 'good' : 'bad']"
    >
      <div>
        <span>压测判定</span>
        <strong>{{ pressureResult.assessment.summary || (pressureResult.assessment.ok ? "达标" : "未达标") }}</strong>
      </div>
      <p>
        受理 {{ pressureResult.assessment.observed?.accepted ?? "-" }} /
        {{ pressureResult.assessment.observed?.count ?? "-" }}，
        失败 {{ pressureResult.assessment.observed?.failed ?? 0 }}，
        平均 {{ pressureResult.assessment.observed?.submit_average_ms ?? "-" }} ms，
        总耗时 {{ pressureResult.assessment.observed?.elapsed_seconds ?? "-" }} s
      </p>
      <ul v-if="pressureResult.assessment.failures?.length">
        <li v-for="item in pressureResult.assessment.failures" :key="item">{{ item }}</li>
      </ul>
    </section>
    <div v-if="pressureResult.site_photos" class="pressure-summary">
      <article>
        <span>照片上传</span>
        <strong>{{ pressureResult.site_photos.enabled ? "已启用" : "未启用" }}</strong>
      </article>
      <article>
        <span>预期附件数</span>
        <strong>{{ pressureResult.site_photos.expected_uploads ?? 0 }}</strong>
      </article>
      <article>
        <span>预期附件大小</span>
        <strong>{{ formatBytes(pressureResult.site_photos.expected_bytes) }}</strong>
      </article>
      <article>
        <span>提交平均耗时</span>
        <strong>{{ pressureResult.submit_average_ms ?? "-" }} ms</strong>
      </article>
    </div>
    <details class="raw-diagnostic" :open="Boolean(pressureResult.assessment)">
      <summary>查看详细压测数据</summary>
      <pre>{{ prettyJson(pressureResult) }}</pre>
    </details>
  </section>
</template>

<script setup lang="ts">
import { formatBytes, prettyJson } from "../adminToolsUtils";
import type { Dict } from "../api/client";

type PressureModel = {
  count: number;
  concurrency: number;
  scenario: string;
  include_site_photos: boolean;
  site_photo_count: number;
  site_photo_kb: number;
  max_submit_average_ms: number;
  max_total_seconds: number;
  max_failed: number;
};

defineProps<{
  pressure: PressureModel;
  pressureResult: Dict;
  busy: boolean;
}>();

const emit = defineEmits<{
  run: [];
  "update-pressure": [key: keyof PressureModel, value: number | string | boolean];
}>();

function inputTarget(event: Event): HTMLInputElement | HTMLSelectElement {
  return event.target as HTMLInputElement | HTMLSelectElement;
}

function updateNumber(key: keyof PressureModel, event: Event): void {
  emit("update-pressure", key, Number(inputTarget(event).value));
}

function updateString(key: keyof PressureModel, event: Event): void {
  emit("update-pressure", key, String(inputTarget(event).value));
}

function updateBoolean(key: keyof PressureModel, event: Event): void {
  emit("update-pressure", key, Boolean((event.target as HTMLInputElement).checked));
}
</script>

<style scoped>
.pane {
  display: grid;
  gap: 12px;
}

.pressure-form {
  display: flex;
  align-items: center;
  gap: 8px;
  flex-wrap: wrap;
  border: 1px solid rgba(216, 229, 247, 0.9);
  border-radius: 20px;
  padding: 12px;
  background: rgba(248, 251, 255, 0.76);
}

.pressure-form label {
  min-width: 160px;
}

.inline-check {
  display: inline-flex;
  grid-template-columns: none;
  align-items: center;
  gap: 6px;
  min-height: 35px;
}

.inline-check input {
  width: auto;
}

label {
  display: grid;
  gap: 5px;
  color: #475569;
  font-size: 13px;
}

input,
select {
  width: 100%;
  border: 1px solid #d8e5f7;
  border-radius: 14px;
  padding: 8px 10px;
  background: rgba(255, 255, 255, 0.9);
  color: #0f172a;
  font: inherit;
}

input:focus,
select:focus {
  border-color: #005bff;
  outline: none;
  box-shadow: 0 0 0 3px rgba(0, 91, 255, 0.14);
}

.btn,
button {
  min-height: 36px;
  border: 1px solid #c5d9f2;
  border-radius: 14px;
  padding: 8px 12px;
  background: #ffffff;
  color: #09204a;
  font-size: 14px;
  font-weight: 720;
  line-height: 1;
  cursor: pointer;
  transition: transform 0.12s ease, box-shadow 0.12s ease, border-color 0.12s ease;
}

.btn:hover:not(:disabled),
button:hover:not(:disabled) {
  border-color: #8dbbfb;
  box-shadow: 0 8px 20px rgba(27, 101, 213, 0.12);
  transform: translateY(-1px);
}

.btn.blue {
  border-color: transparent;
  background: linear-gradient(135deg, #1e63ff, #1554df);
  color: #ffffff;
  box-shadow: 0 10px 22px rgba(30, 99, 255, 0.22);
}

button:disabled {
  cursor: not-allowed;
  opacity: 0.58;
}

.pressure-summary {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
  gap: 8px;
}

.pressure-assessment,
.pressure-summary article,
.raw-diagnostic {
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  background: rgba(255, 255, 255, 0.96);
  box-shadow: 0 8px 18px rgba(0, 47, 135, 0.06);
}

.pressure-assessment {
  display: grid;
  gap: 8px;
  padding: 12px;
}

.pressure-assessment.good {
  border-color: #bbf7d0;
  background: #f0fdf4;
}

.pressure-assessment.bad {
  border-color: #fecaca;
  background: #fef2f2;
}

.pressure-assessment div {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 10px;
}

.pressure-assessment span,
.pressure-summary span {
  color: #64748b;
  font-size: 12px;
}

.pressure-assessment strong {
  color: #0f172a;
  font-size: 20px;
}

.pressure-assessment p {
  margin: 0;
}

.pressure-assessment ul {
  margin: 0;
  padding-left: 18px;
  color: #991b1b;
}

.pressure-summary article {
  padding: 10px;
}

.pressure-summary strong {
  display: block;
  margin-top: 4px;
  color: #0757d7;
  font-size: 18px;
}

.raw-diagnostic summary {
  padding: 10px 12px;
  color: #0757d7;
  cursor: pointer;
  font-size: 13px;
  font-weight: 700;
}

.raw-diagnostic pre {
  margin: 0 10px 10px;
}

pre {
  max-height: 320px;
  overflow: auto;
  padding: 12px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 18px;
  background: #071634;
  color: #e2e8f0;
  font-size: 12px;
}
</style>
