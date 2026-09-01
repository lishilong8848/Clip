<template>
  <article class="morning-sheet" :style="{ '--morning-content-size': `${contentFontSize}px` }">
    <img class="morning-logo" src="/assets/vnet-logo.png" alt="世纪互联" />
    <table aria-label="EA118 H楼晨会">
      <colgroup>
        <col class="column-a" /><col class="column-b" /><col class="column-c" /><col class="column-d" />
        <col class="column-e" /><col class="column-f" /><col class="column-g" /><col class="column-h" />
      </colgroup>
      <tbody>
        <tr class="title-row"><th colspan="8">EA118-H楼晨会</th></tr>
        <tr class="weather-row">
          <th>日期</th><td>{{ dateText }}</td>
          <th>天气</th><td>{{ model.weather_condition || "" }}</td>
          <th>干球温度（℃）</th><td>{{ temperatureText(model.dry_bulb_temperature) }}</td>
          <th>湿球温度（℃）</th><td>{{ temperatureText(model.wet_bulb_temperature) }}</td>
        </tr>
        <tr class="heading-row"><th>楼号</th><th colspan="7"></th></tr>
        <tr v-for="row in rows" :key="row.scope" class="content-row">
          <th>{{ row.label }}</th>
          <td colspan="7">
            <div v-for="(line, index) in row.lines" :key="`${row.scope}-${index}`">
              {{ index + 1 }}、{{ line }}
            </div>
          </td>
        </tr>
      </tbody>
    </table>
  </article>
</template>

<script setup lang="ts">
import { computed } from "vue";

type Dict = Record<string, any>;

const props = defineProps<{ model: Dict }>();

const rows = computed(() => Array.isArray(props.model.rows) ? props.model.rows : []);
const dateText = computed(() => {
  const match = String(props.model.date || "").match(/^(\d{4})-(\d{2})-(\d{2})$/);
  return match ? `${match[1]}年${Number(match[2])}月${Number(match[3])}日` : "";
});
const contentFontSize = computed(() => {
  const count = rows.value.reduce((total: number, row: Dict) => total + (Array.isArray(row.lines) ? row.lines.length : 0), 0);
  return Math.max(6, Math.min(11, 11 - Math.max(0, count - 18) * 0.08));
});

function temperatureText(value: unknown): string {
  if (value === null || value === undefined || value === "") return "";
  const number = Number(value);
  return Number.isFinite(number) ? number.toFixed(1) : "";
}
</script>

<style scoped>
.morning-sheet {
  position: relative;
  width: 277mm;
  height: 190mm;
  overflow: hidden;
  background: #fff;
  color: #000;
  font-family: "SimSun", "宋体", serif;
}
.morning-logo {
  position: absolute;
  z-index: 2;
  top: 4mm;
  left: 5mm;
  width: 66mm;
  height: auto;
  pointer-events: none;
}
table { width: 100%; height: 100%; border-collapse: collapse; table-layout: fixed; }
.column-a { width: 7.7%; }
.column-b { width: 11.4%; }
.column-c { width: 5.4%; }
.column-d { width: 7.9%; }
.column-e { width: 15.2%; }
.column-f { width: 14.6%; }
.column-g { width: 13.8%; }
.column-h { width: 24%; }
th, td { border: .2mm solid #000; padding: 1.2mm; vertical-align: middle; }
th { font-weight: 700; text-align: center; }
.title-row { height: 11mm; }
.title-row th { font-size: 14pt; }
.weather-row { height: 7mm; font-size: 10pt; }
.heading-row { height: 9mm; background: #d6e3bc; font-size: 10pt; }
.content-row th { width: 21mm; font-size: 10pt; }
.content-row td { padding-left: 2mm; font-size: var(--morning-content-size); line-height: 1.28; text-align: left; white-space: normal; overflow-wrap: anywhere; }
.content-row td div + div { margin-top: .4mm; }
</style>
