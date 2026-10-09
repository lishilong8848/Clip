<template>
  <div class="lhs" :class="{ disabled }" :aria-busy="!!field.directory_loading">
    <label class="lhs-exempt"><input type="checkbox" :checked="local.exempt" :disabled="disabled" :aria-label="exemptLabel" @change="onExempt" /><span>{{ exemptLabel }}</span></label>
    <p v-if="!local.exempt && !validScope" class="lhs-error" role="alert">请在上方通告中仅选择一栋楼</p>
    <template v-if="!local.exempt && validScope">
      <p v-if="field.directory_loading" class="lhs-loading" role="status"><Loader2 :size="15" class="lhs-spin" />正在读取工单和人员…</p>
      <p v-else-if="field.directory_error" class="lhs-error" role="alert">{{ field.directory_error }}</p>
      <section v-if="directoryReady" class="lhs-grid">
        <section class="lhs-block lhs-sop" aria-label="工单SOP">
          <div class="lhs-h">
            <label class="lhs-label" :for="id + '-sop'">工单SOP<span class="lhs-req" aria-hidden="true">*</span></label>
            <button type="button" class="lhs-icon-btn" :disabled="disabled" aria-label="重新加载目录" title="重新加载目录" @click="refreshOptions"><RefreshCw :size="15" aria-hidden="true" /></button>
          </div>
          <select :id="id + '-sop'" ref="sopRef" class="lhs-input" :value="local.sop_id" :disabled="disabled" required :aria-label="'工单SOP'" @change="selectSop">
            <option value="">请选择SOP</option>
            <option v-for="o in sopOptions" :key="o.value" :value="o.value" :disabled="!!o.sop.blocked_reason">{{ o.label }}</option>
          </select>
          <p v-if="!sopOptions.length" class="lhs-empty">当前楼栋暂无此类工单 SOP</p>
          <p v-if="staleVersion" class="lhs-blocked" role="alert">所选 SOP 已更新，请核对步骤后<button type="button" class="lhs-quick" :disabled="disabled" @click="acceptVersion">采用当前版本</button></p>
          <span v-if="selectedSop && selectedSop.blocked_reason" class="lhs-blocked"><Info :size="14" aria-hidden="true" /><span>{{ selectedSop.blocked_reason }}</span></span>
          <div v-if="selectedSop" class="lhs-sop-detail">
            <button type="button" class="lhs-detail-toggle" :aria-expanded="showSteps" :aria-controls="id + '-steps'" @click="showSteps = !showSteps"><span>{{ sopSteps.length }} 个步骤<span v-if="sopAttachments.length"> · {{ sopAttachments.length }} 份附件</span></span><ChevronUp v-if="showSteps" :size="14" /><ChevronDown v-else :size="14" /></button>
            <UiTransition name="lhs-detail"><div v-if="showSteps" :id="id + '-steps'" class="lhs-detail-wrap"><div>
            <ol v-if="sopSteps.length" class="lhs-steps">
              <li v-for="(step, si) in sopSteps" :key="si" class="lhs-step">
                <span class="lhs-step-txt">{{ si + 1 }}. {{ stepText(step) }}<small v-for="(rule, ri) in step.repeat_rules || []" :key="ri" class="lhs-loop">{{ repeatLabel(rule) }}</small></span>
                <span v-if="Number(step.delay_reminder_minutes) > 0" class="lhs-delay" :title="'延时提醒 ' + step.delay_reminder_minutes + ' 分钟'"><Clock3 :size="13" aria-hidden="true" />{{ step.delay_reminder_minutes }}分钟</span>
              </li>
            </ol>
            <p v-else class="lhs-empty">该SOP暂无步骤</p>
            <div v-if="sopAttachments.length" class="lhs-attach"><span class="lhs-label">附件</span><ul class="lhs-attach-list"><li v-for="(at, ai) in sopAttachments" :key="ai">{{ at.name }}</li></ul></div>
            </div></div></UiTransition>
          </div>
        </section>

        <section class="lhs-block lhs-people" aria-label="人员">
          <div class="lhs-h">
            <span class="lhs-label">人员<span class="lhs-req" aria-hidden="true">*</span></span>
            <div class="lhs-query">
              <input :id="id + '-people-query'" type="search" v-model.trim="peopleQuery" :disabled="disabled" placeholder="检索人员" aria-label="检索人员" @keydown.enter.prevent="searchPeople" />
              <button type="button" class="lhs-icon-btn" :disabled="disabled || !peopleQuery" aria-label="搜索人员" title="搜索人员" @click="searchPeople"><Search :size="15" aria-hidden="true" /></button>
            </div>
          </div>
          <div class="lhs-people-cols">
            <div class="lhs-pick">
              <label class="lhs-label" :for="id + '-operator'">操作人<span class="lhs-req" aria-hidden="true">*</span></label>
              <VnetSelect :input-id="id + '-operator'" :label="'操作人'" :options="operatorLabels" :model-value="operatorLabel" :disabled="disabled" placeholder="请选择操作人" :menu-z-index="10010" required @update:model-value="selectOperator" />
            </div>
            <div class="lhs-pick">
              <div class="lhs-pick-head">
                <label class="lhs-label" :for="id + '-reviewer'">现场审核人<span class="lhs-req" aria-hidden="true">*</span></label>
                <button v-if="hDuty" type="button" class="lhs-quick" :disabled="disabled || String(local.operator_record_id || '') === String(hDuty.record_id || '')" title="设为H楼值班账号" @click="setHReviewer">H楼值班账号</button>
              </div>
              <VnetSelect :input-id="id + '-reviewer'" :label="'现场审核人'" :options="reviewerLabels" :model-value="reviewerLabel" :disabled="disabled" placeholder="请选择现场审核人" :menu-z-index="10010" required @update:model-value="selectReviewer" />
            </div>
          </div>
        </section>

        <section v-if="showRuns" class="lhs-block lhs-runs" aria-label="轮巡/制冷">
          <template v-if="isPolling">
            <div class="lhs-runs-count"><label class="lhs-label" :for="id + '-run-count'">轮巡次数</label><select :id="id + '-run-count'" class="lhs-input" :value="runCount" :disabled="disabled" required @change="onRunCount"><option :value="1">1</option><option :value="2">2</option></select></div>
            <div v-for="(run, i) in displayRuns" :key="i" class="lhs-run">
              <div class="lhs-run-field"><label class="lhs-label" :for="id + '-run-' + i + '-from'">第{{ i + 1 }}次起点</label><select :id="id + '-run-' + i + '-from'" class="lhs-input" :value="String(run.from_unit || '')" :disabled="disabled" required @change="onPollingFrom(i, $event)"><option value="">请选择</option><option v-for="u in fromOptions(i)" :key="u" :value="u">{{ u }}</option></select></div>
              <div class="lhs-run-field"><label class="lhs-label" :for="id + '-run-' + i + '-to'">第{{ i + 1 }}次终点</label><select :id="id + '-run-' + i + '-to'" class="lhs-input" :value="String(run.to_unit || '')" :disabled="disabled" required @change="onPollingTo(i, $event)"><option value="">请选择</option><option v-for="u in toOptions(i)" :key="u" :value="u">{{ u }}</option></select></div>
            </div>
          </template>
          <template v-else-if="isCooling">
            <div class="lhs-cool-block">
              <span class="lhs-label">制冷模式变更</span>
              <div class="lhs-cool-row">
                <div class="lhs-run-field"><label class="lhs-label" :for="id + '-cool-unit'">制冷单元</label><select :id="id + '-cool-unit'" class="lhs-input" :value="coolUnit" :disabled="disabled" required @change="onCoolUnit($event)"><option value="">请选择</option><option v-for="u in UNITS" :key="u" :value="u">{{ u }}制冷单元</option></select></div>
                <div class="lhs-run-field"><label class="lhs-label" :for="id + '-cool-from'">当前运行模式</label><select :id="id + '-cool-from'" ref="coolFromRef" class="lhs-input" :class="{ 'lhs-invalid': coolSame }" :value="coolFrom" :disabled="disabled" required :aria-invalid="coolSame ? 'true' : undefined" @change="onCoolFrom($event)"><option value="">请选择</option><option v-for="m in MODE_OPTIONS" :key="m.value" :value="m.value">{{ m.label }}</option></select><p v-if="coolSame" class="lhs-inline-err" role="alert">当前运行模式与切换后模式不能相同</p></div>
                <div class="lhs-run-field"><label class="lhs-label" :for="id + '-cool-to'">切换后运行模式</label><select :id="id + '-cool-to'" ref="coolToRef" class="lhs-input" :class="{ 'lhs-invalid': coolSame }" :value="coolTo" :disabled="disabled" required :aria-invalid="coolSame ? 'true' : undefined" @change="onCoolTo($event)"><option value="">请选择</option><option v-for="m in MODE_OPTIONS" :key="m.value" :value="m.value">{{ m.label }}</option></select></div>
              </div>
            </div>
          </template>
        </section>
      </section>
      <section v-else-if="!field.directory_loading" class="lhs-block lhs-ready" aria-label="读取目录"><button type="button" class="lhs-load-btn" :disabled="disabled" @click="refreshOptions"><RefreshCw :size="14" />{{ field.directory_error ? '重试读取工单' : '读取工单和人员' }}</button></section>
    </template>
  </div>
</template>

<script setup lang="ts">
import { computed, nextTick, onMounted, ref, watch } from "vue";
import { ChevronDown, ChevronUp, Clock3, Info, Loader2, RefreshCw, Search } from "lucide-vue-next";
import VnetSelect from "./VnetSelect.vue";

const props = defineProps<{ field: Record<string, any>; modelValue?: any; disabled?: boolean; id: string }>();
const emit = defineEmits<{ (e: "update:modelValue", value: any): void; (e: "load-options", payload: { scope: string; q?: string }): void }>();

const POLL_GROUPS: string[][] = [["1#", "2#", "3#"], ["4#", "5#", "6#"]];
const UNITS: string[] = ["1#", "2#", "3#", "4#", "5#", "6#"];
const MODE_OPTIONS = [{ value: "1#", label: "停机状态" }, { value: "2#", label: "板换模式" }, { value: "3#", label: "预冷模式" }, { value: "4#", label: "制冷模式" }];
type Run = { from_unit?: string; to_unit?: string; other_unit?: string; run_index?: number; label?: string };
const blankRun = (): Run => ({ from_unit: "", to_unit: "", other_unit: "" });

const DEFAULT_VALUE: Record<string, any> = { exempt: false, scope: "", sop_id: "", operator_record_id: "", reviewer_record_id: "", runs: [] as Run[] };
const local = computed<Record<string, any>>(() => {
  const v = props.modelValue && typeof props.modelValue === "object" && !Array.isArray(props.modelValue) ? props.modelValue : {};
  return { ...DEFAULT_VALUE, ...v };
});
function update(patch: Record<string, any>): void {
  const base = local.value && typeof local.value === "object" && !Array.isArray(local.value) ? local.value : {};
  emit("update:modelValue", { ...base, ...patch });
}

const exemptLabel = computed(() => { const w = String(props.field?.work_type || ""); if (w === "maintenance") return "本次维保不使用工单"; if (w === "adjust") return "本次调整不使用工单"; if (w === "polling") return "非制冷单元/二次泵轮巡"; return "不使用工单"; });
function onExempt(event: Event): void {
  const checked = (event.target as HTMLInputElement).checked;
  if (!!local.value.exempt === checked) return;
  update({ exempt: checked });
}

const effectiveScope = computed(() => (local.value.exempt === true ? "" : String(props.field?.notice_scope || "").trim()));
const validScope = computed(() => Boolean(effectiveScope.value));
const directoryReady = computed(() => Boolean(effectiveScope.value) && String(props.field?.directory_scope || "") === effectiveScope.value);

const requestedScope = ref("");
function requestOnce(scope: string): void {
  if (!scope || props.disabled) return;
  if (String(props.field?.directory_scope || "") === scope) return;
  if (requestedScope.value === scope) return;
  requestedScope.value = scope;
  emit("load-options", { scope });
}
function maybeEmitLoad(): void {
  const scope = effectiveScope.value;
  if (!scope || props.disabled) return;
  if (String(props.field?.directory_scope || "") !== scope) requestOnce(scope);
}
function refreshOptions(): void {
  const scope = effectiveScope.value;
  if (!scope || props.disabled) return;
  requestedScope.value = scope;
  emit("load-options", { scope });
}
watch([effectiveScope, () => props.disabled, () => String(props.field?.directory_scope || "")], () => maybeEmitLoad());
watch(directoryReady, (ready) => {
  if (ready) requestedScope.value = "";
});
function searchPeople(): void {
  const scope = effectiveScope.value;
  if (!scope || props.disabled) return;
  const q = peopleQuery.value.trim();
  requestedScope.value = scope;
  emit("load-options", { scope, ...(q ? { q } : {}) });
}

const sops = computed(() => (directoryReady.value ? (Array.isArray(props.field?.sops) ? props.field.sops : []) : []));
const sopOptions = computed(() => { const used = new Set<string>(); const out: Array<{ value: string; label: string; sop: any }> = []; for (const s of sops.value) { const name = String(s?.name || "").trim() || "未命名SOP"; const reason = String(s?.blocked_reason || "").trim(); const suffix = reason ? `（已锁定：${reason}）` : ""; let label = `${name}${suffix}`; let n = 1; while (used.has(label)) { n += 1; label = `${name} ${n}${suffix}`; } used.add(label); out.push({ value: String(s.sop_id || ""), label, sop: s }); } return out; });
const selectedSop = computed(() => sopOptions.value.find((o) => o.value === String(local.value.sop_id || ""))?.sop || null);
const showSteps = ref(false);
const staleVersion = computed(() => selectedSop.value && 'sop_version' in local.value && Number(local.value.sop_version) !== Number(selectedSop.value.version));
function acceptVersion(): void { if (selectedSop.value) update({ sop_version: Number(selectedSop.value.version) }); }
const sopSteps = computed(() => (Array.isArray(selectedSop.value?.steps) ? [...selectedSop.value.steps] : []).sort((a: any, b: any) => (Number(a?.order) || 0) - (Number(b?.order) || 0)));
const sopAttachments = computed(() => (Array.isArray(selectedSop.value?.documents) ? selectedSop.value.documents : []));
function repeatLabel(rule: Record<string, any>): string {
  const start = sopSteps.value.findIndex((step: Record<string, any>) => step.step_id === rule.from_step_id) + 1;
  const end = sopSteps.value.findIndex((step: Record<string, any>) => step.step_id === rule.to_step_id) + 1;
  return start && end ? `本步后第${start}–${end}步再循环${rule.count}遍` : '';
}
const sopRef = ref<HTMLSelectElement | null>(null);
function syncSopValidity(): void { void nextTick(() => { const el = sopRef.value; if (!el) return; if (local.value.sop_id && selectedSop.value?.blocked_reason) el.setCustomValidity(`该SOP已锁定：${selectedSop.value.blocked_reason}`); else el.setCustomValidity(""); }); }
watch([() => local.value.sop_id, sopOptions], syncSopValidity);

const workType = computed(() => String(props.field?.work_type || ""));
const isPolling = computed(() => workType.value === "polling");
const isCooling = computed(() => workType.value === "adjust" && String(selectedSop.value?.mode || "") === "cooling");
const effectiveMode = computed(() => (isPolling.value ? "polling" : isCooling.value ? "cooling" : "normal"));
const showRuns = computed(() => isPolling.value || isCooling.value);

function modeOfSop(id: string): string {
  if (isPolling.value) return "polling";
  const sop = sops.value.find((s: any) => String(s?.sop_id || "") === id);
  const mode = String(sop?.mode || "");
  return workType.value === "adjust" && mode === "cooling" ? "cooling" : "normal";
}
function defaultRunsFor(mode: string): Run[] {
  if (mode === "polling") return [blankRun()];
  if (mode === "cooling") return [blankRun()];
  return [];
}
function selectSop(event: Event): void {
  const id = (event.target as HTMLSelectElement).value;
  if (id === String(local.value.sop_id || "")) return;
  const oldMode = effectiveMode.value;
  const newMode = modeOfSop(id);
  const patch: Record<string, any> = { sop_id: id };
  if ('sop_version' in local.value) patch.sop_version = Number(sops.value.find((s: any) => s.sop_id === id)?.version || 0);
  if (newMode !== oldMode) patch.runs = defaultRunsFor(newMode);
  update(patch);
  void nextTick(syncSopValidity);
}

const peopleQuery = ref("");
const peopleAll = computed(() => (directoryReady.value ? (Array.isArray(props.field?.people) ? props.field.people : []) : []));
const hDuty = computed(() => peopleAll.value.find((p: any) => ["H楼值班账号"].includes(String(p?.label || "")) || ["H楼值班账号"].includes(String(p?.name || "")) || ["h_duty_account", "H楼值班账号"].includes(String(p?.record_id || ""))) || null);
const labeledPeople = computed(() => { const used = new Set<string>(); const out: Array<{ value: string; label: string }> = []; for (const p of peopleAll.value) { const base = String(p?.label || p?.name || "").trim() || "未命名人员"; let label = base; let n = 1; while (used.has(label)) { n += 1; label = `${base} #${n}`; } used.add(label); const value = String(p?.record_id || "").trim(); if (value) out.push({ value, label }); } return out; });
const operatorLabels = computed(() => labeledPeople.value.filter((o) => String(o.value) !== String(hDuty.value?.record_id || "") && String(o.value) !== String(local.value.reviewer_record_id || "")).map((o) => o.label));
const reviewerLabels = computed(() => labeledPeople.value.filter((o) => String(o.value) !== String(local.value.operator_record_id || "")).map((o) => o.label));
const operatorLabel = computed(() => labeledPeople.value.find((p) => p.value === String(local.value.operator_record_id || ""))?.label || "");
const reviewerLabel = computed(() => labeledPeople.value.find((p) => p.value === String(local.value.reviewer_record_id || ""))?.label || "");
function recordIdByLabel(label: string): string { return labeledPeople.value.find((p) => p.label === label)?.value || ""; }
function selectOperator(label: string): void {
  const rid = recordIdByLabel(label);
  if (!rid || String(rid) === String(local.value.operator_record_id || "") || String(rid) === String(hDuty.value?.record_id || "")) return;
  const patch: Record<string, any> = { operator_record_id: rid };
  if (String(local.value.reviewer_record_id || "") === rid) patch.reviewer_record_id = "";
  update(patch);
}
function selectReviewer(label: string): void {
  const rid = recordIdByLabel(label);
  if (!rid || String(rid) === String(local.value.reviewer_record_id || "") || String(rid) === String(local.value.operator_record_id || "")) return;
  update({ reviewer_record_id: rid });
}
function setHReviewer(): void {
  const rid = hDuty.value?.record_id;
  if (!rid || String(local.value.operator_record_id || "") === String(rid) || String(local.value.reviewer_record_id || "") === String(rid)) return;
  update({ reviewer_record_id: String(rid) });
}

const runs = computed<Run[]>(() => (Array.isArray(local.value.runs) ? local.value.runs : []));
const displayRuns = computed<Run[]>(() => (runs.value.length ? runs.value : [blankRun()]));
const runCount = computed(() => Math.max(1, Math.min(2, runs.value.length || 1)));
const groupOf = (u?: string): string[] => POLL_GROUPS.find((g) => g.includes(String(u || ""))) || [];
function copyRuns(): Run[] { return runs.value.map((r: Run) => ({ ...r })); }
function computeUsed(list: Run[], exceptRun: number, exceptKey: string): Set<string> { const s = new Set<string>(); list.forEach((r, ri) => { if (ri === exceptRun) return; for (const k of ["from_unit", "to_unit"]) { if (ri === exceptRun && k === exceptKey) continue; const v = String(r?.[k as keyof Run] || "").trim(); if (v) s.add(v); } }); return s; }
function fromOptions(i: number): string[] { const list = copyRuns(); const used = computeUsed(list, i, "from_unit"); const current = String(list[i]?.from_unit || "").trim(); const out: string[] = []; for (const unit of UNITS) { if (used.has(unit)) { if (unit === current) out.push(unit); continue; } const grp = groupOf(unit); if (grp.some((c) => c !== unit && !used.has(c))) out.push(unit); } if (current && !out.includes(current)) out.push(current); return out; }
function toOptions(i: number): string[] { const list = copyRuns(); const from = String(list[i]?.from_unit || "").trim(); if (!from) return []; const grp = groupOf(from); if (!grp.length) return []; const used = computeUsed(list, i, "to_unit"); const current = String(list[i]?.to_unit || "").trim(); const out: string[] = []; for (const unit of grp) { if (unit === from || used.has(unit)) { if (unit === current) out.push(unit); continue; } out.push(unit); } if (current && !out.includes(current)) out.push(current); return out; }
function onRunCount(event: Event): void {
  const target = Math.max(1, Math.min(2, Number((event.target as HTMLSelectElement).value) || 1));
  const list = copyRuns();
  while (list.length < target) list.push(blankRun());
  if (list.length > target) list.length = target;
  update({ runs: list });
}
function onPollingFrom(i: number, event: Event): void {
  const value = (event.target as HTMLSelectElement).value;
  const list = copyRuns();
  if (String(list[i]?.from_unit || "") === value) return;
  if (!list[i]) list[i] = blankRun();
  list[i].from_unit = value;
  const currentTo = String(list[i].to_unit || "").trim();
  if (currentTo) { const grp = groupOf(value); const used = computeUsed(list, i, "to_unit"); if (!grp.includes(currentTo) || currentTo === value || used.has(currentTo)) list[i].to_unit = ""; }
  update({ runs: list });
}
function onPollingTo(i: number, event: Event): void {
  const value = (event.target as HTMLSelectElement).value;
  const list = copyRuns();
  if (String(list[i]?.to_unit || "") === value) return;
  if (!list[i]) list[i] = blankRun();
  list[i].to_unit = value;
  update({ runs: list });
}

const coolUnit = computed(() => String(runs.value[0]?.other_unit || ""));
const coolFrom = computed(() => String(runs.value[0]?.from_unit || ""));
const coolTo = computed(() => String(runs.value[0]?.to_unit || ""));
const coolSame = computed(() => Boolean(coolFrom.value && coolTo.value && coolFrom.value === coolTo.value));
function stepText(step: Record<string, any>): string {
  const text = String(step.content || '');
  return isCooling.value ? text.replace('{{from}}', coolUnit.value ? coolUnit.value + '制冷单元' : '待选择制冷单元') : text;
}
const coolFromRef = ref<HTMLSelectElement | null>(null);
const coolToRef = ref<HTMLSelectElement | null>(null);
function syncCoolValidity(): void { void nextTick(() => { const message = coolSame.value ? "当前运行模式与切换后模式不能相同" : ""; if (coolFromRef.value) coolFromRef.value.setCustomValidity(message); if (coolToRef.value) coolToRef.value.setCustomValidity(message); }); }
watch(coolSame, syncCoolValidity, { immediate: true });
function setCoolRun(patch: Partial<Run>): void { const list = copyRuns(); if (!list[0]) list[0] = blankRun(); list[0] = { ...list[0], ...patch }; update({ runs: list }); }
const onCoolUnit = (event: Event): void => setCoolRun({ other_unit: (event.target as HTMLSelectElement).value });
const onCoolFrom = (event: Event): void => setCoolRun({ from_unit: (event.target as HTMLSelectElement).value });
const onCoolTo = (event: Event): void => setCoolRun({ to_unit: (event.target as HTMLSelectElement).value });

watch(effectiveScope, (ns) => {
  if (!ns) return;
  if (String(local.value.scope || "") !== ns) {
    update({ scope: ns, sop_id: "", ...('sop_version' in local.value ? { sop_version: 0 } : {}), runs: [] });
  }
  maybeEmitLoad();
});

onMounted(() => {
  if (effectiveScope.value && local.value.scope !== effectiveScope.value) update({ scope: effectiveScope.value, sop_id: '', ...('sop_version' in local.value ? { sop_version: 0 } : {}), runs: [] });
  maybeEmitLoad();
  void nextTick(syncCoolValidity);
});
</script>

<style scoped>
.lhs { display: flex; flex-direction: column; gap: 8px; min-width: 0; max-width: 100%; font-size: 13px; color: var(--lh-charcoal, #183353); }
.lhs.disabled { opacity: 0.85; }
.lhs-loading { display: flex; align-items: center; gap: 7px; margin: 0; font-size: 12px; color: var(--lh-muted); }
.lhs-spin { animation: lhs-spin .9s linear infinite; } @keyframes lhs-spin { to { transform: rotate(360deg); } }
.lhs-exempt { display: inline-flex; align-items: center; gap: 7px; min-height: 36px; font-size: 13px; cursor: pointer; min-width: 0; }
.lhs-exempt input { width: 16px; height: 16px; flex: 0 0 auto; accent-color: var(--lh-accent, #2f6fed); }
.lhs-exempt:has(input:disabled) { cursor: not-allowed; color: var(--lh-faint-muted, #8a99ac); }
.lhs-error { margin: 0; padding: 7px 9px; border-radius: 8px; font-size: 12px; line-height: 1.4; color: var(--lh-warn, #9a4c10); background: var(--lh-warn-soft, #fff7ed); min-width: 0; overflow-wrap: anywhere; }
.lhs-grid { display: grid; grid-template-columns: minmax(0, 1fr); gap: 12px; min-width: 0; }
@media (min-width: 760px) { .lhs-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
.lhs-block { display: flex; flex-direction: column; gap: 7px; min-width: 0; }
.lhs-sop, .lhs-people, .lhs-runs, .lhs-ready { grid-column: 1 / -1; }
.lhs-h { display: flex; align-items: center; justify-content: space-between; gap: 8px; min-width: 0; }
.lhs-label { font-size: 12px; font-weight: 600; color: var(--lh-muted, #48586e); line-height: 1.3; overflow-wrap: anywhere; }
.lhs-req { color: var(--lh-danger, #d03030); font-weight: 600; }
.lhs-query { display: flex; align-items: center; gap: 5px; min-width: 0; }
.lhs-query input { min-width: 0; width: 140px; box-sizing: border-box; font: inherit; font-size: 12px; padding: 5px 7px; color: var(--lh-charcoal, #142b49); background: var(--lh-surface, #fff); border: 1px solid var(--lh-input-border, #c8d2e0); border-radius: 8px; }
.lhs-query input:focus { outline: 2px solid var(--lh-accent-ring, rgba(30, 99, 255, 0.4)); outline-offset: 0; }
.lhs-query input:disabled { background: var(--lh-surface-subtle, #f3f6fa); color: var(--lh-muted, #8a99ac); cursor: not-allowed; }
.lhs-icon-btn { display: inline-flex; align-items: center; justify-content: center; width: 30px; height: 30px; flex: 0 0 30px; border: 1px solid var(--lh-border, #cbd8e8); border-radius: 8px; background: var(--lh-surface-hover, #f7faff); color: var(--lh-accent, #47709e); cursor: pointer; }
.lhs-icon-btn:hover:not(:disabled) { background: var(--lh-surface-hover, #edf4ff); }
.lhs-icon-btn:disabled { color: var(--lh-faint-muted, #b6c1ce); cursor: not-allowed; }
.lhs-load-btn { align-self: flex-start; border: 1px solid var(--lh-border, #cbd8e8); border-radius: 8px; background: var(--lh-surface-hover, #f7faff); color: var(--lh-accent, #47709e); font: inherit; font-size: 12px; font-weight: 600; padding: 6px 12px; cursor: pointer; }
.lhs-load-btn:disabled { color: var(--lh-faint-muted, #b6c1ce); cursor: not-allowed; }
.lhs-blocked { display: flex; align-items: flex-start; gap: 6px; margin: 0; padding: 6px 8px; border-radius: 8px; font-size: 12px; line-height: 1.4; color: var(--lh-warn, #9a4c10); background: var(--lh-warn-soft, #fff7ed); min-width: 0; overflow-wrap: anywhere; }
.lhs-blocked svg { flex: 0 0 auto; margin-top: 1px; }
.lhs-blocked span { min-width: 0; overflow-wrap: anywhere; }
.lhs-sop-detail { display: flex; flex-direction: column; gap: 6px; min-width: 0; border-block: 1px solid var(--lh-border, #dbe5f1); padding: 5px 0; }
.lhs-detail-toggle { display: flex; align-items: center; justify-content: space-between; gap: 8px; width: 100%; border: 0; background: transparent; color: var(--lh-muted); cursor: pointer; font: inherit; font-size: 12px; padding: 5px 0; }
.lhs-detail-wrap { display: grid; grid-template-rows: 1fr; }
.lhs-detail-wrap > div { min-height: 0; overflow: hidden; }
.lhs-detail-enter-active, .lhs-detail-leave-active { transition: grid-template-rows 180ms ease, opacity 180ms ease; }
.lhs-detail-enter-from, .lhs-detail-leave-to { grid-template-rows: 0fr; opacity: 0; }
.lhs :is(button, select, input):focus-visible { outline: 2px solid var(--lh-accent); outline-offset: 2px; }
@media (prefers-reduced-motion: reduce) { .lhs-detail-enter-active, .lhs-detail-leave-active { transition: none; } }
.lhs-steps { list-style: none; margin: 0; padding: 0; max-height: 150px; overflow: auto; overscroll-behavior: contain; display: flex; flex-direction: column; gap: 5px; }
.lhs-step { display: flex; align-items: flex-start; justify-content: space-between; gap: 8px; font-size: 12px; line-height: 1.4; color: var(--lh-charcoal, #183353); min-width: 0; }
.lhs-step-txt { min-width: 0; overflow-wrap: anywhere; }
.lhs-loop { display: block; color: var(--lh-muted, #56708f); }
.lhs-delay { flex: 0 0 auto; display: inline-flex; align-items: center; gap: 3px; white-space: nowrap; font-size: 11px; color: var(--lh-accent, #47709e); }
.lhs-delay svg { color: var(--lh-accent, #47709e); }
.lhs-empty { margin: 0; font-size: 12px; color: var(--lh-faint-muted, #93a1b3); }
.lhs-attach { display: flex; flex-direction: column; gap: 4px; min-width: 0; border-top: 1px solid var(--lh-border, #dbe5f1); padding-top: 6px; }
.lhs-attach-list { list-style: none; margin: 0; padding: 0; display: flex; flex-direction: column; gap: 3px; }
.lhs-attach-list li { font-size: 12px; color: var(--lh-muted, #56708f); overflow-wrap: anywhere; min-width: 0; }
.lhs-people-cols { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 12px; min-width: 0; }
@media (max-width: 480px) { .lhs-people-cols { grid-template-columns: minmax(0, 1fr); } }
.lhs-pick { display: flex; flex-direction: column; gap: 4px; min-width: 0; }
.lhs-pick-head { display: flex; align-items: center; justify-content: space-between; gap: 6px; min-width: 0; }
.lhs-quick { flex: 0 0 auto; border: 1px solid var(--lh-border, #cbd8e8); border-radius: 8px; background: var(--lh-surface-hover, #f7faff); color: var(--lh-accent, #47709e); font-size: 11px; font-weight: 600; padding: 3px 7px; cursor: pointer; }
.lhs-quick:hover:not(:disabled) { background: var(--lh-surface-hover, #edf4ff); }
.lhs-quick:disabled { color: var(--lh-faint-muted, #b6c1ce); cursor: not-allowed; }
.lhs-runs-count { display: flex; align-items: center; gap: 8px; }
.lhs-runs-count select { width: 80px; }
.lhs-input { min-width: 0; max-width: 100%; box-sizing: border-box; font: inherit; font-size: 12px; line-height: 1.4; padding: 6px 7px; color: var(--lh-charcoal, #142b49); background: var(--lh-surface, #fff); border: 1px solid var(--lh-input-border, #c8d2e0); border-radius: 8px; }
.lhs-input:focus { outline: 2px solid var(--lh-accent-ring, rgba(30, 99, 255, 0.4)); outline-offset: 0; }
.lhs-input:disabled { background: var(--lh-surface-subtle, #f3f6fa); color: var(--lh-muted, #8a99ac); cursor: not-allowed; }
.lhs-input.lhs-invalid { border-color: var(--lh-danger, #d03030); }
.lhs-run, .lhs-cool-row { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px 12px; min-width: 0; }
.lhs-cool-block { display: flex; flex-direction: column; gap: 7px; min-width: 0; }
.lhs-cool-row { grid-template-columns: repeat(3, minmax(0, 1fr)); align-items: start; }
.lhs-run-field { display: flex; flex-direction: column; gap: 4px; min-width: 0; }
.lhs-inline-err { margin: 2px 0 0; font-size: 11px; color: var(--lh-danger, #d03030); min-width: 0; overflow-wrap: anywhere; }
.lhs-pick :deep(.vnet-select-trigger), .lhs-pick :deep(.vnet-combobox-control) { font-size: 12px; }
@media (max-width: 560px) { .lhs-cool-row { grid-template-columns: minmax(0, 1fr); } }
</style>
