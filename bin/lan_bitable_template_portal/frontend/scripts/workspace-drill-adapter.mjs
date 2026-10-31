import {parse} from '@vue/compiler-sfc';

function replaceOnce(source, from, to) {
  if (!source.includes(from)) from = from.replaceAll('\n', '\r\n');
  if (!source.includes(from) || source.indexOf(from) !== source.lastIndexOf(from)) {
    throw new Error('Drill presentation anchor changed: ' + from.slice(0, 90));
  }
  return source.replace(from, to);
}

// Reuse the complete legacy controller; only the V2 template and styles change.
export function adaptDrillPresentation(source, filename) {
  const file = filename.replaceAll('\\', '/').split('/').pop();
  if (file === 'LifeGuidePage.vue') {
    source = replaceOnce(source, 'import guideHtml from "../assets/HowToLiveBetter.html?raw";', `import originalGuideHtml from "../assets/HowToLiveBetter.html?raw";
// Only change the first-visit default; explicit open/closed preferences take precedence.
const guideHtml = originalGuideHtml.replace("if (railOpen === null) railOpen = innerWidth > 960 ? '1' : '0';", "if (railOpen === null) railOpen = innerWidth > 960 && TODOS.length > 0 ? '1' : '0';");`);
    return source + '\n<style scoped src="../workspace-v2/lifeGuide.css"></style>\n';
  }
  if (file !== 'DrillManagementPage.vue') return source;
  source = replaceOnce(source, `          <label>
            <span>年份</span>
            <input v-model.trim="uploadForm.year" inputmode="numeric" maxlength="4" required pattern="\\d{4}" />
          </label>
          <label>
            <span>月份</span>
            <input v-model="uploadForm.month" type="month" required />
          </label>`, `          <label>
            <span>演练年月</span>
            <input v-model="uploadForm.month" type="month" required :disabled="busy" />
          </label>`);
  source = replaceOnce(source, '  uploading.value = true;\n  error.value = "";\n  try {\n    const form = new FormData();', `  const uploadMonth = uploadForm.month;
  if (!/^\\d{4}-(0[1-9]|1[0-2])$/.test(uploadMonth)) {
    setNotice("请选择有效的演练年月。", "warning");
    return;
  }
  uploading.value = true;
  error.value = "";
  try {
    const form = new FormData();`);
  source = replaceOnce(source, 'form.append("year", uploadForm.year);', 'form.append("year", uploadMonth.split("-")[0]);');
  source = replaceOnce(source, 'form.append("month", String(Number(uploadForm.month.split("-")[1] || 0)));', 'form.append("month", String(Number(uploadMonth.split("-")[1])));');
  source = replaceOnce(source, 'selectedMonth.value = uploadForm.month;', 'selectedMonth.value = uploadMonth;');
  source=replaceOnce(source,'uniquePeople([...selectedParticipants.value, ...commanderPeople.value])','uniquePeople(selectedParticipants.value)');
  source=replaceOnce(source,'  navigate("/drill-management?mode=admin");','  navigate("/drill-management?mode=admin&return_scope="+encodeURIComponent(activeScope.value)+"&month="+encodeURIComponent(selectedMonth.value));');
  source=replaceOnce(source,'const buildingDrills = computed',`const v2DrillReturnScope=ref(normalizeBuilding(routeParams.get('return_scope')||''));
const v2DrillReturnOptions=computed(()=>authorizedBuildingScopes.value.map(item=>({value:item.value,label:item.label})));
const v2DrillReturnTarget=computed(()=>v2DrillReturnOptions.value.some(item=>item.value===v2DrillReturnScope.value)?v2DrillReturnScope.value:v2DrillReturnOptions.value[0]?.value||'');
const buildingDrills = computed`);
  source=replaceOnce(source,`  if (!participantIds.value.includes(id)) {
    if (participantIds.value.length >= 10) {
      setNotice("参演人员已满 10 人，请先移除不参与本次演练的人员，再选择签名。", "warning");
      return;
    }
    execution.value.participants.push(compactPerson(person));
  }`,`  if (!participantIds.value.includes(id)) {
    setNotice("请先在上方选择本次参演人员，再分配步骤执行人。", "warning");
    return;
  }`);
  const {descriptor} = parse(source), lists = [];
  function visit(node) {
    if (node.tag === 'aside' && node.props?.some(p => p.name === 'class' && p.value?.content === 'drill-list')) lists.push(node);
    for (const child of node.children || []) visit(child);
  }
  visit(descriptor.template.ast);
  if (lists.length !== 2) throw new Error('Expected the drill template and execution lists');
  for (const node of lists.reverse()) {
    const admin = node.loc.source.includes('selectAdminDrill');
    const items = admin ? 'drills' : 'buildingDrills';
    const select = admin ? 'selectAdminDrill' : 'selectBuildingDrill';
    const label = admin ? '演练模板' : '本月演练';
    const status = admin ? 'item.status' : 'drillExecutionStatus(item)';
    let list = node.loc.source;
    list = list.replace(/(aria-label="[^"]+">)/, `$1\n<header class="v2-drill-list-heading"><strong>${label}</strong><span>{{ ${items}.length }} 项</span></header>`);
    list = list.replace('class="drill-list__item"', 'class="drill-list__item" data-v2-navigation :aria-pressed="selectedDrillId === item.drill_id"');
    const mobile = `<div class="v2-drill-mobile-picker"><span>${label}</span><V2DrillSelect aria-label="选择${label}" :value="selectedDrillId" :disabled="busy" :options="${items}.map(item => ({value:item.drill_id,label:(item.name || '未命名演练')+' · '+statusLabel(${status})}))" @update:value="${select}" /></div>`;
    source = source.slice(0, node.loc.start.offset) + mobile + list + source.slice(node.loc.end.offset);
  }
  source = replaceOnce(source, '<section v-if="activeSheet === \'record\'" class="record-editor">', '<section v-if="activeSheet === \'record\'" class="record-editor">\n<h3 class="v2-drill-section-title">基本信息</h3>');
  source = replaceOnce(source, '<SheetTable v-if="previewModel && !previewLoading" :model="previewModel" compact />', `<details class="v2-drill-preview" :key="selectedDrillId">
                <summary><FileSpreadsheet :size="17" />记录表预览</summary>
                <div v-if="previewLoading" class="inline-state" role="status">正在读取记录表</div>
                <MessageBanner v-else-if="previewError" tone="failed" :text="previewError" />
                <SheetTable v-else-if="previewModel" :model="previewModel" compact />
                <div v-else class="inline-state">暂无记录表预览</div>
              </details>`);
  source = replaceOnce(source, '<label v-if="authorizedBuildingScopes.length > 1">', '<label class="v2-drill-scope" v-if="authorizedBuildingScopes.length > 1">');
  source = replaceOnce(source, '<details open :class="{ locked: configurationLocked }">\n              <summary>记录表字段映射</summary>', '<details :class="{ locked: configurationLocked }">\n              <summary>记录表字段映射</summary>');
  source = source.replace(/<button type="button" :class="\{ active: activeSheet/g, '<button type="button" data-v2-navigation :class="{ active: activeSheet');
  source = source.replaceAll('<MessageBanner ', '<MessageBanner data-v2-inline-feedback ').replaceAll('<MessageBanner\n', '<MessageBanner data-v2-inline-feedback\n').replaceAll('<MessageBanner\r\n', '<MessageBanner data-v2-inline-feedback\r\n');
  source = replaceOnce(source, '<button type="button" class="primary-button" @click="uploadOpen = !uploadOpen">', '<button type="button" class="primary-button" data-v2-navigation :aria-expanded="uploadOpen" @click="uploadOpen = !uploadOpen">');
  source = source.replace('<script setup lang="ts">', '<script setup lang="ts">\nimport { NSelect as V2DrillSelect } from "naive-ui";');
  return source + '\n<style scoped src="../workspace-v2/drill.css"></style>\n';
}
