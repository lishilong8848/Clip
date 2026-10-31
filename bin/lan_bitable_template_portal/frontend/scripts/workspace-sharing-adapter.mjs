import { modernizeControls } from './workspace-learning-adapter.mjs';

// Keep the shared controller; V2 exposes every mode and protects hidden manual questions.
export function adaptLearningSharing(source) {
  source = source.replace('<NRadioButton v-if="importing" value="import">', '<NRadioButton value="import">');
  source = source.replace('<section v-if="manualEditing" class="sharing-manual-create"', `<p v-if="!manualEditing && filledManualQuestions.length" class="sharing-hint" role="status">已保留 {{ filledManualQuestions.length }} 道手工题目。切回手工出题可继续编辑；如要改用资料出题或文件导入，请先处理这些题目。</p>
        <section v-if="manualEditing" class="sharing-manual-create"`);
  source = source.replace("  if (action === 'create' && importing.value && materialFiles.value.length)", `  if (action === 'create' && !manualEditing.value && filledManualQuestions.value.length) {
    error.value = '已保留手工题目，请切回手工出题保存，或明确删除这些题目后再使用其他出题方式。';
    return;
  }
  if (action === 'create' && importing.value && materialFiles.value.length)`);
  source = source.replace('将清空本次未创建的标题、材料和人员选择', '将清空本次未创建的标题、材料、手工题目和人员选择');
  return modernizeControls(source.replace('<script setup lang="ts">', '<script setup lang="ts">\nimport { NButton } from "naive-ui";\nimport "../workspace-v2/sharing.css";'));
}
