import { modernizeControls } from './workspace-learning-adapter.mjs';

// Shared sections, recovery and mutations live in LearningSharing; only apply the V2 skin.
export function adaptLearningSharing(source) {
  return modernizeControls(source.replace('<script setup lang="ts">', '<script setup lang="ts">\nimport { NButton } from "naive-ui";\nimport "../workspace-v2/sharing.css";'));
}
