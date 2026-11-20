<script setup lang="ts">
import ToolDeskDialog from './ToolDeskDialog.vue';
defineProps<{r:Record<string,any>}>();
</script>
<template>
  <section v-if="r.item?.can_review" class="td-panel">
    <div class="td-heading"><h2>本台审核</h2><div class="td-actions"><span class="td-chip">{{ r.item.asset.name }}</span>
      <button class="td-primary" :disabled="!r.savedSignature && !r.reviewSignatures[r.item.id]" @click="r.ask('通过本台审核？', r.item.asset.name,()=>r.reviewItems([r.item],true))">通过审核</button>
      <button :disabled="!r.reviewNote.trim()" @click="r.ask('驳回本台审核？',r.reviewNote,()=>r.reviewItems([r.item],false))">驳回重填</button>
    </div></div>
    <label class="td-field">审核意见<textarea v-model="r.reviewNote" maxlength="1000" placeholder="驳回时请填写原因"/></label>
    <div class="td-grid">
      <label class="td-field">审核时间<input type="datetime-local" :value="r.reviewDates[r.item.id] || ''" @input="r.setReviewDate($event)"/><small>留空使用提交时刻</small></label>
      <div class="td-field">审核签名<label class="td-check"><input v-model="r.savedSignature" type="checkbox"/>使用本人已登记签名</label>
        <button @click="r.openSignature(false)">{{ r.reviewSignatures[r.item.id]?'重签本台':'手绘本台签名' }}</button>
        <img v-if="r.reviewSignatures[r.item.id]" :src="r.reviewSignatures[r.item.id]" alt="本台待提交签名" height="48"/></div>
    </div>
  </section>
  <ToolDeskDialog v-if="r.batchOpen" title="批量审核确认" :locked="!!r.busy" @close="r.batchOpen=false">
    <template #actions><button class="td-primary" :disabled="!!r.busy || (!r.batch.signature && !r.batchSavedSignature)" @click="r.applyBatchReview()">确认通过 {{ r.reviewable.length }} 台</button></template>
    <p>本次通过 {{ r.reviewable.length }} 台工具的自检。</p><ul class="td-review-tools"><li v-for="a in r.reviewable" :key="a.id">{{ a.asset.name }} · {{ r.scopeLabel(a.asset.scope) }}</li></ul>
    <label class="td-field">审核时间<input v-model="r.batch.reviewed_at" type="datetime-local"/></label>
    <div class="td-row"><label class="td-check"><input v-model="r.batchSavedSignature" type="checkbox"/>使用本人已登记签名</label>
    <button @click="r.openSignature(true)">{{ r.batch.signature?'重新手绘签名':'手绘共用签名' }}</button></div><img v-if="r.batch.signature" :src="r.batch.signature" height="48" alt="批量签名"/>
    <p v-if="r.error" class="td-notice" role="alert">{{ r.error }}</p>
  </ToolDeskDialog>
  <ToolDeskDialog v-if="r.signatureOpen" title="手绘审核签名" @close="r.signatureOpen=false">
    <template #actions><button @click="r.resetCanvas()">清除笔迹</button><button class="td-primary" :disabled="!r.signed" @click="r.applySignature()">采用签名</button></template>
    <canvas :ref="el=>r.pad=el" width="720" height="250" aria-label="审核签名书写区域" @pointerdown="r.startStroke" @pointermove="r.moveStroke" @pointerup="r.stopStroke" @pointercancel="r.stopStroke"/>
  </ToolDeskDialog>
  <ToolDeskDialog v-if="r.reassignOpen && r.item?.can_reassign" title="改派审核人" :locked="!!r.busy" @close="r.reassignOpen=false">
    <label class="td-field">审核人<select data-native-select v-model="r.reassignReviewer"><option value="">本楼有权限的审核人</option><option v-for="p in r.reviewerOptions" :key="p.id" :value="p.id">{{ p.name }}</option></select></label>
    <p v-if="r.error" role="alert">{{ r.error }}</p><button class="td-primary" :disabled="!!r.busy || r.reviewerLoading" @click="r.manageItem('reassign')">确认改派</button>
  </ToolDeskDialog>
</template>
<style scoped>.td-review-tools{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,12rem),1fr));gap:.35rem 1.2rem;margin:0;padding-inline-start:1.25rem}canvas{width:100%;height:auto;border:1px solid #a8bcb6;border-radius:12px;background:#fff;touch-action:none}</style>