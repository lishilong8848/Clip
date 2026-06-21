<template>
  <section class="mop-summary">
    <article>
      <span>本月维保</span>
      <strong>{{ notices }}</strong>
    </article>
    <article>
      <span>待绑定/待上传</span>
      <strong>{{ pending }}</strong>
    </article>
    <article>
      <span>已绑定</span>
      <strong>{{ bound }}</strong>
    </article>
    <article>
      <span>已上传维护单</span>
      <strong>{{ uploaded }}</strong>
    </article>
    <article>
      <span>MOP 表格</span>
      <strong>{{ mopFiles }}</strong>
    </article>
  </section>
</template>

<script setup lang="ts">
defineProps<{
  notices: number;
  pending: number;
  bound: number;
  uploaded: number;
  mopFiles: number;
}>();
</script>

<style scoped>
.mop-summary {
  display: grid;
  grid-template-columns: repeat(5, minmax(0, 1fr));
  gap: 12px;
}

.mop-summary article {
  min-width: 0;
  border: 1px solid #d8e5f7;
  border-radius: 18px;
  padding: 14px 16px;
  background: rgba(255, 255, 255, 0.94);
  box-shadow: 0 10px 24px rgba(15, 82, 186, 0.08);
}

.mop-summary span {
  display: block;
  color: #64748b;
  font-size: 12px;
  font-weight: 900;
}

.mop-summary strong {
  display: block;
  margin-top: 4px;
  color: #0e4fb2;
  font-size: 23px;
  font-weight: 950;
}

@media (max-width: 1120px) {
  .mop-summary {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}
</style>
