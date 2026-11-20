<template>
  <NModal :show="open" :mask-closable="false" @update:show="value => { if (!value) resolve(false) }">
    <section class="lp-decision" role="dialog" aria-modal="true" :aria-label="title" tabindex="-1">
      <header><h2>{{ title }}</h2><span v-if="tone==='danger'">删除确认</span></header>
      <ul v-if="details?.length"><li v-for="detail in details" :key="detail">{{ detail }}</li></ul>
      <div class="lp-decision-line"><p>{{ message }}</p><div>
        <button v-if="!hideCancel" type="button" @click="resolve(false)">{{ cancelLabel || '取消' }}</button>
        <button type="button" :class="tone==='danger'?'danger':'approve'" @click="resolve(true)">{{ confirmLabel || '确认' }}</button>
      </div></div>
    </section>
  </NModal>
</template>
<script setup lang="ts">
import {NModal} from 'naive-ui';
defineProps<{open:boolean;title:string;message:string;tone?:'primary'|'danger'|'warning';confirmLabel?:string;cancelLabel?:string;hideCancel?:boolean;details?:string[]}>();
const emit=defineEmits<{resolve:[value:boolean]}>();
function resolve(value:boolean){emit('resolve',value);return false;}
</script>
<style scoped>
.lp-decision{box-sizing:border-box;width:min(510px,calc(100vw - 22px));max-height:calc(100dvh - 24px);overflow:auto;padding:14px 16px;border:1px solid #d1e0df;border-radius:15px;background:var(--soft-face,#f3f6f6);color:var(--v2-ink,#243f4c);box-shadow:0 14px 48px #28485138}
.lp-decision header,.lp-decision-line,.lp-decision-line>div{display:flex;align-items:center;gap:6px 9px;flex-wrap:wrap}
.lp-decision h2{margin:0;font-size:17px}.lp-decision header span{color:#8d3d34;font-size:12px}
.lp-decision-line{margin-top:8px;align-items:flex-end}.lp-decision-line p{flex:1 1 170px;margin:0;font-size:14px;overflow-wrap:anywhere}.lp-decision-line>div{margin-inline-start:auto;}
.lp-decision button{border:1px solid #c4d4d5;border-radius:8px;background:#f9fcfb;color:#284751;font:inherit;cursor:pointer;padding:6px 13px;min-height:34px}
.lp-decision .approve{color:white;background:#1b7055;border-color:#1b7055}.lp-decision .danger{color:white;background:#94443b;border-color:#94443b}
.lp-decision button:focus-visible{outline:2px solid #287598;outline-offset:2px}.lp-decision ul{padding-inline-start:20px;font-size:13px}
@media(max-width:600px){.lp-decision button{min-height:40px}}
</style>
