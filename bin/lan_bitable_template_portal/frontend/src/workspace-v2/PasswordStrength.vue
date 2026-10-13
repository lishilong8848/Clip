<template>
  <div v-if="value" class="v2-password-strength" :style="{'--strength-color':colors[score]}">
    <div class="v2-strength-segments" role="meter" aria-label="新密码强度" :aria-valuenow="score" aria-valuemin="0" aria-valuemax="4" :aria-valuetext="labels[score]"><i v-for="n in 4" :key="n"><span :class="{filled:n<=score}" :style="{transitionDelay:(n-1)*35+'ms'}" /></i></div>
    <div class="v2-strength-label" aria-live="polite"><Transition name="v2-strength-text"><span :key="score">{{ labels[score] }}</span></Transition><Check v-if="score===4" :size="16" aria-hidden="true" /></div>
  </div>
</template>
<script setup lang="ts">
import { computed } from 'vue';
import { Check } from 'lucide-vue-next';
import { passwordStrength } from './passwordFeedback';
const props=defineProps<{value:string}>();
const score=computed(()=>passwordStrength(props.value));
const labels=['','太弱','一般','较强','很安全'],colors=['#c4ceca','#c24651','#bd7424','#a18b16','#138879'];
</script>
<style>
.workspace-v2 .v2-password-strength{margin-top:-6px}.workspace-v2 .v2-strength-segments{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:5px}.workspace-v2 .v2-strength-segments i{height:5px;border-radius:3px;background:#e3e9e6;overflow:hidden}.workspace-v2 .v2-strength-segments i>span{display:block;height:100%;background:var(--strength-color);transform:scaleX(0);transform-origin:left;transition:transform 250ms cubic-bezier(.2,.9,.3,1.05),background-color 250ms ease}.workspace-v2 .v2-strength-segments i>span.filled{transform:scaleX(1)}.workspace-v2 .v2-strength-label{position:relative;min-height:24px;color:var(--strength-color);font-size:12px;transition:color 250ms}.workspace-v2 .v2-strength-label>span{position:absolute;left:0;top:4px}.workspace-v2 .v2-strength-label>svg{position:absolute;right:0;top:4px;stroke-dasharray:36;stroke-dashoffset:36;animation:v2-check-draw 250ms ease-out forwards}.v2-strength-text-enter-active,.v2-strength-text-leave-active{transition:opacity 180ms ease-out,transform 180ms ease-out}.v2-strength-text-enter-from{opacity:0;transform:translateY(4px)}.v2-strength-text-leave-to{opacity:0;transform:translateY(-4px)}.workspace-v2 input.v2-password-invalid{border-color:#c24651!important;box-shadow:0 0 0 2px #c2465115;transition:border-color 180ms,box-shadow 180ms}.workspace-v2 input.v2-password-shake{animation:v2-password-no 400ms ease-in-out}@keyframes v2-password-no{0%,100%{transform:translateX(0)}15%{transform:translateX(-10px)}30%{transform:translateX(10px)}45%{transform:translateX(-6px)}60%{transform:translateX(6px)}75%{transform:translateX(-3px)}90%{transform:translateX(3px)}}@media(prefers-reduced-motion:reduce){.workspace-v2 input.v2-password-shake{animation:none}.workspace-v2 .v2-strength-segments i>span{transition:none}.v2-strength-text-enter-active,.v2-strength-text-leave-active{transition:opacity 100ms}.v2-strength-text-enter-from,.v2-strength-text-leave-to{transform:none}}
</style>
