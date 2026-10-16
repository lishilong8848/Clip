<template>
  <span ref="root" class="v2-single-select" @click.prevent>
    <slot />
    <NSelect ref="control" :value="selected" :options="options" :disabled="disabled" :filterable="options.length>8" :to="menuHost || 'body'" :menu-props="{class:'v2-single-select-menu',style:menuTheme}" :consistent-menu-width="true" role="combobox" aria-haspopup="listbox" :aria-expanded="show" :aria-disabled="disabled" :aria-required="required" :aria-invalid="invalid" :aria-label="label" :status="invalid?'error':undefined" @update:value="choose" @update:show="opened" />
  </span>
</template>
<script setup lang="ts">
import {nextTick,onBeforeUnmount,onMounted,onUpdated,ref} from 'vue';
import {NSelect,type SelectInst,type SelectOption} from 'naive-ui';
import {inheritedControlTheme} from '../controlTheme';
const root=ref<HTMLElement>(),control=ref<SelectInst>(),selected=ref<string|null>(null),options=ref<SelectOption[]>([]),disabled=ref(false),label=ref('请选择'),invalid=ref(false);
const menuHost=ref<HTMLElement|null>(null),menuTheme=ref<Record<string,string>>({});
const show=ref(false),required=ref(false);
let native:HTMLSelectElement|null=null,observer:MutationObserver|undefined,stamp='';
function sync(){
  if(!native)return;
  const next=Array.from(native.options).map((option,index)=>({value:String(index),label:option.label,disabled:option.disabled||(option.parentElement instanceof HTMLOptGroupElement&&option.parentElement.disabled)}));
  const state=JSON.stringify([next,native.selectedIndex,native.matches(':disabled'),native.getAttribute('aria-label'),native.required,native.getAttribute('aria-invalid')]);
  if(state===stamp)return;stamp=state;
  options.value=next;selected.value=native.selectedIndex<0?null:String(native.selectedIndex);disabled.value=native.matches(':disabled');
  required.value=native.required;invalid.value=native.getAttribute('aria-invalid')==='true';
  label.value=native.getAttribute('aria-label')||Array.from(native.labels||[]).map(el=>{const copy=el.cloneNode(true) as HTMLElement;copy.querySelectorAll('select,.n-select').forEach(node=>node.remove());return copy.textContent?.trim();}).filter(Boolean).join(' ')||'请选择';
}
async function choose(value:string|null){
  if(!native||disabled.value||value===null)return;
  const index=Number(value),option=native.options[index];if(!option||options.value[index]?.disabled)return;
  native.selectedIndex=index;invalid.value=false;
  // The original select still owns v-model, numeric/object values, FormData and handlers.
  native.dispatchEvent(new Event('input',{bubbles:true}));native.dispatchEvent(new Event('change',{bubbles:true}));
  await nextTick();sync();
}
function focus(){control.value?.focus();}
function validate(event:Event){event.preventDefault();invalid.value=true;focus();}
function opened(value:boolean){show.value=value;if(value){sync();menuTheme.value={...inheritedControlTheme(root.value||null),zIndex:'12050'};}}
onMounted(()=>{
  native=root.value?.querySelector('select')||null;if(!native)return;
  native.tabIndex=-1;native.setAttribute('aria-hidden','true');sync();
  menuHost.value=root.value?.closest<HTMLElement>('.lighthouse-layer, dialog[open]')||null;
  native.addEventListener('change',sync);native.addEventListener('focus',focus);native.addEventListener('invalid',validate);
  observer=new MutationObserver(sync);observer.observe(native,{childList:true,subtree:true,characterData:true,attributes:true,attributeFilter:['disabled','label','value','selected','required','aria-label','aria-invalid']});
  for(let fieldset=native.closest('fieldset');fieldset;fieldset=fieldset.parentElement?.closest('fieldset')||null)observer.observe(fieldset,{attributes:true,attributeFilter:['disabled']});
});
onUpdated(sync);
onBeforeUnmount(()=>{observer?.disconnect();native?.removeEventListener('change',sync);native?.removeEventListener('focus',focus);native?.removeEventListener('invalid',validate);});
</script>
<style>
.workspace-v2 .v2-single-select{position:relative;display:inline-block;vertical-align:middle;min-width:100px;max-width:100%;flex:0 1 auto;line-height:normal}
.workspace-v2 .v2-single-select>select{position:absolute!important;width:1px!important;min-width:0!important;height:1px!important;min-height:0!important;padding:0!important;margin:0!important;border:0!important;opacity:0;pointer-events:none;clip-path:inset(50%)}
.workspace-v2 .v2-single-select>.n-select{width:100%;min-width:0}
.workspace-v2 .v-binder-follower-container:has(.v2-single-select-menu){z-index:12050!important}
.workspace-v2 .v2-string-select,.workspace-v2 .lhs-run-field>.v2-single-select{width:100%}
.workspace-v2 label>.v2-single-select,.workspace-v2 .notice-select>.v2-single-select,.workspace-v2 .notice-field>.v2-single-select{width:100%}
.workspace-v2 .notice-select:has(.v2-single-select)>svg{display:none}
.workspace-v2 .notice-filters label:has(select[aria-label="时段"]) .v2-single-select{width:142px}
.workspace-v2 .v2-single-select-menu{--n-option-color-active-pending:var(--lh-accent-soft,#edf4fc)!important;--n-option-check-color:var(--lh-accent,var(--v2-blue))!important;--n-option-text-color-pressed:var(--lh-accent,var(--v2-blue))!important;--n-option-text-color-disabled:var(--lh-muted,#8b9199)!important}
.workspace-v2 .v2-single-select .n-base-selection{--n-color:var(--lh-surface-subtle,var(--cf-surface,#fff))!important;--n-text-color:var(--lh-charcoal,var(--v2-ink))!important;--n-border:1px solid var(--lh-input-border,var(--v2-border))!important;--n-border-radius:6px!important}
.workspace-v2 .v2-single-select-menu{--n-color:var(--lh-surface,var(--cf-surface,#fff))!important;--n-option-text-color:var(--lh-charcoal,var(--v2-ink))!important;--n-option-text-color-active:var(--lh-accent,var(--v2-blue))!important;--n-option-color-pending:var(--lh-accent-soft,var(--filter-selected,#edf4fc))!important;--n-option-color-active:var(--lh-accent-soft,var(--filter-selected,#edf4fc))!important}
@media(max-width:760px){.workspace-v2 .v2-single-select .n-base-selection{--n-height:44px!important;--n-font-size:16px!important}}
</style>
