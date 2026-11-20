<script setup lang="ts">
import {computed,ref,useId} from 'vue';
import {boxParts,completionPair,pickerValue,tickBox,type ToolDateKind} from './toolWorkbookFields';
const props=defineProps<{value:string;original:string;label:string;kind?:ToolDateKind;disabled?:boolean}>();
const emit=defineEmits<{change:[value:string]}>();
const raw=ref(false);
const group=useId();
const boxes=computed(()=>boxParts(props.value));
const exclusive=computed(()=>completionPair(props.value));
const change=(event:Event)=>emit('change',(event.target as HTMLInputElement).value);
</script>
<template>
  <div class="tool-workbook-input">
    <template v-if="!raw && kind">
      <input :type="kind" :step="kind==='time'||kind==='datetime-local'?1:undefined" :value="pickerValue(kind,value)" :disabled="disabled" :aria-label="label" @input="change"/>
    </template>
    <span v-else-if="!raw && boxes.parts.length" class="tool-boxes" :role="exclusive?'radiogroup':'group'" :aria-label="label">
      <template v-for="(part,i) in boxes.parts" :key="i">{{ part.text }}<input :type="exclusive?'radio':'checkbox'" :name="group" :checked="part.checked" :disabled="disabled"
        :aria-label="label+' 第'+(i+1)+'项'" @change="emit('change',tickBox(value,original,i,($event.target as HTMLInputElement).checked))"/></template>{{ boxes.tail }}
    </span>
    <textarea v-else rows="2" :value="value" :aria-label="label" :disabled="disabled" @input="change"/>
    <button v-if="!disabled && (raw || kind || boxes.parts.length)" type="button" @click="raw=!raw">{{ raw?'结束文本编辑':'编辑文本' }}</button>
  </div>
</template>
<style scoped>
.tool-workbook-input{min-width:0;display:flex;align-items:center;flex-wrap:wrap;gap:4px}
.tool-workbook-input>input,.tool-workbook-input>textarea{min-width:0;width:100%;box-sizing:border-box}
.tool-boxes{white-space:pre-wrap;overflow-wrap:anywhere}.tool-boxes input[type=checkbox]{display:inline-block;width:1.1em;height:1.1em;min-height:0;vertical-align:middle;margin:0 .2em;accent-color:#15704e}
</style>
