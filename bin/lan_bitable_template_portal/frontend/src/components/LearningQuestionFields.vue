<template>
  <fieldset class="learning-question-fields" :disabled="disabled">
    <p v-if="reason" class="review-reason"><strong>反馈说明</strong> {{ reason }}</p>
    <label>题干<textarea v-model="question.stem" rows="3" maxlength="12000" required /></label>
    <label v-if="question.type==='interview'">参考答案<textarea v-model="question.answer_text" rows="3" maxlength="12000" /></label>
    <template v-else>
      <header><strong>选项与正确答案</strong><button type="button" :disabled="question.options.length>=26" @click="add">添加选项</button></header>
      <div v-for="(option,n) in question.options" :key="option.id" class="learning-option">
        <label><input :type="question.type==='single'?'radio':'checkbox'" :name="group" :checked="question.correct_option_ids.includes(option.id)"
          :aria-label="'选项 '+letter(n)+' 为正确答案'" @change="choose(option.id)">{{ letter(n) }}</label>
        <textarea v-model="option.text" rows="1" maxlength="5000" :aria-label="'选项 '+letter(n)+' 内容'" />
        <div class="learning-option-tools">
          <button type="button" :disabled="!Number(n)" aria-label="上移选项" @click="move(Number(n),-1)">↑</button>
          <button type="button" :disabled="Number(n)+1===question.options.length" aria-label="下移选项" @click="move(Number(n),1)">↓</button>
          <button type="button" aria-label="删除选项" @click="remove(Number(n))">删除</button>
        </div>
      </div>
    </template>
    <label>解析<textarea v-model="question.analysis" rows="2" maxlength="12000" /></label>
  </fieldset>
</template>

<script setup lang="ts">
import {useId} from 'vue';
import type {Dict} from '../api/client';
const props=defineProps<{question:Dict;disabled:boolean;reason?:string}>();
const group=useId();
function letter(index:number|string){return String.fromCharCode(65+Number(index));}
function add(){
  const id=Array.from(crypto.getRandomValues(new Uint32Array(4)),v=>v.toString(16).padStart(8,'0')).join('');
  props.question.options.push({id:'o_'+id,text:''});
}
function choose(id:string){
  const q=props.question,ids:string[]=q.correct_option_ids;
  q.correct_option_ids=q.type==='single'?[id]:ids.includes(id)?ids.filter(value=>value!==id):[...ids,id];
}
function remove(index:number){
  const [removed]=props.question.options.splice(index,1);
  props.question.correct_option_ids=props.question.correct_option_ids.filter((id:string)=>id!==removed.id);
}
function move(index:number,delta:number){
  const options=props.question.options;
  if(options[index+delta])options.splice(index+delta,0,...options.splice(index,1));
}
</script>
<style scoped>
.learning-question-fields {display:grid;gap:9px;min-width:0;margin:0;padding:0;border:0}
.review-reason {margin:0;white-space:pre-wrap;overflow-wrap:anywhere}
.learning-question-fields>label {display:grid;gap:4px}
.learning-question-fields textarea {box-sizing:border-box;width:100%;min-width:0;font:inherit;resize:vertical}
.learning-question-fields header,.learning-option-tools,.learning-option label {display:flex;align-items:center;gap:6px}
.learning-question-fields header {justify-content:space-between}
.learning-option {display:grid;grid-template-columns:auto minmax(0,1fr) auto;align-items:start;gap:6px}
.learning-option-tools button {padding-inline:6px}
@media(max-width:520px){.learning-option {grid-template-columns:auto minmax(0,1fr)}.learning-option-tools {grid-column:2;justify-content:flex-end}}
</style>
