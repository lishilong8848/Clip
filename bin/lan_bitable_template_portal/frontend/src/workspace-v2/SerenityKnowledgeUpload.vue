<script setup lang="ts">
import {computed, toRef} from 'vue';
import {FilePlus2, FolderOpen, Upload, X} from 'lucide-vue-next';
import type {LibraryController} from './SerenityKnowledge.vue';
const props = defineProps<{model: LibraryController}>();
const model = props.model;
const fileInput = toRef(model, 'fileInput');
const folderInput = toRef(model, 'folderInput');
const dropzone = toRef(model, 'dropzone');
const locked = computed(() => model.uploadBusy || model.scanningFolder || model.uploadConfirm);
function close() {
  if (locked.value) return;
  model.uploadPanel = false;
  if (!model.pendingFiles.length && !model.textContent.trim()) model.uploadTarget = null;
}
</script>

<template>
  <section class="sk-upload" aria-label="上传文件" data-v2-inline-feedback>
    <header><h2>{{ model.uploadTarget ? '替换文档' : '上传文件' }}</h2>
      <div class="sk-upload-tools">
      <button :aria-pressed="model.uploadMode==='files'" :disabled="locked" @click="model.uploadMode='files'">文件上传</button>
      <button :aria-pressed="model.uploadMode==='text'" :disabled="locked" @click="model.uploadMode='text'">粘贴文本</button>
      </div>
      <button aria-label="收起上传面板" :disabled="locked" @click="close"><X :size="16"/></button></header>
    <p v-if="model.uploadTarget">替换「{{ model.uploadTarget.name }}」</p>
    <input ref="fileInput" aria-label="选择知识库文件" hidden type="file" :accept="model.FILE_ACCEPT" :multiple="!model.uploadTarget" :disabled="locked" @change="model.onUploadPick"/>
    <input ref="folderInput" aria-label="选择知识库文件夹" hidden type="file" multiple webkitdirectory :disabled="locked || !!model.uploadTarget" @change="model.onFolderPick"/>
    <div v-if="model.uploadMode==='files'" class="sk-pick">
      <button ref="dropzone" class="sk-zone" :class="{over:model.dragging}" :disabled="locked" @click="fileInput?.click()">
        <FilePlus2 :size="20"/><strong>选择文件</strong><span>也可拖入或粘贴文件</span>
      </button>
      <button v-if="!model.uploadTarget" :disabled="locked" @click="folderInput?.click()"><FolderOpen :size="16"/>选择文件夹</button>
      <small>{{ model.FILE_ACCEPT.split('.').join('').split(',').join(' / ').toUpperCase() }} · 单文件上限 100 MiB</small>
    </div>
    <div v-else class="sk-write">
      <label>文件名<input v-model="model.textName" placeholder="留空自动命名" :disabled="locked"/></label>
      <label>文本内容<textarea v-model="model.textContent" rows="4" :disabled="locked"/></label>
    </div>
    <ul v-if="model.pendingFiles.length">
      <li v-for="(file,i) in model.pendingFiles" :key="file.name">
        <span>{{ file.name }}</span><small>{{ model.sizeText(file.size) }}</small>
        <button :disabled="locked" :aria-label="'移除 '+file.name" @click="model.pendingFiles.splice(i,1)"><X :size="14"/></button>
      </li>
    </ul>
    <p v-if="model.uploadError" role="alert">{{ model.uploadError }}</p>
    <footer><span role="status">{{ model.scanningFolder ? '正在读取文件夹…' : model.uploadBusy ? '正在上传 '+ Math.round(model.uploadProgress)+'%' : '待上传 '+model.pendingFiles.length+' 个文件' }}</span>
      <progress v-if="model.uploadBusy" max="100" :value="model.uploadProgress" aria-label="上传进度"/>
      <button v-if="model.uploadMode==='text'" :disabled="locked || !model.textContent.trim()" @click="model.addText">加入待上传</button>
      <button class="sk-primary" :disabled="locked || (!model.pendingFiles.length && !model.textContent.trim())" @click="model.requestUpload"><Upload :size="15"/>确认上传</button>
    </footer>
  </section>
</template>
<style scoped>
.sk-upload {padding:14px 16px;display:grid;gap:10px;min-width:0;}
header,footer,li,.sk-upload-tools,.sk-pick {display:flex;align-items:center;gap:8px; flex-wrap:wrap;}
header>button:last-child {margin-inline-start:auto;} h2,p,ul {margin:0;} h2 {font-size:17px;}
.sk-upload-tools [aria-pressed=true] {color:var(--v2-blue,#256584);border-color:currentColor;}
.sk-zone {flex:1 1 230px;min-height:66px!important;border-style:dashed!important;} .sk-zone.over {background:#d8efdf!important;}
.sk-pick small {flex-basis:100%;} small,footer span,.sk-zone span {font-size:12px;color:var(--sk-muted);}
.sk-write {display:grid;gap:9px;} .sk-write label {display:grid;gap:4px;font-size:13px;}
.sk-write input,.sk-write textarea {border-radius:8px;border:1px solid var(--sk-edge);padding:9px;color:inherit;background:var(--sk-field);font:inherit;}
.sk-write button {justify-self:start;} ul {padding:0;max-height:14rem;overflow:auto;}
li {border-bottom:1px solid var(--sk-edge);padding:4px 0;} li span {flex:1;overflow-wrap:anywhere;} p[role=alert] {color:#963c33;}
progress {accent-color:#187653;max-width:100%;}
</style>
