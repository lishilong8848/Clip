<template>
  <div class="kb-group-picker" data-v2-navigation>
    <button type="button" class="kb-group-picker-trigger" role="combobox" aria-label="目标分组" aria-haspopup="dialog" :aria-expanded="open" :disabled="disabled" @click="show">
      <Folder :size="17" aria-hidden="true" />
      <span>{{ selectedName || (modelValue ? '分组已不可用，请重新选择' : '选择目标分组') }}</span>
      <ChevronDown :size="16" aria-hidden="true" />
    </button>
    <p v-if="selectedPath" class="kb-group-picker-path" aria-label="已选分组完整路径">{{ selectedPath }}</p>
    <NModal :show="open" preset="card" title="选择目标分组" aria-label="选择目标分组" class="kb-group-picker-dialog" :bordered="false" @update:show="setOpen">
      <div class="kb-group-picker-body">
        <NInput v-model:value="search" clearable placeholder="搜索文件夹名称或完整路径" :input-props="{'aria-label':'搜索分组'}"><template #prefix><Search :size="16" aria-hidden="true" /></template></NInput>
        <div class="kb-group-picker-tree">
          <NTree :data="tree" :pattern="search.trim()" :filter="filter" :selected-keys="candidate ? [candidate] : []" :default-expanded-keys="branchKeys" :render-label="renderLabel" :render-prefix="renderPrefix" :indent="16" block-line show-line aria-label="分组目录" @update:selected-keys="select">
            <template #empty><NEmpty size="small" :description="search.trim()?'没有匹配的分组':'暂无可用分组'" /></template>
          </NTree>
        </div>
        <div class="kb-group-picker-target"><span>目标位置</span><p aria-label="待选分组完整路径">{{ candidatePath || '尚未选择分组' }}</p></div>
      </div>
      <template #footer><div class="kb-group-picker-actions" data-v2-navigation><NButton @click="open=false">取消</NButton><NButton type="primary" :disabled="disabled||!candidatePath" @click="confirm">选择此分组</NButton></div></template>
    </NModal>
  </div>
</template>

<script setup lang="ts">
import {computed,h,ref,watch} from 'vue';
import {NButton,NEmpty,NInput,NModal,NTree,type TreeOption} from 'naive-ui';
import {ChevronDown,Folder,Search} from 'lucide-vue-next';
import {buildFolderTree,folderOptions,toTreeOptions,type GroupItem} from '../workspace-v2/knowledgeFolders';

const props=defineProps<{modelValue:string;groups:GroupItem[];disabled?:boolean}>();
const emit=defineEmits<{'update:modelValue':[value:string]}>();
const open=ref(false),search=ref(''),candidate=ref('');
const paths=computed(()=>new Map(folderOptions(props.groups).map(option=>[option.value,option.label])));
const tree=computed(()=>toTreeOptions(buildFolderTree(props.groups)));
const branchKeys=computed(()=>[...new Set(props.groups.flatMap(group=>group.parent_id?[group.parent_id]:[]))]);
const selectedName=computed(()=>props.groups.find(group=>group.id===props.modelValue)?.name||'');
const selectedPath=computed(()=>paths.value.get(props.modelValue)||'');
const candidatePath=computed(()=>paths.value.get(candidate.value)||'');
function show(){if(props.disabled)return;candidate.value=props.modelValue;search.value='';open.value=true;}
function setOpen(value:boolean){open.value=value;}
function select(keys:Array<string|number>){candidate.value=typeof keys[0]==='string'?keys[0]:'';}
function filter(pattern:string,node:TreeOption){return (paths.value.get(String(node.key))||'').toLocaleLowerCase().includes(pattern.toLocaleLowerCase());}
function renderLabel({option}:{option:TreeOption}){return h('span',{class:'kb-group-picker-label',title:paths.value.get(String(option.key))},String(option.label||''));}
function renderPrefix(){return h(Folder,{size:16,'aria-hidden':true});}
function confirm(){if(props.disabled||!candidatePath.value)return;emit('update:modelValue',candidate.value);open.value=false;}
watch(()=>props.disabled,disabled=>{if(disabled)open.value=false;});
</script>

<style scoped>
.kb-group-picker{flex:1 1 280px;min-width:0;max-width:560px;display:grid;gap:5px}
.kb-group-picker-trigger{width:100%;display:grid;grid-template-columns:17px minmax(0,1fr) 16px;align-items:center;gap:8px;min-height:38px;padding:8px 10px;border:1px solid var(--cf-border,#d8e5f7);border-radius:6px;background:var(--cf-surface,#fff);color:var(--cf-text,#243244);font:inherit;text-align:left;cursor:pointer}
.kb-group-picker-trigger>span{white-space:normal;overflow-wrap:anywhere;line-height:1.5;font-size:13px}
.kb-group-picker-trigger:focus-visible{outline:2px solid var(--cf-brand-blue,#1e63ff);outline-offset:2px}
.kb-group-picker-trigger:disabled{opacity:.55;cursor:not-allowed}
.kb-group-picker-path{margin:0;color:var(--cf-muted,#64748b);font-size:12px;line-height:1.6;overflow-wrap:anywhere;white-space:normal}
:global(.kb-group-picker-dialog.n-modal){width:min(620px,calc(100vw - 24px));max-height:calc(100dvh - 24px);border-radius:8px}
.kb-group-picker-body{height:min(56dvh,480px);min-height:180px;display:grid;grid-template-rows:auto minmax(0,1fr) auto;gap:12px;min-width:0}
.kb-group-picker-tree{min-height:0;min-width:0;overflow:auto;border-block:1px solid var(--cf-border,#d8e5f7);padding-block:6px;overscroll-behavior:contain}
.kb-group-picker-tree :deep(.n-tree-node-content){min-width:0;min-height:36px;align-items:flex-start;padding-block:5px}
.kb-group-picker-tree :deep(.n-tree-node-content__text){min-width:0;white-space:normal;overflow:visible;text-overflow:clip;line-height:1.6}
.kb-group-picker-tree :deep(.kb-group-picker-label){white-space:normal;overflow-wrap:anywhere;font-size:14px}
.kb-group-picker-target{min-width:0;max-height:110px;overflow:auto;font-size:12px;line-height:1.6;color:var(--cf-muted,#64748b)}
.kb-group-picker-target p{margin:4px 0 0;white-space:normal;overflow-wrap:anywhere;font-size:13px;color:var(--cf-text,#243244)}
.kb-group-picker-actions{display:flex;justify-content:flex-end;gap:8px}
@media(max-width:600px){.kb-group-picker{flex-basis:100%;max-width:none}.kb-group-picker-trigger{min-height:44px}:global(.kb-group-picker-dialog.n-modal){width:calc(100vw - 16px);max-height:calc(100dvh - 16px)}.kb-group-picker-body{height:calc(100dvh - 210px);min-height:160px}.kb-group-picker-tree :deep(.n-tree-node-content){min-height:44px}.kb-group-picker-actions :deep(.n-button){min-height:44px}}
</style>
