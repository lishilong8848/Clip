<template>
  <section ref="root" class="water-editor" :aria-busy="saving || loading" aria-label="水耗记录填写" @paste="paste">
    <header class="water-editor-head">
      <div><span>{{ scope }}楼 · {{ recordId ? '记录填写' : '新增记录' }}</span><h2>{{ form.meter || '水表记录' }}</h2></div>
      <NButton data-v2-navigation quaternary circle aria-label="关闭水耗编辑" :disabled="saving || uploading" @click="$emit('close')"><X :size="20" /></NButton>
    </header>
    <div v-if="loading" class="water-editor-loading"><NSpin size="small" /><span>正在读取完整记录</span></div>
    <div v-else-if="recordId && !detail.record_id" class="water-editor-loading">
      <CircleAlert :size="28" /><strong>记录读取失败</strong><p>{{ error }}</p><NButton @click="$emit('reload')">重新读取记录</NButton>
    </div>
    <form v-else class="water-editor-form" novalidate @submit.prevent="$emit('save')">
      <div class="water-editor-body">
        <p v-if="recordId && !admin" class="water-policy" :class="{locked}">{{ locked ? '本条记录仅可查看，修改次数已用完或尚未取得修改权限。' : `还可修改 ${detail.edit_policy?.remaining_edits ?? '—'} 次，保存成功后计为一次。` }}</p>
        <p v-if="uncertain" class="water-save-uncertain" aria-live="assertive">上次保存结果尚未确认。请继续原任务核验，当前填写已保留。</p>
        <NForm :model="form" :disabled="disabled" label-placement="top" :show-require-mark="true" class="water-form-grid">
          <NFormItem label="统计日期" required data-water-field="statisticDate" :validation-status="errors.statisticDate ? 'error' : undefined" :feedback="errors.statisticDate">
            <label class="water-date-label"><span class="water-sr-only">统计日期</span><NDatePicker v-model:formatted-value="form.statisticDate" type="date" value-format="yyyy-MM-dd" :disabled="disabled" @update:formatted-value="changed('statisticDate')" /></label>
          </NFormItem>
          <NFormItem label="水表" required data-water-field="meter" :validation-status="errors.meter ? 'error' : undefined" :feedback="errors.meter">
            <NSelect v-model:value="form.meter" :options="selectOptions(options.meters)" filterable :disabled="disabled" aria-label="选择水表" @update:value="changed('meter')" />
          </NFormItem>
          <NFormItem label="统计频次" required data-water-field="frequency" :validation-status="errors.frequency ? 'error' : undefined" :feedback="errors.frequency">
            <NSelect v-model:value="form.frequency" :options="selectOptions(options.frequencies)" :disabled="disabled" aria-label="统计频次" @update:value="changed('frequency')" />
          </NFormItem>
          <NFormItem label="班次" required data-water-field="shift" :validation-status="errors.shift ? 'error' : undefined" :feedback="errors.shift">
            <NSelect v-model:value="form.shift" :options="selectOptions(options.shifts)" :disabled="disabled" aria-label="班次" @update:value="changed('shift')" />
          </NFormItem>
          <NFormItem label="本次水表读数" required class="water-reading" data-water-field="meterValue" :validation-status="errors.meterValue ? 'error' : undefined" :feedback="errors.meterValue">
            <NInputNumber v-model:value="form.meterValue" :min="0" :show-button="false" :disabled="disabled" placeholder="填写水表读数" :input-props="{'aria-label':'本次水表读数','inputmode':'decimal'}" @update:value="changed('meterValue')" />
          </NFormItem>
        </NForm>
        <p v-if="recordId" class="water-reference-label">原记录参考 · 修改后的计算结果以保存后为准</p>
        <dl v-if="recordId" class="water-previous">
          <div><dt>上期日期</dt><dd>{{ detail.previous_date_text || '—' }}</dd></div>
          <div><dt>上期读数</dt><dd>{{ waterNumberText(detail.previous_value_text) }}</dd></div>
          <div><dt>公式耗水量</dt><dd>{{ waterNumberText(detail.computed_usage) }} <small>t</small></dd></div>
          <div><dt>同比</dt><dd>{{ ratio(detail.yoy_ratio) }}</dd></div>
        </dl>
        <div class="water-correction">
          <NButton data-v2-navigation text :aria-expanded="correctionOpen" @click="correctionOpen = !correctionOpen"><ChevronDown :size="15" :class="{rotated:correctionOpen}" />{{ form.correctedUsage === null ? '修正当期耗水量（选填）' : `修正耗水量 ${waterNumberText(form.correctedUsage)} t` }}</NButton>
          <NFormItem v-if="correctionOpen" label="当期耗水量（修正）" data-water-field="correctedUsage" :validation-status="errors.correctedUsage ? 'error' : undefined" :feedback="errors.correctedUsage">
            <NInputNumber v-model:value="form.correctedUsage" :show-button="false" :disabled="disabled" placeholder="留空使用公式结果" :input-props="{'aria-label':'当期耗水量修正'}" @update:value="changed('correctedUsage')" />
          </NFormItem>
        </div>
        <section class="water-photos" data-water-field="photos" :class="{'water-field-error':!!errors.photos}" aria-label="水表照片">
          <header><div><strong>水表照片 <span>*</span></strong><small>{{ readyPhotoCount }} 张已就绪<template v-if="failedPhotos"> · 有照片上传失败</template></small></div>
            <NButton data-v2-navigation size="small" :disabled="disabled || uploading" @click="fileInput?.click()"><Plus :size="15" />添加照片</NButton>
          </header>
          <input ref="fileInput" class="water-hidden-file" type="file" multiple accept="image/*" :disabled="disabled || uploading" aria-label="添加水表照片" @change="filesChanged" />
          <div class="water-photo-drop" :class="{dragging}" role="button" :tabindex="disabled || uploading ? -1 : 0" :aria-disabled="disabled || uploading" aria-label="选择或粘贴水表照片" @click="!disabled && !uploading && fileInput?.click()" @keydown.enter.prevent="!disabled && !uploading && fileInput?.click()" @keydown.space.prevent="!disabled && !uploading && fileInput?.click()" @dragover.prevent="dragging = !disabled && !uploading" @dragleave.prevent="dragging=false" @drop.prevent="drop" @paste="paste">
            <Upload :size="20" /><div><strong>{{ uploading ? '正在上传照片…' : '点击、拖入或粘贴照片' }}</strong><small>单张不超过 8MB</small></div>
          </div>
          <p v-if="errors.photos" class="water-field-feedback" aria-live="assertive">{{ errors.photos }}</p>
          <div v-if="retained.length || photos.length" class="water-photo-grid">
            <article v-for="(photo,index) in retained" :key="photo.image_id">
              <button type="button" data-v2-navigation class="water-photo-preview" :aria-label="'查看水表照片'+(index+1)" @click="$emit('preview',imageUrl(photo.image_id,'original'),photo.name||'水表照片')"><img :src="imageUrl(photo.image_id,'thumb')" :alt="photo.name || '水表照片'" loading="lazy" /></button>
              <span>已保存</span><NButton data-v2-navigation quaternary circle class="water-photo-remove" :disabled="disabled || uploading" :aria-label="'移除已保存照片'+(index+1)" @click="$emit('remove-retained',photo)"><X :size="14" /></NButton>
            </article>
            <article v-for="photo in photos" :key="photo.localId" :class="{failed:photo.error}">
              <button type="button" data-v2-navigation class="water-photo-preview" :aria-label="'查看'+photo.name" @click="$emit('preview',photo.previewUrl,photo.name)"><img :src="photo.previewUrl" :alt="photo.name" /></button>
              <span v-if="photo.uploading"><NSpin :size="12" />上传中</span><span v-else>{{ photo.error ? '上传失败' : '待保存' }}</span>
              <NButton v-if="photo.error" data-v2-navigation size="tiny" class="water-photo-retry" :disabled="disabled || uploading" @click="$emit('retry-photo',photo)">重试此照片</NButton>
              <small v-if="photo.error" class="water-upload-error" :title="photo.error">{{ photo.error }}</small>
              <NButton data-v2-navigation quaternary circle class="water-photo-remove" :disabled="disabled || uploading" :aria-label="'移除'+photo.name" @click="$emit('remove-photo',photo)"><X :size="14" /></NButton>
            </article>
          </div>
        </section>
        <details v-if="recordId" class="water-record-extra"><summary>更多记录信息</summary><dl><div v-for="item in extra" :key="item.label"><dt>{{ item.label }}</dt><dd>{{ item.value || '—' }}</dd></div></dl></details>
        <p v-if="error && detail.record_id || error && !recordId" class="water-editor-error" aria-live="assertive">{{ error }}</p>
      </div>
      <footer class="water-editor-actions"><span>{{ saving ? '正在保存，请等待结果' : uploading ? '照片正在上传' : dirty ? '有未保存修改' : locked ? '仅查看' : '尚未修改' }}</span>
        <NButton v-if="uncertain" type="primary" :loading="saving" :disabled="saving" @click="$emit('retry-save')">继续原保存任务</NButton>
        <NButton v-else type="primary" attr-type="submit" :loading="saving" :disabled="saving || uploading || locked || !dirty"><Save :size="16" />{{ recordId ? '保存记录' : '新增记录' }}</NButton>
      </footer>
    </form>
  </section>
</template>
<script setup lang="ts">
import { computed, nextTick, ref, watch } from 'vue';
import { NButton, NDatePicker, NForm, NFormItem, NInputNumber, NSelect, NSpin } from 'naive-ui';
import { ChevronDown, CircleAlert, Plus, Save, Upload, X } from 'lucide-vue-next';
import type { Dict } from './client';
import { waterNumber, waterNumberText, type WaterDraft, type WaterPhoto } from './water';
const props=defineProps<{scope:string;recordId:string;form:WaterDraft;detail:Dict;options:Dict;retained:Dict[];photos:WaterPhoto[];errors:Record<string,string>;error:string;loading:boolean;saving:boolean;uploading:boolean;locked:boolean;admin:boolean;dirty:boolean;uncertain:boolean}>();
const emit=defineEmits<{close:[];reload:[];save:[];'retry-save':[];change:[field:string];upload:[files:File[]];'retry-photo':[photo:WaterPhoto];'remove-photo':[photo:WaterPhoto];'remove-retained':[photo:Dict];preview:[url:string,name:string]}>();
const root=ref<HTMLElement>(),fileInput=ref<HTMLInputElement>(),dragging=ref(false),correctionOpen=ref(false);
const disabled=computed(()=>props.locked||props.saving||props.uncertain);
const readyPhotoCount=computed(()=>props.retained.length+props.photos.filter(photo=>photo.uploadId).length);
const failedPhotos=computed(()=>props.photos.some(photo=>photo.error));
watch(()=>props.recordId,()=>correctionOpen.value=props.form.correctedUsage!==null);
watch(()=>props.form.correctedUsage,value=>{if(value!==null)correctionOpen.value=true;});
const extra=computed(()=>[{label:'变更编码与描述',value:props.detail.title},{label:'自增编号',value:props.detail.auto_number},{label:'上期耗水量',value:waterNumberText(props.detail.previous_usage)},{label:'创建时间',value:props.detail.created_time}]);
const selectOptions=(values:unknown)=>Array.isArray(values)?values.map(value=>({label:String(value),value:String(value)})):[];
const ratio=(value:unknown)=>waterNumber(value)===null?'—':waterNumberText(Number(value)*100,1)+'%';
function imageUrl(id:string,variant:string){return '/api/capacity/water/images/'+encodeURIComponent(id)+'?'+new URLSearchParams({scope:props.scope,variant});}
function changed(field:string){emit('change',field);}
function filesChanged(event:Event){const input=event.target as HTMLInputElement;emit('upload',[...input.files||[]]);input.value='';}
function drop(event:DragEvent){dragging.value=false;if(!disabled.value&&!props.uploading)emit('upload',[...event.dataTransfer?.files||[]]);}
function paste(event:ClipboardEvent){if(event.defaultPrevented)return;const files=[...event.clipboardData?.files||[]];if(files.length&&!disabled.value&&!props.uploading){event.preventDefault();emit('upload',files);}}
async function focus(field:string){await nextTick();const element=root.value?.querySelector<HTMLElement>(`[data-water-field="${field}"]`);element?.scrollIntoView({block:'nearest',behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth'});element?.querySelector<HTMLElement>('input,button,[tabindex="0"]')?.focus({preventScroll:true});}
defineExpose({focus});
</script>
<style scoped>
.water-editor{display:grid;grid-template-rows:auto minmax(0,1fr);height:100%;min-height:0;background:#fff;border:1px solid var(--v2-border);border-radius:8px;overflow:hidden}.water-editor-head{display:flex;align-items:center;justify-content:space-between;padding:15px 18px;border-bottom:1px solid var(--v2-border);gap:12px}.water-editor-head span{font-size:11px;color:var(--v2-muted)}.water-editor-head h2{margin:4px 0 0;font-size:17px;line-height:1.4;overflow-wrap:anywhere}.water-editor-form{display:grid;grid-template-rows:minmax(0,1fr) auto;min-height:0}.water-editor-body{overflow:auto;overscroll-behavior:contain;padding:16px 18px;min-height:0}.water-form-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:0 12px}.water-form-grid :deep(.n-form-item){min-width:0}.water-form-grid :deep(.n-date-picker),.water-form-grid :deep(.n-input-number){width:100%}.water-reading{grid-column:1/-1}.water-reading :deep(.n-input__input-el){font-size:21px;font-variant-numeric:tabular-nums}.water-reading :deep(.n-input){height:46px}.water-previous{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:14px 12px;margin:0 0 18px;padding:12px 0;border-block:1px solid var(--v2-border)}.water-previous dt,.water-record-extra dt{font-size:11px;color:var(--v2-muted)}.water-previous dd{font-size:15px;font-variant-numeric:tabular-nums;margin:5px 0 0}.water-previous small{font-size:11px;color:var(--v2-muted)}.water-correction{margin-bottom:16px}.water-correction>.n-button{font-size:12px;max-width:100%}.water-correction :deep(.n-button__content){white-space:normal}.water-correction .n-form-item{margin-top:12px}.water-correction svg{transition:transform 200ms}.water-correction svg.rotated{transform:rotate(180deg)}.water-policy,.water-save-uncertain{padding:10px 12px;background:#edf4ff;border-left:3px solid #2e78c6;font-size:12px;line-height:1.6;margin:0 0 16px;border-radius:4px}.water-policy.locked,.water-save-uncertain{background:#fff7e8;border-color:#bb812b}.water-photos header{display:flex;align-items:center;justify-content:space-between;gap:10px}.water-photos header strong{font-size:13px}.water-photos header strong span{color:#c93d4c}.water-photos header small{display:block;font-size:11px;color:var(--v2-muted);margin-top:4px}.water-photo-drop{display:flex;align-items:center;justify-content:center;gap:12px;min-height:72px;margin-top:10px;border:1px dashed #a9bdcc;border-radius:6px;background:#f8fafc;color:#55778b;cursor:pointer;transition:background-color 180ms,border-color 180ms}.water-photo-drop[aria-disabled=true]{cursor:default;opacity:.65}.water-photo-drop.dragging{background:#eaf4ff;border-color:#0071e3}.water-photo-drop strong{display:block;font-size:12px}.water-photo-drop small{display:block;font-size:11px;margin-top:4px}.water-photo-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:8px;margin-top:12px}.water-photo-grid article{position:relative;min-width:0;border:1px solid var(--v2-border);border-radius:6px;overflow:hidden;background:#fff}.water-photo-grid article.failed{border-color:#d66969}.water-photo-preview{display:block;width:100%;padding:0;border:0;background:#f4f6f8;cursor:zoom-in;aspect-ratio:4/3;overflow:hidden}.water-photo-preview img{display:block;width:100%;height:100%;object-fit:cover}.water-photo-grid article>span{display:flex;align-items:center;gap:5px;padding:6px 8px;font-size:11px;color:var(--v2-muted)}.water-photo-remove{position:absolute;right:3px;top:3px;background:#fff!important}.water-photo-retry{margin:0 6px 6px}.water-upload-error{display:block;margin:0 6px 6px;font-size:10px;line-height:1.5;color:#a63945;overflow-wrap:anywhere}.water-hidden-file{position:absolute;width:1px;height:1px;clip-path:inset(50%);overflow:hidden}.water-field-feedback,.water-editor-error{color:#b53645;font-size:12px;line-height:1.6}.water-field-error .water-photo-drop{border-color:#c93d4c}.water-record-extra{border-top:1px solid var(--v2-border);margin-top:18px;font-size:12px}.water-record-extra summary{min-height:40px;display:flex;align-items:center;cursor:pointer;color:var(--v2-muted)}.water-record-extra dl{display:grid;gap:12px;margin:4px 0 0}.water-record-extra dd{margin:4px 0 0;overflow-wrap:anywhere}.water-editor-actions{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:12px 18px;border-top:1px solid var(--v2-border);background:#fff}.water-editor-actions>span{font-size:11px;color:var(--v2-muted)}.water-editor-loading{display:flex;flex-direction:column;justify-content:center;align-items:center;gap:14px;padding:36px 20px;color:var(--v2-muted);text-align:center}.water-editor-loading p{font-size:13px;line-height:1.6;margin:0}
@media(max-width:760px){.water-editor{border:0;border-radius:0;height:100dvh}.water-editor-head{padding:12px 14px}.water-editor-head .n-button{width:44px;height:44px}.water-editor-body{padding:14px}.water-form-grid{gap:0 10px}.water-editor-actions{padding:10px 14px calc(10px + env(safe-area-inset-bottom))}.water-editor-actions .n-button{height:44px}.water-photo-remove{width:34px;height:34px}.water-photo-drop{min-height:70px}}
@media(prefers-reduced-motion:reduce){.water-correction svg,.water-photo-drop{transition:none}}
.water-date-label{display:block;width:100%;min-width:0}.water-sr-only{position:absolute;width:1px;height:1px;overflow:hidden;clip-path:inset(50%);white-space:nowrap}
/* Keep the primary action clear of the assistant's default bottom-right position. */
.water-editor-actions{flex-direction:row-reverse}
.water-reference-label{margin:0 0 8px;font-size:11px;color:var(--v2-muted)}
.water-editor{animation:water-editor-enter 220ms cubic-bezier(.22,.61,.36,1)}@keyframes water-editor-enter{from{opacity:.25;transform:translateX(10px)}to{opacity:1;transform:translateX(0)}}@media(prefers-reduced-motion:reduce){.water-editor{animation:none}}
</style>
