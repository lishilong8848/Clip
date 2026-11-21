<script setup lang="ts">
import {toRefs,useId} from 'vue';
import {AlertCircle, Save, X} from 'lucide-vue-next';
import type {LibraryController} from './SerenityDirectory.vue';
const props = defineProps<{model:LibraryController}>();
const model = props.model;
const {modalElement, modalTitleId, modalError, modalDirty, editor, saving, categories, closeModal} = toRefs(model);
const categoryListId = useId();
</script>
<template>
  <Teleport to="body">
    <div class="dir-modal-layer"
        @mousedown.self="!modalDirty && closeModal()"
      >
        <section
          ref="modalElement"
          class="dir-modal" data-v2-inline-feedback
          role="dialog"
          aria-modal="true"
          :aria-labelledby="modalTitleId"
          tabindex="-1"
        >
          <header class="dir-modal-head">
            <h2 :id="modalTitleId" class="dir-modal-title">
              {{ String(editor.id || "") ? "编辑链接" : "新增链接" }}
            </h2>
            <button
              type="button"
              class="dir-icon-btn"
              aria-label="关闭"
              :disabled="saving"
              @click="closeModal"
            >
              <X :size="16" aria-hidden="true" />
            </button>
          </header>

          <form class="dir-form" novalidate @submit.prevent="model.canEdit && model.saveEntry()">
            <div class="dir-fields">
            <label class="dir-field">
              <span class="dir-field-label">名称</span>
              <input v-model="editor.name" type="text" required class="dir-input" :disabled="saving" />
            </label>
            <label class="dir-field">
              <span class="dir-field-label">链接 URL</span>
              <input
                v-model="editor.url"
                type="url"
                required
                class="dir-input"
                :disabled="saving"
                placeholder="https://…"
                spellcheck="false"
                autocomplete="off"
              />
            </label>
            <label class="dir-field">
              <span class="dir-field-label">分类</span>
              <input
                v-model="editor.category"
                type="text"
                class="dir-input"
                :disabled="saving"
                :list="categoryListId"

              />
              <datalist :id="categoryListId">
                <option v-for="name in categories" :key="name" :value="name" />
              </datalist>
            </label>
            <label class="dir-field">
              <span class="dir-field-label">用途</span>
              <textarea
                v-model="editor.purpose"
                class="dir-input dir-textarea"
                rows="3"
                :disabled="saving"
                maxlength="2000"
              />
            </label>
            <label class="dir-field">
              <span class="dir-field-label">排序值（0–1000000）</span>
              <input v-model.number="editor.sort" type="number" min="0" max="1000000" step="1" class="dir-input" :disabled="saving" />
            </label>

            <p v-if="modalError" class="dir-form-error" role="alert">
              <AlertCircle :size="15" aria-hidden="true" />
              <span>{{ modalError }}</span>
            </p>
            </div>

            <footer class="dir-modal-actions">
              <button type="button" class="dir-btn" :disabled="saving" @click="closeModal">取消</button>
              <button type="submit" class="dir-btn dir-btn-primary" :disabled="saving || !model.canEdit">
                <Save :size="16" aria-hidden="true" />
                <span>{{ saving ? "保存中…" : "保存" }}</span>
              </button>
            </footer>
          </form>
        </section>
      </div>
  </Teleport>
</template>
<style scoped>
.dir-modal-layer {position:fixed;inset:0;z-index:2200;display:grid;place-items:center;background:#18334065;padding:12px;}
.dir-modal {min-width:0;width:min(540px,100%);max-height:90dvh;overflow:auto;box-sizing:border-box;padding:clamp(14px,2vw,22px);border-radius:16px;background:var(--soft-face,#f2f6f5);color:var(--v2-ink,#244046);box-shadow:0 12px 50px #19374235;}
.dir-modal-head,.dir-modal-actions {display:flex;align-items:center;justify-content:space-between;gap:8px;}
.dir-modal-title {font-size:20px;margin:0;}.dir-form {display:grid;gap:10px;margin-top:14px;}.dir-field {display:grid;gap:4px;min-width:0;font-size:13px;}
.dir-input {min-width:0;box-sizing:border-box;width:100%;background:var(--soft-field,#e8eeee);border:1px solid #a5b8b9;border-radius:8px;padding:8px 10px;font:inherit;color:inherit;}.dir-textarea {resize:vertical;}
button {display:inline-flex;align-items:center;justify-content:center;gap:6px;min-height:36px;background:var(--soft-face,#f2f6f5);border:1px solid #a5b8b9;border-radius:8px;font:inherit;font-size:14px;color:inherit;padding:6px 12px;cursor:pointer;}
.dir-btn-primary {background:var(--v2-palette-accent);color:var(--v2-palette-on-accent);border-color:transparent;}button:disabled {opacity:.55;cursor:default;}.dir-form-error {padding:7px 10px;color:#803c26;background:#fff0e2;border-radius:8px;margin:0;}.dir-modal-actions {justify-content:flex-end;}
:focus-visible {outline:3px solid var(--v2-blue,#2f82aa);outline-offset:2px;}
.dir-modal {display:flex;flex-direction:column;overflow:hidden;}
.dir-form {display:flex;flex-direction:column;min-height:0;}
.dir-fields {display:grid;align-content:start;gap:9px;min-height:0;overflow:auto;overscroll-behavior:contain;padding:3px;}
.dir-modal-head,.dir-modal-actions {flex:none;}
</style>
