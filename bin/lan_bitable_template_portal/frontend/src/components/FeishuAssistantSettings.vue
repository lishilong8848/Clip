<template>
  <form class="feishu-settings" aria-label="飞书智能体配置" @submit.prevent="save">
    <header>
      <h3>飞书智能体</h3>
      <button type="button" class="icon-button" aria-label="重新读取飞书智能体配置" title="重新读取" :disabled="busy" @click="load"><RefreshCw :size="17" :class="{ spin: loading }" /></button>
    </header>
    <p class="scope-note">独立消息应用，不修改灯塔的飞书登录或多维表应用凭证。保存后重启程序生效。</p>
    <p v-if="loading" role="status"><Loader2 :size="16" class="spin" />正在读取配置…</p>
    <fieldset :disabled="busy || !loaded">
      <label class="toggle"><input v-model="enabled" type="checkbox" /><span>启用飞书智能体长连接</span></label>
      <label>App ID<input v-model="appId" type="text" maxlength="124" autocomplete="off" spellcheck="false" placeholder="cli_…" /></label>
      <label>App Secret<input v-model="appSecret" type="password" maxlength="500" autocomplete="new-password" spellcheck="false" :placeholder="hasSecret ? '已配置，留空保留原密钥' : '填写消息应用的 App Secret'" @copy.prevent @cut.prevent @contextmenu.prevent @dragstart.prevent /></label>
      <p class="scope-note">{{ hasSecret ? '密钥已保存，不向浏览器返回原值。' : '尚未配置密钥。' }}更换 App ID 时需重新填写对应密钥。</p>
    </fieldset>
    <p v-if="error" class="failure" role="alert"><AlertCircle :size="16" />{{ error }}</p>
    <p v-if="message" class="saved" role="status"><CheckCircle2 :size="16" />{{ message }}</p>
    <p v-else-if="restartRequired" class="pending" role="status">配置已保存，等待程序重启后生效。</p>
    <footer>
      <span class="scope-note">飞书开放平台需启用机器人、长连接事件订阅 im.message.receive_v1，并发布应用。</span>
      <button type="submit" class="primary" :disabled="busy || !loaded || !appId.trim() || ((!hasSecret || appId.trim() !== savedAppId) && !appSecret.trim())"><Loader2 v-if="saving" :size="16" class="spin" /><Save v-else :size="16" />{{ saving ? '保存中…' : '保存配置' }}</button>
    </footer>
  </form>
</template>

<script setup lang="ts">
import { onBeforeUnmount, onMounted, ref, computed } from 'vue';
import { AlertCircle, CheckCircle2, Loader2, RefreshCw, Save } from 'lucide-vue-next';
import { requestJson } from '../api/client';

const enabled = ref(false), appId = ref(''), appSecret = ref(''), savedAppId = ref('');
const hasSecret = ref(false), restartRequired = ref(false), loaded = ref(false);
const revision = ref(''), loading = ref(false), saving = ref(false), error = ref(''), message = ref('');
const busy = computed(() => loading.value || saving.value);
let disposed = false;

function apply(data: Record<string, any>): void {
  enabled.value = data.enabled === true;
  appId.value = savedAppId.value = String(data.app_id || '');
  appSecret.value = '';
  hasSecret.value = !!data.has_secret;
  restartRequired.value = !!data.restart_required;
  revision.value = String(data.revision || '');
  loaded.value = true;
}

async function load(): Promise<void> {
  loading.value = true;
  error.value = message.value = '';
  try {
    const data = await requestJson('/api/assistant/feishu-settings');
    if (!disposed) apply(data);
  } catch (cause) {
    if (!disposed) error.value = cause instanceof Error ? cause.message : '配置读取失败';
  } finally { loading.value = false; }
}

async function save(): Promise<void> {
  if (busy.value || !loaded.value) return;
  saving.value = true;
  error.value = message.value = '';
  try {
    const data = await requestJson('/api/assistant/feishu-settings', { method: 'POST', body: JSON.stringify({
      app_id: appId.value.trim(), app_secret: appSecret.value, enabled: enabled.value, revision: revision.value,
    }) });
    if (!disposed) {
      apply(data);
      message.value = data.restart_required ? '配置已保存，请重启程序后使用新配置。当前会话未中断。' : '配置已保存。';
    }
  } catch (cause) {
    if (!disposed) error.value = cause instanceof Error ? cause.message : '配置保存失败';
  } finally { saving.value = false; }
}

onMounted(() => { void load(); });
onBeforeUnmount(() => { disposed = true; appSecret.value = ''; });
</script>

<style scoped>
.feishu-settings { display: grid; gap: 14px; max-width: 760px; color: var(--cf-text, #17365b); }
header, footer { display: flex; gap: 12px; align-items: center; justify-content: space-between; }
h3, p { margin: 0; }
h3 { font-size: 17px; }
.scope-note { font-size: 12px; color: var(--cf-muted, #63758a); line-height: 1.6; }
fieldset { display: grid; gap: 14px; min-width: 0; margin: 0; padding: 0; border: 0; }
label { display: grid; gap: 6px; font-size: 14px; }
input:not([type=checkbox]) { box-sizing: border-box; width: 100%; min-height: 38px; border: 1px solid #cad9ec; border-radius: 8px; padding: 8px 10px; color: inherit; background: #fff; font: inherit; }
input:focus-visible, button:focus-visible { outline: 2px solid #2469e8; outline-offset: 2px; }
.toggle { display: flex; align-items: center; gap: 8px; }
.toggle input { accent-color: #2469e8; }
button { display: inline-flex; align-items: center; justify-content: center; gap: 6px; cursor: pointer; font: inherit; border-radius: 8px; flex-shrink: 0; }
.primary { border: 0; background: #2469e8; color: #fff; min-height: 38px; padding: 0 14px; }
.icon-button { border: 1px solid #cad9ec; width: 36px; height: 36px; color: #2469e8; background: #fff; }
button:disabled { opacity: .55; cursor: not-allowed; }
.failure, .saved { display: flex; align-items: center; gap: 8px; font-size: 13px; }
.failure { color: #ae2539; } .saved { color: #087655; } .pending { color: #92620d; font-size: 13px; }
.spin { animation: feishu-spin 1s linear infinite; }
@keyframes feishu-spin { to { transform: rotate(360deg); } }
</style>
