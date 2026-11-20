<script setup lang="ts">
import { nextTick, onBeforeUnmount, onMounted, ref } from 'vue';

type Rack = { room: string; rack: string; rack_type?: string };
type Detector = { detect: (source: HTMLVideoElement) => Promise<{ rawValue: string }[]> };
const props = defineProps<{ scope: string; racks: Rack[] }>();
const emit = defineEmits<{close: []; select: [room: string, rack: string]}>();
const modal = ref<HTMLDialogElement>(), video = ref<HTMLVideoElement>(), entry = ref<HTMLInputElement>();
const code = ref(''), message = ref(''), choices = ref<Rack[]>([]), active = ref(false), starting = ref(false);
let stream: MediaStream | undefined, timer = 0, generation = 0;
function stop() {
  ++generation;
  clearTimeout(timer);
  stream?.getTracks().forEach(track => track.stop());
  stream = undefined;
  if (video.value) video.value.srcObject = null;
  active.value = false;
  starting.value = false;
}
function lookup() {
  stop();
  const raw = code.value.trim();
  if (!raw) return;
  let rack = raw, room = '', scope = '';
  if (/^https?:\/\//i.test(raw)) {
    try {
      const params = new URL(raw).searchParams;
      rack = params.get('rack') || '';
      room = params.get('room') || '';
      scope = params.get('scope') || '';
    } catch { rack = ''; }
  }
  choices.value = [];
  if (scope && scope.toUpperCase() !== props.scope) { message.value = '此码属于其他楼栋，请切换楼栋后查询。'; return; }
  const hits = props.racks.filter(item => item.rack.toUpperCase() === rack.trim().toUpperCase() && (!room || item.room === room));
  if (hits.length === 1) { emit('select', hits[0].room, hits[0].rack); return; }
  choices.value = hits;
  message.value = hits.length ? '多个包间使用此机柜号，请选择包间。' : '当前楼栋名册中没有匹配的机柜，请核对柜号。';
}
async function camera() {
  stop();
  const DetectorClass = (window as Window & { BarcodeDetector?: new () => Detector }).BarcodeDetector;
  if (!DetectorClass || !navigator.mediaDevices?.getUserMedia) {
    message.value = '当前浏览器无法使用相机识码，可连接扫码枪或输入柜号。';
    entry.value?.focus(); return;
  }
  starting.value = true; message.value = ''; choices.value = [];
  const id = generation;
  try {
    const detector = new DetectorClass();
    const input = await navigator.mediaDevices.getUserMedia({ video: { facingMode: { ideal: 'environment' } }, audio: false });
    if (id !== generation) { input.getTracks().forEach(track => track.stop()); return; }
    stream = input; active.value = true; await nextTick();
    if (!video.value || id !== generation) return;
    video.value.srcObject = stream; await video.value.play();
    if (id !== generation) return;
    starting.value = false;
    async function detect() {
      if (id !== generation || !video.value) return;
      try {
        const [result] = await detector.detect(video.value);
        if (id !== generation) return;
        if (result?.rawValue) { code.value = result.rawValue; lookup(); return; }
        timer = window.setTimeout(detect, 200);
      } catch { if (id === generation) { stop(); message.value = '相机识码中断，可重试或输入柜号。'; } }
    }
    await detect();
  } catch { if (id === generation) { stop(); message.value = '相机未能打开，请检查浏览器权限或输入柜号。'; } }
}
function visibility() { if (document.hidden) stop(); }
onMounted(() => { modal.value?.showModal(); entry.value?.focus(); document.addEventListener('visibilitychange', visibility); });
onBeforeUnmount(() => { stop(); document.removeEventListener('visibilitychange', visibility); });
</script>
<template>
  <dialog ref="modal" class="cab-scan-dialog cab-dialog" aria-label="扫码查机柜" @cancel.prevent="emit('close')">
    <header class="cab-section-head"><h2>{{ scope }}楼 · 扫码查机柜</h2><button @click="emit('close')">关闭</button></header>
    <form class="cab-fields" @submit.prevent="lookup"><label>机柜号或标签链接<input ref="entry" v-model.trim="code" type="search" autocomplete="off" placeholder="扫码枪输入或手动填写" @input="choices=[];message=''"></label>
      <button class="cab-primary" :disabled="!code">查询</button><button type="button" @click="active || starting ? stop() : camera()">{{ active || starting ? '停止相机' : '开启相机' }}</button></form>
    <video v-if="active" ref="video" playsinline muted aria-label="机柜标签取景"/>
    <p role="status">{{ message || (starting ? '正在打开相机…' : active ? '将机柜标签放入取景框。' : '') }}</p>
    <div v-if="choices.length" class="cab-list"><button v-for="r in choices" :key="r.room+'/'+r.rack" @click="emit('select',r.room,r.rack)">{{ r.room }} · {{ r.rack }} · {{ r.rack_type }}</button></div>
  </dialog>
</template>
