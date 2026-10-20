import { computed, onBeforeUnmount, ref, watch } from 'vue';
import { requestJson, type Dict, type ApiRequestOptions } from './api/client';

type Phase = 'idle' | 'permission' | 'recording' | 'transcribing' | 'synthesizing' | 'playing';
type Capability = { available: boolean; note: string };
type Token = { owner: string; generation: number };
const MAX_BYTES = 20 * 1024 * 1024, MAX_SECONDS = 120;
const unavailable = (note: string) => ({ transcription: { available: false, note }, synthesis: { available: false, note } });

export function useLighthouseVoice(options: { owner: () => string; active: () => boolean; onTranscript: (text: string) => void }) {
  const phase = ref<Phase>('idle'), elapsed = ref(0), error = ref(''), playingId = ref('');
  const capabilities = ref(unavailable('正在检测语音服务'));
  const configuration = ref<Dict | null>(null);
  let disposed = false, generation = 0, capabilitySerial = 0;
  let capabilityController: AbortController | undefined;
  const controllers = new Set<AbortController>();
  let recorder: MediaRecorder | undefined, stream: MediaStream | undefined, audio: HTMLAudioElement | undefined;
  let elapsedTimer = 0, stopTimer = 0;
  const token = (): Token => ({ owner: options.owner(), generation });
  const current = (value: Token) => !disposed && !!value.owner && value.owner === options.owner() && value.generation === generation;
  const active = (value: Token) => current(value) && options.active();
  const recordingType = () => typeof MediaRecorder === 'undefined' ? '' : ['audio/webm;codecs=opus', 'audio/webm', 'audio/mp4', 'audio/ogg;codecs=opus'].find(type => MediaRecorder.isTypeSupported?.(type)) || '';
  const microphoneReason = computed(() => {
    if (!window.isSecureContext) return '麦克风需要 HTTPS 或 localhost 安全环境';
    if (!navigator.mediaDevices?.getUserMedia || !recordingType()) return '当前浏览器不支持 WebM、M4A 或 Ogg 麦克风录音';
    return capabilities.value.transcription.available ? '' : capabilities.value.transcription.note;
  });
  const synthesisReason = computed(() => capabilities.value.synthesis.available ? '' : capabilities.value.synthesis.note);

  async function request(path: string, init: ApiRequestOptions, value: Token): Promise<Dict> {
    if (!current(value)) throw new DOMException('Cancelled', 'AbortError');
    const controller = new AbortController(); controllers.add(controller);
    try {
      const data = await requestJson(path, { timeoutMs: 180000, ...init, signal: controller.signal });
      if (!current(value) || controller.signal.aborted) throw new DOMException('Cancelled', 'AbortError');
      return data;
    } finally { controllers.delete(controller); }
  }
  function stopTracks(): void { stream?.getTracks().forEach(track => track.stop()); stream = undefined; }
  function clearTimers(): void { window.clearInterval(elapsedTimer); window.clearTimeout(stopTimer); elapsedTimer = 0; stopTimer = 0; }
  function stopAudio(): void {
    if (audio) { const previous = audio; audio = undefined; previous.onended = null; previous.onerror = null; previous.pause(); previous.removeAttribute('src'); previous.load(); }
    playingId.value = '';
  }
  function reset(): void {
    ++generation; ++capabilitySerial; capabilityController?.abort();
    for (const controller of controllers) controller.abort();
    controllers.clear(); clearTimers();
    if (recorder) { const previous = recorder; recorder = undefined; previous.ondataavailable = null; previous.onstop = null; previous.onerror = null; if (previous.state !== 'inactive') previous.stop(); }
    stopTracks(); stopAudio(); phase.value = 'idle'; elapsed.value = 0; error.value = '';
  }
  async function refreshCapabilities(): Promise<void> {
    const value = token(); if (!current(value)) return;
    capabilityController?.abort(); const controller = new AbortController(); capabilityController = controller;
    const serial = ++capabilitySerial; capabilities.value = unavailable('正在检测语音服务');
    try {
      const data = await requestJson('/api/assistant/voice', { signal: controller.signal, cache: 'no-store', timeoutMs: 12000 });
      if (!current(value) || controller.signal.aborted || serial !== capabilitySerial) return;
      const raw = data.capabilities || data;
      const normalize = (item: Dict | undefined, fallback: boolean, note: string): Capability => ({ available: item ? item.available === true : fallback, note: item?.note || note });
      capabilities.value = {
        transcription: normalize(raw.transcription, raw.stt === true, '语音识别尚未配置，请联系管理员'),
        synthesis: normalize(raw.synthesis, raw.tts === true, '语音播报尚未配置，请联系管理员'),
      };
      configuration.value = data.can_manage === true && data.configuration ? data.configuration : null;
    } catch (cause) {
      if (current(value) && !controller.signal.aborted && serial === capabilitySerial) { capabilities.value = unavailable(cause instanceof Error ? cause.message : '语音服务检测失败'); configuration.value = null; }
    } finally { if (capabilityController === controller) capabilityController = undefined; }
  }
  async function transcribe(parts: Blob[], mime: string, value: Token): Promise<void> {
    if (!active(value)) return;
    phase.value = 'transcribing';
    try {
      const suffix = ({ 'audio/webm': '.webm', 'audio/mp4': '.m4a', 'audio/ogg': '.ogg' } as Record<string, string>)[mime.split(';')[0].trim().toLowerCase()];
      if (!suffix) throw new Error('录音设备返回了不支持的音频格式，请换用其他浏览器');
      const file = new File(parts, 'lighthouse-voice' + suffix, { type: mime });
      if (!file.size || file.size > MAX_BYTES) throw new Error('录音须非空且不超过20MiB');
      const body = new FormData(); body.append('files', file);
      const receipt = await request('/api/assistant/files', { method: 'POST', body }, value);
      if (!active(value)) return;
      const id = receipt.files?.[0]?.id;
      if (typeof id !== 'string' || !id) throw new Error('录音上传未完成');
      const result = await request('/api/assistant/voice', { method: 'POST', body: JSON.stringify({ action: 'transcribe', file_id: id }) }, value);
      if (!active(value)) return;
      if (typeof result.text !== 'string' || !result.text.trim()) throw new Error('未识别到文字，请重试');
      options.onTranscript(result.text);
    } catch (cause) { if (active(value)) error.value = cause instanceof Error ? cause.message : '录音转写失败'; }
    finally { if (active(value)) { phase.value = 'idle'; elapsed.value = 0; } }
  }
  function finishRecording(): void {
    if (!recorder || recorder.state === 'inactive') return;
    phase.value = 'transcribing'; clearTimers();
    recorder.stop(); stopTracks();
  }
  async function toggleRecording(): Promise<void> {
    const value = token(); if (!active(value)) return;
    if (phase.value === 'recording') { finishRecording(); return; }
    if (phase.value !== 'idle') return;
    if (microphoneReason.value) { error.value = microphoneReason.value; return; }
    phase.value = 'permission'; error.value = '';
    try {
      const granted = await navigator.mediaDevices.getUserMedia({ audio: true });
      if (!active(value)) { granted.getTracks().forEach(track => track.stop()); return; }
      stream = granted;
      const requestedType = recordingType();
      const recording = new MediaRecorder(granted, { mimeType: requestedType, audioBitsPerSecond: 128000 });
      recorder = recording;
      const parts: Blob[] = []; let bytes = 0, failed = false;
      recording.ondataavailable = event => {
        if (!active(value) || recorder !== recording || !event.data.size) return;
        parts.push(event.data); bytes += event.data.size;
        if (bytes > MAX_BYTES) { failed = true; error.value = '录音超过20MiB，请缩短后重试'; finishRecording(); }
      };
      recording.onerror = () => {
        if (!active(value) || recorder !== recording) return;
        failed = true; recorder = undefined; recording.ondataavailable = null; recording.onstop = null; recording.onerror = null;
        if (recording.state !== 'inactive') recording.stop();
        error.value = '录音设备出错，请重试'; clearTimers(); stopTracks(); phase.value = 'idle'; elapsed.value = 0;
      };
      recording.onstop = () => {
        if (!active(value) || recorder !== recording) return;
        recorder = undefined; clearTimers(); stopTracks();
        if (failed) { phase.value = 'idle'; elapsed.value = 0; return; }
        void transcribe(parts, recording.mimeType || requestedType, value);
      };
      recording.start(1000); phase.value = 'recording'; elapsed.value = 0;
      const started = Date.now();
      elapsedTimer = window.setInterval(() => { elapsed.value = Math.min(MAX_SECONDS, Math.floor((Date.now() - started) / 1000)); }, 250);
      stopTimer = window.setTimeout(finishRecording, MAX_SECONDS * 1000);
    } catch (cause) {
      if (!active(value)) return;
      stopTracks(); recorder = undefined; clearTimers(); phase.value = 'idle';
      const code = cause instanceof Error ? cause.name : '';
      error.value = ['NotAllowedError', 'SecurityError'].includes(code) ? '麦克风权限未获允许，请在浏览器设置中授权后重试'
        : code === 'NotFoundError' ? '未检测到可用麦克风'
        : cause instanceof Error ? cause.message : '无法启动录音，请检查麦克风';
    }
  }
  async function readReply(id: string, text: string): Promise<void> {
    const value = token(); if (!active(value)) return;
    if (phase.value === 'playing' && playingId.value === id) { stopAudio(); phase.value = 'idle'; return; }
    if (!['idle', 'playing'].includes(phase.value)) return;
    if (synthesisReason.value) { error.value = synthesisReason.value; return; }
    if (!text.trim() || text.length > 4000) { error.value = '语音播报每次支持1至4000字，请选择较短的回复'; return; }
    stopAudio(); phase.value = 'synthesizing'; error.value = '';
    try {
      const result = await request('/api/assistant/voice', { method: 'POST', body: JSON.stringify({ action: 'synthesize', text }) }, value);
      if (!active(value)) return;
      if (typeof result.url !== 'string' || !/^\/api\/assistant\/files\/[a-f0-9]{32}$/.test(result.url)) throw new Error('语音服务未返回有效的个人文件地址');
      const player = new Audio(result.url); audio = player; playingId.value = id; phase.value = 'playing';
      const ended = (failed = false) => {
        if (!active(value) || audio !== player) return;
        stopAudio(); phase.value = 'idle'; if (failed) error.value = '播放未完成，请检查浏览器播放权限后重试';
      };
      player.onended = () => ended(); player.onerror = () => ended(true);
      try { await player.play(); } catch { ended(true); }
    } catch (cause) { if (active(value)) { stopAudio(); phase.value = 'idle'; error.value = cause instanceof Error ? cause.message : '语音播报失败'; } }
  }
  watch([options.active, options.owner], ([visible, owner], previous) => {
    reset();
    if (!previous || owner !== previous[1]) { configuration.value = null; capabilities.value = unavailable('语音服务尚未检测'); }
    if (visible && owner) void refreshCapabilities();
  }, { immediate: true, flush: 'sync' });
  function dispose(): void { disposed = true; reset(); configuration.value = null; capabilities.value = unavailable('语音已关闭'); }
  onBeforeUnmount(dispose);
  return { phase, elapsed, error, capabilities, configuration, playingId, microphoneReason, synthesisReason, toggleRecording, readReply, refreshCapabilities, dispose };
}
