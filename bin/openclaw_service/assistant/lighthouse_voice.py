"""Opt-in speech transport; conversation routing and business permissions stay upstream.

Voice(store, files): capabilities(), configuration(actor), configure(actor, payload).
Configuration lives only at lighthouse_ai/voice: allowed_origins plus stt/tts
profiles (enabled, endpoint, model, api_key; tts also requires voice). Endpoints
are complete HTTPS /audio/transcriptions or /audio/speech URLs. No model config
is inherited. Configuration reads/writes require an authenticated administrator.

await transcribe(actor, file_id) returns {'text': ...}; it does not chat.
await synthesize(actor, text) returns an ordinary private file receipt (WAV).
Routes must resolve the original actor and topic before calling these methods,
then send transcripts through the original text validation/conversation handler.
The parent extends LighthouseFiles.upload to accept audio without extraction.
"""
import asyncio
import copy
import hashlib
import io
import ipaddress
import json
import re
import threading
import uuid
import wave
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import httpx

from upload_event_module.services.http_client import verified_tls_context
from .lighthouse_ai import AssistantError, NAMESPACE, protect_key, safe_text, unprotect_key
from .lighthouse_files import EXTRACTION_VERSION, FILE_NAMESPACE

CONFIG_KEY = 'voice'
MAX_AUDIO_BYTES = 20 * 1024 * 1024
MAX_TEXT_CHARS = 4000
MAX_TRANSCRIPT_CHARS = 12000
REQUEST_TIMEOUT = 60
UNSUPPORTED = '语音识别尚未配置，请联系管理员配置语音服务，或改发文字。'
TTS_UNSUPPORTED = '语音播报尚未配置，请联系管理员配置语音服务。'
_TYPES = {'.wav': 'audio/wav', '.mp3': 'audio/mpeg', '.ogg': 'audio/ogg',
          '.flac': 'audio/flac', '.m4a': 'audio/mp4', '.webm': 'audio/webm'}


def _actor(actor, admin=False):
    if (not isinstance(actor, dict) or not isinstance(actor.get('id'), str) or not actor['id'].strip()
            or actor.get('is_guest') or actor.get('role') == 'guest'):
        raise AssistantError('请使用正式账号登录。', 403)
    if admin and actor.get('is_admin') is not True:
        raise AssistantError('仅管理员可配置语音服务。', 403)


def _url(value, *, origin=False, kind='stt'):
    try:
        if (not isinstance(value, str) or len(value) > 1000 or not value.isascii()
                or any(c.isspace() or ord(c) < 32 for c in value) or '\\' in value or '%' in value):
            raise ValueError()
        parts = urlsplit(value)
        host, port = parts.hostname, parts.port
        if (parts.scheme != 'https' or not host or parts.username is not None or parts.password is not None
                or parts.query or parts.fragment or '?' in value or '#' in value or port == 0):
            raise ValueError()
        try:
            address = ipaddress.ip_address(host)
        except ValueError:
            address = None
        if host in {'localhost', 'localhost.localdomain'} or host.endswith('.localhost'):
            raise ValueError()
        if address:
            if address.is_loopback or address.is_link_local or address.is_unspecified or address.is_multicast:
                raise ValueError()
        elif (len(host) > 253 or any(not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', p)
                                    for p in host.split('.'))):
            raise ValueError()
        authority = ('[' + host + ']' if ':' in host else host) + (':' + str(port) if port and port != 443 else '')
        base = 'https://' + authority
        path = parts.path
        if origin:
            if path not in {'', '/'}:
                raise ValueError()
            return base
        ending = '/audio/transcriptions' if kind == 'stt' else '/audio/speech'
        if (not path.endswith(ending) or not re.fullmatch(r'/[A-Za-z0-9_./-]+', path)
                or any(p in {'.', '..'} for p in path.split('/')) or '//' in path):
            raise ValueError()
        return urlunsplit(('https', authority, path, '', ''))
    except (ValueError, TypeError):
        raise AssistantError('语音服务须使用允许列表中的完整HTTPS地址，不含账号、查询参数或跳转。') from None


def validate_audio(content, name=''):
    """Validate a bounded audio container and normalize its upload MIME/name."""
    if not isinstance(content, bytes) or not content or len(content) > MAX_AUDIO_BYTES:
        raise AssistantError('音频须非空，且不超过20MiB。', 413)
    suffix = ''
    if content.startswith(b'RIFF') and content[8:12] == b'WAVE':
        try:
            with wave.open(io.BytesIO(content), 'rb') as audio:
                length = audio.getnframes() * audio.getnchannels() * audio.getsampwidth()
                if (not length or length > len(content) or audio.getframerate() <= 0
                        or len(audio.readframes(audio.getnframes())) != length):
                    raise ValueError()
            suffix = '.wav'
        except (wave.Error, EOFError, ValueError):
            pass
    elif content.startswith(b'OggS') and (b'OpusHead' in content[:256] or b'\x01vorbis' in content[:256]):
        suffix = '.ogg'
    elif content.startswith(b'fLaC') and len(content) >= 42:
        suffix = '.flac'
    elif content[4:8] == b'ftyp' and b'soun' in content and b'vide' not in content:
        suffix = '.m4a'
    elif (content.startswith(b'\x1aE\xdf\xa3') and b'webm' in content[:128]
          and any(codec in content[:65536] for codec in (b'A_OPUS', b'A_VORBIS'))
          and b'V_' not in content[:65536]):
        suffix = '.webm'
    else:
        offset = 0
        if content.startswith(b'ID3') and len(content) >= 10 and not any(b & 128 for b in content[6:10]):
            offset = 10 + sum(b << shift for b, shift in zip(content[6:10], (21, 14, 7, 0)))
            if content[5] & 16:
                offset += 10
        header = content[offset:offset + 4]
        if (len(header) == 4 and header[0] == 255 and header[1] & 224 == 224
                and header[1] & 24 != 8 and header[1] & 6 and header[2] >> 4 not in {0, 15}
                and header[2] & 12 != 12):
            suffix = '.mp3'
    if not suffix:
        raise AssistantError('无法识别音频格式，请发送WAV、MP3、Ogg/Opus、FLAC、M4A或WebM音频。', 415)
    clean = re.sub(r'[\x00-\x1f]', '', str(name or '').replace('\\', '/').split('/')[-1])[:180]
    supplied = Path(clean).suffix.lower()
    aliases = {'.opus': '.ogg', '.wave': '.wav', '.mp4': '.m4a'}
    if supplied and aliases.get(supplied, supplied) != suffix:
        raise AssistantError('音频文件扩展名与实际格式不一致。', 415)
    return 'voice' + suffix, _TYPES[suffix]


class Voice:
    def __init__(self, store, files, *, client_factory=None, protect=protect_key, unprotect=unprotect_key):
        self.store, self.files = store, files
        self._client_factory, self._protect, self._unprotect = client_factory, protect, unprotect
        self._lock = threading.RLock()

    def _config(self):
        return copy.deepcopy(self.store.get_document(NAMESPACE, CONFIG_KEY) or
                             {'allowed_origins': [], 'stt': {}, 'tts': {}})

    @staticmethod
    def _profile(saved, kind):
        item = saved.get(kind) or {}
        if not item.get('enabled') or not item.get('key_cipher') or not item.get('model'):
            raise AssistantError(UNSUPPORTED if kind == 'stt' else TTS_UNSUPPORTED, 503, category='voice_unconfigured')
        endpoint = _url(item.get('endpoint'), kind=kind)
        if _url('https://' + urlsplit(endpoint).netloc, origin=True) not in saved.get('allowed_origins', []):
            raise AssistantError('语音服务地址尚未获管理员授权。', 403)
        if kind == 'tts' and not item.get('voice'):
            raise AssistantError(TTS_UNSUPPORTED, 503, category='voice_unconfigured')
        return {**item, 'endpoint': endpoint}

    def capabilities(self):
        saved = self._config()
        result = {}
        for kind in ('stt', 'tts'):
            try:
                self._profile(saved, kind)
                result[kind] = True
            except AssistantError:
                result[kind] = False
        return {**result, 'unsupported_message': UNSUPPORTED, 'max_audio_bytes': MAX_AUDIO_BYTES,
                'max_text_chars': MAX_TEXT_CHARS, 'audio_types': list(_TYPES.values()), 'output_mime': 'audio/wav'}

    def configuration(self, actor):
        _actor(actor, True)
        saved = self._config()
        return {'allowed_origins': saved['allowed_origins'], **{kind: {
            **{key: saved[kind].get(key, False if key == 'enabled' else '')
               for key in ('enabled', 'endpoint', 'model', *(['voice'] if kind == 'tts' else []))},
            'has_key': bool(saved[kind].get('key_cipher'))} for kind in ('stt', 'tts')}}

    def configure(self, actor, payload):
        _actor(actor, True)
        if not isinstance(payload, dict) or set(payload) - {'allowed_origins', 'stt', 'tts'}:
            raise AssistantError('语音配置格式无效。')
        with self._lock:
            saved = self._config()
            allowed = payload.get('allowed_origins', saved['allowed_origins'])
            if not isinstance(allowed, list) or len(allowed) > 10:
                raise AssistantError('语音服务允许列表须为最多10个HTTPS来源。')
            saved['allowed_origins'] = list(dict.fromkeys(_url(v, origin=True) for v in allowed))
            for kind in ('stt', 'tts'):
                patch = payload.get(kind, {})
                fields = {'enabled', 'endpoint', 'model', 'api_key', 'clear_key'} | ({'voice'} if kind == 'tts' else set())
                if not isinstance(patch, dict) or set(patch) - fields:
                    raise AssistantError('语音配置格式无效。')
                item = saved[kind]
                old_endpoint = item.get('endpoint', '')
                for field in ('enabled', 'clear_key'):
                    if field in patch and not isinstance(patch[field], bool):
                        raise AssistantError('语音配置开关无效。')
                if 'enabled' in patch:
                    item['enabled'] = patch['enabled']
                if 'endpoint' in patch:
                    if not isinstance(patch['endpoint'], str):
                        raise AssistantError('语音服务地址格式无效。')
                    item['endpoint'] = _url(patch['endpoint'], kind=kind) if patch['endpoint'] else ''
                    if urlsplit(item['endpoint']).netloc != urlsplit(old_endpoint).netloc:
                        item.pop('key_cipher', None)
                for field in ('model', 'voice'):
                    if field in patch:
                        value = patch[field]
                        if not isinstance(value, str) or value and not re.fullmatch(r'[A-Za-z0-9_./:-]{1,200}', value):
                            raise AssistantError('请填写有效的语音模型和音色标识。')
                        item[field] = value
                secret = patch.get('api_key', '')
                if not isinstance(secret, str) or len(secret) > 4000 or any(ord(c) < 33 or ord(c) > 126 for c in secret):
                    raise AssistantError('语音认证信息格式无效。')
                try:
                    if secret:
                        item['key_cipher'] = self._protect(secret)
                except Exception:
                    raise AssistantError('语音凭证加密未完成。', 503) from None
                if patch.get('clear_key'):
                    item.pop('key_cipher', None)
                    item['enabled'] = False
                if item.get('enabled'):
                    self._profile(saved, kind)
            try:
                self.store.put_document(NAMESPACE, CONFIG_KEY, saved)
            except Exception:
                raise AssistantError('语音配置保存未完成。', 503) from None
        return self.configuration(actor)

    def _input(self, actor, identity):
        if not isinstance(identity, str):
            raise AssistantError('语音附件编号无效。', 404)
        item = self.files.get(actor, identity)
        if item.get('owner') != actor['id']:
            raise AssistantError('仅可转写本人上传的语音附件。', 403)
        try:
            with Path(item['path']).open('rb') as source:
                content = source.read(MAX_AUDIO_BYTES + 1)
        except OSError:
            raise AssistantError('语音原文件不可用，请重新上传。', 404) from None
        name, mime = validate_audio(content, item['name'])
        if hashlib.sha256(content).hexdigest() != item['sha256']:
            raise AssistantError('语音附件已变化，请重新上传。', 409)
        return {'content': content, 'name': name, 'mime': mime}

    async def _request(self, profile, *, maximum, **kwargs):
        try:
            secret = await asyncio.to_thread(self._unprotect, profile['key_cipher'])
            if not isinstance(secret, str) or not secret or any(ord(c) < 33 or ord(c) > 126 for c in secret):
                raise ValueError()
        except Exception:
            raise AssistantError('语音凭证不可用，请联系管理员重新配置。', 503) from None
        try:
            async with asyncio.timeout(REQUEST_TIMEOUT):
                if self._client_factory:
                    client = self._client_factory()
                else:
                    context = await asyncio.to_thread(verified_tls_context, trust_env=False)
                    client = httpx.AsyncClient(verify=context, trust_env=False, follow_redirects=False,
                                               timeout=httpx.Timeout(REQUEST_TIMEOUT, connect=5, pool=5))
                async with client:
                    async with client.stream('POST', profile['endpoint'],
                            headers={'Authorization': 'Bearer ' + secret, 'Accept-Encoding': 'identity'},
                            follow_redirects=False, **kwargs) as response:
                        if response.status_code != 200:
                            raise AssistantError('语音服务未完成请求，请稍后重试或改发文字。', 502)
                        if response.headers.get('content-encoding', 'identity').lower() != 'identity':
                            raise AssistantError('语音服务返回了不支持的压缩响应。', 502)
                        length = response.headers.get('content-length')
                        if length and (not length.isdigit() or int(length) > maximum):
                            raise AssistantError('语音服务响应超过大小限制。', 413)
                        result = bytearray()
                        async for chunk in response.aiter_bytes():
                            if len(result) + len(chunk) > maximum:
                                raise AssistantError('语音服务响应超过大小限制。', 413)
                            result.extend(chunk)
                        return bytes(result), response.headers.get('content-type', '').split(';')[0].strip().lower()
        except AssistantError:
            raise
        except (TimeoutError, httpx.TimeoutException):
            raise AssistantError('语音服务请求超时，请稍后重试或改发文字。', 504) from None
        except Exception:
            raise AssistantError('语音服务连接或认证失败，请联系管理员检查配置。', 502) from None

    async def transcribe(self, actor, file_id):
        _actor(actor)
        profile = self._profile(await asyncio.to_thread(self._config), 'stt')
        item = await asyncio.to_thread(self._input, actor, file_id)
        raw, mime = await self._request(profile, maximum=128 * 1024,
            data={'model': profile['model'], 'response_format': 'json'},
            files={'file': (item['name'], item['content'], item['mime'])})
        try:
            data = json.loads(raw) if mime == 'application/json' else None
            text = data.get('text') if isinstance(data, dict) else None
        except (ValueError, UnicodeError, RecursionError):
            text = None
        if not isinstance(text, str) or not text.strip():
            raise AssistantError('未识别到有效语音，请重新录音或改发文字。', 422)
        if len(text) > MAX_TRANSCRIPT_CHARS:
            raise AssistantError('语音转写超过12000字，请分段发送。', 413)
        return {'text': text.strip()}

    async def synthesize(self, actor, text):
        _actor(actor)
        profile = self._profile(await asyncio.to_thread(self._config), 'tts')
        if not isinstance(text, str) or not text.strip() or len(text) > MAX_TEXT_CHARS:
            raise AssistantError('播报文字须为1至4000字。')
        text = safe_text(text, limit=None).strip()
        if not text:
            raise AssistantError('播报内容包含受保护信息，未发送至语音服务。', 403)
        content, mime = await self._request(profile, maximum=MAX_AUDIO_BYTES,
            json={'model': profile['model'], 'voice': profile['voice'], 'input': text, 'response_format': 'wav'})
        if mime not in {'audio/wav', 'audio/x-wav', 'audio/wave', 'audio/vnd.wave'}:
            raise AssistantError('语音服务未返回WAV音频。', 502)
        validate_audio(content, 'speech.wav')
        try:
            return await asyncio.to_thread(self._output, actor, text, content)
        except AssistantError:
            raise
        except Exception:
            raise AssistantError('语音文件保存未完成，请重试。', 503) from None

    def _output(self, actor, text, content):
        receipt = self.files.upload(actor, 'speech_' + uuid.uuid4().hex + '.wav',
                                    content, extract=False, source_scopes=actor.get('scopes', []))
        item = self.files.get(actor, receipt['id'])
        # Generated speech already has verified source text; no STT or extractor is needed.
        item.update(text=text, error='', extracted=True, extraction_version=EXTRACTION_VERSION,
                    extraction_complete=True, empty=False)
        self.files.store.put_document(FILE_NAMESPACE, receipt['id'], item)
        return self.files.public(item)
