"""One resident OpenClaw gateway, private agents and server-owned model credentials."""
import asyncio
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import socket
import subprocess
import sys
import time
from contextlib import AsyncExitStack, asynccontextmanager, nullcontext
from http.cookiejar import CookieJar, DefaultCookiePolicy
from pathlib import Path
from urllib.parse import unquote, urlsplit

from ..gateway_log import GatewayLog
from ..protocol import MAX_CONCURRENT_ACCOUNTS, OPENCLAW_MAX_CONCURRENT
from ..protocol import atomic_json
from .lighthouse_ai import AssistantError
from .lighthouse_context import execution_key
from .lighthouse_startup_log import emit as startup_log


PIN = json.loads((Path(__file__).parent / "openclaw/runtime.json").read_text(encoding="utf-8"))
PROJECT = Path(__file__).resolve().parents[3]
LOGGER = logging.getLogger(__name__)


class AssistantStartupError(AssistantError):
    code = "runtime_startup"


def account_key(identity):
    if not isinstance(identity, str) or not identity.strip():
        raise AssistantError("缺少登录身份。", 403)
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()[:32]


def runtime_files(root=None):
    candidates = [Path(root)] if root else [PROJECT / "bin/runtime/lighthouse_openclaw", PROJECT / "build_output/lighthouse_openclaw_verified", PROJECT / "build_output/lighthouse_openclaw"]
    for candidate in candidates:
        marker = candidate / "runtime-ready.json"
        if not marker.is_file():
            continue
        try:
            saved = json.loads(marker.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if not isinstance(saved, dict) or saved.get("node_version") != PIN["node_version"] or saved.get("openclaw_version") != PIN["openclaw_version"]:
            continue
        node = candidate / ("node-v" + PIN["node_version"] + "-win-x64/node.exe")
        entry = candidate / "node_modules/openclaw/openclaw.mjs"
        package = candidate / "node_modules/openclaw/package.json"
        if node.is_file() and entry.is_file() and package.is_file():
            try:
                metadata = json.loads(package.read_text(encoding='utf-8'))
            except (ValueError, OSError):
                continue
            if not isinstance(metadata, dict) or metadata.get("version") != PIN["openclaw_version"]:
                continue
            with node.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if digest != PIN["node_executable_sha256"] or digest != saved.get("node_sha256"):
                raise AssistantError("助手运行环境校验失败，请重新放置运行环境文件夹。", 503)
            return node.resolve(), entry.resolve()
    raise AssistantError("缺少灯塔助手运行环境，请将 lighthouse_openclaw 文件夹放到程序的 bin/runtime 目录。", 503)


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def agent_id(identity):
    return 'lh-' + account_key(identity)


def model_provider_error(status, body=b''):
    """Keep native recovery semantics without disclosing upstream error details."""
    if status in {400, 413, 415, 422}:
        try:
            payload = json.loads(body)
            error = payload.get('error', payload) if isinstance(payload, dict) else {}
            code = str(error.get('code', '')).lower() if isinstance(error, dict) else ''
            message = str(error.get('message', '')).lower() if isinstance(error, dict) else ''
        except (ValueError, RecursionError):
            code, message = '', ''
        if code in {'context_length_exceeded', 'context_window_exceeded', 'max_context_length'} or \
                'maximum context length' in message or 'prompt is too long' in message or \
                any(term in message for term in ('context length', 'context window')) and \
                any(term in message for term in ('exceed', 'too long', 'longer than', 'limit')):
            return {'code': 'context_length_exceeded', 'message': 'Context length exceeded'}
        if 'embedding' in message and re.search(r'not.*embedding|is.embedding|embedding.*(?:only|unsupported)', message):
            return {'code': 'embedding_unsupported', 'message': 'Chat models cannot provide embeddings'}
        if code in {'vision_unsupported', 'unsupported_image', 'image_not_supported', 'invalid_image', 'invalid_image_url', 'invalid_image_format'} or \
                re.search(r'image|vision|multimodal|multi.modal|图像|图片|视觉', message) and \
                re.search(r'unsupported|not support|invalid|not enabled|disabled|不支持|无效|未启用', message):
            return {'code': 'vision_unsupported', 'message': 'Model provider rejected image input'}
    if status in {401, 403}:
        code, message = 'invalid_api_key', 'Model provider authentication failed'
    elif status == 429:
        code, message = 'rate_limit_exceeded', 'Model provider rate limit exceeded'
    elif status in {408, 504}:
        code, message = 'model_timeout', 'Model provider timed out'
    elif status == 404:
        code, message = 'model_not_found', 'Model provider model not found'
    elif status >= 500:
        code, message = 'model_service_unavailable', 'Model provider unavailable'
    else:
        code, message = 'model_request_rejected', 'Model provider rejected request'
    return {'code': code, 'message': message}


def model_provider_id(profile):
    if not profile.get('_fallback_model'):
        return 'lighthouse'
    # A failed upstream key must not cool down the alternative's loopback route.
    identity = json.dumps([profile.get(key) for key in ('endpoint', 'model', 'key_cipher')], sort_keys=True)
    return 'lighthouse-fallback-' + hashlib.sha256(identity.encode()).hexdigest()[:20]


def build_configuration(root, accounts, port, *, model_url, plugin, tool_names=(), max_concurrent=OPENCLAW_MAX_CONCURRENT):
    """Only the private loopback broker credential reaches the Node process.

    ``max_concurrent`` is written to OpenClaw's schema-valid positive integer
    ``agents.defaults.maxConcurrent`` (zod: number().int().positive()). The
    unlimited sentinel 0 is never used here; pass a positive runtime value.
    """
    entries, model_groups = {}, {'lighthouse': []}
    from .lighthouse_context import model_context_limit
    for key, item in accounts.items():
        profile = item['profile']
        provider = model_provider_id(profile)
        from .lighthouse_ai import model_capabilities
        capabilities = model_capabilities(profile)
        model = {
            "id": item['agent_id'], "name": 'Lighthouse account model', "reasoning": capabilities["reasoning"],
            "input": ["text", "image"] if capabilities["image_input"] else ["text"],
            "contextWindow": model_context_limit(profile) if 'context_window' in profile else 64000,
            "compat": {"maxTokensField": "max_tokens", "supportsDeveloperRole": False},
        }
        if capabilities["reasoning"]:
            model["compat"].update(supportsReasoningEffort=True,
                supportedReasoningEfforts=capabilities["reasoning_efforts"])
        ref = provider + '/' + model['id']
        model_groups.setdefault(provider, []).append(model)
        entries[item['agent_id']] = {
            'name': item['config_fingerprint'], 'workspace': str(item['root'] / 'workspace'),
            'agentDir': str(item['root'] / 'agent'), 'model': ref,
            'modelPolicy': {'allow': [ref]}, 'skills': [],
        }
        if item.get('text_only'):
            entries[item['agent_id']]['tools'] = {'deny': ['*']}
        elif not capabilities["tool_calls"]:
            entries[item['agent_id']]['tools'] = {'deny': list(tool_names)}
        elif 'tool_names' in item and set(item['tool_names']) != set(tool_names):
            entries[item['agent_id']]['tools'] = {'allow': item['tool_names']}
        if item.get('model_parameters'):
            entries[item['agent_id']]['models'] = {ref: {'params': item['model_parameters']}}
    config = {
        'meta': {'lastTouchedVersion': PIN['openclaw_version']},
        "gateway": {"mode": "local", "bind": "loopback", "port": port,
                    "auth": {"mode": "token", "token": "${LIGHTHOUSE_GATEWAY_TOKEN}"},
                    "controlUi": {"enabled": False}},
        "models": {"mode": "replace", "catalogRefresh": {"enabled": False}, "providers": {provider: {
            'baseUrl': model_url, 'api': 'openai-completions', 'apiKey': '${LIGHTHOUSE_GATEWAY_TOKEN}', 'models': models}
            for provider, models in model_groups.items()}},
        "agents": {"entries": entries,
                   "defaults": {"maxConcurrent": max_concurrent, "heartbeat": {"every": "0m"},
                                "compaction": {"mode": "safeguard"}, "skipBootstrap": True, "timeoutSeconds": 180}},
        "commands": {"text": False, "native": False, "nativeSkills": False, "restart": False, "config": False},
        "tools": {"allow": list(tool_names), "loopDetection": {"enabled": True},
                  "sessions": {"visibility": "self"}, "agentToAgent": {"enabled": False},
                  "deny": ["exec", "process", "read", "write", "edit", "apply_patch", "browser", "sessions_spawn", "sessions_send", "sessions_list", "sessions_history", "agents_list", "gateway", "cron"]},
        # An empty bundled allowlist means unrestricted in the pinned SDK.
        # Lighthouse skills are authorized through the Python tool bridge.
        "skills": {"allowBundled": ["lighthouse-tools"], "load": {"watch": False}}, "cron": {"enabled": False},
        "browser": {"enabled": False},
        "update": {"checkOnStart": False, "auto": {"enabled": False}},
        "logging": {"level": "info" if os.environ.get('OPENCLAW_GATEWAY_STARTUP_TRACE') == '1' else "warn", "consoleLevel": "info" if os.environ.get('OPENCLAW_GATEWAY_STARTUP_TRACE') == '1' else "warn", "file": str(root / "diagnostic.log"),
                    "maxFileBytes": 2_000_000, "audit": {"enabled": False, "messages": "off"}},
        # Retain the preceding layout while widening state ownership to accounts.
        "session": {"reset": {"mode": "none"}, "store": str(root / 'agents/{agentId}/sessions/sessions.json')},
        # Custom models use the SDK's core openai-completions transport. The
        # bundled OpenAI multimedia/OAuth plugin is not part of this route.
        "plugins": {"allow": ["lighthouse-tools"], "slots": {"memory": "none"}},
    }
    # The pinned runtime prewarms its reply modules against a default agent.
    # Every user RPC still supplies the private agent ID and session explicitly.
    if entries:
        first = next(iter(entries.values()))
        first['default'] = True
        config['agents']['defaults']['model'] = first['model']
        config['agents']['defaults']['workspace'] = str(root / 'workspace')
    config['gateway']['reload'] = {'mode': 'hybrid'}
    config["plugins"].update(load={"paths": [str(plugin)]}, entries={"lighthouse-tools": {"enabled": True}})
    return config


class OpenClawRuntime:
    """Unlimited private agents share one process when ``max_accounts`` is the
    unlimited sentinel (0); never share an agent. An explicit positive
    ``max_accounts`` is still honored as the active-account admission cap."""
    def __init__(self, state_root, *, runtime_root=None, max_accounts=MAX_CONCURRENT_ACCOUNTS, startup_timeout=180):
        self.root = Path(state_root).resolve()
        self.runtime_root = runtime_root
        self.maximum = max_accounts
        # OpenClaw's config schema requires a positive integer for
        # agents.defaults.maxConcurrent. When admission is unlimited (<=0),
        # build the schema-valid large value so the native gateway does not
        # impose an arbitrary account cap; explicit positive caps are preserved.
        self._openclaw_max_concurrent = int(max_accounts) if int(max_accounts) > 0 else OPENCLAW_MAX_CONCURRENT
        self.accounts, self.locks = {}, {}
        self.lock = asyncio.Lock()
        self.closing = False
        self.starting = {}
        self.startup_timeout = startup_timeout
        self.prepared = None
        self.prepare_lock = asyncio.Lock()
        self.prepare_retry_at = 0.0
        self.prepare_error = None
        self.config_check_lock = asyncio.Lock()
        self.gateway = None
        self.startup = None
        self.bridge_token = secrets.token_urlsafe(40)
        self.model_url = None
        self.http = None
        self.model_tls = None
        self.http_lock = asyncio.Lock()
        self.plugin_hash = None
        self.tool_names = None

    async def prepare(self, *, progress=lambda _: None):
        """Verify/install dependencies before a question, without account credentials."""
        async with self.prepare_lock:
            if self.closing:
                raise AssistantStartupError("助手服务正在关闭。", 503)
            if self.prepared is None:
                if self.prepare_error is not None and time.monotonic() < self.prepare_retry_at:
                    raise AssistantStartupError(str(self.prepare_error), 503)
                startup_log('runtime_check', node=PIN['node_version'], openclaw=PIN['openclaw_version'])
                try:
                    try:
                        self.prepared = await asyncio.to_thread(runtime_files, self.runtime_root)
                    except AssistantError:
                        if self.runtime_root is not None:
                            raise
                        from .lighthouse_distribution import install_runtime
                        root = PROJECT / "bin/runtime/lighthouse_openclaw"
                        startup_log('runtime_install')
                        await asyncio.to_thread(install_runtime, root, progress=progress)
                        self.prepared = await asyncio.to_thread(runtime_files, root)
                except Exception as exc:
                    self.prepare_error = str(exc) if isinstance(exc, AssistantError) else '助手运行环境准备失败，请稍后重试。'
                    self.prepare_retry_at = time.monotonic() + 60
                    startup_log('startup_failed', error=type(exc).__name__)
                    raise
                self.prepare_error, self.prepare_retry_at = None, 0.0
                startup_log('runtime_ready', node=PIN['node_version'], openclaw=PIN['openclaw_version'])
            return self.prepared

    async def acquire(self, actor, model, profile, **kwargs):
        task = asyncio.current_task()
        self.starting[task] = None
        try:
            item = await self._acquire(actor, model, profile, **kwargs)
            if not item.get('configured'):
                try:
                    await self._configured(item)
                    item['configured'] = True
                except BaseException:
                    item['busy'] = False
                    raise
            return item
        except asyncio.CancelledError:
            item = self.starting.get(task)
            if item is not None:
                item['busy'] = False
            raise
        finally:
            self.starting.pop(task, None)

    async def _acquire(self, actor, model, profile, *, plugin=None, plugin_catalog=None, tool_names=(), bridge_token="", bridge_url="", model_parameters=None, progress=lambda _: None):
        identity = execution_key(actor)
        key = account_key(identity)
        async with self.locks.setdefault(key, asyncio.Lock()):
            if self.closing:
                raise AssistantError("助手服务正在关闭。", 503)
            shared_plugin = plugin_catalog or plugin
            plugin_hash = hashlib.sha256(b''.join(file.read_bytes() for file in sorted(Path(shared_plugin).iterdir()) if file.is_file())).hexdigest() if shared_plugin else ''
            catalog_names = [item['name'] for item in json.loads((Path(plugin_catalog) / 'tools.json').read_text(encoding='utf-8'))] if plugin_catalog else list(tool_names)
            text_only = plugin_catalog is not None and not tool_names
            fingerprint = hashlib.sha256(json.dumps({"profile": profile, "scopes": actor.get("allowed_scopes", actor["scopes"]),
                "tools": list(tool_names), "plugin": plugin_hash, "bridge_url": bridge_url,
                "bridge_token": bridge_token, "parameters": model_parameters, 'text_only': text_only}, sort_keys=True).encode()).hexdigest()
            existing = self.accounts.get(key)
            if existing and existing.get('blocked'):
                await self._recover_blocked(existing)
            if existing and existing.get('busy') and existing.get('process') and existing['process'].poll() is None:
                raise AssistantError('当前账号已有处理中的会话，请先停止或等待完成。', 409)
            if existing and existing.get('process') and existing["process"].poll() is None and existing.get('configured') and not existing.get('stopped') and existing["fingerprint"] == fingerprint:
                if 0 < self.maximum <= sum(bool(item.get('busy')) for item in self.accounts.values()):
                    raise AssistantError('助手正在处理其他会话，请稍后继续。', 503)
                existing["used_at"], existing["busy"] = time.monotonic(), True
                self.starting[asyncio.current_task()] = existing
                return existing
            node, entry = await self.prepare(progress=progress)
            async with self.lock:
                if self.gateway and self.gateway['process'].poll() is not None:
                    self.gateway['stopped'] = True
                    await asyncio.to_thread(self.gateway['output'].close)
                    self.gateway = None
                    for old_item in self.accounts.values():
                        old_item['stopped'] = True
                    self.accounts.clear()
                    self.startup = None
                if 0 < self.maximum <= sum(bool(item.get('busy')) for item in self.accounts.values()):
                    raise AssistantError("助手正在处理其他会话，请稍后继续。", 503)
                if self.plugin_hash is not None and self.plugin_hash != plugin_hash:
                    raise AssistantError('助手工具契约已变化，请重启程序加载更新；未停止其他账号。', 409)
                if not self.model_url:
                    self.model_url = lambda: bridge_url.rsplit('/', 1)[0] + '/openclaw-models'
                if self.gateway and self.gateway['bridge_url'] != bridge_url:
                    raise AssistantError('助手内部工具地址已变化，请重启程序。', 409)
                root = self.root / key
                root.mkdir(parents=True, exist_ok=True)
                (root / "workspace").mkdir(exist_ok=True)
                try:
                    model_key = model.unprotect(profile['key_cipher'])
                except Exception:
                    raise AssistantError('当前系统无法读取本人模型凭证，请在模型设置中重新配置。', 503) from None
                from .lighthouse_ai import model_capabilities
                from .lighthouse_context import model_context_limit
                config_fingerprint = hashlib.sha256(json.dumps({'context': model_context_limit(profile),
                    'output_policy': 'provider_default_v1',
                    'capabilities': model_capabilities(profile), 'plugin': plugin_hash, 'tools': list(tool_names),
                    'provider': model_provider_id(profile),
                    'parameters': model_parameters, 'text_only': text_only}, sort_keys=True).encode()).hexdigest()
                item = {"key": key, "agent_id": agent_id(identity), "root": root, "fingerprint": fingerprint,
                        'config_fingerprint': config_fingerprint,
                        'text_only': text_only,
                        'tool_names': list(tool_names),
                        "used_at": time.monotonic(), "busy": True, "profile": dict(profile),
                        "model_key": model_key, "model_parameters": model_parameters}
                if existing and existing.get('configured') and not existing.get('stopped') and existing.get('config_fingerprint') == config_fingerprint:
                    item['configured'] = True
                self.accounts[key] = item
                self.starting[asyncio.current_task()] = item
                if self.startup is None:
                    self.plugin_hash = plugin_hash
                    self.tool_names = catalog_names
                    self.startup = asyncio.create_task(self._start_shared(node, entry, shared_plugin, catalog_names, bridge_url))
                startup = self.startup
            # One cold startup is shared; cancelling one caller cannot cancel
            # the process startup awaited by the other nineteen accounts.
            await asyncio.shield(startup)
            async with self.lock:
                if self.closing or not self.gateway or self.gateway.get('stopped'):
                    raise AssistantStartupError('助手服务正在关闭，原消息已保留。', 503)
                item.update(process=self.gateway['process'], port=self.gateway['port'],
                            token=self.gateway['token'], protocol=self.gateway['protocol'])
                if not item.get('configured'):
                    try:
                        await self._write_configuration(tool_names)
                    except BaseException:
                        item['busy'] = False
                        raise
            return item

    async def _write_configuration(self, tool_names, accounts=None):
        gateway = self.gateway
        tool_names = self.tool_names if self.tool_names is not None else tool_names
        config = build_configuration(gateway['root'], self.accounts if accounts is None else accounts, gateway['port'], model_url=self.model_url(),
                                     plugin=gateway['plugin'], tool_names=tool_names, max_concurrent=self._openclaw_max_concurrent)
        digest = hashlib.sha256(json.dumps(config, sort_keys=True).encode()).hexdigest()
        if gateway.get('config_digest') == digest:
            return
        for attempt in range(8):
            try:
                await asyncio.to_thread(atomic_json, gateway['root'] / 'openclaw.json', config)
                gateway['config_digest'] = digest
                return
            except PermissionError:
                if os.name != 'nt' or attempt == 7:
                    raise
                # The native config watcher can briefly hold the destination.
                await asyncio.sleep(.05 * (attempt + 1))

    async def _start_shared(self, node, entry, plugin, tool_names, bridge_url):
        from .lighthouse_gateway import GatewayClient, GatewayError
        import shutil
        # Prewarm the reply runtime once before registering a login burst.
        await asyncio.sleep(.1)
        process, log, gateway = None, None, None
        try:
            async with self.lock:
                root = self.root / 'shared-gateway'
                (root / 'workspace').mkdir(parents=True, exist_ok=True)
                (root / 'native-skills').mkdir(exist_ok=True)
                shared_plugin = root / 'plugin'
                shared_plugin.mkdir(parents=True, exist_ok=True)
                if plugin:
                    for file in Path(plugin).iterdir():
                        if file.is_file():
                            shutil.copyfile(file, shared_plugin / file.name)
                port, token = free_port(), secrets.token_urlsafe(32)
                # Load the existing allowlist explicitly, not the unused bundled inventory.
                gateway = {'root': root, 'plugin': shared_plugin,
                           'port': port, 'token': token, 'bridge_url': bridge_url}
                self.gateway = gateway
                first_key = next(iter(self.accounts))
                bootstrap = {first_key: self.accounts[first_key]}
                await self._write_configuration(tool_names, bootstrap)
                authored = {first_key: self.accounts[first_key]['config_fingerprint']}
                # The pinned launcher still enables its versioned cache; keep this PID as the gateway.
                environment = {**os.environ, "OPENCLAW_STATE_DIR": str(self.root), "OPENCLAW_CONFIG_PATH": str(root / 'openclaw.json'),
                               "OPENCLAW_HOME": str(root), "OPENCLAW_NO_RESPAWN": "1", "OPENCLAW_EXEC_SHELL_SNAPSHOT": "0",
                               "OPENCLAW_PACKAGED_COMPILE_CACHE_RESPAWNED": "1",
                               "OPENCLAW_DISABLE_BUNDLED_PLUGINS": "1",
                               "OPENCLAW_DISABLE_BONJOUR": "1", "OPENCLAW_SKIP_CHANNELS": "1",
                               "OPENCLAW_SKIP_CANVAS_HOST": "1", "OPENCLAW_SKIP_STARTUP_MODEL_PREWARM": "1",
                               "OPENCLAW_BUNDLED_SKILLS_DIR": str(root / 'native-skills'),
                               "GIT_CEILING_DIRECTORIES": str(self.root),
                               "NODE_COMPILE_CACHE": str(root / 'node-compile-cache'),
                               "LIGHTHOUSE_GATEWAY_TOKEN": token, "LIGHTHOUSE_BRIDGE_TOKEN": self.bridge_token,
                               'LIGHTHOUSE_SDK_ROOT': str(entry.parent),
                               'LIGHTHOUSE_PYTHON': sys.executable,
                               'LIGHTHOUSE_PLUGIN_DIR': str(shared_plugin),
                               "LIGHTHOUSE_BRIDGE_URL": bridge_url, "NO_COLOR": "1", "NODE_OPTIONS": "--max-old-space-size=2048"}
                environment.pop('LIGHTHOUSE_MODEL_KEY', None)
                from openclaw_service.diagnostics import capture_gateway_line
                log = GatewayLog(root / 'gateway.log', secrets=environment, on_line=capture_gateway_line)
                flags = (subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS) if os.name == "nt" else 0
                try:
                    preload = Path(__file__).parent / 'openclaw/native-paths.mjs'
                    # Bound native compilation workers, not the twenty asynchronous account sessions.
                    gateway_entry = preload.with_name('gateway-start.mjs')
                    process = subprocess.Popen([str(node), '--v8-pool-size=2', '--import', preload.resolve().as_uri(), str(gateway_entry), str(port)],
                                               cwd=root, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, creationflags=flags)
                except OSError:
                    raise AssistantStartupError('助手进程未能启动，原消息已保留。', 503) from None
                if os.name == "nt":
                    from upload_event_module.services.process_lifetime import register_child_process
                    if not register_child_process(process.pid):
                        raise AssistantStartupError("助手进程生命周期绑定失败，未启动。", 503)
                log.attach(process.stdout)
                gateway.update(process=process, output=log)
                for item in self.accounts.values():
                    item.update(process=process, port=port, token=token)
                probe = next(iter(self.accounts.values()))
            started = time.monotonic()
            startup_log('gateway_starting', pid=process.pid, port=port)
            await self._ready(probe, started + self.startup_timeout, GatewayClient, GatewayError)
            gateway['protocol'] = probe.get('protocol', 4)
            for key, item in self.accounts.items():
                item['protocol'] = gateway['protocol']
                if authored.get(key) == item['config_fingerprint']:
                    item['configured'] = True
            startup_log('gateway_ready', pid=process.pid, port=port, protocol=gateway['protocol'],
                        elapsed_ms=round((time.monotonic() - started) * 1000, 1))
        except BaseException as exc:
            from openclaw_service.diagnostics import capture
            capture('startup', 'startup_failed', category=getattr(exc, 'category', ''), error=exc)
            startup_log('startup_failed', error=type(exc).__name__)
            await asyncio.to_thread(self._terminate_startup_process, process)
            if log:
                await asyncio.to_thread(log.close)
            if gateway:
                gateway['stopped'] = True
            self.gateway = None
            self.startup = None
            self.plugin_hash = None
            for item in self.accounts.values():
                item['stopped'] = True
            self.accounts.clear()
            raise

    async def _configured(self, item):
        async with self.config_check_lock:
            if item.get('configured'):
                return
            await self._observe_configuration(item)

    async def _observe_configuration(self, item):
        from .lighthouse_gateway import GatewayClient, GatewayError
        from websockets.exceptions import InvalidHandshake
        # Registration runs concurrently, letting the native watcher coalesce
        # a login burst into one reload instead of twenty serialized reloads.
        deadline, roster = time.monotonic() + 60, {}
        while not self.closing and time.monotonic() < deadline:
            if item['process'].poll() is not None:
                raise AssistantStartupError('共享网关已停止，原消息已保留。', 503)
            try:
                async with asyncio.timeout(min(15, max(.01, deadline - time.monotonic()))):
                    async with GatewayClient('ws://127.0.0.1:' + str(item['port']), item['token']) as client:
                        while time.monotonic() < deadline:
                            roster = await client.request('agents.list', {}, timeout=5)
                            observed = {entry.get('id'): entry.get('name') for entry in roster.get('agents', [])}
                            if observed.get(item['agent_id']) == item['config_fingerprint']:
                                await self._verify_tools(client, item)
                                item['configured'] = True
                                return
                            await asyncio.sleep(.3)
            except (OSError, asyncio.TimeoutError, InvalidHandshake):
                await asyncio.sleep(.3)
            except GatewayError as exc:
                if exc.code not in {'UNAVAILABLE', 'DISCONNECTED', 'HANDSHAKE_TIMEOUT', 'TIMEOUT'}:
                    raise AssistantStartupError('本人 Agent 配置核验未通过，未继续会话。', 503) from None
                await asyncio.sleep(.3)
        LOGGER.warning('Private agent configuration not observed: agent=%s roster_count=%s', item['agent_id'], len(roster.get('agents', [])))
        raise AssistantStartupError('本人 Agent 配置尚未就绪，未使用其他账号模型。', 503)

    @staticmethod
    def _terminate_startup_process(process):
        """Stop the owned shared child on startup failure or host shutdown."""
        if process is None or process.poll() is not None:
            return
        try:
            process.terminate()
        except OSError:
            pass
        try:
            process.wait(timeout=5)
        except (subprocess.TimeoutExpired, OSError):
            try:
                process.kill()
            except OSError:
                pass
            try:
                process.wait(timeout=5)
            except (subprocess.TimeoutExpired, OSError):
                pass

    async def _ready(self, item, deadline, GatewayClient, GatewayError):
        from websockets.exceptions import InvalidHandshake
        process, port, token = item['process'], item['port'], item['token']
        while not self.closing and time.monotonic() < deadline:
            if process.poll() is not None:
                raise AssistantStartupError("助手运行环境启动失败，请查看本机助手诊断。", 503)
            try:
                async with asyncio.timeout(min(15, max(.01, deadline - time.monotonic()))):
                    async with GatewayClient("ws://127.0.0.1:" + str(port), token) as client:
                        # hello-ok can precede agent readiness. This local, read-only
                        # RPC shares the Gateway's startup gate with the agent RPC.
                        await client.request("models.list", {'agentId': item['agent_id']}, timeout=5)
                        await self._verify_tools(client, item)
                        item["protocol"] = client.protocol
                        return item
            except (OSError, InvalidHandshake):
                item["startup_stage"] = "port_unavailable"
                await asyncio.sleep(.2)
            except asyncio.TimeoutError:
                item["startup_stage"] = "handshake_timeout"
                await asyncio.sleep(.2)
            except GatewayError as exc:
                item["startup_stage"] = exc.code
                if exc.code in {"UNAVAILABLE", "DISCONNECTED", "HANDSHAKE_TIMEOUT", "TIMEOUT"}:
                    await asyncio.sleep(.3)
                else:
                    raise AssistantStartupError("助手网关握手未通过（" + exc.code + "），未启动会话。", 503) from None
        raise AssistantStartupError("助手启动超时，原消息已保留，可继续。", 503)

    async def _verify_tools(self, client, item):
        from .lighthouse_ai import model_capabilities
        from .lighthouse_gateway import GatewayError
        expected = set(item.get('tool_names', self.tool_names or ()))
        if not expected or item.get('text_only') or not model_capabilities(item.get('profile', {}))['tool_calls']:
            return
        # Catalog loading activates lazy plugins; effective tools verify real factories and policy.
        session = 'agent:' + item['agent_id'] + ':lighthouse-readiness'
        try:
            await client.request('tools.catalog', {'agentId': item['agent_id'], 'includePlugins': True}, timeout=5)
            await client.request('sessions.patch', {'key': session}, timeout=5)
            value = await client.request('tools.effective', {'agentId': item['agent_id'], 'sessionKey': session}, timeout=5)
        except GatewayError as exc:
            if exc.code in {'UNAVAILABLE', 'DISCONNECTED', 'HANDSHAKE_TIMEOUT', 'TIMEOUT'}:
                raise
            raise AssistantStartupError('助手查询工具加载核验失败，原消息和已有任务已保留。',
                                        503, category='runtime_tools') from None
        groups = value.get('groups', []) if isinstance(value, dict) else []
        groups = groups if isinstance(groups, list) else []
        available = {tool.get('id') for group in groups if isinstance(group, dict) and isinstance(group.get('tools'), list)
                     for tool in group['tools'] if isinstance(tool, dict) and isinstance(tool.get('id'), str)
                     and tool.get('pluginId') == 'lighthouse-tools' and not tool.get('deniedBySession')}
        if not isinstance(value, dict) or value.get('agentId') != item['agent_id'] or not expected <= available:
            LOGGER.warning('Assistant tools not ready: expected=%s available=%s', len(expected), len(available))
            raise AssistantStartupError('助手查询工具尚未完整加载，未启动问答；原消息和已有任务已保留。',
                                        503, category='runtime_tools')

    async def _stop(self, item):
        # Abort acknowledgement may be lost. Revoke this account's callbacks and
        # model access, not the shared process that is serving other accounts.
        item['blocked'], item['busy'] = True, False

    async def _recover_blocked(self, item):
        from .lighthouse_gateway import GatewayClient, GatewayError
        if item.get('process') and item['process'].poll() is not None:
            item['blocked'] = False
            return
        if item.get('run_id'):
            try:
                async with GatewayClient('ws://127.0.0.1:' + str(item['port']), item['token']) as client:
                    result = await client.request('agent.wait', {'runId': item['run_id'], 'timeoutMs': 0}, timeout=5)
                    if result.get('status') in {'ok', 'error'}:
                        item['blocked'] = False
                        return
            except (GatewayError, OSError, asyncio.TimeoutError):
                pass
        raise AssistantError('本账号上一轮停止结果尚未确认，请稍后继续；其他账号可正常使用。', 409)

    async def model_client(self):
        """Prewarm TLS and return a request scope; injected transports are borrowed."""
        import httpx
        from .lighthouse_public import _verified_tls_context
        if self.closing:
            raise AssistantStartupError('助手服务正在关闭。', 503)
        if self.http is not None:
            return nullcontext(self.http)
        # Certificate loading has no sockets; shutdown must not await its lock.
        context = self.model_tls
        if context is None:
            context = await asyncio.to_thread(_verified_tls_context, trust_env=False)
        async with self.http_lock:
            if self.closing:
                raise AssistantStartupError('助手服务正在关闭。', 503)
            self.model_tls = context

        @asynccontextmanager
        async def connection():
            if self.closing:
                raise AssistantStartupError('助手服务正在关闭。', 503)
            async with httpx.AsyncClient(verify=context,
                    timeout=httpx.Timeout(180, connect=10, pool=10),
                    limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
                    cookies=CookieJar(policy=DefaultCookiePolicy(allowed_domains=[])),
                    follow_redirects=False, trust_env=False) as client:
                yield client

        return connection()

    async def forward_model(self, request):
        from fastapi.responses import JSONResponse, StreamingResponse
        from anyio import CancelScope
        import httpx
        gateway = self.gateway
        authorization = request.headers.get('authorization', '')
        supplied = authorization[7:] if authorization.startswith('Bearer ') else ''
        if not request.client or request.client.host != '127.0.0.1' or request.headers.get('origin') \
                or not supplied.isascii() or not gateway or not hmac.compare_digest(supplied, gateway['token']):
            return JSONResponse({'error': {'message': 'Model invocation expired'}}, status_code=403)
        body = bytearray()
        async for chunk in request.stream():
            body.extend(chunk)
            if len(body) > 16 * 1024 * 1024:
                return JSONResponse({'error': {'message': 'Model request too large'}}, status_code=413)
        try:
            payload = json.loads(body)
        except (ValueError, RecursionError):
            return JSONResponse({'error': {'message': 'Invalid model request'}}, status_code=400)
        alias = payload.get('model') if isinstance(payload, dict) else None
        key = alias.removeprefix('lh-') if isinstance(alias, str) and alias.startswith('lh-') else ''
        item = self.accounts.get(key)
        if not item or item.get('blocked') or not item.get('busy') or alias != item['agent_id']:
            return JSONResponse({'error': {'message': 'Model identity mismatch'}}, status_code=403)
        from openclaw_service.diagnostics import capture, native_run_id
        diagnostic_run = native_run_id(item.get('run_id'))
        model_name = str(item['profile'].get('model', '')).lower()
        model_family = next((name for name in ('deepseek', 'minimax', 'glm', 'qwen') if name in model_name), 'other')
        def report_failure(reason, status=502, *, response_bytes=0, response_type='other'):
            capture('assistant', 'model_request', run_id=diagnostic_run, category='model_transport',
                    error=AssistantError('Model transport failed', status), native=reason,
                    details={'model_family': model_family, 'request_bytes': len(body),
                             'response_bytes': response_bytes, 'response_type': response_type})
        payload['model'] = item['profile']['model']
        if unquote(urlsplit(item['profile']['endpoint']).path).rstrip('/').lower().endswith('/embeddings'):
            return JSONResponse({'error': {'code': 'embedding_unsupported',
                'message': 'Chat models cannot provide embeddings'}}, status_code=400)
        from .lighthouse_ai import model_capabilities, model_reasoning_options
        capabilities = model_capabilities(item['profile'])
        # Enforce the account profile even if the SDK cached another effort or tool policy.
        for name in ('reasoning_effort', 'thinking', 'enable_thinking'):
            payload.pop(name, None)
        payload.update(model_reasoning_options(item['profile']))
        if item.get('text_only') or not capabilities['tool_calls']:
            for name in ('tools', 'tool_choice', 'parallel_tool_calls'):
                payload.pop(name, None)
        if isinstance(payload.get('messages'), list):
            for message in payload['messages']:
                if isinstance(message, dict) and isinstance(message.get('content'), list) and any(
                        isinstance(part, dict) and part.get('type') in {'input_audio', 'audio', 'input_video', 'video'}
                        for part in message['content']):
                    return JSONResponse({'error': {'code': 'unsupported_media',
                        'message': 'Audio/video input is not supported by this chat endpoint; use configured speech tools.'}}, status_code=400)
                if isinstance(message, dict) and not capabilities['image_input'] and isinstance(message.get('content'), list):
                    message['content'] = [part for part in message['content'] if not isinstance(part, dict)
                                          or part.get('type') not in {'image_url', 'input_image', 'image'}]
        # Recovery compaction in the pinned SDK does not always emit WS events.
        messages = payload.get('messages')
        if item.get('run_id') and isinstance(messages, list) and any(isinstance(message, dict) and
                message.get('role') == 'system' and isinstance(message.get('content'), str) and
                message['content'].startswith('You are a context summarization assistant.') for message in messages):
            item['compaction_run_id'] = item['run_id']
        from .lighthouse_context import bound_model_payload, compact_model_messages, model_output_limit
        compacted = compact_model_messages(payload)
        if compacted is not payload and item.get('run_id'):
            item['compaction_run_id'] = item['run_id']
        payload = compacted
        recovering_output = item['profile'].get('_output_recovery') is True
        if recovering_output:
            payload = {key: value for key, value in payload.items() if key != 'max_completion_tokens'}
            payload['max_tokens'] = model_output_limit(item['profile'])
        try:
            # OpenClaw injects its own default max_tokens; the upstream service must choose the initial limit.
            bounded = bound_model_payload(payload, item['profile'], provider_default=not recovering_output)
        except AssistantError as exc:
            report_failure('Context length exceeded' if exc.category == 'model_context' else 'Model provider rejected request', exc.status)
            return JSONResponse({'error': {'code': 'context_length_exceeded' if exc.category == 'model_context' else 'model_request_rejected',
                'message': 'Context length exceeded' if exc.category == 'model_context' else 'Model provider rejected request'}}, status_code=exc.status)
        if bounded.get('messages') != payload.get('messages') and item.get('run_id'):
            item['compaction_run_id'] = item['run_id']
        payload = bounded
        connection = AsyncExitStack()
        streaming = False
        try:
            client = await connection.enter_async_context(await self.model_client())
            for attempt in range(2):
                if self.closing or self.accounts.get(key) is not item or item.get('blocked') or not item.get('busy'):
                    return JSONResponse({'error': {'message': 'Model invocation expired'}}, status_code=403)
                response = await client.send(client.build_request('POST', item['profile']['endpoint'], json=payload,
                    headers={'Authorization': 'Bearer ' + item['model_key'], 'Cookie': ''}), stream=True)
                if response.status_code == 200:
                    connection.push_async_callback(response.aclose)
                    if 'text/html' in response.headers.get('content-type', '').lower():
                        report_failure('Model provider returned invalid response', response_type='html')
                        return JSONResponse({'error': {'code': 'invalid_provider_response',
                            'message': 'Model provider returned invalid response'}}, status_code=502)
                    break
                error_body = bytearray()
                try:
                    if response.status_code in {400, 413, 415, 422}:
                        async with asyncio.timeout(2):
                            async for chunk in response.aiter_bytes():
                                if len(error_body) + len(chunk) > 16384:
                                    error_body.clear()
                                    break
                                error_body.extend(chunk)
                except (httpx.HTTPError, OSError, asyncio.TimeoutError):
                    error_body.clear()
                finally:
                    with CancelScope(shield=True):
                        await response.aclose()
                error = model_provider_error(response.status_code, error_body)
                report_failure(error['message'], response.status_code, response_bytes=len(error_body))
                # Only a rejected model request may be retried; never replay a tool or an accepted stream.
                if attempt == 0 and error.get('code') == 'context_length_exceeded':
                    budget = max(2000, len(json.dumps(payload.get('messages', []), ensure_ascii=False)) // 2)
                    compacted = compact_model_messages(payload, budget)
                    if compacted is not payload or 'max_tokens' not in payload:
                        context_changed = compacted is not payload
                        try:
                            payload = bound_model_payload({**compacted, 'max_tokens': model_output_limit(item['profile'])}, item['profile'])
                        except AssistantError:
                            return JSONResponse({'error': {'code': 'context_length_exceeded',
                                'message': 'Context length exceeded'}}, status_code=413)
                        if context_changed and item.get('run_id'):
                            item['compaction_run_id'] = item['run_id']
                        continue
                return JSONResponse({'error': error}, status_code=response.status_code)

            async def chunks():
                size = 0
                content_type = response.headers.get('content-type', '').lower()
                response_type = 'event_stream' if 'text/event-stream' in content_type else 'json' if 'json' in content_type else 'other'
                try:
                    async for chunk in response.aiter_bytes():
                        if self.accounts.get(key) is not item or item.get('blocked') or not item.get('busy'):
                            return
                        size += len(chunk)
                        if size > 32 * 1024 * 1024:
                            report_failure('Model response too large', response_bytes=size, response_type=response_type)
                            return
                        yield chunk
                except (httpx.HTTPError, OSError):
                    report_failure('Model stream interrupted', response_bytes=size, response_type=response_type)
                    raise
                if size == 0:
                    report_failure('Model provider returned empty response', response_type=response_type)
            class ModelStream(StreamingResponse):
                async def __call__(self, scope, receive, send):
                    try:
                        await super().__call__(scope, receive, send)
                    finally:
                        # Disconnect may cancel the response before its iterator starts.
                        with CancelScope(shield=True):
                            await connection.aclose()

            result = ModelStream(chunks(), media_type=response.headers.get('content-type', 'application/json'),
                                 headers={'Cache-Control': 'no-store'})
            streaming = True
            return result
        except AssistantStartupError:
            return JSONResponse({'error': {'message': 'Model invocation expired'}}, status_code=403)
        except httpx.TimeoutException:
            report_failure('Model provider timed out', 504)
            return JSONResponse({'error': model_provider_error(504)}, status_code=504)
        except (httpx.HTTPError, OSError):
            report_failure('Model provider unavailable')
            return JSONResponse({'error': model_provider_error(502)}, status_code=502)
        finally:
            if not streaming:
                with CancelScope(shield=True):
                    await connection.aclose()

    async def close(self):
        self.closing = True
        if self.startup and not self.startup.done():
            self.startup.cancel()
            await asyncio.gather(self.startup, return_exceptions=True)
        pending = [task for task in tuple(self.starting) if task is not asyncio.current_task()]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        async with self.lock:
            if self.gateway and not self.gateway.get('stopped'):
                self.gateway['stopped'] = True
                await asyncio.to_thread(self._terminate_startup_process, self.gateway['process'])
                await asyncio.to_thread(self.gateway['output'].close)
                startup_log('gateway_stopped', pid=self.gateway['process'].pid)
            for item in self.accounts.values():
                item['stopped'] = True
            self.accounts.clear()
            self.locks.clear()
        async with self.http_lock:
            self.model_tls = None
            if self.http is not None:
                await self.http.aclose()
                self.http = None


def install_model_route(app, manager):
    from fastapi import Request
    async def model_proxy(request: Request):
        return await manager.forward_model(request)
    app.add_api_route('/api/assistant/openclaw-models/chat/completions', model_proxy,
                      methods=['POST'], name='lighthouse_internal_model')
