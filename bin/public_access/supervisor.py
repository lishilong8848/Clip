"""One owned FRPC child with bounded logs, backoff and nonblocking shutdown."""
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import os
import socket
import subprocess
import threading
import time

from .config import DATA, SPEC, atomic_json
from .distribution import install, previous, verify_binary
from .ingress import StreamingIngress

BACKOFF = (5, 15, 30, 60)


class PublicAccess:
    def __init__(self, settings, upstream, token, *, data=DATA, spec_path=SPEC, deployment=None):
        self.settings, self.upstream, self.token = settings, upstream, token
        self.data, self.spec_path = data, spec_path
        self.deployment = deployment
        self.stop_event = threading.Event()
        self.thread = self.child = self.server = None
        self.lock = threading.Lock()
        self.state = ''
        self.logger = logging.getLogger('clipflow.public_access.' + token[:8])
        self.logger.propagate = False
        self.logger.setLevel(logging.INFO)

    def status(self, state, message):
        with self.lock:
            if self.state == state:
                return
            self.state = state
            try:
                atomic_json(self.data / 'status.json', {'state': state, 'message': message,
                    'updated_at': time.time(), 'owner_pid': os.getpid(), 'public_url': self.settings.origin})
            except OSError:
                pass
        print('[ClipFlow][FRP] ' + message, flush=True)

    def start(self):
        with self.lock:
            if self.thread is not None:
                return
            self.thread = threading.Thread(target=self._run, name='ClipFlowPublicAccess', daemon=True)
            self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.server:
            self.server.should_exit = True
        child = self.child
        if child is not None:
            try:
                child.terminate()
            except OSError:
                pass

    def _read_logs(self, child, root, spec):
        for raw in iter(lambda: child.stdout.readline(8192), b''):
            line = raw.decode('utf-8', errors='replace').strip()
            for secret in (self.settings.token, self.token):
                line = line.replace(secret, '[redacted]')
            self.logger.info('%s', line)
            if self.stop_event.is_set():
                continue
            if 'start proxy success' in line and '[clipflow-public]' in line:
                try:
                    atomic_json(root / 'working.json', spec)
                except OSError:
                    self.logger.warning('Cannot persist last working FRPC version.')
                self.status('connected', '公网隧道已连接 | ' + self.settings.origin)
            elif any(term in line.lower() for term in ('login to server failed', 'try to reconnect',
                                                       'reconnect to server', 'connect to server error',
                                                       'connection closed', 'start error', 'session shutdown')):
                self.status('reconnecting', '公网连接暂不可用，正在重连；局域网可继续使用')

    def _serve(self, sock):
        from upload_event_module.services.process_lifetime import lower_current_thread_priority
        lower_current_thread_priority()
        async def serve():
            try:
                await self.server.serve(sockets=[sock])
            finally:
                await self.proxy.close()
        try:
            asyncio.run(serve())
        except BaseException as exc:
            self.status('ingress_failed', '公网隔离入口启动失败，局域网不受影响 | ' + type(exc).__name__)

    def _run(self):
        sock = None
        handler = None
        server_thread = None
        config_written = False
        try:
            from upload_event_module.services.process_lifetime import lower_current_thread_priority, register_child_process
            lower_current_thread_priority(background_io=True)
            self.data.mkdir(parents=True, exist_ok=True)
            handler = RotatingFileHandler(self.data / 'frpc.log', maxBytes=1024 * 1024,
                                          backupCount=3, encoding='utf-8')
            self.logger.addHandler(handler)
            # Bind before download: a second instance cannot start a competing client.
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if os.name == 'nt':
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
            sock.bind(('127.0.0.1', self.settings.ingress_port))
            if self.deployment is not None:
                from .provisioning import provision
                attempt = 0
                while not self.stop_event.is_set():
                    try:
                        self.status('provisioning', '正在后台核验正式机公网凭据；局域网不受影响')
                        self.settings = provision(self.deployment, self.data, cancelled=self.stop_event.is_set)
                        break
                    except Exception as exc:
                        if self.stop_event.is_set():
                            return
                        self.status('provision_retry', '公网凭据准备未完成，将退避重试 | error=' + type(exc).__name__)
                        if self.stop_event.wait(BACKOFF[min(attempt, len(BACKOFF) - 1)]):
                            return
                        attempt += 1
                if self.stop_event.is_set():
                    return
            root = self.data / 'frpc'
            self.status('preparing', '正在后台准备公网组件；局域网已独立运行')
            old_binary, old_spec = previous(root)
            attempt = 0
            while not self.stop_event.is_set():
                try:
                    binary, spec = install(root, spec_path=self.spec_path, cancelled=self.stop_event.is_set)
                    break
                except Exception as exc:
                    if self.stop_event.is_set():
                        return
                    if old_binary:
                        binary, spec = old_binary, old_spec
                        self.status('fallback', '公网组件更新未完成，继续使用已验证旧版本')
                        break
                    self.status('install_retry', '公网组件下载未完成，将退避重试；局域网不受影响 | error=' + type(exc).__name__)
                    if self.stop_event.wait(BACKOFF[min(attempt, len(BACKOFF) - 1)]):
                        return
                    attempt += 1
            if self.stop_event.is_set():
                return
            config_file = self.data / 'frpc.generated.json'
            atomic_json(config_file, self.settings.frpc_config())
            config_written = True
            try:
                verify_binary(binary, spec, config_file)
            except Exception:
                if not old_binary or old_binary == binary:
                    raise
                binary, spec = old_binary, old_spec
                verify_binary(binary, spec, config_file)
                self.status('fallback', '新公网组件未通过运行校验，已回退旧版本')
            import uvicorn
            self.proxy = StreamingIngress(self.upstream, self.token, self.settings)
            self.server = uvicorn.Server(uvicorn.Config(self.proxy, host='127.0.0.1',
                port=self.settings.ingress_port, lifespan='off', proxy_headers=False,
                access_log=False, log_level='warning', log_config=None, ws='none',
                limit_concurrency=self.settings.max_requests + 8, timeout_keep_alive=5,
                timeout_graceful_shutdown=2))
            server_thread = threading.Thread(target=self._serve, args=(sock,), name='ClipFlowPublicIngress', daemon=True)
            server_thread.start()
            deadline = time.monotonic() + 10
            while not self.server.started:
                if self.stop_event.wait(.1) or not server_thread.is_alive() or time.monotonic() > deadline:
                    raise RuntimeError('Public ingress did not start.')
            attempt = 0
            while not self.stop_event.is_set() and server_thread.is_alive():
                self.status('connecting', '正在连接公网服务器；局域网不受影响')
                child = subprocess.Popen([str(binary), '-c', str(config_file)], cwd=self.data,
                    stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0) | getattr(subprocess, 'BELOW_NORMAL_PRIORITY_CLASS', 0))
                self.child = child
                if not register_child_process(child.pid):
                    raise RuntimeError('Cannot bind FRPC lifetime to the portal.')
                if self.stop_event.is_set():
                    child.terminate()
                reader = threading.Thread(target=self._read_logs, args=(child, root, spec),
                                          name='ClipFlowPublicLog', daemon=True)
                reader.start()
                started = time.monotonic()
                while child.poll() is None and server_thread.is_alive() and not self.stop_event.wait(1):
                    pass
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait(timeout=2)
                reader.join(timeout=2)
                child.stdout.close()
                self.child = None
                if not server_thread.is_alive():
                    raise RuntimeError('Public ingress stopped.')
                if self.stop_event.is_set():
                    break
                if time.monotonic() - started >= 60:
                    attempt = 0
                elif old_binary and binary != old_binary:
                    binary, spec = old_binary, old_spec
                self.status('retrying', '公网客户端已退出，将退避重试；局域网可继续使用')
                if self.stop_event.wait(BACKOFF[min(attempt, len(BACKOFF) - 1)]):
                    break
                attempt += 1
        except Exception as exc:
            if not self.stop_event.is_set():
                self.status('failed', '公网组件未启动，局域网不受影响 | error=' + type(exc).__name__)
        finally:
            was_stopping = self.stop_event.is_set()
            self.stop()
            if self.child:
                try:
                    self.child.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.child.kill()
                    self.child.wait(timeout=2)
            if server_thread:
                server_thread.join(timeout=3)
            if sock:
                sock.close()
            if handler:
                self.logger.removeHandler(handler)
                handler.close()
            # FRPC needs a plaintext runtime config, but it need not survive shutdown.
            try:
                if config_written:
                    (self.data / 'frpc.generated.json').unlink(missing_ok=True)
            except OSError:
                pass
            if was_stopping:
                self.status('stopped', '公网组件已停止')
