"""Capture Zhihang authentication from an application-owned browser login window."""
import atexit
import hashlib
import json
import os
import re
import subprocess
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlsplit

import requests

from upload_event_module.services.process_lifetime import lower_current_thread_priority
from upload_event_module.utils import get_data_file_path
from . import plan_convergence_auth as auth

LOGIN_TIMEOUT = 300


def browser_command(profile):
    for root, relative in (
        ('PROGRAMFILES(X86)', 'Microsoft/Edge/Application/msedge.exe'),
        ('PROGRAMFILES', 'Microsoft/Edge/Application/msedge.exe'),
        ('LOCALAPPDATA', 'Microsoft/Edge/Application/msedge.exe'),
        ('PROGRAMFILES', 'Google/Chrome/Application/chrome.exe'),
        ('LOCALAPPDATA', 'Google/Chrome/Application/chrome.exe'),
    ):
        path = Path(os.environ.get(root, '')) / relative
        if path.is_file():
            return [str(path), '--remote-debugging-port=0', '--remote-debugging-address=127.0.0.1',
                    '--user-data-dir=' + str(profile), '--no-first-run', '--no-default-browser-check',
                    '--new-window', 'about:blank']
    raise ValueError('未找到 Edge 或 Chrome，请在运行程序的电脑上安装其中一种浏览器')


def request_token(message):
    if message.get('method') not in {'Network.requestWillBeSent', 'Network.requestWillBeSentExtraInfo'}:
        return None
    params = message.get('params') or {}
    request = params.get('request') or {}
    url = request.get('url') or params.get('_request_url') or ''
    actual, expected = urlsplit(url), urlsplit(auth.ZH_BASE)
    if (actual.scheme, actual.netloc) != (expected.scheme, expected.netloc) or not actual.path.startswith('/api/'):
        return None
    headers = request.get('headers') or params.get('headers') or {}
    if message['method'] == 'Network.requestWillBeSentExtraInfo':
        origin = next((value for key, value in headers.items() if key.casefold() == 'origin'), '')
        if origin != auth.ZH_BASE:
            return None
    value = next((value for key, value in headers.items() if key.casefold() == 'authorization'), '')
    if not isinstance(value, str) or value[:7].casefold() != 'bearer ':
        return None
    try:
        return auth.clean_token(value)
    except ValueError:
        return None


class BrowserLogin:
    def __init__(self, store):
        self.store = store
        self._lock = threading.RLock()
        self._thread = None
        self._stop = threading.Event()
        self._state = {'job_id': '', 'status': 'idle', 'message': ''}
        atexit.register(self.close)

    def status(self):
        with self._lock:
            return dict(self._state)

    def start(self):
        with self._lock:
            if self._thread and self._thread.is_alive():
                if self._stop.is_set():
                    raise ValueError('上一认证窗口正在关闭，请稍后重试')
                return dict(self._state)
            self._stop = threading.Event()
            job_id = uuid.uuid4().hex
            self._state = {'job_id': job_id, 'status': 'starting', 'message': '正在打开主机智航登录窗口…'}
            self._thread = threading.Thread(target=self._run, args=(job_id, self._stop),
                                            name='ZhihangBrowserLogin', daemon=True)
            self._thread.start()
            return dict(self._state)

    def cancel(self, job_id):
        with self._lock:
            if not job_id or job_id != self._state['job_id']:
                raise ValueError('认证任务已变化，请重新读取状态')
            if self._state['status'] not in {'starting', 'waiting', 'verifying'}:
                return dict(self._state)
            self._stop.set()
            self._state.update(status='cancelled', message='智航认证已取消，原认证保持不变')
            return dict(self._state)

    def close(self):
        self._stop.set()
        if self._thread and self._thread is not threading.current_thread():
            self._thread.join(timeout=2)

    def _update(self, job_id, status, message):
        with self._lock:
            if self._state['job_id'] == job_id and not self._stop.is_set():
                self._state.update(status=status, message=message)

    def _run(self, job_id, stop):
        lower_current_thread_priority()
        tokens = self._tokens(stop, time.monotonic() + LOGIN_TIMEOUT)
        seen = set()
        try:
            self._update(job_id, 'waiting', '等待主机智航登录，完成登录后自动认证…')
            for token in tokens:
                if stop.is_set():
                    return
                fingerprint = hashlib.sha256(token.encode()).digest()
                if fingerprint in seen:
                    continue
                seen.add(fingerprint)
                self._update(job_id, 'verifying', '已收到智航认证，正在验证…')
                try:
                    auth.test_connection(token)
                except ValueError:
                    self._update(job_id, 'waiting', '智航登录尚未有效，请在主机登录窗口完成登录…')
                    continue
                with self._lock:
                    if stop.is_set() or job_id != self._state['job_id']:
                        return
                    owner = str(auth._jwt(token).get('UserName') or '')
                    auth.save_config({'token': token, 'login_name': owner, 'token_owner_note': owner,
                                      'display_name': ''}, self.store)
                    self._state.update(status='success', message='智航认证成功，已自动保存')
                return
            if not stop.is_set():
                self._update(job_id, 'failed', '未取得有效智航认证，登录窗口已关闭或等待超时，请重新登录')
        except requests.RequestException:
            self._update(job_id, 'failed', '智航认证连接失败，请检查主机内网或 VPN 连接后重新登录')
        except ValueError as exc:
            self._update(job_id, 'failed', str(exc))
        except Exception:
            self._update(job_id, 'failed', '智航登录窗口连接失败，请关闭该窗口后重新登录')
        finally:
            tokens.close()

    def _tokens(self, stop, deadline):
        import websocket
        profile = Path(get_data_file_path('plan_convergence_auth')) / 'browser_profile'
        profile.mkdir(parents=True, exist_ok=True)
        port_file = profile / 'DevToolsActivePort'
        port_file.unlink(missing_ok=True)
        if os.name == 'nt':
            import win32api
            import win32con
            import win32event
            import win32job
            import win32process
        args = browser_command(profile)
        process = None
        process_handle = None
        thread_handle = None
        connection = None
        window_job = None
        try:
            if os.name == 'nt':
                window_job = win32job.CreateJobObject(None, '')
                info = win32job.QueryInformationJobObject(window_job, win32job.JobObjectExtendedLimitInformation)
                info['BasicLimitInformation']['LimitFlags'] |= win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
                win32job.SetInformationJobObject(window_job, win32job.JobObjectExtendedLimitInformation, info)
                # Assign before resuming so the launcher cannot create unowned browser children.
                process_handle, thread_handle, _, _ = win32process.CreateProcess(
                    None, subprocess.list2cmdline(args), None, None, False,
                    win32con.CREATE_SUSPENDED | win32con.CREATE_NO_WINDOW,
                    None, None, win32process.STARTUPINFO())
                win32job.AssignProcessToJobObject(window_job, process_handle)
                win32process.ResumeThread(thread_handle)
                thread_handle.Close()
                thread_handle = None
            else:
                process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            startup_deadline = min(deadline, time.monotonic() + 15)
            while not stop.is_set() and time.monotonic() < startup_deadline:
                exit_code = win32process.GetExitCodeProcess(process_handle) if process_handle is not None else process.poll()
                if exit_code not in (None, 0, 259):
                    raise ValueError('智航登录窗口启动失败，请检查 Edge/Chrome 后重试')
                try:
                    lines = port_file.read_text(encoding='utf-8').splitlines()
                    if len(lines) >= 2 and lines[0].isdigit() and 0 < int(lines[0]) < 65536 and re.fullmatch(r'/devtools/browser/[\w-]+', lines[1]):
                        connection = websocket.create_connection('ws://127.0.0.1:' + lines[0] + lines[1],
                            timeout=2, suppress_origin=True, http_no_proxy=['127.0.0.1', 'localhost'])
                        break
                except (OSError, websocket.WebSocketException):
                    pass
                stop.wait(.2)
            if stop.is_set():
                return
            if connection is None:
                raise ValueError('智航登录窗口启动超时，请检查浏览器是否限制自动认证')
            sequence = 0

            def send(method, params=None, session=None):
                nonlocal sequence
                sequence += 1
                packet = {'id': sequence, 'method': method, 'params': params or {}}
                if session:
                    packet['sessionId'] = session
                connection.send(json.dumps(packet))
                return sequence

            def command(method, params=None, session=None):
                ident = send(method, params, session)
                while not stop.is_set() and time.monotonic() < deadline:
                    try:
                        reply = json.loads(connection.recv())
                    except websocket.WebSocketTimeoutException:
                        continue
                    if reply.get('id') == ident:
                        if reply.get('error'):
                            raise ValueError('智航登录窗口初始化失败，请重新登录')
                        return reply.get('result') or {}
                raise ValueError('智航登录等待已结束')

            pages = command('Target.getTargets')['targetInfos']
            target = next(row['targetId'] for row in pages if row.get('type') == 'page')
            session = command('Target.attachToTarget', {'targetId': target, 'flatten': True})['sessionId']
            command('Network.enable', session=session)
            command('Page.navigate', {'url': auth.ZH_BASE}, session)
            send('Target.setAutoAttach', {'autoAttach': True, 'waitForDebuggerOnStart': True, 'flatten': True})
            request_urls = {}
            # Edge may hand off to its browser child; the CDP connection owns the window lifetime.
            while not stop.is_set() and time.monotonic() < deadline:
                try:
                    raw = connection.recv()
                    if not raw:
                        return
                    message = json.loads(raw)
                except websocket.WebSocketTimeoutException:
                    continue
                params = message.get('params') or {}
                if message.get('method') == 'Target.attachedToTarget':
                    if params.get('targetInfo', {}).get('type') in {'page', 'worker', 'service_worker'}:
                        send('Network.enable', session=params['sessionId'])
                    send('Runtime.runIfWaitingForDebugger', session=params['sessionId'])
                key = (message.get('sessionId'), params.get('requestId'))
                if message.get('method') == 'Network.requestWillBeSent':
                    request_urls[key] = params.get('request', {}).get('url', '')
                elif message.get('method') == 'Network.requestWillBeSentExtraInfo':
                    params['_request_url'] = request_urls.get(key, '')
                token = request_token(message)
                if token:
                    yield token
                if message.get('method') in {'Network.loadingFinished', 'Network.loadingFailed'}:
                    request_urls.pop(key, None)
        finally:
            if connection is not None:
                try:
                    connection.send(json.dumps({'id': 999999, 'method': 'Browser.close'}))
                    connection.close()
                except Exception:
                    pass
            if window_job is not None:
                try:
                    until = time.monotonic() + 2
                    while connection is not None and time.monotonic() < until:
                        info = win32job.QueryInformationJobObject(window_job, win32job.JobObjectBasicAccountingInformation)
                        if not info['ActiveProcesses']:
                            break
                        time.sleep(.05)
                    win32job.TerminateJobObject(window_job, 0)
                    until = time.monotonic() + 2
                    while time.monotonic() < until:
                        info = win32job.QueryInformationJobObject(window_job, win32job.JobObjectBasicAccountingInformation)
                        if not info['ActiveProcesses']:
                            break
                        time.sleep(.05)
                finally:
                    window_job.Close()
            if thread_handle is not None:
                thread_handle.Close()
            if process_handle is not None:
                try:
                    if win32event.WaitForSingleObject(process_handle, 1000) != win32event.WAIT_OBJECT_0:
                        win32api.TerminateProcess(process_handle, 0)
                        win32event.WaitForSingleObject(process_handle, 1000)
                finally:
                    process_handle.Close()
            if process is not None:
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.terminate()
                    try:
                        process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=2)
