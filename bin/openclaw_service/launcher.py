"""A hidden assistant worker owned by the current ClipFlow backend process."""
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import threading
import time

from .protocol import PROTOCOL, ServiceError, control_key, descriptor, process_stamp


class ManagedService:
    def __init__(self, project, state, *, runtime_root=None):
        self.project, self.state = Path(project).resolve(), Path(state).resolve()
        self.runtime_root = Path(runtime_root).resolve() if runtime_root else None
        self.process = None
        self.closed = False
        self.lock = threading.Lock()

    def _stop_saved(self, saved, *, reason):
        """Stop only an authenticated, exact same-project instance; never kill by PID."""
        import httpx
        key = control_key(self.state)
        try:
            with httpx.Client(verify=False, trust_env=False, follow_redirects=False, timeout=2) as client:
                response = client.post(f"http://127.0.0.1:{saved['port']}/shutdown",
                    headers={'Authorization': 'Bearer ' + key},
                    json={'protocol': PROTOCOL, 'instance': saved['instance'], 'reason': reason})
                value = response.json()
                if response.status_code != 200 or not isinstance(value, dict) or not value.get('ok'):
                    raise ServiceError('旧助手未确认退出，未启动重复实例。', code='service_stop_failed')
        except (httpx.HTTPError, ValueError):
            # A successful stop may close its HTTP listener before replying.
            pass
        deadline = time.monotonic() + 5
        while process_stamp(saved['pid']) == saved['process_stamp'] and time.monotonic() < deadline:
            time.sleep(.1)
        if process_stamp(saved['pid']) == saved['process_stamp']:
            raise ServiceError('旧助手尚未退出，未启动重复实例。', code='service_stop_failed')

    def start(self, project=None):
        with self.lock:
            if self.closed:
                raise ServiceError('主程序正在退出，未启动助手。', code='service_stopping')
            if project is not None and Path(project).resolve() != self.project:
                raise ServiceError('助手项目路径不匹配。', code='invalid_project')
            if (self.state / 'update-hold.json').is_file():
                raise ServiceError('助手正在更新，其他业务可继续使用。', code='service_updating')
            if self.process is not None and self.process.poll() is None:
                return False
            saved = descriptor(self.state, self.project)
            if saved:
                parent = saved.get('parent_pid')
                stamp = saved.get('parent_stamp')
                if parent and stamp is not None and process_stamp(parent) == stamp:
                    raise ServiceError('助手已由另一主程序管理，未启动重复实例。', 409, 'service_owner_active')
                # One-generation migration from the formerly detached host.
                self._stop_saved(saved, reason='update')
            python = self.project / 'bin/.venv/Scripts/python.exe'
            if not python.is_file():
                if getattr(sys, 'frozen', False):
                    raise ServiceError('项目 Python 尚未准备完成，未启动助手。', code='python_unavailable')
                python = Path(sys.executable)
            env = os.environ.copy()
            env['PYTHONPATH'] = str(self.project / 'bin')
            env['PYTHONUNBUFFERED'] = '1'
            env['PYTHONIOENCODING'] = 'utf-8'
            env['CLIPFLOW_OPENCLAW_LOG_OWNER_PID'] = str(os.getpid())
            command = [str(python), '-B', '-u', '-m', 'openclaw_service',
                '--project-root', str(self.project), '--state-root', str(self.state),
                '--parent-pid', str(os.getpid())]
            if self.runtime_root is not None:
                command.extend(['--runtime-root', str(self.runtime_root)])
            flags = 0
            if os.name == 'nt':
                flags = subprocess.CREATE_NO_WINDOW | subprocess.BELOW_NORMAL_PRIORITY_CLASS
            from upload_event_module.services.process_lifetime import register_child_process
            self.process = subprocess.Popen(command, cwd=self.project, env=env,
                stdin=subprocess.DEVNULL, creationflags=flags)
            # The child also installs an exact-parent watchdog before heavy imports.
            register_child_process(self.process.pid)
            return True

    def close(self):
        with self.lock:
            self.closed = True
            process, self.process = self.process, None
            if process is None or process.poll() is not None:
                return
            try:
                saved = descriptor(self.state, self.project)
                if saved and saved['pid'] == process.pid:
                    self._stop_saved(saved, reason='system')
            except Exception:
                pass
            if process.poll() is None:
                # Popen holds the exact process handle, including during startup.
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
