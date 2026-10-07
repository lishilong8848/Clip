"""Pause only the owned assistant before changing its code or shared dependencies."""
from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path
import time
import uuid

import httpx

from .protocol import BAT, ServiceError, atomic_json, code_digest, control_key, descriptor, process_stamp, read_json


def affected_path(relative):
    path = Path(str(relative).replace('\\', '/'))
    parts = tuple(part.lower() for part in path.parts)
    return (path.as_posix().lower() in {BAT.lower(), '启动程序openclaw.bat', '启动程序openclaw.py', 'bin/refactored_main.py'}
        or parts[:2] in {('bin', 'openclaw_service'), ('bin', '.venv'), ('bin', 'runtime')}
        or path.as_posix().lower() in {'bin/lan_bitable_template_portal/lighthouse_routes.py',
            'bin/lan_bitable_template_portal/lighthouse_bridge.py', 'bin/clipflow_backend/api_models.py',
            'bin/upload_event_module/services/dependency_bootstrap.py', 'bin/upload_event_module/services/process_lifetime.py'})


class AssistantUpdateGuard:
    def __init__(self, project, state):
        self.project, self.state = Path(project).resolve(), Path(state).resolve()
        self.identity = uuid.uuid4().hex
        self.hold = self.state / 'update-hold.json'
        self.paused, self.was_running = False, False
        self.before = None
        self.record = None

    @staticmethod
    def _same(first, second):
        if not first.is_file() or not second.is_file() or first.stat().st_size != second.stat().st_size:
            return False
        def digest(path):
            result = hashlib.sha256()
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    result.update(chunk)
            return result.digest()
        return hmac.compare_digest(digest(first), digest(second))

    def pause(self, patch_root, files, deletions=(), *, dependency_change=False):
        patch_root = Path(patch_root).resolve()
        changed = dependency_change
        for source in files:
            relative = Path(source).resolve().relative_to(patch_root)
            target = (self.project / relative).resolve()
            if not target.is_relative_to(self.project):
                raise ServiceError('补丁目标路径不安全，未应用更新。', code='unsafe_patch')
            if affected_path(relative) and not self._same(Path(source), target):
                changed = True
        changed = changed or any(affected_path(relative) and (self.project / relative).exists() for relative in deletions)
        if not changed:
            return False
        previous = read_json(self.hold, {})
        if previous and previous.get('process_stamp') is not None and process_stamp(previous.get('pid')) == previous['process_stamp']:
            raise ServiceError('助手已有更新任务，未并发应用补丁。', code='service_updating')
        saved = descriptor(self.state, self.project)
        self.was_running = saved is not None
        self.before = code_digest(self.project)
        import os
        self.record = {'update_id': self.identity, 'pid': os.getpid(), 'process_stamp': process_stamp(os.getpid()),
            'was_running': self.was_running, 'phase': 'stopping', 'at': time.time()}
        atomic_json(self.hold, self.record)
        self.paused = True
        if saved is not None:
            key = control_key(self.state)
            try:
                with httpx.Client(verify=False, trust_env=False, follow_redirects=False, timeout=3) as client:
                    response = client.post(f"http://127.0.0.1:{saved['port']}/shutdown",
                        json={'protocol': saved['protocol'], 'instance': saved['instance'], 'reason': 'update'},
                        headers={'Authorization': 'Bearer ' + key})
                    value = response.json()
                    if response.status_code != 200 or not isinstance(value, dict) or not value.get('ok'):
                        raise ServiceError('助手服务未确认停止，未应用本次更新。', code='service_stop_failed')
            except (httpx.HTTPError, ValueError):
                # The console can exit immediately after accepting shutdown.
                # Inspect the exact process, never retry shutdown or kill by PID.
                pass
            deadline = time.monotonic() + 5
            while process_stamp(saved['pid']) == saved['process_stamp'] and time.monotonic() < deadline:
                time.sleep(.1)
            if process_stamp(saved['pid']) == saved['process_stamp']:
                raise ServiceError('助手服务尚未退出，未替换代码或依赖。', code='service_stop_failed')
        self.record['phase'] = 'applying'
        atomic_json(self.hold, self.record)
        return True

    def finish(self, applied, *, dependencies_ready=True):
        if not self.paused or read_json(self.hold, {}).get('update_id') != self.identity:
            return ''
        compatible = dependencies_ready and (applied or code_digest(self.project) == self.before)
        if not compatible:
            atomic_json(self.hold, {'update_id': self.identity, 'phase': 'needs_repair', 'was_running': self.was_running})
            return '助手代码或依赖尚未恢复一致，已保留配置并停止助手；其他业务不受影响。'
        self.hold.unlink(missing_ok=True)
        # The portal owner resumes a rolled-back worker, or the restarted main
        # program loads the new code. The updater never launches a detached host.
        return ''
