"""Private local host identity and durable state, never browser configuration."""
from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import time
import uuid

PROTOCOL = 1
MAX_CONCURRENT_ACCOUNTS = 20
PROJECT = Path(__file__).resolve().parents[2]
STATE = PROJECT / 'bin/data/lighthouse_openclaw'
BAT = '\u542f\u52a8\u7a0b\u5e8f.bat'


class ServiceError(RuntimeError):
    def __init__(self, message, status=503, code='service_unavailable'):
        super().__init__(message)
        self.status, self.code = status, code


def identity(project=PROJECT, state=STATE):
    if os.name == 'nt':
        import win32api
        import win32con
        import win32security
        token = win32security.OpenProcessToken(win32api.GetCurrentProcess(), win32con.TOKEN_QUERY)
        try:
            user = win32security.ConvertSidToStringSid(win32security.GetTokenInformation(token, win32security.TokenUser)[0])
        finally:
            token.Close()
    else:
        user = str(os.getuid())
    return hashlib.sha256(json.dumps([os.path.normcase(str(Path(project).resolve())),
        os.path.normcase(str(Path(state).resolve())), user]).encode()).hexdigest()[:32]


def read_json(path, default=None):
    try:
        with Path(path).open('rb') as stream:
            content = stream.read(65537)
        if len(content) > 65536:
            return default
        value = json.loads(content)
        return value if isinstance(value, dict) else default
    except (OSError, ValueError, RecursionError):
        return default


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        with temporary.open('x', encoding='utf-8') as output:
            json.dump(value, output, ensure_ascii=False, separators=(',', ':'))
            output.flush()
            os.fsync(output.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def prepare_tool_plugin(source, destination, definitions):
    """Keep identical plugin files untouched so native watchers stay idle."""
    source, destination = Path(source), Path(destination)
    files = {path.name: path.read_bytes() for path in source.iterdir() if path.is_file()}
    names = [item['name'] for item in definitions]
    manifest = json.loads(files['openclaw.plugin.json'])
    manifest.update(activation={'onStartup': True}, contracts={'tools': names},
                    toolMetadata={name: {'optional': False} for name in names})
    for name, value in (('openclaw.plugin.json', manifest), ('tools.json', definitions)):
        files[name] = json.dumps(value, ensure_ascii=False, sort_keys=True).encode('utf-8')
    destination.mkdir(parents=True, exist_ok=True)
    for name, content in files.items():
        path = destination / name
        if not path.is_file() or path.read_bytes() != content:
            path.write_bytes(content)


def control_key(state=STATE):
    import win32crypt
    path = Path(state) / 'service-key.dpapi'
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        key = secrets.token_urlsafe(48)
        encrypted = win32crypt.CryptProtectData(key.encode(), 'ClipFlow OpenClaw Host', None, None, None, 0)
        try:
            with path.open('xb') as output:
                output.write(base64.b64encode(encrypted))
        except FileExistsError:
            pass
    for _ in range(5):
        try:
            saved = path.read_bytes()
            if len(saved) > 8192:
                break
            value = win32crypt.CryptUnprotectData(base64.b64decode(saved, validate=True), None, None, None, 0)[1].decode()
            if 40 <= len(value) <= 100:
                return value
        except Exception:
            time.sleep(.02)
    raise ServiceError('无法读取本人助手服务凭证，请联系管理员；原配置未修改。', code='service_credentials')


def protect_state_directory(state):
    """Protect only owned assistant data, never an external link or business DB."""
    root = Path(state).absolute()
    def reject_link(path):
        if path.is_symlink() or path.is_junction():
            raise ServiceError('助手数据目录含外部链接，未修改目录权限。', code='unsafe_state_directory')
    reject_link(root)
    root.mkdir(parents=True, exist_ok=True)
    resolved_root = root.resolve()
    paths = [root]
    for directory, names, files in os.walk(root, followlinks=False):
        for name in (*names, *files):
            path = Path(directory) / name
            reject_link(path)
            if not path.resolve().is_relative_to(resolved_root):
                raise ServiceError('助手数据目录路径无效，未修改目录权限。', code='unsafe_state_directory')
            paths.append(path)
    if os.name != 'nt':
        return
    import win32api
    import win32con
    import win32security
    token = win32security.OpenProcessToken(win32api.GetCurrentProcess(), win32con.TOKEN_QUERY)
    try:
        owner = win32security.GetTokenInformation(token, win32security.TokenUser)[0]
    finally:
        token.Close()
    acl = win32security.ACL()
    owners = (owner, win32security.CreateWellKnownSid(win32security.WinLocalSystemSid, None),
              win32security.CreateWellKnownSid(win32security.WinBuiltinAdministratorsSid, None))
    expected_sids = {win32security.ConvertSidToStringSid(sid) for sid in owners}
    for sid in owners:
        acl.AddAccessAllowedAceEx(win32security.ACL_REVISION,
            win32con.OBJECT_INHERIT_ACE | win32con.CONTAINER_INHERIT_ACE, 0x1F01FF, sid)
    for path in paths:
        reject_link(path)
        try:
            saved = win32security.GetNamedSecurityInfo(str(path), win32security.SE_FILE_OBJECT,
                win32security.DACL_SECURITY_INFORMATION)
            previous = saved.GetSecurityDescriptorDacl()
            flags = win32con.OBJECT_INHERIT_ACE | win32con.CONTAINER_INHERIT_ACE if path.is_dir() else 0
            entries = [previous.GetAce(index) for index in range(previous.GetAceCount())] if previous else []
            if saved.GetSecurityDescriptorControl()[0] & win32security.SE_DACL_PROTECTED and len(entries) == 3 \
                    and all(ace[0] == (win32security.ACCESS_ALLOWED_ACE_TYPE, flags) and ace[1] == 0x1F01FF for ace in entries) \
                    and {win32security.ConvertSidToStringSid(ace[2]) for ace in entries} == expected_sids:
                continue
        except Exception:
            pass  # Apply the original restrictive ACL if reading/comparing it failed.
        try:
            win32security.SetNamedSecurityInfo(str(path), win32security.SE_FILE_OBJECT,
                win32security.DACL_SECURITY_INFORMATION | win32security.PROTECTED_DACL_SECURITY_INFORMATION,
                None, None, acl, None)
        except Exception:
            raise ServiceError('无法保护本人助手数据，助手未启动；其他业务不受影响。', code='state_permissions') from None


def process_stamp(pid):
    """Match an exact process object, not just a reusable PID."""
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return None
    if os.name == 'nt':
        import win32api
        import win32process
        try:
            handle = win32api.OpenProcess(0x1000, False, pid)
            try:
                if win32process.GetExitCodeProcess(handle) != 259:
                    return None
                return str(win32process.GetProcessTimes(handle)['CreationTime'].timestamp())
            finally:
                handle.Close()
        except Exception:
            return None
    try:
        return Path(f'/proc/{pid}/stat').read_text().split(') ', 1)[1].split()[19]
    except (OSError, IndexError):
        return None


def descriptor(state=STATE, project=PROJECT):
    record = read_json(Path(state) / 'service.json')
    if not record or record.get('identity') != identity(project, state):
        return None
    port, pid = record.get('port'), record.get('pid')
    if (not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535
            or not isinstance(record.get('instance'), str) or len(record['instance']) != 32
            or process_stamp(pid) != record.get('process_stamp') or record.get('process_stamp') is None):
        return None
    return record


def code_digest(project=PROJECT):
    root = Path(project)
    service = root / 'bin/openclaw_service'
    paths = list(service.rglob('*.py'))
    paths += [root / BAT]
    portal = root / 'bin/lan_bitable_template_portal'
    paths += [portal / name for name in ('lighthouse_routes.py', 'lighthouse_bridge.py')]
    assets = service / 'assistant/openclaw'
    paths += [assets / name for name in ('runtime.json', 'distribution.json')]
    paths += [path for path in assets.rglob('*') if path.is_file()]
    digest = hashlib.sha256()
    for path in sorted(paths):
        if path.is_file() and '__pycache__' not in path.parts:
            digest.update(path.relative_to(root).as_posix().encode())
            digest.update(hashlib.sha256(path.read_bytes()).digest())
    return digest.hexdigest()


def migrate_accounts(state):
    """Move only the preceding generation's hashed account directories."""
    state = Path(state).resolve()
    target = state / 'accounts'
    if target.is_symlink() or target.is_junction():
        raise ServiceError('助手账号目录不可使用外部链接，原数据未修改。', code='unsafe_account_directory')
    sources = [path for path in state.iterdir() if re.fullmatch(r'[a-f0-9]{32}', path.name)]
    for source in sources:
        if not source.is_dir() or source.is_symlink() or source.is_junction() or (target / source.name).exists():
            raise ServiceError('助手账号目录存在冲突，请保留原数据并联系管理员。', code='account_migration_conflict')
    target.mkdir(exist_ok=True)
    for source in sources:
        destination = target / source.name
        if not source.resolve().is_relative_to(state) or not destination.resolve().is_relative_to(state):
            raise ServiceError('助手账号目录迁移路径无效。', code='unsafe_account_directory')
        source.rename(destination)
    return len(sources)


class InstanceLock:
    def __init__(self, project=PROJECT, state=STATE):
        self.name = 'Local\\ClipFlowOpenClaw-' + identity(project, state)
        self.handle = None

    def __enter__(self):
        import win32event
        self.handle = win32event.CreateMutex(None, False, self.name)
        if win32event.WaitForSingleObject(self.handle, 0) not in (0, 0x80):
            self.handle.Close()
            self.handle = None
            raise ServiceError('助手服务已在运行，不重复启动。', 409, 'already_running')
        return self

    def __exit__(self, *_):
        if self.handle:
            import win32event
            win32event.ReleaseMutex(self.handle)
            self.handle.Close()
            self.handle = None
