"""Container-only Python analysis; importing this module never starts a runtime.

Interface for the parent (no model/route registration here)::

    status() -> {available, reason, message, ...}
    prepared = run(files, actor, code, file_ids, image=None)
    # Reauthorize the request/account NOW, before exposing any prepared data.
    delivered = publish(files, fresh_actor, prepared)

``run`` returns an internal, non-JSON result with bounded ``outputs`` (name/content
bytes), stdout, exit_code, timed_out, input manifest and private provenance. Never expose or
persist it directly. ``publish`` rechecks all source ACLs/hashes/scopes/topic and
uploads via LighthouseFiles, returning public file descriptors. Do not accept a
prepared result from a client. Unavailable runs return a Chinese unsupported
result, never execute code on the host, and do not upload anything.

An administrator must set LIGHTHOUSE_SANDBOX_IMAGE to a trusted, already installed
Linux image's sha256 ID or repository@sha256 digest. It must have Python at
/usr/local/bin/python, no VOLUME declarations, credentials or private data.
Optional ``image`` must equal that configured reference; it cannot select images.
Only a local Docker engine or native Linux Podman with seccomp, cgroups v2 and enforced
memory/CPU/PID quotas is supported. Images are never pulled. No host bind/volume
mounts, inherited host environment, production paths, or network reach the code.

Code reads /tmp/input/input01.<suffix> etc. and manifest.json (file_id/name/path),
and writes flat, nonempty files in /tmp/output (its cwd). Limits: 6 inputs/20MiB
total, 64KiB code, one concurrent run, 30s wall/15s CPU per process, 256MiB memory/no swap,
one CPU, 32 PIDs, 32MiB tmpfs, 8 outputs/4MiB total/2MiB each, 32KiB log.
The stdin protocol avoids Docker's prohibition on cp into a read-only rootfs.
Containers still share their engine's kernel; the operator owns engine/image
patching and trust. Killing the service itself requires operator cleanup of any
remaining lighthouse-analysis-* containers.
"""
import base64
import binascii
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

from .lighthouse_ai import AssistantError, safe_text
from .lighthouse_document_tools import checked_file
from .lighthouse_files import ALLOWED_EXTENSIONS, FILE_NAMESPACE, GENERATED_EXTENSIONS

MAX_INPUT_BYTES = 20 * 1024 * 1024
MAX_OUTPUT_BYTES = 4 * 1024 * 1024
MAX_FILE_BYTES = 2 * 1024 * 1024
MAX_OUTPUTS = 8
MAX_LOG_BYTES = 32768
WALL_SECONDS = 30
_WIRE_BYTES = 6 * 1024 * 1024
_SLOT = threading.BoundedSemaphore(1)
_IMAGE_ID = re.compile(r'sha256:[a-f0-9]{64}')
_IMAGE_REF = re.compile(r'(?:[a-z0-9][a-z0-9./:_-]*@)?sha256:[a-f0-9]{64}')
_OUTPUT_NAME = re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,100}')

# This script is data passed only to a container's Python, never a host Python.
_RUNNER = r'''
import base64, json, os, resource, signal, stat, subprocess, sys
os.environ.clear()
os.environ.update(PATH='/usr/local/bin:/usr/bin:/bin', HOME='/tmp', TMPDIR='/tmp',
                  OMP_NUM_THREADS='1', OPENBLAS_NUM_THREADS='1', MKL_NUM_THREADS='1')
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
resource.setrlimit(resource.RLIMIT_CPU, (15, 15))
resource.setrlimit(resource.RLIMIT_FSIZE, (2097152, 2097152))
resource.setrlimit(resource.RLIMIT_NOFILE, (64, 64))
signal.alarm(25)
with open('/proc/self/status', encoding='ascii') as source:
    process_status = dict(line.split(':', 1) for line in source if ':' in line)
if (os.getuid() == 0 or process_status['NoNewPrivs'].strip() != '1'
        or process_status['Seccomp'].strip() != '2'
        or any(int(process_status[key].strip(), 16) for key in ('CapInh', 'CapPrm', 'CapEff', 'CapBnd', 'CapAmb'))):
    raise RuntimeError('process isolation unavailable')
# Verify effective kernel limits before reading inputs or executing caller code.
def quota(name):
    with open('/sys/fs/cgroup/' + name, encoding='ascii') as source:
        return source.read().strip()
if not (0 < int(quota('memory.max')) <= 268435456
        and int(quota('memory.swap.max')) == 0
        and 0 < int(quota('pids.max')) <= 32):
    raise RuntimeError('resource isolation unavailable')
cpu_quota, cpu_period = map(int, quota('cpu.max').split())
if not 0 < cpu_quota <= cpu_period:
    raise RuntimeError('CPU isolation unavailable')
payload = json.loads(sys.stdin.buffer.read(30000001))
os.mkdir('/tmp/input', 0o700)
os.mkdir('/tmp/output', 0o700)
manifest = []
for item in payload['inputs']:
    path = '/tmp/input/' + item['path']
    with open(path, 'wb') as target:
        target.write(base64.b64decode(item['content'], validate=True))
    os.chmod(path, 0o400)
    manifest.append({key: item[key] for key in ('file_id', 'name', 'path')})
with open('/tmp/input/manifest.json', 'w', encoding='utf-8') as target:
    json.dump(manifest, target, ensure_ascii=False)
with open('/tmp/main.py', 'w', encoding='utf-8') as target:
    target.write(payload['code'])
del payload
timed_out = False
try:
    with open('/tmp/log', 'wb') as log:
        child = subprocess.run(['/usr/local/bin/python', '-I', '-B', '/tmp/main.py'],
                               cwd='/tmp/output', stdin=subprocess.DEVNULL,
                               stdout=log, stderr=subprocess.STDOUT, timeout=22)
    exit_code = child.returncode
except subprocess.TimeoutExpired:
    timed_out, exit_code = True, 124
with open('/tmp/log', 'rb') as log:
    text = log.read(32769)
outputs, total = [], 0
directory = os.open('/tmp/output', os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
try:
    for index, name in enumerate(os.listdir(directory)):
        if index >= 8:
            raise ValueError('too many outputs')
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory)
        with os.fdopen(fd, 'rb') as source:
            info = os.fstat(source.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or not 0 < info.st_size <= 2097152:
                raise ValueError('invalid output')
            content = source.read(2097153)
        total += len(content)
        if len(content) > 2097152 or total > 4194304:
            raise ValueError('outputs too large')
        outputs.append({'name': name, 'content': base64.b64encode(content).decode('ascii')})
finally:
    os.close(directory)
print(json.dumps({'exit_code': exit_code, 'timed_out': timed_out, 'stdout': text[:32768].decode('utf-8', 'replace'),
                  'log_truncated': len(text) > 32768, 'outputs': outputs}))
'''


def _topic(actor):
    topic = actor.get('_feishu_topic') if actor.get('channel', '').startswith('feishu:') else None
    return topic.get('id') if isinstance(topic, dict) else None


def _inputs(files, actor, file_ids, maximum=6):
    if (not isinstance(file_ids, list) or len(file_ids) > maximum
            or any(not isinstance(identity, str) for identity in file_ids)
            or len(set(file_ids)) != len(file_ids)):
        raise AssistantError(f'每次最多选择{maximum}个不同的授权附件，不接受本机路径。')
    result, total = [], 0
    for identity in file_ids:
        item = files.get(actor, identity)
        if item.get('topic_id') != _topic(actor):
            raise AssistantError('处理附件须属于当前话题，不能合并其他话题的来源。', 403)
        content = checked_file(item)
        total += len(content)
        if total > MAX_INPUT_BYTES:
            raise AssistantError('输入附件合计最多20MiB。', 413)
        result.append((item, content))
    return result


def _prepared(actor, inputs, outputs, **metadata):
    return {'ok': True, **metadata, 'outputs': outputs, '_actor_id': actor['id'],
            '_topic_id': _topic(actor), '_sources': [
                {key: item.get(key) for key in ('id', 'sha256', 'source_scopes', 'topic_id')}
                for item, _ in inputs]}


def _recheck(files, actor, result):
    if actor['id'] != result['_actor_id'] or _topic(actor) != result['_topic_id']:
        raise AssistantError('处理结果不属于当前账号或话题。', 403)
    scopes = set()
    for source in result['_sources']:
        item = files.get(actor, source['id'])
        if any(item.get(key) != source.get(key) for key in ('sha256', 'source_scopes', 'topic_id')):
            raise AssistantError('来源附件或权限范围已变化，请重新处理。', 409)
        checked_file(item)
        scopes.update(item.get('source_scopes', []))
    return sorted(scopes)


def _check_outputs(outputs):
    if not isinstance(outputs, list) or len(outputs) > MAX_OUTPUTS:
        raise AssistantError('生成文件数量超过上限。', 413)
    names, total = set(), 0
    for item in outputs:
        if not isinstance(item, dict) or set(item) != {'name', 'content'}:
            raise AssistantError('生成文件格式无效。', 422)
        name, content = item['name'], item['content']
        if (not isinstance(name, str) or not _OUTPUT_NAME.fullmatch(name) or '..' in name
                or name.casefold() in names
                or Path(name).suffix.lower() not in ALLOWED_EXTENSIONS | GENERATED_EXTENSIONS):
            raise AssistantError('生成文件名称或格式不安全。', 422)
        if not isinstance(content, bytes) or not 0 < len(content) <= MAX_FILE_BYTES:
            raise AssistantError('生成文件为空或超过2MiB。', 413)
        names.add(name.casefold())
        total += len(content)
        if total > MAX_OUTPUT_BYTES:
            raise AssistantError('生成文件合计超过4MiB。', 413)


def publish(files, actor, result):
    """Call only with a freshly reauthorized actor and an internal prepared result.

    No results are exposed before rechecking every input. On a partial failure,
    newly uploaded records are tombstoned; their private disk bytes await storage
    cleanup. A storage failure can prevent rollback and is explicitly reported.
    No transaction/idempotency API exists; do not automatically retry publishing.
    """
    if not result.get('ok'):
        return result
    _check_outputs(result['outputs'])
    scopes = _recheck(files, actor, result)
    attachments = []
    try:
        for output in result['outputs']:
            _recheck(files, actor, result)
            attachments.append(files.upload(actor, output['name'], output['content'],
                                            extract=False, source_scopes=scopes))
        _recheck(files, actor, result)
    except Exception:
        try:
            for attachment in attachments:
                record = files.store.get_document(FILE_NAMESPACE, attachment['id'])
                if not record or record.get('owner') != actor['id']:
                    raise ValueError()
                files.store.put_document(FILE_NAMESPACE, attachment['id'], {**record, 'deleted_at': time.time()})
        except Exception:
            raise AssistantError('结果发布失败，部分生成文件撤回未确认，请联系管理员。', 503) from None
        raise
    return {key: value for key, value in result.items()
            if not key.startswith('_') and key != 'outputs'} | {'files': attachments}


class _RuntimeError(Exception):
    pass


def _command(runtime, args, *, data=None, timeout=8, limit=65536):
    """Bound both pipes while executing only the installed container CLI."""
    with tempfile.TemporaryDirectory(prefix='lighthouse-cli-') as directory:
        env = {key: value for key, value in os.environ.items() if key.upper() in {
            'PATH', 'SYSTEMROOT', 'WINDIR', 'TEMP', 'TMP', 'HOME', 'USERPROFILE', 'XDG_RUNTIME_DIR'}}
        env.update(DOCKER_CONFIG=directory, XDG_CONFIG_HOME=directory)
        command = [*runtime, *args]
        try:
            process = subprocess.Popen(command, stdin=subprocess.PIPE if data is not None else subprocess.DEVNULL,
                                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=directory,
                                       env=env, shell=False, close_fds=True)
        except OSError:
            raise _RuntimeError() from None
        buffers, exceeded = [bytearray(), bytearray()], threading.Event()

        def drain(stream, buffer):
            try:
                while True:
                    block = stream.read(8192)
                    if not block:
                        break
                    if len(buffer) + len(block) > limit:
                        exceeded.set()
                        process.kill()
                        break
                    buffer.extend(block)
            except (OSError, ValueError):
                exceeded.set()
            finally:
                stream.close()

        def feed():
            try:
                process.stdin.write(data)
            except (BrokenPipeError, OSError):
                pass
            finally:
                process.stdin.close()

        threads = [threading.Thread(target=drain, args=(stream, buffer), daemon=True)
                   for stream, buffer in zip((process.stdout, process.stderr), buffers)]
        if data is not None:
            threads.append(threading.Thread(target=feed, daemon=True))
        for thread in threads:
            thread.start()
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            raise AssistantError('容器处理超时，原附件未改动。', 408) from None
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=3)
            for thread in threads:
                thread.join(timeout=3)
        if exceeded.is_set() or any(thread.is_alive() for thread in threads) or process.returncode:
            # CLI diagnostics can contain private paths/environment; never relay them.
            raise _RuntimeError()
        return bytes(buffers[0])


def _runtime():
    docker = shutil.which('docker')
    if docker:
        endpoint = 'npipe:////./pipe/docker_engine' if os.name == 'nt' else 'unix:///var/run/docker.sock'
        return [docker, '--host', endpoint]
    podman = shutil.which('podman')
    if podman and sys.platform.startswith('linux'):
        return [podman, '--remote=false']
    return None


def _unsupported(reason):
    return {'ok': False, 'available': False, 'reason': reason,
            'message': '当前环境不支持安全沙箱高级分析：需要已安装且可用的本地Docker或Linux Podman、cgroups v2资源隔离及管理员配置的本地可信固定镜像；不会在本机执行Python，也不会自动下载镜像。'}


def _probe(image=None):
    configured = os.environ.get('LIGHTHOUSE_SANDBOX_IMAGE', '')
    if not _IMAGE_REF.fullmatch(configured):
        return _unsupported('image_not_configured'), None, None
    if image is not None and image != configured:
        return _unsupported('image_not_allowed'), None, None
    runtime = _runtime()
    if not runtime:
        return _unsupported('runtime_missing'), None, None
    try:
        podman = '--remote=false' in runtime
        if podman:
            info = json.loads(_command(runtime, ['info', '--format', 'json']))['host']
            secure = (info['os'] == 'linux' and info['security']['seccompEnabled']
                      and info['cgroupVersion'] == 'v2'
                      and {'cpu', 'memory', 'pids'} <= set(info['cgroupControllers']))
        else:
            template = ('{"os":{{json .OSType}},"security":{{json .SecurityOptions}},'
                        '"memory":{{json .MemoryLimit}},"swap":{{json .SwapLimit}},'
                        '"pids":{{json .PidsLimit}},"cpu":{{json .CPUQuota}},"cgroups":{{json .CgroupVersion}}}')
            info = json.loads(_command(runtime, ['info', '--format', template]))
            secure = (info['os'] == 'linux' and info['cgroups'] == '2'
                      and any('seccomp' in option for option in info['security'])
                      and all(info[key] is True for key in ('memory', 'swap', 'pids', 'cpu')))
        if not secure:
            return _unsupported('isolation_unavailable'), None, None
        template = '{"id":{{json .Id}},"os":{{json .Os}},"volumes":{{json .Config.Volumes}}}'
        installed = json.loads(_command(runtime, ['image', 'inspect', '--format', template, configured]))
        identity = installed['id']
        if not _IMAGE_ID.fullmatch(identity) or installed['os'] != 'linux' or installed['volumes']:
            return _unsupported('unsafe_image'), None, None
    except (AssistantError, _RuntimeError, ValueError, KeyError, TypeError):
        return _unsupported('runtime_or_image_unavailable'), None, None
    return {'available': True, 'reason': '', 'message': '本地容器沙箱可用，运行时将再次检查隔离配置。'}, runtime, identity


def status():
    """Read-only capability probe; no image pulls, containers or host Python."""
    return _probe()[0]


def _container_safe(info, podman=False):
    config, host = info['Config'], info['HostConfig']
    mounts = info.get('Mounts', [])
    caps_safe = (not host.get('CapAdd') and
                 (all(key in info and not info[key] for key in ('EffectiveCaps', 'BoundingCaps')) if podman
                  else 'ALL' in {str(cap).upper() for cap in host['CapDrop']}))
    return (config['User'] == '65534:65534' and not config.get('Volumes')
            and host['ReadonlyRootfs'] is True and host['NetworkMode'] == 'none'
            and host['PidMode'] in {'', 'private'} and host['IpcMode'] == 'none'
            and host.get('CgroupMode' if podman else 'CgroupnsMode') == 'private'
            and not host.get('Privileged') and not host.get('Binds') and not host.get('Devices')
            and not host.get('DeviceRequests') and not host.get('VolumesFrom')
            and set(host.get('Tmpfs', {})) == {'/tmp'}
            and {'rw', 'noexec', 'nosuid', 'nodev', 'size=32m'} <= set(host['Tmpfs']['/tmp'].split(','))
            and host['LogConfig']['Type'] == 'none'
            and all(mount['Type'] == 'tmpfs' and mount['Destination'] == '/tmp' for mount in mounts)
            and host['PidsLimit'] == 32 and host['Memory'] == 268435456
            and host['MemorySwap'] == 268435456
            and (host.get('NanoCpus') == 1000000000
                 or host.get('CpuQuota') == host.get('CpuPeriod') == 100000)
            and caps_safe
            and any(option in {'no-new-privileges', 'no-new-privileges:true'} for option in host['SecurityOpt'])
            and not any('unconfined' in option for option in host['SecurityOpt']))


def _decode_packet(content):
    try:
        packet = json.loads(content)
        if (not isinstance(packet, dict) or set(packet) != {'exit_code', 'timed_out', 'stdout', 'log_truncated', 'outputs'}
                or type(packet['exit_code']) is not int or not -255 <= packet['exit_code'] <= 255
                or type(packet['timed_out']) is not bool
                or not isinstance(packet['stdout'], str)
                or len(packet['stdout'].encode('utf-8')) > MAX_LOG_BYTES * 3
                or type(packet['log_truncated']) is not bool
                or not isinstance(packet['outputs'], list) or len(packet['outputs']) > MAX_OUTPUTS):
            raise ValueError()
        outputs = []
        for output in packet['outputs']:
            if not isinstance(output, dict) or set(output) != {'name', 'content'} or not isinstance(output['content'], str):
                raise ValueError()
            outputs.append({'name': output['name'], 'content': base64.b64decode(output['content'], validate=True)})
        _check_outputs(outputs)
    except AssistantError:
        raise
    except (ValueError, TypeError, KeyError, binascii.Error, RecursionError):
        raise AssistantError('容器结果格式无效，未发布任何文件。', 422) from None
    return {**packet, 'stdout': safe_text(packet['stdout'], limit=MAX_LOG_BYTES), 'outputs': outputs}


def run(files, actor, code, file_ids, image=None):
    """Prepare a private result. ``publish`` requires a fresh authorization later."""
    try:
        valid_code = isinstance(code, str) and bool(code.strip()) and len(code.encode('utf-8')) <= 65536
    except UnicodeError:
        valid_code = False
    if not valid_code:
        raise AssistantError('分析代码须为非空Python文本，最多64KiB。', 413)
    inputs = _inputs(files, actor, file_ids)
    capability, runtime, identity = _probe(image)
    if not capability['available']:
        return capability
    if not _SLOT.acquire(blocking=False):
        raise AssistantError('沙箱正在处理其他任务，请稍后重试。', 429)
    name = 'lighthouse-analysis-' + uuid.uuid4().hex
    prepared = _prepared(actor, inputs, [])
    manifest = []
    for index, (item, content) in enumerate(inputs, 1):
        suffix = Path(item['name']).suffix.lower()
        if not re.fullmatch(r'\.[a-z0-9]{1,10}', suffix):
            suffix = '.bin'
        manifest.append({'file_id': item['id'], 'name': safe_text(item['name'], limit=180),
                         'path': f'input{index:02d}{suffix}', 'content': base64.b64encode(content).decode('ascii')})
    payload = json.dumps({'code': code, 'inputs': manifest}, ensure_ascii=True).encode('utf-8')
    args = ['create', '--name', name, '--pull=never', '--interactive', '--read-only',
            '--user', '65534:65534', '--cap-drop=ALL', '--security-opt=no-new-privileges',
            '--network=none', '--ipc=none', '--pid=', '--cgroupns=private',
            '--pids-limit=32', '--memory=256m', '--memory-swap=256m', '--cpus=1',
            '--ulimit=nofile=64:64', '--ulimit=core=0:0', '--ulimit=fsize=2097152:2097152',
            '--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=32m,mode=1777', '--no-healthcheck',
            '--restart=no', '--log-driver=none', '--stop-timeout=1', '--workdir=/tmp',
            '--entrypoint=/usr/local/bin/python']
    if '--remote=false' in runtime:
        args.remove('--pid=')
        args += ['--http-proxy=false', '--unsetenv-all', '--image-volume=ignore',
                 '--read-only-tmpfs=false', '--no-hosts', '--pid=private']
    args += [identity, '-I', '-B', '-c', _RUNNER]
    attempted = False
    try:
        _recheck(files, actor, prepared)
        attempted = True
        _command(runtime, args)
        info = json.loads(_command(runtime, ['inspect', '--format', '{{json .}}', name]))
        if not _container_safe(info, podman='--remote=false' in runtime):
            raise _RuntimeError()
        packet = _decode_packet(_command(runtime, ['start', '--attach', '--interactive', name],
                                         data=payload, timeout=WALL_SECONDS, limit=_WIRE_BYTES))
        _recheck(files, actor, prepared)
        return {**prepared, **packet, 'available': True,
                'inputs': [{key: item[key] for key in ('file_id', 'name', 'path')} for item in manifest]}
    except AssistantError:
        raise
    except (_RuntimeError, ValueError, KeyError, TypeError):
        raise AssistantError('安全容器运行失败或隔离条件不满足，未发布任何文件；原附件未改动。', 503) from None
    finally:
        try:
            if attempted:
                try:
                    _command(runtime, ['rm', '--force', '--volumes', name])
                except (AssistantError, _RuntimeError):
                    # A failed create may have made no container at all.
                    if _command(runtime, ['ps', '--all', '--filter', f'name=^/?{name}$', '--format', '{{.Names}}']).strip():
                        raise _RuntimeError()
        except (AssistantError, _RuntimeError):
            raise AssistantError('沙箱清理未确认，请联系管理员检查；结果未发布。', 503) from None
        finally:
            _SLOT.release()
