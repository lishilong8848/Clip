"""Small, versioned FRPC packages using the existing Feishu dependency mirror."""
import hashlib
import json
import shutil
import subprocess
import uuid
import zipfile
from pathlib import Path

from .config import SPEC


def checksum(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def create_archive(source, output, version):
    from openclaw_service.assistant.lighthouse_distribution import BASE, TABLE, PART_BYTES, validate_spec
    source, output = Path(source), Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    files = {name: checksum(source / name) for name in ('frpc.exe', 'LICENSE')}
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=3) as bundle:
        for name in files:
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            with (source / name).open('rb') as reader, bundle.open(info, 'w') as writer:
                shutil.copyfileobj(reader, writer, 1024 * 1024)
    spec = {'format': 1, 'component': 'frpc', 'version': version, 'platform': 'windows_amd64',
            'base': BASE, 'table': TABLE, 'files': files, 'sha256': checksum(output),
            'size': output.stat().st_size, 'parts': []}
    with output.open('rb') as stream:
        while content := stream.read(PART_BYTES):
            index = len(spec['parts']) + 1
            path = output.with_name(output.name + '.%03d' % index)
            path.write_bytes(content)
            spec['parts'].append({'index': index, 'size': len(content), 'name': path.name,
                                  'sha256': hashlib.sha256(content).hexdigest()})
    return validate_spec(spec, published=False, component='frpc')


def installed(root, spec):
    path = Path(root) / 'versions' / spec['sha256']
    if all((path / name).is_file() and checksum(path / name) == digest for name, digest in spec['files'].items()):
        return path / 'frpc.exe'
    return None


def previous(root):
    from openclaw_service.assistant.lighthouse_distribution import validate_spec
    try:
        spec = validate_spec(json.loads((Path(root) / 'working.json').read_text(encoding='utf-8')), component='frpc')
        binary = installed(root, spec)
        return (binary, spec) if binary else (None, None)
    except (OSError, ValueError, KeyError):
        return None, None


def install(root, *, spec_path=SPEC, mirror_factory=None, cancelled=lambda: False):
    from openclaw_service.assistant.lighthouse_distribution import FeishuRuntimeMirror, validate_spec
    spec = validate_spec(json.loads(Path(spec_path).read_text(encoding='utf-8')), component='frpc')
    binary = installed(root, spec)
    if binary:
        return binary, spec
    root = Path(root)
    cache = root / 'downloads'
    cache.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(cache).free < 150 * 1024 ** 2:
        raise ValueError('公网组件安装至少需要150MiB可用空间。')
    archive = cache / (spec['sha256'] + '.zip')
    if not archive.is_file() or checksum(archive) != spec['sha256']:
        mirror = (mirror_factory or FeishuRuntimeMirror)()
        parts = []
        try:
            for part in spec['parts']:
                if cancelled():
                    raise InterruptedError('Public component installation cancelled.')
                path = cache / (spec['sha256'] + '.%03d' % part['index'])
                if not path.is_file() or path.stat().st_size != part['size'] or checksum(path) != part['sha256']:
                    temp = path.with_suffix(path.suffix + '.tmp')
                    try:
                        mirror.download_part(part, temp)
                        if temp.stat().st_size != part['size'] or checksum(temp) != part['sha256']:
                            raise ValueError('公网组件下载校验失败。')
                        temp.replace(path)
                    finally:
                        temp.unlink(missing_ok=True)
                parts.append(path)
        finally:
            mirror.close()
        temp = archive.with_suffix('.tmp')
        try:
            with temp.open('wb') as writer:
                for part in parts:
                    with part.open('rb') as reader:
                        shutil.copyfileobj(reader, writer, 1024 * 1024)
            if temp.stat().st_size != spec['size'] or checksum(temp) != spec['sha256']:
                raise ValueError('公网组件整包校验失败。')
            temp.replace(archive)
        finally:
            temp.unlink(missing_ok=True)
    if cancelled():
        raise InterruptedError('Public component installation cancelled.')
    target = root / 'versions' / spec['sha256']
    staging = root / ('staging-' + uuid.uuid4().hex)
    staging.mkdir()
    try:
        with zipfile.ZipFile(archive) as bundle:
            members = bundle.infolist()
            if len(members) != 2 or {item.filename for item in members} != {'frpc.exe', 'LICENSE'} \
                    or sum(item.file_size for item in members) > 64 * 1024 ** 2:
                raise ValueError('公网组件压缩包包含非允许文件。')
            for item in members:
                with bundle.open(item) as reader, (staging / item.filename).open('wb') as writer:
                    shutil.copyfileobj(reader, writer, 1024 * 1024)
        if any(checksum(staging / name) != digest for name, digest in spec['files'].items()):
            raise ValueError('公网组件解压校验失败。')
        target.parent.mkdir(exist_ok=True)
        if target.exists():
            # A corrupt release never replaces the last known working release.
            target.rename(root / ('corrupt-' + uuid.uuid4().hex))
        staging.rename(target)
    finally:
        if staging.exists():
            shutil.rmtree(staging)
    return target / 'frpc.exe', spec


def verify_binary(binary, spec, config_file):
    flags = getattr(subprocess, 'CREATE_NO_WINDOW', 0)
    result = subprocess.run([str(binary), '--version'], capture_output=True, timeout=10,
                            creationflags=flags, check=True)
    if result.stdout.decode('utf-8').strip() != spec['version']:
        raise ValueError('FRPC实际版本与分发清单不一致。')
    # Verification never opens a tunnel. Do not print stdout/stderr containing config.
    subprocess.run([str(binary), 'verify', '-c', str(config_file)], capture_output=True,
                   timeout=10, creationflags=flags, check=True)
