"""Pinned BGE model, distributed through the designated Feishu dependency table."""
import hashlib
import json
import logging
from pathlib import Path
import shutil
import tempfile
import threading
import zipfile

REVISION = '46fbe35fd4374a00fee7de77dfddaeb6dd6a2c59'
BUNDLE = Path('bin/resources/knowledge_model/bge-small-zh-v1.5')
SPEC = Path(__file__).parent / 'openclaw/knowledge-model.json'
INSTALL_LOCK = threading.Lock()
FILES = {
    'config.json': '9088751d39abbf86ec3d19ffca92ad62ad19075f7e59712e6c71217fa125d1d3',
    'model_optimized.onnx': '1294ea4b6331115a353d81f96b85e8c8d7fdcc284453d5b2fab5b016230aad38',
    'special_tokens_map.json': 'b6d346be366a7d1d48332dbc9fdf3bf8960b5d879522b7799ddba59e76237ee3',
    'tokenizer.json': '48cea5d44424912a6fd1ea647bf4fe50b55ab8b1e5879c3275f80e339e8fae26',
    'tokenizer_config.json': 'e6f3b96db926a37d4039995fbf5ad17de158dfb8f6343d607e4dbaad18d75f5a',
}


def verify_model(folder):
    folder = Path(folder)
    for name, expected in FILES.items():
        with (folder / name).open('rb') as stream:
            actual = hashlib.file_digest(stream, 'sha256').hexdigest()
        if actual != expected:
            raise ValueError(f'知识库模型文件校验失败：{name}，请重新更新程序。')
    return folder


def create_archive(source, archive):
    from .lighthouse_distribution import BASE, TABLE, PART_BYTES, checksum
    source, archive = verify_model(source), Path(archive)
    archive.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=3) as bundle:
        for name in sorted(FILES):
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            with (source / name).open('rb') as src, bundle.open(info, 'w') as target:
                shutil.copyfileobj(src, target, 1024 * 1024)
    spec = {'format': 1, 'component': 'knowledge_model', 'version': REVISION,
            'base': BASE, 'table': TABLE, 'size': archive.stat().st_size, 'sha256': checksum(archive), 'parts': []}
    with archive.open('rb') as src:
        while content := src.read(PART_BYTES):
            index = len(spec['parts']) + 1
            part = archive.with_name(archive.name + '.%03d' % index)
            part.write_bytes(content)
            spec['parts'].append({'index': index, 'size': len(content), 'sha256': hashlib.sha256(content).hexdigest(), 'name': part.name})
    return spec


def install_model(models, *, mirror_factory=None):
    from .lighthouse_ai import AssistantError
    from .lighthouse_distribution import FeishuRuntimeMirror, checksum, extract_verified, validate_spec
    models = Path(models).resolve()
    target = models / 'bge-small-zh-v1.5'
    with INSTALL_LOCK:
        try:
            return verify_model(target)
        except (OSError, ValueError):
            pass
        try:
            spec = validate_spec(json.loads(SPEC.read_text(encoding='utf-8')), component='knowledge_model')
        except (OSError, ValueError) as exc:
            raise AssistantError('知识库模型下载清单缺失或无效，请更新程序。', 503) from exc
        cache = models / 'downloads' / spec['sha256']
        cache.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(models).free < 300 * 1024 ** 2:
            raise AssistantError('知识库模型安装需要至少300MiB可用空间。', 503)
        mirror = None
        parts = []
        try:
            for part in spec['parts']:
                path = cache / ('part-%03d' % part['index'])
                if not path.is_file() or path.stat().st_size != part['size'] or checksum(path) != part['sha256']:
                    if mirror is None:
                        mirror = (mirror_factory or FeishuRuntimeMirror)()
                    logging.info('知识库模型正在从飞书下载：%s/%s', part['index'], len(spec['parts']))
                    temporary = path.with_suffix('.tmp')
                    try:
                        mirror.download_part(part, temporary)
                        if temporary.stat().st_size != part['size'] or checksum(temporary) != part['sha256']:
                            raise AssistantError('知识库模型下载校验失败，请重新索引后重试。', 502)
                        temporary.replace(path)
                    finally:
                        temporary.unlink(missing_ok=True)
                parts.append(path)
        finally:
            if mirror is not None:
                mirror.close()
        # Only complete, verified files replace local model assets; partial downloads remain resumable.
        with tempfile.TemporaryDirectory(prefix='bge-install-', dir=models) as staging:
            staging = Path(staging)
            archive = staging / 'model.zip'
            with archive.open('wb') as out:
                for part in parts:
                    with part.open('rb') as src:
                        shutil.copyfileobj(src, out, 1024 * 1024)
            if archive.stat().st_size != spec['size'] or checksum(archive) != spec['sha256']:
                raise AssistantError('知识库模型整包校验失败，未安装。', 502)
            with zipfile.ZipFile(archive) as bundle:
                if len(bundle.infolist()) != len(FILES) or set(bundle.namelist()) != set(FILES) \
                        or sum(item.file_size for item in bundle.infolist()) > 100 * 1024 ** 2:
                    raise AssistantError('知识库模型压缩包内容无效，未安装。', 502)
            extracted = staging / 'verified'
            extract_verified(archive, extracted, spec['sha256'])
            verify_model(extracted)
            target.mkdir(parents=True, exist_ok=True)
            for name in FILES:
                (extracted / name).replace(target / name)
        for part in parts:
            part.unlink(missing_ok=True)
        logging.info('知识库本地 BGE 模型已校验并就绪。')
        return target
