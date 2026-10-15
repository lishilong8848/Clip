"""Publish and download verified patch bytes through the designated Feishu table."""
import hashlib
import json
import logging
import shutil
import tempfile
import time
from pathlib import Path

from openclaw_service.assistant.lighthouse_distribution import (
    BASE, PATCH_TABLE, PART_BYTES, FeishuRuntimeMirror, checksum, validate_spec,
)


def _manifest_spec(manifest):
    spec = validate_spec(manifest.get('feishu_patch'), component='portal_patch')
    if (spec['sha256'] != manifest.get('zip_sha256') or spec['size'] != manifest.get('zip_size')
            or spec['version'] != manifest.get('target_version')):
        raise ValueError('飞书补丁与更新清单的版本、大小或校验值不一致。')
    return spec


def download_patch(manifest, target, *, mirror_factory=FeishuRuntimeMirror):
    spec = _manifest_spec(manifest)
    target = Path(target)
    mirror = mirror_factory(table=PATCH_TABLE)
    try:
        with tempfile.TemporaryDirectory(prefix='.feishu-patch-', dir=target.parent) as temp:
            folder = Path(temp)
            assembled = folder / 'patch.zip'
            with assembled.open('wb') as output:
                for part in spec['parts']:
                    downloaded = folder / 'part.bin'
                    logging.info('飞书补丁下载：分片 %s/%s', part['index'], len(spec['parts']))
                    for attempt in range(3):
                        try:
                            mirror.download_part(part, downloaded)
                            if downloaded.stat().st_size != part['size'] or checksum(downloaded) != part['sha256']:
                                raise ValueError('飞书补丁分片校验失败。')
                            break
                        except Exception:
                            downloaded.unlink(missing_ok=True)
                            if attempt == 2:
                                raise
                            time.sleep(2 * (attempt + 1))
                    with downloaded.open('rb') as source:
                        shutil.copyfileobj(source, output, 1024 * 1024)
                    downloaded.unlink()
            if assembled.stat().st_size != spec['size'] or checksum(assembled) != spec['sha256']:
                raise ValueError('飞书补丁整包校验失败。')
            assembled.replace(target)
    finally:
        mirror.close()
    return target


def publish_patch(archive, manifest, journal, *, mirror_factory=FeishuRuntimeMirror):
    archive, journal = Path(archive), Path(journal)
    if archive.stat().st_size != manifest['zip_size'] or checksum(archive) != manifest['zip_sha256']:
        raise ValueError('待上传补丁与本地清单不一致。')
    spec = {'format': 1, 'component': 'portal_patch', 'version': manifest['target_version'],
            'platform': 'windows_amd64', 'base': BASE, 'table': PATCH_TABLE,
            'sha256': manifest['zip_sha256'], 'size': manifest['zip_size'], 'parts': []}
    journal.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.publish-feishu-patch-', dir=archive.parent) as temp:
        folder = Path(temp)
        with archive.open('rb') as source:
            while content := source.read(PART_BYTES):
                index = len(spec['parts']) + 1
                name = archive.name if spec['size'] <= PART_BYTES else archive.name + '.%03d' % index
                (folder / name).write_bytes(content)
                spec['parts'].append({'index': index, 'name': name, 'size': len(content),
                                      'sha256': hashlib.sha256(content).hexdigest()})
        validate_spec(spec, published=False, component='portal_patch')
        previous = manifest.get('feishu_patch')
        if not journal.exists() and isinstance(previous, dict) and previous.get('sha256') == spec['sha256']:
            previous = _manifest_spec(manifest)
            journal.write_text(json.dumps({spec['sha256'] + ':' + str(p['index']): {
                'record_id': p['record_id'], 'file_token': p['file_token']} for p in previous['parts']}), encoding='utf-8')
        mirror = mirror_factory(table=PATCH_TABLE)
        try:
            spec = mirror.publish(spec, folder, journal, component='portal_patch')
        finally:
            mirror.close()
        download_patch({**manifest, 'feishu_patch': spec}, folder / 'verified.zip', mirror_factory=mirror_factory)
    return spec
