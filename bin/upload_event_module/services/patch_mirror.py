"""Publish and download verified patch bytes through the designated Feishu table."""
import hashlib
import json
import logging
import shutil
import tempfile
import time
import uuid
from pathlib import Path

from openclaw_service.assistant.lighthouse_distribution import (
    BASE, PATCH_TABLE, PART_BYTES, FeishuRuntimeMirror, checksum, validate_spec,
)

RELEASE_COMPONENT = '灯塔补丁发布清单'


def _release_key(manifest):
    if not isinstance(manifest, dict) or any(type(manifest.get(key)) is not int or manifest[key] < 1
            for key in ('major_version', 'target_patch_version')):
        raise ValueError('飞书补丁发布清单的版本号无效。')
    if not isinstance(manifest.get('zip_name'), str) or not manifest['zip_name']:
        raise ValueError('飞书补丁发布清单缺少文件名。')
    _manifest_spec(manifest)
    return manifest['major_version'], manifest['target_patch_version']


def _text(value):
    return ''.join(item.get('text', '') for item in value) if isinstance(value, list) else value


def _read_release(record):
    fields = record.get('fields') or {}
    manifest = json.loads(_text(fields.get('更新说明')) or '{}')
    _release_key(manifest)
    if (fields.get('启用') is not True or _text(fields.get('组件名称')) != RELEASE_COMPONENT
            or _text(fields.get('整包SHA256')) != manifest['zip_sha256']
            or _text(fields.get('版本')) != manifest['target_version']):
        raise ValueError('飞书补丁发布记录与清单不一致，停止更新。')
    return manifest


def _releases(mirror):
    page, seen, records = '', set(), []
    while True:
        data = mirror.request('POST', '/records/search',
            params={'page_size': 100, **({'page_token': page} if page else {})},
            json_payload={'field_names': ['组件名称', '版本', '启用', '整包SHA256', '更新说明'],
                          'filter': {'conjunction': 'and', 'conditions': [
                {'field_name': '组件名称', 'operator': 'is', 'value': [RELEASE_COMPONENT]},
            ]}})
        items = data.get('items')
        if items is None and data.get('has_more') is False and data.get('total') == 0:
            items = []
        if (not isinstance(items, list) or not all(isinstance(item, dict) for item in items)
                or not isinstance(data.get('has_more'), bool)):
            raise ValueError('飞书补丁发布清单响应不完整，停止更新。')
        records.extend(_read_release(record) for record in items if (record.get('fields') or {}).get('启用') is True)
        if not data['has_more']:
            return records
        page = data.get('page_token')
        if not isinstance(page, str) or not page or page in seen:
            raise ValueError('飞书补丁发布清单分页不完整，停止更新。')
        seen.add(page)


def fetch_manifest(*, mirror_factory=FeishuRuntimeMirror):
    mirror = mirror_factory(table=PATCH_TABLE)
    try:
        releases = _releases(mirror)
        if not releases:
            return {}
        latest = max(releases, key=_release_key)
        if any(item != latest for item in releases if _release_key(item) == _release_key(latest)):
            raise ValueError('同一补丁号存在不同发布清单，停止更新。')
        return latest
    finally:
        mirror.close()


def publish_manifest(manifest, *, mirror_factory=FeishuRuntimeMirror):
    """Call only after the complete archive has passed remote download verification."""
    key = _release_key(manifest)
    content = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    mirror = mirror_factory(table=PATCH_TABLE)
    try:
        mirror.ensure_fields()
        for existing in _releases(mirror):
            if _release_key(existing) == key:
                if existing != manifest:
                    raise ValueError('此补丁号已发布且内容不同，请重新打包生成新补丁号。')
                return
        fields = {'文本': '灯塔更新-' + manifest['target_version'], '组件名称': RELEASE_COMPONENT,
                  '版本': manifest['target_version'], '平台': 'Windows x64', '启用': True,
                  '整包SHA256': manifest['zip_sha256'], '文件大小': manifest['zip_size'], '更新说明': content}
        # Stable token covers a lost create response; the preceding read also covers later retries.
        token = str(uuid.uuid5(uuid.NAMESPACE_URL, hashlib.sha256(content.encode('utf-8')).hexdigest()))
        result = mirror.request('POST', '/records', params={'client_token': token}, json_payload={'fields': fields})
        record_id = result['record']['record_id']
        record = mirror.request('GET', '/records/' + record_id).get('record') or {}
        if _read_release(record) != manifest:
            raise ValueError('飞书补丁发布清单回读校验失败。')
    finally:
        mirror.close()


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
                        except Exception as exc:
                            received = downloaded.stat().st_size if downloaded.exists() else 0
                            logging.warning('飞书补丁分片下载未完成（%s/3）：分片 %s/%s，已接收 %.2f/%.2f MiB，原因=%s%s',
                                            attempt + 1, part['index'], len(spec['parts']),
                                            received / 1024 ** 2, part['size'] / 1024 ** 2,
                                            type(exc).__name__, '，稍后重试' if attempt < 2 else '，保留原补丁')
                            downloaded.unlink(missing_ok=True)
                            if attempt == 2:
                                raise
                            time.sleep(2 * (attempt + 1))
                    logging.info('飞书补丁分片校验通过：%s/%s', part['index'], len(spec['parts']))
                    with downloaded.open('rb') as source:
                        shutil.copyfileobj(source, output, 1024 * 1024)
                    downloaded.unlink()
            if assembled.stat().st_size != spec['size'] or checksum(assembled) != spec['sha256']:
                raise ValueError('飞书补丁整包校验失败。')
            assembled.replace(target)
            logging.info('飞书补丁整包大小及 SHA256 校验通过')
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
