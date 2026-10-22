"""Bounded source-image snapshots; access is granted by the sharing batch, not a URL."""
import base64
import hashlib
import io
import re
import uuid

from .learning import LearningError

MAX_IMAGES = 24
MAX_IMAGE = 20 * 1024 * 1024


def document_images(blocks, url, revision, title):
    images = []
    for block in blocks:
        image = block.get('image') if isinstance(block, dict) else None
        if image is None:
            continue
        token = image.get('token') if isinstance(image, dict) else None
        if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9]+', token):
            raise LearningError('分享材料中的图片标识不完整，请重新读取。', 502)
        images.append({'token': token, 'source_url': url, 'revision': revision,
                       'block_id': block.get('block_id', ''), 'label': f'{title} · 图{len(images) + 1}'})
        if len(images) > MAX_IMAGES:
            raise LearningError('单份分享材料最多包含24张图片，请拆分后读取，不会截断出题。', 413)
    return images


def jpeg(content):
    from PIL import Image, ImageOps, UnidentifiedImageError
    if not isinstance(content, bytes) or not 0 < len(content) <= MAX_IMAGE:
        raise LearningError('题图为空或超过20MiB，请核对原图片。', 413)
    try:
        with Image.open(io.BytesIO(content)) as image:
            if image.width * image.height > 40_000_000 or image.format not in {'JPEG', 'PNG', 'WEBP', 'GIF', 'BMP'}:
                raise LearningError('题图格式不支持或像素过大，请缩小后重试。', 413)
            image = ImageOps.exif_transpose(image).convert('RGB')
            output = io.BytesIO()
            image.save(output, format='JPEG', quality=92)
            data = output.getvalue()
            if len(data) > MAX_IMAGE:
                raise LearningError('题图转换后超过20MiB，请缩小后重试。', 413)
            return data
    except (UnidentifiedImageError, OSError, ValueError, Image.DecompressionBombError):
        raise LearningError('题图内容无法识别，请核对原图片。', 422) from None


class SharingImages:
    def __init__(self, learning, portal):
        self.root = (learning.root / 'sharing_images').resolve()
        self.portal = portal

    def path(self, identity):
        if not isinstance(identity, str) or not re.fullmatch(r'[a-f0-9]{64}', identity):
            raise LearningError('题图标识无效。', 404)
        path = (self.root / (identity + '.jpg')).resolve()
        if path.parent != self.root:
            raise LearningError('题图路径无效。', 403)
        return path

    def download(self, item):
        from .sharing_sources import cloud_reader
        token = item.get('token')
        if not isinstance(token, str) or not re.fullmatch(r'[A-Za-z0-9]+', token):
            raise LearningError('题图来源无效，请重新读取材料。', 422)
        _, _, kind, get = cloud_reader(self.portal, item.get('source_url'))
        if kind != 'docx':
            raise LearningError('题图来源必须为飞书文档。', 422)
        return get('drive/v1/medias/' + token + '/download').get('content')

    def save(self, identity, data):
        path = self.path(identity)
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix('.' + uuid.uuid4().hex + '.tmp')
        try:
            temporary.write_bytes(data)
            temporary.replace(path)
        finally:
            temporary.unlink(missing_ok=True)
        return path

    def prepare(self, items):
        from openclaw_service.assistant.lighthouse_ai import AssistantError
        from openclaw_service.assistant.lighthouse_files import recognize_text
        if not isinstance(items, list) or len(items) > MAX_IMAGES:
            raise LearningError('每批分享材料最多24张图片，请拆分后读取。', 413)
        result, seen, total = [], set(), 0
        for item in items:
            content = self.download(item)
            if not isinstance(content, bytes):
                raise LearningError('题图下载未完整返回，请重新读取。', 502)
            total += len(content)
            if total > 100 * 1024 * 1024:
                raise LearningError('本批题图超过100MiB，请拆分后读取。', 413)
            identity = hashlib.sha256(content).hexdigest()
            if identity in seen:
                continue
            data = jpeg(content)
            rendered_hash = hashlib.sha256(data).hexdigest()
            self.save(rendered_hash, data)
            try:
                text = recognize_text(data).strip()
            except AssistantError:
                text = ''
            if len(text) > 2000:
                raise LearningError('单张题图文字超过2000字，请拆分图片，避免遗漏依据。', 413)
            result.append({**item, 'id': identity, 'sha256': rendered_hash,
                           'text': text, 'mode': 'ocr' if text else 'unreadable'})
            seen.add(identity)
        from .sharing_sources import cloud_reader
        for url, revision in {(item['source_url'], item['revision']) for item in items}:
            _, token, _, get = cloud_reader(self.portal, url)
            if get('docx/v1/documents/' + token).get('document', {}).get('revision_id') != revision:
                raise LearningError('读取题图期间材料已变化，请重新生成；未混用不同版本。', 409)
        return result

    def content(self, item):
        path = self.path(item.get('sha256'))
        try:
            if path.stat().st_size <= MAX_IMAGE:
                content = path.read_bytes()
                if hashlib.sha256(content).hexdigest() == item.get('sha256'):
                    return content
        except FileNotFoundError:
            pass
        original = self.download(item)
        if not isinstance(original, bytes) or hashlib.sha256(original).hexdigest() != item['id']:
            raise LearningError('原题图已变化，无法恢复发布时的图片；未替换成其他版本。', 409)
        content = jpeg(original)
        if hashlib.sha256(content).hexdigest() != item.get('sha256'):
            raise LearningError('题图快照校验未通过，请联系管理员核对。', 409)
        self.save(item['sha256'], content)
        return content

    def model_input(self, item):
        from PIL import Image
        with Image.open(io.BytesIO(self.content(item))) as image:
            image.thumbnail((1600, 1600))
            output = io.BytesIO()
            image.save(output, format='JPEG', quality=85)
        return {'id': item['id'], 'ocr_text': item['text'],
                'data_url': 'data:image/jpeg;base64,' + base64.b64encode(output.getvalue()).decode('ascii')}


def public_images(batch, ids=None, *, editor=False):
    images = {item['id']: item for item in batch.get('source', {}).get('images', [])}
    return [{'id': item['id'], 'label': item['label'], 'mode': item['mode'],
             'url': f"/api/learning/sharing/{batch['id']}/images/{item['id']}",
             **({'text': item['text']} if editor else {})}
            for identity in dict.fromkeys(ids if ids is not None else images) if (item := images.get(identity))]
