"""Authorized raster preprocessing, never model/vision inference.

Parent interface::

    status() -> {available, ...}
    prepared = preprocess(files, actor, file_id, crop=None,
                          tile_height=1600, overlap=128, ocr=False)
    # Obtain a fresh actor from the caller's authorization callback.
    public = publish(files, fresh_actor, prepared)
    prepared_comparison = compare(files, actor, [first_id, second_id])

``publish`` is the same two-phase, ACL/hash/topic-checked upload API as sandbox.
Prepared results contain private byte outputs and provenance, must remain inside
the server, and must never be accepted back from clients. Crops are [left, top,
right, bottom] integer pixels AFTER EXIF orientation; tile boxes use that same
coordinate system. No scaling, overwriting, OCR cache mutation or external calls.
Static PNG/JPEG/WebP/GIF/BMP only; animated images and vector formats are refused.
Limits: 16M pixels/image, 24M pixels/comparison, 8 tiles, 2MiB/tile, 4MiB total;
OCR is opt-in with a 30s scheduling budget (plus worker queue/teardown time) and
explicit coverage/errors; the existing OCR worker downsamples edges above 2200px.
Originals are unchanged. Generated PNGs strip source EXIF/text metadata and inherit
the union of source scopes and the caller's authorized topic on publication.
Comparison reports exact oriented RGBA pixel differences, not semantic findings;
different dimensions are reported as noncomparable and are never resized.
"""
import hashlib
import importlib.util
import io
import tempfile
import time
import warnings
from contextlib import ExitStack
from pathlib import Path

from .lighthouse_ai import AssistantError, safe_text
from .lighthouse_document_tools import run_local
from .lighthouse_files import IMAGE_EXTENSIONS
from .lighthouse_sandbox import _check_outputs, _inputs, _prepared, _recheck, publish

MAX_PIXELS = 16000000
MAX_COMPARISON_PIXELS = 24000000
OCR_SECONDS = 30


def status():
    available = importlib.util.find_spec('PIL') is not None
    return {'available': available, 'vision_enabled': False, 'external_model': False,
            'message': '本地图片预处理可用；OCR按请求检查离线依赖。' if available else '当前环境不支持图片预处理，请安装Pillow。'}


def _raster(item, content):
    try:
        from PIL import Image, ImageOps
    except ImportError:
        raise AssistantError('当前环境不支持图片预处理，请安装Pillow。', 503) from None
    if Path(item['name']).suffix.lower() not in IMAGE_EXTENSIONS:
        raise AssistantError('图片预处理仅接受已授权的栅格图片附件。')
    try:
        with warnings.catch_warnings():
            warnings.simplefilter('error', Image.DecompressionBombWarning)
            with Image.open(io.BytesIO(content)) as source:
                if source.format not in {'PNG', 'JPEG', 'WEBP', 'GIF', 'BMP'}:
                    raise AssistantError('仅支持PNG、JPEG、WebP、GIF、BMP栅格图片。')
                if source.width * source.height > MAX_PIXELS:
                    raise AssistantError('图片最多1600万像素，请先缩小或拆分。', 413)
                if getattr(source, 'n_frames', 1) != 1:
                    raise AssistantError('不支持多帧或动画图片，请先导出单张截图。')
                source.verify()
            with Image.open(io.BytesIO(content)) as source:
                with ImageOps.exif_transpose(source) as oriented:
                    if oriented.width * oriented.height > MAX_PIXELS:
                        raise AssistantError('方向校正后的图片超过像素上限。', 413)
                    raster = oriented.convert('RGBA')
                    raster.info.clear()
                    return raster
    except AssistantError:
        raise
    except Exception:
        raise AssistantError('图片损坏、尺寸过大或格式不受支持，原图未改动。', 422) from None


def _boxes(size, crop, tile_height, overlap):
    if (type(tile_height) is not int or not 128 <= tile_height <= 4096
            or type(overlap) is not int or not 0 <= overlap < min(512, tile_height)):
        raise AssistantError('切片高度须为128至4096像素，重叠须小于高度且不超过511像素。')
    width, height = size
    if crop is None:
        crop = [0, 0, width, height]
    if (not isinstance(crop, (list, tuple)) or len(crop) != 4
            or any(type(value) is not int for value in crop)):
        raise AssistantError('裁剪区域须为四个整数像素坐标。')
    left, top, right, bottom = crop
    if not 0 <= left < right <= width or not 0 <= top < bottom <= height:
        raise AssistantError('裁剪区域超出图片范围或为空。')
    boxes, position = [], top
    while position < bottom:
        boxes.append([left, position, right, min(position + tile_height, bottom)])
        if len(boxes) > 8:
            raise AssistantError('切片最多8张，请缩小裁剪范围或增加切片高度。', 413)
        if boxes[-1][3] == bottom:
            break
        position += tile_height - overlap
    return list(crop), boxes


def _ocr(outputs, regions):
    metadata = {'requested': True, 'complete': True, 'regions': [],
                'note': 'OCR仅为文字转录，可能识别错误；重叠切片文字未去重，不代表已理解图形。'}
    deadline = time.monotonic() + OCR_SECONDS
    total_characters = 0
    with tempfile.TemporaryDirectory(prefix='lighthouse-visual-') as directory:
        for index, (output, region) in enumerate(zip(outputs, regions)):
            remaining = deadline - time.monotonic()
            if remaining <= 0 or total_characters >= 16000:
                metadata['complete'] = False
                metadata['reason'] = 'budget_exhausted'
                break
            path = Path(directory) / f'ocr-{index:02d}.png'
            path.write_bytes(output['content'])
            try:
                result = run_local('ocr', {'path': str(path),
                    'sha256': hashlib.sha256(output['content']).hexdigest(), 'page': 1},
                    timeout=min(10, remaining))
                raw = result.get('text', '')
                limit = min(4000, 16000 - total_characters)
                text = safe_text(raw, limit=limit)
                complete = bool(result.get('extraction_complete')) and len(raw) <= limit
                total_characters += len(text)
                metadata['complete'] &= complete
                metadata['regions'].append({'output_index': index, 'box': region['box'],
                    'text': text, 'complete': complete,
                    'method': 'local_offline_ocr',
                    'downsampled': max(region['size']) > 2200,
                    'low_confidence_count': result.get('low_confidence_count', 0)})
            except Exception:
                metadata['complete'] = False
                metadata['regions'].append({'output_index': index, 'box': region['box'],
                    'text': '', 'complete': False,
                    'error': '当前区域离线OCR不可用或超时，图片已保留。'})
    return metadata


def preprocess(files, actor, file_id, *, crop=None, tile_height=1600, overlap=128, ocr=False):
    """Prepare PNG crops/vertical tiles and optional bounded offline OCR metadata."""
    if type(ocr) is not bool:
        raise AssistantError('OCR选项须为布尔值。')
    inputs = _inputs(files, actor, [file_id], maximum=1)
    item, content = inputs[0]
    outputs, regions = [], []
    with _raster(item, content) as image:
        crop, boxes = _boxes(image.size, crop, tile_height, overlap)
        size = list(image.size)
        for index, box in enumerate(boxes):
            with image.crop(box) as tile:
                tile.info.clear()
                output = io.BytesIO()
                tile.save(output, format='PNG')
                outputs.append({'name': f'visual-{file_id[:12]}-{index + 1:02d}.png',
                                'content': output.getvalue()})
                _check_outputs(outputs)
                regions.append({'output_index': index, 'box': box,
                                'size': [box[2] - box[0], box[3] - box[1]]})
    result = _prepared(actor, inputs, outputs, operation='raster_preprocess', file_id=file_id,
                       oriented_size=size, crop=crop, regions=regions, vision_enabled=False,
                       ocr={'requested': False, 'complete': False, 'regions': []})
    _recheck(files, actor, result)
    if ocr:
        result['ocr'] = _ocr(outputs, regions)
        _recheck(files, actor, result)
    return result


def compare(files, actor, file_ids):
    """Prepare exact pixel/size comparison metadata for two authorized rasters."""
    if not isinstance(file_ids, list) or len(file_ids) != 2:
        raise AssistantError('图片比较须选择两张不同的授权附件。')
    inputs = _inputs(files, actor, file_ids, maximum=2)
    with ExitStack() as stack:
        left, right = [stack.enter_context(_raster(*value)) for value in inputs]
        from PIL import ImageChops
        if left.width * left.height + right.width * right.height > MAX_COMPARISON_PIXELS:
            raise AssistantError('比较图片合计最多2400万像素。', 413)
        comparison = {'file_ids': file_ids[:], 'sizes': [list(left.size), list(right.size)],
                      'comparable': left.size == right.size, 'coordinates': 'oriented_rgba_pixels',
                      'note': '仅比较方向校正后的像素，未缩放；不代表语义、设备状态或签字判断。'}
        if left.size == right.size:
            diff = stack.enter_context(ImageChops.difference(left, right))
            bands = [stack.enter_context(band) for band in diff.split()]
            mask = bands[0]
            for band in bands[1:]:
                mask = stack.enter_context(ImageChops.lighter(mask, band))
            histogram = mask.histogram()
            count = left.width * left.height - histogram[0]
            comparison.update(identical=count == 0, changed_pixels=count,
                              changed_fraction=count / (left.width * left.height),
                              difference_box=list(mask.getbbox()) if count else None)
        result = _prepared(actor, inputs, [], operation='raster_compare',
                           comparison=comparison, vision_enabled=False)
    _recheck(files, actor, result)
    return result
