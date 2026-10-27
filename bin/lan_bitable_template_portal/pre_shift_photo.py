"""Render the handover stamp locally; never invent GPS or live temperature."""
import datetime as dt
import base64
import io
import json
import math
import os
import re
import threading
import unicodedata
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont, ImageOps
try:
    from pillow_heif import register_heif_opener
except (ImportError, OSError):
    register_heif_opener = None
else:
    register_heif_opener()

TZ = dt.timezone(dt.timedelta(hours=8))
_RENDER_SLOTS = threading.BoundedSemaphore(2)
_IMPORT_SLOT = threading.BoundedSemaphore(1)
_OCR_SLOT = threading.BoundedSemaphore(1)
_MARKER = b'ClipFlowWatermark:'
DELIVERY_IMAGE_BYTES = 10 * 1024 * 1024
UPLOAD_IMAGE_EXTS = {'.jpg', '.jpeg', '.jpe', '.jfif', '.png', '.webp', '.heic', '.heif', '.hif',
                     '.avif', '.gif', '.tif', '.tiff', '.bmp', '.dib', '.ico', '.icns', '.jp2', '.j2k',
                     '.psd', '.tga', '.pcx', '.ppm', '.pgm', '.pbm', '.pnm', '.qoi'}


def prepare_upload(source, name):
    """Normalize common photo formats while preserving capture time and orientation."""
    if Path(name).suffix.lower() in {'.heic', '.heif', '.hif'} and register_heif_opener is None:
        raise ValueError('HEIC照片解码组件尚未就绪，请等待程序更新组件完成后重试；其他格式照片仍可上传。')
    # Decode only one full-resolution phone original at a time.
    with _IMPORT_SLOT, Image.open(source) as image:
        ext = {'JPEG': '.jpg', 'PNG': '.png', 'WEBP': '.webp'}.get(image.format)
        pixels = image.width * image.height
        if pixels > 60_000_000:
            raise ValueError('图片像素过大，请缩小照片后上传。')
        if not ext or pixels > 25_000_000 or getattr(image, 'n_frames', 1) > 1:
            exif, comment = image.getexif().tobytes(), image.info.get('comment', b'')
            image.draft('RGB', (2560, 2560))
            image.seek(0)
            image.load()
            image.thumbnail((4096, 4096), Image.Resampling.LANCZOS)
            ext = '.png' if 'A' in image.getbands() or 'transparency' in image.info else '.jpg'
            converted = image.convert('RGBA' if ext == '.png' else 'RGB')
            temporary = Path(source).with_suffix('.normalized')
            try:
                converted.save(temporary, format='PNG' if ext == '.png' else 'JPEG', quality=95, exif=exif, comment=comment)
                # Close the original Windows file before replacing it.
                image.close()
                temporary.replace(source)
            finally:
                converted.close()
                temporary.unlink(missing_ok=True)
        suffix = Path(name).suffix.lower()
        return name if suffix == ext or suffix == '.jpeg' and ext == '.jpg' else str(Path(name).with_suffix(ext))


def _logo_image(value):
    if not value:
        return None
    try:
        if value == 'vnet':
            source = Path(__file__).resolve().parent / 'frontend' / 'dist' / 'assets' / 'vnet-logo.png'
        else:
            match = re.fullmatch(r'data:image/(png|jpeg|webp);base64,([A-Za-z0-9+/=]+)', value)
            if not match or len(match[2]) > 2800:
                raise ValueError()
            data = base64.b64decode(match[2], validate=True)
            if not 0 < len(data) <= 2048:
                raise ValueError()
            source = io.BytesIO(data)
        with Image.open(source) as image:
            if image.width > 512 or image.height > 512 or image.width * image.height > 65536:
                raise ValueError()
            if value != 'vnet' and (image.format or '').lower() != match[1]:
                raise ValueError()
            image = ImageOps.exif_transpose(image).convert('RGBA')
            bounds = image.getbbox()
            if not bounds:
                raise ValueError()
            return image.crop(bounds)
    except (ValueError, OSError, Image.DecompressionBombError):
        raise ValueError('Logo图片无效，请重新选择PNG、JPG或WebP图片。') from None


def photo_options(value):
    if value is None:
        return None
    allowed = {'enabled', 'existing', 'scale', 'title', 'area', 'time', 'weather', 'location', 'logo', 'position'}
    if not isinstance(value, dict) or set(value) - allowed or type(value.get('enabled')) is not bool or type(value.get('existing', False)) is not bool:
        raise ValueError('水印设置无效。')
    scale = value.get('scale', .4)
    if type(scale) not in (int, float) or not math.isfinite(scale) or not .18 <= scale <= .75:
        raise ValueError('水印大小须在18%至75%之间。')
    result = {**value, 'scale': scale, 'existing': value.get('existing', False)}
    for key, limit in {'title':100,'area':80,'time':32,'weather':100,'location':200}.items():
        text = value.get(key, '')
        if not isinstance(text, str) or len(text) > limit or any(ord(ch) < 32 for ch in text):
            raise ValueError('水印文字格式或长度无效。')
        result[key] = text.strip()
    logo = value.get('logo', '')
    if not isinstance(logo, str) or len(logo) > 2850:
        raise ValueError('Logo设置无效。')
    _logo_image(logo)
    if logo:
        result['logo'] = logo
    else:
        result.pop('logo', None)  # Keep pre-logo upload hashes stable across upgrades/retries.
    position = value.get('position', {'x': 0, 'y': 1})
    if (not isinstance(position, dict) or set(position) != {'x', 'y'}
            or any(type(v) not in (int, float) or not math.isfinite(v) or not 0 <= v <= 1 for v in position.values())):
        raise ValueError('水印位置无效。')
    if position != {'x': 0, 'y': 1}:
        result['position'] = {key: round(position[key], 4) for key in ('x', 'y')}
    else:
        result.pop('position', None)
    if result['enabled'] and not result['existing']:
        if not result['title'] or not result['area']:
            raise ValueError('请填写水印标题和施工区域。')
        try:
            date = dt.datetime.strptime(result['time'], '%Y-%m-%d %H:%M')
            if date.year < 2000:
                raise ValueError()
        except ValueError:
            raise ValueError('请选择照片的拍照时间。') from None
    return result


def photo_style(options):
    if not isinstance(options, dict):
        raise ValueError('水印样式无效。')
    checked = photo_options({'enabled': False, 'existing': False,
                             **{key: options[key] for key in ('scale', 'logo', 'position') if key in options}})
    return {'scale': checked['scale'], 'logo': checked.get('logo', ''),
            'position': checked.get('position', {'x': 0, 'y': 1})}


def time_audit(info, value, actor, current):
    original = info.get('original_time') or ''
    return {'original_time': original, 'origin': info.get('time_origin') or 'unknown',
            'source': info.get('time_source') or '未识别拍摄时间', 'display_time': value,
            'modified': bool(value and value != original), 'actor_id': actor['id'],
            'actor_name': actor.get('name', ''), 'confirmed_at': current.isoformat()}


def prepare_delivery_image(source, name):
    path = Path(source)
    size = path.stat().st_size
    if not 0 < size <= 20 * 1024 * 1024:
        raise ValueError('照片为空或超过20MiB，原文件未修改。')
    if size <= DELIVERY_IMAGE_BYTES:
        return {'name': name, 'mime': {'.png': 'image/png', '.webp': 'image/webp'}.get(path.suffix.lower(), 'image/jpeg'), 'content': None}
    with _RENDER_SLOTS, Image.open(path) as original:
        if original.width * original.height > 25_000_000:
            raise ValueError('照片像素过大，原文件未修改。')
        image = ImageOps.exif_transpose(original).convert('RGB')
        image.thumbnail((2560, 2560), Image.Resampling.LANCZOS)
        for quality in (88, 80, 72, 64, 48):
            buffer = io.BytesIO()
            image.save(buffer, format='JPEG', quality=quality, optimize=True)
            if buffer.tell() <= DELIVERY_IMAGE_BYTES:
                return {'name': Path(name).stem[:100] + '_发送副本.jpg', 'mime': 'image/jpeg', 'content': buffer.getvalue()}
    raise ValueError('照片发送副本未能压缩至10MiB以内，原文件已保留。')


def _photo_time(exif, current=None):
    try:
        nested = exif.get_ifd(34665) if hasattr(exif, 'get_ifd') else {}
    except (ValueError, TypeError, KeyError, OSError):
        nested = {}
    for key in (36867, 36868):
        raw = exif.get(key) or nested.get(key)
        try:
            if isinstance(raw, bytes):
                raw = raw.decode('ascii')
            value = dt.datetime.strptime(str(raw).strip(), '%Y:%m:%d %H:%M:%S')
            if 2000 <= value.year <= 2100 and (current is None or value.replace(tzinfo=TZ) <= current + dt.timedelta(minutes=5)):
                return value.strftime('%Y-%m-%d %H:%M')
        except (ValueError, TypeError, UnicodeError):
            continue
    return ''


def _editable_base(image, limit):
    image.thumbnail((limit, limit), Image.Resampling.LANCZOS)
    scale = min(max(1, 720 / image.width), limit / image.height)
    if scale > 1:
        image = image.resize((max(1, round(image.width * scale)), max(1, round(image.height * scale))), Image.Resampling.LANCZOS)
    return image


def inspect_photo(source, scope, current, weather=None, *, handover=True, capture=None):
    with _RENDER_SLOTS, Image.open(source) as original:
        details = watermark_details(scope, current, weather, original.getexif(), handover=handover, capture=capture)
        taken, time_source = _photo_time(original.getexif(), current), '照片拍摄信息'
        time_origin = 'exif' if taken else 'unknown'
        marker = original.info.get('comment', b'')
        existing = isinstance(marker, bytes) and marker.startswith(_MARKER)
        existing_locked = existing
        if existing:
            try:
                saved = json.loads(marker[len(_MARKER):])
                saved_time = saved.get('time') if isinstance(saved,dict) else ''
                if isinstance(saved_time,str) and re.fullmatch(r'20\d{2}[.-]\d{2}[.-]\d{2} \d{2}:\d{2}',saved_time):
                    taken = saved_time.replace('.', '-')
                    time_origin = 'watermark'
                time_source = '照片水印'
            except (ValueError, TypeError):
                pass
        image = _editable_base(ImageOps.exif_transpose(original).convert('RGB'), 1280)
    recognition = 'identified' if existing else 'unavailable'
    if not existing and _OCR_SLOT.acquire(False):
        try:
            from openclaw_service.assistant.lighthouse_files import recognize_text
            content = io.BytesIO()
            image.save(content, format='JPEG', quality=85)
            raw_text = unicodedata.normalize('NFKC', recognize_text(content.getvalue(), timeout=5))
            # Windows OCR separates Chinese characters/digits and emits full-width punctuation.
            text = re.sub(r'[^\S\r\n]+', '', raw_text)
            existing = bool(re.search(r'水印相机|今日水印|施工区域|拍[摄照]时间', text))
            match = re.search(r'拍\s*[摄照]\s*时\s*间\s*[:：]?\s*(20\d{2})[年./-](\d{1,2})[月./-](\d{1,2})日?\s+(\d{1,2})[:：](\d{2})', raw_text)
            # After removing OCR spaces, only fixed-width day/hour are unambiguous.
            match = match or re.search(r'拍[摄照]时间\s*[:：]?\s*(20\d{2})[年./-](\d{1,2})[月./-](\d{2})日?(\d{2})[:：](\d{2})', text)
            if match and not taken:
                try:
                    taken = dt.datetime(*map(int, match.groups())).strftime('%Y-%m-%d %H:%M')
                    time_source = '照片水印（请核对）'
                    time_origin = 'watermark'
                except ValueError:
                    pass
            recognition = 'identified' if existing else 'not_detected'
        except Exception:
            pass
        finally:
            _OCR_SLOT.release()
    if not taken and details['time'] != '未获取':
        taken = details['time'].replace('.', '-')
        time_source = '本次拍照'
        time_origin = 'camera'
    if not taken:
        time_source = '请选择拍照时间'
    preview = io.BytesIO()
    image.save(preview, format='JPEG', quality=85)
    return {'image': 'data:image/jpeg;base64,' + base64.b64encode(preview.getvalue()).decode('ascii'),
            'width':image.width,'height':image.height,'can_watermark':min(image.size)>=64,'existing_watermark':existing,'existing_locked':existing_locked,'recognition':recognition,
            'time_source':time_source,'time_origin':time_origin,'original_time':taken,
            'options':{'enabled':not existing and recognition == 'not_detected' and bool((capture or {}).get('captured_at')),
                'existing':existing,'scale':.4,**{key:details[key] for key in ('title','area','weather','location')},'time':taken}}


def _font(size, bold=False):
    root = Path(os.environ.get('WINDIR', 'C:/Windows')) / 'Fonts'
    for name in (('msyhbd.ttc', 'simhei.ttf', 'msyh.ttc') if bold else ('msyh.ttc', 'simhei.ttf')):
        if (root / name).is_file():
            return ImageFont.truetype(str(root / name), size)
    raise ValueError('系统缺少中文水印字体，请管理员安装微软雅黑或黑体。')


def photo_position(capture, current):
    position = capture.get('position') if isinstance(capture, dict) else None
    if isinstance(position, dict):
        values = [position.get(k) for k in ('latitude', 'longitude', 'accuracy', 'timestamp')]
        if (all(type(v) in (int, float) and math.isfinite(v) for v in values)
                and -90 <= values[0] <= 90 and -180 <= values[1] <= 180 and 0 <= values[2] <= 1000
                and -60 <= current.timestamp() - values[3] / 1000 <= 600):
            return position
    return None


def watermark_details(scope, current, weather=None, exif=None, *, handover=True, capture=None):
    if scope not in ('110', 'A', 'B', 'C', 'D', 'E', 'H'):
        raise ValueError('交接班照片楼栋无效。')
    current = current.astimezone(TZ)
    captured = None
    try:
        raw = (exif or {}).get(36867)
        if raw is None and hasattr(exif, 'get_ifd'):
            raw = exif.get_ifd(34665).get(36867)
        if isinstance(raw, bytes):
            raw = raw.decode('ascii')
        value = dt.datetime.strptime(raw, '%Y:%m:%d %H:%M:%S').replace(tzinfo=TZ)
        if 2000 <= value.year and value <= current + dt.timedelta(minutes=5):
            captured = value
    except (ValueError, TypeError, UnicodeError, KeyError, OSError):
        pass
    capture = capture if isinstance(capture, dict) else {}
    if captured is None:
        stamp = capture.get('captured_at')
        if type(stamp) in (int, float) and math.isfinite(stamp) and -60 <= current.timestamp() - stamp / 1000 <= 1800:
            captured = dt.datetime.fromtimestamp(stamp / 1000, TZ)
    building = '110站' if scope == '110' else scope + '楼'
    location, description = '', ''
    local_weather = True
    weather = weather if isinstance(weather, dict) else {}
    position = photo_position(capture, current)
    if position:
        lat, lon = position['latitude'], position['longitude']
        location = capture.get('_location_name', '')
        if not isinstance(location, str) or len(location) > 200:
            location = ''
        # The existing live weather feed covers Nantong only, never another location.
        local_weather = abs(lat - 32.0167) <= .15 and abs(lon - 120.8667) <= .15
        if local_weather:
            live = weather.get('weather') if isinstance(weather.get('weather'), dict) else {}
            try:
                observed = dt.datetime.fromisoformat(live.get('observed_at') or '')
                temperature, condition = live.get('temperature'), live.get('condition')
                if (weather.get('source_state') == 'fresh' and observed.tzinfo is not None
                        and -60 <= current.timestamp() - observed.timestamp() <= 1800
                        and isinstance(condition,str) and 0 < len(condition.strip()) <= 40
                        and type(temperature) in (int,float) and math.isfinite(temperature) and -70 <= temperature <= 65):
                    description = f'{condition.strip()} {temperature:g}℃'
            except (TypeError,ValueError):
                pass
    forecast = weather.get('forecast') if isinstance(weather.get('forecast'), dict) else {}
    fetched = forecast.get('fetched_at')
    if (not description and local_weather and forecast.get('available') is True
            and forecast.get('city') in ('南通','南通市') and forecast.get('date') == current.date().isoformat()
            and type(fetched) in (int,float) and math.isfinite(fetched) and -60 <= current.timestamp()-fetched <= 1800
            and isinstance(forecast.get('description'),str) and 0 < len(forecast['description'].strip()) <= 40):
        description = '南通预报 ' + forecast['description'].strip()
        low, high = forecast.get('minimum'), forecast.get('maximum')
        if all(type(v) in (int,float) and math.isfinite(v) and -70 <= v <= 65 for v in (low,high)) and low <= high:
            description += f' {low:g}～{high:g}℃'
    title = f'EA118南通机房{building}' + ('交接班' if handover else '班前检查')
    return {'title': title, 'scope': scope, 'area': building,
            'time_label': '拍照时间',
            'time': captured.strftime('%Y.%m.%d %H:%M') if captured else '未获取',
            'weather': description, 'location': location}


def render_handover(source, destination, scope, current, weather=None, *, handover=True, capture=None):
    with _RENDER_SLOTS, Image.open(source) as original:
        if original.width * original.height > 25_000_000:
            raise ValueError('图片像素过大，无法添加水印。')
        details = watermark_details(scope, current, weather, original.getexif(), handover=handover, capture=capture)
        options = photo_options((capture or {}).get('watermark'))
        if options:
            details.update({key:options[key] for key in ('title','area','time','weather','location')})
            audit = (capture or {}).get('_time_audit')
            if isinstance(audit, dict):
                details['audit'] = audit
                if audit.get('modified'):
                    details['time_label'] = '拍照时间（手动修改）' if audit.get('original_time') else '拍照时间（手动填写）'
        logo = _logo_image(options.get('logo', '')) if options else None
        if logo is not None:
            details['logo'] = 'vnet' if options['logo'] == 'vnet' else 'custom'
        details['applied'] = True
        lines = ['施工区域：' + details['area'], details['time_label'] + '：' + details['time']]
        if details['weather']:
            lines.append('天　　气：' + details['weather'])
        if details['location']:
            lines.append('地　　点：' + details['location'])
        image = ImageOps.exif_transpose(original).convert('RGB')
        if options:
            image = _editable_base(image, 2560)
            if min(image.size) < 64:
                raise ValueError('照片过窄，无法容纳可读水印，请保留原图。')
        else:
            image.thumbnail((2560, 2560), Image.Resampling.LANCZOS)
        # Small uploaded thumbnails still need a readable stamp, without cropping.
        if image.width < 720 and not options:
            scale = min(720 / image.width, 2560 / image.height)
            image = image.resize((max(1, round(image.width * scale)), max(1, round(image.height * scale))), Image.Resampling.LANCZOS)
            if image.width < 720:
                image = ImageOps.pad(image, (720, image.height), color=(245, 246, 248))
        width, height = image.size
        margin = max(8, round(min(width, height) * .02))
        panel_width = (min(width - margin * 2, round(width * options['scale'])) if options
                       else min(width - margin * 2, max(round(width * .40), min(630, width - margin * 2))))
        if options:
            panel_width = min(panel_width, (height - margin * 2) * 18 / (1.95 + 1.5 * len(lines) + .35))
        size = max(1 if options else 9, round(panel_width / 18))
        line_height = round(size * 1.5)
        header_height = round(size * 1.95)
        panel_height = header_height + line_height * len(lines) + round(size * .35)
        if panel_height + margin * 2 > height:
            image = ImageOps.pad(image, (width, panel_height + margin * 2), color=(245, 246, 248))
            height = image.height
        position = options.get('position', {'x': 0, 'y': 1}) if options else {'x': 0, 'y': 1}
        left = margin + round(max(0, width - margin * 2 - panel_width) * position['x'])
        top = margin + round(max(0, height - margin * 2 - panel_height) * position['y'])
        overlay = Image.new('RGBA', image.size)
        draw = ImageDraw.Draw(overlay)
        radius = max(5, round(size * .45))
        draw.rounded_rectangle((left, top, left + panel_width, top + panel_height), radius, fill=(255, 255, 255, 187))
        draw.rounded_rectangle((left, top, left + panel_width, top + header_height), radius, fill=(52, 120, 224, 235))
        draw.rectangle((left, top + header_height // 2, left + panel_width, top + header_height), fill=(52, 120, 224, 235))
        title_x = left + size * 1.7
        if logo is not None:
            box_height = max(4, round(header_height * .72))
            box_width = max(4, min(round(box_height * logo.width / logo.height), round(panel_width * .22)))
            box_left, box_top = left + round(size * .4), top + (header_height - box_height) // 2
            padding = max(1, round(size * .1))
            draw.rounded_rectangle((box_left, box_top, box_left + box_width, box_top + box_height),
                                   max(2, round(size * .15)), fill=(255, 255, 255, 255))
            logo.thumbnail((max(1, box_width - padding * 2), max(1, box_height - padding * 2)), Image.Resampling.LANCZOS)
            overlay.alpha_composite(logo, (box_left + (box_width - logo.width) // 2, box_top + (box_height - logo.height) // 2))
            title_x = box_left + box_width + size * .4
        else:
            x, y = left + size // 2, top + header_height // 2
            draw.ellipse((x, y - size // 5, x + size * .45, y + size // 5), fill=(255, 198, 59))

        def text_line(value, xy, available, bold=False, fill=(12, 17, 22)):
            font_size = size
            font = _font(font_size, bold)
            while font_size > 8 and draw.textlength(value, font=font) > available:
                font_size -= 1
                font = _font(font_size, bold)
            length = draw.textlength(value, font=font)
            if length > available:
                label = Image.new('RGBA', (math.ceil(length) + 2, line_height))
                ImageDraw.Draw(label).text((0, line_height / 2), value, font=font, fill=fill, anchor='lm')
                overlay.alpha_composite(label.resize((max(1, int(available)), line_height), Image.Resampling.LANCZOS),
                                        (int(xy[0]), round(xy[1] - line_height / 2)))
            else:
                draw.text(xy, value, font=font, fill=fill, anchor='lm')

        text_line(details['title'], (title_x, top + header_height / 2), left + panel_width - title_x - size * .3, True, (255, 255, 255))
        for index, line in enumerate(lines):
            text_line(line, (left + size * .5, top + header_height + line_height * (index + .5)), panel_width - size)
        result = Image.alpha_composite(image.convert('RGBA'), overlay).convert('RGB')
        public_details = {**details}
        if 'audit' in public_details:
            public_details['audit'] = {k: v for k, v in details['audit'].items() if k not in ('actor_id', 'actor_name')}
        result.save(destination, format='JPEG', quality=88, optimize=True,
                    comment=_MARKER + json.dumps(public_details,ensure_ascii=False).encode('utf-8'))
        if Path(destination).stat().st_size > 10 * 1024 * 1024:
            raise ValueError('水印照片超过飞书图片大小限制，请选择较小图片。')
        return details
