"""Render the handover stamp locally; never invent GPS or live temperature."""
import datetime as dt
import math
import os
import threading
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont, ImageOps

TZ = dt.timezone(dt.timedelta(hours=8))
_RENDER_SLOTS = threading.BoundedSemaphore(2)


def _font(size, bold=False):
    root = Path(os.environ.get('WINDIR', 'C:/Windows')) / 'Fonts'
    for name in (('msyhbd.ttc', 'simhei.ttf', 'msyh.ttc') if bold else ('msyh.ttc', 'simhei.ttf')):
        if (root / name).is_file():
            return ImageFont.truetype(str(root / name), size)
    raise ValueError('系统缺少中文水印字体，请管理员安装微软雅黑或黑体。')


def watermark_details(scope, current, weather=None, exif=None):
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
    except (ValueError, TypeError, UnicodeError):
        pass
    building = '110站' if scope == '110' else scope + '楼'
    description = '未获取'
    weather = weather or {}
    fetched = weather.get('fetched_at')
    if (weather.get('available') is True and weather.get('city') in ('南通', '南通市')
            and weather.get('date') == current.date().isoformat()
            and isinstance(fetched, (int, float)) and -300 <= current.timestamp() - fetched <= 1800
            and isinstance(weather.get('description'), str) and 0 < len(weather['description']) <= 40):
        description = '今日预报 ' + weather['description'].strip()
        low, high = weather.get('minimum'), weather.get('maximum')
        if all(type(v) in (int, float) and math.isfinite(v) and -70 <= v <= 65 for v in (low, high)) and low <= high:
            description += f' {low:g}～{high:g}℃'
    return {'title': f'EA118南通机房{building}交接班', 'scope': scope, 'area': building,
            'time_label': '拍摄时间' if captured else '上传时间',
            'time': (captured or current).strftime('%Y.%m.%d %H:%M'),
            'weather': description, 'location': f'南通机房 · {building}（任务地点）'}


def render_handover(source, destination, scope, current, weather=None):
    with _RENDER_SLOTS, Image.open(source) as original:
        if original.width * original.height > 25_000_000:
            raise ValueError('图片像素过大，无法添加水印。')
        details = watermark_details(scope, current, weather, original.getexif())
        image = ImageOps.exif_transpose(original).convert('RGB')
        image.thumbnail((2560, 2560), Image.Resampling.LANCZOS)
        # Small uploaded thumbnails still need a readable stamp, without cropping.
        if image.width < 720:
            scale = min(720 / image.width, 2560 / image.height)
            image = image.resize((max(1, round(image.width * scale)), max(1, round(image.height * scale))), Image.Resampling.LANCZOS)
            if image.width < 720:
                image = ImageOps.pad(image, (720, image.height), color=(245, 246, 248))
        width, height = image.size
        margin = max(8, round(min(width, height) * .02))
        panel_width = min(width - margin * 2, max(round(width * .40), min(630, width - margin * 2)))
        size = max(9, round(panel_width / 18))
        line_height = round(size * 1.5)
        header_height = round(size * 1.95)
        panel_height = header_height + line_height * 4 + round(size * .35)
        if panel_height + margin * 2 > height:
            image = ImageOps.pad(image, (width, panel_height + margin * 2), color=(245, 246, 248))
            height = image.height
        left, top = margin, height - margin - panel_height
        overlay = Image.new('RGBA', image.size)
        draw = ImageDraw.Draw(overlay)
        radius = max(5, round(size * .45))
        draw.rounded_rectangle((left, top, left + panel_width, top + panel_height), radius, fill=(255, 255, 255, 187))
        draw.rounded_rectangle((left, top, left + panel_width, top + header_height), radius, fill=(52, 120, 224, 235))
        draw.rectangle((left, top + header_height // 2, left + panel_width, top + header_height), fill=(52, 120, 224, 235))
        x, y = left + size // 2, top + header_height // 2
        draw.ellipse((x, y - size // 5, x + size * .45, y + size // 5), fill=(255, 198, 59))

        def text_line(value, xy, available, bold=False, fill=(12, 17, 22)):
            font_size = size
            font = _font(font_size, bold)
            while font_size > 8 and draw.textlength(value, font=font) > available:
                font_size -= 1
                font = _font(font_size, bold)
            draw.text(xy, value, font=font, fill=fill, anchor='lm')

        text_line(details['title'], (left + size * 1.7, top + header_height / 2), panel_width - size * 2, True, (255, 255, 255))
        lines = ['施工区域：' + details['area'], details['time_label'] + '：' + details['time'],
                 '天　　气：' + details['weather'], '地　　点：' + details['location']]
        for index, line in enumerate(lines):
            text_line(line, (left + size * .5, top + header_height + line_height * (index + .5)), panel_width - size)
        result = Image.alpha_composite(image.convert('RGBA'), overlay).convert('RGB')
        result.save(destination, format='JPEG', quality=88, optimize=True)
        if Path(destination).stat().st_size > 10 * 1024 * 1024:
            raise ValueError('水印照片超过飞书图片大小限制，请选择较小图片。')
        return details
