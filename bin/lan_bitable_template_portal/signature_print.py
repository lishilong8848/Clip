"""Render stored signatures with solid ink at their final print size."""

from __future__ import annotations

import io
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from PIL import Image


def print_signature_image(
    source: Image.Image,
    size: tuple[int, int],
) -> Image.Image:
    """Keep the handwriting black after shrinking while retaining its shape."""

    from PIL import Image, ImageChops, ImageOps

    width, height = (max(1, int(value)) for value in size)
    image = ImageOps.exif_transpose(source).convert("RGBA")
    if image.size != (width, height):
        image = image.resize((width, height), Image.Resampling.LANCZOS)
    darkness = ImageOps.invert(image.convert("RGB").convert("L"))
    coverage = ImageChops.multiply(darkness, image.getchannel("A"))
    ink = coverage.point(lambda value: 255 if value >= 16 else 0)
    if not ink.getbbox():
        raise ValueError("签名图片缩小后没有可见笔迹。")
    result = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    result.putalpha(ink)
    return result


def print_signature_png(content: bytes, size: tuple[int, int]) -> bytes:
    from PIL import Image

    with Image.open(io.BytesIO(content)) as source:
        image = print_signature_image(source, size)
    output = io.BytesIO()
    image.save(output, format="PNG", optimize=True)
    return output.getvalue()
