import io
import unittest

from PIL import Image, ImageDraw

from bin.lan_bitable_template_portal.signature_print import (
    print_signature_image,
    print_signature_png,
)


class SignaturePrintTests(unittest.TestCase):
    def test_shrunk_signature_has_solid_black_ink_and_clear_background(self):
        source = Image.new("RGBA", (600, 240), (0, 0, 0, 0))
        ImageDraw.Draw(source).line((25, 190, 300, 25, 570, 190), fill=(0, 0, 0, 180), width=10)
        original = source.tobytes()

        result = print_signature_image(source, (120, 48))

        self.assertEqual(source.tobytes(), original)
        self.assertEqual(result.size, (120, 48))
        self.assertEqual(set(result.getchannel("A").getdata()), {0, 255})
        self.assertEqual(result.getpixel((0, 0))[3], 0)
        self.assertGreater(result.getchannel("A").histogram()[255], 100)

    def test_white_background_and_gray_source_print_as_black_handwriting(self):
        source = Image.new("RGBA", (200, 80), "white")
        ImageDraw.Draw(source).line((10, 65, 180, 10), fill=(125, 125, 125, 255), width=6)
        buffer = io.BytesIO()
        source.save(buffer, format="PNG")

        with Image.open(io.BytesIO(print_signature_png(buffer.getvalue(), (100, 40)))) as result:
            self.assertEqual(result.getpixel((0, 0))[3], 0)
            self.assertGreater(result.getchannel("A").histogram()[255], 50)
            self.assertEqual(result.getpixel((50, 20))[:3], (0, 0, 0))

    def test_blank_signature_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "可见笔迹"):
            print_signature_image(Image.new("RGBA", (50, 20), (0, 0, 0, 0)), (25, 10))


if __name__ == "__main__":
    unittest.main()
