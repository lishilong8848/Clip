"""Extract cabinet confirmation rows from screenshots without changing ledgers."""
import asyncio
import datetime as dt
import io
import multiprocessing
import os
import re
from PIL import Image


DATE = re.compile(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})(\d{2}):(\d{2})(?::?(\d{2}))?(?!\d)")
RACK = re.compile(r"(?<![A-Z0-9])([A-Z]\d{2})(?!\d)")
ROOM = re.compile(r"([A-E])([1-4])[.\-_](\d{1,2})")
ACTIONS = ("上正式电", "上测试电", "测试电转正式电", "正式电转测试电", "下正式电", "下测试电")


def _compact(value):
    return str(value or "").upper().translate(str.maketrans({
        " ": "", "\u3000": "", "．": ".", "。": ".", "·": ".", "：": ":", "－": "-", "—": "-",
    }))


def _date(value):
    cleaned = re.sub(r"(?<!\d)2[CO](\d{2})(?=[./-])", r"20\1", _compact(value))
    match = DATE.search(cleaned)
    if not match:
        return ""
    try:
        parts = [int(part or 0) for part in match.groups()]
        return dt.datetime(*parts).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return ""


def recognize_image(content):
    from winocr import recognize_pil

    with Image.open(io.BytesIO(content)) as source:
        source.verify()
    with Image.open(io.BytesIO(content)) as source:
        if source.width * source.height > 40_000_000:
            raise ValueError("图片像素过大")
        image = source.convert("RGB")
    loop = asyncio.new_event_loop()
    try:
        result = loop.run_until_complete(recognize_pil(image, "zh-Hans-CN"))
    finally:
        loop.close()
    lines = []
    for line in result.lines:
        words = [
            (str(word.text or ""), float(word.bounding_rect.x), float(word.bounding_rect.y))
            for word in line.words if str(word.text or "").strip()
        ]
        if words:
            lines.append((_compact(line.text), words))
    headers = {}
    for text, words in lines:
        for key, marker in (("room", "包间"), ("action", "操作类"),
                            ("expected", "期望完成"), ("actual", "实际完成"),
                            ("supplier", "运营商机柜"), ("result", "结果")):
            if marker in text:
                x = min((w[1] for w in words if "包" in w[0]), default=words[0][1]) if key == "room" else min(w[1] for w in words)
                if key != "room" or key not in headers or x < headers[key][0]:
                    headers[key] = (x, min(w[2] for w in words))
    if not all(key in headers for key in ("room", "action", "expected", "actual")):
        return []
    room_x = headers["room"][0]
    action_x = headers["action"][0]
    expected_x = headers["expected"][0]
    actual_x = headers["actual"][0]
    supplier_x = headers.get("supplier", (image.width, 0))[0]
    result_x = headers.get("result", (image.width, 0))[0]
    header_y = max(headers[key][1] for key in ("room", "action", "expected", "actual"))
    words = sorted((text, x, y) for _line, items in lines for text, x, y in items if y > header_y + 45)
    rack_words = [(text, x, y) for text, x, y in words
                  if room_x + 80 < x < action_x and RACK.search(_compact(text))]
    rows = []
    rack_words.sort(key=lambda item: item[2])
    for index, (rack_text, _x, rack_y) in enumerate(rack_words):
        top = (rack_words[index - 1][2] + rack_y) / 2 if index else rack_y - 46
        bottom = (rack_y + rack_words[index + 1][2]) / 2 if index + 1 < len(rack_words) else rack_y + 46
        nearby = sorted((text, x, y) for text, x, y in words if top <= y < bottom and abs(y - rack_y) <= 46)

        def column(left, right):
            return "".join(text for text, x, y in sorted(nearby, key=lambda item: (item[2] >= rack_y + 5, item[1]))
                           if left <= x < right)

        room_text = _compact(column(room_x - 10, room_x + 115))
        room = ROOM.search(room_text)
        rack = RACK.search(_compact(rack_text))
        if not room or not rack:
            continue
        action_text = _compact(column(action_x - 15, expected_x - 15))
        action = next((item for item in ACTIONS if item in action_text), "")
        expected = _date(column(expected_x - 15, actual_x - 15))
        actual = _date(column(actual_x - 15, min(supplier_x, result_x) - 15))
        supplier = RACK.search(_compact(column(supplier_x - 10, result_x - 10)))
        result_text = _compact(column(result_x - 10, image.width)) if result_x < image.width else ""
        result = "失败" if "失败" in result_text else "成功" if "成功" in result_text else ""
        candidate = {
            "scope": room[1], "room": f"{room[2]}{int(room[3]):02d}",
            "rack": rack[1], "supplier_rack": supplier[1] if supplier else "",
            "action": action, "expected": expected, "actual": actual, "result": result,
            "raw": {"room": room_text, "action": action_text, "result": result_text},
        }
        if not any(previous["room"] == candidate["room"] and previous["rack"] == candidate["rack"]
                   for previous in rows):
            rows.append(candidate)
    return rows


def _recognize_worker(content, sender):
    try:
        if os.name == "nt":
            try:
                import ctypes
                kernel = ctypes.WinDLL("kernel32", use_last_error=True)
                kernel.SetPriorityClass(kernel.GetCurrentProcess(), 0x00004000)
            except Exception:
                pass
        sender.send((True, recognize_image(content)))
    except BaseException as exc:
        sender.send((False, str(exc)))
    finally:
        sender.close()


def recognize_image_with_timeout(content, timeout=45):
    """Run WinOCR out of process so one bad image cannot block later work."""
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_recognize_worker, args=(content, sender), daemon=True)
    try:
        process.start()
    except Exception:
        receiver.close()
        sender.close()
        raise
    sender.close()
    try:
        if not receiver.poll(max(1, float(timeout))):
            raise TimeoutError(f"图片识别超过 {int(timeout)} 秒")
        succeeded, payload = receiver.recv()
        if not succeeded:
            raise RuntimeError(payload or "图片识别失败")
        return payload
    finally:
        receiver.close()
        if process.is_alive():
            process.terminate()
        process.join(timeout=2)
        if process.is_alive():
            process.kill()
            process.join(timeout=1)
