"""Extract cabinet confirmation rows from screenshots without changing ledgers."""
import asyncio
import datetime as dt
import io
import multiprocessing
import os
import re
import tempfile
from pathlib import Path
from PIL import Image


DATE = re.compile(r"(20\d{2})[./-](\d{1,2})[./-](\d{1,2})(\d{2})[:.](\d{1,2})(?:[:.](\d{1,2}))?(?!\d)")
RACK = re.compile(r"(?<![A-Z0-9])([A-Z]\d{2})(?!\d)")
ROOM = re.compile(r"([A-E])([1-4])[.\-_](\d{1,2})")
ACTIONS = ("上正式电", "上测试电", "测试电转正式电", "正式电转测试电", "下正式电", "下测试电")


def ensure_thumbnail(source, destination, size=(320, 240)):
    source, destination = Path(source), Path(destination)
    if destination.is_file() and destination.stat().st_mtime_ns >= source.stat().st_mtime_ns:
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    with Image.open(source) as image:
        image.thumbnail(size, Image.Resampling.LANCZOS)
        rendered = image.convert("RGBA") if image.mode in ("RGBA", "LA") else image.convert("RGB")
        fd, temporary = tempfile.mkstemp(prefix=".thumb-", suffix=".png", dir=destination.parent)
        os.close(fd)
        try:
            rendered.save(temporary, "PNG", optimize=True)
            os.replace(temporary, destination)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    return destination


def _compact(value):
    return str(value or "").upper().translate(str.maketrans({
        " ": "", "\u3000": "", "．": ".", "。": ".", "·": ".", "：": ":", "℃": ":",
        "－": "-", "—": "-", "一": "-",
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


def _date_words(items):
    lines=[]
    numeric=[item for item in items if re.search(r"\d",_compact(item[0]).translate(str.maketrans({"O":"0","I":"1","L":"1"})))]
    for item in sorted(numeric,key=lambda value:(value[2],value[1])):
        if not lines or item[2]-sum(value[2] for value in lines[-1])/len(lines[-1])>6: lines.append([item])
        else: lines[-1].append(item)
    line_parts=[]
    for line in lines:
        parts=[]
        for text,_x,_y in sorted(line,key=lambda value:value[1]):
            cleaned=_compact(text).translate(str.maketrans({"O":"0","I":"1","L":"1"}))
            parts.extend(re.findall(r"\d+",cleaned))
        line_parts.append(parts)
    if len(line_parts)>=2 and len(line_parts[0])==5 and len(line_parts[0][4])==1 and len(line_parts[1])>=2 and len(line_parts[1][0])==1:
        joined=[*line_parts[0][:4],line_parts[0][4]+line_parts[1][0],line_parts[1][1]]
        try: return dt.datetime(*(int(value) for value in joined)).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError: pass
    parts=[part for line in line_parts for part in line]
    if len(parts)<5 or len(parts[0])!=4:
        return ""
    values=[int(value) for value in parts[:6]]
    if len(values)==5:
        values.append(0)
    try:
        return dt.datetime(*values).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return ""


def _rack(value):
    cleaned=_compact(value)
    match=re.search(r"(?<![A-Z0-9])([A-Z])([0-9OIL]{2})(?![A-Z0-9])",cleaned)
    if not match:
        return ""
    return match[1]+match[2].translate(str.maketrans({"O":"0","I":"1","L":"1"}))


def _room(value):
    cleaned=_compact(value).translate(str.maketrans({"O":"0","I":"1","L":"1"}))
    return ROOM.search(cleaned)


def _action(value):
    cleaned=_compact(value).replace("丬则","测").replace("则试","测试")
    exact=next((item for item in ACTIONS if item in cleaned),"")
    if exact:
        return exact
    if "正式电转" in cleaned and "试" in cleaned.split("正式电转",1)[1]:
        return "正式电转测试电"
    if "测试电转" in cleaned and "正式" in cleaned.split("测试电转",1)[1]:
        return "测试电转正式电"
    return ""


def _rows_from_ocr(lines,image_width):
    headers={}; room_headers=[]; room_header_ys=[]
    for text,words in lines:
        header_text=text.replace("包问","包间")
        markers=(("room","包间"),("action","操作类"),("expected","期望完成"),("actual","实际完成"),("result","结果"))
        for key,marker in markers:
            if marker not in header_text:
                continue
            x=min(word[1] for word in words)
            if key=="room":
                room_headers.append(x); room_header_ys.append(min(word[2] for word in words))
            elif key not in headers or x<headers[key][0]:
                headers[key]=(x,min(word[2] for word in words))
        if "运营商机柜" in text or "营商机柜编" in text or "机柜编号" in text:
            headers["supplier"]=(min(word[1] for word in words),min(word[2] for word in words))
    if room_headers:
        headers["room"]=(min(room_headers),min(room_header_ys))
    if not all(key in headers for key in ("room","action","expected","actual")):
        return []
    room_x=headers["room"][0]; action_x=headers["action"][0]
    expected_x=headers["expected"][0]; actual_x=headers["actual"][0]
    supplier_x=headers.get("supplier",(image_width,0))[0]; result_x=headers.get("result",(image_width,0))[0]
    header_y=max(headers[key][1] for key in ("room","action","expected","actual"))
    words=sorted((text,x,y) for _line,items in lines for text,x,y in items if y>header_y+35)
    rack_left=(max(room_headers)+action_x)/2 if len(room_headers)>1 else room_x+80
    rack_items=sorted(((text,x,y) for text,x,y in words if rack_left<=x<action_x-10),key=lambda item:(item[2],item[1]))
    clusters=[]
    for item in rack_items:
        if not clusters or item[2]-max(member[2] for member in clusters[-1])>35:
            clusters.append([item])
        else:
            clusters[-1].append(item)
    anchors=[]
    for cluster in clusters:
        rack=_rack("".join(item[0] for item in sorted(cluster,key=lambda item:(item[2],item[1]))))
        if rack:
            anchors.append((rack,sum(item[2] for item in cluster)/len(cluster)))
    rows=[]
    for index,(rack,rack_y) in enumerate(anchors):
        top=(anchors[index-1][1]+rack_y)/2 if index else rack_y-46
        bottom=(rack_y+anchors[index+1][1])/2 if index+1<len(anchors) else rack_y+46
        nearby=[item for item in words if top<=item[2]<bottom]
        def column_items(left,right):
            return [item for item in nearby if left<=item[1]<right]
        def ordered(items):
            lines=[]
            for item in sorted(items,key=lambda value:(value[2],value[1])):
                if not lines or item[2]-sum(value[2] for value in lines[-1])/len(lines[-1])>6:
                    lines.append([item])
                else:
                    lines[-1].append(item)
            return [item for line in lines for item in sorted(line,key=lambda value:value[1])]
        def column(left,right):
            return "".join(item[0] for item in ordered(column_items(left,right)))
        room_text=_compact(column(room_x-10,rack_left-5)); room_match=_room(room_text)
        room_value=None
        package_right=max(room_headers)-8 if len(room_headers)>1 else rack_left-5
        package=column_items(room_x-10,package_right)
        for text,x,y in package:
            prefix=re.search(r"([A-E])([1-4])",_compact(text).translate(str.maketrans({"O":"0","I":"1","L":"1"})))
            if not prefix:
                continue
            suffix=next((re.sub(r"\D","",_compact(value)) for value,xx,yy in sorted(package,key=lambda item:item[1])
                         if xx>x and abs(yy-y)<=10 and re.fullmatch(r"\d{1,2}",re.sub(r"\D","",_compact(value)))),"")
            if suffix:
                room_value=(prefix[1],f"{prefix[2]}{int(suffix):02d}")
                break
        if room_value is None and room_match:
            room_value=(room_match[1],f"{room_match[2]}{int(room_match[3]):02d}")
        if room_value is None:
            continue
        action_text=_compact(column(action_x-15,expected_x-15)); action=_action(action_text)
        expected_items=ordered(column_items(expected_x-15,actual_x-15))
        actual_items=ordered(column_items(actual_x-15,min(supplier_x,result_x)-15))
        expected=_date_words(expected_items) or _date("".join(item[0] for item in expected_items))
        actual=_date_words(actual_items) or _date("".join(item[0] for item in actual_items))
        supplier_right=min(result_x-10,supplier_x+100)
        supplier=_rack(column(supplier_x-10,supplier_right)) if supplier_x<image_width else ""
        result_start=result_x-10 if result_x<image_width else supplier_x+70 if supplier_x<image_width else actual_x+120
        result_text=_compact(column(result_start,image_width))
        result="失败" if "失败" in result_text or "FAIL" in result_text else "成功" if "成功" in result_text or "SUCCES" in result_text else ""
        candidate={"scope":room_value[0],"room":room_value[1],"rack":rack,
                   "supplier_rack":supplier,"action":action,"expected":expected,"actual":actual,"result":result,
                   "raw":{"room":room_text,"action":action_text,"result":result_text}}
        if not any((previous["scope"],previous["room"],previous["rack"],previous["actual"])==
                   (candidate["scope"],candidate["room"],candidate["rack"],candidate["actual"]) for previous in rows):
            rows.append(candidate)
    return rows


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
    return _rows_from_ocr(lines,image.width)


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
