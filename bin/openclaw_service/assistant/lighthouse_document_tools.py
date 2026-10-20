"""Local, bounded document/OCR/chart tools over authorized assistant attachments."""
import hashlib
import io
import math
import multiprocessing
import os
import socket
import threading
from contextlib import closing
from pathlib import Path

from .lighthouse_ai import AssistantError
from .lighthouse_artifacts import check_content

_SLOT = threading.BoundedSemaphore(1)
_CACHE = 'lighthouse_document_tools'
DOCUMENT_SUFFIXES = {'.docx', '.xlsx', '.xlsm', '.pptx', '.pdf'}


def _offline(*args, **kwargs):
    raise OSError('Local document tools cannot access the network')


def checked_file(item):
    path = Path(item['path'])
    if path.stat().st_size > 20 * 1024 * 1024:
        raise AssistantError('单文件最多20MiB。', 413)
    content = path.read_bytes()
    if hashlib.sha256(content).hexdigest() != item['sha256']:
        raise AssistantError('附件已变化，请重新上传。', 409)
    return content


def _worker(kind, payload, sender):
    try:
        for name in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS', 'NUMEXPR_NUM_THREADS'):
            os.environ[name] = '1'
        if os.name == 'nt':
            import ctypes
            ctypes.windll.kernel32.SetPriorityClass(ctypes.windll.kernel32.GetCurrentProcess(), 0x4000)
        socket.getaddrinfo = _offline
        for method in ('connect', 'connect_ex'):
            original = getattr(socket.socket, method)
            def connect(sock, address, original=original):
                # Windows asyncio requires a loopback socket pair for its own event loop.
                if kind == 'flowchart' and isinstance(address, tuple) and address[0] in {'127.0.0.1', '::1'}:
                    return original(sock, address)
                return _offline()
            setattr(socket.socket, method, connect)
        if kind == 'chart':
            result = chart_png(**payload)
        elif kind == 'spreadsheet':
            from .lighthouse_office_tools import analyze_tables
            result = analyze_tables(**payload)
        elif kind == 'pdf':
            from .lighthouse_office_tools import organize_pdf
            result = organize_pdf(**payload)
        elif kind == 'flowchart':
            from .lighthouse_diagrams import render_flowchart
            result = render_flowchart(**payload)
        elif kind in {'markdown', 'ocr'}:
            path = Path(payload['path'])
            content = checked_file(payload)
            result = ocr_text(content, path.suffix.lower(), payload['page']) if kind == 'ocr' else document_markdown(content, path.suffix.lower())
        else:
            raise AssistantError('不支持此文档工具。')
        sender.send((True, result))
    except Exception as exc:
        sender.send((False, (str(exc), exc.status) if isinstance(exc, AssistantError)
                     else ('本地文档处理未完成，请检查文件或更新运行依赖；原文件未改动。', 422)))
    finally:
        sender.close()


def run_local(kind, payload, timeout=45):
    # One low-priority child bounds CPU use; completion/timeout releases all model memory.
    if not _SLOT.acquire(timeout=5):
        raise AssistantError('本地文档工具正忙，请稍后重试；普通聊天和业务办理不受影响。', 429)
    context = multiprocessing.get_context('spawn')
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(target=_worker, args=(kind, payload, sender), daemon=True)
    try:
        process.start()
        if os.name == 'nt':
            from upload_event_module.services.process_lifetime import register_child_process
            if not register_child_process(process.pid) and process.is_alive():
                raise AssistantError('文档任务生命周期绑定失败，未继续处理。', 503)
        sender.close()
        if not receiver.poll(timeout):
            raise AssistantError('本地处理超时，原文件保留；请缩小范围后重试。', 408)
        try:
            ok, result = receiver.recv()
        except EOFError:
            raise AssistantError('文档任务已中断，原文件保留。', 503) from None
        if not ok:
            raise AssistantError(*result)
        return result
    finally:
        if process.is_alive():
            process.terminate()
        if process.pid:
            process.join(timeout=2)
            if process.is_alive():
                process.kill()
                process.join(timeout=2)
            if not process.is_alive():
                process.close()
        receiver.close()
        sender.close()
        _SLOT.release()


def document_markdown(content, suffix):
    from .lighthouse_files import extract_document
    if suffix not in DOCUMENT_SUFFIXES:
        raise AssistantError('增强阅读支持Word、Excel、PPT及PDF；其他文本使用附件阅读。')
    return {**extract_document(content, suffix, markdown=True), 'method': 'Local document extraction'}


def ocr_text(content, suffix, page=1):
    from PIL import Image
    from .lighthouse_files import IMAGE_EXTENSIONS, safe_document
    if type(page) is not int or not 1 <= page <= 100:
        raise AssistantError('请选择1至100的页码。')
    total = 1
    if suffix == '.pdf':
        import pypdfium2 as pdfium
        with pdfium.PdfDocument(content) as pdf:
            total = len(pdf)
            if total > 100 or page > total:
                raise AssistantError('PDF最多100页，页码不能超出文档范围。')
            with closing(pdf[page - 1]) as item:
                width, height = item.get_size()
                scale = min(2, 2200 / max(width, height))
                with closing(item.render(scale=scale)) as bitmap:
                    image = bitmap.to_pil().convert('RGB')
    elif suffix in IMAGE_EXTENSIONS and page == 1:
        with Image.open(io.BytesIO(content)) as original:
            if original.width * original.height > 40000000:
                raise AssistantError('图片像素过大，请缩小后上传。', 413)
            image = original.convert('RGB')
        image.thumbnail((2200, 2200))
    else:
        raise AssistantError('OCR支持图片或PDF，图片页码须为1。')
    import cv2
    import numpy as np
    import rapidocr
    from rapidocr import RapidOCR
    cv2.setNumThreads(1)
    model_dir = Path(rapidocr.__file__).parent / 'models'
    models = {'Det': 'PP-OCRv6_det_small.onnx', 'Rec': 'PP-OCRv6_rec_small.onnx',
              'Cls': 'ch_ppocr_mobile_v2.0_cls_mobile.onnx'}
    if not all((model_dir / name).is_file() for name in models.values()):
        raise AssistantError('本地OCR模型不完整，请更新运行依赖；不会临时联网下载。', 503)
    engine = RapidOCR(params={'EngineConfig.onnxruntime.intra_op_num_threads': 1,
        'EngineConfig.onnxruntime.inter_op_num_threads': 1, 'EngineConfig.onnxruntime.use_cuda': False,
        'Global.log_level': 'error', **{key + '.model_path': str(model_dir / name) for key, name in models.items()}})
    try:
        result = engine(np.asarray(image))
    finally:
        image.close()
    text = safe_document('\n'.join(result.txts or ()))
    return {'text': text, 'method': 'PP-OCR / ONNX CPU',
            'references': [{'kind': 'page_ocr', 'page': page, 'start': 0, 'end': len(text)}] if text else [],
            'extraction_complete': total == 1 and bool(text), 'empty': not text.strip(),
            'page': page, 'total_pages': total, 'next_page': page + 1 if page < total else None,
            'low_confidence_count': sum(float(score) < .8 for score in (result.scores or ())),
            'note': 'OCR可能识别错误；设备编号、金额和安全操作参数须核对原件。仅识别本页，不代表已读取其他页。'}


def read_attachment(files, actor, file_id, mode='markdown', offset=0, page=1):
    from .lighthouse_files import EXTRACTION_VERSION, text_excerpt
    if mode not in {'markdown', 'ocr'} or type(offset) is not int or not 0 <= offset <= 100000 or type(page) is not int or not 1 <= page <= 100:
        raise AssistantError('请选择markdown或ocr，读取位置须为非负整数，页码为1至100。')
    item = files.get(actor, file_id)
    key = f'{file_id}:{item["sha256"]}:{mode}:{page}:v{EXTRACTION_VERSION}'
    result = files.store.get_document(_CACHE, key)
    if result is None:
        result = run_local(mode, {'path': item['path'], 'sha256': item['sha256'], 'page': page})
        files.get(actor, file_id)
        files.store.put_document(_CACHE, key, result)
    excerpt = text_excerpt(result, offset, 6000)
    if mode == 'ocr' and result.get('total_pages', 1) > 1:
        excerpt['partial'] = True
    return {**result, 'ok': True, 'file_id': file_id, 'name': item['name'], **excerpt}


def chart_png(kind, title, labels, series, unit=''):
    check_content({'title': title, 'labels': labels, 'series': series, 'unit': unit})
    if kind not in {'bar', 'line', 'pie'} or not isinstance(title, str) or not 1 <= len(title) <= 60 or not isinstance(unit, str) or len(unit) > 12:
        raise AssistantError('图表支持bar、line、pie，标题1至60字，单位最多12字。')
    if not isinstance(labels, list) or not 1 <= len(labels) <= 16 or any(not isinstance(label, str) or not 1 <= len(label) <= 24 for label in labels) or len(set(labels)) != len(labels):
        raise AssistantError('请提供1至16个不重复类别，每个最多24字。')
    if not isinstance(series, list) or not 1 <= len(series) <= 3:
        raise AssistantError('每张图支持1至3组数据。')
    values = []
    for row in series:
        if not isinstance(row, dict) or set(row) != {'name', 'values'} or not isinstance(row['name'], str) or not 1 <= len(row['name']) <= 20:
            raise AssistantError('每组数据须包含name（1至20字）和values。')
        data = row['values']
        if not isinstance(data, list) or len(data) != len(labels) or any(type(v) not in {int, float} or not math.isfinite(v) or abs(v) > 1e12 for v in data):
            raise AssistantError('每组有限数值数量须与类别一致，缺失数据不能替换为0。')
        values.append(data)
    if kind == 'pie' and (len(series) != 1 or len(labels) > 10 or min(values[0]) < 0 or sum(values[0]) <= 0):
        raise AssistantError('饼图须为单组、最多10类非负数据，合计大于0。')
    from reportlab.graphics.shapes import Drawing, String
    from reportlab.graphics.charts.barcharts import VerticalBarChart
    from reportlab.graphics.charts.linecharts import HorizontalLineChart
    from reportlab.graphics.charts.piecharts import Pie
    from reportlab.graphics import renderPDF
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.lib.colors import HexColor
    font_path = Path(os.environ.get('WINDIR', 'C:/Windows')) / 'Fonts/msyh.ttc'
    if font_path.is_file():
        pdfmetrics.registerFont(TTFont('LighthouseChart', str(font_path), subfontIndex=0))
    else:
        from reportlab.pdfbase.cidfonts import UnicodeCIDFont
        pdfmetrics.registerFont(UnicodeCIDFont('STSong-Light'))
    font = 'LighthouseChart' if font_path.is_file() else 'STSong-Light'
    colors = [HexColor(value) for value in ('#2377C5', '#109D82', '#CE8B32', '#8A6BA6', '#CE5B72', '#55746B', '#788AAB', '#B28B73', '#608C90', '#959340')]
    drawing = Drawing(900, 540)
    for index in range(0, len(title), 36):
        drawing.add(String(36, 508 - index // 36 * 24, title[index:index + 36], fontName=font, fontSize=18, fillColor=HexColor('#222E39')))
    if kind == 'pie':
        chart = Pie()
        chart.x, chart.y, chart.width, chart.height = 50, 120, 290, 290
        chart.data = values[0]
        chart.labels = []
        for i, label in enumerate(labels):
            chart.slices[i].fillColor = colors[i]
            text = f'{label}: {values[0][i]:g}{unit} ({values[0][i] / sum(values[0]):.1%})'
            size = min(12, 470 * 12 / max(1, pdfmetrics.stringWidth(text, font, 12)))
            drawing.add(String(390, 423 - i * 31, text, fontName=font, fontSize=size, fillColor=colors[i]))
    else:
        chart = VerticalBarChart() if kind == 'bar' else HorizontalLineChart()
        chart.x, chart.y, chart.width, chart.height = 80, 133, 774, 299
        chart.data = values
        wrap = min(6, max(3, int(chart.width / len(labels) / 10)))
        chart.categoryAxis.categoryNames = ['\n'.join(label[j:j + wrap] for j in range(0, len(label), wrap)) for label in labels]
        chart.categoryAxis.labels.fontName = font
        chart.categoryAxis.labels.fontSize = 9
        chart.valueAxis.labels.fontName = font
        chart.valueAxis.labels.fontSize = 10
        chart.valueAxis.valueMin = min(0, min(min(row) for row in values) * 1.1)
        chart.valueAxis.valueMax = max(1, max(max(row) for row in values) * 1.15)
        chart.valueAxis.visibleGrid = True
        chart.valueAxis.gridStrokeColor = HexColor('#E3E9ED')
        for i, row in enumerate(series):
            if kind == 'bar':
                chart.bars[i].fillColor = colors[i]
                chart.bars[i].strokeColor = None
                chart.barLabelFormat = '%g'
                chart.barLabels.fontName, chart.barLabels.fontSize = font, 9
            else:
                from reportlab.graphics.widgets.markers import makeMarker
                chart.lines[i].strokeColor, chart.lines[i].strokeWidth = colors[i], 2.5
                chart.lines[i].symbol = makeMarker('FilledCircle', size=5, fillColor=colors[i], strokeColor=colors[i])
            drawing.add(String(80 + i * 258, 32, row['name'], fontName=font, fontSize=12, fillColor=colors[i]))
        drawing.add(String(80, 445, unit, fontName=font, fontSize=10))
    drawing.add(chart)
    import pypdfium2 as pdfium
    with pdfium.PdfDocument(renderPDF.drawToString(drawing)) as pdf:
        with closing(pdf[0]) as page:
            with closing(page.render(scale=1.5)) as bitmap:
                with bitmap.to_pil() as image:
                    output = io.BytesIO()
                    image.save(output, format='PNG')
                    return output.getvalue()
