"""Bounded text extraction + chunk splitting for shared knowledge-base uploads.

Extraction preserves the original document text verbatim (sensitive-data
blocking is the caller's job before shared indexing/download), so this module
never calls safe_text to silently drop lines.  All size limits fail loudly with
an actionable Chinese AssistantError instead of silently truncating.

Bounding is enforced incrementally while streaming rows/paragraphs, before the
interesting structures are built in memory:
  * CSV is read with the stdlib ``csv`` reader; row/col limits and the running
    character total are checked per logical row (blank lines still count
    towards row numbers so original numbering is preserved), and only bounded
    batches are kept in memory at a time.
  * XLSX is streamed with openpyxl ``read_only`` iter_rows; the whole workbook
    is bounded cumulatively (not per sheet) and MAX_CHARS is enforced while
    text batches accumulate.
  * DOCX paragraph text is reconstructed only from ``w:t`` / ``w:tab`` /
    ``w:br`` nodes (never ``itertext``, which also picks up metatext), and
    table-cell paragraphs retain their table+row association where practical.
The scanned-PDF error is explicit that this module does not bundle OCR.
"""
from __future__ import annotations

import csv
import io
import re
import zipfile
from pathlib import Path
from xml.etree import ElementTree

from .lighthouse_ai import AssistantError
from .lighthouse_files import _check_archive, recognize_text

MAX_BYTES = 100 * 1024 * 1024
MAX_CHARS = 2_000_000
MAX_PAGES = 300
MAX_ROWS = 20_000
MAX_COLS = 100
MAX_CHUNKS = 8_000
BATCH_ROWS = 200
SUPPORTED_EXTENSIONS = frozenset({
    ".txt", ".md", ".csv", ".pdf", ".docx", ".xlsx", ".xlsm",
    ".png", ".jpg", ".jpeg", ".webp",
})

_W = "{http://schemas.openxmlformats.org/wordprocessingml/2006/main}"


# ---------------------------------------------------------------------------
# shared helpers
# ---------------------------------------------------------------------------

def _clean(value):
    return "" if value is None else str(value).strip()


def _decode_text(content):
    for encoding in ("utf-8-sig", "gb18030"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    raise AssistantError("无法识别文本编码，请另存为UTF-8或GB18030后重新上传。")


def _paragraphs(text):
    """Split text at blank-line boundaries into non-empty readable blocks."""
    blocks = []
    for block in re.split(r"\n[ \t]*\n", text):
        block = block.strip()
        if block:
            blocks.append(block)
    return blocks


def _align_boundary(text, start, end):
    """Pull a chunk end back to a paragraph boundary when it is practical."""
    window = text[start:end]
    idx = window.rfind("\n\n")
    if idx != -1:
        candidate = start + idx + 2
        # Only align when it does not shrink the chunk past half its budget.
        if end - candidate <= max(1, (end - start) // 2):
            return candidate
    return end


def _split_text(text, size, overlap):
    """Split one section's text into readable, overlapping parts.

    Uses a fixed-size window that advances by size-overlap, and pulls each
    chunk end back onto a blank-line paragraph boundary where practical so the
    output stays readable.  Oversized paragraphs are split mid-text, but every
    part still keeps the requested overlap with its neighbours.

    The loop is guaranteed to make forward progress: each iteration strictly
    increases ``start``, so no runaway guard is needed (the old million-
    iteration guard was removed).
    """
    text = text.strip()
    if not text:
        return []
    n = len(text)
    chunks = []
    start = 0
    while start < n:
        prev = start
        end = min(n, start + size)
        aligned = _align_boundary(text, start, end)
        chunk = text[start:aligned]
        # Overlap is computed over the raw text, so chunks are kept as-is;
        # that keeps adjacent chunks sharing exactly the requested tail/head.
        if chunk.strip():
            chunks.append(chunk)
        if aligned >= n:
            break
        nxt = aligned - overlap
        start = nxt if nxt > start else aligned
        # Simple guaranteed-progress guard: never let start stall.
        if start <= prev:
            start = prev + 1
    return chunks


# ---------------------------------------------------------------------------
# format handlers
# ---------------------------------------------------------------------------

def _text_sections(content, filename):
    text = _decode_text(content)
    if len(text) > MAX_CHARS:
        raise AssistantError("文档提取文字超过200万字符上限，请拆分文件后上传。")
    blocks = _paragraphs(text)
    if not blocks:
        raise AssistantError("文本文件未包含可读内容，请确认不是空文件。")
    return [{"text": block, "location": f"{filename}:段落{index}"}
            for index, block in enumerate(blocks, 1)]


def _csv_sections(content, filename):
    text = _decode_text(content)
    reader = csv.reader(io.StringIO(text))
    header = None
    header_index = 0
    batch = []          # bounded list of (original_row_no, cells)
    sections = []
    total_chars = 0

    def add_section():
        nonlocal batch, total_chars
        if not batch:
            return
        body = "\n".join("\t".join(cells) for _, cells in batch)
        block = "\t".join(header)
        if body:
            block += "\n" + body
        col_ctx = f"（列：{'、'.join(h for h in header if h)}）" if any(header) else ""
        first, last = batch[0][0], batch[-1][0]
        sections.append({
            "text": block,
            "location": f"{filename}:行{first}-{last}{col_ctx}",
        })
        total_chars += len(block)
        if total_chars > MAX_CHARS:
            raise AssistantError(
                "文档提取文字超过200万字符上限，请拆分文件后上传。")
        batch = []

    # Each logical csv row maps to one row number; blank lines are returned by
    # csv.reader as [] and therefore still advance the original row counter.
    for row_number, row in enumerate(reader, 1):
        if row_number > MAX_ROWS:
            raise AssistantError(
                f"CSV超过{MAX_ROWS}行（含空行），请拆分后上传。")
        if not row:                      # blank line counts toward the row count
            continue
        cells = [_clean(cell) for cell in row]
        if len(cells) > MAX_COLS:
            raise AssistantError(
                f"CSV超过{MAX_COLS}列，请精简列后上传。")
        if header is None:
            header = cells
            header_index = row_number
            continue
        if not any(cells):               # a blank-but-present row
            continue
        batch.append((row_number, cells))
        if len(batch) >= BATCH_ROWS:
            add_section()

    if header is None:
        raise AssistantError("CSV文件未包含可读取的行。")
    if not sections and not batch:
        # Header-only CSV is still indexed, not silently dropped.
        col_ctx = f"（列：{'、'.join(h for h in header if h)}）" if any(header) else ""
        sections.append({
            "text": "\t".join(header),
            "location": f"{filename}:行{header_index}{col_ctx}",
        })
    else:
        add_section()
    if not sections:
        raise AssistantError("CSV文件未包含可读取的行。")
    return sections


def _paragraph_text(paragraph):
    """Reconstruct a paragraph's visible text from w:t/w:tab/w:br only.

    Unlike ``itertext`` (which also picks up instrText, proofErr and other
    metatext), we intentionally restrict ourselves to the three text-bearing
    nodes so only visible text is indexed.
    """
    parts = []
    for node in paragraph.iter():
        tag = node.tag
        if tag == _W + "t":
            parts.append(node.text or "")
        elif tag == _W + "tab":
            parts.append("\t")
        elif tag == _W + "br":
            parts.append("\n")
    return "".join(parts).strip()


def _iter_docx_paragraphs(root):
    """Yield (p_element, table_index, row_index) in document order.

    ``table_index``/``row_index`` are None for paragraphs outside tables.
    Table-cell paragraphs retain their row association; nested tables get a
    fresh table index.
    """
    table_counter = 0

    def walk(elem, table_idx, row_idx):
        tag = elem.tag
        if tag == _W + "p":
            yield (elem, table_idx, row_idx)
            return
        if tag == _W + "tbl":
            nonlocal table_counter
            table_counter += 1
            current = table_counter
            for rindex, tr in enumerate(
                    (child for child in elem if child.tag == _W + "tr"), 1):
                for tc in (child for child in tr if child.tag == _W + "tc"):
                    for child in tc:
                        yield from walk(child, current, rindex)
            return
        for child in elem:
            yield from walk(child, table_idx, row_idx)

    yield from walk(root, None, None)


def _docx_sections(content, filename):
    _check_archive(content)
    with zipfile.ZipFile(io.BytesIO(content)) as archive:
        if "word/document.xml" not in archive.namelist():
            raise AssistantError("无效的Word文档：缺少 document.xml。")
        xml = archive.read("word/document.xml")
    root = ElementTree.fromstring(xml)
    sections = []
    counter = 0
    total_chars = 0
    for paragraph, table_idx, row_idx in _iter_docx_paragraphs(root):
        counter += 1
        block = _paragraph_text(paragraph)
        if not block:
            continue
        total_chars += len(block)
        if total_chars > MAX_CHARS:
            raise AssistantError("文档提取文字超过200万字符上限，请拆分文件后上传。")
        if table_idx is None:
            location = f"{filename}:段落{counter}"
        else:
            location = f"{filename}:表格{table_idx}行{row_idx}段落{counter}"
        sections.append({"text": block, "location": location})
    if not sections:
        raise AssistantError("Word文档未提取到可读文字。")
    return sections


def _pdf_page_need_ocr(page_no):
    # pypdf can return '' for scanned pages.  This module intentionally does
    # NOT bundle/render OCR (fitz is not part of the runtime), so we fail
    # explicitly instead of indexing a blank page or fabricating content.
    raise AssistantError(
        f"PDF第{page_no}页未提取到文字，疑似扫描件；本系统不支持PDF内置OCR，"
        "请转换为带文字层的PDF，或先用其他OCR识别后再上传。")


def _pdf_sections(content, filename):
    from pypdf import PdfReader
    reader = PdfReader(io.BytesIO(content))
    if reader.is_encrypted:
        raise AssistantError("加密PDF无法读取，请上传未加密的PDF文件。")
    if len(reader.pages) > MAX_PAGES:
        raise AssistantError(f"PDF超过{MAX_PAGES}页，请拆分文件后上传。")
    sections = []
    total_chars = 0
    for index, page in enumerate(reader.pages, 1):
        block = (page.extract_text() or "").strip()
        if not block:
            _pdf_page_need_ocr(index)
        total_chars += len(block)
        if total_chars > MAX_CHARS:
            raise AssistantError("文档提取文字超过200万字符上限，请拆分文件后上传。")
        sections.append({"text": block, "location": f"{filename}:第{index}页"})
    return sections


def _xlsx_sections(content, filename):
    _check_archive(content)
    from openpyxl import load_workbook

    workbook = load_workbook(io.BytesIO(content), read_only=True, data_only=False)
    try:
        sections = []
        total_rows = 0     # cumulative across the whole workbook (not per sheet)
        max_col = 0        # widest width seen anywhere in the workbook
        total_chars = 0
        produced = False

        def add_section(sheet_title, header_cells, batch_rows):
            nonlocal total_chars, produced
            body = "\n".join("\t".join(cells) for _, cells in batch_rows)
            block = "\t".join(header_cells)
            if body:
                block += "\n" + body
            col_ctx = (f"（列：{'、'.join(h for h in header_cells if h)}）"
                       if any(header_cells) else "")
            first, last = batch_rows[0][0], batch_rows[-1][0]
            sections.append({
                "text": block,
                "location": f"{filename}:工作表「{sheet_title}」行{first}-{last}{col_ctx}",
            })
            total_chars += len(block)
            if total_chars > MAX_CHARS:
                raise AssistantError(
                    "文档提取文字超过200万字符上限，请拆分文件后上传。")
            produced = True

        for sheet in workbook.worksheets:
            produced = False
            header = None
            header_row = 0
            batch = []      # bounded batch of (original_row_no, cells)
            for row_number, row in enumerate(sheet.iter_rows(values_only=True), 1):
                total_rows += 1
                if total_rows > MAX_ROWS:
                    raise AssistantError(
                        f"Excel所有工作表合计超过{MAX_ROWS}行，请拆分后上传。")
                cells = [_clean(cell) for cell in row]
                max_col = max(max_col, len(cells))
                if max_col > MAX_COLS:
                    raise AssistantError(
                        f"Excel超过{MAX_COLS}列，请精简列后上传。")
                if header is None:
                    header = cells
                    header_row = row_number
                    continue
                if not any(cells):       # blank-but-present row
                    continue
                batch.append((row_number, cells))
                if len(batch) >= BATCH_ROWS:
                    add_section(sheet.title, header, batch)
                    batch = []
            if header is None:
                continue                 # truly empty sheet
            if batch:
                add_section(sheet.title, header, batch)
            elif not produced:
                # Header-only sheet is still indexed, not silently dropped.
                col_ctx = (f"（列：{'、'.join(h for h in header if h)}）"
                           if any(header) else "")
                block = "\t".join(header)
                sections.append({
                    "text": block,
                    "location": f"{filename}:工作表「{sheet.title}」行{header_row}{col_ctx}",
                })
                total_chars += len(block)
                if total_chars > MAX_CHARS:
                    raise AssistantError(
                        "文档提取文字超过200万字符上限，请拆分文件后上传。")
                produced = True
        if not sections:
            raise AssistantError("Excel文件未提取到可读文字。")
        return sections
    finally:
        workbook.close()


def _image_sections(content, filename, ocr):
    if ocr is None:
        block = recognize_text(content)
    else:
        block = ocr(content)
    block = _clean(block)
    if not block:
        raise AssistantError("图片未识别出文字，可改用图片模型查看或补充文字。")
    return [{"text": block, "location": f"{filename}:图片OCR"}]


# ---------------------------------------------------------------------------
# public API
# ---------------------------------------------------------------------------

def extract_sections(content, filename, *, ocr=None):
    """Extract bounded, located sections from one in-memory document.

    Returns a list of {"text": str, "location": str}.  Raises AssistantError
    with an actionable Chinese message on any bound violation instead of
    silently truncating.
    """
    if not isinstance(content, bytes):
        raise AssistantError("文件内容必须是字节数据。")
    if not filename or not isinstance(filename, str):
        raise AssistantError("缺少文件名。")
    if not content:
        raise AssistantError("文件内容为空，请上传有效的文件。")
    if len(content) > MAX_BYTES:
        raise AssistantError("单文件不得超过100MiB。")

    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED_EXTENSIONS:
        raise AssistantError(
            "不支持的文件类型，仅支持 txt/md/csv/pdf/docx/xlsx/xlsm 以及图片。")

    if suffix in (".png", ".jpg", ".jpeg", ".webp"):
        sections = _image_sections(content, filename, ocr)
    elif suffix == ".pdf":
        sections = _pdf_sections(content, filename)
    elif suffix == ".docx":
        sections = _docx_sections(content, filename)
    elif suffix in (".xlsx", ".xlsm"):
        sections = _xlsx_sections(content, filename)
    elif suffix == ".csv":
        sections = _csv_sections(content, filename)
    else:
        sections = _text_sections(content, filename)

    total = sum(len(section["text"]) for section in sections)
    if total > MAX_CHARS:
        raise AssistantError(
            "文档提取文字超过200万字符上限，请拆分文件后上传。")
    return sections


def split_sections(sections, size=420, overlap=60):
    """Split extracted sections into bounded, readable, overlapping parts.

    Each returned item keeps the source location on every part (anchored with
    a fragment suffix).  Chunks advance by size-overlap and are pulled back onto
    paragraph boundaries where practical; oversized single paragraphs are split
    mid-text with overlap retained.  Fails explicitly if the result would exceed
    MAX_CHUNKS.
    """
    size = max(1, int(size))
    overlap = max(0, min(int(overlap), size - 1))
    if not isinstance(sections, list) or not all(
            isinstance(section, dict) and isinstance(section.get("text"), str)
            for section in sections):
        raise AssistantError("拆分输入必须是 {text, location} 字典列表。")

    parts = []
    for section in sections:
        text = section.get("text", "")
        location = section.get("location", "")
        if not text:
            continue
        chunks = _split_text(text, size, overlap)
        for index, chunk in enumerate(chunks, 1):
            anchor = f"（片段{index}）"
            parts.append({"text": chunk, "location": location + anchor})
            if len(parts) > MAX_CHUNKS:
                raise AssistantError(
                    f"拆分片段超过{MAX_CHUNKS}个上限，请缩小文件后重试。")
    return parts
