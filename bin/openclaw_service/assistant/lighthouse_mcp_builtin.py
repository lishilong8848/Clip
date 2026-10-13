"""Packaged stateless MCP utilities; no network, filesystem or business writes."""
import csv
import io
import json

from .lighthouse_basics import calculate as calculate_value, date_time as time_value


TOOLS = {
    'calculate': '精确计算算式、百分比和整数次方，不执行代码',
    'date_time': '北京时间、星期、日期间隔',
    'format_json': '校验、格式化或压缩JSON文本',
    'convert_table': 'CSV与JSON表格互转，结果可交给文件生成工具',
    'text_lines': '文本逐行去重、排序和计数',
}


def checked_text(text):
    if not isinstance(text, str) or not text.strip() or len(text.encode('utf-8')) > 32768:
        raise ValueError('请提供非空文本，最多32KiB。')
    return text


def format_json(text: str, compact: bool = False) -> dict:
    value = json.loads(checked_text(text), parse_constant=lambda _: (_ for _ in ()).throw(ValueError('不支持非有限数值。')))
    return {'text': json.dumps(value, ensure_ascii=False, indent=None if compact else 2, allow_nan=False)}


def convert_table(text: str, direction: str = 'csv_to_json') -> dict:
    checked_text(text)
    if direction == 'csv_to_json':
        rows = list(csv.reader(io.StringIO(text)))
        if not rows or len(rows) > 1001 or len(rows[0]) > 100:
            raise ValueError('最多100列、1000条数据。')
        headers, data = rows[0], rows[1:]
        if not all(headers) or len(set(headers)) != len(headers) or any(len(row) != len(headers) for row in data):
            raise ValueError('列名须非空且不重复，各行列数须一致。')
        return {'text': json.dumps([dict(zip(headers, row)) for row in data], ensure_ascii=False), 'rows': len(data)}
    if direction != 'json_to_csv':
        raise ValueError('请选择csv_to_json或json_to_csv。')
    rows = json.loads(text)
    if not isinstance(rows, list) or not 1 <= len(rows) <= 1000 or any(not isinstance(row, dict) for row in rows):
        raise ValueError('请提供1至1000条JSON对象组成的数组。')
    headers = list(dict.fromkeys(key for row in rows for key in row))
    if not 1 <= len(headers) <= 100 or any(isinstance(v, (dict, list)) for row in rows for v in row.values()):
        raise ValueError('最多100列，单元格不能嵌套对象或数组。')
    output = io.StringIO(newline='')
    writer = csv.writer(output)
    # Treat headers and values as text, never as spreadsheet formulas.
    def literal(value):
        return "'" + value if isinstance(value, str) and value.lstrip().startswith(('=', '+', '-', '@')) else value
    writer.writerow([literal(key) for key in headers])
    for row in rows:
        writer.writerow([literal(row.get(key, '')) for key in headers])
    return {'text': output.getvalue(), 'rows': len(rows)}


def text_lines(text: str, deduplicate: bool = True, sort: bool = False) -> dict:
    lines = [line.strip() for line in checked_text(text).splitlines() if line.strip()]
    count = len(lines)
    if deduplicate:
        lines = list(dict.fromkeys(lines))
    if sort:
        lines.sort()
    return {'text': '\n'.join(lines), 'original_count': count, 'count': len(lines)}


def main():
    import os
    import sys
    if os.name == 'nt':
        import ctypes
        ctypes.windll.kernel32.SetPriorityClass(ctypes.windll.kernel32.GetCurrentProcess(), 0x4000)
    from mcp.server.fastmcp import FastMCP
    from mcp.types import ToolAnnotations
    server = FastMCP('Lighthouse utilities', log_level='ERROR')
    functions = {
        'calculate': calculate_value, 'date_time': time_value,
        'format_json': format_json, 'convert_table': convert_table, 'text_lines': text_lines,
    }
    descriptions = TOOLS
    if '--office' in sys.argv:
        from . import lighthouse_mcp_office
        functions, descriptions = lighthouse_mcp_office.functions(), lighthouse_mcp_office.TOOLS
    elif '--public' in sys.argv:
        from . import lighthouse_mcp_public
        functions, descriptions = lighthouse_mcp_public.functions(), lighthouse_mcp_public.TOOLS
    for name, function in functions.items():
        server.add_tool(function, name=name, description=descriptions[name],
                        annotations=ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint='--public' in sys.argv))
    server.run(transport='stdio')


if __name__ == '__main__':
    main()
