"""Bounded, credential-free OpenClaw lifecycle logs shared with the Qt launcher."""
import json
import logging
import math
import os
from pathlib import Path
import re

PREFIX = '[ClipFlow][OpenClaw] '
STAGES = {
    'preparing': '正在后台准备助手',
    'service_connecting': '正在后台启动灯塔助手',
    'service_ready': '灯塔助手后台已连接',
    'service_failed': '灯塔助手仍在后台准备，其他业务可继续使用',
    'runtime_check': '正在校验运行环境',
    'runtime_install': '本地运行环境缺失，正在准备安装',
    'runtime_ready': '运行环境已就绪',
    'skills_ready': '技能目录已加载',
    'awaiting_login': '等待登录账号，登录后自动准备独立 Agent',
    'gateway_starting': '正在启动共享常驻网关',
    'gateway_ready': '共享常驻网关已就绪',
    'startup_failed': '助手准备或启动失败',
    'warmup_skipped': '当前账号没有可用模型，未启动网关',
    'gateway_stopped': '共享常驻网关已停止',
    'legacy_engine': '当前使用兼容引擎，未启动 OpenClaw',
}
TEXT_FIELDS = {
    'node': r'\d+\.\d+\.\d+', 'openclaw': r'\d+\.\d+\.\d+',
    'account': r'[0-9a-f]{8,32}', 'error': r'[A-Za-z][A-Za-z0-9_]{0,60}',
    'last_stage': r'[A-Za-z][A-Za-z0-9_]{0,60}',
}
NUMBERS = {'pid', 'port', 'elapsed_ms', 'protocol', 'skills', 'backend_pid', 'exit_code'}


def render(line, *, backend_pid=None):
    if not isinstance(line, str) or not line.startswith(PREFIX) or len(line) > 2048:
        return None
    try:
        data = json.loads(line[len(PREFIX):])
    except (ValueError, RecursionError):
        return None
    if not isinstance(data, dict) or not isinstance(data.get('stage'), str) or data['stage'] not in STAGES or set(data) - {'stage'} - TEXT_FIELDS.keys() - NUMBERS:
        return None
    if backend_pid is not None and data.get('backend_pid') != backend_pid:
        return None
    for key, value in data.items():
        if key in TEXT_FIELDS and (not isinstance(value, str) or not re.fullmatch(TEXT_FIELDS[key], value)):
            return None
        if key in NUMBERS and (isinstance(value, bool) or not isinstance(value, (int, float)) or
                               abs(value) > 1_000_000_000 or not math.isfinite(value)):
            return None
    details = ' '.join(f'{key}={data[key]}' for key in (*TEXT_FIELDS, 'skills', 'pid', 'port', 'protocol', 'elapsed_ms', 'exit_code') if key in data)
    return '[ClipFlow][OpenClaw] ' + STAGES[data['stage']] + (' | ' + details if details else '')


def emit(stage, **fields):
    try:
        owner = os.environ.get('CLIPFLOW_OPENCLAW_LOG_OWNER_PID', '')
        backend_pid = int(owner) if owner.isascii() and owner.isdecimal() else os.getpid()
        line = PREFIX + json.dumps({'stage': stage, 'backend_pid': backend_pid, **fields}, ensure_ascii=False, separators=(',', ':'))
    except (TypeError, ValueError):
        return
    if render(line) is None:
        return
    logging.getLogger('openclaw_service.lifecycle').info('%s', line)
    try:
        print(line, flush=True)
    except (OSError, RuntimeError):
        logging.getLogger(__name__).info('%s', line)


def relay_file(path, offset, stopped, backend_pid, *, sink=print):
    """Read only new log bytes; a sleeping worker never touches the Qt UI thread."""
    pending = b''
    discard_line = False
    while not stopped.wait(.5):
        try:
            with Path(path).open('rb') as stream:
                if stream.seek(0, 2) < offset:
                    offset, pending, discard_line = 0, b'', False
                stream.seek(offset)
                chunk = stream.read(64 * 1024)
                offset = stream.tell()
        except OSError:
            continue
        for part in chunk.splitlines(keepends=True):
            end = part.endswith(b'\n')
            if not discard_line:
                pending += part
                if len(pending) > 2048:
                    pending, discard_line = b'', True
            if end:
                if not discard_line:
                    text = render(pending.decode('utf-8', errors='replace').rstrip(), backend_pid=backend_pid)
                    if text:
                        try:
                            sink(text)
                        except (OSError, RuntimeError):
                            return
                pending, discard_line = b'', False
