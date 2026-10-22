"""Read the existing knowledge store without constructing another index worker."""
from contextlib import closing
import json
from pathlib import Path
import sqlite3


def index_error(message):
    text = str(message or '')
    for words, result in (
        (('敏感', '凭证', '身份证'), '内容包含疑似敏感信息，已阻止索引，请先脱敏。'),
        (('模型', '嵌入', '向量'), '本地嵌入模型或向量索引未就绪，请检查模型与运行依赖后重试。'),
        (('原文件缺失',), '知识库本地原文件缺失，等待从已缓存来源恢复。'),
        (('10万', '内存预算', '上限'), '知识库容量达到上限，请联系管理员处理。'),
        (('正文', 'OCR', '解析'), '文件正文解析未完成，请检查文件格式或图片文字识别结果。'),
    ):
        if any(word in text for word in words):
            return result
    return '文件解析或索引未完成，系统将按原任务重试。' if text else ''


def read_index_states(path, prefix, current):
    """None means unreadable; {} means no current source documents, never ready."""
    path = Path(path)
    if not path.is_file():
        return {}
    try:
        with closing(sqlite3.connect(path.resolve().as_uri() + '?mode=ro', uri=True, timeout=2)) as db:
            db.row_factory = sqlite3.Row
            db.execute('BEGIN')
            config = db.execute("SELECT payload FROM config WHERE key='embedding'").fetchone()
            config = json.loads(config[0]) if config else {}
            if not isinstance(config, dict):
                return None
            vector = config.get('mode') not in ('keyword', 'hybrid')
            rows = db.execute('''SELECT s.source_key,s.payload,d.id,d.active_version,d.status,d.error
                FROM document_sources s JOIN documents d ON d.id=s.document_id
                WHERE substr(s.source_key,1,?)=? AND s.enabled=1 AND d.deleted=0''', (len(prefix), prefix)).fetchall()
            result = {}
            for row in rows:
                if not current(json.loads(row['payload'])):
                    continue
                missing = False
                if row['status'] == 'ready':
                    chunks, vectors = db.execute('''SELECT count(*),count(v.chunk_id) FROM chunks c
                        LEFT JOIN local_vectors v ON v.chunk_id=c.id WHERE c.document_id=? AND c.version=?''',
                        (row['id'], row['active_version'])).fetchone()
                    missing = not chunks or vector and chunks != vectors
                result[row['source_key']] = {'status': 'queued' if missing else row['status'],
                    'error': '知识库索引缺失，等待补入。' if missing else index_error(row['error']), 'missing': missing}
            return result
    except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
        return None
