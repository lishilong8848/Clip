"""Sharing source adapters over the existing Qt reader and offline document tools."""
import hashlib
import json
from pathlib import Path
import re
import tempfile
import uuid
from urllib.parse import urlsplit

from .learning import LearningError, stamp

MAX_FILE = 20 * 1024 * 1024
SUFFIXES = {'.pdf', '.docx', '.xlsx', '.xlsm', '.pptx'}


def source_url(value, *, minutes=False):
    if not isinstance(value, str) or len(value) > 2048 or re.search(r'[\x00-\x20\\]', value):
        raise LearningError('请提供企业飞书docx/wiki链接。')
    try:
        parsed = urlsplit(value)
    except ValueError:
        raise LearningError('请提供企业飞书docx/wiki链接。') from None
    kinds = 'docx|wiki' if minutes else 'docx|wiki|file'
    if parsed.scheme != 'https' or parsed.netloc.lower() != 'vnet.feishu.cn' or not re.fullmatch(r'/(' + kinds + r')/[A-Za-z0-9]+/?', parsed.path):
        raise LearningError('纪要仅支持企业飞书docx/wiki；材料另支持file链接，其他文件请显式上传。')
    return 'https://vnet.feishu.cn' + parsed.path.rstrip('/')


class _MaterialReader:
    """Adapt read-only metadata and bounded bytes to the Qt document auth retry."""
    def __init__(self, portal):
        self.portal = portal

    def _auth_headers(self):
        return self.portal._auth_headers()

    def _request_payload(self, method, url, *, context, headers, params):
        if url == 'https://open.feishu.cn/open-apis/drive/v1/metas/batch_query':
            return self.portal._request_payload('POST', url, context=context, headers=headers,
                json_payload={'request_docs': [{'doc_token': params['token'], 'doc_type': 'file'}]}, retries=0)
        if re.fullmatch(r'https://open\.feishu\.cn/open-apis/drive/v1/(files|medias)/[A-Za-z0-9]+/download', url):
            client = self.portal._http_client._client_for_request()
            with client.stream('GET', url, headers=headers, follow_redirects=False) as response:
                if 300 <= response.status_code < 400:
                    raise LearningError('材料下载要求跳转，未跟随跳转；请下载原文件后显式上传。', 502)
                size = response.headers.get('content-length')
                if size and (not size.isdecimal() or int(size) > MAX_FILE):
                    raise LearningError('材料下载大小无效或超过20MiB。', 413)
                content = bytearray()
                for chunk in response.iter_bytes(65536):
                    if len(content) + len(chunk) > MAX_FILE:
                        raise LearningError('材料下载超过20MiB，已停止。', 413)
                    content.extend(chunk)
                if 'json' in response.headers.get('content-type', '').lower():
                    try:
                        value = json.loads(content)
                    except (ValueError, UnicodeError):
                        value = None
                    if isinstance(value, dict) and type(value.get('code')) is int and value['code']:
                        return value
                    raise LearningError('附件接口未返回可识别文件，请下载原文件后显式上传。', 502)
                if response.status_code != 200 or not content:
                    raise LearningError('材料下载未完成，请核对文件授权或显式上传原文件。', 502)
                return {'code': 0, 'data': {'content': bytes(content)}}
        return self.portal._request_payload(method, url, context=context, headers=headers, params=params, retries=0)


def cloud_reader(portal, url, *, minutes=False):
    from httpx import TransportError
    from openclaw_service.assistant.lighthouse_documents import document_reader
    from openclaw_service.assistant.lighthouse_ai import AssistantError
    target = source_url(url, minutes=minutes)
    request = document_reader(_MaterialReader(portal), '分享材料只读解析')

    def get(path, params=None):
        try:
            result = request('https://open.feishu.cn/open-apis/' + path, params)
        except LearningError:
            raise
        except AssistantError as exc:
            raise LearningError(str(exc), exc.status) from None
        except TransportError:
            raise LearningError('分享材料读取时网络超时或连接中断，请稍后重试。', 503) from None
        except Exception:
            raise LearningError('分享材料无法读取，请核对当前应用和文件授权，或显式上传原文件。', 502) from None
        if not isinstance(result, dict) or type(result.get('code')) is not int or result['code'] != 0 or not isinstance(result.get('data'), dict):
            code = result.get('code', 'unknown') if isinstance(result, dict) else 'unknown'
            raise LearningError(f'分享材料读取未完成（code={code}），请核对当前应用权限和原文档授权。', 502)
        return result['data']

    token = target.rsplit('/', 1)[1]
    kind = target.split('/')[-2]
    if '/wiki/' in target:
        node = get('wiki/v2/spaces/get_node', {'token': token}).get('node') or {}
        if not isinstance(node, dict):
            raise LearningError('Wiki节点信息无法识别，请核对链接或显式上传原文件。', 502)
        token, kind = node.get('obj_token'), node.get('obj_type')
        if kind not in ('docx', 'file') or minutes and kind != 'docx' or not isinstance(token, str) or not re.fullmatch('[A-Za-z0-9]+', token):
            raise LearningError('Wiki节点类型无法用于本次读取；纪要须为docx，材料须为docx或支持格式的文件，请核对或显式上传。')
    return target, token, kind, get


def resolve_document(portal, url):
    target, token, _, _ = cloud_reader(portal, url, minutes=True)
    return 'https://vnet.feishu.cn/docx/' + token, target


def _read_linked_file(get, token, name, *, embedded=False):
    if not isinstance(name, str) or len(name) > 240 or Path(name).suffix.lower() not in SUFFIXES:
        raise LearningError('材料文件名或格式无法识别，仅支持PDF、DOCX、XLSX、XLSM和PPTX；请核对原文件后显式上传。')
    content = get('drive/v1/' + ('medias/' if embedded else 'files/') + token + '/download').get('content')
    if not isinstance(content, bytes) or not 0 < len(content) <= MAX_FILE:
        raise LearningError('材料附件为空、响应无效或超过20MiB。', 413)
    digest = hashlib.sha256(content).hexdigest()
    with tempfile.TemporaryDirectory(prefix='sharing-material-') as folder:
        path = Path(folder) / ('material' + Path(name).suffix.lower())
        path.write_bytes(content)
        parsed = SharingMaterials.parse(path, digest)
    return {'title': name, 'sha256': digest, **parsed}


def _file_metadata(get, token):
    data = get('drive/v1/metas/batch_query', {'token': token})
    metas = data.get('metas')
    if (data.get('failed_list') or not isinstance(metas, list) or len(metas) != 1
            or not isinstance(metas[0], dict) or metas[0].get('doc_token') != token or metas[0].get('doc_type') != 'file'
            or type(metas[0].get('latest_modify_time')) is not int):
        raise LearningError('无法完整核验材料文件信息或访问权限，请核对链接或显式上传原文件。', 502)
    return metas[0]


def read_material_link(portal, url):
    target, token, kind, get = cloud_reader(portal, url)
    if kind == 'file':
        meta = _file_metadata(get, token)
        source = _read_linked_file(get, token, meta.get('title'))
        current = _file_metadata(get, token)
        if any(current.get(key) != meta.get(key) for key in ('title', 'latest_modify_time')):
            raise LearningError('读取期间材料文件已变化，请重新提交。', 409)
        return {**source, 'url': target, 'revision': meta['latest_modify_time']}
    path = 'docx/v1/documents/' + token
    document = get(path).get('document') or {}
    if not isinstance(document, dict):
        raise LearningError('分享材料文档信息无法识别，请核对链接或显式上传原文件。', 502)
    revision = document.get('revision_id')
    text = get(path + '/raw_content', {'lang': 1}).get('content')
    if not revision or not isinstance(text, str) or len(text) > 100000:
        raise LearningError('分享材料正文为空、过大或版本不完整，请拆分后重新读取。')
    files, cursor, seen, blocks = {}, '', set(), []
    for _ in range(40):
        data = get(path + '/blocks', {'page_size': 500, 'document_revision_id': revision,
                   **({'page_token': cursor} if cursor else {})})
        if not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
            raise LearningError('材料附件分页不完整，未使用部分材料。', 502)
        blocks.extend(data['items'])
        for block in data['items']:
            file = block.get('file') if isinstance(block, dict) else None
            if file is None:
                continue
            if not isinstance(file, dict) or not isinstance(file.get('token'), str) or not re.fullmatch('[A-Za-z0-9]+', file['token']):
                raise LearningError('文档附件标识无法识别，请显式上传原文件。')
            if file['token'] in files and files[file['token']] != file.get('name'):
                raise LearningError('文档附件信息冲突，请明确选择文件链接或显式上传。')
            files[file['token']] = file.get('name')
        if len(files) > 1:
            raise LearningError('文档包含多个附件，无法确定出题材料；请分别提供文件链接或显式上传所需文件。')
        if not data['has_more']:
            break
        cursor = data.get('page_token')
        if not isinstance(cursor, str) or not cursor or cursor in seen:
            raise LearningError('材料附件分页不完整，未使用部分材料。', 502)
        seen.add(cursor)
    else:
        raise LearningError('材料附件分页超过上限，请拆分文档。', 413)
    extra = {}
    if files:
        file_token, name = next(iter(files.items()))
        attached = _read_linked_file(get, file_token, name, embedded=True)
        offset = len(text) + 2 if text else 0
        text = text + '\n\n' + attached['text'] if text else attached['text']
        extra = {'attachment': {'name': name, 'sha256': attached['sha256']},
                 'references': [{**ref, 'start': ref['start'] + offset, 'end': ref['end'] + offset} for ref in attached['references']],
                 'low_confidence_count': attached['low_confidence_count']}
    if not text.strip() or len(text) > 100000:
        raise LearningError('分享材料正文为空或超过100000字，请拆分后重新读取。')
    if get(path).get('document', {}).get('revision_id') != revision:
        raise LearningError('读取期间分享材料已变化，请重新提交。', 409)
    from .sharing_images import document_images
    images = document_images(blocks, 'https://vnet.feishu.cn/docx/' + token, revision, document.get('title') or '分享材料')
    return {'url': target, 'title': document.get('title') or '分享材料', 'revision': revision,
            'text': text, 'sha256': hashlib.sha256(text.encode()).hexdigest(),
            'extraction_complete': True, 'read_at': stamp(), **extra, **({'images': images} if images else {})}


class SharingMaterials:
    def __init__(self, learning):
        self.learning = learning
        self.root = (learning.root / 'sharing_materials').resolve()

    def upload(self, actor, name, content):
        self.learning._admin(actor)
        if not isinstance(name, str) or len(name) > 240 or Path(name).suffix.lower() not in SUFFIXES:
            raise LearningError('支持PDF、DOCX、XLSX、XLSM和PPTX材料。')
        if not isinstance(content, bytes) or not 0 < len(content) <= MAX_FILE:
            raise LearningError('单份材料须为非空文件，最大20MiB。', 413)
        identity = uuid.uuid4().hex
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / (identity + Path(name).suffix.lower())
        with path.open('xb') as output:
            output.write(content)
        item = {'id': identity, 'owner': actor['id'], 'name': Path(name).name, 'size': len(content),
                'path': str(path), 'sha256': hashlib.sha256(content).hexdigest()}
        try:
            # Staging is local-only; the immutable parsed snapshot travels with its batch.
            with self.learning.transaction() as conn:
                self.learning._put('sharing_material', identity, item, conn, False)
        except BaseException:
            path.unlink(missing_ok=True)
            raise
        return {key: item[key] for key in ('id', 'name', 'size')}

    def read(self, identity, owner):
        if not isinstance(identity, str) or not re.fullmatch('[a-f0-9]{32}', identity):
            raise LearningError('材料编号无效。')
        item = self.learning._get('sharing_material', identity) or {}
        if item.get('owner') != owner:
            raise LearningError('材料不存在或不属于当前创建人。', 403)
        path = Path(item.get('path', '')).resolve()
        if not path.is_relative_to(self.root) or not path.is_file():
            raise LearningError('原材料已不可用，请重新上传。', 404)
        return {'id': identity, 'title': item['name'], 'sha256': item['sha256'], **self.parse(path, item['sha256'])}

    @staticmethod
    def parse(path, sha256):
        from openclaw_service.assistant.lighthouse_document_tools import run_local
        payload = {'path': str(path), 'sha256': sha256, 'page': 1}
        result = run_local('markdown', payload)
        if path.suffix.lower() == '.pdf' and not result.get('extraction_complete'):
            total = result.get('total_pages', 0)
            pages = {ref['page']: result['text'][ref['start']:ref['end']] for ref in result.get('references', []) if ref.get('kind') == 'page'}
            if type(total) is not int or not 1 <= total <= 100:
                raise LearningError('PDF页数不完整或超过100页，未使用部分材料。')
            low = 0
            for page in range(1, total + 1):
                if page in pages:
                    continue
                recognized = run_local('ocr', {**payload, 'page': page})
                if not recognized.get('text', '').strip():
                    raise LearningError(f'PDF第{page}页OCR未提取到有效正文，未使用不完整材料。')
                pages[page] = f'[page {page}]\n' + recognized['text']
                low += recognized.get('low_confidence_count', 0)
            texts, references, offset = [], [], 0
            for page in range(1, total + 1):
                text = pages[page]
                texts.append(text)
                references.append({'kind': 'page', 'page': page, 'start': offset, 'end': offset + len(text)})
                offset += len(text) + 1
            result.update(text='\n'.join(texts), references=references,
                          extraction_complete=True, low_confidence_count=low)
        if not result.get('extraction_complete') or result.get('empty') and not result.get('text', '').strip():
            raise LearningError('材料未完整提取到可读正文，不能据此出题。')
        if not result.get('text', '').strip() or len(result['text']) > 100000:
            raise LearningError('材料正文为空或过大，请拆分后上传。')
        return {'text': result['text'],
                'extraction_complete': True, 'references': result.get('references', []),
                'low_confidence_count': result.get('low_confidence_count', 0), 'read_at': stamp()}


def combine_sources(sources):
    text = '\n\n'.join(source['text'] for source in sources)
    if len(text) > 24000:
        raise LearningError('合并材料超过24000字出题上限，请拆分批次；未截断材料。')
    return {'text': text, 'sources': sources, 'extraction_complete': True}
