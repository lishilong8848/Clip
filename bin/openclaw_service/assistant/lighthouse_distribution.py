"""Verified offline runtime packages and the explicitly designated Feishu mirror."""
import hashlib
import json
import os
import re
import shutil
import stat
import threading
import uuid
import zipfile
from pathlib import Path
from urllib.parse import quote, urljoin, urlsplit

from .lighthouse_ai import AssistantError
from .lighthouse_startup_log import emit as startup_log

BASE = 'H7DLbpdQiaIw4ssg5sCcSoGMn5M'
TABLE = 'tblUECEItaUpOWvd'
PATCH_TABLE = 'tblhc8hUnGsjiNRY'
API = 'https://open.feishu.cn/open-apis/bitable/v1/apps/' + BASE + '/tables/' + TABLE
PART_BYTES = 14 * 1024 * 1024
INSTALL_LOCK = threading.Lock()
SPEC = Path(__file__).parent / 'openclaw/distribution.json'
FIELDS = {'组件名称': 1, '版本': 1, '平台': 1, '安装包': 17, '分片序号': 2, '分片总数': 2,
          '分片SHA256': 1, '整包SHA256': 1, '文件大小': 2, '启用': 7, '更新说明': 1}
OMITTED = {'node_modules/@anthropic-ai/claude-agent-sdk-win32-x64/claude.exe'}


def validate_spec(spec, *, published=True, component='runtime'):
    if component == 'portal_patch':
        version_ok = isinstance(spec, dict) and spec.get('component') == component \
            and isinstance(spec.get('version'), str) and 0 < len(spec['version']) <= 120 \
            and spec.get('platform') == 'windows_amd64'
        max_size = 1024 ** 3
    elif component == 'frpc':
        version_ok = isinstance(spec, dict) and spec.get('component') == 'frpc' \
            and isinstance(spec.get('version'), str) and re.fullmatch(r'\d+\.\d+\.\d+', spec['version']) \
            and spec.get('platform') == 'windows_amd64'
        max_size = 64 * 1024 ** 2
        files = spec.get('files') if isinstance(spec, dict) else None
        if not isinstance(files, dict) or set(files) != {'frpc.exe', 'LICENSE'} or any(
                not isinstance(value, str) or not re.fullmatch(r'[0-9a-f]{64}', value) for value in files.values()):
            raise AssistantError('公网组件文件清单无效。', 503)
    elif component in {'public_access_profile', 'photo_location_profile'}:
        version_ok = isinstance(spec, dict) and spec.get('component') == component \
            and spec.get('version') == spec.get('sha256') and spec.get('platform') == 'windows_amd64' \
            and isinstance(spec.get('parts'), list) and len(spec['parts']) == 1
        max_size = 8192
    elif component == 'knowledge_model':
        from .lighthouse_knowledge_model import REVISION
        version_ok = isinstance(spec, dict) and spec.get('component') == component and spec.get('version') == REVISION
        max_size = 128 * 1024 ** 2
    else:
        from .lighthouse_runtime import PIN
        version_ok = component == 'runtime' and isinstance(spec, dict) and spec.get('component', 'runtime') == 'runtime' \
            and spec.get('version') == PIN['openclaw_version'] and spec.get('node_version') == PIN['node_version']
        max_size = 1024 ** 3
    integer = lambda value: type(value) is int
    digest = lambda value: isinstance(value, str) and re.fullmatch(r'[0-9a-f]{64}', value)
    table = PATCH_TABLE if component == 'portal_patch' else TABLE
    if not isinstance(spec, dict) or spec.get('format') != 1 or spec.get('base') != BASE or spec.get('table') != table \
            or not version_ok \
            or not digest(spec.get('sha256')) or not integer(spec.get('size')) or not 0 < spec['size'] <= max_size \
            or not isinstance(spec.get('parts'), list) or not 1 <= len(spec['parts']) <= 74:
        raise AssistantError('助手依赖分发清单无效。', 503)
    for index, part in enumerate(spec['parts'], 1):
        if not isinstance(part, dict) or not integer(part.get('index')) or part['index'] != index \
                or not integer(part.get('size')) or not 0 < part['size'] <= PART_BYTES or not digest(part.get('sha256')):
            raise AssistantError('助手依赖分片清单无效。', 503)
        if published and any(not isinstance(part.get(key), str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,200}', part[key])
                             for key in ('record_id', 'file_token')):
            raise AssistantError('助手依赖镜像尚未发布。', 503)
    if sum(part['size'] for part in spec['parts']) != spec['size']:
        raise AssistantError('助手依赖分片大小不一致。', 503)
    return spec


def checksum(file):
    with Path(file).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def create_archive(runtime, output):
    runtime, output = Path(runtime).resolve(), Path(output).resolve()
    pin = json.loads((Path(__file__).parent / 'openclaw/runtime.json').read_text(encoding='utf-8'))
    ready = json.loads((runtime / 'runtime-ready.json').read_text(encoding='utf-8'))
    if ready['openclaw_version'] != pin['openclaw_version'] or ready['node_version'] != pin['node_version']:
        raise ValueError('Unverified runtime version.')
    output.parent.mkdir(parents=True, exist_ok=True)
    entries = [path for path in runtime.rglob('*') if path.is_file() and
               (path.relative_to(runtime).parts[0] == 'node_modules' or
                path.relative_to(runtime).as_posix() in {'package.json', 'package-lock.json', 'runtime-ready.json',
                    'node-v' + pin['node_version'] + '-win-x64/node.exe', 'node-v' + pin['node_version'] + '-win-x64/LICENSE'})]
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED, compresslevel=3) as bundle:
        for path in sorted(entries):
            if not path.resolve().is_relative_to(runtime):
                raise ValueError('Runtime link escapes package.')
            name = path.relative_to(runtime).as_posix()
            # The app uses API models only; no CLI-agent execution is permitted.
            if name in OMITTED or name.endswith('.pdb'):
                continue
            info = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            if name == 'runtime-ready.json':
                bundle.writestr(info, json.dumps({key: ready[key] for key in ('node_version', 'openclaw_version', 'node_sha256')}, sort_keys=True))
            else:
                with path.open('rb') as source, bundle.open(info, 'w') as destination:
                    shutil.copyfileobj(source, destination, 1024 * 1024)
    spec = {'format': 1, 'version': pin['openclaw_version'], 'node_version': pin['node_version'],
            'sha256': checksum(output), 'size': output.stat().st_size, 'base': BASE, 'table': TABLE, 'parts': []}
    with output.open('rb') as source:
        index = 1
        while content := source.read(PART_BYTES):
            file = output.with_name(output.name + '.%03d' % index)
            file.write_bytes(content)
            spec['parts'].append({'index': index, 'size': len(content), 'sha256': hashlib.sha256(content).hexdigest(), 'name': file.name})
            index += 1
    return spec


class FeishuRuntimeMirror:
    """Only the designated dependency and patch tables are allowed; no record deletion."""
    def __init__(self, *, client=None, headers=None, table=TABLE, timeout=None):
        if table not in {TABLE, PATCH_TABLE}:
            raise AssistantError('未授权的分发表。', 403)
        self.table = table
        self.api = 'https://open.feishu.cn/open-apis/bitable/v1/apps/' + BASE + '/tables/' + table
        if client is None:
            from upload_event_module.services.feishu_token_manager import FeishuTokenError, token_manager
            from upload_event_module.services.http_client import FeishuHttpClient
            try:
                token = token_manager.get_tenant_token()
            except FeishuTokenError as exc:
                startup_log('runtime_auth_failed')
                raise AssistantError('助手依赖镜像认证失败：' + str(exc), 503) from None
            if not token:
                raise AssistantError('飞书依赖镜像认证不可用。', 503)
            client, headers = FeishuHttpClient(timeout=timeout or 60, retries=1), {'Authorization': 'Bearer ' + token}
        self.client, self.headers = client, headers
        self.timeout = timeout
        self._download_client = None

    def request(self, method, suffix='', **kwargs):
        value = self.client.request_json(method, self.api + suffix, headers=self.headers, **kwargs)
        if value.get('code') != 0:
            raise AssistantError('依赖镜像请求未完成（code=' + str(value.get('code')) + '）。', 503)
        return value.get('data') or {}

    def ensure_fields(self):
        listed, page = {}, ''
        while True:
            result = self.request('GET', '/fields', params={'page_size': 100, **({'page_token': page} if page else {})})
            for field in result.get('items', []):
                listed[field['field_name']] = field['type']
            if not result.get('has_more'):
                break
            new_page = result.get('page_token')
            if not new_page or new_page == page:
                raise AssistantError('依赖表字段分页不完整。', 502)
            page = new_page
        for name, kind in FIELDS.items():
            if name in listed and listed[name] != kind:
                raise AssistantError('依赖镜像字段类型不一致：' + name, 409)
            if name not in listed:
                self.request('POST', '/fields', json_payload={'field_name': name, 'type': kind})

    def publish(self, spec, folder, journal, *, component='runtime'):
        validate_spec(spec, published=False, component=component)
        if spec['table'] != self.table:
            raise AssistantError('分发组件与目标表不一致。', 409)
        portal_patch = component == 'portal_patch'
        model = component == 'knowledge_model'
        frpc = component == 'frpc'
        profile = component == 'public_access_profile'
        location = component == 'photo_location_profile'
        self.ensure_fields()
        journal = Path(journal)
        state = json.loads(journal.read_text(encoding='utf-8')) if journal.is_file() else {}
        def save():
            temporary = journal.with_suffix('.tmp')
            temporary.write_text(json.dumps(state, indent=2), encoding='utf-8')
            temporary.replace(journal)
        for part in spec['parts']:
            identity = spec['sha256'] + ':' + str(part['index'])
            entry = state.setdefault(identity, {'client_token': str(uuid.uuid4())})
            save()
            if not entry.get('file_token'):
                path = Path(folder) / part['name']
                if checksum(path) != part['sha256']:
                    raise ValueError('Local runtime part hash mismatch.')
                response = self.client.request_file_json('POST', 'https://open.feishu.cn/open-apis/drive/v1/medias/upload_all',
                    headers=self.headers, file_path=str(path), file_name=part['name'], data={
                        'file_name': part['name'], 'parent_type': 'bitable_file', 'parent_node': BASE, 'size': str(part['size'])})
                if response.get('code') != 0 or not response.get('data', {}).get('file_token'):
                    raise AssistantError('依赖分片上传未完成。', 503)
                entry['file_token'] = response['data']['file_token']
                save()
            fields = {'文本': 'Lighthouse-' + spec['version'] + '-' + str(part['index']),
                '组件名称': '灯塔完整更新补丁' if portal_patch else '灯塔照片地点解析配置（受限）' if location else '灯塔公网部署凭据（受限）' if profile else '公网客户端 FRPC' if frpc else '知识库模型 bge-small-zh-v1.5' if model else '灯塔助手运行环境', '版本': spec['version'], '平台': 'ONNX CPU' if model else 'Windows x64',
                '安装包': [{'file_token': entry['file_token']}], '分片序号': part['index'], '分片总数': len(spec['parts']),
                '分片SHA256': part['sha256'], '整包SHA256': spec['sha256'], '文件大小': part['size'], '启用': True,
                '更新说明': ('完整程序补丁；按分片顺序合并，核对整包大小和SHA256后才可安装。更新保留用户数据与配置。' if portal_patch else '附件含地图服务 Key，仅供灯塔服务端自动配置。请限制表格及附件读取权限，不得公开分享。补丁仅包含下载清单，不含此 Key。' if location else '仅限指定正式部署机使用。附件包含隧道认证信息，请限制表格及附件读取权限，不得分享或转发。' if profile else '仅含 FRPC 及许可证，不含服务器配置、认证令牌或业务数据。' if frpc else '仅含 BGE 中文嵌入模型及分词器，不含公司文档、账号或凭证。' if model
                             else '仅含固定 Node/OpenClaw 依赖，不含账号、凭证、会话或业务数据。')}
            if not entry.get('record_id'):
                result = self.request('POST', '/records', params={'client_token': entry['client_token']}, json_payload={'fields': fields})
                entry['record_id'] = result['record']['record_id']
                save()
            record = self.request('GET', '/records/' + entry['record_id']).get('record') or {}
            values = record.get('fields') or {}
            def text(value):
                return ''.join(item.get('text', '') for item in value) if isinstance(value, list) else value
            if text(values.get('整包SHA256')) != spec['sha256'] or text(values.get('分片SHA256')) != part['sha256'] or not any(
                    item.get('file_token') == entry['file_token'] for item in values.get('安装包', [])):
                raise AssistantError('依赖镜像回读校验未通过。', 503)
            part.update(record_id=entry['record_id'], file_token=entry['file_token'])
        return spec

    def download_part(self, part, target):
        data = self.request('GET', '/records/' + quote(part['record_id'], safe=''))
        record = data.get('record') or {}
        fields = record.get('fields') or {}
        if fields.get('启用') is False:
            raise AssistantError('依赖镜像已停用，未安装。', 503)
        attachments = fields.get('安装包') or []
        if not any(value.get('file_token') == part['file_token'] for value in attachments):
            raise AssistantError('依赖镜像附件已变化，未安装。', 409)
        url = 'https://open.feishu.cn/open-apis/drive/v1/medias/' + quote(part['file_token'], safe='') + '/download'
        # Stream directly; do not buffer an arbitrary remote body into memory.
        import httpx
        from upload_event_module.services.http_client import verified_tls_context
        if self._download_client is None:
            # Feishu attachments must not inherit an unrelated system proxy.
            self._download_client = httpx.Client(
                timeout=self.timeout or httpx.Timeout(connect=10, read=90, write=30, pool=10),
                trust_env=False, follow_redirects=False, verify=verified_tls_context())
        for attempt in range(4):
            # Never forward the API bearer to an attachment CDN.
            headers = self.headers if urlsplit(url).netloc == 'open.feishu.cn' else {}
            with self._download_client.stream('GET', url, headers=headers) as response:
                if response.status_code in {301, 302, 303, 307, 308}:
                    redirected = urljoin(url, response.headers.get('location', ''))
                    parsed = urlsplit(redirected)
                    host = parsed.hostname or ''
                    if not response.headers.get('location') or parsed.scheme != 'https' or parsed.username or parsed.password \
                            or parsed.port not in (None, 443) or not any(host == domain or host.endswith('.' + domain)
                                for domain in ('feishu.cn', 'feishucdn.com', 'feishu-attachment.com')):
                        raise AssistantError('依赖下载跳转不安全，未安装。', 502)
                    url = redirected
                    continue
                if response.status_code != 200:
                    raise AssistantError('依赖分片下载未完成（HTTP ' + str(response.status_code) + '）。', 502)
                size = 0
                with Path(target).open('wb') as output:
                    for chunk in response.iter_bytes(1024 * 1024):
                        size += len(chunk)
                        if size > part['size']:
                            raise AssistantError('依赖分片大小异常，未安装。', 502)
                        output.write(chunk)
                break
        else:
            raise AssistantError('依赖下载跳转次数过多。', 502)
        if size != part['size'] or checksum(target) != part['sha256']:
            raise AssistantError('依赖分片校验失败，未安装。', 502)

    def close(self):
        try:
            self.client.close()
        finally:
            if self._download_client is not None:
                self._download_client.close()
                self._download_client = None


def extract_verified(archive, destination, expected_hash):
    if checksum(archive) != expected_hash:
        raise AssistantError('助手运行环境整包校验失败，未安装。', 502)
    destination = Path(destination).resolve()
    with zipfile.ZipFile(archive) as bundle:
        names, total = set(), 0
        for member in bundle.infolist():
            name = member.filename
            normalized = name.casefold()
            target = (destination / name).resolve()
            total += member.file_size
            parts = name.rstrip('/').split('/')
            if normalized in names or not target.is_relative_to(destination) or '\\' in name or ':' in name or name.startswith('/') \
                    or '\x00' in name or stat.S_ISLNK(member.external_attr >> 16) or total > 3 * 1024 ** 3 \
                    or any(part in {'', '.', '..'} or part.endswith((' ', '.'))
                           or re.fullmatch(r'(CON|PRN|AUX|NUL|COM[1-9]|LPT[1-9])(?:\..*)?', part, re.I) for part in parts):
                raise AssistantError('依赖压缩包包含不安全路径，未安装。', 502)
            names.add(normalized)
        bundle.extractall('\\\\?\\' + str(destination) if os.name == 'nt' and not str(destination).startswith('\\\\?\\') else destination)


def install_runtime(destination, *, mirror_factory=FeishuRuntimeMirror, progress=lambda _: None):
    destination = Path(destination).resolve()
    with INSTALL_LOCK:
        from .lighthouse_runtime import runtime_files
        try:
            runtime_files(destination)
            return destination
        except AssistantError:
            pass
        if not SPEC.is_file():
            raise AssistantError('助手依赖分发清单尚未就绪，请放置运行环境文件夹。', 503)
        try:
            spec = validate_spec(json.loads(SPEC.read_text(encoding='utf-8')))
        except (ValueError, OSError):
            raise AssistantError('助手依赖分发清单无效。', 503) from None
        cache = destination.parent / '.lighthouse_download'
        cache.mkdir(parents=True, exist_ok=True)
        if shutil.disk_usage(cache).free < 3 * 1024 ** 3:
            raise AssistantError('助手依赖安装至少需要3GiB可用空间。', 503)
        archive = cache / (spec['sha256'] + '.zip')
        parts = [cache / (spec['sha256'] + '.%03d' % index) for index in range(1, len(spec['parts']) + 1)]
        if not archive.is_file() or archive.stat().st_size != spec['size'] or checksum(archive) != spec['sha256']:
            mirror = mirror_factory()
            try:
                for part, path in zip(spec['parts'], parts):
                    progress('正在下载助手运行环境 ' + str(part['index']) + '/' + str(len(parts)))
                    if not path.is_file() or path.stat().st_size != part['size'] or checksum(path) != part['sha256']:
                        startup_log('runtime_download', part=part['index'], parts=len(parts))
                        temporary = path.with_suffix(path.suffix + '.tmp')
                        try:
                            mirror.download_part(part, temporary)
                            if temporary.stat().st_size != part['size'] or checksum(temporary) != part['sha256']:
                                raise AssistantError('依赖分片校验失败，未安装。', 502)
                            temporary.replace(path)
                        finally:
                            temporary.unlink(missing_ok=True)
            finally:
                mirror.close()
            with archive.open('wb') as output:
                for path in parts:
                    with path.open('rb') as input_file:
                        shutil.copyfileobj(input_file, output, 1024 * 1024)
        if archive.stat().st_size != spec['size']:
            raise AssistantError('助手依赖整包大小异常。', 502)
        staging = destination.with_name(destination.name + '.staging-' + uuid.uuid4().hex)
        backup = None
        try:
            progress('正在校验并安装助手运行环境')
            startup_log('runtime_extract')
            extract_verified(archive, staging, spec['sha256'])
            runtime_files(staging)
            if destination.exists():
                backup = destination.with_name(destination.name + '.previous-' + uuid.uuid4().hex)
                destination.replace(backup)
            try:
                staging.replace(destination)
            except OSError:
                if backup and not destination.exists():
                    backup.replace(destination)
                raise
        finally:
            if staging.exists() and staging.resolve().parent == destination.parent and staging.name.startswith(destination.name + '.staging-'):
                shutil.rmtree(staging)
        for part in parts:
            part.unlink(missing_ok=True)
        progress('助手运行环境已就绪')
        return destination
