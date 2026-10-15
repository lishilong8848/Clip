"""Local deployment configuration; never shipped in a portable update."""
import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

PROJECT = Path(__file__).resolve().parents[2]
DATA = PROJECT / 'bin/data/public_access'
CONFIG = DATA / 'config.json'
SPEC = Path(__file__).with_name('frpc-distribution.json')
DEPLOYMENT = Path(__file__).with_name('deployment.json')
DISABLED = DATA / 'autostart-disabled.json'


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix('.tmp')
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    temporary.replace(path)


@dataclass(frozen=True)
class Settings:
    enabled: bool = False
    public_url: str = ''
    server_addr: str = ''
    server_port: int = 7000
    remote_port: int = 6100
    ingress_port: int = 18767
    token: str = field(default='', repr=False)
    max_requests: int = 24

    @classmethod
    def read(cls, path=CONFIG):
        path = Path(path)
        if not path.is_file():
            return cls()
        return cls.from_value(json.loads(path.read_text(encoding='utf-8-sig')))

    @classmethod
    def from_value(cls, value):
        if not isinstance(value, dict) or set(value) - set(cls.__dataclass_fields__):
            raise ValueError('公网配置字段无效。')
        config = cls(**value)
        if type(config.enabled) is not bool:
            raise ValueError('公网启用状态必须为 true 或 false。')
        for key in ('server_port', 'remote_port', 'ingress_port'):
            port = getattr(config, key)
            if type(port) is not int or not 1024 <= port <= 65535:
                raise ValueError('公网配置端口必须在 1024~65535 之间。')
        if type(config.max_requests) is not int or not 4 <= config.max_requests <= 64:
            raise ValueError('公网并发请求上限必须在 4~64 之间。')
        if not all(isinstance(getattr(config, key), str) for key in ('public_url', 'server_addr', 'token')):
            raise ValueError('公网地址及认证配置格式无效。')
        if config.enabled:
            config.validate_enabled()
        return config

    def validate_enabled(self, *, require_token=True):
        url = urlsplit(self.public_url)
        if url.scheme != 'https' or not url.hostname or url.username or url.password or url.query or url.fragment \
                or url.path not in ('', '/') or url.port not in (None, 443):
            raise ValueError('启用公网前必须配置有效的 HTTPS 域名，不允许 HTTP 或子路径。')
        if not re.fullmatch(r'[A-Za-z0-9.-]+', url.hostname) or '.' not in url.hostname \
                or url.hostname in {'127.0.0.1', 'localhost'}:
            raise ValueError('公网域名无效。')
        if not re.fullmatch(r'[A-Za-z0-9.:-]+', self.server_addr) or require_token and not self.token.strip():
            raise ValueError('公网服务器或认证令牌尚未配置。')

    @property
    def origin(self):
        return self.public_url.rstrip('/').lower()

    def frpc_config(self):
        # Generate a single safe proxy; never import arbitrary plugins/local targets.
        return {
            'serverAddr': self.server_addr, 'serverPort': self.server_port,
            'loginFailExit': False, 'auth': {'method': 'token', 'token': self.token},
            'transport': {'tls': {'enable': True}, 'dialServerTimeout': 10, 'tcpMux': True},
            'log': {'to': 'console', 'level': 'info', 'disablePrintColor': True},
            'proxies': [{'name': 'clipflow-public', 'type': 'tcp', 'localIP': '127.0.0.1',
                         'localPort': self.ingress_port, 'remotePort': self.remote_port}],
        }
