"""Configure the designated public host locally; changes apply at next startup."""
import argparse
from dataclasses import asdict
import json
import sys
import tomllib
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from public_access.config import CONFIG, DATA, DISABLED, Settings, atomic_json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--import-frpc', type=Path)
    parser.add_argument('--public-url')
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--enable', action='store_true')
    mode.add_argument('--disable', action='store_true')
    parser.add_argument('--confirm-server-ready', action='store_true',
                        help='Confirm server HTTPS, FRPS port isolation and Feishu callback are configured.')
    parser.add_argument('--status', action='store_true')
    parser.add_argument('--prepare', action='store_true', help='Download and verify FRPC without opening a tunnel.')
    args = parser.parse_args()
    if (args.enable or args.disable) and not (args.import_frpc or args.public_url or args.prepare or args.status):
        if args.enable:
            if not args.confirm_server_ready:
                raise ValueError('Verify server HTTPS, private FRPS port and OAuth callback before enabling.')
            from public_access.provisioning import read_deployment
            read_deployment()
            DISABLED.unlink(missing_ok=True)
        else:
            atomic_json(DISABLED, {'disabled': True})
        print('Designated-host public autostart %s. Applies on next portal start; other hosts cannot start the tunnel.' %
              ('enabled' if args.enable else 'disabled'))
        return
    if args.disable:
        atomic_json(DISABLED, {'disabled': True})
    if args.prepare:
        import tempfile
        from public_access.distribution import install, verify_binary
        settings = Settings.read()
        binary, spec = install(DATA / 'frpc')
        with tempfile.TemporaryDirectory(dir=DATA) as temp:
            config = Path(temp) / 'verify.json'
            atomic_json(config, settings.frpc_config())
            verify_binary(binary, spec, config)
        print('FRPC downloaded and verified. No tunnel was started. Version=' + spec['version'])
        return
    if args.status:
        state = DATA / 'status.json'
        print(state.read_text(encoding='utf-8') if state.is_file() else 'Public access has not been started.')
        return
    value = json.loads(CONFIG.read_text(encoding='utf-8')) if CONFIG.is_file() else asdict(Settings())
    if args.import_frpc:
        with args.import_frpc.open('rb') as stream:
            source = tomllib.load(stream)
        candidates = [p for p in source.get('proxies', []) if p.get('type') == 'tcp' and p.get('localPort') == 18766]
        if len(candidates) != 1:
            raise ValueError('Only a single TCP proxy targeting the portal may be imported.')
        value.update(server_addr=source.get('serverAddr', ''), server_port=source.get('serverPort', 7000),
                     token=(source.get('auth') or {}).get('token', ''), remote_port=candidates[0]['remotePort'],
                     enabled=False)
    if args.public_url:
        value['public_url'] = args.public_url
    if args.enable:
        if not args.confirm_server_ready:
            raise ValueError('Verify server HTTPS, private FRPS port and OAuth callback before enabling.')
        value['enabled'] = True
    elif args.disable:
        value['enabled'] = False
    if not (args.enable or args.disable or args.import_frpc or args.public_url):
        parser.print_help()
        return
    candidate = DATA / 'config.candidate.json'
    try:
        atomic_json(candidate, value)
        Settings.read(candidate)
        candidate.replace(CONFIG)
        if args.enable:
            DISABLED.unlink(missing_ok=True)
    finally:
        candidate.unlink(missing_ok=True)
    print('Public access configuration saved; enabled=%s. Applies on next portal start. Credentials not displayed.' % value['enabled'])


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print('Public access configuration not changed: ' + str(exc), file=sys.stderr)
        raise SystemExit(1)
