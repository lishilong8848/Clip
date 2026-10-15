"""Publish FRPC and LICENSE only; credentials are never part of the archive."""
import argparse
import json
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from public_access.config import SPEC, atomic_json
from public_access.distribution import create_archive


def build_and_publish(source, output, publish=False):
    from openclaw_service.assistant.lighthouse_distribution import FeishuRuntimeMirror, validate_spec
    source, output = Path(source), Path(output)
    version = subprocess.run([str(source / 'frpc.exe'), '--version'], capture_output=True,
        timeout=10, check=True, creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0)).stdout.decode().strip()
    output.mkdir(parents=True, exist_ok=True)
    spec = create_archive(source, output / 'frpc-windows-amd64.zip', version)
    if publish:
        mirror = FeishuRuntimeMirror()
        try:
            previous = json.loads(SPEC.read_text(encoding='utf-8')) if SPEC.is_file() else {}
            journal = output / 'upload-journal.json'
            if previous.get('sha256') == spec['sha256'] and not journal.exists():
                validate_spec(previous, component='frpc')
                atomic_json(journal, {spec['sha256'] + ':' + str(p['index']):
                    {'record_id': p['record_id'], 'file_token': p['file_token']} for p in previous['parts']})
            spec = mirror.publish(spec, output, journal, component='frpc')
        finally:
            mirror.close()
        atomic_json(SPEC, spec)
    atomic_json(output / 'frpc-distribution.json', spec)
    print('[FRPC] %s: version=%s, parts=%s, %.1f MiB; no configuration or credentials included.' %
          ('Published and verified' if publish else 'Built', version, len(spec['parts']), spec['size'] / 1024 ** 2))
    return spec


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--publish', action='store_true')
    args = parser.parse_args()
    build_and_publish(args.source, args.output, args.publish)
