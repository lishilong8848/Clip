"""Build a deterministic runtime package; publish only to the designated mirror."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lan_bitable_template_portal.lighthouse_distribution import FeishuRuntimeMirror, SPEC, create_archive


def build_and_publish(runtime, output, publish=False):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    package = output / 'lighthouse_openclaw.zip'
    spec = create_archive(runtime, package)
    previous = json.loads(SPEC.read_text(encoding='utf-8')) if SPEC.is_file() else {}
    if previous.get('sha256') == spec['sha256'] and all(part.get('record_id') and part.get('file_token') for part in previous.get('parts', [])):
        print('[LighthouseRuntime] Existing verified dependency mirror reused.')
        return previous
    if publish:
        mirror = FeishuRuntimeMirror()
        try:
            spec = mirror.publish(spec, output, output / 'upload-journal.json')
        finally:
            mirror.close()
        SPEC.write_text(json.dumps(spec, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (output / 'distribution.json').write_text(json.dumps(spec, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print('[LighthouseRuntime] Package verified: %s parts, %.1f MiB.' % (len(spec['parts']), spec['size'] / 1024 ** 2))
    return spec


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--publish', action='store_true')
    args = parser.parse_args()
    build_and_publish(args.runtime, args.output, args.publish)
