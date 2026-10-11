"""Publish only verified BGE model files to the designated dependency table."""
import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from openclaw_service.assistant.lighthouse_distribution import FeishuRuntimeMirror, validate_spec
from openclaw_service.assistant.lighthouse_knowledge_model import SPEC, create_archive


def build_and_publish(model, output, publish=False):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    spec = create_archive(model, output / 'bge-small-zh-v1.5.zip')
    previous = json.loads(SPEC.read_text(encoding='utf-8')) if SPEC.is_file() else {}
    if publish:
        mirror = FeishuRuntimeMirror()
        try:
            if previous.get('sha256') == spec['sha256']:
                validate_spec(previous, component='knowledge_model')
                # Reuse verified record/token pairs after a publisher restart.
                journal = output / 'upload-journal.json'
                if not journal.exists():
                    journal.write_text(json.dumps({spec['sha256'] + ':' + str(p['index']):
                        {'record_id': p['record_id'], 'file_token': p['file_token']} for p in previous['parts']}), encoding='utf-8')
            spec = mirror.publish(spec, output, output / 'upload-journal.json', component='knowledge_model')
        finally:
            mirror.close()
        temporary = SPEC.with_suffix('.tmp')
        temporary.write_text(json.dumps(spec, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        temporary.replace(SPEC)
    (output / 'knowledge-model.json').write_text(json.dumps(spec, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print('[KnowledgeModel] %s: %s parts, %.1f MiB.' % ('Published and verified' if publish else 'Built', len(spec['parts']), spec['size'] / 1024**2))
    return spec


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--publish', action='store_true')
    args = parser.parse_args()
    build_and_publish(args.model, args.output, args.publish)
