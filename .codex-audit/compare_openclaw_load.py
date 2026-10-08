"""Synthetic account acceptance with optional previous plugin/cache configuration."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'bin'), str(ROOT / 'bin/tools')]
from openclaw_service.assistant import lighthouse_runtime as runtime
from openclaw_service.gateway_log import GatewayLog
import check_lighthouse_shared_gateway as fixture


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--baseline', action='store_true')
    args = parser.parse_args()
    import win32api
    import win32process
    win32process.SetPriorityClass(win32api.GetCurrentProcess(), win32process.BELOW_NORMAL_PRIORITY_CLASS)
    samples = []
    build, launch, close = runtime.build_configuration, subprocess.Popen, GatewayLog.close
    with tempfile.TemporaryDirectory(prefix='preload-baseline-') as directory:
        preload = Path(directory) / 'native-paths.mjs'
        if args.baseline:
            _, baseline_entry = runtime.runtime_files()
            preload.write_bytes(subprocess.check_output(['git', 'show',
                'HEAD:bin/openclaw_service/assistant/openclaw/native-paths.mjs'], cwd=ROOT))

        def configuration(*values, **options):
            result = build(*values, **options)
            if args.baseline:
                result.pop('browser', None)
                result.pop('update', None)
                result['models'].pop('catalogRefresh', None)
                result['skills'].pop('load', None)
                result['plugins']['allow'].insert(0, 'openai')
                result['plugins']['entries']['openai'] = {'enabled': True}
                result['plugins']['load']['paths'].append(str(baseline_entry.parent / 'dist/extensions/openai'))
            return result

        def popen(command, *values, **options):
            command = list(command)
            if options.get('env', {}).get('LIGHTHOUSE_SDK_ROOT'):
                if args.baseline:
                    index = command.index('--import') + 1
                    command[index] = preload.as_uri()
                command[1:1] = ['--import', (ROOT / '.codex-audit/runtime-footprint.mjs').as_uri()]
            return launch(command, *values, **options)

        def finish(log):
            close(log)
            if log.path.exists():
                for line in log.path.read_text(encoding='utf8').splitlines():
                    if line.startswith('[RuntimeFootprint] '):
                        sample = json.loads(line.removeprefix('[RuntimeFootprint] '))
                        if sample not in samples:
                            samples.append(sample)

        try:
            with patch.object(runtime, 'build_configuration', configuration), \
                    patch.object(subprocess, 'Popen', popen), patch.object(GatewayLog, 'close', finish):
                asyncio.run(fixture.run(2))
        finally:
            label = 'before' if args.baseline else 'after'
            (ROOT / f'.codex-audit/runtime-footprint-{label}.json').write_text(json.dumps(samples, indent=2), encoding='utf8')
            latest = {sample['pid']: sample for sample in samples}
            print('[Footprint]', label, json.dumps(list(latest.values())), flush=True)


if __name__ == '__main__':
    main()
