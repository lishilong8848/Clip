"""Read-only public health diagnostic. PyCharm can run this file directly."""
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from public_access.health import Monitor
from public_access.provisioning import read_deployment


def main():
    profile, settings = read_deployment()
    monitor = Monitor(profile['deployment_host'], settings.origin)
    try:
        result = monitor.check_once(notify=False)
        print(json.dumps({k: result[k] for k in ('ok', 'state', 'public_url', 'checked_at', 'reason')}, ensure_ascii=False))
        return 0 if result['ok'] else 1
    finally:
        if monitor.client:
            monitor.client.close()


if __name__ == '__main__':
    raise SystemExit(main())
