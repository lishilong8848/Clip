"""Publish an explicitly authorized FRP credential attachment, never a release zip."""
import argparse
from dataclasses import asdict, replace
import hashlib
import json
from pathlib import Path
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from public_access.config import CONFIG, DATA, DEPLOYMENT, Settings, atomic_json
from public_access.provisioning import read_deployment, decode_credential


def publish(deployment_host, *, config_path=CONFIG, output=DEPLOYMENT, mirror_factory=None):
    from openclaw_service.assistant.lighthouse_distribution import BASE, TABLE, FeishuRuntimeMirror
    settings = replace(Settings.read(config_path), enabled=True)
    settings.validate_enabled()
    value = {'format': 1, 'deployment_host': deployment_host, 'settings': asdict(settings)}
    content = json.dumps(value, ensure_ascii=False, sort_keys=True).encode('utf-8')
    digest = hashlib.sha256(content).hexdigest()
    part = {'index': 1, 'size': len(content), 'sha256': digest, 'name': 'clipflow-public-profile.json'}
    spec = {'format': 1, 'component': 'public_access_profile', 'version': digest,
            'platform': 'windows_amd64', 'base': BASE, 'table': TABLE,
            'sha256': digest, 'size': len(content), 'parts': [part]}
    public_settings = asdict(settings)
    public_settings.pop('token')
    profile = {'format': 1, 'deployment_host': deployment_host, 'settings': public_settings, 'credential': spec}
    DATA.mkdir(parents=True, exist_ok=True)
    from openclaw_service.protocol import protect_state_directory
    protect_state_directory(DATA)
    with tempfile.TemporaryDirectory(prefix='publish-profile-', dir=DATA) as temp:
        folder = Path(temp)
        candidate = folder / 'deployment.json'
        # Validate before the first cloud write; placeholder identifiers are never published.
        atomic_json(candidate, {**profile, 'credential': {**spec,
            'parts': [{**part, 'record_id': 'unpublished', 'file_token': 'unpublished'}]}})
        validated, _ = read_deployment(candidate)
        decode_credential(content, validated)
        (folder / part['name']).write_bytes(content)
        journal = DATA / 'profile-upload-journal.json'
        if Path(output).is_file() and not journal.exists():
            previous, _ = read_deployment(output)
            if previous['credential']['sha256'] == digest:
                old_part = previous['credential']['parts'][0]
                atomic_json(journal, {digest + ':1': {
                    'record_id': old_part['record_id'], 'file_token': old_part['file_token']}})
        mirror = (mirror_factory or FeishuRuntimeMirror)()
        try:
            mirror.publish(spec, folder, journal, component='public_access_profile')
            mirror.download_part(spec['parts'][0], folder / 'verified.json')
            decode_credential((folder / 'verified.json').read_bytes(), profile)
        finally:
            mirror.close()
    atomic_json(output, profile)
    print('[FRP] Designated host profile published and downloaded for verification; host=' + deployment_host
          + '. Credentials omitted from output and release manifest.')
    return profile


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--deployment-host', required=True)
    parser.add_argument('--publish', action='store_true', required=True)
    args = parser.parse_args()
    try:
        publish(args.deployment_host)
    except Exception as exc:
        print('[FRP] Profile publication failed; error=' + type(exc).__name__, file=sys.stderr)
        raise SystemExit(1)
