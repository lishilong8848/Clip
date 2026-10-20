"""Synthetic local attachments and mocked CLIs only; no Docker/model/cloud calls."""
import base64
import copy
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_ai import AssistantError
from openclaw_service.assistant.lighthouse_files import FILE_NAMESPACE, LighthouseFiles
from openclaw_service.assistant import lighthouse_sandbox as sandbox

ACTOR = {'id': 'sandbox-fixture', 'scopes': ['D', 'E']}
IMAGE = 'sha256:' + 'a' * 64


class Store:
    def __init__(self, path):
        self.db_path, self.docs = path, {}

    def get_document(self, namespace, key):
        return copy.deepcopy(self.docs.get((namespace, key)))

    def put_document(self, namespace, key, value):
        self.docs[namespace, key] = copy.deepcopy(value)


def safe_container():
    return {'Config': {'User': '65534:65534', 'Volumes': None}, 'Mounts': [],
            'HostConfig': {'ReadonlyRootfs': True, 'NetworkMode': 'none', 'PidMode': '',
                'IpcMode': 'none', 'CgroupnsMode': 'private', 'Privileged': False,
                'PidsLimit': 32, 'Memory': 268435456, 'MemorySwap': 268435456,
                'NanoCpus': 1000000000, 'CapDrop': ['ALL'],
                'SecurityOpt': ['no-new-privileges'], 'LogConfig': {'Type': 'none'},
                'Tmpfs': {'/tmp': 'rw,noexec,nosuid,nodev,size=32m,mode=1777'}}}


class SandboxTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.files = LighthouseFiles(Store(self.root / 'state.sqlite3'))
        self.actor = copy.deepcopy(ACTOR)
        self.calls = []
        self.container = safe_container()
        self.engine_info = {'os': 'linux', 'security': ['name=seccomp,profile=builtin'],
                            'memory': True, 'swap': True, 'pids': True, 'cpu': True, 'cgroups': '2'}
        self.image_info = {'id': IMAGE, 'os': 'linux', 'volumes': None}
        self.packet = {'exit_code': 0, 'timed_out': False, 'stdout': 'computed 3\n', 'log_truncated': False,
                       'outputs': [{'name': 'result.csv', 'content': base64.b64encode(b'value\n3\n').decode()}]}
        self.start_hook = None
        self.addCleanup(patch.stopall)
        patch.dict(os.environ, {'LIGHTHOUSE_SANDBOX_IMAGE': IMAGE}).start()
        patch.object(sandbox, '_runtime', return_value=['container-cli', '--host', 'unix:///local.sock']).start()
        self.command = patch.object(sandbox, '_command', side_effect=self.fake_command).start()

    def fake_command(self, runtime, args, **options):
        self.calls.append((args, options))
        if args[0] == 'info':
            return json.dumps(self.engine_info).encode()
        if args[:2] == ['image', 'inspect']:
            return json.dumps(self.image_info).encode()
        if args[0] == 'inspect':
            return json.dumps(self.container).encode()
        if args[0] == 'start':
            if self.start_hook:
                self.start_hook()
            return json.dumps(self.packet).encode()
        return b''

    def upload(self, scopes=None, actor=None):
        return self.files.upload(actor or self.actor, 'source.csv', b'value\n1\n2\n',
                                 extract=False, source_scopes=scopes or ['D'])['id']

    def run_code(self, ids=None):
        return sandbox.run(self.files, self.actor, 'print(1 + 2)', ids or [])

    def test_disabled_without_fixed_image_runtime_or_allowed_override(self):
        for configured in ('', 'python:latest', 'sha256:abc', 'https://image'):
            with self.subTest(configured=configured), patch.dict(os.environ, {'LIGHTHOUSE_SANDBOX_IMAGE': configured}):
                result = self.run_code()
                self.assertFalse(result['available'])
                self.assertIn('不支持', result['message'])
        with patch.object(sandbox, '_runtime', return_value=None):
            self.assertEqual(sandbox.status()['reason'], 'runtime_missing')
        result = sandbox.run(self.files, self.actor, 'pass', [], image='sha256:' + 'b' * 64)
        self.assertEqual(result['reason'], 'image_not_allowed')
        self.command.assert_not_called()

    def test_probe_is_read_only_accepts_local_digest_and_refuses_unsafe_engine_or_image(self):
        with patch.dict(os.environ, {'LIGHTHOUSE_SANDBOX_IMAGE': 'example.invalid/trusted@' + IMAGE}):
            self.assertTrue(sandbox.status()['available'])
        self.assertEqual([args[0] for args, _ in self.calls], ['info', 'image'])
        for change in ({'os': 'windows'}, {'security': []}, {'memory': False},
                       {'swap': False}, {'cpu': False}, {'pids': False}, {'cgroups': '1'}):
            with self.subTest(change=change):
                self.engine_info = {**self.engine_info, **change}
                self.assertFalse(sandbox.status()['available'])
                self.engine_info = {'os': 'linux', 'security': ['seccomp'],
                                    'memory': True, 'swap': True, 'pids': True, 'cpu': True, 'cgroups': '2'}
        for image in ({'id': IMAGE, 'os': 'linux', 'volumes': {'/host': {}}},
                      {'id': IMAGE, 'os': 'windows', 'volumes': None},
                      {'id': 'mutable:tag', 'os': 'linux', 'volumes': None}):
            self.image_info = image
            self.assertFalse(sandbox.status()['available'])

    def test_only_authorized_bytes_reach_stdin_with_no_host_paths_or_user_code_in_argv(self):
        source = self.upload()
        result = self.run_code([source])
        self.assertTrue(result['ok'])
        self.assertEqual(result['outputs'][0]['content'], b'value\n3\n')
        create = next(args for args, _ in self.calls if args[0] == 'create')
        for flag in ('--read-only', '--pull=never', '--network=none', '--cap-drop=ALL',
                     '--security-opt=no-new-privileges', '--pids-limit=32', '--memory=256m',
                     '--memory-swap=256m', '--cpus=1', '--pid=', '--no-healthcheck', '--log-driver=none'):
            self.assertIn(flag, create)
        self.assertFalse(any(arg.startswith(('--mount', '--volume=', '-v=', '--env-file')) for arg in create))
        self.assertNotIn('print(1 + 2)', create)
        self.assertEqual(create[create.index('--entrypoint=/usr/local/bin/python') + 1], IMAGE)
        start = next(options for args, options in self.calls if args[0] == 'start')
        payload = json.loads(start['data'])
        self.assertEqual(base64.b64decode(payload['inputs'][0]['content']), b'value\n1\n2\n')
        self.assertEqual(payload['inputs'][0]['path'], 'input01.csv')
        self.assertNotIn(str(self.root), start['data'].decode())
        self.assertNotIn('owner', payload['inputs'][0])
        self.assertEqual(start['timeout'], sandbox.WALL_SECONDS)
        self.assertEqual(self.calls[-1][0][0], 'rm')
        self.assertEqual(len(self.files.store.docs), 1, 'run must never upload before caller reauthorization')

    def test_input_acl_topic_hash_and_parameter_failures_never_start_container(self):
        identity = self.upload()
        for actor in ({**self.actor, 'id': 'other', 'is_admin': True},
                      {**self.actor, 'scopes': ['A']},
                      {**self.actor, 'channel': 'feishu:test', '_feishu_topic': {'id': 'topic'}}):
            with self.subTest(actor=actor), self.assertRaises(AssistantError):
                sandbox.run(self.files, actor, 'pass', [identity])
        for ids in ([identity, identity], ['../secrets'], [None], 'source.csv', [identity] * 7):
            with self.subTest(ids=ids), self.assertRaises(AssistantError):
                sandbox.run(self.files, self.actor, 'pass', ids)
        for code in ('', ' ' * 10, 'x' * 65537, '\ud800', None):
            with self.assertRaises(AssistantError):
                sandbox.run(self.files, self.actor, code, [])
        Path(self.files.get(self.actor, identity)['path']).write_bytes(b'changed')
        with self.assertRaises(AssistantError):
            self.run_code([identity])
        self.command.assert_not_called()

    def test_input_batch_limit_checked_on_real_bytes(self):
        ids = [self.upload(), self.upload()]
        with patch.object(sandbox, 'MAX_INPUT_BYTES', 10), self.assertRaises(AssistantError) as raised:
            self.run_code(ids)
        self.assertEqual(raised.exception.status, 413)
        self.command.assert_not_called()

    def test_actual_container_is_checked_before_any_payload_is_sent(self):
        changes = [('Config', 'User', 'root'), ('HostConfig', 'ReadonlyRootfs', False),
                   ('HostConfig', 'NetworkMode', 'host'), ('HostConfig', 'PidMode', 'host'),
                   ('HostConfig', 'Privileged', True), ('HostConfig', 'Binds', ['/secret:/secret']),
                   ('HostConfig', 'Memory', 0), ('HostConfig', 'MemorySwap', -1),
                   ('HostConfig', 'NanoCpus', 0), ('HostConfig', 'PidsLimit', 0),
                   ('HostConfig', 'CapAdd', ['SYS_ADMIN']),
                   ('HostConfig', 'CapDrop', []), ('HostConfig', 'SecurityOpt', ['seccomp=unconfined']),
                   ('HostConfig', 'Tmpfs', {'/tmp': 'rw'}),
                   ('HostConfig', 'LogConfig', {'Type': 'json-file'})]
        for section, key, value in changes:
            self.container = safe_container()
            self.container[section][key] = value
            self.calls.clear()
            with self.subTest(key=key), self.assertRaises(AssistantError):
                self.run_code()
            self.assertNotIn('start', [args[0] for args, _ in self.calls])
            self.assertEqual(self.calls[-1][0][0], 'rm')
        self.container = safe_container()
        self.container['Mounts'] = [{'Type': 'volume', 'Destination': '/data'}]
        with self.assertRaises(AssistantError):
            self.run_code()

    def test_malicious_or_oversize_outputs_are_rejected_without_upload(self):
        for name in ('../evil.csv', '/tmp/evil.csv', 'C:\\secret.txt', 'evil:stream.txt',
                     '.hidden.txt', 'unsupported.exe', 'a..b.txt'):
            self.packet['outputs'][0]['name'] = name
            with self.subTest(name=name), self.assertRaises(AssistantError):
                self.run_code()
        self.packet['outputs'][0]['name'] = 'result.csv'
        self.packet['outputs'][0]['content'] = 'invalid base64!'
        with self.assertRaises(AssistantError):
            self.run_code()
        self.packet['outputs'][0]['content'] = base64.b64encode(b'x' * (sandbox.MAX_FILE_BYTES + 1)).decode()
        with self.assertRaises(AssistantError):
            self.run_code()
        self.packet['outputs'] = [{'name': f'{i}.txt', 'content': 'eA=='} for i in range(9)]
        with self.assertRaises(AssistantError):
            self.run_code()
        self.assertEqual(self.files.store.docs, {})

    def test_publish_rechecks_permissions_and_inherits_union_scopes_and_topic(self):
        self.actor.update(channel='feishu:chat', _feishu_topic={'id': 'topic-one'})
        ids = [self.upload(['D']), self.upload(['E'])]
        result = self.run_code(ids)
        with self.assertRaises(AssistantError):
            sandbox.publish(self.files, {**self.actor, 'scopes': ['D']}, result)
        with self.assertRaises(AssistantError):
            sandbox.publish(self.files, {**self.actor, '_feishu_topic': {'id': 'other'}}, result)
        self.assertEqual(len(self.files.store.docs), 2)
        delivered = sandbox.publish(self.files, copy.deepcopy(self.actor), result)
        generated = self.files.get(self.actor, delivered['files'][0]['id'])
        self.assertEqual(generated['source_scopes'], ['D', 'E'])
        self.assertEqual(generated['topic_id'], 'topic-one')
        self.assertFalse(any(key.startswith('_') for key in delivered))
        self.assertNotIn('outputs', delivered)

    def test_revocation_mutation_and_deletion_after_execution_block_publication(self):
        for change in ({'deleted_at': 1}, {'source_scopes': ['E']}, {'sha256': 'changed'}):
            identity = self.upload()
            result = self.run_code([identity])
            original = self.files.store.docs[FILE_NAMESPACE, identity]
            original.update(change)
            before = len(self.files.store.docs)
            with self.subTest(change=change), self.assertRaises(AssistantError):
                sandbox.publish(self.files, self.actor, result)
            self.assertEqual(len(self.files.store.docs), before)
        identity = self.upload()
        result = self.run_code([identity])
        Path(self.files.get(self.actor, identity)['path']).write_bytes(b'changed')
        with self.assertRaises(AssistantError):
            sandbox.publish(self.files, self.actor, result)

    def test_revocation_during_publish_tombstones_only_new_generated_records(self):
        identity = self.upload()
        original = copy.deepcopy(self.files.store.docs[FILE_NAMESPACE, identity])
        result = self.run_code([identity])
        result['outputs'].append({'name': 'second.csv', 'content': b'fixture'})
        upload, generated = self.files.upload, []

        def revoke(*args, **options):
            attachment = upload(*args, **options)
            generated.append(attachment['id'])
            self.files.store.docs[FILE_NAMESPACE, identity]['deleted_at'] = 1
            return attachment

        with patch.object(self.files, 'upload', side_effect=revoke), self.assertRaises(AssistantError):
            sandbox.publish(self.files, self.actor, result)
        self.assertEqual(len(generated), 1)
        self.assertIn('deleted_at', self.files.store.docs[FILE_NAMESPACE, generated[0]])
        with self.assertRaises(AssistantError):
            self.files.get(self.actor, generated[0])
        self.assertEqual(self.files.store.docs[FILE_NAMESPACE, identity], {**original, 'deleted_at': 1})

    def test_upload_failure_rolls_back_prior_outputs_and_rollback_failure_is_explicit(self):
        identity = self.upload()
        result = self.run_code([identity])
        result['outputs'].append({'name': 'second.csv', 'content': b'fixture'})
        upload, generated = self.files.upload, []

        def fail(*args, **options):
            if generated:
                raise AssistantError('fixture upload failure', 503)
            attachment = upload(*args, **options)
            generated.append(attachment['id'])
            return attachment

        with patch.object(self.files, 'upload', side_effect=fail), self.assertRaises(AssistantError):
            sandbox.publish(self.files, self.actor, result)
        self.assertIn('deleted_at', self.files.store.docs[FILE_NAMESPACE, generated[0]])
        self.assertNotIn('deleted_at', self.files.store.docs[FILE_NAMESPACE, identity])
        generated.clear()
        put = self.files.store.put_document

        def broken_rollback(namespace, key, record):
            if 'deleted_at' in record:
                raise OSError('PRIVATE_STORAGE_DIAGNOSTIC')
            return put(namespace, key, record)

        with patch.object(self.files, 'upload', side_effect=fail), patch.object(self.files.store, 'put_document', side_effect=broken_rollback):
            with self.assertRaises(AssistantError) as raised:
                sandbox.publish(self.files, self.actor, result)
        self.assertIn('撤回未确认', str(raised.exception))
        self.assertNotIn('PRIVATE_STORAGE_DIAGNOSTIC', str(raised.exception))

    def test_shared_plan_does_not_allow_merging_different_topics_into_derivatives(self):
        owner = {**self.actor, 'channel': 'feishu:chat', '_feishu_topic': {'id': 'source-topic'}}
        identity = self.upload(actor=owner)
        receiver = {**self.actor, 'id': 'recipient', 'channel': 'feishu:chat',
                    '_feishu_topic': {'id': 'target-topic'},
                    '_shared_plan': {'file_ids': [identity], 'chat_id': 'chat', 'topic_id': 'target-topic'}}
        self.assertEqual(self.files.get(receiver, identity)['id'], identity)
        with self.assertRaises(AssistantError):
            sandbox.run(self.files, receiver, 'pass', [identity])
        self.command.assert_not_called()

    def test_hostile_packet_and_combined_output_limits_fail_closed(self):
        for content in (b'[]', b'{}', b'[' * 2000 + b']' * 2000,
                        json.dumps({**self.packet, 'exit_code': True}).encode(),
                        json.dumps({**self.packet, 'timed_out': 'yes'}).encode()):
            with self.subTest(content=content[:30]), self.assertRaises(AssistantError):
                sandbox._decode_packet(content)
        self.packet['outputs'] = [{'name': f'result{i}.csv', 'content': 'eA=='} for i in range(3)]
        with patch.object(sandbox, 'MAX_OUTPUT_BYTES', 2), self.assertRaises(AssistantError):
            self.run_code()
        self.packet['outputs'][1]['name'] = 'RESULT0.csv'
        with self.assertRaises(AssistantError):
            self.run_code()

    def test_revocation_during_execution_and_timeout_release_slot_and_remove_container(self):
        identity = self.upload()
        self.start_hook = lambda: self.files.store.docs[FILE_NAMESPACE, identity].update(deleted_at=1)
        with self.assertRaises(AssistantError):
            self.run_code([identity])
        self.start_hook = lambda: (_ for _ in ()).throw(AssistantError('fixture timeout', 408))
        with self.assertRaises(AssistantError) as raised:
            self.run_code()
        self.assertEqual(raised.exception.status, 408)
        self.assertEqual(self.calls[-1][0][0], 'rm')
        self.assertTrue(sandbox._SLOT.acquire(blocking=False))
        sandbox._SLOT.release()

    def test_cleanup_failure_is_reported_and_no_result_is_delivered(self):
        original = self.fake_command

        def command(runtime, args, **options):
            if args[0] == 'rm':
                raise sandbox._RuntimeError('secret diagnostic must not leak')
            if args[0] == 'ps':
                return b'lighthouse-analysis-still-present'
            return original(runtime, args, **options)

        self.command.side_effect = command
        with self.assertRaises(AssistantError) as raised:
            self.run_code()
        self.assertIn('清理未确认', str(raised.exception))
        self.assertNotIn('secret diagnostic', str(raised.exception))

    def test_busy_slot_fails_without_create(self):
        sandbox._SLOT.acquire()
        try:
            with self.assertRaises(AssistantError) as raised:
                self.run_code()
            self.assertEqual(raised.exception.status, 429)
            self.assertNotIn('create', [args[0] for args, _ in self.calls])
        finally:
            sandbox._SLOT.release()

    def test_native_podman_checks_delegated_controllers_and_sets_no_inheritance_flags(self):
        self.engine_info = {'host': {'os': 'linux', 'security': {'seccompEnabled': True},
                                   'cgroupVersion': 'v2', 'cgroupControllers': ['cpu', 'memory', 'pids']}}
        self.container['HostConfig']['CgroupMode'] = 'private'
        self.container['HostConfig'].pop('CgroupnsMode')
        self.container['HostConfig']['CapDrop'] = ['CAP_CHOWN', 'CAP_NET_RAW']
        self.container.update(EffectiveCaps=[], BoundingCaps=[])
        with patch.object(sandbox, '_runtime', return_value=['podman', '--remote=false']):
            self.assertTrue(self.run_code()['ok'])
            create = next(args for args, _ in self.calls if args[0] == 'create')
            self.assertIn('--unsetenv-all', create)
            self.assertIn('--no-hosts', create)
            self.assertIn('--pid=private', create)
            self.container['BoundingCaps'] = ['CAP_SYS_ADMIN']
            with self.assertRaises(AssistantError):
                self.run_code()
            self.engine_info['host']['cgroupControllers'] = ['cpu']
            self.assertFalse(sandbox.status()['available'])

    def test_logs_are_filtered_and_exit_code_is_preserved(self):
        self.packet.update(stdout='password=DO_NOT_RETURN\nsk-FAKEKEY123456\nresult 3', exit_code=1,
                           log_truncated=True)
        result = self.run_code()
        self.assertNotIn('DO_NOT_RETURN', result['stdout'])
        self.assertNotIn('sk-FAKEKEY123456', result['stdout'])
        self.assertIn('result 3', result['stdout'])
        self.assertEqual(result['exit_code'], 1)
        self.assertTrue(result['log_truncated'])

    def test_reported_container_timeout_is_bounded_metadata(self):
        self.packet.update(exit_code=124, timed_out=True, outputs=[])
        result = self.run_code()
        self.assertTrue(result['timed_out'])
        self.assertEqual(result['exit_code'], 124)
        self.assertEqual(sandbox.publish(self.files, self.actor, result)['files'], [])


class CommandBoundaryTests(unittest.TestCase):
    def process(self, stdout=b'', stderr=b''):
        process = Mock()
        process.stdout, process.stderr, process.stdin = io.BytesIO(stdout), io.BytesIO(stderr), io.BytesIO()
        process.returncode = 0
        process.poll.return_value = 0
        return process

    def test_cli_uses_no_shell_sanitizes_environment_and_bounds_output(self):
        process = self.process(stdout=b'123456789')
        with patch.dict(os.environ, {'DOCKER_HOST': 'tcp://remote', 'API_KEY': 'DO_NOT_INHERIT',
                                     'HTTP_PROXY': 'http://private'}), patch.object(sandbox.subprocess, 'Popen', return_value=process) as popen:
            with self.assertRaises(sandbox._RuntimeError):
                sandbox._command(['docker', '--host', 'unix:///local.sock'], ['info'], limit=8)
            options = popen.call_args.kwargs
            self.assertFalse(options['shell'])
            self.assertNotIn('API_KEY', options['env'])
            self.assertNotIn('DOCKER_HOST', options['env'])
            self.assertNotIn('HTTP_PROXY', options['env'])
            self.assertNotIn('DO_NOT_INHERIT', str(popen.call_args))
            process.kill.assert_called_once()

    def test_cli_timeout_kills_process_and_raises_sanitized_error(self):
        process = self.process()
        process.wait.side_effect = [subprocess.TimeoutExpired(['private'], 1), 0]
        process.poll.return_value = None
        with patch.object(sandbox.subprocess, 'Popen', return_value=process), self.assertRaises(AssistantError) as raised:
            sandbox._command(['docker'], ['start'], timeout=.01)
        self.assertEqual(raised.exception.status, 408)
        process.kill.assert_called_once()


if __name__ == '__main__':
    unittest.main()
