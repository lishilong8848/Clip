"""Offline schema migration tests. All stores are temporary; no real sends."""
import copy
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from types import SimpleNamespace
import unittest
import uuid
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.feishu_identity_documents import (
    IdentityMigrationConflict, TARGET_APP_ID, transform_document,
)

PREVIOUS = 'ou_fixturePrevious'
CURRENT = 'ou_fixtureCurrent'
OTHER = 'ou_fixtureUnmapped'
PREVIOUS_APP = 'cli_fixturePrevious'
MAPPING = {PREVIOUS: CURRENT}


def digest(parts):
    return hashlib.sha256(json.dumps(parts, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def checkpoint(owner, topic, operation=None):
    if operation is not None:
        return 'research:' + hashlib.sha256(json.dumps([owner, topic, operation]).encode()).hexdigest()
    return 'workspace:v1:' + hashlib.sha256(
        json.dumps([owner, topic], ensure_ascii=True, separators=(',', ':')).encode('ascii')).hexdigest()


def person(identity=PREVIOUS):
    return {'id': 'recFixture', 'name': 'Fixture', 'position': '\u8fd0\u7ef4\u503c\u73ed\u5458',
            'scopes': ['D'], 'building': 'D\u697c', 'selectable': True, 'inactive': False,
            'needs_setup': False, 'disabled_reason': '', 'password_revision': 'fixture-digest-' + PREVIOUS,
            'account_nature': 'VNET', 'login_ids': [identity],
            'feishu_open_ids': {PREVIOUS_APP: identity}, 'assignments': ['D'],
            'password_hash': 'fixture-only-hash', 'employee_no': 'fixture-employee'}


class IdentityDocumentTests(unittest.TestCase):
    def setUp(self):
        self.enterContext(patch('socket.create_connection', side_effect=AssertionError('network forbidden')))
        self.enterContext(patch('socket.socket.connect', side_effect=AssertionError('network forbidden')))

    def transform(self, namespace, key, payload, mapping=None):
        original = copy.deepcopy(payload)
        result = transform_document(namespace, key, payload, MAPPING if mapping is None else mapping)
        self.assertEqual(payload, original, 'pure transformation must not mutate caller input')
        return result

    def test_unknown_namespace_and_unknown_ai_key_are_untouched(self):
        payload = {'owner': PREVIOUS, 'native_history_session': 'cache', 'people': {PREVIOUS: {'id': PREVIOUS}}}
        for namespace, key in [('unknown_module', PREVIOUS), ('lighthouse_ai', 'future:' + PREVIOUS)]:
            with self.subTest(namespace=namespace):
                newkey, result = self.transform(namespace, key, payload)
                self.assertEqual((newkey, result), (key, payload))
                self.assertIsNot(result, payload)
                result['people'].clear()
                self.assertTrue(payload['people'])

    def test_empty_mapping_is_a_noop(self):
        payload = {'owner': PREVIOUS, 'native_history_session': 'cache'}
        self.assertEqual(self.transform('lighthouse_ai', 'conversation:' + PREVIOUS, payload, {}),
                         ('conversation:' + PREVIOUS, payload))

    def test_mapping_rejects_merges_chains_and_invalid_ids(self):
        for mapping in [{PREVIOUS: CURRENT, OTHER: CURRENT}, {PREVIOUS: CURRENT, CURRENT: OTHER},
                        {PREVIOUS: CURRENT, CURRENT: PREVIOUS}, {'': CURRENT}, {PREVIOUS: ' '},
                        {PREVIOUS: CURRENT + ':lane'}, {PREVIOUS: 3}, []]:
            with self.subTest(mapping=mapping), self.assertRaises(ValueError):
                transform_document('unknown', 'k', {}, mapping)

    def test_conversation_owner_and_lanes_change_but_graph_and_history_do_not(self):
        lane = PREVIOUS + ':a' * 2
        payload = {'id': 'archive-1', 'owner': PREVIOUS, 'active_run_id': 'run-1',
            'native_history_session': 'native-cache-old-authority',
            'execution_lanes': {lane: {'owner': PREVIOUS, 'active_run_id': 'run-2', 'busy': True}},
            'turns': [{'id': 'turn-1', 'operation_id': 'op-1', 'actor': {'id': PREVIOUS},
                       'question': PREVIOUS, 'answer': PREVIOUS,
                       'content': {'owner': PREVIOUS}, 'result': {'owner': PREVIOUS},
                       'receipt': {'sender_id': PREVIOUS}}]}
        newkey, result = self.transform('lighthouse_ai', 'conversation:' + PREVIOUS, payload)
        self.assertEqual(newkey, 'conversation:' + CURRENT)
        self.assertEqual(result['owner'], CURRENT)
        self.assertNotIn('native_history_session', result)
        self.assertEqual(result['execution_lanes'][CURRENT + ':a' * 2],
                         {'owner': CURRENT, 'active_run_id': 'run-2', 'busy': True})
        self.assertEqual((result['id'], result['active_run_id']), ('archive-1', 'run-1'))
        turn = result['turns'][0]
        self.assertEqual(turn['actor']['id'], CURRENT)
        for field in ['id', 'operation_id', 'question', 'answer', 'content', 'result', 'receipt']:
            self.assertEqual(turn[field], payload['turns'][0][field])

    def test_model_and_topic_account_keys_only(self):
        for key, expected in [('model:' + PREVIOUS, 'model:' + CURRENT),
                              ('feishu-conversation:' + PREVIOUS, 'feishu-conversation:' + CURRENT),
                              ('conversation:' + PREVIOUS + ':channelHash', 'conversation:' + CURRENT + ':channelHash'),
                              ('topic-conversation:topicHash:' + PREVIOUS, 'topic-conversation:topicHash:' + CURRENT)]:
            with self.subTest(key=key):
                newkey, value = self.transform('lighthouse_ai', key, {'owner': PREVIOUS, 'id': 'stable'})
                self.assertEqual(newkey, expected)
                self.assertEqual(value, {'owner': CURRENT, 'id': 'stable'})

    def test_archive_run_and_plan_keys_and_ids_are_stable(self):
        payload = {'id': 'stable-plan', 'owner': PREVIOUS, 'archive_id': 'archive-1',
                   'conversation_id': 'conv-1', 'run_id': 'run-1', 'operation_id': 'operation-1',
                   'plan': {'owner': PREVIOUS, 'id': 'plan-1', 'status': 'pending'}}
        for namespace in ['lighthouse_messages', 'lighthouse_runs', 'lighthouse_agent_plans', 'lighthouse_files']:
            with self.subTest(namespace=namespace):
                key, result = self.transform(namespace, 'archive-1:operation-1', payload)
                self.assertEqual(key, 'archive-1:operation-1')
                self.assertEqual(result['owner'], CURRENT)
                self.assertEqual(result['plan']['owner'], CURRENT)
                self.assertEqual({k: result[k] for k in payload if k not in {'owner', 'plan'}},
                                 {k: payload[k] for k in payload if k not in {'owner', 'plan'}})
                self.assertEqual(result['plan']['status'], 'pending')

    def test_topic_people_keys_and_grants_without_expansion(self):
        payload = {'id': 'topic-1', 'people': {PREVIOUS: {'id': PREVIOUS, 'role': 'reader', 'scopes': ['D']}},
                   'owners': [PREVIOUS], 'readers': [PREVIOUS], 'writers': [], 'eligible': [OTHER]}
        key, result = self.transform('lighthouse_topics', 'topic-1', payload)
        self.assertEqual(key, 'topic-1')
        self.assertEqual(result['people'], {CURRENT: {'id': CURRENT, 'role': 'reader', 'scopes': ['D']}})
        self.assertEqual((result['owners'], result['readers'], result['writers'], result['eligible']),
                         ([CURRENT], [CURRENT], [], [OTHER]))

    def test_dictionary_and_lane_collisions_raise_instead_of_merging(self):
        for field, values in [('users', {PREVIOUS: {}, CURRENT: {}}),
                              ('people', {PREVIOUS: {}, CURRENT: {}}),
                              ('execution_lanes', {PREVIOUS + ':lane': {}, CURRENT + ':lane': {}})]:
            with self.subTest(field=field), self.assertRaises(IdentityMigrationConflict):
                self.transform('lighthouse_ai', 'conversation:' + PREVIOUS, {field: values})

    def test_layout_and_appearance_preserve_all_visible_values(self):
        key = json.dumps([PREVIOUS, 'mobile', 'chat'], separators=(',', ':'))
        payload = {'revision': 4, 'value': {'widgets': [{'id': PREVIOUS, 'title': PREVIOUS}]}}
        newkey, result = self.transform('workspace_layout', key, payload)
        self.assertEqual(json.loads(newkey), [CURRENT, 'mobile', 'chat'])
        self.assertEqual(result, payload)
        self.assertEqual(self.transform('lighthouse_appearance', PREVIOUS, payload), (CURRENT, payload))
        self.assertEqual(self.transform('workspace_layout', 'invalid-json', payload), ('invalid-json', payload))

    def test_real_personnel_snapshot_promotes_verified_identity_and_keeps_password_properties(self):
        empty = {**person(), 'id': 'recNoLogin', 'login_ids': [], 'feishu_open_ids': {}}
        payload = {'schema': 3, 'app_id': PREVIOUS_APP, 'loaded_at': 1000,
                   'people': {'recFixture': person(), 'recNoLogin': empty},
                   'removed_identities': {PREVIOUS: 'blocked reason'}}
        key, result = self.transform('personnel_password_login', 'directory-table', payload)
        self.assertEqual(key, 'directory-table')
        self.assertEqual((result['schema'], result['app_id'], result['loaded_at']), (4, TARGET_APP_ID, 1000))
        self.assertEqual(result['people']['recFixture']['login_ids'], [CURRENT])
        self.assertEqual(result['removed_identities'], {CURRENT: 'blocked reason'})
        for rid, original in payload['people'].items():
            migrated = result['people'][rid]
            self.assertNotIn('feishu_open_ids', migrated)
            self.assertEqual({k: v for k, v in migrated.items() if k != 'login_ids'},
                             {k: v for k, v in original.items() if k not in {'login_ids', 'feishu_open_ids'}})

    def test_unmapped_old_directory_identity_does_not_claim_target_app(self):
        payload = {'schema': 3, 'app_id': PREVIOUS_APP,
                   'people': {'recFixture': person(), 'recUnknown': {**person(OTHER), 'id': 'recUnknown'}}}
        _, result = self.transform('personnel_password_login', 'table', payload)
        self.assertEqual((result['schema'], result['app_id']), (3, PREVIOUS_APP))
        self.assertFalse(result['identity_migration']['directory_verified'])
        self.assertEqual(result['people']['recUnknown']['login_ids'], [OTHER])

    def test_shared_hr_rows_keep_every_record_and_existing_disabled_state(self):
        people = {f'recFixture{index}': {**person(), 'id': f'recFixture{index}', 'selectable': False,
                    'disabled_reason': 'duplicate identity'} for index in range(9)}
        _, result = self.transform('personnel_password_login', 'directory', {'schema': 3, 'people': people})
        self.assertEqual(set(result['people']), set(people))
        self.assertEqual(result['schema'], 4)
        for rid, migrated in result['people'].items():
            self.assertEqual(migrated['id'], rid)
            self.assertEqual(migrated['login_ids'], [CURRENT])
            self.assertFalse(migrated['selectable'])
            self.assertEqual(migrated['password_revision'], people[rid]['password_revision'])

    def test_learning_person_only_changes_login_ids_not_stable_hr_identity(self):
        original = {**person(), 'person_id': 'recFixture', 'active': True, 'aliases': ['stable-alias']}
        key, result = self.transform('learning_person', 'recFixture', original)
        self.assertEqual(key, 'recFixture')
        self.assertEqual(result['login_ids'], [CURRENT])
        self.assertNotIn('feishu_open_ids', result)
        for field in ['id', 'person_id', 'active', 'aliases', 'password_revision', 'scopes']:
            self.assertEqual(result[field], original[field])
        self.assertNotIn('schema', result)
        self.assertNotIn('app_id', result)

    def test_pre_shift_roster_root_list_maps_records_not_text(self):
        people = [{'id': 'recFixture', 'open_id': PREVIOUS, 'scope': 'D', 'name': PREVIOUS}]
        key, result = self.transform('rosters', '2026-10-20', people)
        self.assertEqual(key, '2026-10-20')
        self.assertEqual(result, [{'id': 'recFixture', 'open_id': CURRENT, 'scope': 'D', 'name': PREVIOUS}])

    def test_pre_shift_task_people_items_answers_evidence_and_fixed_recipients(self):
        payload = {'people': [{'open_id': PREVIOUS}],
                   'items': [{'id': 'item-1', 'text': PREVIOUS, 'fixed_recipients': [PREVIOUS], 'recipient_ids': [PREVIOUS]}],
                   'answers': [{'id': 'item-1', 'actor_open_id': PREVIOUS, 'text': PREVIOUS}],
                   'evidence': {'owner': PREVIOUS, 'content': {'owner': PREVIOUS}}, 'submitted': 'unchanged'}
        _, result = self.transform('tasks', 'task-1', payload)
        self.assertEqual(result['people'], [{'open_id': CURRENT}])
        self.assertEqual(result['items'], [{'id': 'item-1', 'text': PREVIOUS,
            'fixed_recipients': [CURRENT], 'recipient_ids': [CURRENT]}])
        self.assertEqual(result['answers'], [{'id': 'item-1', 'actor_open_id': CURRENT, 'text': PREVIOUS}])
        self.assertEqual(result['evidence'], {'owner': CURRENT, 'content': {'owner': PREVIOUS}})
        for field in ['people', 'items', 'answers', 'evidence']:
            with self.subTest(column=field):
                self.assertEqual(self.transform('tasks', 'task-1', payload[field])[1], result[field])

    def test_risk_brief_identity_is_not_an_object_id(self):
        payload = {'id': PREVIOUS, 'open_id': PREVIOUS, 'scopes': ['D'], 'revoked': True,
                   'items': [{'id': PREVIOUS, 'text': PREVIOUS}]}
        _, result = self.transform('risk_people', 'release-1', payload)
        self.assertEqual((result['id'], result['open_id']), (CURRENT, CURRENT))
        self.assertEqual(result['items'], payload['items'])
        self.assertTrue(result['revoked'])
        release = {'id': 'release-1', 'owner': {'id': PREVIOUS, 'open_id': PREVIOUS},
                   'reviewers': [{'id': PREVIOUS}], 'final_managers': [{'id': PREVIOUS, 'scopes': ['D']}]}
        _, result = self.transform('risk_releases', 'release-1', release)
        self.assertEqual(result['owner'], {'id': CURRENT, 'open_id': CURRENT})
        self.assertEqual(result['reviewers'], [{'id': CURRENT}])
        self.assertEqual(result['final_managers'], [{'id': CURRENT, 'scopes': ['D']}])

    def test_risk_job_account_references_change_without_rekeying_queued_job(self):
        key = 'review_notice:release:' + PREVIOUS
        payload = {'person_id': PREVIOUS, 'subject_id': PREVIOUS, 'status': 'queued',
                   'person': {'id': PREVIOUS, 'open_id': PREVIOUS}, 'operation_id': 'original-op',
                   'content': {'person_id': PREVIOUS}, 'receipt': {'subject_id': PREVIOUS}}
        newkey, result = self.transform('risk_jobs', key, payload)
        self.assertEqual(newkey, key)
        self.assertEqual((result['person_id'], result['subject_id']), (CURRENT, CURRENT))
        self.assertEqual(result['person'], {'id': CURRENT, 'open_id': CURRENT})
        for field in ['status', 'operation_id', 'content', 'receipt']:
            self.assertEqual(result[field], payload[field])

    def test_dedicated_namespaces_and_known_identity_columns(self):
        fields = ['open_id', 'created_by_openid', 'actor_open_id', 'signer_open_id', 'approver_open_id',
                  'requested_by_openid', 'created_by_open_id', 'submitted_by_open_id', 'updated_by_open_id',
                  'uploaded_by_open_id', '_auth_open_id']
        for namespace in ['auth_permissions', 'auth_sessions', 'permission_requests', 'notice_action_job',
                          'notice_upload_attachments', 'qt_active_cache', 'qt_notice_history', 'runtime_task_queue',
                          'risk_people', 'rosters']:
            with self.subTest(namespace=namespace):
                key, result = self.transform(namespace, 'stable-key', dict.fromkeys(fields, PREVIOUS))
                self.assertEqual(key, 'stable-key')
                self.assertEqual(result, dict.fromkeys(fields, CURRENT))

    def test_wrapped_queued_requests_migrate_without_changing_uuid_or_status(self):
        request = {'_auth_open_id': PREVIOUS, 'user': {'open_id': PREVIOUS}, 'operation_id': 'uuid4-stable',
                   'text': PREVIOUS, 'content': {'owner': PREVIOUS}, 'status': 'queued'}
        for wrapper in ['request', 'retry_request', 'payload', 'prepared', 'kwargs']:
            with self.subTest(wrapper=wrapper):
                _, result = self.transform('notice_action_job', 'job-1', {wrapper: request})
                migrated = result[wrapper]
                self.assertEqual(migrated['_auth_open_id'], CURRENT)
                self.assertEqual(migrated['user'], {'open_id': CURRENT})
                self.assertEqual(migrated['operation_id'], 'uuid4-stable')
                self.assertEqual(migrated['status'], 'queued')
                self.assertEqual(migrated['text'], PREVIOUS)
                self.assertEqual(migrated['content'], request['content'])

    def test_legacy_permissions_users_rekey_preserves_disabled_and_grants(self):
        grants = {'enabled': False, 'role': 'building', 'scopes': ['D'], 'modules': [], 'locked': True}
        _, result = self.transform('permissions', 'legacy', {'users': {PREVIOUS: grants}, 'default_scopes': []})
        self.assertEqual(result, {'users': {CURRENT: grants}, 'default_scopes': []})

    def test_reviewer_account_id_maps_without_rewriting_submitter_names_or_internal_ids(self):
        payload = {'reviewer_id': PREVIOUS, 'reviewer_ids': [PREVIOUS],
                   'submitted_by_open_id': PREVIOUS, 'submitted_by': PREVIOUS, 'reviewed_by': PREVIOUS,
                   'id': PREVIOUS, 'person_id': PREVIOUS, 'item_id': PREVIOUS,
                   'result': {'reviewer_id': PREVIOUS}}
        _, result = self.transform('risk_releases', 'release-1', payload)
        for field in ['reviewer_id', 'submitted_by_open_id']:
            self.assertEqual(result[field], CURRENT)
        self.assertEqual(result['reviewer_ids'], [CURRENT])
        for field in ['submitted_by', 'reviewed_by', 'id', 'person_id', 'item_id', 'result']:
            self.assertEqual(result[field], payload[field])

    def test_real_handover_reader_recipient_and_version_files_migrate_without_queue_replay(self):
        from lan_bitable_template_portal.work_handover import Handovers, HandoverError

        with tempfile.TemporaryDirectory() as temporary:
            owner = {'id': OTHER, 'open_id': OTHER, 'name': 'Owner', 'role': 'building',
                     'modules': ['work_handover'], 'scope_rules': ['D']}
            previous = {**owner, 'id': PREVIOUS, 'open_id': PREVIOUS, 'name': 'Recipient'}
            current = {**previous, 'id': CURRENT, 'open_id': CURRENT}
            people = {PREVIOUS: previous, CURRENT: current}
            store = Handovers(Path(temporary) / 'handover.sqlite3', lambda: [], people.get)
            handover_id, item_id, file_id = 'a' * 32, 'b' * 32, 'c' * 32
            draft = store.create(owner, handover_id)
            draft = store.save(owner, handover_id, draft['version'], {
                'title': 'Fixture handover', 'notes': PREVIOUS, 'recipient_id': PREVIOUS,
                'items': [{'id': item_id, 'title': 'Fixture item', 'notes': PREVIOUS,
                           'scope': 'D', 'kind': 'todo', 'source_id': ''}]})
            with store.connect(True) as db:
                db.execute('INSERT INTO work_handover_files VALUES(?,?,?,?,?,?)',
                           (file_id, handover_id, 'fixture.txt', 12, draft['updated_at'], 1))
            # This creates only a local pending outbox row; no sender/worker runs.
            pending = store.action(owner, handover_id, draft['version'], 'send')
            with store.connect(True) as db:
                before = dict(db.execute('SELECT * FROM work_handover_outbox').fetchone())
                original = store._read(db, handover_id)
                db.execute('UPDATE work_handovers SET recipient=? WHERE id=?', (CURRENT, handover_id))
            self.assertEqual(store.list(current)['total'], 1)
            with self.assertRaises(HandoverError) as caught:
                store.get(current, handover_id)
            self.assertEqual(caught.exception.status, 403, 'reproduce migrated SQL but stale JSON')
            with store.connect(True) as db:
                key, payload = self.transform('work_handovers', handover_id, original)
                self.assertEqual(key, handover_id)
                db.execute('UPDATE work_handovers SET payload=? WHERE id=?', (json.dumps(payload), handover_id))
                for row in db.execute('SELECT handover_id,version,payload FROM work_handover_versions').fetchall():
                    key, archive = self.transform('work_handover_versions', row['handover_id'], json.loads(row['payload']))
                    self.assertEqual((key, archive['recipient_id']), (handover_id, CURRENT))
                    db.execute('UPDATE work_handover_versions SET payload=? WHERE handover_id=? AND version=?',
                               (json.dumps(archive), key, row['version']))
                key, queued = self.transform('work_handover_outbox', before['id'], json.loads(before['payload']))
                self.assertEqual((key, queued['recipient_id']), (before['id'], CURRENT))
                db.execute('UPDATE work_handover_outbox SET target=?,payload=? WHERE id=?',
                           (CURRENT, json.dumps(queued), key))
            recovered = Handovers(store.db_path, lambda: [], people.get)
            result = recovered.get(current, handover_id)
            self.assertTrue(result['is_recipient'])
            self.assertFalse(result['is_owner'])
            self.assertEqual((result['id'], result['recipient_id'], result['status'], result['version']),
                             (handover_id, CURRENT, 'pending', pending['version']))
            self.assertEqual(result['notes'], PREVIOUS)
            self.assertEqual(result['items'], original['items'])
            self.assertEqual([file['id'] for file in result['files']], [file_id])
            for actor in [previous, {**current, 'id': 'ou_fixtureStranger'}]:
                with self.subTest(actor=actor['id']), self.assertRaises(HandoverError) as caught:
                    recovered.get(actor, handover_id)
                self.assertEqual(caught.exception.status, 403)
            with self.assertRaises(HandoverError) as caught:
                recovered.get({**current, 'modules': []}, handover_id)
            self.assertEqual(caught.exception.status, 403)
            with recovered.connect() as db:
                after = dict(db.execute('SELECT * FROM work_handover_outbox').fetchone())
                self.assertEqual({k: v for k, v in after.items() if k not in {'target', 'payload'}},
                                 {k: v for k, v in before.items() if k not in {'target', 'payload'}})
                self.assertEqual(after['state'], 'queued')
                self.assertEqual(db.execute('SELECT count(*) FROM work_handover_outbox').fetchone()[0], 1)
                saved = store._read(db, handover_id)
                self.assertEqual(saved['card_token'], original['card_token'])
                self.assertEqual(saved['sent_version'], original['sent_version'])

    def test_people_persons_recipients_identity_lists_and_objects(self):
        payload = {'persons': [{'id': PREVIOUS, 'open_id': PREVIOUS}], 'recipients': [PREVIOUS],
                   'user': {'id': PREVIOUS, 'open_id': PREVIOUS}, 'operator': {'id': PREVIOUS}}
        _, result = self.transform('notice_action_job', 'job', payload)
        self.assertEqual(result, {'persons': [{'id': CURRENT, 'open_id': CURRENT}], 'recipients': [CURRENT],
                                 'user': {'id': CURRENT, 'open_id': CURRENT}, 'operator': {'id': CURRENT}})

    def test_only_data_turn_events_are_structural(self):
        payload = {'events': [{'type': 'data-turn', 'data': {'owner': PREVIOUS, 'answer': PREVIOUS}},
                              {'type': 'text-delta', 'data': {'owner': PREVIOUS}}]}
        _, result = self.transform('lighthouse_runs', 'run', payload)
        self.assertEqual(result['events'][0]['data'], {'owner': CURRENT, 'answer': PREVIOUS})
        self.assertEqual(result['events'][1], payload['events'][1])

    def test_question_card_and_sender_keys_keep_provider_and_ticket_ids(self):
        key, result = self.transform('feishu_question_cards', PREVIOUS + ':topic', {'key': PREVIOUS + ':topic', 'owner': PREVIOUS})
        self.assertEqual((key, result['key']), (CURRENT + ':topic', CURRENT + ':topic'))
        self.assertEqual(self.transform('feishu_question_cards', 'ticket:opaque', {'owner': PREVIOUS})[0], 'ticket:opaque')
        self.assertEqual(self.transform('feishu_assistant_sender', PREVIOUS_APP + ':' + PREVIOUS, {})[0],
                         PREVIOUS_APP + ':' + CURRENT)

    def test_previous_app_question_cards_archive_and_expire_without_forging_provider_ids(self):
        from lan_bitable_template_portal.feishu_assistant_questions import AssistantError, read_question_reply

        payload = {'key': PREVIOUS + ':chat:topic', 'owner': PREVIOUS, 'ticket': 'original-ticket',
                   'token': 'fixture-card-token', 'card_id': 'original-card-id', 'message_id': 'om_original',
                   'app_id': PREVIOUS_APP, 'expires': time.time() + 1800,
                   'conversation_id': 'conv-1', 'operation_id': 'original-op', 'question': PREVIOUS}
        for key in [payload['key'], 'ticket:original-ticket']:
            with self.subTest(key=key):
                newkey, result = self.transform('feishu_question_cards', key, payload)
                self.assertEqual(newkey, CURRENT + ':chat:topic' if key == payload['key'] else key)
                self.assertEqual(result['owner'], CURRENT)
                self.assertEqual((result['status'], result['expires']), ('archived', 0))
                self.assertTrue(result['identity_migration']['archived'])
                self.assertFalse(result['identity_migration']['resume_allowed'])
                self.assertEqual(result['identity_migration']['previous_expires'], payload['expires'])
                for field in ['ticket', 'token', 'card_id', 'message_id', 'app_id', 'conversation_id', 'operation_id', 'question']:
                    self.assertEqual(result[field], payload[field])
                self.assertEqual(self.transform('feishu_question_cards', newkey, result), (newkey, result))
                documents = {result['key']: result, 'ticket:original-ticket': result}
                store = SimpleNamespace(get_document=lambda namespace, identity: documents.get(identity))
                bot = SimpleNamespace(runtime=SimpleNamespace(state_store=store))
                message = {'question_form': {'ticket': 'original-ticket'}, 'chat_id': None}
                with self.assertRaises(AssistantError) as caught:
                    read_question_reply(bot, message, {'id': CURRENT}, {'conversation_id': 'conv-1'})
                self.assertEqual(caught.exception.status, 409)

    def test_verified_research_hash_rekeys_and_preserves_operation(self):
        payload = {'owner': PREVIOUS, 'topic_id': 'topic', 'operation_id': 'op', 'status': 'paused', 'plan': {'id': 'plan'}}
        key, result = self.transform('lighthouse_ai', checkpoint(PREVIOUS, 'topic', 'op'), payload)
        self.assertEqual(key, checkpoint(CURRENT, 'topic', 'op'))
        self.assertEqual(result, {**payload, 'owner': CURRENT})

    def test_explicit_checkpoint_inputs_rekey_but_opaque_checkpoint_is_archived(self):
        inputs = {'owner': PREVIOUS, 'topic_id': 'topic', 'operation_id': 'op'}
        payload = {'identity_migration': {'checkpoint_inputs': inputs}, 'id': 'checkpoint-1'}
        key, result = self.transform('lighthouse_ai', checkpoint(PREVIOUS, 'topic', 'op'), payload)
        self.assertEqual(key, checkpoint(CURRENT, 'topic', 'op'))
        self.assertEqual(result['identity_migration']['checkpoint_inputs']['owner'], CURRENT)
        for opaque in [{'owner': PREVIOUS, 'steps': ['completed']}, {**inputs, 'topic_id': 'wrong'}]:
            with self.subTest(opaque=opaque):
                original_key = checkpoint(PREVIOUS, 'topic', 'op')
                key, result = self.transform('lighthouse_ai', original_key, opaque)
                self.assertEqual(key, original_key)
                self.assertFalse(result['resume_available'])
                self.assertTrue(result['identity_migration']['archived'])
                self.assertFalse(result['identity_migration']['resume_allowed'])
                self.assertEqual(self.transform('lighthouse_ai', key, result), (key, result))

    def test_workspace_hash_distinguishes_none_and_empty_topic_and_preserves_objects(self):
        for topic in [None, '', '\u8bdd\u9898']:
            payload = {'owner': PREVIOUS, 'topic_id': topic,
                       'documents': {'file-1': {'id': 'file-1', 'owner': PREVIOUS, 'content': PREVIOUS}},
                       'packages': {}, 'memories': {}}
            with self.subTest(topic=topic):
                key, result = self.transform('lighthouse_ai', checkpoint(PREVIOUS, topic), payload)
                self.assertEqual(key, checkpoint(CURRENT, topic))
                self.assertEqual(result['documents'], {'file-1': {'id': 'file-1', 'owner': CURRENT, 'content': PREVIOUS}})
        self.assertNotEqual(checkpoint(CURRENT, None), checkpoint(CURRENT, ''))

    def test_completed_message_job_keeps_id_receipt_and_original_uuid(self):
        oldpart, newpart = digest([PREVIOUS, 'text']), digest([CURRENT, 'text'])
        jobid = digest([PREVIOUS, 'original-operation'])
        part = {'status': 'sent', 'message_id': 'om_history', 'sender_app_id': PREVIOUS_APP}
        payload = {'id': jobid, 'owner': PREVIOUS, 'status': 'completed', 'text': PREVIOUS,
                   'fingerprint': 'unchanged', 'recipients': [{'open_id': PREVIOUS}], 'files': [],
                   'parts': {oldpart: part}, 'receipts': {'app_id': PREVIOUS_APP}, 'operation_id': 'original-operation'}
        key, result = self.transform('message_delivery', jobid, payload)
        self.assertEqual((key, result['id'], result['operation_id']), (jobid, jobid, 'original-operation'))
        self.assertEqual(result['owner'], CURRENT)
        self.assertEqual(result['identity_migration']['previous_owner'], PREVIOUS)
        self.assertEqual(result['parts'][newpart], {**part, 'message_uuid': str(uuid.uuid5(uuid.NAMESPACE_URL, jobid + oldpart))})
        for field in ['status', 'receipts', 'text', 'fingerprint']:
            self.assertEqual(result[field], payload[field])

    def test_queued_message_reuploads_without_changing_local_file_or_explicit_uuid(self):
        previous = digest([PREVIOUS, 'file:sha'])
        payload = {'id': 'opaque-job', 'owner': PREVIOUS, 'status': 'queued', 'text': '',
                   'recipients': [{'open_id': PREVIOUS}],
                   'files': [{'sha256': 'sha', 'path': '/original/file', 'file_key': 'old-file-key'}],
                   'parts': {previous: {'status': 'failed', 'message_uuid': 'original-uuid'}}}
        _, result = self.transform('message_delivery', 'opaque-job', payload)
        self.assertEqual(result['files'], [{'sha256': 'sha', 'path': '/original/file'}])
        self.assertEqual(result['parts'][digest([CURRENT, 'file:sha'])]['message_uuid'], 'original-uuid')
        self.assertFalse(result['identity_migration'].get('blocked', False))

    def test_uncertain_and_opaque_message_parts_are_blocked_without_dropping_receipts(self):
        for status, parts in [('sending', {digest([PREVIOUS, 'text']): {'status': 'sending'}}),
                              ('queued', {'unknown-hash': {'status': 'sent', 'message_id': 'om_keep'}})]:
            payload = {'id': 'job', 'owner': PREVIOUS, 'status': status, 'text': 'hello',
                       'recipients': [{'open_id': PREVIOUS}], 'parts': parts}
            with self.subTest(status=status):
                key, result = self.transform('message_delivery', 'job', payload)
                self.assertEqual(key, 'job')
                self.assertTrue(result['identity_migration']['blocked'])
                self.assertEqual(len(result['parts']), len(parts))

    def test_message_recipient_collision_raises(self):
        with self.assertRaises(IdentityMigrationConflict):
            self.transform('message_delivery', 'job', {'owner': PREVIOUS,
                'recipients': [{'open_id': PREVIOUS}, {'open_id': CURRENT}], 'parts': {}})

    def test_workpartner_completed_operation_rekeys_verified_digest_not_receipt(self):
        payload = {'app': PREVIOUS_APP, 'owner': PREVIOUS, 'operation_id': 'stable-uuid4', 'status': 'completed',
                   'fingerprint': 'keep', 'result': {'operation_id': 'stable-uuid4', 'owner': PREVIOUS, 'app': PREVIOUS_APP}}
        key, result = self.transform('feishu_workpartner', 'operation:' + digest([PREVIOUS_APP, PREVIOUS, 'stable-uuid4']), payload)
        self.assertEqual(key, 'operation:' + digest([TARGET_APP_ID, CURRENT, 'stable-uuid4']))
        self.assertEqual((result['app'], result['owner']), (TARGET_APP_ID, CURRENT))
        self.assertEqual(result['identity_migration']['origin_app_id'], PREVIOUS_APP)
        for field in ['status', 'operation_id', 'result', 'fingerprint']:
            self.assertEqual(result[field], payload[field])

    def test_workpartner_unconfirmed_operations_remain_non_executable(self):
        for status in ['running', 'uncertain', 'rejected']:
            payload = {'app': PREVIOUS_APP, 'owner': PREVIOUS, 'operation_id': 'op', 'status': status}
            if status == 'rejected':
                payload['prior_unconfirmed'] = True
            with self.subTest(status=status):
                _, result = self.transform('feishu_workpartner', 'operation:' + digest([PREVIOUS_APP, PREVIOUS, 'op']), payload)
                self.assertEqual(result['status'], 'uncertain')
                self.assertTrue(result['identity_migration']['blocked'])
                self.assertEqual(result['operation_id'], 'op')

    def test_workpartner_explicit_rejection_without_uncertainty_stays_rejected(self):
        payload = {'app': PREVIOUS_APP, 'owner': PREVIOUS, 'operation_id': 'op', 'status': 'rejected'}
        _, result = self.transform('feishu_workpartner', 'operation:' + digest([PREVIOUS_APP, PREVIOUS, 'op']), payload)
        self.assertEqual(result['status'], 'rejected')
        self.assertNotIn('blocked', result['identity_migration'])

    def test_workpartner_resource_owner_lists_and_scope_are_preserved(self):
        payload = {'app': PREVIOUS_APP, 'kind': 'document', 'resource_id': 'doc-1', 'owner': PREVIOUS,
                   'owners': [PREVIOUS], 'readers': [PREVIOUS], 'writers': [], 'scopes': ['D']}
        key, result = self.transform('feishu_workpartner', 'resource:' + digest([PREVIOUS_APP, 'document', 'doc-1']), payload)
        self.assertEqual(key, 'resource:' + digest([TARGET_APP_ID, 'document', 'doc-1']))
        self.assertEqual((result['owner'], result['owners'], result['readers']), (CURRENT, [CURRENT], [CURRENT]))
        self.assertEqual((result['writers'], result['scopes'], result['resource_id']), ([], ['D'], 'doc-1'))

    def test_workpartner_opaque_hash_is_not_invented(self):
        payload = {'app': PREVIOUS_APP, 'owner': PREVIOUS, 'operation_id': 'op', 'status': 'uncertain'}
        key, result = self.transform('feishu_workpartner', 'operation:unknown', payload)
        self.assertEqual((key, result['app']), ('operation:unknown', PREVIOUS_APP))
        self.assertTrue(result['identity_migration']['blocked'])
        self.assertTrue(result['identity_migration']['opaque_key'])

    def test_public_delivery_verified_route_keeps_hash_app_and_receipt(self):
        key = hashlib.sha256(json.dumps(['open_id', PREVIOUS, 'original-uuid']).encode()).hexdigest()
        receipt = {'message_uuid': 'original-uuid', 'sender_app_id': PREVIOUS_APP, 'message_id': 'om_keep'}
        payload = {'status': 'sent', 'owner': PREVIOUS, 'app_id': PREVIOUS_APP, 'receipt': receipt}
        newkey, result = self.transform('feishu_public_delivery', key, payload)
        self.assertEqual((newkey, result['app_id'], result['receipt']), (key, PREVIOUS_APP, receipt))
        metadata = result['identity_migration']
        self.assertEqual((metadata['previous_receive_id'], metadata['new_receive_id'], metadata['message_uuid']),
                         (PREVIOUS, CURRENT, 'original-uuid'))
        self.assertTrue(metadata['route_digest_verified'])
        self.assertEqual(result['owner'], CURRENT)

    def test_public_delivery_without_uuid_retains_opaque_key_and_blocks_uncertain(self):
        key, result = self.transform('feishu_public_delivery', 'opaque', {'status': 'posting', 'receipt': {'message_id': 'om_keep'}})
        self.assertEqual(key, 'opaque')
        self.assertTrue(result['identity_migration']['opaque_key'])
        self.assertTrue(result['identity_migration']['blocked'])
        self.assertNotIn('message_uuid', result['identity_migration'])

    def test_repeated_transform_is_idempotent(self):
        fixtures = [('personnel_password_login', 'table', {'schema': 3, 'people': {'recFixture': person()}}),
                    ('lighthouse_ai', 'conversation:' + PREVIOUS, {'owner': PREVIOUS}),
                    ('permissions', 'legacy', {'users': {PREVIOUS: {'enabled': False}}})]
        for namespace, key, payload in fixtures:
            with self.subTest(namespace=namespace):
                newkey, result = self.transform(namespace, key, payload)
                self.assertEqual(self.transform(namespace, newkey, result), (newkey, result))

    def test_real_portal_auth_get_session_reads_both_migrated_json_columns_offline(self):
        with tempfile.TemporaryDirectory() as temporary, patch.dict(os.environ, {'CLIPFLOW_DATA_DIR': temporary}):
            from lan_bitable_template_portal.portal_auth import PortalAuthManager
            with patch('lan_bitable_template_portal.portal_auth.get_data_file_path',
                       side_effect=lambda name: str(Path(temporary) / name)), patch.object(
                           PortalAuthManager, 'current_feishu_app_id', return_value=TARGET_APP_ID):
                auth = PortalAuthManager()
                try:
                    now = time.time()
                    snapshot = {'schema': 3, 'app_id': PREVIOUS_APP, 'loaded_at': now,
                                'people': {'recFixture': person()}}
                    _, directory = self.transform('personnel_password_login', 'table', snapshot)
                    auth._account_directory_provider = lambda: directory
                    grants = {'enabled': True, 'role': 'building', 'scopes': ['D'], 'locked': False,
                              'access_version': 1, 'level': 'L4', 'modules': ['learning'], 'category': 'duty'}
                    _, permissions = self.transform('permissions', 'legacy', {'users': {PREVIOUS: grants}})
                    auth._save_permissions_locked(permissions)
                    session_id = 'fixture-session-cookie'
                    session_hash = auth._secret_hash(session_id)
                    original = {'user': {'open_id': PREVIOUS, 'name': PREVIOUS}, 'role': 'building',
                                'allowed_scopes': ['D'], 'expires_at': now + 3600, 'created_at_ts': now}
                    auth._state_store.put_auth_session(session_hash, original)
                    with closing(sqlite3.connect(auth._state_store.db_path)) as db, db:
                        user_json, payload_json = db.execute(
                            'SELECT user_json,payload_json FROM auth_sessions WHERE session_hash=?', (session_hash,)).fetchone()
                        user_key, migrated_user = self.transform('auth_sessions', session_hash, json.loads(user_json))
                        payload_key, migrated_payload = self.transform('auth_sessions', session_hash, json.loads(payload_json))
                        self.assertEqual((user_key, payload_key), (session_hash, session_hash))
                        self.assertEqual(migrated_user['open_id'], CURRENT)
                        self.assertEqual(migrated_payload['user']['open_id'], CURRENT)
                        db.execute('UPDATE auth_sessions SET user_json=?,payload_json=? WHERE session_hash=?',
                                   (json.dumps(migrated_user), json.dumps(migrated_payload), session_hash))
                    auth._sessions.clear()
                    session = auth.get_session(session_id)
                    self.assertIsNotNone(session)
                    self.assertEqual(session['user'], {'open_id': CURRENT, 'name': PREVIOUS})
                    self.assertEqual((session['role'], session['allowed_scopes']), ('building', ['D']))
                    self.assertEqual(auth.account_profile(CURRENT)['modules'], ['learning'])
                    self.assertFalse(auth.account_profile(CURRENT)['can_manage_accounts'])
                    self.assertEqual(auth.scopes_for_open_id(PREVIOUS), [])
                    self.assertEqual(auth.current_feishu_open_id(PREVIOUS, TARGET_APP_ID), None)
                    self.assertEqual(auth.current_feishu_open_id(CURRENT, TARGET_APP_ID), CURRENT)
                    with closing(sqlite3.connect(auth._state_store.db_path)) as db:
                        self.assertEqual(db.execute('SELECT count(*) FROM auth_sessions').fetchone()[0], 1)
                    permissions['users'][CURRENT]['enabled'] = False
                    auth._save_permissions_locked(permissions)
                    self.assertIsNone(auth.get_session(session_id))
                    self.assertEqual(auth.scopes_for_open_id(CURRENT), [])
                finally:
                    auth.shutdown()
                    auth._cleanup_executor.shutdown(wait=True, cancel_futures=True)
                    auth._state_store.shutdown_write_worker()


if __name__ == '__main__':
    unittest.main()
