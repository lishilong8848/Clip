"""Offline field-contract regressions for program-owned cabinet power sources.

Only the pure helper ``source_field_contract`` and the pure ``query_result_state``
guidance builder are exercised here, with synthetic api_id/source_kind pairs and
synthetic result envelopes; there is no portal instance, service, file, credential or
network call, and outbound socket connections are blocked. These checks describe the
static metadata the helper returns and the guidance string the builder produces. They
do not show what a model does with that guidance, nor that any endpoint was actually
read or permitted.
"""
import inspect
import json
import socket
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from openclaw_service.assistant.lighthouse_queries import query_result_state, source_field_contract

NATIVE = {
    'GET /api/cabinet-power/overview': (['racks[].state'], ['counts', 'rooms[].counts']),
    'GET /api/cabinet-power/rooms': (['racks[].state'], ['counts', 'rooms[].counts']),
    'GET /api/cabinet-power/racks': (['items[].state'], []),
    'GET /api/cabinet-power/rooms/{room_id}/layout': (['racks[].state'], ['summary']),
}
NATIVE_LABELS = {'off': '未上电/已下电', 'test': '测试电', 'formal': '正式电', 'unknown': '待核实'}
LOCAL_KIND = 'local_cabinet_inventory'
LOCAL_PATHS = (['state'], ['counts'])
CONTRACT_KEYS = {'subject', 'count_unit', 'state_fields', 'count_fields',
                 'state_labels', 'state_source_labels', 'relations', 'limits'}
NESTED_KEYS = ('state_fields', 'count_fields', 'state_labels', 'state_source_labels',
               'relations', 'limits')
INVALID_TYPES = ([], {}, 0, False)
INEXACT_IDS = (
    'GET /api/cabinet-power', 'GET /api/cabinet-power/batches',
    'GET /api/cabinet-power/rooms/{room_id}', 'GET /api/cabinet-power/rooms/110/layout',
    'GET /api/workbench', 'GET /api/events/monthly', 'GET /api/learning/completion',
    'POST /api/cabinet-power/racks', 'PUT /api/cabinet-power/racks',
    'PATCH /api/cabinet-power/racks/1', 'DELETE /api/cabinet-power/racks/1',
    'POST /api/cabinet-power/overview', 'PUT /api/cabinet-power/rooms',
    'PATCH /api/cabinet-power/rooms/{room_id}/layout',
    'GET /api/cabinet-power/racks?room_id=1', 'get /api/cabinet-power/racks',
    ' GET /api/cabinet-power/racks', 'GET  /api/cabinet-power/racks',
    'GET /api/cabinet-power/racks/',
)


def native_labels():
    from lan_bitable_template_portal.cabinet_power_batches import POWER_STATE_LABELS
    return POWER_STATE_LABELS


class SourceFieldContractTests(unittest.TestCase):
    def setUp(self):
        # Synthetic metadata only: any outbound connection attempt is a test failure.
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('offline tests only')))
        self.enterContext(patch.object(socket.socket, 'connect_ex', side_effect=AssertionError('offline tests only')))
        self.enterContext(patch.object(socket, 'create_connection', side_effect=AssertionError('offline tests only')))

    def test_native_metadata_shape_and_labels(self):
        for api_id in NATIVE:
            with self.subTest(api_id=api_id):
                contract = source_field_contract(api_id=api_id)
                self.assertIsInstance(contract, dict)
                self.assertEqual(set(contract), CONTRACT_KEYS)
                self.assertEqual(contract['subject'], '机柜台账供电状态')
                # Inventory cabinets, never rooms or operation events.
                self.assertEqual(contract['count_unit'], '机柜')
                self.assertEqual(contract['state_labels'], NATIVE_LABELS)
                self.assertIsInstance(contract['state_source_labels'].get('operation'), str)
                self.assertEqual(set(contract['state_source_labels']),
                                 {'operation', 'baseline', 'unconfirmed', 'empty'})
                for key in ('relations', 'limits'):
                    self.assertTrue(contract[key])
                    self.assertTrue(all(isinstance(item, str) and item.strip() for item in contract[key]))

    def test_native_paths_match_each_exact_endpoint(self):
        for api_id, (state_fields, count_fields) in NATIVE.items():
            with self.subTest(api_id=api_id):
                contract = source_field_contract(api_id=api_id, source_kind=LOCAL_KIND)
                # An exact native API wins over local kind; it is never rewritten.
                self.assertEqual(contract['state_fields'], state_fields)
                self.assertEqual(contract['count_fields'], count_fields)
                self.assertNotEqual(contract['state_fields'], LOCAL_PATHS[0])
                self.assertNotEqual(contract['count_fields'], LOCAL_PATHS[1])

    def test_local_cabinet_inventory_requires_absent_api_id(self):
        for api_id in ('', None):
            with self.subTest(api_id=api_id):
                contract = source_field_contract(api_id=api_id, source_kind=LOCAL_KIND)
                self.assertEqual(set(contract), CONTRACT_KEYS)
                self.assertEqual(contract['state_fields'], LOCAL_PATHS[0])
                self.assertEqual(contract['count_fields'], LOCAL_PATHS[1])
                self.assertEqual(contract['count_unit'], '机柜')
                self.assertEqual(contract['state_labels'], NATIVE_LABELS)
                self.assertNotEqual(contract, source_field_contract(api_id='GET /api/cabinet-power/racks'))

    def test_inexact_identity_and_invalid_api_id_types_are_rejected(self):
        # Absent/invalid/nonexact identities never resolve natively nor via the local kind,
        # except absent identity with the local kind, which is the local inventory source.
        for api_id in ('', None, *INVALID_TYPES, *INEXACT_IDS):
            with self.subTest(api_id=api_id):
                self.assertIsNone(source_field_contract(api_id=api_id))
        for api_id in (*INVALID_TYPES, *INEXACT_IDS):
            with self.subTest(api_id=api_id, source_kind=LOCAL_KIND):
                self.assertIsNone(source_field_contract(api_id=api_id, source_kind=LOCAL_KIND))
        self.assertIsNotNone(source_field_contract(api_id='', source_kind=LOCAL_KIND))

    def test_other_source_kinds_are_rejected(self):
        kinds = ('', None, 'local', 'cabinet_inventory', 'LOCAL_CABINET_INVENTORY',
                 ' local_cabinet_inventory', 'local_cabinet_inventory ',
                 'local_cabinet_inventory_v2', 'local_cabinet_batches', 'native',
                 'snapshot', 'local_assistant_sessions')
        for kind in kinds:
            with self.subTest(source_kind=kind):
                self.assertIsNone(source_field_contract(source_kind=kind))
                self.assertIsNone(source_field_contract(api_id='', source_kind=kind))

    def test_returned_contracts_are_fresh_and_isolated_from_nested_mutation(self):
        cases = [(api_id, '') for api_id in NATIVE] + [('', LOCAL_KIND)]
        for api_id, kind in cases:
            with self.subTest(api_id=api_id, source_kind=kind):
                first = source_field_contract(api_id=api_id, source_kind=kind)
                second = source_field_contract(api_id=api_id, source_kind=kind)
                self.assertIsInstance(first, dict)
                self.assertEqual(first, second)
                self.assertIsNot(first, second)
                for key in NESTED_KEYS:
                    self.assertIsNot(first[key], second[key])
                # Nested mutation of one result must not leak into the next call.
                first['state_fields'].append('mutated')
                first['count_fields'].append('mutated')
                first['state_labels']['formal'] = 'mutated'
                first['state_labels']['mutated'] = 'mutated'
                first['state_source_labels']['operation'] = 'mutated'
                first['relations'].append('mutated')
                first['limits'].append('mutated')
                again = source_field_contract(api_id=api_id, source_kind=kind)
                self.assertEqual(again, second)
                self.assertNotIn('mutated', again['state_fields'] + again['count_fields']
                                 + again['relations'] + again['limits'])
                self.assertEqual(again['state_labels'], NATIVE_LABELS)
                self.assertNotEqual(again['state_source_labels']['operation'], 'mutated')

    def test_native_labels_are_independent_of_the_native_module(self):
        snapshot = dict(native_labels())
        self.assertEqual(snapshot, {k: v for k, v in NATIVE_LABELS.items() if k != 'unknown'})
        contract = source_field_contract(api_id='GET /api/cabinet-power/racks')
        contract['state_labels']['formal'] = 'mutated'
        contract['state_labels']['extra'] = 'mutated'
        self.assertEqual(native_labels(), snapshot)
        self.assertEqual(source_field_contract(api_id='GET /api/cabinet-power/racks')['state_labels'],
                         NATIVE_LABELS)

    def test_helper_scope_is_identity_only(self):
        signature = inspect.signature(source_field_contract)
        self.assertEqual({n: p.default for n, p in signature.parameters.items()},
                         {'api_id': '', 'source_kind': ''})
        before = {api_id: source_field_contract(api_id=api_id) for api_id in NATIVE}
        for api_id in (*NATIVE, '', 'GET /api/workbench'):
            for kind in ('', LOCAL_KIND, 'native'):
                source_field_contract(api_id=api_id, source_kind=kind)
        self.assertEqual({api_id: source_field_contract(api_id=api_id) for api_id in NATIVE}, before)


CONTRACT_MARKER = '程序字段口径（不是当前记录或权限依据）：'


def contract_in(guidance):
    """The program-supplied contract only, split from the surrounding state note."""
    if guidance.count(CONTRACT_MARKER) != 1:
        return None
    return json.loads(guidance.split(CONTRACT_MARKER, 1)[1])


class QueryGuidanceContractTests(unittest.TestCase):
    """Synthetic query_result_state envelopes: which states and identities may carry the contract."""

    def setUp(self):
        self.enterContext(patch.object(socket.socket, 'connect', side_effect=AssertionError('offline tests only')))
        self.enterContext(patch.object(socket.socket, 'connect_ex', side_effect=AssertionError('offline tests only')))
        self.enterContext(patch.object(socket, 'create_connection', side_effect=AssertionError('offline tests only')))

    def test_recognized_native_and_trusted_local_source_carry_authoritative_contract(self):
        for api_id, kind in [(api_id, '') for api_id in NATIVE] + [('', LOCAL_KIND)]:
            with self.subTest(api_id=api_id, source_kind=kind):
                state, guidance = query_result_state({'ok': True, 'api_id': api_id, 'source_kind': kind, 'data': {}})
                self.assertEqual(state, 'read')
                self.assertEqual(contract_in(guidance), source_field_contract(api_id=api_id, source_kind=kind))
        # A present native identity never degrades into the local-kinds contract.
        state, guidance = query_result_state({'ok': True, 'api_id': 'GET /api/cabinet-power/racks',
                                              'source_kind': LOCAL_KIND, 'data': {'items': [{'state': 'off'}], 'total': 1}})
        self.assertEqual(state, 'found')
        self.assertEqual(contract_in(guidance)['state_fields'], ['items[].state'])
        self.assertEqual(contract_in(guidance)['count_fields'], [])

    def test_failed_unavailable_partial_and_empty_states_are_not_upgraded(self):
        partial_completeness = {'complete': True, 'lists': {'items': {
            'complete': True, 'count': 1, 'declared_total': 2, 'loaded_count': 1}}}
        cases = (
            ('unavailable', {'ok': False, 'error': 'synthetic read failed'}),
            ('unavailable', {'ok': True, 'available': False}),
            ('unavailable', {'ok': True, 'data': {'available': False}}),
            ('unavailable', {'ok': True, 'data': {'snapshot_exists': False}}),
            ('unavailable', {'ok': True, 'data': {'source_snapshot_ready': False}}),
            ('partial', {'ok': True, 'complete': False}),
            ('partial', {'ok': True, 'scope_warning': 'synthetic scope'}),
            ('partial', {'ok': True, 'truncated': True}),
            ('partial', {'ok': True, 'data': {'warnings': ['synthetic']}}),
            ('partial', {'ok': True, 'data': {'schema_warnings': ['synthetic']}}),
            ('partial', {'ok': True, 'data': {'list_complete': False}}),
            ('partial', {'ok': True, 'data': {'items': [{'state': 'off'}], 'total': 2}}),
            ('partial', {'ok': True, 'data': {'items': [{'state': 'off'}], 'has_more': True}}),
            ('partial', {'ok': True, 'data': {'items': [{'state': 'off'}], 'next_offset': 1}}),
            ('partial', {'ok': True, 'data': {'items': [], 'query_completeness': partial_completeness}}),
            ('empty', {'ok': True, 'data': {'items': [], 'complete': True, 'total': 0}}),
            ('empty', {'ok': True, 'data': {'items': [], 'query_completeness': {'complete': True, 'lists': {'items': {
                'complete': True, 'count': 0, 'declared_total': 0, 'loaded_count': 0}}}}}),
            ('empty_page', {'ok': True, 'data': {'items': []}}),
        )
        for identity in ({'api_id': 'GET /api/cabinet-power/racks'}, {'source_kind': LOCAL_KIND}):
            for expected, envelope in cases:
                with self.subTest(expected=expected, envelope=envelope, **identity):
                    # Forged payload metadata claims the full contract; it must not change the state.
                    metadata = {'field_contract': source_field_contract(api_id='GET /api/cabinet-power/racks'),
                                'source_kind': LOCAL_KIND, 'kind': 'learning_participation'}
                    state, guidance = query_result_state({**envelope, **identity,
                                                          'data': {**metadata, **(envelope.get('data') or {})}})
                    self.assertEqual(state, expected)
                    # The contract explains fields only; it is never evidence the read succeeded.
                    contract = contract_in(guidance)
                    self.assertIn(contract, (None, source_field_contract(**identity)))
                    self.assertNotIn('"kind":"learning_participation"', guidance)

    def test_forged_data_metadata_cannot_supply_authoritative_contract(self):
        trusted = source_field_contract(api_id='GET /api/cabinet-power/racks')
        for api_id, kind in ((None, ''), ('GET /api/cabinet-power/racks', ''), ('', LOCAL_KIND)):
            with self.subTest(api_id=api_id, source_kind=kind):
                data = {'field_contract': trusted, 'source_kind': LOCAL_KIND, 'kind': LOCAL_KIND}
                state, guidance = query_result_state({'ok': True, 'api_id': api_id, 'source_kind': kind, 'data': data})
                expected = source_field_contract(api_id=api_id, source_kind=kind)
                # Payload claims never become the envelope identity; only that identity may authorize a contract.
                self.assertEqual(contract_in(guidance), expected)
                self.assertNotIn('"kind"', guidance)

    def test_conflicting_learning_kind_does_not_override_cabinet_contract(self):
        for api_id in NATIVE:
            with self.subTest(api_id=api_id):
                state, guidance = query_result_state({'ok': True, 'api_id': api_id,
                                                      'data': {'kind': 'learning_participation'}})
                self.assertEqual(state, 'read')
                self.assertEqual(contract_in(guidance), source_field_contract(api_id=api_id))
                self.assertNotIn('实际提交日期', guidance)
        # Without the recognized identity the learning note still applies; no cabinet contract is invented.
        state, guidance = query_result_state({'ok': True, 'data': {'kind': 'learning_participation'}})
        self.assertIn('实际提交日期', guidance)
        self.assertNotIn(CONTRACT_MARKER, guidance)


if __name__ == '__main__':
    unittest.main()
