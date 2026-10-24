"""Offline contracts for partial claim recovery; synthetic verdicts and packets only."""
import copy
import json
import socket
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))

from openclaw_service.assistant.lighthouse_review import (
    Verdict, _claim_spans, _recover_claims, _validated_claims)

ROSTER = '夜班名单：甲、乙'


class PartialClaimRecoveryTests(unittest.TestCase):
    def setUp(self):
        # Outbound sockets blocked: these helpers must never reach a network or model.
        blocked_connect = patch.object(socket.socket, 'connect', side_effect=AssertionError('no network'))
        blocked_exchange = patch.object(socket, 'getaddrinfo', side_effect=AssertionError('no network'))
        blocked_connect.start()
        blocked_exchange.start()
        self.addCleanup(blocked_exchange.stop)
        self.addCleanup(blocked_connect.stop)

    # Synthetic packet shaped like the review packet: answer, answer_lines (1-based) and evidence sources.
    @staticmethod
    def packet(answer, *sources):
        return {'question': '夜班有几个人？', 'answer': answer,
                'answer_lines': {str(index + 1): line for index, line in enumerate(answer.splitlines()) if line.strip()},
                'evidence': list(sources)}

    @staticmethod
    def source(number, text, **changes):
        return {'number': number, 'kind': 'company_knowledge', 'available': True, 'state': 'ok',
                'data': {'text': text}, **changes}

    @staticmethod
    def verdict(*claims, passed=False):
        return Verdict.model_validate({'passed': passed, 'issues': [] if passed else ['待修订。'],
                                       'claim_assessments': list(claims)})

    @staticmethod
    def supported(status='support', **changes):
        # Keyword pass-through: claim_id, claim and calculations must reach the schema unchanged.
        claim = {'status': status, 'evidence': [{'source_number': 1, 'quote': changes.pop('quote', ROSTER)}], **changes}
        issue = changes.pop('issue', '')
        return {**claim, **({'issue': issue} if status != 'support' else {})}

    @staticmethod
    def texts(claims):
        return [item['claim'] for item in claims]

    def assertBlocks(self, blocked, *expected):
        # Order and repetition carry no meaning: every expected text must be blocked and nothing else.
        self.assertEqual(set(blocked), set(expected))

    @staticmethod
    def span(answer, text):
        # Reuse the production span locator so the overlap guards track its real semantics, not raw offsets.
        spans = list(_claim_spans(answer, text))
        if len(spans) != 1:
            raise AssertionError(f'{text!r} spans are ambiguous: {spans}')
        return spans[0]

    def test_valid_fact_survives_fabricated_quote_and_no_evidence_advice(self):
        answer = 'A楼柴油发电机保养周期为每月一次。[1]\n可向厂家申请免费检测。\n可以联系厂家确认保修范围。'
        packet = self.packet(answer, self.source(1, 'A楼柴油发电机保养周期为每月一次。'))
        verdict = self.verdict(
            {'claim_id': 1, 'status': 'support', 'evidence': [{'source_number': 1, 'quote': '保养周期为每月一次'}]},
            {'claim_id': 2, 'status': 'support', 'evidence': [{'source_number': 1, 'quote': '厂家提供免费检测'}]},
            {'claim_id': 3, 'status': 'missing', 'evidence': [], 'issue': '无证据支持该建议。'})
        recovered, blocked = _recover_claims(verdict, packet)
        self.assertEqual(self.texts([item for item in recovered if item['status'] == 'support']),
                         ['A楼柴油发电机保养周期为每月一次。[1]'])
        self.assertNotIn('可向厂家申请免费检测。', self.texts(recovered))
        self.assertBlocks(blocked, '可向厂家申请免费检测。', '可以联系厂家确认保修范围。')

    def test_parent_support_is_excluded_with_invalid_child_only_when_overlapping(self):
        # Long line: the fragment below is a literal substring, not a line id, so it must be quoted verbatim.
        line = '夜班共有两人，其中一人迟到，当日夜间巡检已签字确认。'
        fragment = '其中一人迟到，'
        packet = self.packet(line, self.source(1, ROSTER))
        parent = {'claim_id': 1, 'status': 'support', 'evidence': [{'source_number': 1, 'quote': ROSTER}]}
        # Child cites a quote that is absent from the source text, so the fragment itself cannot be validated.
        invalid_child = self.supported(status='mismatch', claim=fragment, quote='名单未记录迟到', issue='名单未记录迟到。')
        cases = {'different evidence': [parent, invalid_child],
                 'same evidence': [parent, self.supported(status='mismatch', claim=fragment, quote=ROSTER,
                                                          issue='迟到无依据。')]}
        for name, claims in cases.items():
            with self.subTest(name):
                self.assertIn(fragment, line)
                self.assertNotIn('claim_id', claims[1])
                verdict = self.verdict(*claims)
                recovered, blocked = _recover_claims(verdict, packet)
                # The invalid child blocks its fragment span, so the parent's support cannot survive as a positive and
                # the answer is never partially passed; the rejected line is not offered back as verified text.
                self.assertEqual([item for item in recovered if item['status'] == 'support'], [])
                self.assertBlocks(blocked, fragment)
                self.assertEqual(self.texts(_recover_claims(self.verdict(claims[0], passed=True), packet)[0]), [line])

    def test_independent_fragment_survives_while_invalid_fragment_is_blocked(self):
        answer = 'A楼有3台发电机。B楼有9台发电机。'
        packet = self.packet(answer, self.source(1, 'A楼发电机3台；B楼发电机2台。'))
        verdict = self.verdict(
            self.supported(claim='A楼有3台发电机。', quote='A楼发电机3台'),
            self.supported(status='mismatch', claim='B楼有9台发电机。', quote='B楼发电机2台', issue='B楼实际为2台。'))
        recovered, blocked = _recover_claims(verdict, packet)
        # Neither fragment contains the other, so the correct fragment is independently supported and kept.
        self.assertEqual([(item['claim'], item['status']) for item in recovered],
                         [('A楼有3台发电机。', 'support'), ('B楼有9台发电机。', 'mismatch')])
        self.assertEqual(blocked, ['B楼有9台发电机。'])

    def test_only_independent_support_survives_around_unquotable_fragment(self):
        # Line-shaped like real packets; the bad line contains neither neighbour, so only its own quote can fail.
        bad_text = 'B楼有9台发电机，需立即更换。'
        good_text = 'C楼有4台发电机。'
        answer = f'A楼有3台发电机。\n{bad_text}\n{good_text}'
        packet = self.packet(answer, self.source(1, 'A楼发电机3台；B楼发电机2台；C楼发电机4台。'))
        unquotable = self.supported(status='mismatch', claim=bad_text, quote='B楼发电机9台', issue='B楼实际为2台。')
        independent = self.supported(claim=good_text, quote='C楼发电机4台')
        self.assertNotIn(unquotable['evidence'][0]['quote'], packet['evidence'][0]['data']['text'])
        self.assertNotIn(good_text, bad_text)
        self.assertNotIn(bad_text, good_text)
        # Guards: the bad fragment fails on its own, the good one is valid on its own.
        self.assertBlocks(_recover_claims(self.verdict(unquotable), packet)[1], bad_text)
        self.assertEqual(self.texts(_recover_claims(self.verdict(independent, passed=True), packet)[0]), [good_text])
        recovered, blocked = _recover_claims(self.verdict(independent, unquotable), packet)
        # The invalid fragment's quote is not visible source text; only the separate good fragment survives.
        self.assertEqual([(item['claim'], item['status']) for item in recovered], [(good_text, 'support')])
        self.assertBlocks(blocked, bad_text)

    def test_partial_overlap_fragment_is_blocked_by_one_candidate_sentence(self):
        # One source sentence backs both fragments, and their spans overlap without either containing the other:
        # the rejected span ends inside the positive fragment, so the shared sentence cannot yield a partial pass.
        sentence = 'A楼设备检修须停电；由一名电工执行；作业完成须复电确认。'
        negative_text = 'A楼设备检修须停电，由两名电工执行，作业完成须'
        positive_text = '作业完成须复电确认。'
        line = 'A楼设备检修须停电，由两名电工执行，作业完成须复电确认。'
        answer = f'{line}\nB楼无备用发电机。'
        packet = self.packet(answer, self.source(1, sentence))
        self.assertIn(negative_text, line)
        self.assertIn(positive_text, line)
        self.assertNotIn(negative_text, positive_text)
        self.assertNotIn(positive_text, negative_text)
        # The fragments straddle each other inside the line rather than sitting end to end.
        negative_start, negative_end = self.span(packet['answer'], negative_text)
        positive_start, positive_end = self.span(packet['answer'], positive_text)
        self.assertLess(positive_start, negative_end)
        self.assertLess(negative_start, positive_end)
        good = self.supported(claim=positive_text, quote='作业完成须复电确认')
        negative = self.supported(status='mismatch', claim=negative_text,
                                  quote='A楼设备检修须停电，由两名电工执行', issue='人员数与来源不符。')
        # Independently the fragment is valid, so only the overlap with the rejected span can explain losing it.
        self.assertEqual(self.texts(_recover_claims(self.verdict(good, passed=True), packet)[0]), [positive_text])
        recovered, blocked = _recover_claims(self.verdict(negative, good, passed=True), packet)
        self.assertEqual(recovered, [])
        self.assertBlocks(blocked, negative_text)

    def test_ambiguous_or_missing_claim_anchors_block_whole_recovery(self):
        answer = '夜班共有两人。'
        packet = self.packet(answer, self.source(1, ROSTER))
        valid = self.supported(claim_id=1)
        cases = {
            'no claim or line id': [valid, self.supported()],
            'unknown line id': [valid, self.supported(claim_id=9)],
            'id and text disagree': [valid, self.supported(claim_id=1, claim='夜班共有三人。')],
            'text is not literal': [valid, self.supported(claim='夜班有两人。')],
        }
        for name, claims in cases.items():
            with self.subTest(name):
                self.assertEqual(self.texts(_validated_claims(self.verdict(*claims[:-1]), packet)), [answer])
                recovered, blocked = _recover_claims(self.verdict(*claims), packet)
                self.assertEqual(recovered, [])
                self.assertBlocks(blocked, answer)

    def test_repeated_ambiguous_text_blocks_whole_partial_recovery(self):
        answer = '夜班人员共两人。 夜班人员共两人。'
        packet = self.packet(answer, self.source(1, ROSTER))
        verdict = self.verdict(self.supported(claim='夜班人员共两人。'),
                               self.supported(status='mismatch', claim=answer, issue='句子重复。'))
        recovered, blocked = _recover_claims(verdict, packet)
        # The repeated sentence cannot be located once, so it stays negative and blocks everything.
        self.assertEqual([(item['claim'], item['status']) for item in recovered], [(answer, 'mismatch')])
        self.assertBlocks(blocked, answer)

    # The malformed claim shares this line's text and is listed second, so the arithmetic mismatch already decides
    # the outcome; malformed calculations are proven in isolation by the contract test below.
    def test_arithmetic_mismatch_stays_negative_alongside_malformed_claim(self):
        answer = 'A楼有5台设备，占比50%。'
        packet = self.packet(answer, self.source(1, 'A楼设备5台，B楼设备5台。'))
        arithmetic = self.supported(claim_id=1, quote='A楼设备5台',
                                    calculations=[{'expression': '5/20*100', 'stated_result': '50'}])
        malformed = {**arithmetic, 'calculations': 'not-a-list'}
        for name, claims in (('arithmetic mismatch', [arithmetic]), ('malformed alongside', [arithmetic, malformed])):
            with self.subTest(name):
                verdict = self.verdict(*claims)
                recovered, blocked = _recover_claims(verdict, packet)
                # Arithmetic only demotes: the mismatch text is reported negative and never offered as verified.
                self.assertEqual([(item['claim'], item['status']) for item in recovered], [(answer, 'mismatch')])
                self.assertIn('核算结果为25', recovered[0]['issue'])
                self.assertBlocks(blocked, answer)

    def test_malformed_calculations_never_verify_and_never_enter_recovery(self):
        answer = 'A楼有5台设备，占比50%。'
        packet = self.packet(answer, self.source(1, 'A楼设备5台，B楼设备5台。'))
        malformed = {**self.supported(claim_id=1, quote='A楼设备5台'), 'calculations': 'not-a-list'}
        # No arithmetic issue here, so only the malformed calculations can stop this claim from being recovered.
        self.assertEqual(self.texts(_validated_claims(self.verdict({**malformed, 'calculations': []}, passed=True),
                                                      packet)), [answer])
        recovered, blocked = _recover_claims(self.verdict(malformed, passed=True), packet)
        self.assertEqual(recovered, [])
        self.assertBlocks(blocked, answer)

    def test_unavailable_duplicate_and_conflicting_sources_never_validate(self):
        answer = '夜班共有两人。'
        # claim_id anchors every claim to line 1, so failures below can only come from grounding.
        claim = self.supported(claim_id=1)
        good = self.packet(answer, self.source(1, ROSTER))
        duplicate = self.packet(answer, self.source(1, ROSTER), self.source(1, ROSTER))
        unavailable = self.packet(answer, self.source(1, ROSTER, available=False))
        blocked_state = self.packet(answer, self.source(1, ROSTER, state='unavailable'))
        # Same valid claim against one available unique source must pass; otherwise the cases below prove nothing.
        self.assertEqual(self.texts(_validated_claims(self.verdict(claim, passed=True), good)), [answer])
        self.assertEqual(self.texts(_recover_claims(self.verdict(claim, passed=True), good)[0]), [answer])
        for name, packet in (('duplicate number', duplicate), ('available false', unavailable),
                             ('state unavailable', blocked_state)):
            with self.subTest(name):
                # A grounded claim now reaches source validation and is rejected there, not for a missing anchor.
                with self.assertRaisesRegex(ValueError, 'unavailable or ambiguous source'):
                    _validated_claims(self.verdict(claim, passed=True), packet)
                # The same packets refuse even an unanchored claim, so the block is not merely the missing anchor.
                with self.assertRaisesRegex(ValueError, 'claim is not a literal candidate sentence'):
                    _validated_claims(self.verdict(self.supported(), passed=True), packet)
                recovered, blocked = _recover_claims(self.verdict(claim), packet)
                self.assertEqual(self.texts(recovered), [])
                self.assertBlocks(blocked, answer)
        single_quote = self.supported(claim_id=1, status='conflict', issue='名单互相冲突。',
                                      evidence=[{'source_number': 1, 'quote': '甲'}])
        recovered, blocked = _recover_claims(self.verdict(single_quote), good)
        self.assertEqual(self.texts(recovered), [])
        self.assertBlocks(blocked, answer)
        two_sided = self.supported(claim_id=1, status='conflict', issue='名单互相冲突。',
                                   evidence=[{'source_number': 1, 'quote': '甲'}, {'source_number': 1, 'quote': '乙'}])
        self.assertEqual(self.texts(_validated_claims(self.verdict(two_sided), good)), [answer])
        recovered, blocked = _recover_claims(self.verdict(two_sided), good)
        self.assertEqual([(item['claim'], item['status']) for item in recovered], [(answer, 'conflict')])
        self.assertBlocks(blocked, answer)

    def test_negative_verdict_is_kept_without_promoting_anything(self):
        answer = '夜班共有两人。\n可以联系厂家确认保修范围。'
        packet = self.packet(answer, self.source(1, ROSTER))
        verdict = self.verdict(self.supported(status='mismatch', claim_id=1, issue='人数口径不符。'),
                               self.supported(status='missing', claim_id=2, issue='缺证。'))
        recovered, blocked = _recover_claims(verdict, packet)
        # Each negative stands on its own; neither is dropped as overlapping the other.
        self.assertEqual([(item['claim'], item['status']) for item in recovered],
                         [('夜班共有两人。', 'mismatch'), ('可以联系厂家确认保修范围。', 'missing')])
        self.assertBlocks(blocked, '夜班共有两人。', '可以联系厂家确认保修范围。')

    def test_inputs_are_unchanged(self):
        answer = 'A楼柴油发电机保养周期为每月一次。[1]\n可向厂家申请免费检测。'
        packet = self.packet(answer, self.source(1, 'A楼柴油发电机保养周期为每月一次。'))
        verdict = self.verdict(
            {'claim_id': 1, 'status': 'support', 'evidence': [{'source_number': 1, 'quote': '保养周期为每月一次'}]},
            {'claim_id': 2, 'status': 'support', 'evidence': [{'source_number': 1, 'quote': '免费检测'}]})
        before = json.dumps({'packet': packet, 'verdict': verdict.model_dump()}, ensure_ascii=False, sort_keys=True)
        _recover_claims(verdict, packet)
        _recover_claims(verdict.model_copy(deep=True), copy.deepcopy(packet))
        self.assertEqual(json.dumps({'packet': packet, 'verdict': verdict.model_dump()},
                                    ensure_ascii=False, sort_keys=True), before)

    def test_normalized_output_drops_calculations_and_claim_ids(self):
        answer = 'A楼有5台设备。'
        packet = self.packet(answer, self.source(1, 'A楼设备5台。'))
        claim = {'claim_id': 1, 'status': 'support', 'evidence': [{'source_number': 1, 'quote': 'A楼设备5台'}]}
        verdict = self.verdict({**claim, 'calculations': [{'expression': '2+3', 'stated_result': '5'}]})
        self.assertEqual(self.texts(_validated_claims(verdict, packet)), [answer])
        recovered, blocked = _recover_claims(verdict, packet)
        # The claim is valid: 2+3=5 raises no arithmetic issue. The verdict itself defaulted to passed=False with
        # an issue and carries no negative assessment, so recovery refuses the whole answer by design.
        self.assertEqual(recovered, [])
        self.assertBlocks(blocked, answer)
        # The same valid claim under a passing verdict is recovered, proving arithmetic is not what blocked it.
        clean = self.verdict({**claim, 'calculations': [{'expression': '4+1', 'stated_result': '5'}]}, passed=True)
        recovered, blocked = _recover_claims(clean, packet)
        self.assertEqual(self.texts(recovered), [answer])
        for item in recovered:
            self.assertEqual(set(item), {'claim', 'status', 'evidence', 'issue'})
            self.assertTrue(all(set(part) == {'source_number', 'quote', 'kind'} for part in item['evidence']))
        self.assertBlocks(blocked)
        self.assertEqual(json.loads(json.dumps(recovered))[0]['claim'], answer)

    def test_passed_verdict_recovers_claims_without_blocking(self):
        answer = '夜班共有两人。'
        packet = self.packet(answer, self.source(1, ROSTER))
        recovered, blocked = _recover_claims(self.verdict(self.supported(claim_id=1), passed=True), packet)
        self.assertEqual([(item['claim'], item['status']) for item in recovered], [(answer, 'support')])
        self.assertEqual(blocked, [])


if __name__ == '__main__':
    unittest.main()
