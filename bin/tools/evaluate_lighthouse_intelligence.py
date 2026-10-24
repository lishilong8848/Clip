"""Evaluate the shared engine against synthetic data, never production tables.
Offline mode checks the fixture, not model intelligence. Live answers always need
manual semantic review; mechanical checks alone do not establish answer quality.
"""
import argparse
import asyncio
import copy
import json
import os
import re
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / 'bin'))
from bin.test_lighthouse_intelligence_evaluation import IntelligenceEvaluationTests
from openclaw_service.assistant.lighthouse_ai import CustomModel
from openclaw_service.assistant.lighthouse_model import configured_model

ROWS = ({'id': 'one', 'person': {'id': 'a', 'name': '测试甲'}},
        {'id': 'two', 'person': {'id': 'a', 'name': '测试甲'}},
        {'id': 'three', 'person': {'id': 'b', 'name': '测试乙'}})
CASES = (('names_and_distinct_count', '核对D楼工单参与人员，人数和名单一起给我。', False),
         ('paraphrase', 'D楼这些工单到底谁参与了？同一个人别重复算，把人名也列出来。', False),
         ('missing_page', '列出D楼所有工单的参与人员，说明人数及查询是否完整。', True))
PROFILE = {'name': 'evaluation', 'model': 'fixture', 'tool_calls': True, 'image_input': False,
           'reasoning': False, 'context_window': 64000}
COMPLETE = 'D楼共3条工单，去重后共2名参与人员：测试甲、测试乙。'
PARTIAL = 'D楼第2页读取失败，只取得第1页2条工单（测试甲2条）；其余未查全，参与人数未知。'


def respond(partial):
    def read(scope, page=1, page_size=20):
        if partial and page > 1:
            return {'ok': False, 'status': 503, 'error': 'synthetic page unavailable'}
        start = (page - 1) * 2
        return {'ok': True, 'data': {'items': copy.deepcopy(list(ROWS[start:start + 2])),
            'total': 3, 'page': page, 'page_size': 2, 'has_more': start + 2 < 3}}
    return read


@asynccontextmanager
async def fixture_factory(*_):
    from pydantic_ai.messages import ToolReturnPart
    from pydantic_ai.models.function import DeltaToolCall, FunctionModel

    async def script(messages, info):
        returns = [part.content for message in messages for part in message.parts
                   if isinstance(part, ToolReturnPart) and part.tool_name == 'query'
                   and isinstance(part.content, dict)]
        if returns:
            yield PARTIAL if returns[-1].get('query_state') == 'partial' else COMPLETE
        else:
            yield {0: DeltaToolCall(name='query', json_args=json.dumps({'complete': True,
                'operation': {'api_id': 'GET /api/assistant/work-orders', 'params': {'scope': 'D'}}}))}
    yield FunctionModel(stream_function=script)


def case_checks(result, calls, partial):
    sources = result.get('sources') or []
    data = [source.get('data') or {} for source in sources]
    rows = [row for item in data for row in item.get('items', [])]
    ids = {row.get('id') for row in rows}
    people = {row.get('person', {}).get('id') for row in rows}
    pages = {call.get('page') for call in calls}
    answer = re.sub(r'[*_]', '', result.get('answer', ''))
    counts = re.findall(r'(?<!\d)(\d+|两|二|三|一)\s*(?:位|名|个)?\s*(?:(?:参与|工单参与|答题)\s*)?人', answer)
    if partial:
        coverage = bool(data) and all(item.get('complete') is False for item in data) and ids == {'one', 'two'}
        answer_ok = (bool(re.search(r'第\s*2\s*页|第二页|page\s*2', answer, re.I))
                     and bool(re.search(r'未知|无法(?:确认|确定|统计)|未查全|不完整', answer))
                     and not re.search(r'测试乙|全部查完|完整名单', answer))
    else:
        coverage = bool(data) and all(item.get('complete') is True for item in data) and ids == {'one', 'two', 'three'}
        answer_ok = (all(name in answer for name in ('测试甲', '测试乙'))
                     and bool(counts) and all(count in ('2', '二', '两') for count in counts))
    return {
        'fresh_reads': bool(calls) and bool(sources),
        'scope': bool(calls) and all(call.get('scope') == 'D' for call in calls)
                 and bool(sources) and all(source.get('scopes') == ['D'] for source in sources),
        'pagination': pages == {1, 2} and any(call.get('page_size') == 2 for call in calls),
        'source_coverage': coverage,
        'source_people': people == ({'a'} if partial else {'a', 'b'}),
        'answer_contract': bool(answer_ok),
    }


async def evaluate(factory, profile, *, live=False):
    results = []
    fixture = IntelligenceEvaluationTests()
    for channel in ('', 'feishu:oc_evaluation'):
        for name, question, partial in CASES:
            started = time.monotonic()
            entry = {'case': name, 'channel': 'feishu' if channel else 'web',
                     'semantic_status': 'needs_manual_review' if live else 'not_applicable'}
            try:
                result, calls, _ = await fixture.evaluate(channel, question, None, respond(partial),
                    model_factory=factory, turn={'_profile': profile})
                checks = case_checks(result, calls, partial)
                review = result.get('answer_review')
                entry.update(execution_ok=True, mechanical_checks=checks,
                    mechanical_checks_passed=all(checks.values()), read_calls=len(calls),
                    answer_review=review.get('status') if isinstance(review, dict) else None,
                    synthetic_answer=result.get('answer', ''))
            except Exception as exc:
                entry.update(execution_ok=False, mechanical_checks_passed=False,
                             error=type(exc).__name__, synthetic_answer='', answer_review=None)
            results.append({**entry, 'seconds': round(time.monotonic() - started, 2)})
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live', action='store_true')
    parser.add_argument('--endpoint', default=os.environ.get('LIGHTHOUSE_EVAL_ENDPOINT', ''))
    parser.add_argument('--model', action='append', default=[])
    args = parser.parse_args()
    key = os.environ.get('LIGHTHOUSE_EVAL_API_KEY', '')
    if args.live and (not key or not args.endpoint or not args.model):
        parser.error('Live evaluation requires --endpoint, --model and LIGHTHOUSE_EVAL_API_KEY.')
    reports = []
    for model in args.model or ['fixture']:
        profile, factory = {**PROFILE, 'model': model}, fixture_factory
        if args.live:
            profile.update(endpoint=CustomModel._endpoint(args.endpoint), key_cipher='evaluation-only')
            custom = SimpleNamespace(unprotect=lambda _: key, _endpoint=CustomModel._endpoint)

            @asynccontextmanager
            async def factory(_, selected):
                async with configured_model(custom, selected) as value:
                    yield value
        results = asyncio.run(evaluate(factory, profile, live=args.live))
        reports.append({'model': model, 'mode': 'live-synthetic-data' if args.live else 'offline-fixture',
                        'results': results})
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    return int(any(not case['mechanical_checks_passed'] for report in reports for case in report['results']))


if __name__ == '__main__':
    raise SystemExit(main())
