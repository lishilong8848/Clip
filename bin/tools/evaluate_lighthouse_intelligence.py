"""Opt-in live evaluation against synthetic APIs, never production business data.

Without --live this is a fixture self-check, not a measurement of a real model.
Live credentials come only from LIGHTHOUSE_EVAL_API_KEY, never command arguments.
"""
import argparse
import asyncio
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


CASES = (
    ('names_and_distinct_count', '核对D楼工单参与人员，人数和名单一起给我。', False),
    ('paraphrase', 'D楼这些工单到底谁参与了？同一个人别重复算，把人名也列出来。', False),
    ('missing_page', '列出D楼所有工单的参与人员，说明人数及查询是否完整。', True),
)


def responder(partial):
    rows = [{'id': 'one', 'person': {'id': 'a', 'name': '测试甲'}},
            {'id': 'two', 'person': {'id': 'a', 'name': '测试甲'}},
            {'id': 'three', 'person': {'id': 'b', 'name': '测试乙'}}]
    def respond(scope, page, page_size):
        if partial:
            if page > 1:
                return {'ok': False, 'status': 503, 'error': 'synthetic missing page'}
            return {'ok': True, 'data': {'items': [rows[0]], 'total': 3, 'has_more': True}}
        return {'ok': True, 'data': {'items': rows, 'total': 3, 'complete': True}}
    return respond


async def evaluate(factory, profile):
    report = []
    fixture = IntelligenceEvaluationTests()
    for channel in ('', 'feishu:oc_evaluation'):
        for name, question, partial in CASES:
            started = time.monotonic()
            try:
                result, calls, _ = await fixture.evaluate(channel, question, None, responder(partial),
                    model_factory=factory, turn={'_profile': profile})
                answer = result['answer']
                if partial:
                    passed = bool(re.search(r'不完整|未查全|部分|未知|未取得|未能|缺失|失败|无法确认', answer))
                else:
                    passed = all(name in answer for name in ('测试甲', '测试乙')) and bool(re.search(r'(?:2|二|两)\s*(?:位|名|个)?人', answer))
                report.append({'case': name, 'channel': 'feishu' if channel else 'web', 'passed': passed,
                    'read_calls': len(calls), 'seconds': round(time.monotonic() - started, 2)})
            except Exception as exc:
                report.append({'case': name, 'channel': 'feishu' if channel else 'web', 'passed': False,
                    'error_type': type(exc).__name__, 'seconds': round(time.monotonic() - started, 2)})
    return report


def fixture_factory():
    from pydantic_ai.models.function import DeltaToolCall, FunctionModel
    @asynccontextmanager
    async def factory(*_):
        step = 0
        async def script(messages, info):
            nonlocal step
            step += 1
            if step == 1:
                yield {0: DeltaToolCall(name='query', json_args=json.dumps({'complete': True,
                    'operation': {'api_id': 'GET /api/assistant/work-orders', 'params': {'scope': 'D'}}}))}
            else:
                data = messages[-1].parts[0].content
                yield '当前只取得部分资料，完整人数未知。' if data['query_state'] == 'partial' else '共2人：测试甲、测试乙。'
        yield FunctionModel(stream_function=script)
    return factory


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
        profile = {'name': 'evaluation', 'model': model, 'tool_calls': True, 'image_input': False,
                   'reasoning': False, 'context_window': 64000}
        factory = fixture_factory()
        if args.live:
            profile.update(endpoint=CustomModel._endpoint(args.endpoint), key_cipher='evaluation-only')
            custom = SimpleNamespace(unprotect=lambda _: key, _endpoint=CustomModel._endpoint)
            @asynccontextmanager
            async def factory(_, selected):
                async with configured_model(custom, selected) as value:
                    yield value
        reports.append({'model': model, 'mode': 'live-synthetic-data' if args.live else 'offline-fixture',
                        'results': asyncio.run(evaluate(factory, profile))})
    print(json.dumps(reports, ensure_ascii=False, indent=2))
    return int(any(not case['passed'] for report in reports for case in report['results']))


if __name__ == '__main__':
    raise SystemExit(main())
