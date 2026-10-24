"""Opt-in review calibration using synthetic correct/incorrect answer pairs.

Default validates fixtures only, without reading configuration or calling models.
--live uses existing read-only model settings, never business APIs or Feishu sends.
Reports distinguish false acceptance, false rejection and unavailable review.
"""
import argparse
import asyncio
import json
from pathlib import Path
import sys
import time
from types import SimpleNamespace

BIN = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BIN))

CASES = (
    ('ratio', '这批任务完成多少？', {'completed': 3, 'assigned': 8, 'unit': '项'},
     '已完成3项，共8项。\n进度为3/8，即37.5%。',
     '已完成3项，共8项。\n进度为8/3，即37.5%。', ''),
    ('unit', '这次用了多少水？', {'quantity': 1200, 'unit': 'L', 'conversion': '1立方米=1000L'},
     '用了1200升水。\n换算为1.2立方米。', '用了1200升水。\n换算为12立方米。', ''),
    ('percentage_point', '两个月完成率变化多少？', {'previous_month_rate': 0.4, 'current_month_rate': 0.5},
     '完成率由40%升至50%。\n增加10个百分点，相对增长25%。',
     '完成率由40%升至50%。\n增加25个百分点，相对增长10%。', ''),
    ('overlapping_subsets', '目前有多少未完成任务？',
     {'unfinished': 8, 'active': 6, 'awaiting_upload': 2,
      'definitions': '未完成包括进行中与等待上传；三项不能相加。'},
     '未完成8项，其中进行中6项、等待上传2项。',
     '进行中6项、等待上传2项。\n另有未完成8项，合计16项未完成。', ''),
    ('zero_denominator', '通过率多少？', {'passed': 0, 'reviewed': 0},
     '还没有已审核记录，通过率暂无法计算。', '已通过0项，已审核0项。\n通过率为0%。', ''),
    ('parenthetical_ratio', '这份题单做了多少题？', {'answered': 1, 'assigned': 2},
     '已答1题，共2题（已答/总题数=1/2）。', '已答1题，共2题（已答/总题数=2/1）。', ''),
    ('quoted_code', '原样展示日期和代码示例，不要计算或改写代码。',
     {'date': '2026/10/24', 'example': 'ratio = 2/1'},
     '2026/10/24\n```python\nratio = 2/1\n```',
     '2026/10/24\n```python\nratio = 1/2\n```', ''),
    ('cabinet_state', 'C01机柜当前是什么状态？能确定服务器关机了吗？',
     {'items': [{'rack': 'C01', 'state': 'off', 'state_source': 'baseline'}]},
     'C01台账状态为未上电/已下电，来源为保存的基线。不能据此确定服务器是否关机。',
     'C01台账为off，说明服务器已经关机。', 'GET /api/cabinet-power/racks'),
    ('cabinet_empty', 'C02是否从未上电？',
     {'items': [{'rack': 'C02', 'state': 'off', 'state_source': 'empty'}]},
     'C02台账显示未上电/已下电，且无有效操作记录和基线，不能据此认定从未上电。',
     'C02没有有效操作记录和基线，所以可以确定它从未上电。', 'GET /api/cabinet-power/racks'),
    ('cabinet_counts', '机柜总数和上电率分别是多少？',
     {'counts': {'total': 12, 'formal': 8, 'test': 0, 'powered': 8, 'off': 3, 'unknown': 1},
      'rooms': [{'id': '101'}], 'racks': [{'rack': 'C01', 'state': 'off'}], 'record_count': 27},
     '共有12台机柜，其中8台已上电；上电率按8/12计算，约66.67%。',
     '共有27台机柜；上电率按8/11计算，约72.73%。', 'GET /api/cabinet-power/overview'),
)


def outcome(status, correct):
    if status in {'passed', 'limited'}:
        return 'accepted_correct' if correct else 'false_acceptance'
    if status == 'needs_repair':
        return 'false_rejection' if correct else 'rejected_incorrect'
    return 'unavailable'


async def evaluate(model, profiles, *, cases=CASES):
    from openclaw_service.assistant.lighthouse_review import AnswerReviewer

    actor = {'id': 'synthetic-review-evaluation', 'scopes': ['D'], 'modules': ['assistant']}
    async def authorize():
        return actor
    rows = []
    for index, profile in enumerate(profiles, 1):
        # Isolate each profile: fallback would conceal that model's own failures.
        selected = SimpleNamespace(settings=lambda: {'enabled': True, 'models': []}, complete=model.complete)
        reviewer = AnswerReviewer(SimpleNamespace(model_for=lambda _: selected))
        for name, question, data, good, bad, api_id in cases:
            for correct, answer in ((True, good), (False, bad)):
                started = time.monotonic()
                result = await reviewer.run(actor, {'question': question, '_profile': profile,
                    '_reference_time': '2026-10-24T12:00:00+08:00'}, [],
                    {'answer': answer, 'sources': [{'number': 1, 'available': True,
                        'title': 'Synthetic source', 'data': data, 'api_id': api_id}]}, authorize, defer_repair=True)
                status = result.get('answer_review', {}).get('status')
                rows.append({'model': f'model_{index}', 'case': name, 'correct_candidate': correct,
                    'review_status': status, 'outcome': outcome(status, correct),
                    'seconds': round(time.monotonic() - started, 3)})
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--live', action='store_true')
    args = parser.parse_args()
    if not args.live:
        valid = len({case[0] for case in CASES}) == len(CASES) and all(
            name and question and isinstance(data, dict) and data and good and bad and good != bad
            and isinstance(api_id, str) for name, question, data, good, bad, api_id in CASES)
        print(json.dumps({'mode': 'offline_fixture', 'valid': bool(valid), 'pairs': len(CASES),
                          'model_calls': 0, 'semantic_status': 'not_measured'}))
        return int(not valid)
    from tools.evaluate_lighthouse_models import load_shared_models
    model = None
    try:
        model, profiles = load_shared_models()
        rows = asyncio.run(evaluate(model, profiles))
    except Exception as exc:
        print(json.dumps({'mode': 'live_synthetic', 'execution_ok': False, 'error_type': type(exc).__name__}))
        return 1
    finally:
        if model is not None:
            model.close()
    counts = {name: sum(row['outcome'] == name for row in rows) for name in (
        'accepted_correct', 'rejected_incorrect', 'false_acceptance', 'false_rejection', 'unavailable')}
    print(json.dumps({'mode': 'live_synthetic', 'results': rows, 'counts': counts,
                      'scope': 'fixed_candidate_review_only'}, ensure_ascii=False, indent=2))
    return int(any(counts[name] for name in ('false_acceptance', 'false_rejection', 'unavailable')))


if __name__ == '__main__':
    raise SystemExit(main())
