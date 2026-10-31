"""Small, derived execution receipts for the existing assistant tool loop.

These receipts describe progress, never grant access or authorize a write.
They contain no tool parameters, business payloads, or model reasoning.
"""
import time
import hashlib


def clear_goal_assessments(turn):
    turn.pop('_answer_goals', None)
    turn.pop('_goal_answer_digest', None)
    progress = (turn.get('_task_context') or {}).get('progress')
    if isinstance(progress, dict):
        progress.update(ready=False, reviewed_count=0)
        for goal in progress.get('goals', []):
            goal.update(status='checking', query_refs=[], limitation='')


def finish_progress(turn, result):
    digest = hashlib.sha256(str(result.get('answer', '')).strip().encode()).hexdigest()
    if (result.get('answer_review', {}).get('status') != 'passed'
            or turn.get('_goal_answer_digest') != digest):
        clear_goal_assessments(turn)
    if isinstance(turn.get('_task_context'), dict):
        result['_task_context'] = turn['_task_context']
    return result


def read_timeout(turn, ceiling=45, reserve=12):
    from .lighthouse_ai import AssistantError
    remaining = turn.get('_answer_deadline', float('inf')) - time.monotonic() - reserve
    if remaining <= 0:
        raise AssistantError('本轮查询时间已用完，已取得的资料保留，正在整理可核实内容。',
                             504, category='timeout')
    return min(ceiling, remaining)


def task_progress(turn, sources=(), queries=None):
    from .lighthouse_query_progress import query_progress
    context = turn.get('_task_context') or {}
    contract = context.get('contract') or {}
    expected = {goal['id'] for goal in contract.get('deliverables', [])}
    checked = {item.get('goal_id'): item for item in turn.get('_answer_goals', [])
               if isinstance(item, dict) and item.get('goal_id') in expected}
    coverage = query_progress(sources, queries or {})
    goals = []
    for goal in contract.get('deliverables', [])[:8]:
        result = checked.get(goal['id'])
        # A read receipt is not proof that a particular requested goal is met.
        status = result['status'] if result else 'checking' if coverage['sources'] else 'pending'
        goals.append({'id': goal['id'], 'description': goal['description'], 'kind': goal['kind'],
                      'status': status,
                      'query_refs': list(result.get('query_refs', [])) if result else [],
                      'limitation': result.get('limitation', '') if result else ''})
    progress = {'goals': goals, 'source_count': len(coverage['sources']),
                'source_gaps': len(coverage['gaps']), 'reviewed_count': len(checked),
                'ready': bool(goals) and all(item['status'] == 'complete' for item in goals)}
    if isinstance(turn.get('_task_context'), dict):
        turn['_task_context']['progress'] = progress
    return progress


def prioritize_tools(definitions, task_type):
    """Order existing callable tools by task category; retain discovery/fallbacks."""
    preferred = {
        'statistics': {'query', 'discover', 'table_catalog', 'table_records', 'read_query',
                       'query_statistics', 'calculate', 'duty_roster', 'learning_participants'},
        'document': {'company_documents', 'feishu_document', 'read_file', 'read_attachment_document',
                     'personal_workspace', 'search_history'},
        'mixed': {'query', 'discover', 'company_documents', 'read_query', 'query_statistics'},
        'general': {'calculate', 'date_time', 'public_search', 'public_page', 'weather'},
    }.get(task_type, set())
    common = {'plan_task', 'query_progress', 'review_answer', 'ask_business_question'}
    return sorted(definitions, key=lambda item: 0 if item.name in preferred else 1 if item.name in common else 2)


