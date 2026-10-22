"""Daily projections; reads never refresh directories, send, or enqueue cloud writes."""
import copy
import datetime as dt
import hashlib
import json
from collections import Counter, defaultdict
from contextlib import closing

from . import learning as core
from .pre_shift_checks import windows

KIND = 'daily_progress'
VERSION = 1
STATES = ('未安排', '未到时', '待答', '答题中', '已完成', '逾期', '待核对')
DISPLAY_FIELDS = {'日期': 5, '人员标识': 1, '姓名': 1, '楼栋': 1, '班次': 1, '完成状态': 1,
                  '应答题数': 2, '已答题数': 2, '正确题数': 2, '首次客观正确率': 2,
                  '看答案后数': 2, '主动练习量': 2, '复习次数': 2, '更新时间': 5}


def day(value):
    try:
        result = dt.date.fromisoformat(str(value))
        if result.isoformat() != value:
            raise ValueError()
        return result.isoformat()
    except (TypeError, ValueError):
        raise core.LearningError('请选择有效统计日期。') from None


def date_of(value):
    try:
        parsed = dt.datetime.fromisoformat(value)
        return (parsed.astimezone(core.TZ) if parsed.tzinfo else parsed.replace(tzinfo=core.TZ)).date().isoformat()
    except (ValueError, TypeError):
        return ''


def key(date, pid):
    return 'daily:' + date + ':' + pid


def request_recompute(service, dates):
    if not getattr(service, '_completion_enabled', False):
        return
    with service.transaction() as conn:
        for date in set(dates):
            try:
                date = day(date)
            except core.LearningError:
                continue
            service._put('local', 'completion_pending:' + date, {'date': date}, conn, False)
    service._wake.set()


def _snapshot(service, date):
    from .learning_reminders import ROSTER_VERSION
    value = service._get('local', 'shift_roster:' + date) or service._get('local', 'shift_roster') or {}
    return value if value.get('date') == date and value.get('version') == ROSTER_VERSION and isinstance(value.get('people'), list) else None


def _status(task, current):
    if task.get('cancelled'):
        return '未安排'
    if task.get('uncertain') or task.get('assigned') is None:
        return '待核对'
    assigned, answered = task.get('assigned', 0), task.get('answered', 0)
    if assigned and answered >= assigned:
        return '已完成'
    try:
        start = dt.datetime.fromisoformat(task['start_at'])
        start = start.astimezone(core.TZ) if start.tzinfo else start.replace(tzinfo=core.TZ)
        due = dt.datetime.fromisoformat(task['due_at']) if task.get('due_at') else None
        if due:
            due = due.astimezone(core.TZ) if due.tzinfo else due.replace(tzinfo=core.TZ)
    except (ValueError, TypeError, KeyError):
        return '待核对'
    if current < start:
        return '未到时'
    if due and current >= due:
        return '逾期'
    return '答题中' if answered or task.get('started') else '待答'


def _delivery(value):
    if value.get('status') == 'sent' or value.get('sent_at'):
        return '已发送'
    if value.get('error') or value.get('last_error') or value.get('status') == 'failed':
        return '发送失败'
    return '待发送' if value else '未记录'


def _paper_task(paper, record, date, shift, current, notification=None):
    questions = [q for q in paper.get('questions', []) if not q.get('invalid')]
    counts = Counter()
    for question in questions:
        entry = record.get('entries', {}).get(question['id'], {})
        attempt = entry.get('attempt')
        if not attempt or entry.get('needs_review'):
            continue
        counts['answered'] += 1
        if question.get('type') != 'interview' and isinstance(attempt.get('correct'), bool):
            counts['objective_answered'] += 1
            counts['correct'] += int(attempt['correct'])
        evidence = attempt.get('answer_view_snapshot')
        if isinstance(evidence, dict) and evidence.get('version') == 1:
            counts['answer_viewed'] += int(bool(evidence.get('answer_before_submit')))
            counts['answer_view_unknown'] += int(bool(evidence.get('legacy_unknown')) and not evidence.get('answer_before_submit'))
        else:
            counts['answer_view_unknown'] += 1
    start, due = windows(date, shift) if shift in {'白', '夜'} else (
        dt.datetime.combine(dt.date.fromisoformat(date), dt.time.min, core.TZ),
        dt.datetime.combine(dt.date.fromisoformat(date) + dt.timedelta(days=1), dt.time.min, core.TZ))
    if paper.get('created_at'):
        try:
            issued = dt.datetime.fromisoformat(paper['created_at'])
            start = max(start, issued.astimezone(core.TZ) if issued.tzinfo else issued.replace(tzinfo=core.TZ))
        except ValueError:
            pass
    task = {'task_id': paper['id'], 'source': 'paper', 'shift': shift, 'scope': paper.get('scope', ''),
            'start_at': start.isoformat(), 'due_at': due.isoformat(), 'assigned': len(questions),
            **{field: counts[field] for field in ('answered', 'correct', 'objective_answered', 'answer_viewed', 'answer_view_unknown')},
            'started': bool(paper.get('started_at')),
            'uncertain': not questions or any(entry.get('needs_review') for entry in record.get('entries', {}).values()),
            'delivery_status': _delivery(notification or {})}
    task['status'] = _status(task, current)
    return task


def _data(service):
    with closing(service._connect()) as conn:
        conn.execute('BEGIN')
        return {kind: service._all(kind, conn) for kind in ('person', 'paper', 'record', KIND, 'notification',
                                                         'sharing_batch', 'sharing_attempt', 'sharing_delivery')}


def _sharing_tasks(data, pid, date, current):
    batches = data['_sharing_batches']
    attempts = data['_sharing_by_person'].get(pid, [])
    pairs = Counter(a.get('batch_id') for a in attempts)
    seen = set()
    for attempt in attempts:
        batch = batches.get(attempt.get('batch_id'))
        if not batch or attempt.get('person_id') != pid or attempt.get('open_id') in batch.get('excluded_open_ids', []):
            continue
        business_date = batch.get('business_date') or date_of(batch.get('published_at')) or batch.get('date')
        if business_date != date:
            continue
        if batch['id'] in seen:
            continue
        seen.add(batch['id'])
        questions = {q['id']: q for q in batch.get('questions', [])}
        ids = attempt.get('question_ids', [])
        submitted = bool(attempt.get('submitted_at'))
        objective = sum(questions.get(qid, {}).get('type') in {'single', 'multiple'} for qid in ids)
        job = data['_paper_delivery'].get(attempt['id'], {})
        evidence = attempt.get('answer_view_snapshot', {})
        score = attempt.get('correct')
        conflict = (pairs[batch['id']] != 1 or len(ids) != len(set(ids)) or any(qid not in questions for qid in ids)
                    or submitted and (type(score) is not int or not 0 <= score <= objective))
        task = {'task_id': attempt['id'], 'source': 'sharing', 'scope': attempt.get('scope', ''),
                'batch_id': batch['id'],
                'shift': batch.get('shift', ''), 'start_at': batch.get('published_at', ''),
                'due_at': batch.get('due_at') or batch.get('closed_at', ''),
                'assigned': len(ids), 'answered': len(ids) if submitted else 0,
                'correct': score if submitted and not conflict else 0,
                'objective_answered': objective if submitted else 0,
                'answer_viewed': len(ids) if submitted and evidence.get('answer_before_submit') else 0,
                'answer_view_unknown': len(ids) if submitted and (evidence.get('version') != 1 or
                    evidence.get('legacy_unknown') and not evidence.get('answer_before_submit')) else 0,
                'started': bool(attempt.get('draft_answers')), 'uncertain': not ids or conflict or batch.get('status') == 'closed' and not batch.get('closed_at'),
                'delivery_status': _delivery(job or {'status': attempt.get('delivery_status')})}
        if conflict:
            task.update(assigned=None, answered=0, correct=0, objective_answered=0, answer_viewed=0, answer_view_unknown=0)
        task['status'] = _status(task, current)
        yield task


def project(service, date, *, current=None, data=None, scope=''):
    date = day(date)
    current = current or core.now()
    current = current.astimezone(core.TZ) if current.tzinfo else current.replace(tzinfo=core.TZ)
    data = data or _data(service)
    by_person, sharing_by_person = defaultdict(list), defaultdict(list)
    for attempt in data['sharing_attempt']:
        sharing_by_person[attempt.get('person_id')].append(attempt)
    batches = {b['id']: b for b in data['sharing_batch'] if b.get('status') in {'published', 'closed'}}
    data = {**data, '_sharing_batches': batches, '_sharing_by_person': sharing_by_person,
            '_paper_delivery': {j.get('attempt_id'): j for j in data['sharing_delivery'] if j.get('kind') == 'paper'}}
    records = {item['id']: item for item in data['record']}
    saved = {item['person_id']: item for item in data[KIND] if item.get('date') == date and valid(item)}
    people = {p['id']: p for p in data['person'] if p.get('active') or p['id'] in saved}
    papers = [p for p in data['paper'] if p.get('person_id') and not p.get('deleted_at')]
    for paper in papers:
        by_person[paper['person_id']].append(paper)
        if paper.get('date') == date and paper.get('person'):
            people.setdefault(paper['person_id'], paper['person'])
    for attempt in data['sharing_attempt']:
        batch = batches.get(attempt.get('batch_id'), {})
        business_date = batch.get('business_date') or date_of(batch.get('published_at')) or batch.get('date')
        pid = attempt.get('person_id')
        if batch and business_date == date and isinstance(pid, str) and pid:
            scope_value = attempt.get('scope')
            people.setdefault(pid, {'id': pid, 'name': attempt.get('name') or '', 'active': False,
                                    'scopes': [scope_value] if scope_value in core.SCOPES else []})
    for pid, value in saved.items():
        people.setdefault(pid, value['person'])
    owners = defaultdict(set)
    for pid, person in people.items():
        for oid in person.get('login_ids') or []:
            owners[oid].add(pid)
    roster = _snapshot(service, date)
    roster_read_failed = not roster and any(
        (service._get('local', name) or {}).get('date') == date
        for name in ('shift_roster_retry:' + date, 'shift_roster_retry'))
    planned, uncertain_scopes = defaultdict(list), set()
    if roster:
        for duty in roster['people']:
            if not isinstance(duty, dict) or duty.get('scope') not in core.SCOPES:
                uncertain_scopes.update(core.SCOPES)
                continue
            pid = duty.get('person_id')
            candidates = {pid} if pid in people else owners.get(duty.get('open_id'), set())
            if len(candidates) != 1 or duty.get('shift') not in {'白', '夜'}:
                uncertain_scopes.add(duty.get('scope'))
                continue
            pid = next(iter(candidates))
            if duty.get('scope') not in people[pid].get('scopes', []):
                uncertain_scopes.add(duty.get('scope'))
                continue
            if not any(d['scope'] == duty['scope'] and d['shift'] == duty['shift'] for d in planned[pid]):
                planned[pid].append(duty)
    rows = []
    notifications = [n for n in data['notification'] if n.get('date') == date]
    for pid, person in people.items():
        old = saved.get(pid, {})
        duties = [d for d in planned.get(pid, []) if not scope or d['scope'] == scope]
        tasks = []
        person_papers = by_person[pid]
        daily = [p for p in person_papers if p.get('date') == date and p.get('mode') != 'practice'
                 and (not scope or p.get('scope') == scope)]
        shifts = {duty['shift'] for duty in duties}
        for paper in daily:
            shift = paper.get('shift') or (next(iter(shifts)) if len(shifts) == 1 else '')
            notice = next((n for n in notifications if
                           (n.get('paper_id') == paper['id'] or n.get('open_id') in (person.get('login_ids') or []) and n.get('shift') == shift)), {})
            task = _paper_task(paper, records.get(paper['id'], {}), date, shift, current, notice)
            if not shift and len(shifts) > 1:
                task.update(uncertain=True, assigned=None, answered=0, correct=0, objective_answered=0,
                            answer_viewed=0, answer_view_unknown=0, status='待核对')
            tasks.append(task)
        from .learning_reminders import shift_times
        for duty in duties:
            if any(task['shift'] == duty['shift'] and task['scope'] == duty['scope'] for task in tasks):
                continue
            start, due = windows(date, duty['shift'])
            clock = dt.time.fromisoformat(shift_times(service.settings())[duty['shift']])
            published = dt.datetime.combine(dt.date.fromisoformat(date), clock, core.TZ)
            if duty['shift'] == '夜' and published < start:
                published += dt.timedelta(days=1)
            task = {'task_id': 'roster:' + date + ':' + duty['scope'] + ':' + duty['shift'] + ':' + pid, 'source': 'roster',
                    'shift': duty['shift'], 'scope': duty['scope'], 'start_at': max(start, published).isoformat(),
                    'due_at': due.isoformat(), 'assigned': sum(core.QUOTAS.values()), 'answered': 0,
                    'correct': 0, 'objective_answered': 0, 'answer_viewed': 0, 'answer_view_unknown': 0,
                    'delivery_status': _delivery(next((n for n in notifications if
                        n.get('open_id') == duty.get('open_id') and n.get('shift') == duty['shift']), {}))}
            task['status'] = _status(task, current)
            tasks.append(task)
        # A restored frozen plan remains evidence when the local roster cache is absent.
        if not roster:
            known = {task['task_id'] for task in tasks}
            for task in old.get('tasks', []):
                if (task.get('source') == 'roster' and (not scope or task.get('scope') == scope)
                        and task['task_id'] not in known and not any(t['shift'] == task['shift'] and t['scope'] == task['scope'] for t in tasks)):
                    task = copy.deepcopy(task)
                    task['status'] = _status(task, current)
                    tasks.append(task)
        tasks.extend(t for t in _sharing_tasks(data, pid, date, current) if not scope or t['scope'] == scope)
        activity = Counter()
        practice_questions = set()
        source_papers = {task['task_id'] for task in tasks if task['source'] == 'paper'}
        for paper in person_papers:
            if scope and paper.get('scope') != scope:
                continue
            record = records.get(paper['id'], {})
            for question in paper.get('questions', []):
                if question.get('invalid'):
                    continue
                entry = record.get('entries', {}).get(question['id'], {})
                for index, attempt in enumerate([entry.get('attempt'), *entry.get('practice', [])]):
                    if not attempt or date_of(attempt.get('submitted_at')) != date:
                        continue
                    source_papers.add(paper['id'])
                    if paper.get('mode') == 'practice':
                        practice_questions.add((paper['id'], question['id']))
                        activity['practice_attempts'] += 1
                    if index:
                        activity['review_count'] += 1
        complete_roster = bool(roster or old.get('roster_verified')) and not roster_read_failed
        scopes = set(person.get('scopes') or []) | {t['scope'] for t in tasks if t.get('scope')}
        if scope and scope not in scopes:
            continue
        scopes = {scope} if scope else scopes
        unresolved = bool(scopes & uncertain_scopes)
        states = {task['status'] for task in tasks}
        status = next((state for state in ('待核对', '逾期', '答题中', '待答', '未到时') if state in states),
                      '已完成' if tasks else '未安排' if complete_roster else '待核对')
        if unresolved or not complete_roster:
            status = '待核对'
        totals = {field: sum(task.get(field) or 0 for task in tasks) for field in (
            'assigned', 'answered', 'correct', 'objective_answered', 'answer_viewed', 'answer_view_unknown')}
        value = {'schema_version': VERSION, 'id': key(date, pid), 'date': date, 'person_id': pid,
                 'person': {'id': pid, 'name': person.get('name', ''), 'scopes': sorted(scopes), 'active': bool(person.get('active'))},
                 'scopes': sorted(scopes),
                 'roster_verified': complete_roster and not unresolved, 'status': status, 'tasks': tasks,
                 'source_papers': sorted(source_papers),
                 **totals, 'practice_questions': len(practice_questions), 'practice_attempts': activity['practice_attempts'],
                 'review_count': activity['review_count'],
                 'accuracy': round(totals['correct'] * 100 / totals['objective_answered'], 1) if totals['objective_answered'] else None,
                 'delivery_failed': any(t.get('delivery_status') == '发送失败' for t in tasks)}
        value['source_revision'] = hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=True).encode()).hexdigest()
        value['updated_at'] = old.get('updated_at') if old.get('source_revision') == value['source_revision'] else current.isoformat(timespec='seconds')
        rows.append(value)
    return rows


def valid(value):
    try:
        return (isinstance(value, dict) and value.get('schema_version') == VERSION and
                value.get('id') == key(day(value['date']), value['person_id']) and
                isinstance(value['person_id'], str) and bool(value['person_id']) and
                isinstance(value.get('person'), dict) and value['person'].get('id') == value['person_id'] and
                isinstance(value['person'].get('name'), str) and value.get('status') in STATES and
                isinstance(value.get('scopes'), list) and all(s in core.SCOPES for s in value['scopes']) and
                all(type(value.get(k)) is int and value[k] >= 0 for k in (
                    'assigned', 'answered', 'correct', 'objective_answered', 'answer_viewed', 'answer_view_unknown',
                    'practice_questions', 'practice_attempts', 'review_count')) and
                isinstance(value.get('tasks'), list) and all(isinstance(t, dict) and isinstance(t.get('task_id'), str)
                    and t.get('source') in {'paper', 'roster', 'sharing'} and t.get('status') in STATES
                    and t.get('scope') in (*core.SCOPES, '') and t.get('shift') in {'', '白', '夜'}
                    and all(t.get(k) is None and k == 'assigned' or type(t.get(k)) is int and t[k] >= 0
                            for k in ('assigned', 'answered', 'correct', 'objective_answered', 'answer_viewed', 'answer_view_unknown'))
                    for t in value['tasks']) and bool(date_of(value.get('updated_at'))) and
                (value.get('accuracy') is None or type(value['accuracy']) in {int, float} and 0 <= value['accuracy'] <= 100))
    except (KeyError, TypeError, ValueError, core.LearningError):
        return False


def refresh(service, current=None):
    if not getattr(service, '_completion_enabled', False) or not service._restored:
        return
    current = current or core.now()
    current = current.astimezone(core.TZ) if current.tzinfo else current.replace(tzinfo=core.TZ)
    dates = {current.date().isoformat(), (current.date() - dt.timedelta(days=1)).isoformat()}
    # Existing verified paper dates can be backfilled; absent historical rosters stay unknown.
    if not service._get('local', 'completion_initialized'):
        dates.update(p['date'] for p in service._all('paper') if p.get('date') and p.get('person_id'))
        dates.update(b.get('business_date') or date_of(b.get('published_at')) for b in service._all('sharing_batch') if b.get('status') in {'published', 'closed'})
        for record in service._all('record'):
            for entry in record.get('entries', {}).values():
                dates.update(date_of(a.get('submitted_at')) for a in [entry.get('attempt') or {}, *entry.get('practice', [])])
    with closing(service._connect()) as conn:
        pending = {row[0]: row[1] for row in conn.execute("SELECT day,revision FROM documents WHERE kind='local' AND key LIKE 'completion_pending:%'")}
        dates.update(pending)
    dates = {value for value in dates if isinstance(value, str) and value and date_of(value + 'T00:00:00') == value}
    if getattr(service, '_shift_roster_reader', None):
        from .learning_reminders import roster_snapshot
        for date in sorted(dates):
            if date >= (current.date() - dt.timedelta(days=1)).isoformat():
                roster_snapshot(service, date)
    data = _data(service)
    for date in sorted(dates):
        if date > current.date().isoformat():
            continue
        for value in project(service, date, current=current, data=data):
            if date < current.date().isoformat() and not (value['tasks'] or value['practice_attempts'] or value['review_count'] or value['roster_verified']):
                continue
            previous = service._get(KIND, value['id']) or {}
            if previous.get('source_revision') != value['source_revision']:
                with service.transaction() as conn:
                    service._put(KIND, value['id'], value, conn)
        with service.transaction() as conn:
            conn.execute("DELETE FROM documents WHERE kind='local' AND key=? AND revision=?",
                         ('completion_pending:' + date, pending.get(date, -1)))
    with service.transaction() as conn:
        service._put('local', 'completion_initialized', {'version': VERSION}, conn, False)


def report(service, actor, query):
    access = service._access(actor, scope=query.get('scope'), person_id=query.get('person_id'))
    if access['scope'] and access['scope'] not in core.SCOPES:
        raise core.LearningError('楼栋范围无效。')
    query = {key: value for key, value in query.items() if key not in {'date', 'from', 'to'} or value != ''}
    today = core.now().date().isoformat()
    default = query.get('date', today)
    start, end = day(query.get('from', default)), day(query.get('to', default))
    span = (dt.date.fromisoformat(end) - dt.date.fromisoformat(start)).days
    if span < 0 or span > 30:
        raise core.LearningError('一次最多查询31天，开始日期不能晚于结束日期。')
    data, items = _data(service), []
    for offset in range(span + 1):
        date = (dt.date.fromisoformat(start) + dt.timedelta(days=offset)).isoformat()
        for value in project(service, date, data=data, scope=access['scope']):
            if access['person_id'] and value['person_id'] != access['person_id']:
                continue
            if query.get('status') and query['status'] != value['status']:
                continue
            if query.get('shift') and not any(task.get('shift') == query['shift'] for task in value['tasks']):
                continue
            if query.get('q') and str(query['q']).strip().casefold() not in str(value['person'].get('name') or '').casefold():
                continue
            items.append(value)
    items.sort(key=lambda value: (value['date'], value['person'].get('name') or '', value['person_id']), reverse=True)
    try:
        page, size = max(1, int(query.get('page', 1))), min(100, max(1, int(query.get('page_size', 30))))
    except (ValueError, TypeError):
        raise core.LearningError('分页参数无效。') from None
    return {'items': items[(page - 1) * size:page * size], 'total': len(items), 'page': page, 'page_size': size,
            'from': start, 'to': end, 'summary': dict(Counter(value['status'] for value in items)), 'states': list(STATES)}


def display_fields(value):
    if not valid(value):
        raise ValueError('Invalid daily progress entity')
    midnight = dt.datetime.combine(dt.date.fromisoformat(value['date']), dt.time.min, core.TZ)
    return {'日期': int(midnight.timestamp() * 1000), '人员标识': value['person_id'], '姓名': value['person'].get('name') or '',
            '楼栋': '、'.join(value['scopes']), '班次': '、'.join(sorted({task.get('shift') for task in value['tasks'] if task.get('shift')})),
            '完成状态': value['status'], '应答题数': value['assigned'], '已答题数': value['answered'], '正确题数': value['correct'],
            '首次客观正确率': value['accuracy'], '看答案后数': value['answer_viewed'] if not value['answer_view_unknown'] else None,
            '主动练习量': value['practice_questions'], '复习次数': value['review_count'],
            '更新时间': int(dt.datetime.fromisoformat(value['updated_at']).timestamp() * 1000)}
