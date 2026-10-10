"""Personal allocation and bounded local projections for the learning service."""
import copy
import datetime as dt
import time
from collections import Counter, defaultdict
from contextlib import closing

from . import learning as core
from .learning import BANKS, QUOTAS, SCOPES, LearningError, digest, stamp


def _directory_open_ids(directory):
    """Real Feishu open_id per staff/external record id (nothing fabricated)."""
    staff, external = {}, {}
    for source, bucket in (('staff', staff), ('external', external)):
        for row in directory.get(source) or []:
            rid = str(row.get('record_id') or '').strip()
            open_id = str(row.get('open_id') or '').strip()
            if rid and open_id:
                bucket.setdefault(rid, open_id)
    return staff, external


def _person_login_ids(row, staff_open_ids, external_open_ids):
    """Only real nonblank Feishu open ids may be private login login ids.

    The resolved person itself may be an external signature row without the
    original staff open id, so the explicit staff:record alias is followed
    back to the matching original staff row. Record ids are never login ids.
    """
    login_ids = set()
    own = str(row.get('open_id') or '').strip()
    if own:
        login_ids.add(own)
    for alias in row.get('record_aliases') or []:
        alias = str(alias or '').strip()
        source, _, rid = alias.partition(':')
        if not source or not rid:
            continue
        open_id = (staff_open_ids if source == 'staff' else
                   external_open_ids if source == 'external' else {}).get(rid)
        if open_id:
            login_ids.add(open_id)
    return sorted(login_ids)


def refresh_people(service):
    if service._people_reader is None:
        return
    directory = service._people_reader()
    if not isinstance(directory, dict) or not isinstance(directory.get('people'), list) or any(
            not source.get('ok') for source in directory.get('sources', {}).values()):
        raise LearningError('人员目录未完整读取，已保留上次名单。', 503)
    from .lighthouse_sources import codes
    staff_open_ids, external_open_ids = _directory_open_ids(directory)
    with service.transaction() as conn:
        previous = service._all('person', conn)
        aliases = defaultdict(set)
        for old in previous:
            for alias in old.get('aliases', []):
                aliases[alias].add(old['id'])
        seen, issues = set(), []
        for row in directory['people']:
            keys = sorted(set([row.get('person_key', ''), *row.get('record_aliases', [])]) - {''})
            staff = [key for key in keys if key.startswith('staff:')]
            login_ids = _person_login_ids(row, staff_open_ids, external_open_ids)
            matches = set().union(*(aliases[key] for key in keys))
            scopes = sorted(codes(row.get('building')) & set(SCOPES))
            if not keys or len(staff) > 1 or len(matches) > 1 or row.get('identity_warning') or not scopes:
                issues.append({'name': row.get('name', ''), 'employee_no': row.get('employee_no', ''),
                               'reason': '人员身份或楼栋需核对'})
                continue
            identity = next(iter(matches)) if matches else 'person_' + digest(staff[0] if staff else keys[0])[:32]
            person = {'id': identity, 'person_id': identity, 'name': row.get('name', ''),
                      'employee_no': row.get('employee_no', ''), 'scopes': scopes, 'aliases': keys,
                      'login_ids': login_ids, 'active': True}
            old = service._get('person', identity, conn)
            if not old or any(old.get(k) != v for k, v in person.items()):
                service._put('person', identity, person, conn)
            seen.add(identity)
        for old in previous:
            if old['id'] not in seen and old.get('active'):
                service._put('person', old['id'], {**old, 'active': False}, conn)
        service._put('local', 'people_sync', {'at': stamp(), 'checked_at': time.time(), 'issues': issues}, conn, False)


def person(service, identity, *, scope='', active=False, conn=None):
    value = service._get('person', str(identity or ''), conn)
    if not value:
        raise LearningError('请选择人员；人员目录尚未同步时请稍后重试。', 404)
    if active and (not value.get('active') or scope not in value['scopes']):
        raise LearningError('人员已停用或不属于所选楼栋，请重新选择。', 409)
    return {k: value.get(k) for k in ('id', 'name', 'employee_no', 'scopes', 'active')}


def _request_login_refresh(service):
    """Cold-upgrade: existing people lack login_ids. Wake the existing worker
    (never start a new one) but throttle locally so each page read is cheap and
    never triggers a synchronous full directory fetch."""
    now = time.time()
    last = float(getattr(service, '_people_login_refresh_at', 0.0) or 0.0)
    if now - last < 300:
        return
    service._people_login_refresh_at = now
    request = getattr(service, 'request_refresh', None)
    if callable(request):
        try:
            request()
        except Exception:
            pass


def resolve_self(service, open_id):
    """Stable mapping from the session login to exactly one active local person.

    Matching uses only private real-Feishu-open-id login_ids persisted by
    refresh_people; it never falls back to name/employee-number joins. Reads are
    strictly local: a cold upgrade (people already present, login_ids missing)
    only wakes the existing background worker, which is throttled here.
    """
    people_rows = service._all('person')
    login_sets = [p.get('login_ids') or [] for p in people_rows]
    matches = [p for p, login_ids in zip(people_rows, login_sets)
               if p.get('active') and open_id in set(login_ids)]
    empty = {'person_id': '', 'person': None, 'identity_issue': ''}
    if not matches:
        if people_rows and any(not login_ids for login_ids in login_sets):
            _request_login_refresh(service)
        return {**empty, 'identity_issue': '登录账号与人员名单尚未关联，请管理员先同步人员目录。'}
    if len(matches) > 1:
        return {**empty, 'identity_issue': '登录账号关联到多个人员，请管理员核对后重试。'}
    person_row = matches[0]
    return {'person_id': person_row['id'], 'person': person(service, person_row['id']), 'identity_issue': ''}


def people(service, actor, query):
    scope = service._scope(actor, query.get('scope'))
    words = str(query.get('q') or '').casefold().split()
    self_pid = actor.get('person_id') or ''
    if not actor.get('is_admin') and not actor.get('shared_account'):
        # Ordinary personal accounts see only themselves; never enumerate peers.
        rows = [row for row in service._all('person') if row.get('active') and row['id'] == self_pid]
    else:
        rows = [row for row in service._all('person') if row.get('active')
                and (not scope or scope in row['scopes'])]
    rows = [{k: row.get(k) for k in ('id', 'name', 'employee_no', 'scopes', 'active')} for row in rows
            if all(word in (row['name'] + ' ' + row['employee_no']).casefold() for word in words)]
    rows.sort(key=lambda row: (row['name'], row['employee_no'], row['id']))
    synced = service._get('local', 'people_sync') or {}
    return {**service._page(rows, query), 'updated_at': synced.get('at', ''),
            'issues': synced.get('issues', []) if actor.get('is_admin') else [], 'ready': bool(synced.get('at'))}


def candidates(service, conn):
    return [q for q in service._all('question', conn) if q.get('status') == 'published' and not q.get('problems') and not q['_dirty']]


def choose(questions, seed, *, excluded=(), counts=None, last=None, prepared=None, original=()):
    used, selected = set(excluded), []
    counts, last, prepared = counts or {}, last or {}, prepared if prepared is not None else Counter()
    for bank, wanted in QUOTAS.items():
        # A reserve is a snapshot; check current availability before assigning it.
        available = {q['id']: q for q in questions if q['bank'] == bank}
        kept = [available[q['id']] for q in original if q['bank'] == bank and q['id'] in available
                and available[q['id']]['version'] == q['version']]
        ordered = sorted(available.values(), key=lambda q: (counts.get(q['family_id'], 0),
            prepared.get(q['family_id'], 0), last.get(q['family_id'], ''), digest([seed, q['family_id']])))
        found = 0
        for q in [*kept, *ordered]:
            if q['family_id'] in used:
                continue
            selected.append({k: copy.deepcopy(v) for k, v in q.items() if not k.startswith('_')})
            used.add(q['family_id']); prepared[q['family_id']] += 1; found += 1
            if found == wanted:
                break
    return selected, {bank: count - sum(q['bank'] == bank for q in selected) for bank, count in QUOTAS.items()}


def prepare_batch(service, scope, day, conn, *, prepared=None):
    batch = conn.execute("SELECT COALESCE(MAX(CAST(json_extract(payload,'$.batch') AS INTEGER)),0)+1 FROM documents WHERE kind='reserve' AND scope=? AND day=?", (scope, day)).fetchone()[0]
    last = dict(conn.execute('SELECT family_id,MAX(day) FROM learning_usage GROUP BY family_id'))
    available, result = candidates(service, conn), []
    prepared = prepared if prepared is not None else Counter(q['family_id'] for r in service._documents('reserve', scope=scope, start=day, end=day, conn=conn) for q in r['questions'])
    for slot in range(4):
        identity = f'reserve:{day}:{scope}:{batch}:{slot}'
        questions, shortage = choose(available, identity, last=last, prepared=prepared)
        value = {'id': identity, 'date': day, 'scope': scope, 'batch': batch, 'questions': questions,
                 'shortage': shortage, 'created_at': stamp(), 'person_id': ''}
        service._put('reserve', identity, value, conn)
        result.append(identity)
    return result


def publish(service, day):
    date = day.isoformat()
    with service.transaction() as conn:
        key = service._publication_key(date)
        previous = service._get('publication', key, conn)
        if previous:
            return previous
        notify = (service._get('local', 'manual_publish', conn) or {}).get('date') != date
        reserve_ids, prepared = [], Counter()
        for scope in SCOPES:
            reserve_ids.extend(prepare_batch(service, scope, date, conn, prepared=prepared))
            nid = f'publish:{date}_{scope}'
            if notify and not service._get('notification', nid, conn):
                service._put('notification', nid, {'id': nid, 'kind': 'publish', 'scope': scope,
                    'date': date, 'status': 'pending', 'personal_mode': True}, conn)
        result = {'date': date, 'created_at': stamp(), 'reserve_ids': reserve_ids, 'paper_ids': [], 'notify': notify, 'personal_mode': True}
        service._put('publication', key, result, conn)
    return result


def claim(service, actor, payload):
    scope, self_person = service._claim_context(actor, payload)
    day = core.now().date()
    date = day.isoformat()
    if payload.get('date', date) != date:
        raise LearningError('只能领取今日题单。')
    if not service._restored:
        service._restore_requested = True
        service._wake.set()
        raise LearningError('学习记录正在恢复，完成后即可领取；不会重复分配。', 503)
    with service.transaction() as conn:
        learner = person(service, self_person['id'], scope=scope, active=True, conn=conn)
        identity = 'personal:' + date + ':' + learner['id']
        existing = service._get('paper', identity, conn)
        if existing:
            if existing.get('deleted_at'):
                raise LearningError('该人员今日题单已由管理员删除，今日不重复分配。', 409)
            return service.public_paper(existing, actor)
        publication = service._get('publication', service._publication_key(date), conn)
        if not publication:
            raise LearningError('今日题单尚未发布。', 409)
        if any(not service._get('reserve', rid, conn) for rid in publication.get('reserve_ids', [])):
            raise LearningError('备用题单恢复尚不完整，请先同步学习记录。', 503)
        rows = conn.execute("SELECT payload FROM documents WHERE kind='reserve' AND scope=? AND day=? AND person_id='' ORDER BY key LIMIT 1", (scope, date)).fetchall()
        if not rows:
            prepare_batch(service, scope, date, conn)
            rows = conn.execute("SELECT payload FROM documents WHERE kind='reserve' AND scope=? AND day=? AND person_id='' ORDER BY key LIMIT 1", (scope, date)).fetchall()
        import json
        reserve = json.loads(rows[0]['payload'])
        recent = {row[0] for row in conn.execute('SELECT family_id FROM learning_usage WHERE person_id=? AND day BETWEEN ? AND ?',
            (learner['id'], (day - dt.timedelta(days=6)).isoformat(), date))}
        counts = dict(conn.execute('SELECT family_id,COUNT(*) FROM learning_usage WHERE person_id=? GROUP BY family_id', (learner['id'],)))
        last = dict(conn.execute('SELECT family_id,MAX(day) FROM learning_usage GROUP BY family_id'))
        questions, shortage = choose(candidates(service, conn), identity, excluded=recent, counts=counts, last=last, original=reserve['questions'])
        paper = {'id': identity, 'person_id': learner['id'], 'person': learner, 'date': date, 'scope': scope,
                 'created_at': stamp(), 'claimed_by': actor['id'], 'claimed_by_name': actor.get('name', ''),
                 'questions': questions, 'shortage': shortage, 'reserve_id': reserve['id'], 'notify': publication['notify']}
        service._put('paper', identity, paper, conn)
        service._put('reserve', reserve['id'], {**reserve, 'person_id': learner['id'], 'paper_id': identity}, conn)
    return service.public_paper(paper, actor)


def profile(service, actor, query):
    access = service._access(actor, scope=query.get('scope'), person_id=str(query.get('person_id') or ''))
    scope, identity = access['scope'], access['person_id']
    ordinary = not actor.get('is_admin') and not actor.get('shared_account')
    learner = person(service, identity) if identity else None
    if identity and not actor.get('shared_account'):
        scope = ''  # Personal history follows the person, not a later building transfer.
    if not query.get('from') and not query.get('to') and not query.get('period'):
        query = {**query, 'from': (core.now().date() - dt.timedelta(days=6)).isoformat(), 'to': core.now().date().isoformat()}
    query = service._date_filters(query)
    start, end = query.get('from') or (core.now().date() - dt.timedelta(days=6)).isoformat(), query.get('to') or core.now().date().isoformat()
    if start and (dt.date.fromisoformat(end) - dt.date.fromisoformat(start)).days > 366:
        raise LearningError('单次统计最多366天，请缩小日期范围。')
    clauses, args = ["person_id<>''", 'invalid=0'], []
    for column, value, op in (('scope', scope, '='), ('person_id', identity, '='), ('day', start, '>='), ('day', end, '<=')):
        if value:
            clauses.append(column + op + '?'); args.append(value)
    with closing(service._connect()) as conn:
        results = [dict(row) for row in conn.execute('SELECT * FROM learning_results WHERE ' + ' AND '.join(clauses), args)]
        practices = [dict(row) for row in conn.execute('SELECT * FROM learning_practice WHERE ' + ' AND '.join(c for c in clauses if c != 'invalid=0'), args)]
    papers = [p for p in service._documents('paper', scope=scope, person_id=identity, start=start, end=end) if not p.get('deleted_at')]
    records = {p['id']: service._get('record', p['id']) or {} for p in papers}
    # Older papers can have practice submissions inside the requested period.
    for paper_id in {r['paper_id'] for r in practices}:
        if paper_id not in records:
            records[paper_id] = service._get('record', paper_id) or {}
    practice_times = {}
    practices_by_person, practices_by_day = defaultdict(list), defaultdict(list)
    for row in practices:
        key = (row['paper_id'], row['question_id'])
        if key not in practice_times:
            entry = records[row['paper_id']].get('entries', {}).get(row['question_id'], {})
            practice_times[key] = {a.get('operation_id') or str(n): a['submitted_at'] for n, a in enumerate(entry.get('practice', []))}
        row['submitted_at'] = practice_times[key].get(row['operation_id'], row['day'])
        practices_by_person[row['person_id']].append(row)
        practices_by_day[row['day']].append(row)
    ratio = lambda a, b: round(a * 100 / b, 1) if b else None
    def summarize(rows, assigned, *, pid='', building='', practice_rows=None):
        choices = [r for r in rows if r['type'] != 'interview']
        independent = [r for r in choices if not r['assisted']]
        completed = sum(service._completed(p, records[p['id']]) for p in assigned)
        task_answered = sum(bool(records[p['id']].get('entries', {}).get(q['id'], {}).get('attempt')) and not records[p['id']].get('entries', {}).get(q['id'], {}).get('needs_review') for p in assigned for q in p['questions'] if not q.get('invalid'))
        total = sum(sum(not q.get('invalid') for q in p['questions']) for p in assigned)
        if practice_rows is None:
            practice_rows = practices_by_person.get(pid, []) if pid else practices
        own_practices = [r for r in practice_rows if (not pid or r['person_id'] == pid) and (not building or r['scope'] == building)]
        activity = rows + own_practices
        practice_count = len(own_practices)
        return {'answered': sum(not r['needs_review'] for r in rows), 'wrong': sum(r['correct'] == 0 for r in choices), 'correct': sum(r['correct'] == 1 for r in choices),
            'hinted': sum(bool(r['assisted']) for r in rows if not r['needs_review']),
            'independent_correct': sum(r['correct'] == 1 for r in independent), 'independent_answered': len(independent),
            'choice_answered': len(choices), 'accuracy': ratio(sum(r['correct'] == 1 for r in choices), len(choices)),
            'independent_accuracy': ratio(sum(r['correct'] == 1 for r in independent), len(independent)),
            'hint_rate': ratio(sum(bool(r['assisted']) for r in rows), len(rows)), 'learning_days': len({r['day'] for r in activity}),
            'interview_total': sum(r['type'] == 'interview' for r in rows), 'practice_count': practice_count,
            'attempt_count': len(rows) + practice_count,
            'interview_ratings': dict(Counter(r['self_rating'] for r in rows if r['type'] == 'interview')),
            'review_total': sum(r['correct'] == 0 or r['type'] == 'interview' or r['needs_review'] for r in rows),
            'assigned': total, 'task_answered': task_answered, 'papers': len(assigned), 'completed': completed,
            'completion_rate': ratio(task_answered, total), 'received_people': len({p['person_id'] for p in assigned}),
            'answered_people': len({r['person_id'] for r in activity}),
            'completed_people': len({p['person_id'] for p in assigned if service._completed(p, records[p['id']])}),
            'not_started_people': len({p['person_id'] for p in assigned if not records[p['id']].get('entries') or not any(e.get('attempt') for e in records[p['id']]['entries'].values())})}
    summary = summarize(results, papers)
    people_rows, buildings = [], []
    ids = {r['person_id'] for r in results + practices} | {p['person_id'] for p in papers}
    today = core.now().date().isoformat()
    for pid in sorted(ids):
        own = [p for p in papers if p['person_id'] == pid]
        latest = own[-1] if own else {}
        p = service._get('person', pid) or latest.get('person') or {'id': pid, 'name': '历史人员', 'employee_no': ''}
        rows = [r for r in results if r['person_id'] == pid]
        activity = rows + practices_by_person.get(pid, [])
        latest_activity = max(activity, key=lambda r: r['submitted_at'], default={})
        today_paper = service._get('paper', 'personal:' + today + ':' + pid)
        progress = service.public_paper(today_paper, actor)['stats'] if today_paper and not today_paper.get('deleted_at') and (not scope or today_paper['scope'] == scope) else None
        people_rows.append({'person_id': pid, 'name': p['name'], 'employee_no': p.get('employee_no', ''),
            'scope': latest.get('scope') or latest_activity.get('scope', ''), 'summary': summarize(rows, own, pid=pid), 'today': progress,
            'last_answered_at': latest_activity.get('submitted_at', '')})
    for code in SCOPES:
        if not scope or scope == code:
            buildings.append({'scope': code, **summarize([r for r in results if r['scope'] == code], [p for p in papers if p['scope'] == code], building=code)})
    topics = []
    for topic in sorted({r['topic'] for r in results}):
        rows = [r for r in results if r['topic'] == topic and r['type'] != 'interview']
        topics.append({'topic': topic, 'answered': len(rows), 'wrong': sum(r['correct'] == 0 for r in rows), 'accuracy': ratio(sum(r['correct'] == 1 for r in rows), len(rows))})
    bank_rows = []
    for bank, label in BANKS.items():
        rows = [r for r in results if r['bank'] == bank and r['type'] != 'interview']
        bank_rows.append({'bank': bank, 'label': label, 'answered': len(rows), 'wrong': sum(r['correct'] == 0 for r in rows)})
    first = dt.date.fromisoformat(start or min((r['day'] for r in results), default=today))
    last = dt.date.fromisoformat(end)
    trend = []
    for offset in range(min(367, max(0, (last - first).days + 1))):
        day = (first + dt.timedelta(days=offset)).isoformat()
        rows = [r for r in results if r['day'] == day]
        practice_rows = practices_by_day.get(day, [])
        choices = [r for r in rows if r['type'] != 'interview']
        trend.append({'date': day, 'answered': len(rows), 'practice_count': len(practice_rows), 'attempt_count': len(rows) + len(practice_rows),
                      'people': len({r['person_id'] for r in rows + practice_rows}), 'accuracy': ratio(sum(r['correct'] == 1 for r in choices), len(choices))})
    distribution = [{'label': label, 'count': sum(low <= p['summary']['accuracy'] <= high for p in people_rows if p['summary']['accuracy'] is not None)}
                    for label, low, high in [('0-59%', 0, 59.9), ('60-79%', 60, 79.9), ('80-99%', 80, 99.9), ('100%', 100, 100)]]
    questions = []
    if actor.get('is_admin'):
        for qid in sorted({r['question_id'] for r in results}):
            own = [r for r in results if r['question_id'] == qid and r['type'] != 'interview']
            if own:
                questions.append({'id': qid, 'stem': own[0]['stem'], 'answered': len(own), 'wrong': sum(r['correct'] == 0 for r in own)})
    available = candidates(service, None) if actor.get('is_admin') else []
    inventory = {bank: {'available': len({q['family_id'] for q in available if q['bank'] == bank})} for bank in BANKS}
    today_rows = [r for r in results if r['day'] == today]
    today_papers = [p for p in service._documents('paper', scope=scope, person_id=identity, start=today, end=today) if not p.get('deleted_at')]
    for p in today_papers:
        records.setdefault(p['id'], service._get('record', p['id']) or {})
    return {'person': learner, 'summary': summary, 'today_summary': summarize(today_rows, today_papers, practice_rows=practices_by_day.get(today, [])),
            'buildings': [] if ordinary else buildings, 'people': people_rows, 'trend': trend,
            'topics': topics, 'banks': bank_rows, 'distribution': [] if ordinary else distribution,
            'questions': questions, 'inventory': {} if ordinary else (inventory if actor.get('is_admin') else {}),
            'without_choice_answers': sum(p['summary']['accuracy'] is None for p in people_rows), 'from': first.isoformat(), 'to': end,
            'published': bool(service._get('publication', service._publication_key(today)))}
