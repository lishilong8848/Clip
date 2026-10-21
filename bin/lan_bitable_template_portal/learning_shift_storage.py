"""Person/shift cloud snapshots; local business entities keep their original keys."""
import datetime as dt
import json
import re
import time
from collections import defaultdict
from contextlib import closing
from types import SimpleNamespace

from . import learning as core
from . import learning_completion as completion

KINDS = {'paper', 'record', 'reserve', 'notification', 'engagement', 'sharing_attempt',
         'sharing_delivery', 'daily_progress'}
SHIFTS = {'白', '夜'}
HISTORY_VERSION = 2


def key(owner):
    date, shift, pid = owner
    return '班次人员:' + date + ':' + shift + ':' + pid


def snapshot(service):
    with closing(service._connect()) as conn:
        conn.execute('BEGIN')
        return {(row['kind'], row['key']): {
            'payload': json.loads(row['payload']), 'revision': row['revision'],
            'dirty': bool(row['dirty']), 'updated': row['updated'],
        } for row in conn.execute('SELECT kind,key,payload,revision,dirty,updated FROM documents')}


class Snapshot:
    def __init__(self, documents):
        self.documents = documents
        self.people = {k: v['payload'] for (kind, k), v in documents.items() if kind == 'person'}
        self.owners = defaultdict(set)
        for pid, person in self.people.items():
            for oid in person.get('login_ids') or []:
                self.owners[oid].add(pid)
        self._resolved = {}
        self.dependencies = defaultdict(set)
        self._duties = {}
        self.records_by_person = defaultdict(list)
        for (kind, identity), row in documents.items():
            if kind == 'record':
                self.records_by_person[self.get('paper', identity).get('person_id')].append((identity, row))

    def legacy(self, kind, identity):
        return self.get('local', 'shift_legacy:' + kind + ':' + identity).get('legacy') is True

    def get(self, kind, identity):
        return self.documents.get((kind, identity), {}).get('payload', {})

    def duties(self, pid, date):
        if (pid, date) in self._duties:
            return self._duties[pid, date]
        roster = self.get('local', 'shift_roster:' + date) or self.get('local', 'shift_roster')
        if roster.get('date') != date:
            return []
        values = [d for d in roster.get('people', []) if isinstance(d, dict) and d.get('shift') in SHIFTS
                  and (d.get('person_id') == pid or self.owners.get(d.get('open_id')) == {pid})]
        self._duties[pid, date] = values
        return values

    def at_time(self, pid, timestamp):
        from .pre_shift_checks import windows
        try:
            moment = dt.datetime.fromisoformat(timestamp)
            moment = moment.astimezone(core.TZ) if moment.tzinfo else moment.replace(tzinfo=core.TZ)
        except (ValueError, TypeError):
            return None
        matches = set()
        for date in (moment.date(), moment.date() - dt.timedelta(days=1)):
            for duty in self.duties(pid, date.isoformat()):
                start, end = windows(date.isoformat(), duty['shift'])
                if start <= moment < end:
                    matches.add((date.isoformat(), duty['shift'], pid))
        return next(iter(matches)) if len(matches) == 1 else (moment.date().isoformat(), '未归班', pid)

    def owner(self, kind, identity):
        pair = (kind, identity)
        if pair in self._resolved:
            return self._resolved[pair]
        self._resolved[pair] = None
        value = self.get(kind, identity)
        if kind not in KINDS or not value:
            return None
        frozen = self.get('local', 'shift_owner:' + kind + ':' + identity).get('owner')
        if isinstance(frozen, list) and len(frozen) == 3:
            if value.get('person_id') not in (None, '', frozen[2]):
                raise core.LearningError('学练记录人员与已确认的班次归属不同，未合并。', 409)
            linked = None
            if kind == 'record' or value.get('paper_id'):
                linked = self.get('paper', identity if kind == 'record' else value['paper_id'])
            elif kind == 'sharing_delivery' and value.get('attempt_id'):
                linked = self.get('sharing_attempt', value['attempt_id'])
            if linked is not None and linked.get('person_id') != frozen[2]:
                raise core.LearningError('学练记录与关联题单人员不一致，未合并。', 409)
            self._resolved[pair] = tuple(frozen)
            return tuple(frozen)
        # Weekly/group notifications and unclaimed reserves remain system records.
        if kind == 'engagement' and value.get('kind') not in {'timeout', 'shift_timeout'}:
            return None
        if kind == 'sharing_delivery' and not value.get('attempt_id'):
            return None
        linked_kind, linked = '', ''
        if kind == 'record':
            linked_kind, linked = 'paper', identity
        elif kind in {'reserve', 'notification', 'engagement'} and value.get('paper_id'):
            linked_kind, linked = 'paper', value['paper_id']
        elif kind == 'sharing_delivery':
            linked_kind, linked = 'sharing_attempt', value['attempt_id']
        if linked:
            if not self.get(linked_kind, linked):
                raise core.LearningError('学练关联题单尚未就绪，保留原任务等待同步。', 503)
            linked_owner = self.owner(linked_kind, linked)
            if linked_owner and value.get('person_id') not in (None, '', linked_owner[2]):
                raise core.LearningError('学练记录与题单人员不一致，未合并。', 409)
            self._resolved[pair] = linked_owner
            return linked_owner
        pid = value.get('person_id') or ''
        if not pid and kind == 'notification' and value.get('open_id'):
            candidates = self.owners.get(value['open_id'], set())
            if len(candidates) != 1:
                raise core.LearningError('学练提醒人员身份尚未唯一匹配，保留等待同步。', 503)
            pid = next(iter(candidates))
        if not pid:
            return None
        date = value.get('date') or ''
        shift = value.get('shift') or ''
        if kind == 'daily_progress':
            shifts = sorted({t.get('shift') for t in value.get('tasks', []) if t.get('shift') in SHIFTS})
            if not value.get('tasks') and not value.get('practice_attempts') and not value.get('review_count'):
                return None
            shift = shifts[0] if shifts else ''
        elif kind == 'sharing_attempt':
            batch = self.get('sharing_batch', value.get('batch_id', ''))
            date = batch.get('business_date') or completion.date_of(batch.get('published_at')) or batch.get('date') or date
            shift = batch.get('shift') or shift
        if not date:
            date = completion.date_of(value.get('created_at'))
        try:
            date = completion.day(date)
        except core.LearningError:
            raise core.LearningError('学练记录缺少可靠日期，保留原记录等待核对。', 503) from None
        if shift not in SHIFTS:
            choices = {d['shift'] for d in self.duties(pid, date)}
            shift = next(iter(choices)) if len(choices) == 1 else '未归班'
        if kind == 'paper' and value.get('mode') == 'practice':
            timed = self.at_time(pid, value.get('created_at'))
            if timed:
                date, shift, _ = timed
        owner = (date, shift, pid)
        self._resolved[pair] = owner
        return owner

    def activity_owner(self, paper, timestamp):
        owner = self.at_time(paper.get('person_id'), timestamp)
        if owner and owner[1] == '未归班':
            original = self.owner('paper', paper['id'])
            if original and original[1] in SHIFTS:
                from .pre_shift_checks import windows
                moment = dt.datetime.fromisoformat(timestamp)
                moment = moment.astimezone(core.TZ) if moment.tzinfo else moment.replace(tzinfo=core.TZ)
                start, end = windows(original[0], original[1])
                if start <= moment < end:
                    return original
        return owner

    def groups(self):
        groups, errors = defaultdict(list), {}
        for pair, row in self.documents.items():
            if pair[0] not in KINDS or self.legacy(*pair):
                continue
            try:
                owner = self.owner(*pair)
                if owner:
                    groups[owner].append((pair, row))
                    paper_id = row['payload'].get('paper_id') or (pair[1] if pair[0] == 'record' else '')
                    for dependency in (('paper', paper_id), ('record', paper_id)):
                        if paper_id and dependency in self.documents:
                            self.dependencies[owner].add(dependency)
            except core.LearningError as exc:
                errors[pair] = str(exc)
        # A person's daily projection can contain both shifts, without duplicating its backup.
        for (kind, _), row in self.documents.items():
            value = row['payload']
            if kind == 'daily_progress' and not self.legacy(kind, value['id']) and completion.valid(value):
                for task in value['tasks']:
                    if task.get('shift') in SHIFTS:
                        owner = (value['date'], task['shift'], value['person_id'])
                        groups.setdefault(owner, [])
                        self.dependencies[owner].add((kind, value['id']))
        # Later reviews belong to the shift when performed; the original answer snapshot stays intact.
        for (kind, identity), row in self.documents.items():
            if kind != 'record' or self.legacy(kind, identity):
                continue
            paper = self.get('paper', identity)
            pid = paper.get('person_id')
            if not pid:
                continue
            for entry in row['payload'].get('entries', {}).values():
                attempts = entry.get('practice', [])
                if paper.get('mode') == 'practice' and entry.get('attempt'):
                    attempts = [entry['attempt'], *attempts]
                for attempt in attempts:
                    owner = self.activity_owner(paper, attempt.get('submitted_at'))
                    if owner:
                        groups.setdefault(owner, [])
                        self.dependencies[owner].add((kind, identity))
        return groups, errors

    def summary(self, owner, members):
        date, shift, pid = owner
        current = core.now()
        tasks, scopes, practices, reviews = [], set(), set(), 0
        person = self.people.get(pid, {})
        latest = date + 'T00:00:00+08:00'
        paper_ids = set()
        for pair, row in members:
            value = row['payload']
            latest = max(latest, row['updated'])
            if value.get('scope'):
                scopes.add(value['scope'])
            if pair[0] == 'paper':
                person = person or value.get('person') or {}
                paper_ids.add(pair[1])
            elif pair[0] == 'record':
                paper_ids.add(pair[1])
            elif value.get('paper_id'):
                paper_ids.add(value['paper_id'])
        for paper_id in sorted(paper_ids):
            paper = self.get('paper', paper_id)
            person = person or paper.get('person') or {}
            if paper and paper.get('mode') != 'practice' and not paper.get('deleted_at'):
                notices = [r['payload'] for p, r in members if p[0] == 'notification']
                notice = next((n for n in notices if n.get('paper_id') == paper_id), notices[-1] if notices else {})
                tasks.append(completion._paper_task(paper, self.get('record', paper_id), date,
                                                   shift if shift in SHIFTS else '', current, notice))
        daily = self.get('daily_progress', completion.key(date, pid))
        known = {task['task_id'] for task in tasks}
        for task in daily.get('tasks', []):
            if (task.get('shift') or '未归班') == shift and task['task_id'] not in known:
                tasks.append(task)
                scopes.add(task.get('scope', ''))
        for identity, row in self.records_by_person[pid]:
            paper = self.get('paper', identity)
            if paper.get('deleted_at'):
                continue
            for qid, entry in row['payload'].get('entries', {}).items():
                for index, attempt in enumerate([entry.get('attempt'), *entry.get('practice', [])]):
                    if not attempt or self.activity_owner(paper, attempt.get('submitted_at')) != owner:
                        continue
                    if index:
                        reviews += 1
                    if paper.get('mode') == 'practice':
                        practices.add((identity, qid))
                    if index or paper.get('mode') == 'practice':
                        latest = max(latest, row['updated'])
                        scopes.add(paper.get('scope', ''))
        if not scopes:
            scopes.update(d.get('scope', '') for d in self.duties(pid, date) if d['shift'] == shift)
        states = {t['status'] for t in tasks}
        status = next((s for s in ('待核对', '逾期', '答题中', '待答', '未到时') if s in states),
                      '已完成' if tasks else '未开始' if any(p[0] == 'notification' for p, _ in members) else '未安排')
        if shift == '未归班':
            status = '待核对'
        totals = {field: sum(t.get(field) or 0 for t in tasks) for field in
                  ('assigned', 'answered', 'correct', 'objective_answered', 'answer_viewed', 'answer_view_unknown')}
        return {'date': date, 'shift': shift, 'person_id': pid, 'name': person.get('name', ''),
                'scopes': sorted(scopes - {''}), 'status': status, 'tasks': tasks, **totals,
                'practice_questions': len(practices), 'review_count': reviews, 'updated_at': latest}


def identity_rows(person, directory):
    aliases = {str(person.get('id') or ''), *(person.get('aliases') or [])}
    ids = {alias.split(':', 1)[1] for alias in aliases if alias.startswith('directory:')}
    rows = [(rid, row) for rid, row in directory.items() if rid in ids]
    if not rows:
        login_ids = set(person.get('login_ids') or [])
        rows = [(rid, row) for rid, row in directory.items() if login_ids.intersection(row.get('login_ids') or [])]
    return rows


def person_open_id(person, directory):
    rows = identity_rows(person, directory)
    if len(rows) != 1:
        return ''
    candidates = {oid for oid in rows[0][1].get('login_ids') or [] if re.fullmatch(r'ou_[A-Za-z0-9]+', oid)}
    candidates = {oid for oid in candidates if sum(oid in (row.get('login_ids') or []) for row in directory.values()) == 1}
    return next(iter(candidates)) if len(candidates) == 1 else ''


def history_identity(state, item):
    """Use explicit recipient names only, never task titles, file names or operators."""
    kind, identity, value = item['kind'], item['key'], item['payload']
    if not isinstance(value, dict):
        return {'person': {}, 'names': [], 'required': kind in KINDS}
    personal = kind in {'person', 'paper', 'record', 'sharing_attempt', 'daily_progress', 'shift_person'}
    personal = personal or kind == 'reserve' and bool(value.get('person_id'))
    personal = personal or kind == 'notification' and bool(value.get('person_id') or value.get('open_id') or value.get('paper_id'))
    personal = personal or kind == 'engagement' and value.get('kind') in {'timeout', 'shift_timeout', 'weekly'}
    personal = personal or kind == 'sharing_delivery' and bool(value.get('person_id') or value.get('attempt_id'))
    if not personal:
        return {'person': {}, 'names': [], 'required': False}
    pid = value.get('person_id') or (identity if kind == 'person' else '')
    original = value.get('person') if isinstance(value.get('person'), dict) else {}
    linked = {}
    if kind == 'record' or value.get('paper_id'):
        linked = state.get('paper', identity if kind == 'record' else value['paper_id'])
    elif kind == 'sharing_delivery' and value.get('attempt_id'):
        linked = state.get('sharing_attempt', value['attempt_id'])
    pid = pid or linked.get('person_id') or ''
    person = state.people.get(pid) or original or linked.get('person') or {'id': pid}
    names = [item.get('display_name'), original.get('name')]
    if kind in {'person', 'sharing_attempt', 'sharing_delivery'}:
        names.append(value.get('name'))
    if kind == 'paper':
        names.append(value.get('claimed_by_name'))
    if kind == 'shift_person':
        names.append((value.get('summary') or {}).get('name'))
    linked_person = linked.get('person') or {}
    names.extend([linked_person.get('name'), linked.get('name') if kind == 'sharing_delivery' else None])
    names = sorted({name.strip() for name in names if isinstance(name, str) and name.strip()})
    if not names and isinstance(person.get('name'), str) and person['name'].strip():
        names = [person['name'].strip()]
    return {'person': {key: person[key] for key in ('id', 'aliases', 'login_ids', 'employee_no') if key in person},
            'names': names, 'required': True}


def resolve_history_identity(identity, directory):
    if not identity.get('required'):
        return '', '', []
    names = identity.get('names') or []
    rows = identity_rows(identity.get('person') or {}, directory)
    reason = ''
    if len(names) > 1:
        reason = '原记录中的姓名不一致，需人工核对'
    elif len(rows) > 1:
        reason = '原人员标识对应多条人员记录，需人工核对'
    elif rows:
        if names and str(rows[0][1].get('name') or '').strip() != names[0]:
            reason = '原姓名与人员标识对应的姓名不一致'
    elif not names:
        reason = '原记录没有可核验的人员姓名'
    else:
        rows = [(rid, row) for rid, row in directory.items() if str(row.get('name') or '').strip() == names[0]]
        if len(rows) > 1:
            reason = '人员表中存在重名，未自动选择'
        elif not rows:
            reason = '人员表中未找到完全同名的人员'
    if not reason and rows:
        rid, row = rows[0]
        if row.get('inactive') or row.get('selectable') is False:
            reason = '人员已停用或身份待核对'
        else:
            oid = person_open_id({'id': 'directory:' + rid}, directory)
            if oid:
                return oid, '', []
            reason = '人员缺少唯一的当前应用 openid，需核对人员表'
    candidates = [{'record_id': rid, 'name': str(row.get('name') or ''),
                   'employee_no': str(row.get('employee_no') or ''), 'scopes': row.get('scopes') or []}
                  for rid, row in rows[:20]]
    return '', reason, candidates


def history_issues(service):
    with closing(service._connect()) as conn:
        return [json.loads(row[0]) for row in conn.execute(
            "SELECT payload FROM documents WHERE kind='local' AND key LIKE 'shift_history_issue:%' ORDER BY key")]


def current_directory(service):
    from .learning_personal import _login_directory
    try:
        app_id = service.cloud.current_app_id()
        reader = service._login_people_reader
        value = reader() if callable(reader) else {}
        if not isinstance(value, dict) or value.get('schema') != 4 or value.get('app_id') != app_id:
            return {}, False, app_id
        directory, valid = _login_directory(SimpleNamespace(_login_people_reader=lambda: value))
        return directory, valid, app_id
    except Exception:
        return {}, False, ''


def display(summary, oid=''):
    midnight = dt.datetime.combine(dt.date.fromisoformat(summary['date']), dt.time.min, core.TZ)
    answered = summary['objective_answered']
    return {'日期': int(midnight.timestamp() * 1000), '人员标识': summary['person_id'],
            '人员': [{'id': oid}] if oid else [], '姓名': summary['name'],
            '楼栋': '、'.join(s + ('楼' if s in core.SCOPES else '') for s in summary['scopes']),
            '班次': summary['shift'] + '班' if summary['shift'] in SHIFTS else summary['shift'],
            '完成状态': summary['status'], '应答题数': summary['assigned'], '已答题数': summary['answered'],
            '正确题数': summary['correct'], '首次客观正确率': round(summary['correct'] * 100 / answered, 1) if answered else None,
            '看答案后数': summary['answer_viewed'] if not summary['answer_view_unknown'] else None,
            '主动练习量': summary['practice_questions'], '复习次数': summary['review_count'],
            '更新时间': int(dt.datetime.fromisoformat(summary['updated_at']).timestamp() * 1000)}


def history_fields(state, kind, identity, directory):
    fields = {'完成状态': '系统记录（不计人员完成情况）'}
    try:
        owner = state.owner(kind, identity)
        value = state.get(kind, identity)
        if owner:
            summary = state.summary(owner, [((kind, identity), state.documents[kind, identity])])
            return display(summary, person_open_id(state.people.get(owner[2], {'id': owner[2]}), directory))
        pid = value.get('person_id') or (identity if kind == 'person' else '')
        person = state.people.get(pid, {})
        if person:
            oid = person_open_id(person, directory)
            fields.update(人员标识=pid, 姓名=person.get('name', ''), 人员=[{'id': oid}] if oid else [],
                          楼栋='、'.join(person.get('scopes') or []))
        date = completion.date_of(str(value.get('date') or '') + 'T00:00:00')
        if date:
            fields['日期'] = int(dt.datetime.combine(dt.date.fromisoformat(date), dt.time.min, core.TZ).timestamp() * 1000)
    except (core.LearningError, KeyError, TypeError, ValueError):
        fields = {'完成状态': '历史归属待核对（未合并）'}
    return fields


def sync(service, rows, limit):
    state = Snapshot(snapshot(service))
    groups, errors = state.groups()
    directory, valid, app_id = current_directory(service)
    if not valid:
        directory = {}
    handled, failures = set(), []
    due = {(r[0], r[1]) for r in rows}
    pending = set()
    with service.transaction() as conn:
        for owner, dependencies in state.dependencies.items():
            if due.intersection(dependencies):
                service._put('local', 'shift_summary_pending:' + key(owner), {'pending': True}, conn, False)
                pending.add(owner)
    acknowledged = set()
    count = 0
    for owner, members in sorted(groups.items()):
        identity = key(owner)
        ack = state.get('local', 'shift_cloud:' + identity)
        selected = {pair for pair, row in members if row['dirty'] and pair in due}
        handled.update(selected)
        needs_summary = owner in pending or state.get('local', 'shift_summary_pending:' + identity).get('pending')
        if not ack and not selected and not needs_summary:
            continue
        summary = state.summary(owner, members)
        payload = {'schema_version': 1, 'date': owner[0], 'shift': owner[1], 'person_id': owner[2],
                   'entities': [{'kind': p[0], 'key': p[1], 'payload': r['payload']} for p, r in sorted(members)],
                   'summary': summary}
        fields = display(summary, person_open_id(state.people.get(owner[2], {'id': owner[2]}), directory))
        if not valid:
            fields.pop('人员', None)
        fingerprint = core.digest([payload, fields])
        if ack.get('fingerprint') == fingerprint:
            with service.transaction() as conn:
                for pair, row in members:
                    conn.execute('UPDATE documents SET dirty=0,retry_at=0 WHERE kind=? AND key=? AND revision=?',
                                 (*pair, row['revision']))
                conn.execute("DELETE FROM documents WHERE kind='local' AND key=?", ('shift_summary_pending:' + identity,))
            handled.update(selected)
            acknowledged.update(pair for pair, _ in members)
            continue
        # New summaries without a due entity are written after the associated source snapshots.
        if not selected and members and any(row['dirty'] for _, row in members):
            continue
        member_pairs = {pair for pair, _ in members}
        if any(state.documents[pair]['dirty'] and pair not in member_pairs and pair not in acknowledged
               for pair in state.dependencies[owner]):
            continue
        if count >= limit or service._stop.is_set():
            handled.update(selected)
            continue
        count += 1
        try:
            with service.transaction() as conn:
                for pair, _ in members:
                    service._put('local', 'shift_owner:' + pair[0] + ':' + pair[1], {'owner': list(owner)}, conn, False)
            service.cloud.upsert_entity('shift_person', identity, payload, 'shift:' + fingerprint,
                                        display_fields=fields, expected_app_id=app_id)
            with service.transaction() as conn:
                for pair, row in members:
                    conn.execute('UPDATE documents SET dirty=0,retry_at=0 WHERE kind=? AND key=? AND revision=?',
                                 (*pair, row['revision']))
                service._put('local', 'shift_cloud:' + identity, {'fingerprint': fingerprint}, conn, False)
                conn.execute("DELETE FROM documents WHERE kind='local' AND key=?", ('shift_summary_pending:' + identity,))
            acknowledged.update(member_pairs)
        except Exception as exc:
            failures.append(str(exc)[:180])
            with service.transaction() as conn:
                for pair, row in members:
                    if pair in selected:
                        conn.execute('UPDATE documents SET retry_at=? WHERE kind=? AND key=? AND revision=? AND dirty=1',
                                     (time.time() + 60, *pair, row['revision']))
        handled.update(selected)
    for pair in due:
        if state.legacy(*pair):
            continue
        if pair in errors:
            handled.add(pair)
            failures.append(errors[pair])
        elif pair[0] == 'daily_progress' and state.owner(*pair) is None:
            # No task means no person/shift row; this is a reproducible local projection.
            with service.transaction() as conn:
                conn.execute('UPDATE documents SET dirty=0 WHERE kind=? AND key=? AND revision=?',
                             (*pair, state.documents[pair]['revision']))
            handled.add(pair)
    legacy_fields = {}
    for pair in due:
        if state.legacy(*pair):
            fields = history_fields(state, *pair, directory)
            if not valid:
                fields.pop('人员', None)
            legacy_fields[pair] = (fields, app_id)
    return handled, failures, legacy_fields


def backfill(service, limit=8):
    """Resumable metadata-only enrichment; historical records are never moved or deleted."""
    progress = service._get('local', 'shift_history_fields') or {}
    if progress.get('version') != HISTORY_VERSION:
        progress = {'version': HISTORY_VERSION}
    if progress.get('complete') or progress.get('retry_at', 0) > time.time():
        return
    directory, ready, app_id = current_directory(service)
    if not ready:
        return
    if not progress.get('prepared') or 0 < progress.get('replan_at', 0) <= time.time():
        try:
            records = service.cloud.list_entity_records()
        except Exception:
            with service.transaction() as conn:
                service._put('local', 'shift_history_fields', {**progress, 'retry_at': time.time() + 300}, conn, False)
            raise
        targets = set(progress.get('targets') or [r['record_id'] for r in records])
        completed = set(progress.get('completed_ids') or [])
        missing = targets - {r['record_id'] for r in records} - completed
        documents = {}
        for item in records:
            value = item['payload']
            if item['kind'] == 'shift_person' or not isinstance(value, dict):
                continue
            documents[item['kind'], item['key']] = {'payload': value, 'revision': 1, 'dirty': False,
                'updated': value.get('updated_at') or value.get('created_at') or
                ((value.get('date') or '1970-01-01') + 'T00:00:00+08:00')}
        state = Snapshot(documents)
        with service.transaction() as conn:
            if not progress.get('prepared'):
                conn.execute("DELETE FROM documents WHERE kind='local' AND key LIKE 'shift_history_row:%'")
            for record_id in missing:
                conn.execute("DELETE FROM documents WHERE kind='local' AND key=?", ('shift_history_row:' + record_id,))
                service._put('local', 'shift_history_issue:' + record_id, {
                    'record_id': record_id, 'name': '', 'date': '', 'scope': '', 'candidates': [],
                    'reason': '原记录已不存在，已跳过，不影响其它记录'}, conn, False)
            for item in records:
                if item['record_id'] not in targets - completed:
                    continue
                try:
                    fields = history_fields(state, item['kind'], item['key'], directory) if item['kind'] != 'shift_person' else {}
                    identity = history_identity(state, item)
                except (AttributeError, KeyError, TypeError, ValueError):
                    fields = {}
                    identity = {'person': {}, 'names': [], 'required': True}
                if item.get('display_name'):
                    fields['姓名'] = item['display_name']
                _, reason, candidates = resolve_history_identity(identity, directory)
                job_key = 'shift_history_row:' + item['record_id']
                previous = service._get('local', job_key, conn) or {}
                unchanged = (previous.get('payload_sha256') == item['payload_sha256']
                             and previous.get('identity') == identity)
                job = {
                    'record_id': item['record_id'], 'kind': item['kind'], 'key': item['key'],
                    'payload_sha256': item['payload_sha256'], 'fields': fields, 'identity': identity,
                    'metadata_done': bool(unchanged and previous.get('metadata_done')), 'retry_at': 0}
                service._put('local', job_key, job, conn, False)
                if reason:
                    _history_issue(service, conn, job, reason, candidates)
            progress = {'version': HISTORY_VERSION, 'prepared': True, 'targets': sorted(targets), 'completed_ids': sorted(completed),
                        'missing_ids': sorted(set(progress.get('missing_ids') or []) | missing)}
            service._put('local', 'shift_history_fields', progress, conn, False)
    with closing(service._connect()) as conn:
        jobs = conn.execute("SELECT key,payload FROM documents WHERE kind='local' AND key LIKE 'shift_history_row:%' "
                            "AND COALESCE(json_extract(payload,'$.retry_at'),0)<=? ORDER BY key LIMIT ?", (time.time(), limit)).fetchall()
    for row in jobs:
        job = json.loads(row['payload'])
        oid, reason, candidates = resolve_history_identity(job['identity'], directory)
        fields = dict(job['fields'])
        fields.pop('人员', None)
        if oid:
            fields['人员'] = [{'id': oid}]
        try:
            if not reason or not job.get('metadata_done'):
                service.cloud.enrich_entity_record(job['record_id'], job['kind'], job['key'], fields,
                                                   expected_payload_sha256=job['payload_sha256'], expected_app_id=app_id)
            job['metadata_done'] = True
        except Exception:
            with service.transaction() as conn:
                _history_issue(service, conn, job, reason or '补写结果尚未确认，稍后重新核对并重试', candidates)
                job['retry_at'] = time.time() + 300
                service._put('local', row['key'], job, conn, False)
            progress['replan_at'] = min(progress.get('replan_at') or float('inf'), time.time() + 300)
            continue
        with service.transaction() as conn:
            if reason:
                _history_issue(service, conn, job, reason, candidates)
                job['retry_at'] = time.time() + 300
                service._put('local', row['key'], job, conn, False)
                progress['replan_at'] = min(progress.get('replan_at') or float('inf'), time.time() + 300)
            else:
                conn.execute("DELETE FROM documents WHERE kind='local' AND key IN (?,?)",
                             (row['key'], 'shift_history_issue:' + job['record_id']))
                progress['completed_ids'] = sorted(set(progress.get('completed_ids') or []) | {job['record_id']})
            service._put('local', 'shift_history_fields', progress, conn, False)
    with service.transaction() as conn:
        remaining = conn.execute("SELECT COUNT(*) FROM documents WHERE kind='local' AND key LIKE 'shift_history_row:%'").fetchone()[0]
        service._put('local', 'shift_history_fields', {**progress, 'complete': remaining == 0, 'remaining': remaining}, conn, False)


def _history_issue(service, conn, job, reason, candidates):
    fields = job['fields']
    service._put('local', 'shift_history_issue:' + job['record_id'], {
        'record_id': job['record_id'], 'kind': job['kind'], 'name': '、'.join(job['identity'].get('names') or []),
        'date': fields.get('日期'), 'scope': fields.get('楼栋') or '',
        'reason': reason, 'candidates': candidates}, conn, False)
