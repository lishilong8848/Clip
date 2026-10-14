"""Personal reminders and weekly cards on the existing learning worker/outbox."""
import datetime as dt
import json
import re
from collections import defaultdict
from contextlib import closing
from urllib.parse import quote

from . import learning as core
from .learning_reminders import duty_recipients, reminder_card

WEEKLY_CHAT_ID = 'oc_3bc648b9b761f24a65366a9b04b32eb2'
WEEKLY_SUPERVISOR_SCOPES = tuple('ABCDE')


def mark_started(service, paper, conn):
    if paper.get('started_at') or not paper.get('questions') or service._completed(paper, service._get('record', paper['id'], conn) or {}):
        return
    current = core.now()
    paper['started_at'] = current.isoformat(timespec='seconds')
    service._put('paper', paper['id'], paper, conn)
    paper['_dirty'] = True
    ensure_timeout(service, paper, conn)
    # Personal work must still sync when scheduled publication is switched off.
    service._put('local', 'self_practice_sync', {'enabled': True}, conn, False)


def ensure_timeout(service, paper, conn):
    if not paper.get('started_at') or not paper.get('person_id') or paper.get('deleted_at'):
        return
    identity = 'timeout:' + paper['id']
    if service._get('engagement', identity, conn):
        return
    started = dt.datetime.fromisoformat(paper['started_at'])
    started = started.astimezone(core.TZ) if started.tzinfo else started.replace(tzinfo=core.TZ)
    service._put('engagement', identity, {
        'id': identity, 'kind': 'timeout', 'paper_id': paper['id'], 'person_id': paper['person_id'],
        'date': paper['date'], 'status': 'pending', 'due_at': (started + dt.timedelta(hours=2)).timestamp(),
    }, conn)


def recipients(people):
    from .portal_service import BUILDING_OPEN_ID_MAP
    shared = set(BUILDING_OPEN_ID_MAP.values())
    owners = defaultdict(set)
    for person in people:
        for oid in person.get('login_ids', []):
            if isinstance(oid, str) and re.fullmatch(r'ou_[A-Za-z0-9]+', oid) and oid not in shared:
                owners[oid].add(person['id'])
    result = {}
    for person in people:
        ids = [oid for oid in person.get('login_ids', []) if isinstance(oid, str) and owners.get(oid) == {person['id']}]
        if person.get('active') and len(set(ids)) == 1:
            result[person['id']] = ids[0]
    return result


def supervisors(service):
    from .account_policy import categorize_position
    from .learning_personal import _login_directory, _hr_active, _hr_learning_scopes
    directory, valid = _login_directory(service)
    if not valid:
        return {}
    targets = recipients([{'id': rid, 'active': _hr_active(row), 'login_ids': row.get('login_ids', [])}
                          for rid, row in directory.items()])
    result = defaultdict(list)
    for rid, row in directory.items():
        scopes = _hr_learning_scopes(row)
        category = categorize_position(row.get('position'))
        if category == 'other':
            category = categorize_position(' '.join(row.get('assignments') or []))
        if rid in targets and scopes and scopes[0] in WEEKLY_SUPERVISOR_SCOPES and category == 'supervisor':
            result[scopes[0]].append(targets[rid])
    return result


def weekly_stats(service, start, end, conn, scope=None):
    rows = {}
    def row(pid):
        return rows.setdefault(pid, {'person_id': pid, 'answered': 0, 'choices': 0, 'correct': 0,
            'wrong': 0, 'interviews': 0, 'practice': 0, 'papers': set(), 'days': set(), 'topics': {}, 'ratings': {}, 'scopes': set()})
    scope_filter = ' AND scope=?' if scope else ''
    params = (start, end, scope) if scope else (start, end)
    for value in conn.execute('SELECT * FROM learning_results WHERE day BETWEEN ? AND ? AND invalid=0 AND needs_review=0' + scope_filter, params):
        item = row(value['person_id'])
        item['answered'] += 1
        item['papers'].add(value['paper_id'])
        item['days'].add(value['day']); item['scopes'].add(value['scope'])
        if value['type'] == 'interview':
            item['interviews'] += 1
            rating = value['self_rating'] or '未自评'
            item['ratings'][rating] = item['ratings'].get(rating, 0) + 1
        elif value['correct'] is not None:
            item['choices'] += 1
            item['correct' if value['correct'] else 'wrong'] += 1
            if not value['correct']:
                item['topics'][value['topic']] = item['topics'].get(value['topic'], 0) + 1
    for value in conn.execute('SELECT person_id,paper_id,scope,day,COUNT(*) AS count FROM learning_practice WHERE day BETWEEN ? AND ?' + scope_filter + ' GROUP BY person_id,paper_id,scope,day', params):
        item = row(value['person_id'])
        item['practice'] += value['count']; item['days'].add(value['day']); item['scopes'].add(value['scope'])
        item['papers'].add(value['paper_id'])
    for item in rows.values():
        item['days'] = len(item['days']); item['scopes'] = sorted(item['scopes'])
        item['papers'] = len(item['papers'])
        item['accuracy'] = round(item['correct'] * 100 / item['choices'], 1) if item['choices'] else None
        item['error_rate'] = round(item['wrong'] * 100 / item['choices'], 1) if item['choices'] else None
    return rows


def enqueue_weekly(service, current, duties):
    monday = current.date() - dt.timedelta(days=current.weekday())
    due = dt.datetime.combine(monday, dt.time.min, core.TZ)
    if current < due:
        return
    start = (monday - dt.timedelta(days=7)).isoformat()
    end = (monday - dt.timedelta(days=1)).isoformat()
    identity = 'week:' + start
    if service._get('engagement', identity):
        return
    with service.transaction() as conn:
        if service._get('engagement', identity, conn):
            return
        people = service._all('person', conn)
        if not people:
            return
        stats = weekly_stats(service, start, end, conn)
        expiry = (due + dt.timedelta(days=7)).timestamp()
        base = {'date': start, 'end': end, 'status': 'pending', 'due_at': current.timestamp(), 'expires_at': expiry}
        job_count = 0
        targets = recipients(people)
        for person in people:
            if not person.get('active'):
                continue
            target = targets.get(person['id'])
            if duties is not None and target and target not in duties:
                continue
            key = identity + ':' + person['id']
            job_count += 1
            if not service._get('engagement', key, conn):
                service._put('engagement', key, {**base, 'id': key, 'kind': 'weekly', 'person_id': person['id'],
                    'name': person.get('name', ''), 'stats': stats.get(person['id'], {})}, conn)
        directory = {p['id']: p for p in people}
        reports = [('group', '', stats)]
        reports.extend((scope, scope, weekly_stats(service, start, end, conn, scope)) for scope in WEEKLY_SUPERVISOR_SCOPES)
        for audience, scope, values in reports:
            participants = [{**item, 'name': directory.get(pid, {}).get('name', '人员信息待核对'),
                'employee_no': directory.get(pid, {}).get('employee_no', '')} for pid, item in values.items()]
            participants.sort(key=lambda item: (item['scopes'], item['name'], item['person_id']))
            key = identity + ':summary:' + audience
            job_count += 1
            if not service._get('engagement', key, conn):
                service._put('engagement', key, {**base, 'id': key, 'kind': 'weekly_summary', 'audience': audience,
                    'scope': scope, 'items': participants, 'part': 1, 'parts': 1, 'participants': len(participants)}, conn)
        service._put('engagement', identity, {'id': identity, 'date': start, 'kind': 'weekly_batch', 'job_count': job_count, 'status': 'prepared'}, conn)
        service._put('local', 'self_practice_sync', {'enabled': True}, conn, False)


def rate(value):
    return '—' if value is None else f'{value:g}%'


def summary_card(job, url):
    items = job['items']
    answered = sum(item['answered'] for item in items)
    practice = sum(item['practice'] for item in items)
    # Average people equally, not their rounded percentages or question counts.
    scores = [item['correct'] * 100 / item['choices'] for item in items if item['choices']]
    average = round(sum(scores) / len(scores), 1) if scores else None
    error_rate = round(sum(100 - score for score in scores) / len(scores), 1) if scores else None
    mean_count = f'{answered / len(items):.1f}' if items else '—'
    metrics = [('参与人数', f'{len(items)} 人'), ('首次答题总数', f'{answered} 题'),
               ('人均答题数', f'{mean_count} 题'), ('平均正确率', rate(average))]
    card = reminder_card('', f"{url}/learning?view=overview&from={job['date']}&to={job['end']}")
    button = card['elements'][-1]
    if job.get('scope'):
        button['actions'][0]['url'] += '&scope=' + quote(job['scope'], safe='')
    button['actions'][0]['text']['content'] = '查看本周人员明细'
    elements = [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': f"{job['date']} 至 {job['end']}"}},
        {'tag': 'div', 'fields': [{'is_short': True, 'text': {'tag': 'lark_md', 'content': f'**{label}**\n{value}'}} for label, value in metrics]},
        {'tag': 'div', 'text': {'tag': 'plain_text', 'content': f'复习 {practice} 次 · 平均错误率 {rate(error_rate)}'}}]
    if scores:
        labels = [f'{percent}%' for percent in range(101)]
        counts = [0] * 101
        for item in items:
            if item['choices']:
                # Integer division keeps exact percentages out of the preceding bin.
                counts[item['correct'] * 100 // item['choices']] += 1
        elements.append({'tag': 'chart', 'height': '260px', 'aspect_ratio': '2:1', 'preview': True,
            'chart_spec': {'type': 'line', 'title': {'text': '个人正确率分布（人数）'},
                'data': {'values': [{'range': label, 'count': count} for label, count in zip(labels, counts)]},
                'xField': 'range', 'yField': 'count', 'color': ['#3978D6'],
                'line': {'style': {'curveType': 'monotone', 'lineWidth': 3}},
                'point': {'visible': False},
                'axes': [{'orient': 'left', 'min': 0, 'tick': {'noDecimals': True}},
                    {'orient': 'bottom', 'type': 'band', 'sampling': False, 'tick': {'tickStep': 1},
                     'label': {'autoHide': True, 'autoRotate': False}}],
                'dataZoom': [{'orient': 'bottom', 'start': 0, 'end': 1, 'height': 24, 'filterMode': 'axis'}],
                'label': {'visible': False}, 'legends': {'visible': False}}})
    elif not items:
        elements.append({'tag': 'div', 'text': {'tag': 'plain_text', 'content': '本周暂无参与答题人员。'}})
    if len(items) > len(scores):
        elements.append({'tag': 'note', 'elements': [{'tag': 'plain_text', 'content': f'{len(items) - len(scores)}人本周暂无选择题首答成绩，不计为0%或纳入曲线。'}]})
    if items:
        details = []
        for item in items:
            correct = item['correct'] * 100 / item['choices'] if item['choices'] else None
            details.append({'name': item.get('name') or '人员信息待核对', 'papers': str(item.get('papers', '—')),
                'answered': item['answered'], 'accuracy': rate(round(correct, 1)) if correct is not None else '—',
                'error_rate': rate(round(100 - correct, 1)) if correct is not None else '—'})
        elements.extend([{'tag': 'div', 'text': {'tag': 'lark_md', 'content': f'**本周人员明细 · {len(items)}人**'}},
            {'tag': 'table', 'page_size': 10, 'row_height': 'low', 'freeze_first_column': True,
                'header_style': {'text_align': 'center', 'text_size': 'normal', 'background_style': 'grey', 'bold': True, 'lines': 2},
                'columns': [{'name': key, 'display_name': label, 'data_type': kind, 'width': width,
                    'horizontal_align': 'left' if key == 'name' else 'center', 'vertical_align': 'center'}
                    for key, label, kind, width in [('name', '姓名', 'text', '24%'), ('papers', '答题次数', 'text', '19%'),
                        ('answered', '答题数量', 'number', '19%'), ('accuracy', '正确率', 'text', '19%'), ('error_rate', '错误率', 'text', '19%')]],
                'rows': details}])
    explanation = f'正确率、错误率按有选择题成绩的 {len(scores)} 人等权平均，仅计首次作答；问答及复习不计入。'
    explanation += ' 次数为本周有作答的题单数，题数为首次提交数量，复习另计。'
    explanation += ' 曲线按1个百分点分组，100%单列。'
    if job.get('parts', 1) > 1:
        explanation = f"历史分组 {job['part']}/{job['parts']}：以上仅统计本组 {len(items)} 人。" + explanation
    elements.extend([{'tag': 'note', 'elements': [{'tag': 'plain_text', 'content': explanation}]}, button])
    card['elements'] = elements
    card['header']['title']['content'] = (job['scope'] + '楼 · ' if job.get('scope') else '') + '每周学练汇总'
    return card


def make_card(job, url, paper=None, record=None):
    if job['kind'] == 'timeout':
        remaining = sum(bool(not q.get('invalid') and (not (record or {}).get('entries', {}).get(q['id'], {}).get('attempt')
            or (record or {}).get('entries', {}).get(q['id'], {}).get('needs_review'))) for q in paper['questions'])
        mode = '自主练习' if paper.get('mode') == 'practice' else '每日题单'
        message = f"{paper['date']} · {mode}\n开始作答已满2小时，还有{remaining}题未提交。已提交答案和草稿不受影响，请继续完成。"
        card = reminder_card(message, url + '/learning?view=practice&paper_id=' + quote(paper['id'], safe=''))
        title = '学练待完成提醒'
    elif job['kind'] == 'weekly':
        item = job['stats']
        message = (f"{job['name']} · {job['date']} 至 {job['end']}\n"
            f"作答 {item.get('answered', 0) + item.get('practice', 0)} 次（首次 {item.get('answered', 0)}题 / 复习 {item.get('practice', 0)}次） · 学习 {item.get('days', 0)} 天\n"
            f"选择题 {item.get('choices', 0)} 题：正确 {item.get('correct', 0)}，错误 {item.get('wrong', 0)}\n"
            f"正确率 {rate(item.get('accuracy'))} · 错误率 {rate(item.get('error_rate'))}\n"
            f"问答题 {item.get('interviews', 0)} 题，不计入正确率。")
        if not item:
            message += '\n本周未作答，可从今日学练开始。'
        topics = sorted(item.get('topics', {}).items(), key=lambda pair: (-pair[1], pair[0]))[:3]
        if topics:
            message += '\n建议回顾：' + '、'.join(f'{topic}（{count}题）' for topic, count in topics)
        if item.get('ratings'):
            message += '\n上述问答题当前自评：' + '、'.join(f'{label} {count}题' for label, count in item['ratings'].items())
        card = reminder_card(message, url + '/learning?view=practice&tab=review')
        if item.get('choices'):
            card['elements'].insert(1, {'tag': 'chart', 'height': '240px', 'preview': True,
                'chart_spec': {'type': 'pie', 'title': {'text': '本周选择题首次作答'},
                    'data': {'values': [{'type': '正确', 'count': item['correct']}, {'type': '错误', 'count': item['wrong']}]},
                    'valueField': 'count', 'categoryField': 'type', 'outerRadius': 0.8,
                    'color': ['#20A46B', '#E05A47'], 'label': {'visible': True},
                    'legends': {'visible': True, 'orient': 'bottom'}}})
        else:
            card['elements'].insert(1, {'tag': 'note', 'elements': [{'tag': 'plain_text', 'content': '本周暂无选择题首答成绩，不生成正确/错误饼图。'}]})
        card['elements'][-1]['actions'][0]['text']['content'] = '查看个人错题集'
        title = '个人每周学练回顾'
    else:
        return summary_card(job, url)
    card['header']['title']['content'] = title
    return card


def send_due(service, current):
    current = current.astimezone(core.TZ) if current.tzinfo else current.replace(tzinfo=core.TZ)
    settings = service.settings()
    duties = duty_recipients(service)
    if settings['weekly_report_enabled']:
        enqueue_weekly(service, current, duties)
    with closing(service._connect()) as conn:
        jobs = [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind='engagement' AND json_extract(payload,'$.status')='pending' AND json_extract(payload,'$.due_at')<=? ORDER BY json_extract(payload,'$.due_at'),key LIMIT 10", (current.timestamp(),))]
    if not jobs:
        return
    people = service._all('person')
    people_by_id = {person['id']: person for person in people}
    url = service.portal_url()
    targets = recipients(people) if url else {}
    managers = {}
    if url and any(job['kind'] == 'weekly_summary' and job.get('scope') in WEEKLY_SUPERVISOR_SCOPES for job in jobs):
        try:
            managers = supervisors(service)
        except Exception:
            service._last_error = '本地主管目录暂不可用，楼栋周报等待重试。'
    for job in jobs:
        if service._stop.is_set():
            break
        enabled = service.settings()['timeout_reminder_enabled' if job['kind'] == 'timeout' else 'weekly_report_enabled']
        paper = service._get('paper', job.get('paper_id', ''))
        record = service._get('record', job.get('paper_id', '')) or {}
        person = people_by_id.get(job.get('person_id'))
        if not enabled:
            job['status'] = 'cancelled'
        elif job['kind'] == 'weekly_summary' and not (job.get('audience') == 'group'
                or job.get('scope') in WEEKLY_SUPERVISOR_SCOPES):
            # Also cancel retired recipients in restored or previously queued reports.
            job['status'] = 'cancelled'
        elif job.get('expires_at', float('inf')) <= current.timestamp():
            job['status'] = 'expired'
        elif job['kind'] == 'timeout' and (not paper or paper.get('deleted_at') or service._completed(paper, record)):
            job['status'] = 'skipped_completed'
        elif job.get('person_id') and person and not person.get('active'):
            job['status'] = 'inactive'
        elif job['kind'] in {'weekly', 'timeout'} and duties is not None and targets.get(job.get('person_id')) and targets[job['person_id']] not in duties:
            job['status'] = 'skipped_role'
        else:
            if job['kind'] == 'weekly_summary' and url:
                if job.get('audience') == 'group':
                    recipients_for_job = [WEEKLY_CHAT_ID]
                else:
                    recipients_for_job = managers.get(job['scope'], [])
            else:
                target = targets.get(job.get('person_id')) if duties is not None else None
                recipients_for_job = [target] if target else []
            job['due_at'] = current.timestamp() + 300
            if not recipients_for_job or not url:
                job['error'] = ('学练入口地址尚未就绪' if not url else
                    f"{job['scope']}楼未找到有效主管飞书身份" if job.get('scope') else
                    '本地人员岗位目录暂未就绪' if duties is None else '未找到唯一的个人飞书身份')
                service._last_error = job['error'] + '，提醒已保留等待重试。'
            else:
                card = None
                sent_to = job.setdefault('sent_to', [])
                for target in recipients_for_job:
                    if target in sent_to or service._stop.is_set():
                        continue
                    identity = job['id'] + ':' + target if job.get('scope') else job['id']
                    try:
                        if card is None:
                            card = make_card(job, url, paper, record)
                        result = service._send(target, card, identity)
                        ok = result[0] if isinstance(result, tuple) else bool(result)
                    except Exception:
                        ok = False
                    if ok:
                        sent_to.append(target)
                        # Persist each receipt before sending to another supervisor.
                        with service.transaction() as conn:
                            service._put('engagement', job['id'], job, conn)
                complete = all(target in sent_to for target in recipients_for_job)
                job['error'] = '' if complete else '飞书发送失败，等待重试。'
                if complete:
                    job.update(status='sent', sent_at=current.isoformat(timespec='seconds'))
        with service.transaction() as conn:
            service._put('engagement', job['id'], job, conn)
            if not (service._get('local', 'self_practice_sync', conn) or {}).get('enabled'):
                service._put('local', 'self_practice_sync', {'enabled': True}, conn, False)
