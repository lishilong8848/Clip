"""Personal reminders and weekly cards on the existing learning worker/outbox."""
import datetime as dt
import json
import re
from collections import defaultdict
from contextlib import closing
from fractions import Fraction
from urllib.parse import quote

from . import learning as core
from .learning_reminders import duty_recipients, reminder_card, shift_learners, shift_times

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


def repeated_reminders(service, current):
    from .learning_completion import daily_progress
    from .learning_personal import candidates
    from .learning_schedule import windows

    contexts, unavailable = {}, set()
    if not service._restored or not service.settings()['timeout_reminder_enabled']:
        return contexts, unavailable
    if not service._shift_roster_reader:
        return contexts, {current.date().isoformat(), (current.date() - dt.timedelta(days=1)).isoformat()}
    times = shift_times(service.settings())
    for date in (current.date() - dt.timedelta(days=1), current.date()):
        day = date.isoformat()
        publication = service._get('publication', service._publication_key(day))
        if not publication:
            continue
        try:
            published = dt.datetime.fromisoformat(publication.get('created_at', ''))
        except (TypeError, ValueError):
            unavailable.add(day)
            service._last_error = '题单发布时间缺失或无效，个人学练提醒等待核对。'
            continue
        published = published.astimezone(core.TZ) if published.tzinfo else published.replace(tzinfo=core.TZ)
        for shift, clock in times.items():
            beginning, end = windows(day, shift)
            start = dt.datetime.combine(date, dt.time.fromisoformat(clock), core.TZ)
            if shift == '夜' and start < beginning:
                start += dt.timedelta(days=1)
            start = max(start, beginning, published)
            period = int((current - start).total_seconds() // 7200)
            if period < 1 or current >= end:
                continue
            learners = shift_learners(service, day, shift)
            if learners is None:
                unavailable.add(day)
                continue
            due = start + dt.timedelta(hours=2 * period)
            expires = min(due + dt.timedelta(hours=2), end)
            with service.transaction() as conn:
                progress = daily_progress(service, day, current, conn)
                reserves = [service._get('reserve', rid, conn) for rid in publication.get('reserve_ids', [])]
                available = candidates(service, conn)
                for duty, person in learners:
                    if (person.get('scopes') != [duty['scope']]
                            or str(person.get('name') or '').strip() != duty['name']):
                        continue
                    if progress.get((person['id'], duty['scope'], shift), {}).get('status') == '已完成':
                        continue
                    papers = [p for p in service._documents('paper', person_id=person['id'], start=day, end=day, conn=conn)
                              if p.get('mode') != 'practice']
                    paper = next((p for p in papers if p['scope'] == duty['scope']), None)
                    record = service._get('record', paper['id'], conn) or {} if paper else {}
                    if papers and (not paper or paper.get('deleted_at') or not paper.get('questions') or service._completed(paper, record)):
                        continue
                    if not paper:
                        recent = {r[0] for r in conn.execute('SELECT family_id FROM learning_usage WHERE person_id=? AND day BETWEEN ? AND ?',
                                  (person['id'], (date - dt.timedelta(days=6)).isoformat(), day))}
                        if (not reserves or any(r is None for r in reserves)
                                or not any(r.get('scope') == duty['scope'] and r.get('date') == day for r in reserves)
                                or not any(q['family_id'] not in recent for q in available)):
                            continue
                    assignment = f"unfinished:{day}:{shift}:{duty['scope']}:{person['id']}"
                    context = {'paper': paper, 'person_id': person['id'], 'open_id': duty['open_id']}
                    contexts[assignment] = context
                    identity = assignment + ':' + str(int(due.timestamp()))
                    if service._get('engagement', identity, conn):
                        continue
                    legacy = service._get('engagement', 'timeout:' + paper['id'], conn) if paper else None
                    notice = service._get('notification', f"shift:{day}:{shift}:{duty['open_id']}", conn)
                    delivered = False
                    for old in (legacy, notice):
                        if not old or not (old.get('status') == 'sent' or old.get('sent_to')):
                            continue
                        if old.get('sent_at'):
                            sent = dt.datetime.fromisoformat(old['sent_at'])
                            sent = sent.astimezone(core.TZ) if sent.tzinfo else sent.replace(tzinfo=core.TZ)
                        elif old.get('due_at'):
                            sent = dt.datetime.fromtimestamp(old['due_at'] - 300, core.TZ)
                        else:
                            continue
                        delivered |= due <= sent < expires
                    service._put('engagement', identity, {'id': identity, 'kind': 'shift_timeout',
                        'assignment': assignment, 'person_id': person['id'], 'open_id': duty['open_id'],
                        'paper_id': paper['id'] if paper else '', 'scope': duty['scope'], 'date': day, 'shift': shift,
                        'status': 'skipped_delivered' if delivered else 'pending',
                        'due_at': due.timestamp(), 'expires_at': expires.timestamp()}, conn)
    return contexts, unavailable


def weekly_stats(service, start, end, conn, scope=None, *, person_id=None):
    rows = {}
    def row(pid):
        return rows.setdefault(pid, {'person_id': pid, 'answered': 0, 'choices': 0, 'correct': 0,
            'wrong': 0, 'interviews': 0, 'practice': 0, 'papers': set(), 'days': set(), 'topics': {}, 'ratings': {}, 'scopes': set()})
    scope_filter, params = '', [start, end]
    for field, value in (('scope', scope), ('person_id', person_id)):
        if value:
            scope_filter += ' AND ' + field + '=?'
            params.append(value)
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


def weekly_questions(service, start, end, conn, scope=None):
    params = [start, end]
    clause = ''
    if scope:
        clause = ' AND scope=?'
        params.append(scope)
    rows = [dict(r) for r in conn.execute(
        "SELECT question_id,COUNT(*) AS total,SUM(correct=0) AS wrong FROM learning_results "
        "WHERE day BETWEEN ? AND ? AND invalid=0 AND needs_review=0 AND type<>'interview' "
        "AND correct IN (0,1) AND submitted_at<>'' AND question_id<>''" + clause +
        ' GROUP BY question_id', params)]
    rows.sort(key=lambda r: (-Fraction(r['wrong'], r['total']), -r['total'], r['question_id']))
    for row in rows[:3]:
        sample = conn.execute(
            'SELECT paper_id,stem FROM learning_results WHERE question_id=? AND day BETWEEN ? AND ? '
            "AND invalid=0 AND needs_review=0 AND type<>'interview' AND correct IN (0,1) AND submitted_at<>''" + clause +
            ' ORDER BY submitted_at DESC,paper_id LIMIT 1', [row['question_id'], *params]).fetchone()
        paper = service._get('paper', sample['paper_id'], conn) or {}
        question = next((q for q in paper.get('questions', []) if q['id'] == row['question_id']), {})
        row.update(stem=sample['stem'], options=question.get('options', []))
    return rows[:3]


def weekly_question_elements(report, scope=''):
    if report.get('audience') != 'group' or report.get('scope'):
        return []
    questions = (report.get('question_buildings') or {}).get(scope, []) if scope else report.get('questions', [])
    if not questions:
        return [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': '本周暂无有效选择题首答统计。'}}]
    lines = ['本周错误率最高的3道题（最多3道）']
    for index, q in enumerate(questions[:3], 1):
        stem = ' '.join(q['stem'].split())
        lines.append(f"{index}. {stem[:120]}{'...' if len(stem) > 120 else ''}\n"
                     f"错误率 {q['wrong'] * 100 / q['total']:.1f}% · 错误 {q['wrong']} / 首答样本 {q['total']} 次")
        options = [' '.join(str(o.get('text', '')).split()) for o in q.get('options', [])]
        if options:
            lines.append('；'.join(f"{chr(65 + i)}. {value[:36]}{'...' if len(value) > 36 else ''}" for i, value in enumerate(options[:6])))
    return [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': '\n'.join(lines)}}]


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
        by_building = {scope: [{**item, 'name': directory.get(pid, {}).get('name', '人员信息待核对')}
                              for pid, item in weekly_stats(service, start, end, conn, scope).items()] for scope in 'ABCDEH'}
        reports.extend((scope, scope, {row['person_id']: row for row in by_building[scope]}) for scope in WEEKLY_SUPERVISOR_SCOPES)
        for audience, scope, values in reports:
            participants = [{**item, 'name': directory.get(pid, {}).get('name', '人员信息待核对'),
                'employee_no': directory.get(pid, {}).get('employee_no', '')} for pid, item in values.items()]
            participants.sort(key=lambda item: (item['scopes'], item['name'], item['person_id']))
            key = identity + ':summary:' + audience
            job_count += 1
            if not service._get('engagement', key, conn):
                service._put('engagement', key, {**base, 'id': key, 'kind': 'weekly_summary', 'audience': audience,
                    'scope': scope, 'items': participants, 'part': 1, 'parts': 1, 'participants': len(participants)}, conn)
                report = service._get('engagement', key, conn)
                report['buildings'] = {scope: by_building[scope]} if scope else by_building
                if audience == 'group':
                    report['questions'] = weekly_questions(service, start, end, conn)
                    report['question_buildings'] = {s: weekly_questions(service, start, end, conn, s) for s in by_building}
                service._put('engagement', key, report, conn)
        service._put('engagement', identity, {'id': identity, 'date': start, 'kind': 'weekly_batch', 'job_count': job_count, 'status': 'prepared'}, conn)
        service._put('local', 'self_practice_sync', {'enabled': True}, conn, False)


def rate(value):
    return '—' if value is None else f'{value:g}%'


def weekly_score(item):
    return round(item['correct'] * 100 / item['choices'], 1) if item['choices'] else None


def weekly_score_key(item):
    score = weekly_score(item)
    return score is None, score or 0, item.get('name', ''), item.get('person_id', '')


def summary_card(job, url):
    items = sorted(job['items'], key=weekly_score_key)
    answered = sum(item['answered'] for item in items)
    # Average people equally, not their rounded percentages or question counts.
    scores = [item['correct'] * 100 / item['choices'] for item in items if item['choices']]
    average = round(sum(scores) / len(scores), 1) if scores else None
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
        {'tag': 'div', 'text': {'tag': 'plain_text', 'content': f'选择题答对 {sum(item["correct"] for item in items)} 题'}}]
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
            score = weekly_score(item)
            details.append({'name': item.get('name') or '人员信息待核对', 'correct': item['correct'],
                'answered': item['answered'], 'accuracy': rate(score),
                'score': f'{score:g}' if score is not None else '暂无评分'})
        elements.extend([{'tag': 'div', 'text': {'tag': 'lark_md', 'content': f'**本周人员明细 · {len(items)}人**'}},
            {'tag': 'table', 'page_size': 10, 'row_height': 'low', 'freeze_first_column': True,
                'header_style': {'text_align': 'center', 'text_size': 'normal', 'background_style': 'grey', 'bold': True, 'lines': 2},
                'columns': [{'name': key, 'display_name': label, 'data_type': kind, 'width': width,
                    'horizontal_align': 'left' if key == 'name' else 'center', 'vertical_align': 'center'}
                    for key, label, kind, width in [('name', '姓名', 'text', '24%'), ('answered', '答题数量', 'number', '19%'),
                        ('correct', '答对数量', 'number', '19%'), ('accuracy', '正确率', 'text', '19%'), ('score', '评分', 'text', '19%')]],
                'rows': details}])
    elements.extend(weekly_question_elements(job, job.get('scope', '')))
    explanation = f'正确率按有选择题成绩的 {len(scores)} 人等权平均，仅计首次作答；问答及复习不计入。'
    explanation += ' 自动评分＝选择题首答正确数÷首答题数×100，按评分升序，无成绩置后。曲线按1个百分点分组，100%单列。'
    if job.get('parts', 1) > 1:
        explanation = f"历史分组 {job['part']}/{job['parts']}：以上仅统计本组 {len(items)} 人。" + explanation
    elements.extend([{'tag': 'note', 'elements': [{'tag': 'plain_text', 'content': explanation}]}, button])
    card['elements'] = elements
    card['header']['title']['content'] = (job['scope'] + '楼 · ' if job.get('scope') else '') + '每周学练汇总'
    return card


def make_card(job, url, paper=None, record=None, progress=None):
    if job['kind'] == 'shift_timeout' and progress:
        remaining = max(0, progress['assigned'] - progress['answered'])
        message = (f"{job['date']} · {job['scope']}楼{job['shift']}班\n"
                   + (f"本班已答{progress['answered']}/{progress['assigned']}题，还有{remaining}题未提交。" if progress['answered'] else
                      f"每日题单已发布，尚未开始作答，还有{remaining}题未提交。")
                   + '统计包含每日题单和当班自主练习的有效首答，请完成本班学练。')
        card = reminder_card(message, url + '/learning?view=practice&date=' + quote(job['date'], safe='') + '&shift=' + quote(job['shift'], safe=''))
        title = '学练待完成提醒'
    elif job['kind'] == 'shift_timeout' and not paper:
        message = f"{job['date']} · {job['scope']}楼{job['shift']}班\n每日题单已发布，尚未开始作答，请完成本班学练。"
        card = reminder_card(message, url + '/learning?view=practice&date=' + quote(job['date'], safe='') + '&shift=' + quote(job['shift'], safe=''))
        title = '学练待完成提醒'
    elif job['kind'] in {'timeout', 'shift_timeout'}:
        remaining = sum(bool(not q.get('invalid') and (not (record or {}).get('entries', {}).get(q['id'], {}).get('attempt')
            or (record or {}).get('entries', {}).get(q['id'], {}).get('needs_review'))) for q in paper['questions'])
        mode = '自主练习' if paper.get('mode') == 'practice' else '每日题单'
        message = (f"{paper['date']} · {mode}\n" + ('本班学练尚未完成，' if job['kind'] == 'shift_timeout' else '开始作答已满2小时，')
                   + f"还有{remaining}题未提交。已提交答案和草稿不受影响，请继续完成。")
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
    from . import learning_daily_report
    try:
        learning_daily_report.send_due(service, current)
    except Exception:
        service._last_error = '昨日学练日报暂未完成，原学练提醒继续处理。'
    settings = service.settings()
    duties = duty_recipients(service)
    assignments, unavailable = repeated_reminders(service, current)
    if settings['weekly_report_enabled']:
        enqueue_weekly(service, current, duties)
    with closing(service._connect()) as conn:
        jobs = [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind='engagement' AND COALESCE(json_extract(payload,'$.kind'),'')<>'daily_report' AND json_extract(payload,'$.status')='pending' AND json_extract(payload,'$.due_at')<=? ORDER BY json_extract(payload,'$.due_at'),key LIMIT 10", (current.timestamp(),))]
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
    progress_by_date = {}
    for job in jobs:
        if service._stop.is_set():
            break
        if job['kind'] == 'shift_timeout':
            if not service._restored:
                continue
            with service.transaction() as conn:
                job = service._get('engagement', job['id'], conn)
                if job['status'] != 'pending' or job['due_at'] > current.timestamp() or job.get('sending_until', 0) > current.timestamp():
                    continue
                # The durable lease also serializes schedulers in separate service instances.
                job['sending_until'] = current.timestamp() + 60
                service._put('engagement', job['id'], job, conn)
        enabled = service.settings()['timeout_reminder_enabled' if job['kind'] in {'timeout', 'shift_timeout'} else 'weekly_report_enabled']
        paper = service._get('paper', job.get('paper_id', ''))
        record = service._get('record', job.get('paper_id', '')) or {}
        person = people_by_id.get(job.get('person_id'))
        progress = None
        if job['kind'] == 'shift_timeout':
            context = assignments.get(job['assignment']) or {}
            paper = context.get('paper') or paper
            paper = service._get('paper', paper['id']) if paper else None
            record = service._get('record', paper['id']) or {} if paper else {}
            if job['date'] not in progress_by_date:
                from .learning_completion import daily_progress
                progress_by_date[job['date']] = daily_progress(service, job['date'], current)
            progress = progress_by_date[job['date']].get((job.get('person_id'), job.get('scope'), job.get('shift')))
            completed = progress and progress.get('status') == '已完成'
        if not enabled:
            job['status'] = 'cancelled'
        elif job['kind'] == 'weekly_summary' and not (job.get('audience') == 'group'
                or job.get('scope') in WEEKLY_SUPERVISOR_SCOPES):
            # Also cancel retired recipients in restored or previously queued reports.
            job['status'] = 'cancelled'
        elif job.get('expires_at', float('inf')) <= current.timestamp():
            job['status'] = 'expired'
        elif job['kind'] == 'timeout' and paper and paper.get('mode') != 'practice':
            job['status'] = 'superseded'
        elif job['kind'] == 'shift_timeout' and job['date'] in unavailable:
            job['due_at'] = current.timestamp() + 300
        elif job['kind'] == 'shift_timeout' and (completed or paper and (paper.get('deleted_at') or service._completed(paper, record))):
            job['status'] = 'skipped_completed'
        elif job['kind'] == 'shift_timeout' and (not context or context['open_id'] != job.get('open_id')):
            job['status'] = 'skipped_roster'
        elif job['kind'] == 'timeout' and (not paper or paper.get('deleted_at') or service._completed(paper, record)):
            job['status'] = 'skipped_completed'
        elif job.get('person_id') and person and not person.get('active'):
            job['status'] = 'inactive'
        elif job['kind'] in {'weekly', 'timeout'} and duties is not None and targets.get(job.get('person_id')) and targets[job['person_id']] not in duties:
            job['status'] = 'skipped_role'
        else:
            if job['kind'] == 'shift_timeout':
                recipients_for_job = [context['open_id']]
            elif job['kind'] == 'weekly_summary' and url:
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
                errors = []
                for target in recipients_for_job:
                    if target in sent_to or service._stop.is_set():
                        continue
                    identity = job['id'] + ':' + target if job.get('scope') and job['kind'] != 'shift_timeout' else job['id']
                    try:
                        interactive = getattr(service, '_weekly_card_sender', None)
                        if job['kind'] == 'weekly_summary':
                            if not callable(interactive):
                                raise core.LearningError('飞书智能体周报发送器尚未就绪，原周报等待重试。', 503)
                            result = interactive(job, target, identity)
                        else:
                            if card is None:
                                card = make_card(job, url, paper, record, progress)
                            result = service._send(target, card, identity)
                        ok = result[0] if isinstance(result, tuple) else bool(result)
                        if not ok:
                            errors.append('飞书智能体周报尚未确认送达' if job['kind'] == 'weekly_summary' else '飞书提醒尚未确认送达')
                    except Exception as exc:
                        ok = False
                        errors.append(str(exc) if isinstance(exc, core.LearningError) else '发送未完成（' + type(exc).__name__ + '），原任务等待重试')
                        if job['kind'] == 'weekly_summary' and isinstance(exc, core.LearningError) and exc.status == 410:
                            job['status'] = 'expired'
                            break
                    if ok:
                        sent_to.append(target)
                        # Persist each receipt before sending to another supervisor.
                        with service.transaction() as conn:
                            service._put('engagement', job['id'], job, conn)
                complete = all(target in sent_to for target in recipients_for_job)
                job['error'] = '' if complete else '；'.join(dict.fromkeys(errors))[:800] or '飞书发送失败，等待重试。'
                if not complete:
                    service._last_error = job['error']
                if complete:
                    job.update(status='sent', sent_at=current.isoformat(timespec='seconds'))
        with service.transaction() as conn:
            job.pop('sending_until', None)
            service._put('engagement', job['id'], job, conn)
            if not (service._get('local', 'self_practice_sync', conn) or {}).get('enabled'):
                service._put('local', 'self_practice_sync', {'enabled': True}, conn, False)
