"""Yesterday's daily papers, on the existing learning worker and durable outbox."""
import datetime as dt
import html
import json
import logging
import re
from collections import defaultdict
from contextlib import closing
from types import SimpleNamespace
from urllib.parse import urlencode
from uuid import NAMESPACE_URL, uuid4, uuid5

from . import learning as core
from . import learning_completion as completion
from .learning_reminders import reminder_card, resolve_roster


KIND = 'daily_report'
REPORT_TIME = dt.time(9)
LEASE_SECONDS = 120


def render_card(report, portal_url):
    """Pure renderer. Rows: person_id/name/scope/shift/answered/assigned/reason.

    report: date, completed/unfinished/failed/unverified lists; optional question_issues,
    question_problem_count, example and as_of.
    portal_url is the existing portal base URL, not a new privileged endpoint.
    """
    date = report['date']
    unfinished = list({(r['person_id'], r['scope'], r['shift']): r
                       for r in report.get('unfinished', [])}.values())
    completed = list({(r['person_id'], r['scope'], r['shift']): r
                      for r in report.get('completed', [])}.values())
    all_completed = bool(completed) and not any(report.get(key) for key in ('unfinished', 'failed', 'unverified'))
    people = {row['person_id'] for row in unfinished}
    assignments = {(row['person_id'], row['shift']) for row in unfinished}
    card = reminder_card('', portal_url.rstrip('/') + '/learning?' + urlencode(
        {'tab': 'completion'}), button='查看学练完成情况')
    card['header'] = {'template': 'green' if all_completed else 'orange', 'title': {
        'tag': 'plain_text', 'content': '昨日学练完成汇总' if all_completed else '昨日学练未完成汇总'}}
    elements = []
    if report.get('example'):
        elements.append({'tag': 'div', 'text': {'tag': 'plain_text',
            'content': '虚构示例，不代表实际进度。'}})
    summary = f"排班日：{date}\n白/夜共已核实待完成 {len(assignments)} 人班 · 去重 {len(people)} 人"
    if all_completed:
        correct = sum(row['correct'] for row in completed)
        objective = sum(row['objective_answered'] for row in completed)
        accuracy = f'{correct * 100 / objective:.1f}%（{correct}/{objective}题）' if objective else '暂无可统计客观题'
        summary = (f"排班日：{date}\n全部答题完成 · 答题人员 {len({row['person_id'] for row in completed})} 人"
                   f" · 完成 {len(completed)} 人班\n首次客观题正确率：{accuracy}")
    elements.append({'tag': 'div', 'text': {'tag': 'plain_text', 'content': summary}})
    if report.get('failed') or report.get('unverified'):
        elements.append({'tag': 'div', 'text': {'tag': 'plain_text',
            'content': '以上仅计已核实未完成；发送失败及资料待核对单列，不计为未完成。'}})
    for shift in ('白', '夜'):
        if all_completed:
            rows = [row for row in completed if row['shift'] == shift]
            text = f"已完成 {len({row['person_id'] for row in rows})} 人" if rows else '未安排每日题单'
            elements.append({'tag': 'div', 'text': {'tag': 'lark_md', 'content': f'**{shift}班**\n{text}'}})
            continue
        groups = defaultdict(list)
        for row in unfinished:
            if row['shift'] == shift:
                groups[row['scope']].append(row)
        lines = [f'**{shift}班**']
        for scope, rows in sorted(groups.items()):
            names = '、'.join('<font color="red">' + html.escape(
                f"{row['name']}（{row['answered']}/{row['assigned']}题）") + '</font>'
                for row in sorted(rows, key=lambda r: (r['name'], r['person_id'])))
            lines.append(f'{html.escape(scope)}楼：{names}')
        if not groups:
            lines.append('暂无已核实的未完成人员' if report.get('unverified') or report.get('failed')
                         else '无未完成人员')
        elements.append({'tag': 'div', 'text': {'tag': 'lark_md', 'content': '\n'.join(lines)}})
    for key, title in (('failed', '发送失败 / 未送达'), ('unverified', '资料 / 名单待核对')):
        rows = report.get(key, [])
        if rows:
            lines = [title]
            for row in rows:
                label = ' · '.join(filter(None, [row.get('scope', '') + '楼' if row.get('scope') else '',
                    row.get('shift', '') + '班' if row.get('shift') else '', row.get('name', '')]))
                lines.append((label + '：' if label else '') + row['reason'])
            elements.append({'tag': 'div', 'text': {'tag': 'plain_text', 'content': '\n'.join(lines)}})
    if report.get('question_issues'):
        lines = [f"待处理质疑（截至发送时）· {len(report['question_issues'])} 题"]
        for row in report['question_issues']:
            lines.append(f"{row['stem']}｜当前答案：{row['answer']}｜质疑原因：{'；'.join(row['reasons'])}")
        elements.append({'tag': 'div', 'text': {'tag': 'plain_text', 'content': '\n'.join(lines)}})
    if report.get('question_problem_count'):
        elements.append({'tag': 'div', 'text': {'tag': 'plain_text',
            'content': f"题库待核对 {report['question_problem_count']} 题"}})
    elements.append({'tag': 'note', 'elements': [{'tag': 'plain_text', 'content':
        '口径：每日题单及当班自主练习的有效首答，复习不重复计数；夜班跨零点归排班日，截至发送时核实；含当天上班的长白运维实习生，不含其他长白、分享会及未安排。'
        + (' 核实时间：' + report['as_of'] if report.get('as_of') else '')}]})
    card['elements'] = [*elements, card['elements'][-1]]
    return card


def _question_summary(service, conn):
    questions = {q['id']: q for q in service._all('question', conn)}
    grouped = {}
    for issue in sorted(service._all('issue', conn), key=lambda row: (row.get('updated_at', ''), row['id'])):
        if issue.get('status') not in {'pending', 'processing', 'needs_info'}:
            continue
        qid = issue['question_id']
        question = questions.get(qid) or issue.get('question', {})
        answer = question.get('answer_text') or ('见参考答案附件' if any(
            a.get('kind') == 'answer' for a in question.get('attachments', [])) else '未设置')
        options = question.get('options') or []
        selected = set(question.get('correct_option_ids') or [])
        if question.get('type') != 'interview' and selected and selected <= {o['id'] for o in options}:
            answer = '；'.join(f"{o.get('label') or chr(65 + index)}. {o['text']}"
                for index, o in enumerate(options) if o['id'] in selected)
        row = grouped.setdefault(qid, {'question_id': qid, 'reasons': []})
        row.update(stem=' '.join(core.text(question.get('stem')).split()) or '题干待核对',
                   answer=' '.join(core.text(answer).split()))
        for description in [issue.get('description'), *(c.get('text') for c in issue.get('comments', []) if not c.get('is_admin'))]:
            reason = ' '.join(core.text(description).split())
            if reason and reason not in row['reasons']:
                row['reasons'].append(reason)
    for row in grouped.values():
        if not row['reasons']:
            row['reasons'].append('原因待补充')
    return {'question_issues': list(grouped.values()), 'question_problem_count': sum(
        bool(q.get('problems')) and q.get('status') != 'deleted' for q in questions.values())}


def collect(service, date, current, conn, directory, app_id):
    """Read one SQLite snapshot; only verified daily assignments may become red."""
    from .learning_delivery_status import _delivery
    from .learning_engagement import recipients
    from .learning_shift_storage import person_open_id
    from .learning_schedule import windows
    from .learning_reminders import excluded_long_day

    result = {'date': date, 'as_of': current.isoformat(timespec='seconds'),
              'completed': [], 'unfinished': [], 'failed': [], 'unverified': [], **_question_summary(service, conn)}
    view = SimpleNamespace(_get=lambda kind, key: service._get(kind, key, conn), settings=service.settings)
    roster = completion._snapshot(view, date)
    if roster is None:
        result['unverified'].append({'reason': '昨日排班快照缺失或版本待核对，名单不全，不能判定为零人未完成。'})
        return result
    for issue in roster.get('issues', []):
        if not excluded_long_day(issue):
            result['unverified'].append({k: issue.get(k, '') for k in ('name', 'scope', 'shift')} | {
                'reason': issue.get('reason') or '排班名单待核对'})
    people = service._all('person', conn)
    mapped = [{**p, 'login_ids': [person_open_id(p, directory)]} for p in people]
    targets = {oid: pid for pid, oid in recipients(mapped).items()}
    people_by_id = {p['id']: p for p in people}
    duties = {}
    for raw in roster['people']:
        if isinstance(raw, dict) and (excluded_long_day(raw) or raw.get('shift') in {'长白', '休', '未安排'}):
            continue
        if not isinstance(raw, dict) or raw.get('shift') not in {'白', '夜'} or raw.get('scope') not in core.SCOPES:
            result['unverified'].append({'reason': '排班记录格式或班次待核对，未计入未完成。'})
            continue
        row = {k: raw.get(k, '') for k in ('name', 'scope', 'shift')}
        matches, warnings = resolve_roster([{**raw, 'open_id': raw.get('open_id', '')}], directory)
        oid = matches[0]['open_id'] if matches and not warnings else ''
        pid = targets.get(oid)
        person = people_by_id.get(pid, {})
        if (not app_id or not pid or raw.get('person_id') not in (None, '', pid)
                or person.get('name', '').strip() != row['name'] or person.get('scopes') != [row['scope']]):
            result['unverified'].append({**row, 'reason': '当前应用身份、姓名或楼栋未唯一匹配，人员名单待核对。'})
            continue
        duties[pid, row['scope'], row['shift']] = {**row, 'open_id': oid, 'person_id': pid}

    # Sharing remains separate; autonomous answers are credited by their actual duty window.
    data = {kind: [] for kind in ('person', 'paper', 'record', completion.KIND,
                                  'notification', 'sharing_batch', 'sharing_attempt', 'sharing_delivery')}
    data['person'] = people
    data['paper'] = service._all('paper', conn)
    data['notification'] = service._documents('notification', start=date, end=date, conn=conn)
    data['record'] = [record for paper in data['paper']
                      if (record := service._get('record', paper['id'], conn))]
    normalized = {**roster, 'people': list(duties.values())}
    view._get = lambda kind, key: normalized if kind == 'local' and key in (
        'shift_roster', 'shift_roster:' + date) else service._get(kind, key, conn)
    projected = {r['person_id']: r for r in completion.project(view, date, current=current, data=data)}
    publication = service._get('publication', service._publication_key(date), conn) or {}
    try:
        published_at = dt.datetime.fromisoformat(publication.get('created_at', ''))
        published_at = published_at.astimezone(core.TZ) if published_at.tzinfo else published_at.replace(tzinfo=core.TZ)
    except (ValueError, TypeError):
        published_at = None
    publication_ok = publication.get('date') == date and publication.get('_dirty') is False and published_at
    for (pid, scope, shift), duty in sorted(duties.items()):
        row = {k: duty[k] for k in ('person_id', 'name', 'scope', 'shift')}
        tasks = [t for t in projected.get(pid, {}).get('tasks', []) if
                 t['source'] in {'paper', 'roster'} and t['scope'] == scope and t['shift'] == shift]
        papers = [p for p in data['paper'] if p.get('person_id') == pid
                  and p.get('date') == date and p.get('mode') != 'practice']
        reason = ''
        end = windows(date, shift)[1]
        if current < end:
            reason = '本班尚未到截止时间，未计入未完成。'
        elif papers and all(p.get('deleted_at') for p in papers):
            continue
        elif not publication_ok or published_at >= end:
            reason = '题单未确认在本班截止前发布，不能判定个人未完成。'
        elif len(tasks) != 1:
            reason = '每日题单与本班归属不唯一，未计入未完成。'
        elif tasks[0]['source'] == 'roster' and papers:
            reason = '已有题单属于另一班或班次不明，未重复计算跨班完成情况。'
        elif tasks[0]['status'] == '未安排':
            continue
        elif tasks[0]['status'] == '待核对':
            reason = '题单或作答证据待核对，未计入未完成。'
        elif tasks[0]['status'] not in {'待答', '答题中', '逾期', '已完成'} or dt.datetime.fromisoformat(tasks[0]['start_at']) >= end:
            reason = '本班题单尚未到时或截止后才生成，未计入未完成。'
        saved = service._get(completion.KIND, completion.key(date, pid), conn) or {}
        if not reason and completion.valid(saved):
            for old in saved['tasks']:
                if old['source'] == 'paper' and old['scope'] == scope and old['shift'] == shift and (
                        not any(p['id'] == old['task_id'] for p in papers)
                        or old['answered'] and not service._get('record', old['task_id'], conn)):
                    reason = '历史完成投影的原题单或作答记录缺失，未按零作答催办。'
        if not reason and tasks[0]['source'] == 'roster':
            reserves = [service._get('reserve', rid, conn) for rid in publication.get('reserve_ids', [])]
            if not reserves or any(not r for r in reserves) or not any(
                    r.get('scope') == scope and r.get('date') == date and not r['_dirty'] for r in reserves):
                reason = '派发题库快照不全，不能判定个人未完成。'
            else:
                frozen = [r for r in reserves if r.get('scope') == scope and r.get('date') == date and not r['_dirty']]
                if not any(len({q['id'] for q in r.get('questions', []) if not q.get('invalid')})
                           >= tasks[0]['assigned'] for r in frozen):
                    reason = '昨日冻结题目不足，应答题数待核对；不使用当前题库补推昨日任务。'
        if reason:
            result['unverified'].append({**row, 'reason': reason})
            continue
        task = tasks[0]
        if task['status'] == '已完成':
            result['completed'].append({**row, **{key: task[key] for key in (
                'answered', 'assigned', 'correct', 'objective_answered')}})
            continue
        notices = [n for n in data['notification'] if n.get('kind') == 'shift'
                   and n.get('shift') == shift and n.get('open_id') == duty['open_id']]
        status, reason = _delivery(notices, duty['open_id'], app_id)
        if status != 'sent':
            result['failed' if status in {'failed', 'pending'} else 'unverified'].append({**row, 'reason': reason})
        else:
            task = tasks[0]
            result['unfinished'].append({**row, 'answered': task['answered'], 'assigned': task['assigned']})
    for key in ('failed', 'unverified'):
        result[key] = list({core.digest(row): row for row in result[key]}.values())
    return result


def _send(card, identity):
    from .learning_engagement import WEEKLY_CHAT_ID
    from .portal_service import external_real_write_guard
    guard = external_real_write_guard()
    if guard.get('mock_external') or not guard.get('real_write_allowed'):
        return False, '真实发送未启用', [{'chat_id': WEEKLY_CHAT_ID, 'message_uuid': identity,
                                      'ok': False, 'failure_kind': 'not_sent'}]
    from upload_event_module.services.robot_webhook import _deliver_many
    # The public chat wrapper discards receipts; use its shared dispatcher to retain evidence.
    return _deliver_many('interactive', card, [WEEKLY_CHAT_ID], 'chat_id', identity)


def send_due(service, current):
    """One report per Beijing day. An ambiguous attempt is never replayed automatically."""
    from .learning_engagement import WEEKLY_CHAT_ID
    from .learning_shift_storage import current_directory
    current = current.astimezone(core.TZ) if current.tzinfo else current.replace(tzinfo=core.TZ)
    if not service._restored or service._stop.is_set() or not service.settings()['enabled']:
        return
    date = (current.date() - dt.timedelta(days=1)).isoformat()
    identity = 'daily-report:' + date
    due = dt.datetime.combine(current.date(), REPORT_TIME, core.TZ)
    with service.transaction() as conn:
        # Midnight retires old work, even before today's 09:00 enqueue.
        old = conn.execute("SELECT payload FROM documents WHERE kind='engagement' "
                           "AND json_extract(payload,'$.kind')=? AND day<? "
                           "AND json_extract(payload,'$.status') IN ('pending','sending')", (KIND, date)).fetchall()
        for row in old:
            job = json.loads(row[0])
            job.update(status='needs_review' if job.get('attempted_at') else 'expired')
            if job.get('attempted_at'):
                job['error'] = '原日报跨日仍未确认回执，请核验原消息；未重复发送。'
                service._last_error = job['error']
                logging.getLogger(__name__).warning('%s id=%s', job['error'], job['id'])
            service._put('engagement', job['id'], job, conn)
        if current < due:
            return
        job = service._get('engagement', identity, conn)
        if not job:
            job = {'id': identity, 'kind': KIND, 'date': date, 'status': 'pending',
                   'due_at': due.timestamp(), 'attempt': 0}
        if job['status'] not in {'pending', 'sending'} or job.get('due_at', 0) > current.timestamp():
            if job['status'] == 'needs_review':
                service._last_error = job.get('error') or '原日报发送结果未确认，请核验原消息。'
            return
        if job.get('sending_until', 0) > current.timestamp():
            return
        if job.get('attempted_at'):
            job.update(status='needs_review', error='原日报发送结果未确认，请核验原消息；未重复发送。')
            service._put('engagement', identity, job, conn)
            service._last_error = job['error']
            logging.getLogger(__name__).warning('%s id=%s', job['error'], identity)
            return
        token = str(uuid4())
        job.update(status='pending', lease=token, sending_until=current.timestamp() + LEASE_SECONDS)
        service._put('engagement', identity, job, conn)
        service._put('local', 'self_practice_sync', {'enabled': True}, conn, False)

    def finish(**changes):
        with service.transaction() as conn:
            value = service._get('engagement', identity, conn)
            if value and value.get('lease') == token:
                # A late worker may retain a receipt, but cannot resurrect a cancelled task.
                if value['status'] in {'cancelled', 'expired'}:
                    changes = {k: v for k, v in changes.items() if k in {'delivery', 'sent_at'}}
                value.update(changes)
                value.pop('sending_until', None)
                value.pop('lease', None)
                service._put('engagement', identity, value, conn)
                if value.get('error'):
                    service._last_error = value['error']
                    logging.getLogger(__name__).warning('%s id=%s status=%s', value['error'], identity, value['status'])

    try:
        url = service.portal_url()
        directory, ready, app_id = current_directory(service)
        checked = core.now()
        checked = checked.astimezone(core.TZ) if checked.tzinfo else checked.replace(tzinfo=core.TZ)
        if checked.date() != current.date() or checked < due:
            finish(status='expired')
            return
        with service.transaction() as conn:
            job = service._get('engagement', identity, conn)
            if job.get('lease') != token or job['status'] != 'pending' or service._stop.is_set():
                return
            report = collect(service, date, checked, conn, directory if ready else {}, app_id if ready else '')
            if not any(report[key] for key in ('completed', 'unfinished', 'failed', 'unverified', 'question_issues', 'question_problem_count')):
                job.update(status='skipped_completed', report=report, error='')
            elif not url:
                job.update(due_at=current.timestamp() + 300, error='学练入口地址未就绪，日报等待重试。')
            else:
                job['report'] = report
            service._put('engagement', identity, job, conn)
        if job['status'] != 'pending' or not url:
            finish()
            return
        with service.transaction() as conn:
            latest = service._get('engagement', identity, conn)
            checked = core.now()
            checked = checked.astimezone(core.TZ) if checked.tzinfo else checked.replace(tzinfo=core.TZ)
            allowed = (not service._stop.is_set() and service.settings()['enabled']
                       and latest.get('lease') == token and latest['status'] == 'pending'
                       and checked.timestamp() < job['sending_until'] and checked.date() == current.date())
            if allowed:
                report = collect(service, date, checked, conn, directory if ready else {}, app_id if ready else '')
                latest['report'] = report
                if not any(report[key] for key in ('completed', 'unfinished', 'failed', 'unverified', 'question_issues', 'question_problem_count')):
                    latest.update(status='skipped_completed', attempted_at=None, error='')
                    allowed = False
                else:
                    card = render_card(report, url)
                    latest.update(status='sending', attempted_at=checked.timestamp(), attempt=latest['attempt'] + 1,
                        message_uuid=str(uuid5(NAMESPACE_URL, identity + ':' + str(latest['attempt'] + 1))))
                service._put('engagement', identity, latest, conn)
        if not allowed:
            finish(attempted_at=None, status=latest['status'] if latest['status'] != 'sending' else 'pending')
            return
        if service._stop.is_set() or not service.settings()['enabled']:
            finish(attempted_at=None, status='pending')
            return
        job = latest
        result = _send(card, job['message_uuid'])
        deliveries = result[2] if isinstance(result, tuple) and len(result) > 2 and isinstance(result[2], list) else []
        receipt = deliveries[0] if len(deliveries) == 1 and isinstance(deliveries[0], dict) else {}
        valid = (receipt.get('chat_id') == WEEKLY_CHAT_ID and receipt.get('message_uuid') == job['message_uuid'])
        if (valid and result[0] is True and receipt.get('ok') is True
                and re.fullmatch(r'om_[A-Za-z0-9]+', str(receipt.get('message_id') or ''))
                and receipt.get('sender_app_id') == app_id and app_id):
            finish(status='sent', delivery=deliveries, sent_at=current.isoformat(timespec='seconds'), error='')
        elif valid and result[0] is False and receipt.get('ok') is False and receipt.get('failure_kind') in {
                'not_sent', 'bot_unavailable', 'agent_required'}:
            finish(status='pending', attempted_at=None, delivery=deliveries, due_at=current.timestamp() + 300,
                   error='日报未送达，稍后重新核实完成情况再投递。')
        else:
            finish(status='needs_review', delivery=deliveries,
                   error='日报回执不完整或发送结果未确认，请核验原消息；未重复发送。')
    except Exception:
        with closing(service._connect()) as conn:
            latest = service._get('engagement', identity, conn) or {}
        finish(status='needs_review' if latest.get('attempted_at') else 'pending',
               due_at=current.timestamp() + 300, error='日报核实或投递未完成，已保留任务；未知发送结果不自动补发。')
        service._last_error = '昨日学练日报未完成，请检查原任务及回执。'
