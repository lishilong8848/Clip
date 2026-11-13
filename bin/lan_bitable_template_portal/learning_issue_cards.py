"""Private question-review cards on the existing learning worker and Feishu bridge."""
import asyncio
import hmac
import json
import logging
import re
import secrets
from collections import defaultdict
from contextlib import closing
from uuid import NAMESPACE_URL, uuid5

from . import learning as core
from .feishu_card_lifetime import EXPIRED_CARD_MESSAGE, expired, expired_card
from .learning_shift_storage import current_directory, person_open_id


REVIEWERS = ('陶必亮', '李世龙')
KIND = 'learning_question_issue'
MAX_INPUT = 1000
LOGGER = logging.getLogger(__name__)


def reviewer_open_id(directory, name, app_id):
    rows = [(rid, row) for rid, row in directory.items() if core.text(row.get('name')).strip() == name]
    if len(rows) != 1 or not rows[0][1].get('selectable') or rows[0][1].get('inactive'):
        return ''
    rid, row = rows[0]
    oid = (row.get('feishu_open_ids') or {}).get(app_id) or person_open_id({'id': 'directory:' + rid}, directory)
    if not isinstance(oid, str) or not re.fullmatch(r'ou_[A-Za-z0-9]+', oid):
        return ''
    matches = [key for key, other in directory.items() if oid in (other.get('login_ids') or [])
               or (other.get('feishu_open_ids') or {}).get(app_id) == oid]
    return oid if matches == [rid] else ''


def issue_version(issues):
    return core.digest(sorted((item['id'], item.get('version')) for item in issues))


def plain(content):
    return {'tag': 'plain_text', 'content': content}


def block(content):
    return {'tag': 'div', 'text': plain(content)}


def card(title, elements, theme='orange'):
    return {'schema': '2.0', 'config': {'update_multi': True, 'enable_forward': False},
            'header': {'title': plain(title), 'template': theme}, 'body': {'elements': elements}}


def button(ticket, title, action, *, style='primary', **values):
    return {'tag': 'button', 'text': plain(title), 'type': style,
            'behaviors': [{'type': 'callback', 'value': {
                'kind': KIND, 'ticket': ticket['id'], 'token': ticket['token'], 'action': action, **values,
            }}]}


def reasons(issues):
    comments = [core.text(text).strip() for item in issues for text in
                [item.get('description'), *(c.get('text') for c in item.get('comments', []) if not c.get('is_admin'))]]
    return '；'.join(dict.fromkeys(filter(None, comments))) or '原因待补充'


def answer(question):
    if question.get('type') == 'interview':
        return question.get('answer_text') or ('见参考答案附件' if any(
            a.get('kind') == 'answer' for a in question.get('attachments', [])) else '未设置')
    selected = set(question.get('correct_option_ids', []))
    return '；'.join(f'{chr(65 + i)}. {option["text"]}' for i, option in enumerate(question.get('options', []))
                    if option['id'] in selected) or '未设置'


def summary(ticket, question, issues):
    return card('学练题目质疑', [
        block('题目：' + (question.get('stem') or '题干待核对')[:1000]),
        block('当前答案：' + answer(question)[:1000]),
        block('质疑原因：' + reasons(issues)[:1000]),
        button(ticket, '在卡片中编辑', 'open'),
    ])


def input_box(name, label, value='', *, rows=2):
    return {'tag': 'input', 'name': name, 'input_type': 'multiline_text', 'rows': rows,
            'max_length': MAX_INPUT, 'label': plain(label), 'default_value': value, 'placeholder': plain(label)}


def editor(ticket, question, issues):
    options = question.get('options', [])
    fields = [('stem', '题干'), ('analysis', '解析')]
    if question['type'] == 'interview':
        fields.insert(1, ('answer_text', '参考答案'))
    text_values = [core.text(question.get(name)) for name, _ in fields] + [o['text'] for o in options]
    if (any(len(value.encode('utf-16-le')) // 2 > MAX_INPUT for value in text_values)
            or len(json.dumps(text_values, ensure_ascii=False).encode()) > 14000):
        return card('学练题目质疑', [block('这道题的正文超出飞书卡片可编辑范围，请在灯塔「画像学练 → 题目质疑」完整编辑。')])
    form = [input_box(name, label, core.text(question.get(name))) for name, label in fields]
    if question['type'] != 'interview':
        labels = [chr(65 + i) for i in range(len(options))]
        for index, (label, option) in enumerate(zip(labels, options)):
            form.append(input_box(f'option_{index}', f'选项 {label}', option['text'], rows=1))
        if options:
            form.append({'tag': 'multi_select_static', 'name': 'delete_options', 'required': False,
                         'placeholder': plain('选择要删除的选项（可选）'),
                         'options': [{'text': plain(label), 'value': label} for label in labels]})
        form.append(input_box('new_options', '新增选项（每行一项，编号 N1、N2…）'))
        selected = set(question.get('correct_option_ids') or [])
        correct = '、'.join(label for label, option in zip(labels, options) if option['id'] in selected)
        form.append(input_box('correct', '正确选项（例如 A、C 或 N1）', correct, rows=1))
    form.append(input_box('remark', '处理说明'))
    submit = button(ticket, '保存修改并处理质疑', 'save', version=question.get('version'), issues=issue_version(issues))
    submit.update(name='save', form_action_type='submit')
    form.append(submit)
    reload_button = button(ticket, '读取最新版本', 'open', style='default')
    return card('编辑质疑题目', [block('质疑原因：' + reasons(issues)[:700]),
                              {'tag': 'form', 'name': 'edit_question', 'elements': form}, reload_button])


def changes(question, fields):
    if not isinstance(fields, dict) or len(fields) > 34:
        raise core.LearningError('提交的表单无效，请重新打开卡片。')
    def field(name, previous=''):
        return core.clean(fields.get(name, previous), MAX_INPUT)

    payload = {'stem': field('stem', question['stem']), 'analysis': field('analysis', question.get('analysis', ''))}
    remark = field('remark')
    if not remark:
        raise core.LearningError('请填写处理说明。')
    if question['type'] == 'interview':
        payload['answer_text'] = field('answer_text', question.get('answer_text', ''))
    else:
        deleted = fields.get('delete_options') or []
        options = question.get('options', [])
        labels = [chr(65 + i) for i in range(len(options))]
        if not isinstance(deleted, list) or any(not isinstance(label, str) or label not in labels for label in deleted):
            raise core.LearningError('删除选项无效，请重新打开卡片。')
        selected = set(question.get('correct_option_ids', []))
        correct = re.split(r'[,，、;；\s]+', field('correct', '、'.join(
            label for label, option in zip(labels, options) if option['id'] in selected)).upper())
        by_label = {label: {**option, 'text': field(f'option_{index}', option['text'])}
                    for index, (label, option) in enumerate(zip(labels, options)) if label not in deleted}
        for index, line in enumerate(filter(None, (line.strip() for line in field('new_options').splitlines())), 1):
            by_label['N' + str(index)] = {'id': 'o_' + secrets.token_hex(16), 'text': line}
        if len(by_label) > 26 or not all(label in by_label for label in correct):
            raise core.LearningError('请核对正确选项和选项数量（最多26项）。删除正确选项后需重新填写答案。')
        payload.update(options=list(by_label.values()), correct_option_ids=[by_label[label]['id'] for label in correct])
    return payload, remark


def send_due(service, auth):
    """Send each pending question once per recipient and revision of its issues."""
    if not service.settings()['enabled'] or service._stop.is_set():
        return
    from .portal_service import external_real_write_guard
    guard = external_real_write_guard()
    if guard.get('mock_external') or not guard.get('real_write_allowed'):
        return
    directory, ready, app_id = current_directory(service)
    if not ready or not app_id:
        return
    grouped = defaultdict(list)
    with closing(service._connect()) as conn:
        for issue in service.open_question_issues(conn):
            grouped[issue['question_id']].append(issue)
    attempted = 0
    targets = [(name, oid) for name in REVIEWERS if (oid := reviewer_open_id(directory, name, app_id))
               and authorized(auth, oid, app_id)]
    for qid, issues in grouped.items():
        for name, oid in targets:
            if service._stop.is_set():
                return
            if deliver(service, core.now(), name, oid, app_id, qid, issues):
                attempted += 1
                if attempted >= 4:
                    return


def authorized(auth, oid, app_id):
    if auth.current_feishu_app_id() != app_id:
        return False
    profile = auth.account_profile(oid)
    return profile.get('enabled') and 'learning' in profile.get('modules', [])


def deliver(service, current, name, oid, app_id, qid, issues):
    from .learning_reminders import send_reminder
    identity = 'issue-card:' + core.digest([app_id, name, oid, qid, issue_version(issues)])
    timestamp = current.timestamp()
    with service.transaction() as conn:
        ticket = service._get('local', identity, conn)
        if ticket and (ticket.get('status') in {'sent', 'needs_review'} or ticket.get('retry_at', 0) > timestamp):
            return False
        live = service.open_question_issues(conn, qid)
        if not live or issue_version(live) != issue_version(issues):
            return False
        question = service._get('question', qid, conn) or live[0].get('question') or {}
        if not ticket:
            ticket = {'id': identity, 'question_id': qid, 'target': oid, 'name': name, 'app_id': app_id,
                      'token': secrets.token_urlsafe(32), 'created_at': timestamp}
            ticket['card'] = summary(ticket, question, live)
        if ticket.get('status') == 'sending':
            ticket.update(status='needs_review')
            service._put('local', identity, ticket, conn, False)
            return False
        ticket.update(status='sending', retry_at=timestamp + 300)
        service._put('local', identity, ticket, conn, False)
    receipts = []
    try:
        _, _, receipts = send_reminder(oid, ticket['card'], identity)
    except Exception:
        LOGGER.warning('学练质疑卡片发送结果待核对 id=%s', identity)
    receipt = receipts[0] if isinstance(receipts, list) and len(receipts) == 1 and isinstance(receipts[0], dict) else {}
    valid = receipt.get('open_id') == oid and receipt.get('message_uuid') == str(uuid5(NAMESPACE_URL, identity))
    if (valid and receipt.get('ok') is True and receipt.get('sender_app_id') == app_id
            and re.fullmatch(r'om_[A-Za-z0-9]+', str(receipt.get('message_id') or ''))
            and re.fullmatch(r'oc_[A-Za-z0-9]+', str(receipt.get('chat_id') or ''))):
        ticket.update(status='sent', sent_at=receipt.get('accepted_at') or core.now().timestamp(),
                      message_id=receipt['message_id'], chat_id=receipt['chat_id'])
        ticket.pop('card')
    elif valid and receipt.get('failure_kind') in {'not_sent', 'agent_required', 'bot_unavailable', 'blocked'}:
        ticket.update(status='pending')
    else:
        ticket.update(status='needs_review')
    with service.transaction() as conn:
        service._put('local', identity, ticket, conn, False)
    if ticket['status'] == 'needs_review':
        LOGGER.warning('学练质疑卡片回执待核对 id=%s', identity)
    return True


def apply(service, auth, payload, app_id):
    header, event = payload.get('header') or {}, payload.get('event') or {}
    action, context = event.get('action') or {}, event.get('context') or {}
    value = action.get('value') or {}
    oid = (event.get('operator') or {}).get('open_id', '')
    identity, token = value.get('ticket'), value.get('token')
    if (header.get('app_id') != app_id or header.get('event_type') != 'card.action.trigger'
            or value.get('kind') != KIND or not isinstance(identity, str)
            or not re.fullmatch(r'issue-card:[a-f0-9]{64}', identity) or not isinstance(token, str)
            or not isinstance(oid, str) or not re.fullmatch(r'ou_[A-Za-z0-9]+', oid)):
        raise core.LearningError('质疑卡片回调无效。', 403)
    directory, ready, current_app = current_directory(service)
    if not ready or current_app != app_id or not authorized(auth, oid, app_id):
        raise core.LearningError('当前账号无权处理此质疑。', 403)
    with service.transaction() as conn:
        if not service.settings()['enabled']:
            raise core.LearningError('画像学练暂未启用。', 503)
        ticket = service._get('local', identity, conn)
        if (not ticket or ticket.get('status') != 'sent' or ticket['app_id'] != app_id
                or ticket['target'] != oid or ticket['name'] not in REVIEWERS
                or reviewer_open_id(directory, ticket['name'], app_id) != oid
                or not hmac.compare_digest(token, ticket['token'])
                or ticket['message_id'] != context.get('open_message_id')
                or ticket['chat_id'] != context.get('open_chat_id')):
            raise core.LearningError('只能操作发送给本人的原质疑卡片。', 403)
        if expired(ticket.get('sent_at') or ticket['created_at']):
            return {'ok': True, 'toast': EXPIRED_CARD_MESSAGE, 'card': expired_card()}
        if value.get('action') not in {'open', 'save'}:
            raise core.LearningError('无效的质疑操作。')
        qid = ticket['question_id']
        question = service._get('question', qid, conn)
        issues = service.open_question_issues(conn, qid)
        if not issues:
            return {'ok': True, 'toast': '该题的质疑已处理。', 'card': card('质疑已关闭', [
                block((question or {}).get('stem', '')[:1000])], 'green')}
        if not question or question.get('status') == 'deleted':
            raise core.LearningError('原题已移除或题库暂未同步，请在灯塔核对质疑记录。', 409)
        if value['action'] == 'open':
            return {'ok': True, 'card': editor(ticket, question, issues), 'toast': '已打开题目'}
        if value.get('version') != question.get('version') or value.get('issues') != issue_version(issues):
            raise core.LearningError('题目或质疑已更新。请先保留你的修改，再点「读取最新版本」核对。', 409)
        edits, remark = changes(question, action.get('form_value'))
        # The dedicated grant is bound to this recipient, card, and disputed question.
        actor = {'id': oid, 'name': ticket['name'], 'is_admin': True}
        updated = service._save_question({'id': qid, 'version': question['version'], 'reason': remark, **edits},
                                         actor, conn, require_valid=True)
        for issue in issues:
            service._update_issue({'id': issue['id'], 'version': issue['version'], 'status': 'resolved',
                                   'remark': remark}, actor, conn)
    return {'ok': True, 'toast': '题目已保存，质疑已处理。', 'card': card('质疑已处理', [
        block(updated['stem'][:1000]), block('修订后答案：' + answer(updated)[:1000])], 'green')}


async def callback(runtime, payload, app_id):
    service = getattr(runtime, 'learning_service', None)
    if service is None:
        return {'ok': False, 'toast': '画像学练服务暂不可用，请稍后重试。'}
    try:
        return await asyncio.to_thread(apply, service, runtime.auth_manager, payload, app_id)
    except core.LearningError as exc:
        return {'ok': False, 'toast': str(exc)}
    except Exception:
        LOGGER.exception('学练质疑卡片操作失败')
        return {'ok': False, 'toast': '质疑操作未完成，请稍后重试。'}
