"""Editable follow-up questions, bound to the same authenticated conversation."""
import hashlib
import hmac
import json
import re
import secrets
import time

from openclaw_service.assistant.lighthouse_ai import AssistantError, safe_text

NAMESPACE = 'feishu_question_cards'


def _question_key(actor, message):
    root = message.get('root_id') or (message.get('message_id') if message.get('chat_type') == 'group' else '')
    return actor['id'] + ':' + message['chat_id'] + ':' + root if root else actor['id']


def prepare_question_card(bot, message, actor, conversation, turn):
    clarification = turn.get('clarification')
    if (not isinstance(clarification, dict) or turn.get('plan')
            or turn.get('status') in {'failed', 'stopped', 'cancelled', 'superseded', 'pending'}):
        return None
    options = clarification.get('options', [])
    ticket = secrets.token_urlsafe(24)
    store = bot.runtime.state_store
    key = _question_key(actor, message)
    previous = store.get_document(NAMESPACE, key) or {}
    if previous.get('ticket'):
        store.delete_document(NAMESPACE, 'ticket:' + previous['ticket'])
    stored = {
        'ticket': ticket, 'owner': actor['id'], 'union_id': message['union_id'],
        'conversation_id': conversation['conversation_id'], 'operation_id': turn['operation_id'],
        'chat_id': message['chat_id'], 'options': options, 'multiple': clarification.get('multiple', False),
        'input_type': clarification.get('input_type', 'text'),
        'chat_type': message.get('chat_type'), 'root_id': message.get('root_id') or message['message_id'],
        'question': safe_text(turn.get('question') or message.get('question') or '', limit=12000),
        'expires': time.time() + 1800,
        'key': key,
    }
    store.put_document(NAMESPACE, key, stored)
    store.put_document(NAMESPACE, 'ticket:' + ticket, stored)
    if hasattr(store, 'list_documents'):
        previous_cards = store.list_documents(NAMESPACE, key_prefix=actor['id'])
        for item in previous_cards:
            saved = item.get('payload') or {}
            if saved.get('owner') == actor['id'] and saved.get('expires', 0) < time.time():
                store.delete_document(NAMESPACE, item['key'])
                store.delete_document(NAMESPACE, 'ticket:' + saved.get('ticket', ''))
    # The normal reply carries the complete question, including in live updates.
    # Keep this supplementary form from displaying the same answer a second time.
    elements = [{'tag': 'markdown', 'content': '请在下方补充信息，也可以直接回复消息。'}]
    if options:
        elements.append({'tag': 'multi_select_static' if clarification.get('multiple') else 'select_static',
            'name': 'choice', 'placeholder': {'tag': 'plain_text', 'content': '选择回答'},
            'options': [{'text': {'tag': 'plain_text', 'content': text}, 'value': str(index)}
                        for index, text in enumerate(options)]})
    from .feishu_notice_cards import _simple_component
    answer_control = (_simple_component({'type': stored['input_type']}, 'answer')[0]
        if stored['input_type'] != 'text' else {'tag': 'input', 'name': 'answer', 'input_type': 'multiline_text', 'rows': 2, 'max_length': 1000,
         'placeholder': {'tag': 'plain_text', 'content': '也可以直接填写或补充说明'}})
    elements.extend([
        answer_control,
        {'tag': 'button', 'name': 'reply', 'form_action_type': 'submit', 'type': 'primary',
         'text': {'tag': 'plain_text', 'content': '补充并继续'},
         'behaviors': [{'type': 'callback', 'value': {'kind': 'assistant_question', 'ticket': ticket}}]},
    ])
    if options:
        from .feishu_notice_cards import _compact_form, _field_column
        elements = [elements[0], *_compact_form([
            _field_column('选择回答', elements[1]), _field_column('补充说明', elements[2])]), *elements[3:]]
    return {'schema': '2.0', 'body': {'elements': [{'tag': 'form', 'name': 'question_form', 'elements': elements}]}}


def parse_question_event(payload, app_id):
    if not isinstance(payload, dict):
        return None
    header, event = payload.get('header') or {}, payload.get('event') or {}
    if not isinstance(header, dict) or not isinstance(event, dict):
        return None
    action = event.get('action') or {}
    if not isinstance(action, dict):
        return None
    value = action.get('value') or {}
    if not isinstance(value, dict):
        return None
    if header.get('app_id') != app_id or header.get('event_type') != 'card.action.trigger' or value.get('kind') != 'assistant_question':
        return None
    operator, context = event.get('operator') or {}, event.get('context') or {}
    if not isinstance(operator, dict) or not isinstance(context, dict):
        return None
    values = [operator.get('open_id'), operator.get('union_id'), context.get('open_chat_id'), context.get('open_message_id')]
    if not all(isinstance(value, str) and re.fullmatch(pattern, value) for value, pattern in zip(values,
            (r'ou_[A-Za-z0-9]+', r'on_[A-Za-z0-9]+', r'oc_[A-Za-z0-9]+', r'om_[A-Za-z0-9]+'))):
        return None
    event_id = header.get('event_id')
    ticket, form = value.get('ticket'), action.get('form_value')
    if (not isinstance(event_id, str) or not 1 <= len(event_id) <= 128
            or not isinstance(ticket, str) or not re.fullmatch(r'[A-Za-z0-9_-]{32}', ticket)
            or not isinstance(form, dict) or set(form) - {'choice', 'answer', 'reply'}
            or len(json.dumps(form)) > 16000):
        return None
    identity = hashlib.sha256((event_id + ':' + operator['union_id']).encode()).hexdigest()
    return {'kind': 'question_action', 'message_id': 'om_question' + identity,
            'reply_to_message_id': context['open_message_id'], 'chat_id': context['open_chat_id'],
            'chat_type': 'group', 'sender_id': operator['open_id'], 'union_id': operator['union_id'],
            'question': '补充询问', 'problem': '', 'question_form': {'ticket': ticket, 'values': form}}


def read_question_reply(bot, message, actor, conversation):
    form = message.get('question_form') or {}
    candidate = bot.runtime.state_store.get_document(NAMESPACE, 'ticket:' + str(form.get('ticket') or '')) or {}
    stored = bot.runtime.state_store.get_document(NAMESPACE, candidate.get('key') or actor['id']) or {}
    shared = False
    if (stored.get('ticket') != form.get('ticket') or stored.get('owner') != actor['id']) and getattr(bot, 'inbox', None):
        topic = bot.inbox.thread_for(message)
        if topic and topic['chat_type'] == 'group' and candidate.get('owner') != actor['id'] and candidate.get('chat_type') == 'group' and candidate.get('root_id') == topic['root_id']:
            stored, shared = candidate, True
    if ((not shared and (stored.get('owner') != actor['id'] or stored.get('union_id') != message.get('union_id')
            or stored.get('conversation_id') != conversation.get('conversation_id')
            ))
            or stored.get('chat_id') != message.get('chat_id') or stored.get('expires', 0) <= time.time()
            or not hmac.compare_digest(str(stored.get('ticket', '')), str(form.get('ticket', '')))):
        raise AssistantError('这条询问已失效或不属于你，请使用本人最新消息中的表单。', 409)
    topic_id = stored.get('chat_id', '') + ':' + stored.get('root_id', '') if stored.get('chat_type') == 'group' else ''
    last = next((turn for turn in reversed(conversation.get('turns', [])) if turn.get('question')
                 and (not topic_id or turn.get('feishu_topic') == topic_id or turn.get('operation_id') == stored.get('operation_id'))), {})
    operation = 'feishu_' + hashlib.sha256(message['message_id'].encode()).hexdigest()[:40]
    if not shared and last.get('operation_id') not in {stored['operation_id'], operation}:
        raise AssistantError('已经有新的对话，请在最新消息中补充。', 409)
    values = form.get('values') or {}
    answer = values.get('answer') or ''
    choices = values.get('choice') or []
    choices = choices if isinstance(choices, list) else [choices]
    options = stored.get('options', [])
    if (not isinstance(answer, str) or len(answer) > 1000 or len(choices) > len(options)
            or not stored.get('multiple') and len(choices) > 1
            or any(not isinstance(item, str) or item not in {str(i) for i in range(len(options))} for item in choices)):
        raise AssistantError('请使用表单提供的候选或填写有效的补充文字。')
    text = '、'.join(options[int(item)] for item in dict.fromkeys(choices))
    if answer.strip() and stored.get('input_type', 'text') != 'text':
        from .feishu_notice_cards import _coerce_card_string
        from .feishu_notice_forms import typed_child_value
        answer = str(typed_child_value({'type': stored['input_type'], 'label': '补充回答'}, _coerce_card_string(answer)))
    text = '\n'.join(filter(None, (text, answer.strip())))
    if not text:
        raise AssistantError('请选择一项回答或填写补充说明。')
    if shared:
        return '针对话题问题：' + stored.get('question', '') + '\n补充回答：' + text, None
    return text, stored['operation_id']
