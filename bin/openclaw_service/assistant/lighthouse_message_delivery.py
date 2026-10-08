"""Conversation-only content choices for natural-language message delivery."""
import datetime as dt
import hashlib
import re

MESSAGE_INTENT = re.compile(r"发给|发送给|转发给|(?:发|发送|转发).{0,30}(?:飞书|本人|自己)|飞书.{0,20}(?:发|发送|转发)|(?:给|向).{1,30}(?:发|发送|转发)")


def full_notice_self_request(question):
    """Only unqualified current notice lists; other recipients/filters use tools."""
    return bool(re.search(r'完整|全部|所有', question) and re.search(r'进行中|未结束', question)
        and re.fullmatch(r'(?:请|帮我|麻烦|把|将|的|完整|全部|所有|当前|现在|今天|今日|'
                         r'[ABCDEH]楼?|110站?|、|，|\s|进行中|未结束|维保|变更|检修|轮巡|设备调整|上下电)*'
                         r'通告(?:清单|列表|明细)?(?:的|完整|全部|所有|\s)*'
                         r'(?:发给|发送给|转发给)(?:我|本人|自己)[。！!\s]*', question, re.I))


def turn_downloads(turn):
    links = list(turn.get('downloads') or [])
    for result in (turn.get('plan') or {}).get('results', []):
        links.extend(result.get('downloads') or [])
        links.extend((result.get('job_result') or {}).get('downloads') or [])
    return list({item['url']: item for item in links if isinstance(item, dict) and item.get('url')}.values())


def content_field(agent, actor, body, queries, file_ids, index):
    choices, selected = {}, []
    text = str(body.get('text') or '')
    if text.strip():
        choices['typed_text'] = {'kind': 'text', 'text': text, 'label': '本次待发文字：' + text[:90].replace('\n', ' ')}
        selected.append('typed_text')
    candidates = list(agent.assistant._state(actor).get('turns', []))[-10:]
    for value in (queries or {}).values():
        if isinstance(value, dict) and value.get('is_historical'):
            candidates.extend(value.get('items') or [])
    for turn in candidates:
        if not agent.assistant._allowed(turn, actor) or turn.get('status') != 'completed':
            continue
        for link in turn_downloads(turn):
            try:
                operation = agent.catalog.read_operation({'api_id': 'GET ' + link['url']})
            except Exception:
                continue
            key = 'download_' + hashlib.sha256(link['url'].encode()).hexdigest()[:20]
            choices[key] = {'kind': 'download', 'operation': operation, 'label': '文件 · ' + str(link.get('name') or '会话生成文件')}
        if not turn.get('answer') or turn.get('plan'):
            continue
        if turn['answer'] == text:
            continue
        identity = str(turn.get('operation_id') or '')
        if not identity:
            continue
        stamp = dt.datetime.fromtimestamp(float(turn.get('at') or 0), dt.timezone(dt.timedelta(hours=8))).strftime('%m-%d %H:%M')
        choices['turn_' + identity] = {'kind': 'text', 'text': turn['answer'],
            'label': stamp + ' · ' + str(turn.get('question') or turn['answer'])[:90].replace('\n', ' ')}
    for identity in dict.fromkeys(file_ids):
        file = agent.files.get(actor, identity)
        choices['file_' + identity] = {'kind': 'file', 'id': identity, 'label': '文件 · ' + file['name']}
    if not choices:
        return None
    return {'name': f'step{index}.message_content', 'operation_index': index, 'path': 'message_content', 'section': 'body',
            'type': 'multiselect', 'label': '要发送哪些内容（可多选）', 'required': False, 'native_message_content': True,
            'options': [{'value': key, 'label': item['label']} for key, item in choices.items()], '_contents': choices, 'value': selected}
