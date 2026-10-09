"""Feishu-only, exact-request consent. Web sessions keep their existing policy."""
import hashlib
import json
import secrets
import threading
import time

from openclaw_service.assistant.lighthouse_ai import AssistantError

NAMESPACE = 'assistant_read_consent'


class ReadConsent:
    def __init__(self, store):
        self.store = store
        self.lock = threading.Lock()

    @staticmethod
    def key(actor, action, payload):
        owner = hashlib.sha256((actor['id'] + ':' + actor.get('channel', '')).encode()).hexdigest()
        request = hashlib.sha256(json.dumps([action, payload], sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        return owner + ':' + request

    def require(self, actor, action, payload, summary, question):
        if not actor.get('channel', '').startswith('feishu:'):
            return None
        key = self.key(actor, action, payload)
        with self.lock:
            current = self.store.get_document(NAMESPACE, key) or {}
            if current.get('expires', 0) > time.time() and current.get('question') == question:
                if current.get('approved'):
                    return None
            else:
                current = {'owner': actor['id'], 'channel': actor['channel'], 'action': action,
                    'payload': payload, 'question': question, 'summary': summary,
                    'code': secrets.token_hex(4), 'expires': time.time() + 300, 'approved': False}
                self.store.put_document(NAMESPACE, key, current)
        return {'confirmation_required': True, 'confirmation_code': current['code'],
            'message': f"需要你确认：{summary}。仅查询本人权限内数据，结果私信返回，不向群内展开。\n"
                       f"继续请回复“确认查询 {current['code']}”，5分钟内有效；也可回复“取消查询”。不确认则不读取。"}

    def approve(self, actor, code):
        with self.lock:
            prefix = self.key(actor, '', {}).split(':')[0] + ':'
            for row in self.store.list_documents(NAMESPACE, key_prefix=prefix):
                value = row.get('payload', row)
                if value.get('expires', 0) <= time.time():
                    self.store.delete_document(NAMESPACE, row['key'])
                    continue
                if (value.get('code') == code and value.get('owner') == actor['id']
                        and value.get('channel') == actor.get('channel') and value.get('expires', 0) > time.time()
                        and not value.get('approved')):
                    value['approved'] = True
                    self.store.put_document(NAMESPACE, self.key(actor, value['action'], value['payload']), value)
                    return value['question']
        raise AssistantError('该查询确认已过期、已使用或不属于当前账号和会话，请重新提出原问题。', 403)

    def pending(self, actor, question):
        prefix = self.key(actor, '', {}).split(':')[0] + ':'
        rows = self.store.list_documents(NAMESPACE, key_prefix=prefix)
        return [row['payload'] for row in rows if row['payload'].get('question') == question
                and not row['payload'].get('approved') and row['payload'].get('expires', 0) > time.time()]

    def cancel(self, actor):
        prefix = self.key(actor, '', {}).split(':')[0] + ':'
        with self.lock:
            for row in self.store.list_documents(NAMESPACE, key_prefix=prefix):
                self.store.delete_document(NAMESPACE, row['key'])
