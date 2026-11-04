"""Read-only reconciliation of an inspection reminder with an uncertain receipt."""

import json
import math
import re
import time
from contextlib import closing


def known_chat(store, open_id, app_id):
    # Only an actual, same-app successful private-message receipt is evidence of
    # which conversation belongs to this recipient. Never infer it from a name.
    with store._lock, closing(store._connect()) as conn:
        store._ensure_schema_locked(conn)
        rows = conn.execute('''SELECT payload_json FROM json_documents
            WHERE namespace='feishu_public_delivery' AND json_valid(payload_json)
              AND json_extract(payload_json,'$.status')='sent'
              AND json_extract(payload_json,'$.receive_type')='open_id'
              AND json_extract(payload_json,'$.receive_id')=?
              AND json_extract(payload_json,'$.app_id')=?
            ORDER BY updated_at DESC LIMIT 100''', (open_id, app_id)).fetchall()
    chats = set()
    for row in rows:
        try:
            value = json.loads(row['payload_json'])
            receipt = value.get('receipt') or {}
            chat = receipt.get('chat_id') or ''
            if (receipt.get('sender_app_id') == app_id and value.get('message_uuid')
                    and receipt.get('message_uuid') == value.get('message_uuid') and
                    re.fullmatch(r'om_[A-Za-z0-9]+', str(receipt.get('message_id') or '')) and
                    re.fullmatch(r'oc_[A-Za-z0-9]+', str(chat))):
                chats.add(chat)
        except (TypeError, ValueError, AttributeError):
            continue
    return next(iter(chats)) if len(chats) == 1 else ''


def reconcile_inspection_delivery(store, open_id, message_uuid, target, route):
    """Confirm an old send only when an exact message is found; never infer absence."""
    from upload_event_module.services import robot_webhook as robot

    credentials = robot.current_app_credentials()
    app_id = credentials[0]
    attempted = route.get('attempted_at')
    if (route.get('status') not in {'posting', 'pending', 'sending', 'uncertain'}
            or route.get('channel') not in {'direct', 'agent'} or not app_id or route.get('app_id') != app_id
            or route.get('target') != ['open_id', open_id] or (route.get('identity_migration') or {}).get('blocked')
            or not isinstance(attempted, (int, float)) or not math.isfinite(attempted)
            or attempted <= 0 or time.time() - attempted < 3000):
        return None
    if isinstance(target, dict) and target.get('kind') == 'card' and isinstance(target.get('card'), dict):
        kind, content = 'interactive', target['card']
    elif isinstance(target, str):
        kind, content = 'text', {'text': target}
    else:
        return None
    chat_id = known_chat(store, open_id, app_id)
    if not chat_id:
        return None
    token, error = robot._get_tenant_access_token()
    if error or not token or robot.current_app_credentials(refresh=False) != credentials:
        return None
    # The normal sender stops retrying after 3000 seconds; allow for request
    # latency and modest clock skew. A missing/deleted message is never proof
    # of non-delivery, so it cannot authorize a fresh send.
    first, last = max(0, int(attempted - 120)), int(min(time.time(), attempted + 3300))
    params = {'container_id_type': 'chat', 'container_id': chat_id, 'start_time': str(first),
              'end_time': str(last), 'sort_type': 'ByCreateTimeAsc', 'page_size': 50,
              'card_msg_content_type': 'user_card_content'}
    seen = set()
    for _ in range(100):
        result = robot._request_json('GET', 'https://open.feishu.cn/open-apis/im/v1/messages',
            headers={'Authorization': 'Bearer ' + token}, params=params, retries=1)
        if robot.current_app_credentials(refresh=False) != credentials:
            return None
        data = result.get('data') if isinstance(result, dict) and result.get('code') == 0 else None
        if not isinstance(data, dict) or not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
            return None
        for item in data['items']:
            if (not isinstance(item, dict) or item.get('chat_id') != chat_id or item.get('msg_type') != kind
                    or item.get('deleted') is not False or (item.get('sender') or {}).get('id') != app_id):
                continue
            message_id = item.get('message_id') or ''
            if not re.fullmatch(r'om_[A-Za-z0-9]+', str(message_id)):
                continue
            try:
                created = int(item.get('create_time') or 0) / 1000
                body = (item.get('body') or {}).get('content')
                body = json.loads(body) if isinstance(body, str) else body
            except (ValueError, TypeError, AttributeError):
                continue
            if first <= created <= last and body == content:
                return {'message_id': message_id, 'chat_id': chat_id, 'sender_app_id': app_id,
                        'message_uuid': message_uuid, 'accepted_at': created,
                        'delivery_channel': route['channel'], 'reconciled_from_history': True}
        if not data['has_more']:
            return None
        cursor = data.get('page_token')
        if not isinstance(cursor, str) or not cursor or cursor in seen:
            return None
        seen.add(cursor)
        params['page_token'] = cursor
    return None
