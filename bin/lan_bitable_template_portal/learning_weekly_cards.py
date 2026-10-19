"""Weekly report navigation and grading over the existing assistant connection."""
import asyncio
import hashlib
import hmac
import json
import math
import re
import secrets
import time
import uuid
from .learning import LearningError
from .learning_engagement import rate, summary_card, weekly_score, weekly_score_key


def key(*parts):
    return hashlib.sha256(':'.join(parts).encode()).hexdigest()


def plain(text):
    return {'tag': 'plain_text', 'content': str(text)}


def render(ticket, url, *, scope='', page=0, person_id=''):
    report = ticket['report']
    buildings = report.get('buildings') or {}
    scope = report.get('scope') or scope
    items = buildings.get(scope, []) if scope else report['items']
    base = summary_card({**report, 'items': items, 'scope': scope}, url)
    elements = base['elements'][:3]

    def button(label, action, **values):
        return {'tag': 'button', 'text': plain(label), 'type': 'default',
                'value': {'kind': 'learning_weekly', 'ticket': ticket['id'], 'token': ticket['token'],
                          'action': action, 'scope': scope, 'page': page, **values}}

    scopes = [report['scope']] if report.get('scope') else ['', *buildings]
    elements.insert(1, {'tag': 'column_set', 'flex_mode': 'flow', 'columns': [
        {'tag': 'column', 'width': 'auto', 'elements': [button(s + '楼' if s else '各楼汇总', 'view', scope=s, page=0)]} for s in scopes]})
    if person_id:
        row = next((r for r in items if r['person_id'] == person_id), None)
        if not scope or row is None:
            raise LearningError('该人员不属于本楼本周报告。', 403)
        score = weekly_score(row)
        metrics = [('首答题数', f"{row['answered']}题"), ('选择题答对', f"{row['correct']} / {row['choices']}题"),
                   ('正确率', rate(score)), ('自动评分', f'{score:g}分' if score is not None else '暂无评分'),
                   ('学习天数', f"{row.get('days', 0)}天"), ('问答题', f"{row.get('interviews', 0)}题"),
                   ('复习次数', f"{row.get('practice', 0)}次")]
        elements = [base['elements'][0], {'tag': 'div', 'text': plain(f"{row.get('name') or '人员信息待核对'} · {scope}楼")},
                    {'tag': 'div', 'fields': [{'is_short': True, 'text': {'tag': 'lark_md', 'content': f'**{label}**\n{value}'}} for label, value in metrics]}]
        if row['choices']:
            elements.append({'tag': 'chart', 'height': '240px', 'preview': True, 'chart_spec': {
                'type': 'pie', 'title': {'text': '本周选择题首答'},
                'data': {'values': [{'type': '正确', 'count': row['correct']}, {'type': '错误', 'count': row['choices'] - row['correct']}]},
                'valueField': 'count', 'categoryField': 'type', 'outerRadius': 0.8,
                'color': ['#20A46B', '#E05A47'], 'label': {'visible': True}, 'legends': {'visible': True, 'orient': 'bottom'}}})
        topics = sorted(row.get('topics', {}).items(), key=lambda item: (-item[1], item[0]))[:5]
        if topics:
            elements.append({'tag': 'div', 'text': plain('重点复习：' + '、'.join(f'{name}（{count}题）' for name, count in topics))})
        if row.get('ratings'):
            elements.append({'tag': 'div', 'text': plain('问答自评：' + '、'.join(f'{name} {count}题' for name, count in sorted(row['ratings'].items())))})
        elements.append(button('返回' + scope + '楼汇总', 'view'))
        base['header']['title'] = plain((row.get('name') or '人员') + ' · 本周学练画像')
    elif not scope:
        for building, rows in buildings.items():
            choices = sum(r['choices'] for r in rows)
            correct = sum(r['correct'] for r in rows)
            elements.append({'tag': 'column_set', 'columns': [
                {'tag': 'column', 'width': 'weighted', 'weight': 4, 'elements': [{'tag': 'div', 'text': plain(
                    f"{building}楼 · {len(rows)}人 · 答题 {sum(r['answered'] for r in rows)}题 · 答对 {correct}题 · 正确率 {rate(round(correct * 100 / choices, 1) if choices else None)}")}]},
                {'tag': 'column', 'width': 'auto', 'elements': [button('查看', 'view', scope=building, page=0)]}]})
        elements.extend(e for e in base['elements'] if e['tag'] == 'chart')
    else:
        rows = sorted(items, key=weekly_score_key)
        pages = max(1, math.ceil(len(rows) / 8))
        page = min(max(0, page), pages - 1)
        elements.append({'tag': 'div', 'text': plain(f'{scope}楼人员 · 自动评分升序，暂无评分置后 · {page + 1}/{pages}')})
        for row in rows[page * 8:(page + 1) * 8]:
            pid = row['person_id']
            score = weekly_score(row)
            score_text = f'{score:g}分' if score is not None else '暂无评分'
            elements.append({'tag': 'column_set', 'columns': [
                {'tag': 'column', 'width': 'weighted', 'weight': 3, 'elements': [{'tag': 'div', 'text': plain(
                    f"{row.get('name') or '人员信息待核对'} · {score_text}\n答题 {row['answered']}题 · 答对 {row['correct']}题 · 正确率 {rate(score)}")}]},
                {'tag': 'column', 'width': 'auto', 'elements': [
                    button('个人画像', 'profile', person_id=pid)]}]})
        if not rows:
            elements.append({'tag': 'div', 'text': plain('本周暂无作答记录。')})
        elements.append({'tag': 'column_set', 'columns': [{'tag': 'column', 'width': 'auto', 'elements': [b]} for b in (
            [button('上一页', 'view', page=page - 1)] if page else []) + (
            [button('下一页', 'view', page=page + 1)] if page + 1 < pages else [])]})
    elements.append({'tag': 'div', 'text': plain('自动评分＝本周有效选择题首答正确数÷首答题数×100，保留一位小数。问答题和复习不计入评分。')})
    return {'schema': '2.0', 'config': {'update_multi': True, 'wide_screen_mode': True},
            'header': base['header'], 'body': {'elements': [e for e in elements if e.get('columns', [True])]}}


class WeeklyCards:
    def __init__(self, runtime, get_learning):
        self.runtime, self.get_learning, self.loop = runtime, get_learning, None

    def bot(self):
        bot = getattr(self.runtime, 'feishu_assistant_channel', None)
        if not bot or not getattr(bot, 'messenger', None) or not getattr(bot, 'config', {}).get('app_id'):
            raise LearningError('飞书智能体尚未就绪，周报卡片等待重试。', 503)
        return bot

    def send_from_worker(self, job, target, identity):
        if self.loop is None or self.loop.is_closed():
            raise LearningError('飞书智能体周报发送器尚未就绪，原周报等待重试。', 503)
        future = asyncio.run_coroutine_threadsafe(self.send(job, target, identity), self.loop)
        try:
            return future.result(timeout=25)
        except LearningError:
            raise
        except Exception:
            future.cancel()
            raise LearningError('飞书智能体周报回执尚未确认，保留原任务重试，不改用旧应用重复发送。', 503) from None

    def prepare(self, job, target, app_id):
        service = self.get_learning()
        identity = 'weekly-card:' + key(job['id'], target)
        with service.transaction() as conn:
            ticket = service._get('engagement', identity, conn)
            if not ticket:
                report = dict(job)
                # Older queued summaries did not have per-building snapshots.
                if 'buildings' not in report:
                    from .learning_engagement import weekly_stats
                    people = {p['id']: p for p in service._all('person', conn)}
                    report['buildings'] = {s: [{**r, 'name': people.get(pid, {}).get('name', '人员信息待核对')}
                                              for pid, r in weekly_stats(service, job['date'], job['end'], conn, s).items()]
                                           for s in ([job['scope']] if job.get('scope') else 'ABCDEH')}
                ticket = {'id': identity, 'kind': 'weekly_card', 'status': 'prepared', 'token': secrets.token_urlsafe(32),
                          'app_id': app_id, 'target': target, 'report': report, 'message_id': '', 'chat_id': '',
                          'view_scope': job.get('scope', ''), 'page': 0}
                service._put('engagement', identity, ticket, conn)
            if ticket['app_id'] != app_id:
                if ticket.get('message_id') or ticket.get('attempted_at'):
                    raise LearningError('周报所属飞书应用已变化，请核对原应用，未重复发送。', 409)
                ticket['app_id'] = app_id
                service._put('engagement', identity, ticket, conn)
            return ticket

    async def send(self, job, target, identity):
        from .portal_service import external_real_write_guard
        if not external_real_write_guard().get('real_write_allowed'):
            raise LearningError('当前环境不允许真实周报发送，原任务保留。', 503)
        bot, service = self.bot(), self.get_learning()
        ticket = await asyncio.to_thread(self.prepare, job, target, bot.config['app_id'])
        if ticket['message_id']:
            return True
        if ticket.get('attempted_at') and time.time() - ticket['attempted_at'] > 3500:
            raise LearningError('原周报发送结果未确认且已超过飞书去重窗口，请核验原消息，未重复发送。', 503)
        if target.startswith('oc_'):
            receive_id, receive_type = target, 'chat_id'
        else:
            from upload_event_module.services.robot_webhook import DeliveryAttempt
            sender = getattr(self.runtime, 'feishu_public_sender', None)
            if sender is None or sender.closing:
                raise LearningError('飞书智能体接收人映射尚未就绪，原周报等待重试。', 503)
            recipient = await sender.recipient(bot, target)
            if isinstance(recipient, DeliveryAttempt):
                raise LearningError(recipient.message, 503)
            receive_type, receive_id = recipient
        card = render(ticket, service.portal_url(), scope=ticket['view_scope'])
        client = await bot.messenger.client()
        headers = await bot.messenger.headers()
        def attempted():
            with service.transaction() as conn:
                ticket.setdefault('attempted_at', time.time())
                service._put('engagement', ticket['id'], ticket, conn)
        await asyncio.to_thread(attempted)
        response = await client.post('https://open.feishu.cn/open-apis/im/v1/messages',
            params={'receive_id_type': receive_type}, headers=headers,
            json={'receive_id': receive_id, 'msg_type': 'interactive', 'content': json.dumps(card, ensure_ascii=False),
                  'uuid': str(uuid.uuid5(uuid.NAMESPACE_URL, identity))})
        data = response.json()
        sent = data.get('data') or {}
        code = data.get('code')
        if response.status_code < 500 and response.status_code != 408 and type(code) is int and code != 0:
            def rejected():
                with service.transaction() as conn:
                    ticket.pop('attempted_at', None)
                    service._put('engagement', ticket['id'], ticket, conn)
            await asyncio.to_thread(rejected)
            raise LearningError(f'飞书智能体拒绝周报（HTTP {response.status_code}，code={code}）。请确认智能体已加入指定汇总群、可发送消息并支持卡片；原任务等待重试。', 503)
        if response.status_code >= 400 or type(code) is not int or code != 0 or not re.fullmatch(r'om_[A-Za-z0-9]+', str(sent.get('message_id') or '')) or not re.fullmatch(r'oc_[A-Za-z0-9]+', str(sent.get('chat_id') or '')):
            raise LearningError('飞书智能体周报回执不完整，原任务等待核验，未改用旧应用发送。', 503)
        if receive_type == 'chat_id' and sent['chat_id'] != receive_id:
            raise LearningError('飞书智能体周报回执的群标识不一致，原任务等待核验，未重复发送。', 503)
        def receipt():
            with service.transaction() as conn:
                ticket.update(message_id=sent['message_id'], chat_id=sent['chat_id'], status='sent')
                service._put('engagement', ticket['id'], ticket, conn)
        await asyncio.to_thread(receipt)
        return True

    def apply(self, payload, app_id, user):
        service, auth = self.get_learning(), self.runtime.auth_manager
        header, event = payload.get('header') or {}, payload.get('event') or {}
        action, context = event.get('action') or {}, event.get('context') or {}
        value = action.get('value') or {}
        if header.get('app_id') != app_id or header.get('event_type') != 'card.action.trigger' or value.get('kind') != 'learning_weekly':
            raise LearningError('周报卡片回调无效。', 403)
        identity = value.get('ticket')
        if not isinstance(identity, str) or not re.fullmatch(r'weekly-card:[a-f0-9]{64}', identity):
            raise LearningError('周报标识无效。', 403)
        oid = user.get('open_id', '')
        profile = auth.account_profile(oid)
        if not oid or 'learning' not in profile.get('modules', []):
            raise LearningError('当前账号没有画像学练权限。', 403)
        with service.transaction() as conn:
            ticket = service._get('engagement', identity, conn)
            token = value.get('token')
            if not ticket or ticket['app_id'] != app_id or not isinstance(token, str) or not hmac.compare_digest(token, ticket['token']):
                raise LearningError('周报卡片已失效。', 403)
            if not ticket['message_id'] or ticket['message_id'] != context.get('open_message_id') or ticket['chat_id'] != context.get('open_chat_id'):
                raise LearningError('此操作不属于原周报消息。', 403)
            if ticket['target'].startswith('ou_') and ticket['target'] != oid:
                raise LearningError('只能操作发送给本人的周报卡片。', 403)
            scope, page = value.get('scope', ''), value.get('page', 0)
            report = ticket['report']
            if report.get('scope') and not (profile.get('level') in {'L0', 'L1'} or
                    profile.get('category') in {'supervisor', 'manager'} and report['scope'] in profile.get('scope_rules', [])):
                raise LearningError('当前已无该楼栋周报查看权限。', 403)
            if scope and scope not in report['buildings'] or report.get('scope') and scope != report['scope']:
                raise LearningError('无权切换到该楼栋。', 403)
            if type(page) is not int or page < 0 or page > 1000:
                raise LearningError('周报页码无效。')
            act = value.get('action')
            # Older cards refresh to the automatic result without saving submitted scores.
            if act not in {'view', 'profile', 'edit', 'score'}:
                raise LearningError('不支持的周报操作。')
            person_id = value.get('person_id', '') if act == 'profile' else ''
            if act == 'profile' and (not isinstance(person_id, str) or not person_id or not scope):
                raise LearningError('请从楼栋人员列表打开个人画像。', 400)
            card = render(ticket, service.portal_url(), scope=scope, page=page, person_id=person_id)
        return {'ok': True, 'toast': '已按答题结果自动评分，无需手动填写' if act in {'edit', 'score'} else '已切换', 'card': card}

    async def callback(self, payload, app_id):
        try:
            operator = (payload.get('event') or {}).get('operator') or {}
            user = await asyncio.wait_for(self.bot()._resolve_user({'sender_id': operator.get('open_id', ''),
                'union_id': operator.get('union_id', '')}), timeout=1.5)
            return await asyncio.to_thread(self.apply, payload, app_id, user)
        except asyncio.TimeoutError:
            return {'ok': False, 'toast': '账号核对暂忙，请稍后重试。'}
        except Exception as exc:
            return {'ok': False, 'toast': str(exc) if isinstance(exc, LearningError) else '周报操作未完成，请重新打开卡片重试。'}
