"""Learning-owned endpoints and background jobs; reuse the existing Feishu socket."""
import asyncio
import json
import logging
import re
import uuid
from contextlib import closing

from fastapi import Request
from fastapi.responses import JSONResponse, Response
from starlette.formparsers import MultiPartException, MultiPartParser
from starlette.requests import ClientDisconnect

from .learning import LearningError, now, stamp
from .learning_sharing import SharingExercises, exercise_card, key, read_source, weekly_card
from .sharing_sources import SharingMaterials, combine_sources, read_material_link, source_url


class SharingRuntime:
    def __init__(self, runtime, get_learning):
        self.runtime, self.get_learning = runtime, get_learning
        self.core = None
        self.worker = self.generation = None
        self.wake = asyncio.Event()
        self.closing = False
        self.active_batch = ''
        self.restore_checked = False
        self.question_bank = None
        self.delivery_lock = asyncio.Lock()
        self.active_delivery_batch = ''
        self.deleting_batches = set()

    def service(self):
        if self.core is None:
            self.core = SharingExercises(self.get_learning())
        return self.core

    def allowed(self, open_id):
        auth = self.runtime.auth_manager
        if auth._open_id_explicitly_disabled(open_id):
            return False
        if 'learning' not in auth.account_profile(open_id).get('modules', []):
            return False
        core = self.service()
        people = core.learning._all('person')
        matches = [p for p in people if open_id in p.get('login_ids', [])]
        return not matches or len(matches) == 1 and bool(matches[0].get('active'))

    def bot(self):
        bot = getattr(self.runtime, 'feishu_assistant_channel', None)
        if not bot or not getattr(bot, 'messenger', None) or not getattr(bot, 'config', {}).get('app_id'):
            raise LearningError('飞书智能体尚未连接，请在设置中核对应用及卡片回调。', 503)
        return bot

    async def callback(self, payload, app_id):
        try:
            event = payload.get('event') or {}
            header = payload.get('header') or {}
            operator, action = event.get('operator') or {}, event.get('action') or {}
            value = action.get('value') or {}
            if header.get('app_id') != app_id or header.get('event_type') != 'card.action.trigger' or value.get('kind') != 'sharing_exercise':
                raise LearningError('无效的答题卡回调。', 403)
            result = await asyncio.to_thread(self.service().submit, value, action.get('form_value'), operator, app_id, self.allowed)
            self.wake.set()
            return result
        except LearningError as exc:
            return {'ok': False, 'toast': str(exc)}

    def update_batch(self, identity, *, expected_version=None, **fields):
        core = self.service()
        with core.learning.transaction() as conn:
            batch = core.learning._get('sharing_batch', identity, conn)
            if (not batch or batch.get('status') in {'published', 'closed', 'deleted'}
                    or expected_version is not None and batch.get('version') != expected_version):
                return False
            batch.update(fields)
            core.learning._put('sharing_batch', identity, batch, conn)
            return True

    async def finish_generation(self, batch, source, state):
        from .sharing_generation import generate_checked_questions

        async def checkpoint(questions, progress_state, progress):
            if self.closing or not await asyncio.to_thread(self.update_batch, batch['id'],
                    expected_version=batch['version'], source=source, title=source['title'],
                    participants=source['participants'], questions=questions,
                    generation_state=progress_state, progress=progress):
                raise asyncio.CancelledError()

        if batch.get('auto_generate', True):
            generate = getattr(self.runtime, 'generate_sharing_questions', None)
            if not callable(generate):
                raise LearningError('模型出题服务尚未就绪，已保留核对进度。', 503)
            if batch.get('import_questions'):
                from .sharing_question_import import import_questions
                questions = await import_questions(batch, source, state, generate, checkpoint)
            else:
                questions = await generate_checked_questions(batch, source, state, generate, checkpoint)
        else:
            questions = []
        notes = list(state['notes'])
        repaired = sum(bool(slot.get('repaired')) for slot in state['slots'])
        discarded = sum(slot['status'] == 'discarded' for slot in state['slots'])
        if repaired:
            notes.append(f'已自动修订或重新生成{repaired}道问题题目，其余合格题保持不变。')
        if discarded:
            notes.append(f'{discarded}道题经自动修订仍未通过校验，未纳入发布题目，已保留其余合格题。')
        if batch.get('import_questions'):
            notes.append(f'已从原文件提取{len(questions)}题，未补造题目或答案。请逐题核对，缺少答案或解析的题目补齐后再发布；导入题全部必答。')
        await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'],
            status='draft', progress='', title=source['title'], source=source,
            participants=source['participants'], questions=questions, error='', generation_state={}, generation_note=' '.join(notes))

    async def generate(self, batch):
        core = self.service()
        try:
            if not await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'], status='generating', error='', progress='正在读取完整纪要和参会人'):
                return
            saved = batch.get('generation_state')
            if isinstance(saved, dict) and saved.get('schema') == 1:
                await self.finish_generation(batch, batch['source'], saved)
                return
            sources = []
            from .sharing_images import SharingImages
            image_store = SharingImages(core.learning, self.runtime.service)
            material_state = (batch.get('material_state') or {}) if batch['status'] in {'queued', 'failed', 'generating'} else {}
            async def read_material(identity, label, reader):
                saved = material_state.get(identity, {})
                if saved.get('status') == 'ready':
                    return saved['source']
                material_state[identity] = {'title': label, 'status': 'reading', 'error': ''}
                await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'],
                    material_state=material_state, progress=f'正在读取材料：{label}')
                try:
                    value = await reader()
                except Exception as exc:
                    material_state[identity].update(status='failed', error=str(exc)[:300] if isinstance(exc, LearningError) else '材料读取未完成，已保留其他材料；可重试此项。')
                    await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'], material_state=material_state)
                    raise
                material_state[identity].update(status='ready', title=value.get('title') or label, source=value)
                await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'], material_state=material_state)
                return value
            # Explicit recipients do not require an @attendee section in the document.
            reader = read_material_link if batch.get('requested_participants') else read_source
            options = {'image_store': image_store} if batch.get('requested_participants') else {}
            minutes = await read_material('minutes', '出题文档', lambda: asyncio.to_thread(
                reader, self.runtime.service, batch['url'], **options)) if batch.get('url') else {}
            if minutes:
                sources.append(minutes)
            for url in batch.get('material_urls', []):
                sources.append(await read_material(url, url, lambda: asyncio.to_thread(read_material_link, self.runtime.service, url, image_store=image_store)))
            materials = SharingMaterials(core.learning)
            for identity in batch.get('material_ids', []):
                metadata = await asyncio.to_thread(core.learning._get, 'sharing_material', identity) or {}
                sources.append(await read_material(identity, metadata.get('name') or '上传材料 ' + identity[:8], lambda: asyncio.to_thread(materials.read, identity, batch['owner'])))
            source = combine_sources(sources)
            source.update(title=batch.get('requested_title') or minutes.get('title') or (sources[0].get('title') if sources else '分享会练习'),
                          participants=batch.get('requested_participants') or minutes.get('participants', []))
            if not minutes and not source['participants']:
                raise LearningError('没有可核验的答题名单，未生成或发送。')
            people = await asyncio.to_thread(core.learning._all, 'person')
            for person in source['participants']:
                matches = [p for p in people if person['open_id'] in p.get('login_ids', []) and p.get('active')]
                if len(matches) > 1:
                    raise LearningError('参会人对应多条学练身份，请先核对人员目录。')
                match = matches[0] if matches else {}
                scopes = match.get('scopes') or []
                person.update(person_id=match.get('id') or 'feishu:' + person['open_id'], scope=scopes[0] if len(scopes) == 1 else '')
            generate = getattr(self.runtime, 'generate_sharing_questions', None)
            if not await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'],
                    source=source, title=source['title'], participants=source['participants']):
                return
            if batch.get('auto_generate', True) and not callable(generate): raise LearningError('模型出题服务尚未就绪。', 503)
            from .sharing_images import SharingImages
            image_store = SharingImages(core.learning, self.runtime.service)
            image_refs = [image for item in sources for image in item.get('images', [])]
            source_images = []
            notes = []
            if image_refs:
                await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'], progress=f'正在读取{len(image_refs)}张题图，准备识别现场内容')
                source_images = await asyncio.to_thread(image_store.prepare, image_refs, ocr=not batch.get('auto_generate', True))
                source['images'] = source_images
                if not await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'], source=source):
                    return
                if callable(generate) and batch.get('auto_generate', True):
                    for offset in range(len(source_images)):
                        if self.closing:
                            return
                        group = source_images[offset:offset + 1]
                        if not await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'],
                                progress=f'MiniMax正在理解第{offset + 1}/{len(source_images)}张题图（现场内容及文字）'):
                            return
                        inputs = [await asyncio.to_thread(image_store.model_input, image) for image in group]
                        analyzed = await generate(batch['owner'], '', [], 1, mode='image_sources', images=inputs)
                        results = analyzed.get('images') if isinstance(analyzed, dict) else None
                        if (not isinstance(results, list) or len(results) != len(group)
                                or any(not isinstance(value, dict) or value.get('id') != original['id']
                                    or value.get('mode') not in {'ocr', 'vision', 'unreadable'}
                                    or not isinstance(value.get('text'), str) or len(value['text']) > 2000
                                    for value, original in zip(results, group))):
                            raise LearningError('图片识别结果无法与原图一一对应，请重新生成。', 502)
                        for image, value in zip(group, results):
                            image.update(text=value['text'], mode=value['mode'])
                            if image['mode'] == 'unreadable':
                                from openclaw_service.assistant.lighthouse_files import recognize_text
                                from openclaw_service.assistant.lighthouse_ai import AssistantError
                                try:
                                    text = await asyncio.to_thread(recognize_text, await asyncio.to_thread(image_store.content, image))
                                except AssistantError:
                                    text = ''
                                if text.strip() and len(text) <= 2000:
                                    image.update(text=text.strip(), mode='ocr')
                        if not await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'], source=source):
                            return
                source['images'] = source_images
                if len(source['text']) + sum(len(image['text']) for image in source_images) > 24000:
                    raise LearningError('正文与图片识别内容超过24000字，请拆分材料，未截断出题。', 413)
                modes = {image['mode'] for image in source_images}
                if 'vision' in modes:
                    notes.append('图片识别内容须对照原图核对题目与答案后发布。')
                if 'ocr' in modes:
                    notes.append('MiniMax未就绪或未可靠识别的图片仅按OCR文字出题，未推断图形、颜色或设备状态。')
                unreadable = sum(image['mode'] == 'unreadable' for image in source_images)
                if unreadable:
                    notes.append(f'{unreadable}张图片未识别到可用依据，未据此自动出题；仍可查看原图并手动录题。')
            await self.finish_generation(batch, source, {'schema': 1, 'slots': [], 'offset': 0, 'exhausted': False, 'notes': notes})
        except asyncio.CancelledError:
            current = await asyncio.to_thread(core.learning._get, 'sharing_batch', batch['id']) or {}
            resumable = bool(current.get('generation_state'))
            await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'],
                status='queued' if resumable else 'failed', error='生成暂停，已核对题目和修订进度保留，重启后继续。' if resumable else '生成被中断，请重新生成；未发布或发送。', progress='')
            raise
        except Exception as exc:
            from openclaw_service.assistant.lighthouse_ai import AssistantError
            error = str(exc) if isinstance(exc, (LearningError, AssistantError)) else '读取纪要或生成题目失败，请核对文档权限和本人模型配置后重试。'
            await asyncio.to_thread(self.update_batch, batch['id'], expected_version=batch['version'], status='failed', error=error[:500], progress='')
            logging.warning('Sharing exercise generation failed: %s', type(exc).__name__)
        finally:
            if self.active_batch == batch['id']:
                self.active_batch = ''
            self.wake.set()

    def pending(self, kind, limit):
        with closing(self.service().learning._connect()) as conn:
            if kind == 'sharing_batch':
                return [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind=? AND json_extract(payload,'$.status')='queued' ORDER BY updated LIMIT ?", (kind, limit))]
            return [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind=? AND json_extract(payload,'$.status')='pending' AND json_extract(payload,'$.due_at')<=? ORDER BY (json_extract(payload,'$.kind')='bank'), updated LIMIT ?", (kind, now().timestamp(), limit))]

    async def send(self, job):
        async with self.delivery_lock:
            if job['batch_id'] in self.deleting_batches:
                return
            self.active_delivery_batch = job['batch_id']
            try:
                await self._send(job)
            finally:
                self.active_delivery_batch = ''

    async def _send(self, job):
        from .portal_service import external_real_write_guard
        from .learning_engagement import WEEKLY_CHAT_ID
        core = self.service()
        batch = await asyncio.to_thread(core.learning._get, 'sharing_batch', job['batch_id'])
        if not batch or batch.get('status') == 'deleted':
            return
        attempt = await asyncio.to_thread(core.learning._get, 'sharing_attempt', job.get('attempt_id', ''))
        skipped = bool(job['kind'] in {'paper', 'reminder'} and (batch['status'] != 'published' or not attempt or attempt['submitted_at']))
        skipped |= (job['kind'] == 'reminder' and job.get('date') != now().date().isoformat())
        skipped |= (job['kind'] == 'weekly' and not core.learning.settings()['weekly_report_enabled'])
        try:
            if skipped:
                job['status'] = 'skipped'
            else:
                guard = external_real_write_guard()
                if not guard.get('real_write_allowed') or guard.get('mock_external'):
                    raise LearningError('当前环境未允许真实消息发送。', 403)
                if job['kind'] == 'bank':
                    question = next((q for q in batch['questions'] if q['id'] == job.get('question_id')), None)
                    if batch['status'] not in {'published', 'closed'} or question is None:
                        raise LearningError('题库入库任务不对应已发布题目，请核对原批次。', 409)
                    def upload():
                        from .learning_cloud import LearningCloud, LearningCloudError
                        if self.question_bank is None:
                            self.question_bank = LearningCloud(enabled=True)
                        try:
                            return self.question_bank.save_sharing_question(batch, question, job['id'])
                        except LearningCloudError as exc:
                            raise LearningError(str(exc), 503) from exc
                    job['record_id'] = await asyncio.to_thread(upload)
                    mid = ''
                elif job['kind'] == 'weekly':
                    from .feishu_delivery import FeishuDeliveryError, send_agent_message
                    url = core.learning.portal_url()
                    if not url: raise LearningError('学练入口尚未配置。')
                    try:
                        receipt = await send_agent_message(self.runtime, WEEKLY_CHAT_ID, weekly_card(job, batch, url),
                            receive_id_type='chat_id', message_uuid=str(uuid.uuid5(uuid.NAMESPACE_URL, job['id'])))
                    except FeishuDeliveryError as exc:
                        raise LearningError(str(exc), 503) from None
                    mid = receipt['message_id']
                else:
                    bot = self.bot()
                    if batch['app_id'] != bot.config['app_id']:
                        raise LearningError('飞书应用已更换，旧批次不能跨应用发送；请核对原应用设置。', 409)
                    if not await asyncio.to_thread(self.allowed, attempt['open_id']):
                        raise LearningError('参会账号已停用或无学练权限，未发送。', 403)
                    if batch.get('image_page_url'):
                        url = core.learning.portal_url()
                        if url:
                            batch = {**batch, 'image_page_url': url.rstrip('/') + '/learning?tab=sharing&batch_id=' + batch['id']}
                    card = exercise_card(batch, attempt, reminder=job['kind'] == 'reminder', result=job['kind'] == 'result')
                    client = await bot.messenger.client()
                    response = await client.post('https://open.feishu.cn/open-apis/im/v1/messages',
                        params={'receive_id_type': 'union_id' if attempt.get('union_id') else 'open_id'}, headers=await bot.messenger.headers(),
                        json={'receive_id': attempt.get('union_id') or attempt['open_id'], 'msg_type': 'interactive',
                              'content': json.dumps(card, ensure_ascii=False), 'uuid': key('sharing', job['id'])})
                    result = response.json()
                    mid = (result.get('data') or {}).get('message_id')
                    if result.get('code') != 0 or not mid:
                        raise LearningError(f"飞书消息发送未确认（code={result.get('code', 'unknown')}），请核对应用消息权限及接收范围。", 502)
                job.update(status='sent', message_id=mid, sent_at=stamp(), error='')
        except Exception as exc:
            job['failures'] += 1
            job.update(status='failed' if job['failures'] >= 5 and job['kind'] != 'weekly' else 'pending',
                       due_at=now().timestamp() + min(600, 30 * 2 ** job['failures']),
                       error=str(exc)[:500] if isinstance(exc, LearningError) else
                       '题库入库暂未确认，将核验原题后重试；已发答题卡不重发。' if job['kind'] == 'bank' else '消息发送结果未确认，将用原任务标识核验重试。')
        with core.learning.transaction() as conn:
            core.learning._put('sharing_delivery', job['id'], job, conn)
            if attempt:
                current = core.learning._get('sharing_attempt', attempt['id'], conn)
                if current:
                    # Never overwrite a concurrent answer with the pre-network snapshot.
                    prefix = {'paper': 'delivery', 'reminder': 'reminder_delivery', 'result': 'result_delivery'}[job['kind']]
                    current.update({prefix + '_status': job['status'], prefix + '_error': job.get('error', ''),
                                    prefix + '_at': job.get('sent_at', '')})
                    core.learning._put('sharing_attempt', current['id'], current, conn)
        core.changed_batch(batch, now().date().isoformat())

    async def run(self):
        while not self.closing:
            try:
                await asyncio.wait_for(self.wake.wait(), timeout=30)
            except asyncio.TimeoutError:
                pass
            self.wake.clear()
            try:
                if self.generation is None or self.generation.done():
                    batches = await asyncio.to_thread(self.pending, 'sharing_batch', 1)
                    if batches:
                        self.active_batch = batches[0]['id']
                        self.generation = asyncio.create_task(self.generate(batches[0]))
                core = self.service()
                if core.learning._restored:
                    if not self.restore_checked:
                        await asyncio.to_thread(self.recover_interrupted)
                        self.restore_checked = True
                    await asyncio.to_thread(core.schedule, now())
                    jobs = await asyncio.to_thread(self.pending, 'sharing_delivery', 5)
                    for job in jobs:
                        if self.closing: break
                        await self.send(job)
                        if job['kind'] == 'bank':
                            # Recheck new answer-card jobs between individual bank uploads.
                            self.wake.set()
                            break
                    if len(jobs) == 5:
                        await asyncio.sleep(0.5)
                        self.wake.set()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logging.warning('Sharing exercise worker deferred: %s', type(exc).__name__)

    def recover_interrupted(self):
        core = self.service()
        active = self.active_batch if self.generation is not None and not self.generation.done() else ''
        with core.learning.transaction() as conn:
            for batch in core.learning._all('sharing_batch', conn):
                if batch['status'] == 'generating' and batch['id'] != active:
                    resumable = bool(batch.get('generation_state'))
                    batch.update(status='queued' if resumable else 'failed', progress='', error='已恢复上次核对进度，将继续未完成的单题修订。' if resumable else '上次生成被中断，尚未发布；可重新生成。')
                    core.learning._put('sharing_batch', batch['id'], batch, conn)

    async def startup(self):
        await asyncio.to_thread(self.recover_interrupted)
        self.worker = asyncio.create_task(self.run())
        self.wake.set()

    async def close(self):
        self.closing = True
        for task in (self.worker, self.generation):
            if task: task.cancel()
        await asyncio.gather(*(t for t in (self.worker, self.generation) if t), return_exceptions=True)
        if self.question_bank:
            await asyncio.to_thread(self.question_bank.close)


def install(app, controller, runtime, get_learning, identity, read_json, check_origin, error_response):
    feature = SharingRuntime(runtime, get_learning)
    runtime.sharing_exercises = feature
    app.add_event_handler('startup', feature.startup)
    app.add_event_handler('shutdown', feature.close)

    async def endpoint(request: Request):
        try:
            session, actor = await asyncio.to_thread(identity, request)
            if not session: return controller._auth_required_response()
            core = await asyncio.to_thread(feature.service)
            bid = request.path_params.get('batch_id')
            if request.method == 'GET':
                if request.url.path.endswith('/sharing/people'):
                    core.learning._admin(actor)
                    data = {'items': await asyncio.to_thread(core.people)}
                elif bid and request.query_params.get('person'):
                    data = await asyncio.to_thread(core.answer_detail, bid, actor, request.query_params['person'])
                else:
                    data = await asyncio.to_thread(core.detail, bid, actor, progress_only=request.query_params.get('progress') == '1', compact=request.query_params.get('compact') == '1') if bid else await asyncio.to_thread(core.listing, actor, request.query_params.get('page', 1))
            else:
                check_origin(request)
                payload = await read_json(request)
                if bid and request.path_params.get('action') == 'answers':
                    data = await asyncio.to_thread(core.answer, bid, payload, actor, feature.allowed)
                elif not bid:
                    if payload.get('url'):
                        source_url(payload['url'], minutes=True)
                    data = await asyncio.to_thread(core.create, payload.get('url'), payload.get('operation_id'), actor, payload.get('question_types'),
                        material_urls=payload.get('material_urls'), material_ids=payload.get('material_ids'),
                        participant_ids=payload.get('participant_ids'), auto_generate=payload.get('auto_generate', True), title=payload.get('title', ''),
                        import_questions=payload.get('import_questions', False), initial_questions=payload.get('initial_questions'))
                elif request.method == 'POST' and request.path_params.get('action') == 'delete':
                    if feature.active_delivery_batch == bid or bid in feature.deleting_batches:
                        raise LearningError('后台正在处理发送或入库任务，请稍后再删除；已上传题目不会删除。', 409)
                    feature.deleting_batches.add(bid)
                    try:
                        data = await asyncio.to_thread(core.delete, bid, payload.get('version'), actor)
                    finally:
                        feature.deleting_batches.discard(bid)
                    if feature.active_batch == bid and feature.generation and not feature.generation.done():
                        feature.generation.cancel()
                elif request.method == 'PUT':
                    data = await asyncio.to_thread(core.edit, bid, payload, actor)
                else:
                    core.learning._admin(actor)
                    action = request.path_params['action']
                    if action == 'publish':
                        bot = feature.bot()
                        data = await asyncio.to_thread(core.publish, bid, payload.get('version'), actor, bot.config['app_id'])
                    else:
                        def update():
                            with core.learning.transaction() as conn:
                                batch = core.batch(bid, conn)
                                if payload.get('version') != batch['version']: raise LearningError('批次已变化，请刷新。', 409)
                                if action == 'retry' and batch['status'] == 'failed':
                                    # An explicit user retry grants a new bounded round only to unresolved slots.
                                    for slot in batch.get('generation_state', {}).get('slots', []):
                                        if slot['status'] in {'discarded', 'pending'}:
                                            slot.update(status='pending', attempts=0)
                                    state = batch.get('generation_state', {})
                                    valid = [slot['question'] for slot in state.get('slots', []) if slot['status'] == 'valid']
                                    if state and batch.get('import_questions'):
                                        state['exhausted'] = False
                                    elif state and (len(valid) < 5 or set(batch.get('question_types', [])) - {q['type'] for q in valid}):
                                        state['replenish'] = True
                                    batch.update(status='queued', error='', progress='', version=batch['version'] + 1)
                                elif action == 'close' and batch['status'] == 'published':
                                    batch.update(status='closed', closed_at=stamp(), version=batch['version'] + 1)
                                elif action == 'retry-delivery' and batch['status'] in {'published', 'closed'}:
                                    for row in conn.execute("SELECT payload FROM documents WHERE kind='sharing_delivery' AND json_extract(payload,'$.batch_id')=? AND json_extract(payload,'$.status')='failed'", (bid,)).fetchall():
                                        job = json.loads(row[0]); job.update(status='pending', failures=0, due_at=now().timestamp())
                                        core.learning._put('sharing_delivery', job['id'], job, conn)
                                else:
                                    raise LearningError('此批次当前不支持该操作。', 409)
                                core.learning._put('sharing_batch', bid, batch, conn)
                            return {'id': bid}, batch
                        data, updated_batch = await asyncio.to_thread(update)
                        core.changed_batch(updated_batch, now().date().isoformat())
                feature.wake.set()
            return await asyncio.to_thread(controller._json_ok, request, session, data)
        except Exception as exc:
            return error_response(exc)

    async def material_upload(request: Request):
        form, parser = None, None
        try:
            session, actor = await asyncio.to_thread(identity, request)
            if not session:
                return controller._auth_required_response()
            check_origin(request)
            core = await asyncio.to_thread(feature.service)
            core.learning._admin(actor)
            async def stream():
                import shutil
                import tempfile
                checked = 0
                try:
                    async for chunk in request.stream():
                        if checked <= 0:
                            if shutil.disk_usage(tempfile.gettempdir()).free < 64 * 1024 * 1024:
                                raise LearningError('临时磁盘空间不足，上传已停止；请释放空间后重试剩余文件。', 507)
                            checked = 16 * 1024 * 1024
                        checked -= len(chunk)
                        yield chunk
                except ClientDisconnect:
                    raise MultiPartException('上传已中断。') from None
            parser = MultiPartParser(request.headers, stream(), max_files=10, max_fields=0)
            form = await parser.parse()
            files = form.getlist('files')
            if set(form) != {'files'} or not 1 <= len(files) <= 10 or any(not hasattr(file, 'read') for file in files):
                raise LearningError('请选择1至10份分享材料。')
            items = []
            for file in files:
                items.append(await asyncio.to_thread(SharingMaterials(core.learning).upload, actor, file.filename, file.file))
            return await asyncio.to_thread(controller._json_ok, request, session, {'items': items})
        except MultiPartException:
            return error_response(LearningError('材料上传格式无效、文件数量超限或上传已中断。', 400))
        except Exception as exc:
            return error_response(exc)
        finally:
            if form is not None:
                await form.close()
            elif parser is not None:
                for handle in parser._files_to_close_on_error:
                    handle.close()

    async def source_image(request: Request):
        try:
            session, actor = await asyncio.to_thread(identity, request)
            if not session:
                return controller._auth_required_response()
            core = await asyncio.to_thread(feature.service)
            bid, image_id = request.path_params['batch_id'], request.path_params['image_id']
            item = await asyncio.to_thread(core.image, bid, image_id, actor)
            from .sharing_images import SharingImages
            content = await asyncio.to_thread(SharingImages(core.learning, runtime.service).content, item)
            # Recheck a revocation or batch removal that happened while recovering a missing cache.
            current_session, current_actor = await asyncio.to_thread(identity, request)
            if not current_session or current_actor['id'] != actor['id']:
                return controller._auth_required_response()
            await asyncio.to_thread(core.image, bid, image_id, current_actor)
            return Response(content, media_type='image/jpeg', headers={
                'Cache-Control': 'private, no-store', 'X-Content-Type-Options': 'nosniff'})
        except Exception as exc:
            return error_response(exc)

    app.add_api_route('/api/learning/sharing/{batch_id}/images/{image_id}', source_image, methods=['GET'])
    app.add_api_route('/api/learning/sharing/materials', material_upload, methods=['POST'])
    app.add_api_route('/api/learning/sharing/people', endpoint, methods=['GET'])
    app.add_api_route('/api/learning/sharing', endpoint, methods=['GET', 'POST'])
    app.add_api_route('/api/learning/sharing/{batch_id}', endpoint, methods=['GET', 'PUT'])
    app.add_api_route('/api/learning/sharing/{batch_id}/{action}', endpoint, methods=['POST'])
