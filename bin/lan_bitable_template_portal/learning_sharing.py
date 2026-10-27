"""Meeting exercises, immutable per-person papers and durable delivery receipts."""
import copy
import datetime as dt
import hashlib
import hmac
import html
import json
import logging
import random
import re
import secrets
import unicodedata
from contextlib import closing

from .learning import LearningError, TZ, now, stamp

KINDS = {'sharing_batch', 'sharing_attempt', 'sharing_delivery'}
QUESTION_TYPES = {'single': '单选题', 'multiple': '多选题', 'interview': '问答题'}


def selected_types(values):
    if (not isinstance(values, list) or not values or len(values) > 3
            or any(not isinstance(value, str) or value not in QUESTION_TYPES for value in values)
            or len(set(values)) != len(values)):
        raise LearningError('请至少选择一种题型：单选、多选或问答。')
    return [kind for kind in QUESTION_TYPES if kind in values]


def question_mix(count, types):
    kinds = selected_types(types)
    if type(count) is not int or not len(kinds) <= count <= 40:
        raise LearningError('题量不足以覆盖所选题型。')
    weights = {'single': 6, 'multiple': 3, 'interview': 1}
    total = sum(weights[kind] for kind in kinds)
    raw = {kind: count * weights[kind] / total for kind in kinds}
    amounts = {kind: max(1, int(raw[kind])) for kind in kinds}
    while sum(amounts.values()) < count:
        amounts[max(kinds, key=lambda kind: raw[kind] - amounts[kind])] += 1
    while sum(amounts.values()) > count:
        amounts[max((kind for kind in kinds if amounts[kind] > 1), key=lambda kind: amounts[kind] - raw[kind])] -= 1
    return [kind for kind in kinds for _ in range(amounts[kind])]


def sample_questions(questions, rng):
    manual = [q['id'] for q in questions if q.get('origin', 'ai') == 'manual']
    needed = max(0, 5 - len(manual))
    questions = [q for q in questions if q.get('origin', 'ai') == 'ai']
    if len(questions) < needed:
        raise LearningError('人工题不足5道时，需要足够的AI题补齐；请补充题目后发布。')
    if not needed:
        return manual
    kinds = [kind for kind in QUESTION_TYPES if any(q['type'] == kind for q in questions)]
    if needed < len(kinds):
        return manual + [q['id'] for q in rng.sample(questions, needed)]
    mix = question_mix(needed, kinds)
    chosen = list(manual)
    for kind in kinds:
        pool = [q['id'] for q in questions if q['type'] == kind]
        chosen.extend(rng.sample(pool, min(len(pool), mix.count(kind))))
    chosen.extend(rng.sample([q['id'] for q in questions if q['id'] not in chosen], max(0, 5 - len(chosen))))
    return chosen


def key(*parts):
    return hashlib.sha256(':'.join(parts).encode()).hexdigest()[:32]


def plain(value):
    return {'tag': 'plain_text', 'content': str(value)}


def normalized(value):
    return re.sub(r'\W+', '', value).lower()


def evidence_text(value):
    value = re.sub(r'\s+', ' ', unicodedata.normalize('NFKC', value)).strip()
    # Chinese line wraps are layout; keep separators between independent numbers/words.
    return re.sub(r'(?<=[\u3400-\u9fff]) +| +(?=[\u3400-\u9fff])', '', value)


def evidence_locations(batch, question):
    quote = question.get('evidence', '')
    if not quote:
        return []
    basis, result = evidence_text(quote), []
    source = batch.get('source') or {}
    for item in source.get('sources') or [source]:
        text = item.get('text', '')
        if basis not in evidence_text(text):
            continue
        references = [ref for ref in item.get('references', [])
                      if basis in evidence_text(text[ref.get('start', 0):ref.get('end', len(text))])]
        position = text.find(quote)
        result.append({'title': item.get('title') or batch['title'], 'url': item.get('url', ''),
                       'references': references, 'excerpt': text[max(0, position - 100):position + len(quote) + 100] if position >= 0 else quote})
    for image in source.get('images', []):
        if image['id'] in question.get('image_ids', []) and basis in evidence_text(image.get('text', '')):
            result.append({'title': image['label'], 'image_id': image['id'], 'excerpt': image['text'], 'references': []})
    return result


def validate_questions(questions, source, types=None, *, draft=False, images=()):
    if not isinstance(questions, list) or not (0 if draft else 5) <= len(questions) <= 100:
        raise LearningError('每批发布至少5题、合计最多100题；AI题最多40题。')
    if sum(isinstance(q, dict) and q.get('origin', 'ai') == 'ai' for q in questions) > 40:
        raise LearningError('AI题最多40题。')
    allowed = selected_types(types) if types is not None else list(QUESTION_TYPES)
    result, seen = [], set()
    image_by_id = {image['id']: image for image in images}
    source_basis = evidence_text(source)
    image_basis = {image['id']: evidence_text(image.get('text', '')) if image.get('mode') in {'ocr', 'vision'} else '' for image in images}
    for index, q in enumerate(questions):
        if not isinstance(q, dict):
            raise LearningError('题目格式无效。')
        origin = q.get('origin', 'ai')
        if not isinstance(origin, str) or origin not in {'ai', 'manual'}:
            raise LearningError('题目来源无效。')
        permitted_types = allowed if origin == 'ai' else list(QUESTION_TYPES)
        incomplete_draft = draft and origin == 'manual'
        values = {}
        for name, limit in (('stem', 500), ('explanation', 600)):
            value = q.get(name)
            if not isinstance(value, str) or not value.strip() and not incomplete_draft or len(value) > limit:
                raise LearningError(f'第{index + 1}题的题干、解析或纪要依据缺失/过长。')
            values[name] = value.strip()
        evidence = q.get('evidence', '')
        if not isinstance(evidence, str) or len(evidence) > 800:
            raise LearningError('原文依据格式无效或超过800字。')
        values['evidence'] = evidence.strip()
        if 'origin' in q:
            values['origin'] = origin
        ids = q.get('image_ids', [])
        if (not isinstance(ids, list) or len(ids) > 3 or any(not isinstance(i, str) or i not in image_by_id for i in ids)
                or len(set(ids)) != len(ids)):
            raise LearningError(f'第{index + 1}题的图片必须从本批材料选择，最多3张且不能重复。')
        basis = evidence_text(values['evidence'])
        if origin == 'ai' and (len(basis) < 8 or not any(basis in text for text in [source_basis, *(image_basis[i] for i in ids)])):
            raise LearningError(f'第{index + 1}题的依据无法在纪要原文中核对，请修订后再发布。')
        if ids:
            confirmed = q.get('image_confirmed', False)
            if type(confirmed) is not bool:
                raise LearningError('题图核对状态无效。')
            if not draft and any(image_by_id[i].get('mode') == 'vision' for i in ids) and not confirmed:
                raise LearningError(f'第{index + 1}题包含图片识别内容，请对照原图核对题目与答案后再发布。')
            values.update(image_ids=ids, image_confirmed=confirmed)
        identity = normalized(values['stem'])
        if not identity and not incomplete_draft or identity and identity in seen:
            raise LearningError('存在重复题干，请修订；不通过改编号凑足题量。')
        if identity:
            seen.add(identity)
        options = q.get('options')
        correct = q.get('correct')
        if q.get('type') == 'interview':
            reference = q.get('reference_answer')
            if not isinstance(reference, str) or not reference.strip() and not incomplete_draft or len(reference) > 1000 or options not in (None, []) or correct not in (None, []):
                raise LearningError(f'第{index + 1}道问答题需填写参考答案（最多1000字），不能有选择题选项。')
            if 'interview' not in permitted_types:
                raise LearningError('生成题型不符合本批选择，请重新生成或核对题目。')
            result.append({**values, 'id': f'q{index + 1}', 'type': 'interview', 'options': [], 'correct': [], 'reference_answer': reference.strip()})
            continue
        if (not isinstance(options, list) or not 2 <= len(options) <= 6
                or any(not isinstance(o, str) or not o.strip() and not incomplete_draft or len(o) > 300 for o in options)
                or len({normalized(o) for o in options if o.strip()}) != len([o for o in options if o.strip()])):
            raise LearningError(f'第{index + 1}题须有2至6个不重复的非空选项。')
        if (not isinstance(correct, list) or not correct and not incomplete_draft or len(set(str(v) for v in correct)) != len(correct)
                or any(type(v) is not int or not 0 <= v < len(options) for v in correct)):
            raise LearningError(f'第{index + 1}题的正确选项无效。')
        kind = q.get('type', 'single') if incomplete_draft and not correct else 'single' if len(correct) == 1 else 'multiple'
        if kind not in permitted_types or q.get('type', kind) != kind:
            raise LearningError(f'第{index + 1}题的题型与正确选项数量或所选题型不一致。')
        result.append({**values, 'id': f'q{index + 1}', 'options': [o.strip() for o in options], 'correct': sorted(correct), 'type': kind})
    ai_types = {q['type'] for q in result if q.get('origin', 'ai') == 'ai'}
    if not draft and types is not None and ai_types and ai_types != set(allowed):
        raise LearningError('题目未包含所有已选题型，请补充题目后再发布。')
    return result


def read_source(portal, url):
    from openclaw_service.assistant.lighthouse_ai import AssistantError
    from openclaw_service.assistant.lighthouse_documents import document_reader, document_url
    if isinstance(url, str) and '/wiki/' in url:
        from .sharing_sources import resolve_document
        url, _original = resolve_document(portal, url)
    target = document_url(url)
    if not target:
        raise LearningError('请输入 vnet.feishu.cn/docx/ 开头的纪要链接。')
    base = 'https://open.feishu.cn/open-apis/docx/v1/documents/' + target.rsplit('/', 1)[1]
    request = document_reader(portal, '分享会练习读取纪要')

    def get(suffix='', params=None):
        try:
            value = request(base + suffix, params)
        except AssistantError as exc:
            raise LearningError(str(exc), exc.status) from None
        if not isinstance(value, dict) or value.get('code') != 0 or not isinstance(value.get('data'), dict):
            raise LearningError('飞书纪要读取失败，请核对应用的文档读取权限及此文档的访问授权。', 502)
        return value['data']

    document = get().get('document') or {}
    revision = document.get('revision_id')
    raw = get('/raw_content', {'lang': 1}).get('content')
    if not isinstance(raw, str) or not 200 <= len(raw) <= 24000 or not revision:
        raise LearningError('纪要须包含200至24000字有效正文；过长请拆分为独立分享会，不截断出题。')
    attendance = re.search(r'^\s*参会人[：:]([^\n]+)', raw, re.M)
    if not attendance:
        raise LearningError('纪要中未找到“参会人”名单，未猜测接收人。')
    names = [name.strip() for name in attendance[1].split('@') if name.strip()]

    source_blocks = []

    def mentions(id_type):
        found, cursor, seen, active = [], '', set(), False
        for _ in range(40):
            data = get('/blocks', {'page_size': 500, 'document_revision_id': revision, 'user_id_type': id_type,
                                  **({'page_token': cursor} if cursor else {})})
            if not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
                raise LearningError('纪要块分页不完整，未使用部分参会人名单。', 502)
            if id_type == 'open_id':
                source_blocks.extend(data['items'])
            for block in data['items']:
                for value in block.values():
                    if not isinstance(value, dict) or not isinstance(value.get('elements'), list):
                        continue
                    elements = value['elements']
                    text = ''.join(e.get('text_run', {}).get('content', '') for e in elements)
                    users = [e['mention_user'].get('user_id', '') for e in elements if isinstance(e.get('mention_user'), dict)]
                    if re.search(r'参会人[：:]', text):
                        active = True
                    elif re.sub(r'[\s,，、;；]+', '', text):
                        active = False
                    if active:
                        found.extend(users)
            if not data['has_more']:
                return found
            cursor = data.get('page_token')
            if not isinstance(cursor, str) or not cursor or cursor in seen:
                break
            seen.add(cursor)
        raise LearningError('纪要分页未完成，未使用部分参会人名单。', 502)

    open_ids, union_ids = mentions('open_id'), mentions('union_id')
    if get().get('document', {}).get('revision_id') != revision:
        raise LearningError('读取期间纪要已变化，请重新导入。', 409)
    if not names or len(names) != len(open_ids) or len(open_ids) != len(union_ids):
        raise LearningError('参会人姓名与飞书人员引用无法完整对应；请使用真实@人员的纪要，不按同名猜测。')
    people = {}
    for name, oid, uid in zip(names, open_ids, union_ids):
        if not re.fullmatch(r'ou_[A-Za-z0-9]+', oid) or not re.fullmatch(r'on_[A-Za-z0-9]+', uid):
            raise LearningError('参会人缺少可核验的飞书标识，请核对应用权限。')
        if oid in people and people[oid]['union_id'] != uid:
            raise LearningError('纪要参会人身份有冲突，未发送。')
        people[oid] = {'open_id': oid, 'union_id': uid, 'name': name}
    if len({p['union_id'] for p in people.values()}) != len(people) or len(people) > 500:
        raise LearningError('参会名单身份有冲突或超过500人，未发送。')
    from .sharing_images import document_images
    images = document_images(source_blocks, target, revision, document.get('title') or '分享会纪要')
    return {'url': target, 'title': document.get('title') or '分享会纪要', 'revision': revision,
            'text': raw, 'participants': list(people.values()), 'read_at': stamp(), **({'images': images} if images else {})}


def assigned_participants(batch):
    excluded = set(batch.get('excluded_open_ids') or [])
    return [person for person in batch.get('participants', []) if person['open_id'] not in excluded]


class SharingExercises:
    def __init__(self, learning):
        self.learning = learning
        with learning.transaction() as conn:
            conn.execute("CREATE INDEX IF NOT EXISTS sharing_due ON documents(json_extract(payload,'$.status'),json_extract(payload,'$.due_at')) WHERE kind='sharing_delivery'")
            conn.execute("CREATE INDEX IF NOT EXISTS sharing_batch_status ON documents(json_extract(payload,'$.status')) WHERE kind='sharing_batch'")
            conn.execute("CREATE INDEX IF NOT EXISTS sharing_attempt_batch ON documents(json_extract(payload,'$.batch_id'),json_extract(payload,'$.open_id')) WHERE kind='sharing_attempt'")
            conn.execute("CREATE INDEX IF NOT EXISTS sharing_delivery_batch ON documents(json_extract(payload,'$.batch_id')) WHERE kind='sharing_delivery'")

    def batch(self, identity, conn=None):
        value = self.learning._get('sharing_batch', identity, conn)
        if not value or value.get('status') == 'deleted':
            raise LearningError('分享会批次不存在。', 404)
        if value.get('status') == 'failed' and not value.get('participants'):
            value['participants'] = copy.deepcopy(value.get('requested_participants') or [])
        return value

    def changed(self, *dates):
        callback = getattr(self.learning, 'completion_changed', None)
        if callable(callback):
            try:
                callback(*dict.fromkeys(date for date in dates if date))
            except Exception:
                logging.warning('Sharing completion projection deferred after committed transaction')

    def changed_batch(self, batch, *dates):
        published = ''
        try:
            value = dt.datetime.fromisoformat(str(batch.get('published_at') or '').replace('Z', '+00:00'))
            published = (value if value.tzinfo else value.replace(tzinfo=TZ)).astimezone(TZ).date().isoformat()
        except (TypeError, ValueError):
            pass
        self.changed(batch.get('business_date'), published, batch.get('date'), *dates)

    def people(self):
        rows, seen = [], set()
        for person in self.learning._all('person'):
            ids = person.get('login_ids') or []
            if not person.get('active') or len(ids) != 1 or not re.fullmatch(r'ou_[A-Za-z0-9]+', str(ids[0])):
                continue
            rows.append({'id': person['id'], 'person_id': person['id'], 'name': person.get('name', ''),
                         'open_id': ids[0], 'union_id': '',
                         'scope': person.get('scopes', [''])[0] if len(person.get('scopes', [])) == 1 else ''})
            if ids[0] in seen:
                raise LearningError('人员目录存在重复登录身份，请先核对目录。', 409)
            seen.add(ids[0])
        return rows

    def delete(self, identity, version, actor):
        self.learning._admin(actor)
        with self.learning.transaction() as conn:
            batch = self.learning._get('sharing_batch', identity, conn)
            if not batch:
                raise LearningError('分享会批次不存在。', 404)
            if batch.get('status') == 'deleted':
                return {'id': identity, 'deleted': True}
            if version != batch['version']:
                raise LearningError('批次已变化，请刷新后再删除。', 409)
            # Keep the tombstone and delivery receipts for restore/idempotency; never delete bank records.
            batch.update(status='deleted', deleted_at=stamp(), deleted_by=actor['id'], version=batch['version'] + 1, progress='')
            self.learning._put('sharing_batch', identity, batch, conn)
            for row in conn.execute("SELECT key,payload FROM documents WHERE kind='sharing_delivery' AND json_extract(payload,'$.batch_id')=?", (identity,)).fetchall():
                job = json.loads(row[1])
                if job['status'] in {'pending', 'failed'}:
                    job.update(status='skipped', error='批次已删除，停止后续任务；已上传题目保留。')
                    self.learning._put('sharing_delivery', row[0], job, conn)
        self.changed_batch(batch, now().date().isoformat())
        return {'id': identity, 'deleted': True}

    def create(self, url, operation_id, actor, question_types=None, *, material_urls=None, material_ids=None,
               participant_ids=None, auto_generate=True, title='', import_questions=False, initial_questions=None):
        self.learning._admin(actor)
        types = selected_types(question_types if question_types is not None else ['single'])
        from .sharing_sources import source_url
        target = source_url(url) if url is not None and url != '' else ''
        links, files, selected = [value if value is not None else [] for value in (material_urls, material_ids, participant_ids)]
        if (type(auto_generate) is not bool or type(import_questions) is not bool or not isinstance(title, str) or len(title) > 200
                or any(not isinstance(values, list) or len(values) != len(set(str(v) for v in values)) for values in (links, files, selected))
                or len(links) + len(files) > 10 or len(selected) > 500
                or any(not isinstance(value, str) for values in (links, files, selected) for value in values)):
            raise LearningError('材料或名单参数无效。')
        links = [source_url(link) for link in links]
        if import_questions and (not auto_generate or not (links or files or target)):
            raise LearningError('文件导入需要提供含有题目的材料。')
        initial = []
        if initial_questions is not None:
            if auto_generate or not isinstance(initial_questions, list) or any(
                    not isinstance(q, dict) or q.get('origin') != 'manual' for q in initial_questions):
                raise LearningError('初始题目仅用于关闭AI生成后的人工出题草稿。')
            initial = validate_questions(initial_questions, '', draft=True)
        if (auto_generate and not (target or links or files)) or not isinstance(operation_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{16,80}', operation_id):
            raise LearningError('请选择出题资料，并重新提交。')
        if not target and not selected:
            raise LearningError('仅材料或人工录题时必须显式选择答题名单。')
        directory = {row['id']: row for row in self.people()} if selected else {}
        if any(pid not in directory for pid in selected):
            raise LearningError('所选人员不在当前有效名单中。', 403)
        request_parts = [target, types, links, files, selected, auto_generate, title]
        if import_questions or initial:
            request_parts += [import_questions, initial]
        fingerprint = hashlib.sha256(json.dumps(request_parts, ensure_ascii=False).encode()).hexdigest()
        identity = key(actor['id'], operation_id)
        with self.learning.transaction() as conn:
            prior = self.learning._get('sharing_batch', identity, conn)
            if prior:
                if (prior['url'] != target or prior.get('question_types', ['single']) != types
                        or prior.get('request_fingerprint', fingerprint) != fingerprint
                        or not prior.get('request_fingerprint') and (links or files or selected or not auto_generate or title)):
                    raise LearningError('重复提交的链接已变化，请刷新。', 409)
                return {'id': identity}
            active = conn.execute("SELECT count(*) FROM documents WHERE kind='sharing_batch' AND json_extract(payload,'$.status') IN ('queued','generating')").fetchone()[0]
            if active >= 3 and (auto_generate or not selected):
                raise LearningError('已有纪要正在生成，请等待完成后再提交。', 429)
            batch = {'id': identity, 'url': target, 'title': '正在读取纪要', 'status': 'queued', 'version': 1,
                     'owner': actor['id'], 'created_at': stamp(), 'date': now().date().isoformat(), 'questions': initial, 'participants': [], 'question_types': types}
            batch.update(material_urls=links, material_ids=files, requested_participants=[directory[pid] for pid in selected],
                         auto_generate=auto_generate, requested_title=title.strip(), request_fingerprint=fingerprint,
                         import_questions=import_questions)
            batch['participants'] = copy.deepcopy(batch['requested_participants'])
            if not auto_generate and selected and not (target or links or files):
                batch.update(status='draft', title=title.strip() or '手动录题', questions=initial, source={'text': '', 'sources': []})
            self.learning._put('sharing_batch', identity, batch, conn)
            self.learning._put('local', 'self_practice_sync', {'enabled': True}, conn, False)
        return {'id': identity}

    def attempts(self, identity, conn=None, *, summary=False):
        own = conn is None
        conn = conn or self.learning._connect()
        try:
            projection = "json_remove(payload,'$.answers','$.draft_answers','$.saved_operations','$.ticket'),(SELECT count(*) FROM json_each(documents.payload,'$.draft_answers'))" if summary else 'payload'
            rows = conn.execute("SELECT " + projection + " FROM documents WHERE kind='sharing_attempt' AND json_extract(payload,'$.batch_id')=? ORDER BY key", (identity,))
            return [{**json.loads(row[0]), **({'saved_count': row[1]} if summary else {})} for row in rows]
        finally:
            if own: conn.close()

    def permitted(self, actor, person):
        return bool(actor.get('is_admin') or person.get('open_id') == actor['id']
                    or actor.get('shared_account') and actor.get('scope') and actor['scope'] == person.get('scope'))

    def detail(self, identity, actor, *, progress_only=False, compact=False):
        batch = self.batch(identity)
        participants = [p for p in batch['participants'] if self.permitted(actor, p)]
        if not actor.get('is_admin') and (not participants or batch['status'] not in {'published', 'closed'}):
            raise LearningError('无权查看此分享会练习。', 403)
        all_attempts = self.attempts(identity, summary=progress_only or compact)
        attempts = [p for p in all_attempts if self.permitted(actor, p)]
        values = {p['open_id']: p for p in attempts}
        with closing(self.learning._connect()) as conn:
            delivery = [json.loads(row[0]) for row in conn.execute("""SELECT payload FROM (
                SELECT payload,ROW_NUMBER() OVER (PARTITION BY json_extract(payload,'$.attempt_id'),json_extract(payload,'$.kind')
                    ORDER BY COALESCE(json_extract(payload,'$.date'),'') DESC,updated DESC,key DESC) AS latest
                FROM documents WHERE kind='sharing_delivery' AND json_extract(payload,'$.batch_id')=?
                    AND json_extract(payload,'$.kind') IN ('paper','reminder','result')) WHERE latest=1""", (identity,))]
        # Original task receipts repair older mixed status fields without resending anything.
        by_attempt = {p['id']: p for p in attempts}
        for job in delivery:
            attempt = by_attempt.get(job.get('attempt_id'))
            if attempt:
                prefix = {'paper': 'delivery', 'reminder': 'reminder_delivery', 'result': 'result_delivery'}[job['kind']]
                attempt.update({prefix + '_status': job['status'], prefix + '_error': job.get('error', ''), prefix + '_at': job.get('sent_at', '')})
        question_by_id = {q['id']: q for q in batch['questions']}
        excluded = set(batch.get('excluded_open_ids') or [])
        participants = [{k: p.get(k, '') for k in ('name', 'person_id', 'scope', 'open_id')} | {
            'exempted': p['open_id'] in excluded,
            'submitted_at': values.get(p['open_id'], {}).get('submitted_at', ''),
            'correct': values.get(p['open_id'], {}).get('correct'),
            'choice_count': sum(question_by_id.get(qid, {}).get('type') in {'single', 'multiple'} for qid in values.get(p['open_id'], {}).get('question_ids', [])),
            'delivery_error': values.get(p['open_id'], {}).get('delivery_error', ''),
            'delivery_status': values.get(p['open_id'], {}).get('delivery_status', 'pending'),
            **{prefix + suffix: values.get(p['open_id'], {}).get(prefix + suffix, '')
               for prefix in ('delivery', 'reminder_delivery', 'result_delivery') for suffix in ('_error', '_at', '_status')
               if prefix != 'delivery' or suffix != '_status'},
            'question_count': len(values.get(p['open_id'], {}).get('question_ids', [])),
            'saved_count': values.get(p['open_id'], {}).get('saved_count', len(values.get(p['open_id'], {}).get('draft_answers', {}))),
        } for p in participants]
        result = {k: batch.get(k, '') for k in ('id', 'title', 'url', 'status', 'version', 'created_at', 'published_at', 'error', 'progress', 'generation_note')}
        result.update(participants=participants, question_count=len(batch['questions']),
                      completed=sum(bool(p['submitted_at']) for p in participants if not p['exempted']),
                      total=sum(not p['exempted'] for p in participants), attendee_total=len(participants))
        result['question_types'] = batch.get('question_types') or [kind for kind in QUESTION_TYPES if any(q['type'] == kind for q in batch['questions'])]
        result.update(manual_count=sum(q.get('origin', 'ai') == 'manual' for q in batch['questions']),
                      ai_count=sum(q.get('origin', 'ai') == 'ai' for q in batch['questions']))
        if (batch['status'] in {'published', 'closed'} and len(all_attempts) != len(assigned_participants(batch))
                or batch['status'] == 'draft' and all_attempts):
            result['error'] = '历史分配恢复不完整，已暂停此批提醒与汇总；请同步恢复，不能重新抽题代替。'
        mine = next((p for p in attempts if p['open_id'] == actor['id']), None)
        if mine and (progress_only or compact) and not actor.get('shared_account'):
            mine = self.learning._get('sharing_attempt', mine['id'])
        if mine and not actor.get('shared_account'):
            result['paper'] = {k: mine.get(k, '' if k == 'submitted_at' else {}) for k in ('id', 'open_id', 'question_ids', 'draft_answers', 'submitted_at')}
            result['paper'].update(progress_version=mine.get('progress_version', 0), question_count=len(mine['question_ids']), page_size=5)
        if actor.get('is_admin') and not progress_only:
            result['questions'] = copy.deepcopy(batch['questions'])
            result['source_text'] = (batch.get('source') or {}).get('text', '')
            result['sources'] = [{k: v for k, v in source.items() if k not in {'text', 'images'}} for source in (batch.get('source') or {}).get('sources', [])]
            from .sharing_images import public_images
            result['source_images'] = public_images(batch, editor=True)
        if actor.get('is_admin'):
            result['material_status'] = [{'id': key, **{k: value.get(k, '') for k in ('title', 'status', 'error')}}
                                         for key, value in (batch.get('material_state') or {}).items()]
        if not progress_only:
            own_questions = [copy.deepcopy(q) for q in batch['questions'] if mine and q['id'] in mine['question_ids']]
            for q in own_questions:
                if not mine.get('submitted_at'):
                    for name in ('correct', 'reference_answer', 'explanation', 'evidence'): q.pop(name, None)
                else:
                    q['selected'] = mine['answers'][q['id']]
            result['my_questions' if actor.get('is_admin') else 'questions'] = own_questions
        from .sharing_images import public_images
        for question in result.get('questions', []) + result.get('my_questions', []):
            if question.get('image_ids'):
                question['images'] = public_images(batch, question['image_ids'])
        if not progress_only:
            result['evidence_locations'] = {q['id']: evidence_locations(batch, q) for q in result.get('questions', []) if q.get('evidence')}
        if actor.get('is_admin'):
            if not progress_only:
                fields = ('name', 'open_id', 'correct', 'submitted_at') if compact else ('name', 'open_id', 'question_ids', 'answers', 'correct', 'submitted_at')
                result['results'] = [{k: p.get(k) for k in fields} for p in attempts]
            with closing(self.learning._connect()) as conn:
                notices = [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind='sharing_delivery' AND json_extract(payload,'$.batch_id')=? AND json_extract(payload,'$.kind')='weekly' ORDER BY updated DESC LIMIT 10", (identity,))]
            result['weekly_reports'] = [{k: row.get(k) for k in ('date', 'end', 'part', 'parts', 'status', 'error', 'sent_at')} for row in notices]
            with closing(self.learning._connect()) as conn:
                uploads = [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind='sharing_delivery' AND json_extract(payload,'$.batch_id')=? AND json_extract(payload,'$.kind')='bank'", (identity,))]
            result['bank_sync'] = {'total': len(uploads), 'synced': sum(row['status'] == 'sent' for row in uploads),
                                  'failed': sum(row['status'] == 'failed' for row in uploads),
                                  'error': next((row.get('error', '') for row in uploads if row.get('error') and row['status'] != 'sent'), '')}
        return result

    def answer_detail(self, identity, actor, open_id):
        self.learning._admin(actor)
        self.batch(identity)
        if not isinstance(open_id, str) or not re.fullmatch(r'ou_[A-Za-z0-9]+', open_id):
            raise LearningError('答卷人员标识无效。')
        attempt = self.learning._get('sharing_attempt', key(identity, open_id))
        if not attempt or attempt['batch_id'] != identity or attempt['open_id'] != open_id:
            raise LearningError('此批次没有该人员答卷。', 404)
        return {k: attempt.get(k) for k in ('name', 'open_id', 'question_ids', 'answers', 'correct', 'submitted_at')}

    def listing(self, actor, page=1):
        try: page = max(1, min(10000, int(page)))
        except (TypeError, ValueError): raise LearningError('页码无效。') from None
        where, args = "kind='sharing_batch' AND COALESCE(json_extract(payload,'$.status'),'')!='deleted'", []
        if not actor.get('is_admin'):
            where += " AND json_extract(payload,'$.status') IN ('published','closed') AND EXISTS (SELECT 1 FROM json_each(documents.payload,'$.participants') p WHERE "
            if actor.get('shared_account') and actor.get('scope'):
                where += "json_extract(p.value,'$.scope')=?)"; args.append(actor['scope'])
            else:
                where += "json_extract(p.value,'$.open_id')=?)"; args.append(actor['id'])
        with closing(self.learning._connect()) as conn:
            total = conn.execute('SELECT count(*) FROM documents WHERE ' + where, args).fetchone()[0]
            rows = conn.execute("SELECT json_object('id',key,'title',json_extract(payload,'$.title'),'status',json_extract(payload,'$.status'),'version',json_extract(payload,'$.version'),'created_at',json_extract(payload,'$.created_at'),'question_count',json_array_length(payload,'$.questions'),'participants',json_extract(payload,'$.participants'),'excluded',json_extract(payload,'$.excluded_open_ids')) FROM documents WHERE " + where + ' ORDER BY updated DESC LIMIT 15 OFFSET ?', [*args, (page - 1) * 15]).fetchall()
            items = [json.loads(row[0]) for row in rows]
            ids = [item['id'] for item in items]
            submitted = {(row[0], row[1]) for row in conn.execute("SELECT json_extract(payload,'$.batch_id'),json_extract(payload,'$.open_id') FROM documents WHERE kind='sharing_attempt' AND COALESCE(json_extract(payload,'$.submitted_at'),'')<>'' AND json_extract(payload,'$.batch_id') IN (" + ','.join('?' for _ in ids) + ')', ids)} if ids else set()
        for item in items:
            excluded = set(item.pop('excluded') or [])
            people = [p for p in item.pop('participants') or [] if self.permitted(actor, p) and p['open_id'] not in excluded]
            item.update(total=len(people), completed=sum((item['id'], p['open_id']) in submitted for p in people))
        return {'items': items, 'total': total, 'page': page}

    def image(self, identity, image_id, actor):
        detail = self.detail(identity, actor)
        visible = detail.get('source_images', []) if actor.get('is_admin') else [
            image for question in detail.get('questions', []) for image in question.get('images', [])]
        if not any(image['id'] == image_id for image in visible):
            raise LearningError('无权查看此题图，或题图不属于当前题单。', 403)
        return next(image for image in self.batch(identity)['source']['images'] if image['id'] == image_id)

    def edit(self, identity, payload, actor):
        self.learning._admin(actor)
        with self.learning.transaction() as conn:
            batch = self.batch(identity, conn)
            if payload.get('version') != batch['version']:
                raise LearningError('批次已变化，请刷新后核对。', 409)
            if batch['status'] not in {'draft', 'failed'}:
                raise LearningError('只有待核对草稿可修改，已发布题目保持原快照。', 409)
            if 'participant_ids' in payload:
                ids = payload['participant_ids']
                if not isinstance(ids, list) or len(ids) > 500 or any(not isinstance(pid, str) for pid in ids) or len(set(ids)) != len(ids):
                    raise LearningError('补充人员名单无效。')
                directory = {person['id']: person for person in self.people()}
                if any(pid not in directory for pid in ids):
                    raise LearningError('补充人员已不在有效目录，请重新选择。', 409)
                known_ids = {person['open_id'] for person in batch['participants']}
                batch['participants'] += [directory[pid] for pid in ids if directory[pid]['open_id'] not in known_ids]
            excluded = payload.get('excluded_open_ids', batch.get('excluded_open_ids', []))
            known = {person['open_id'] for person in batch['participants']}
            if (not isinstance(excluded, list) or any(not isinstance(oid, str) or oid not in known for oid in excluded)
                    or len(set(excluded)) != len(excluded)):
                raise LearningError('免做人员必须从本批参会名单选择，且不能重复。')
            batch['questions'] = validate_questions(payload.get('questions'), batch.get('source', {}).get('text', ''), batch.get('question_types'), draft=True, images=batch.get('source', {}).get('images', []))
            batch['excluded_open_ids'] = excluded
            if batch['status'] == 'failed':
                batch.update(status='draft', auto_generate=False, error='', progress='', generation_state={})
            batch['version'] += 1
            batch.update(edited_by=actor['id'], edited_at=stamp())
            self.learning._put('sharing_batch', identity, batch, conn)
        self.changed_batch(batch, now().date().isoformat())
        return self.detail(identity, actor)

    def enqueue(self, conn, kind, identity, **values):
        if not self.learning._get('sharing_delivery', identity, conn):
            self.learning._put('sharing_delivery', identity, {'id': identity, 'kind': kind, 'status': 'pending',
                'due_at': now().timestamp(), 'failures': 0, **values}, conn)

    def publish(self, identity, version, actor, app_id):
        self.learning._admin(actor)
        if not self.learning._restored:
            raise LearningError('学练历史数据尚未恢复完整，请先同步题库与人员，避免重复发布。', 409)
        if not app_id:
            raise LearningError('请先配置并连接飞书智能体及 card.action.trigger 回调。', 503)
        with self.learning.transaction() as conn:
            batch = self.batch(identity, conn)
            if batch['status'] == 'published': return {'id': identity, 'published': True}
            if batch['status'] != 'draft' or version != batch['version']:
                raise LearningError('仅可发布当前版本的已核对草稿。', 409)
            if self.attempts(identity, conn):
                raise LearningError('此批已有分配记录但发布状态尚未恢复完整，禁止重新抽题；请先恢复同步。', 409)
            questions = validate_questions(batch['questions'], batch['source']['text'], batch.get('question_types'), images=batch['source'].get('images', []))
            if any(q.get('image_ids') for q in questions):
                url = self.learning.portal_url()
                if not url:
                    raise LearningError('题图需要可用的学练入口，请核对门户地址后发布。', 503)
                batch['image_page_url'] = url.rstrip('/') + '/learning?tab=sharing&batch_id=' + identity
            participants = assigned_participants(batch)
            if not participants: raise LearningError('请至少保留一位参会人参加答题。')
            batch['date'] = now().date().isoformat()
            batch['business_date'] = batch['date']
            # Reserve the full image-key size before publishing any recipient's paper.
            sizing_batch = {**batch, 'questions': questions, 'app_id': app_id, 'card_images': {
                image['id']: {'image_key': 'img_' + 'x' * 500, 'app_id': app_id, 'sha256': image['sha256']}
                for image in batch['source'].get('images', [])}}
            rng = random.SystemRandom()
            for person in participants:
                aid = key(identity, person['open_id'])
                attempt = {**person, 'id': aid, 'batch_id': identity, 'date': batch['date'], 'app_id': app_id,
                           'question_ids': sample_questions(questions, rng), 'ticket': secrets.token_urlsafe(32),
                           'answers': {}, 'correct': None, 'submitted_at': '', 'delivery_status': 'pending'}
                attempt.update(draft_answers={}, progress_version=0)
                source_questions = {q['id']: q for q in questions}
                completed = {**attempt, 'correct': sum(source_questions[qid]['type'] != 'interview' for qid in attempt['question_ids']),
                             'answers': {qid: ('答' * 1000 if source_questions[qid]['type'] == 'interview' else list(range(len(source_questions[qid]['options'])))) for qid in attempt['question_ids']}}
                for page in range((len(attempt['question_ids']) + 4) // 5):
                    for card in (exercise_card(sizing_batch, attempt, page=page), exercise_card(sizing_batch, completed, result=True, page=page)):
                        if len(json.dumps(card, ensure_ascii=False).encode()) > 28000:
                            raise LearningError('题目或解析过长，超过飞书每页5题卡片大小限制；请缩短后重新发布，尚未发送。')
                self.learning._put('sharing_attempt', aid, attempt, conn)
                self.enqueue(conn, 'paper', 'paper:' + aid, batch_id=identity, attempt_id=aid)
            for question in questions:
                self.enqueue(conn, 'bank', f"bank:{identity}:{question['id']}", batch_id=identity, question_id=question['id'])
            batch.update(status='published', questions=questions, version=batch['version'] + 1,
                         published_at=stamp(), published_by=actor['id'], app_id=app_id)
            self.learning._put('sharing_batch', identity, batch, conn)
        self.changed_batch(batch, now().date().isoformat())
        return {'id': identity, 'published': True}

    def submit(self, value, fields, operator, app_id, allowed):
        aid = value.get('attempt_id')
        if not isinstance(aid, str) or not re.fullmatch(r'[a-f0-9]{32}', aid):
            raise LearningError('答题卡标识无效。', 400)
        with self.learning.transaction() as conn:
            attempt = self.learning._get('sharing_attempt', aid, conn)
            same_person = attempt and (operator.get('union_id') == attempt.get('union_id') if attempt.get('union_id') else operator.get('open_id') == attempt['open_id'])
            if (not attempt or app_id != attempt['app_id'] or not same_person
                    or not isinstance(value.get('ticket'), str) or not re.fullmatch(r'[A-Za-z0-9_-]{43}', value['ticket'])
                    or not hmac.compare_digest(attempt['ticket'], value['ticket']) or not allowed(attempt['open_id'])):
                raise LearningError('仅当前仍有学练权限的参会人本人可提交，转发卡片不能代答。', 403)
            if attempt['submitted_at']:
                batch = self.batch(attempt['batch_id'], conn)
                return {'ok': True, 'toast': '已提交，保留首次成绩；答案已安排发送。',
                        'card': exercise_card(batch, attempt, result=True, page=value.get('target_page', value.get('page', 0)))}
            batch = self.batch(attempt['batch_id'], conn)
            if batch['status'] != 'published': raise LearningError('此批次已关闭，不能继续提交。', 409)
            payload = {'page': value.get('page', 0), 'answers': fields,
                       'version': value.get('version', attempt.get('progress_version', 0)),
                       'operation_id': value.get('operation_id', 'legacy-' + key(aid, json.dumps(fields, sort_keys=True))),
                       'submit': value.get('submit', True), 'target_page': value.get('target_page', value.get('page', 0))}
            result = self._save_answers(conn, batch, attempt, payload, operator.get('union_id') or operator.get('open_id'))
            target = payload['target_page']
            result['card'] = exercise_card(batch, attempt, result=bool(attempt['submitted_at']), page=target)
        self.changed_batch(batch, attempt['date'], now().date().isoformat())
        return result

    def answer(self, identity, payload, actor, allowed):
        if actor.get('shared_account') or not allowed(actor['id']):
            raise LearningError('仅当前有权限的本人可保存答题。', 403)
        with self.learning.transaction() as conn:
            batch = self.batch(identity, conn)
            attempt = self.learning._get('sharing_attempt', key(identity, actor['id']), conn)
            if not attempt or attempt['open_id'] != actor['id']:
                raise LearningError('答卷不属于当前账号。', 403)
            if attempt['submitted_at']:
                return {'id': identity, 'submitted': True}
            if batch['status'] != 'published':
                raise LearningError('此批次已关闭，不能保存或提交。', 409)
            result = self._save_answers(conn, batch, attempt, payload, actor['id'])
        self.changed_batch(batch, attempt['date'], now().date().isoformat())
        return {**result, 'id': identity, 'submitted': bool(attempt['submitted_at'])}

    @staticmethod
    def _checked_answer(question, raw):
        if question['type'] == 'interview':
            if not isinstance(raw, str) or not raw.strip() or len(raw) > 1000:
                raise LearningError('请填写问答题答案，每题1至1000字。')
            return raw.strip()
        raw = [raw] if isinstance(raw, str) else raw
        if not isinstance(raw, list) or not raw or any(not isinstance(v, str) or v not in [str(i) for i in range(len(question['options']))] for v in raw):
            raise LearningError('答案选项无效，请重新选择。')
        selected = sorted({int(v) for v in raw})
        if question['type'] == 'single' and len(selected) != 1:
            raise LearningError('单选题只能选择一项。')
        return selected

    def _save_answers(self, conn, batch, attempt, payload, submitted_by):
        page, fields = payload.get('page', 0), payload.get('answers')
        pages = (len(attempt['question_ids']) + 4) // 5
        op = payload.get('operation_id')
        target = payload.get('target_page', page)
        if (type(page) is not int or not 0 <= page < pages or type(target) is not int or not 0 <= target < pages
                or not isinstance(op, str) or not re.fullmatch(r'[A-Za-z0-9_-]{16,80}', op)
                or type(payload.get('submit', False)) is not bool):
            raise LearningError('分页或操作编号无效。')
        expected = attempt['question_ids'][page * 5:page * 5 + 5]
        if not isinstance(fields, dict) or set(fields) - set(expected):
            raise LearningError('提交题目不属于本页答卷。')
        fingerprint = hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        operations = attempt.setdefault('saved_operations', {})
        if op in operations:
            if operations[op] != fingerprint:
                raise LearningError('同一操作编号不能保存不同答案。', 409)
            return {'ok': True, 'toast': '本页进度已保存。', 'progress_version': attempt.get('progress_version', 0)}
        if type(payload.get('version')) is not int or payload['version'] != attempt.get('progress_version', 0):
            raise LearningError('答题进度已变化，请刷新后重试；未覆盖新答案。', 409)
        questions = {q['id']: q for q in batch['questions']}
        draft = dict(attempt.get('draft_answers', {}))
        for qid, raw in fields.items():
            if raw in ('', [], None) and not payload.get('submit'):
                draft.pop(qid, None)
            else:
                draft[qid] = self._checked_answer(questions[qid], raw)
        if payload.get('submit') and set(draft) != set(attempt['question_ids']):
            raise LearningError(f"请完成全部{len(attempt['question_ids'])}道题后再提交；此前保存的进度仍保留。")
        attempt.update(draft_answers=draft, progress_version=attempt.get('progress_version', 0) + 1)
        operations[op] = fingerprint
        while len(operations) > 128:
            del operations[next(iter(operations))]
        if payload.get('submit'):
            correct = sum(q['type'] != 'interview' and draft[qid] == q['correct'] for qid, q in questions.items() if qid in attempt['question_ids'])
            attempt.update(answers=copy.deepcopy(draft), correct=correct, submitted_at=stamp(), submitted_by=submitted_by)
            self.enqueue(conn, 'result', 'result:' + attempt['id'], batch_id=batch['id'], attempt_id=attempt['id'])
        self.learning._put('sharing_attempt', attempt['id'], attempt, conn)
        return {'ok': True, 'toast': '已完成，参考答案将发送给你。' if attempt['submitted_at'] else '本页进度已保存，尚未完成整卷。',
                'progress_version': attempt['progress_version']}

    def schedule(self, current):
        day = current.date().isoformat()
        monday = current.date() - dt.timedelta(days=current.weekday())
        start, end = (monday - dt.timedelta(days=7)).isoformat(), (monday - dt.timedelta(days=1)).isoformat()
        weekly_due = current >= dt.datetime.combine(monday, dt.time.min, TZ)
        with self.learning.transaction() as conn:
            batches = [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind='sharing_batch' AND json_extract(payload,'$.status') IN ('published','closed')")]
            for batch in batches:
                attempts = self.attempts(batch['id'], conn)
                if len(attempts) != len(assigned_participants(batch)):
                    continue
                if batch['status'] == 'published' and current.strftime('%H:%M') >= '13:00':
                    for item in attempts:
                        if not item['submitted_at'] and (batch['published_at'][:10] < day or batch['published_at'][11:16] < '13:00'):
                            self.enqueue(conn, 'reminder', f"reminder:{day}:{item['id']}", batch_id=batch['id'], attempt_id=item['id'], date=day)
                open_at_cutoff = batch['status'] == 'published' or batch.get('closed_at', '')[:10] > end
                if (weekly_due and self.learning.settings()['weekly_report_enabled'] and batch['published_at'][:10] <= end
                        and (open_at_cutoff and any(not p['submitted_at'] or p['submitted_at'][:10] > end for p in attempts)
                             or start <= batch.get('closed_at', '')[:10] <= end
                             or any(start <= p['submitted_at'][:10] <= end for p in attempts))):
                    snapshot = []
                    by_id = {q['id']: q for q in batch['questions']}
                    for p in attempts:
                        answered = bool(p['submitted_at'] and p['submitted_at'][:10] <= end)
                        snapshot.append({'name': p['name'], 'person_id': p['person_id'], 'answered': len(p['question_ids']) if answered else 0,
                            'choices': sum(by_id[qid]['type'] != 'interview' for qid in p['question_ids']) if answered else 0,
                            'correct': p['correct'] if answered else 0,
                            'practice': 0, 'papers': 1 if answered else 0, 'scopes': [p['scope']] if p['scope'] else []})
                    for index in range(0, len(snapshot), 50):
                        part, parts = index // 50 + 1, (len(snapshot) + 49) // 50
                        self.enqueue(conn, 'weekly', f"weekly:{start}:{batch['id']}:{part}", batch_id=batch['id'], date=start, end=end,
                                     items=snapshot[index:index + 50], part=part, parts=parts)


def exercise_card(batch, attempt, *, reminder=False, result=False, page=None):
    count = len(attempt['question_ids'])
    pages = max(1, (count + 4) // 5)
    if page is None:
        missing = next((i for i, qid in enumerate(attempt['question_ids']) if qid not in attempt.get('draft_answers', {})), 0)
        page = missing // 5
    if type(page) is not int or not 0 <= page < pages:
        raise LearningError('答卷页码无效。')
    title = ('分享会练习 · 答题结果' if result else '分享会练习 · 未完成提醒' if reminder else f'分享会练习 · {count}道题')
    elements = [{'tag': 'div', 'text': plain(batch['title'])}, {'tag': 'div', 'text': plain(attempt['name'] + (' · 已完成' if result else f' · 共{count}题，已保存{len(attempt.get("draft_answers", {}))}题') + f' · 第{page + 1}/{pages}页')}]
    form = []
    questions = {q['id']: q for q in batch['questions']}
    image_hashes = {image['id']: image['sha256'] for image in batch.get('source', {}).get('images', [])}
    choices = sum(questions[qid]['type'] != 'interview' for qid in attempt['question_ids'])
    if result:
        score = f"选择题正确 {attempt['correct']}/{choices} 题 · 正确率 {attempt['correct'] * 100 / choices:g}%" if choices else '本次为问答题，不计算正确率。'
        if choices < count:
            score += f'\n问答题 {count - choices} 题已记录，请对照参考答案复习，不计入选择题正确率。'
        elements.append({'tag': 'div', 'text': plain(score)})
    for index, qid in enumerate(attempt['question_ids'][page * 5:page * 5 + 5], page * 5):
        q = questions[qid]
        heading = f"{index + 1}. {QUESTION_TYPES[q['type']]} · {q['stem']}"
        target = elements if result else form
        target.append({'tag': 'div', 'text': plain(heading)})
        inline_images = []
        for image_id in q.get('image_ids', []):
            item = batch.get('card_images', {}).get(image_id, {})
            if (item.get('app_id') == batch.get('app_id') and image_id in image_hashes
                    and item.get('sha256') == image_hashes[image_id] and isinstance(item.get('image_key'), str)
                    and re.fullmatch(r'img_[A-Za-z0-9_-]{1,500}', item['image_key'])):
                inline_images.append({'tag': 'img', 'img_key': item['image_key'], 'alt': plain(f'第{index + 1}题配图'),
                                      'scale_type': 'fit_horizontal', 'preview': True})
        target.extend(inline_images)
        if len(inline_images) < len(q.get('image_ids', [])) and batch.get('image_page_url'):
            target.append({'tag': 'div', 'text': {'tag': 'lark_md',
                'content': f"[查看第{index + 1}题图片（{len(q['image_ids'])}张）]({batch['image_page_url']})"}})
        if q['type'] == 'interview':
            if result:
                raw = attempt['answers'][qid]
                answer = raw[:200] + ('…（完整作答已保存）' if len(raw) > 200 else '')
                target.append({'tag': 'div', 'text': plain('你的作答：' + answer + '\n参考答案：' + q['reference_answer'] + '\n' + q['explanation'] + '\n' + ('专业经验人工题' if q.get('origin') == 'manual' else '原文依据：' + q['evidence']))})
            else:
                target.append({'tag': 'input', 'name': qid, 'input_type': 'multiline_text', 'rows': 3,
                               'max_length': 1000, 'required': False, 'default_value': attempt.get('draft_answers', {}).get(qid, ''), 'placeholder': plain('请输入文字答案，最多1000字')})
            continue
        if result:
            selected, correct = set(attempt['answers'][qid]), set(q['correct'])
            lines = []
            for i, text in enumerate(q['options']):
                text = re.sub(r'([\\`*_\[\]()!#])', r'\\\1', html.escape(text, quote=False))
                label = f'{chr(65 + i)}. {text}'
                if i in correct:
                    label = '<font color="green">' + label + (' 【正确·已选】' if i in selected else ' 【正确·漏选】') + '</font>'
                elif i in selected:
                    label = '<font color="red">' + label + ' 【错选】</font>'
                lines.append(label)
            target.append({'tag': 'markdown', 'content': '\n'.join(lines)})
            target.append({'tag': 'div', 'text': plain('你的选择：' + '、'.join(chr(65 + i) for i in attempt['answers'][qid])
                + '\n正确答案：' + '、'.join(chr(65 + i) for i in q['correct']) + '\n' + q['explanation'] + '\n' + ('专业经验人工题' if q.get('origin') == 'manual' else '原文依据：' + q['evidence']))})
        else:
            target.append({'tag': 'div', 'text': plain('\n'.join(f'{chr(65 + i)}. {text}' for i, text in enumerate(q['options'])))})
            target.append({'tag': 'select_static' if q['type'] == 'single' else 'multi_select_static', 'name': qid,
                'required': False, 'placeholder': plain('请选择答案'),
                **({'initial_option': str(attempt['draft_answers'][qid][0])} if q['type'] == 'single' and attempt.get('draft_answers', {}).get(qid) else {}),
                **({'initial_options': [str(i) for i in attempt['draft_answers'][qid]]} if q['type'] == 'multiple' and attempt.get('draft_answers', {}).get(qid) else {}),
                'options': [{'text': plain(chr(65 + i)), 'value': str(i)} for i in range(len(q['options']))]})
    def button(label, target, complete=False):
        value = {'kind': 'sharing_exercise', 'attempt_id': attempt['id'], 'ticket': attempt['ticket'],
                 'page': page, 'target_page': target, 'submit': complete, 'version': attempt.get('progress_version', 0),
                 'operation_id': key(attempt['id'], str(page), str(target), str(complete), str(attempt.get('progress_version', 0)))}
        return {'tag': 'button', 'name': 'page_' + str(target) + ('_submit' if complete else ''),
                **({} if result else {'form_action_type': 'submit'}), 'type': 'primary' if complete else 'default',
                'text': plain(label), 'behaviors': [{'type': 'callback', 'value': value}]}
    controls = elements if result else form
    if page:
        controls.append(button('上一页' if result else '保存并上一页', page - 1))
    if page + 1 < pages:
        controls.append(button('下一页' if result else '保存并下一页', page + 1))
    elif not result:
        controls.append(button(f'提交全部{count}题', page, True))
    if not result:
        form.append(button('保存本页进度', page))
        elements.append({'tag': 'form', 'name': 'sharing_answers', 'elements': form})
    return {'schema': '2.0', 'header': {'title': plain(title), 'template': 'green' if result else 'blue'}, 'body': {'elements': elements}}


def weekly_card(job, batch, url):
    from .learning_engagement import summary_card
    card = summary_card({**job, 'scope': ''}, url)
    card['header']['title']['content'] = '分享会练习 · 每周汇总'
    count = sum(bool(p['answered']) for p in job['items'])
    attendance = f"参会 {len(job['items'])} 人"
    if batch.get('excluded_open_ids'):
        attendance = f"参会 {len(batch['participants'])} 人 · 需答 {len(assigned_participants(batch))} 人 · 免做 {len(batch['excluded_open_ids'])} 人"
    card['elements'].insert(0, {'tag': 'div', 'text': plain(batch['title'] + f"\n{attendance} · 已答 {count} 人 · 未答 {len(job['items']) - count} 人")})
    card['elements'][2]['fields'][0]['text']['content'] = f"**分配人数**\n{len(job['items'])} 人"
    grouping = f"第{job['part']}/{job['parts']}组，以下统计仅计本组。" if job.get('parts', 1) > 1 else ''
    card['elements'][-2] = {'tag': 'note', 'elements': [plain(grouping + f"截至{job['end']}的本批累计首答成绩；未提交显示为未答，不计入平均正确率，不混入每日15题统计。") ]}
    card['elements'][-1]['actions'][0].update(url=url + '/learning?tab=sharing', text=plain('查看分享会练习'))
    return card
