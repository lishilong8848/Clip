"""Meeting exercises, immutable per-person papers and durable delivery receipts."""
import copy
import datetime as dt
import hashlib
import hmac
import json
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
    kinds = [kind for kind in QUESTION_TYPES if any(q['type'] == kind for q in questions)]
    mix = question_mix(5, kinds)
    chosen = []
    for kind in kinds:
        pool = [q['id'] for q in questions if q['type'] == kind]
        chosen.extend(rng.sample(pool, min(len(pool), mix.count(kind))))
    chosen.extend(rng.sample([q['id'] for q in questions if q['id'] not in chosen], 5 - len(chosen)))
    return chosen


def key(*parts):
    return hashlib.sha256(':'.join(parts).encode()).hexdigest()[:32]


def plain(value):
    return {'tag': 'plain_text', 'content': str(value)}


def normalized(value):
    return re.sub(r'\W+', '', value).lower()


def validate_questions(questions, source, types=None):
    if not isinstance(questions, list) or not 5 <= len(questions) <= 40:
        raise LearningError('每批须有5至40道有纪要依据的题目；内容不足时请补充纪要，不凑题。')
    allowed = selected_types(types) if types is not None else list(QUESTION_TYPES)
    result, seen = [], set()
    for index, q in enumerate(questions):
        if not isinstance(q, dict):
            raise LearningError('题目格式无效。')
        values = {}
        for name, limit in (('stem', 500), ('explanation', 600), ('evidence', 800)):
            value = q.get(name)
            if not isinstance(value, str) or not value.strip() or len(value) > limit:
                raise LearningError(f'第{index + 1}题的题干、解析或纪要依据缺失/过长。')
            values[name] = value.strip()
        if len(values['evidence']) < 8 or unicodedata.normalize('NFKC', values['evidence']) not in unicodedata.normalize('NFKC', source):
            raise LearningError(f'第{index + 1}题的依据无法在纪要原文中核对，请修订后再发布。')
        identity = normalized(values['stem'])
        if not identity or identity in seen:
            raise LearningError('存在重复题干，请修订；不通过改编号凑足题量。')
        seen.add(identity)
        options = q.get('options')
        correct = q.get('correct')
        if q.get('type') == 'interview':
            reference = q.get('reference_answer')
            if not isinstance(reference, str) or not reference.strip() or len(reference) > 1000 or options not in (None, []) or correct not in (None, []):
                raise LearningError(f'第{index + 1}道问答题需填写参考答案（最多1000字），不能有选择题选项。')
            if 'interview' not in allowed:
                raise LearningError('生成题型不符合本批选择，请重新生成或核对题目。')
            result.append({**values, 'id': f'q{index + 1}', 'type': 'interview', 'options': [], 'correct': [], 'reference_answer': reference.strip()})
            continue
        if (not isinstance(options, list) or not 2 <= len(options) <= 6
                or any(not isinstance(o, str) or not o.strip() or len(o) > 300 for o in options)
                or len({normalized(o) for o in options}) != len(options)):
            raise LearningError(f'第{index + 1}题须有2至6个不重复的非空选项。')
        if (not isinstance(correct, list) or not correct or len(set(str(v) for v in correct)) != len(correct)
                or any(type(v) is not int or not 0 <= v < len(options) for v in correct)):
            raise LearningError(f'第{index + 1}题的正确选项无效。')
        kind = 'single' if len(correct) == 1 else 'multiple'
        if kind not in allowed or q.get('type', kind) != kind:
            raise LearningError(f'第{index + 1}题的题型与正确选项数量或所选题型不一致。')
        result.append({**values, 'id': f'q{index + 1}', 'options': [o.strip() for o in options], 'correct': sorted(correct), 'type': kind})
    if types is not None and {q['type'] for q in result} != set(allowed):
        raise LearningError('题目未包含所有已选题型，请补充题目后再发布。')
    return result


def read_source(portal, url):
    from openclaw_service.assistant.lighthouse_documents import document_url
    target = document_url(url)
    if not target:
        raise LearningError('请输入 vnet.feishu.cn/docx/ 开头的纪要链接。')
    base = 'https://open.feishu.cn/open-apis/docx/v1/documents/' + target.rsplit('/', 1)[1]
    headers = portal._auth_headers()

    def get(suffix='', params=None):
        value = portal._request_payload('GET', base + suffix, context='分享会练习读取纪要', headers=headers, params=params or {})
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

    def mentions(id_type):
        found, cursor, seen, active = [], '', set(), False
        for _ in range(40):
            data = get('/blocks', {'page_size': 500, 'document_revision_id': revision, 'user_id_type': id_type,
                                  **({'page_token': cursor} if cursor else {})})
            if not isinstance(data.get('items'), list) or type(data.get('has_more')) is not bool:
                raise LearningError('纪要块分页不完整，未使用部分参会人名单。', 502)
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
    return {'url': target, 'title': document.get('title') or '分享会纪要', 'revision': revision,
            'text': raw, 'participants': list(people.values()), 'read_at': stamp()}


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
        if not value:
            raise LearningError('分享会批次不存在。', 404)
        return value

    def create(self, url, operation_id, actor, question_types=None):
        self.learning._admin(actor)
        types = selected_types(question_types if question_types is not None else ['single'])
        from openclaw_service.assistant.lighthouse_documents import document_url
        target = document_url(url)
        if not target or not isinstance(operation_id, str) or not re.fullmatch(r'[A-Za-z0-9_-]{16,80}', operation_id):
            raise LearningError('请输入有效纪要链接，并重新提交。')
        identity = key(actor['id'], operation_id)
        with self.learning.transaction() as conn:
            prior = self.learning._get('sharing_batch', identity, conn)
            if prior:
                if prior['url'] != target or prior.get('question_types', ['single']) != types:
                    raise LearningError('重复提交的链接已变化，请刷新。', 409)
                return {'id': identity}
            active = conn.execute("SELECT count(*) FROM documents WHERE kind='sharing_batch' AND json_extract(payload,'$.status') IN ('queued','generating')").fetchone()[0]
            if active >= 3:
                raise LearningError('已有纪要正在生成，请等待完成后再提交。', 429)
            batch = {'id': identity, 'url': target, 'title': '正在读取纪要', 'status': 'queued', 'version': 1,
                     'owner': actor['id'], 'created_at': stamp(), 'date': now().date().isoformat(), 'questions': [], 'participants': [], 'question_types': types}
            self.learning._put('sharing_batch', identity, batch, conn)
            self.learning._put('local', 'self_practice_sync', {'enabled': True}, conn, False)
        return {'id': identity}

    def attempts(self, identity, conn=None):
        own = conn is None
        conn = conn or self.learning._connect()
        try:
            return [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind='sharing_attempt' AND json_extract(payload,'$.batch_id')=? ORDER BY key", (identity,))]
        finally:
            if own: conn.close()

    def permitted(self, actor, person):
        return bool(actor.get('is_admin') or person.get('open_id') == actor['id']
                    or actor.get('shared_account') and actor.get('scope') and actor['scope'] == person.get('scope'))

    def detail(self, identity, actor):
        batch = self.batch(identity)
        participants = [p for p in batch['participants'] if self.permitted(actor, p)]
        if not actor.get('is_admin') and (not participants or batch['status'] not in {'published', 'closed'}):
            raise LearningError('无权查看此分享会练习。', 403)
        all_attempts = self.attempts(identity)
        attempts = [p for p in all_attempts if self.permitted(actor, p)]
        values = {p['open_id']: p for p in attempts}
        question_by_id = {q['id']: q for q in batch['questions']}
        excluded = set(batch.get('excluded_open_ids') or [])
        participants = [{k: p.get(k, '') for k in ('name', 'person_id', 'scope', 'open_id')} | {
            'exempted': p['open_id'] in excluded,
            'submitted_at': values.get(p['open_id'], {}).get('submitted_at', ''),
            'correct': values.get(p['open_id'], {}).get('correct'),
            'choice_count': sum(question_by_id.get(qid, {}).get('type') in {'single', 'multiple'} for qid in values.get(p['open_id'], {}).get('question_ids', [])),
            'delivery_error': values.get(p['open_id'], {}).get('delivery_error', ''),
            'delivery_status': values.get(p['open_id'], {}).get('delivery_status', 'pending'),
        } for p in participants]
        result = {k: batch.get(k, '') for k in ('id', 'title', 'url', 'status', 'version', 'created_at', 'published_at', 'error', 'progress')}
        result.update(participants=participants, question_count=len(batch['questions']),
                      completed=sum(bool(p['submitted_at']) for p in participants if not p['exempted']),
                      total=sum(not p['exempted'] for p in participants), attendee_total=len(participants))
        result['question_types'] = batch.get('question_types') or [kind for kind in QUESTION_TYPES if any(q['type'] == kind for q in batch['questions'])]
        if (batch['status'] in {'published', 'closed'} and len(all_attempts) != len(assigned_participants(batch))
                or batch['status'] == 'draft' and all_attempts):
            result['error'] = '历史分配恢复不完整，已暂停此批提醒与汇总；请同步恢复，不能重新抽题代替。'
        if actor.get('is_admin'):
            result['questions'] = batch['questions']
            result['source_text'] = (batch.get('source') or {}).get('text', '')
        else:
            mine = next((p for p in attempts if p['open_id'] == actor['id']), None)
            result['questions'] = [copy.deepcopy(q) for q in batch['questions'] if mine and q['id'] in mine['question_ids']]
            for q in result['questions']:
                if not mine.get('submitted_at'):
                    for name in ('correct', 'reference_answer', 'explanation', 'evidence'): q.pop(name, None)
                else:
                    q['selected'] = mine['answers'][q['id']]
        if actor.get('is_admin'):
            result['results'] = [{k: p.get(k) for k in ('name', 'open_id', 'question_ids', 'answers', 'correct', 'submitted_at')} for p in attempts]
            with closing(self.learning._connect()) as conn:
                notices = [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind='sharing_delivery' AND json_extract(payload,'$.batch_id')=? AND json_extract(payload,'$.kind')='weekly' ORDER BY updated DESC LIMIT 10", (identity,))]
            result['weekly_reports'] = [{k: row.get(k) for k in ('date', 'end', 'part', 'parts', 'status', 'error', 'sent_at')} for row in notices]
            with closing(self.learning._connect()) as conn:
                uploads = [json.loads(row[0]) for row in conn.execute("SELECT payload FROM documents WHERE kind='sharing_delivery' AND json_extract(payload,'$.batch_id')=? AND json_extract(payload,'$.kind')='bank'", (identity,))]
            result['bank_sync'] = {'total': len(uploads), 'synced': sum(row['status'] == 'sent' for row in uploads),
                                  'failed': sum(row['status'] == 'failed' for row in uploads),
                                  'error': next((row.get('error', '') for row in uploads if row.get('error') and row['status'] != 'sent'), '')}
        return result

    def listing(self, actor, page=1):
        try: page = max(1, min(10000, int(page)))
        except (TypeError, ValueError): raise LearningError('页码无效。') from None
        where, args = "kind='sharing_batch'", []
        if not actor.get('is_admin'):
            where += " AND json_extract(payload,'$.status') IN ('published','closed') AND EXISTS (SELECT 1 FROM json_each(documents.payload,'$.participants') p WHERE "
            if actor.get('shared_account') and actor.get('scope'):
                where += "json_extract(p.value,'$.scope')=?)"; args.append(actor['scope'])
            else:
                where += "json_extract(p.value,'$.open_id')=?)"; args.append(actor['id'])
        with closing(self.learning._connect()) as conn:
            total = conn.execute('SELECT count(*) FROM documents WHERE ' + where, args).fetchone()[0]
            rows = conn.execute('SELECT key FROM documents WHERE ' + where + ' ORDER BY updated DESC LIMIT 15 OFFSET ?', [*args, (page - 1) * 15]).fetchall()
        items = []
        for row in rows:
            detail = self.detail(row[0], actor)
            items.append({k: v for k, v in detail.items() if k not in {'questions', 'source_text', 'participants', 'results', 'weekly_reports'}})
        return {'items': items, 'total': total, 'page': page}

    def edit(self, identity, payload, actor):
        self.learning._admin(actor)
        with self.learning.transaction() as conn:
            batch = self.batch(identity, conn)
            if payload.get('version') != batch['version']:
                raise LearningError('批次已变化，请刷新后核对。', 409)
            if batch['status'] != 'draft':
                raise LearningError('只有待核对草稿可修改，已发布题目保持原快照。', 409)
            excluded = payload.get('excluded_open_ids', batch.get('excluded_open_ids', []))
            known = {person['open_id'] for person in batch['participants']}
            if (not isinstance(excluded, list) or any(not isinstance(oid, str) or oid not in known for oid in excluded)
                    or len(set(excluded)) != len(excluded)):
                raise LearningError('免做人员必须从本批参会名单选择，且不能重复。')
            batch['questions'] = validate_questions(payload.get('questions'), batch['source']['text'], batch.get('question_types'))
            batch['excluded_open_ids'] = excluded
            batch['version'] += 1
            batch.update(edited_by=actor['id'], edited_at=stamp())
            self.learning._put('sharing_batch', identity, batch, conn)
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
            questions = validate_questions(batch['questions'], batch['source']['text'], batch.get('question_types'))
            participants = assigned_participants(batch)
            if not participants: raise LearningError('请至少保留一位参会人参加答题。')
            rng = random.SystemRandom()
            for person in participants:
                aid = key(identity, person['open_id'])
                attempt = {**person, 'id': aid, 'batch_id': identity, 'date': batch['date'], 'app_id': app_id,
                           'question_ids': sample_questions(questions, rng), 'ticket': secrets.token_urlsafe(32),
                           'answers': {}, 'correct': None, 'submitted_at': '', 'delivery_status': 'pending'}
                source_questions = {q['id']: q for q in questions}
                completed = {**attempt, 'correct': sum(source_questions[qid]['type'] != 'interview' for qid in attempt['question_ids']),
                             'answers': {qid: ('答' * 1000 if source_questions[qid]['type'] == 'interview' else source_questions[qid]['correct']) for qid in attempt['question_ids']}}
                for card in (exercise_card({**batch, 'questions': questions}, attempt), exercise_card({**batch, 'questions': questions}, completed, result=True)):
                    if len(json.dumps(card, ensure_ascii=False).encode()) > 28000:
                        raise LearningError('题目或解析过长，超过飞书5题卡片大小限制；请缩短后重新发布，尚未发送。')
                self.learning._put('sharing_attempt', aid, attempt, conn)
                self.enqueue(conn, 'paper', 'paper:' + aid, batch_id=identity, attempt_id=aid)
            for question in questions:
                self.enqueue(conn, 'bank', f"bank:{identity}:{question['id']}", batch_id=identity, question_id=question['id'])
            batch.update(status='published', questions=questions, version=batch['version'] + 1,
                         published_at=stamp(), published_by=actor['id'], app_id=app_id)
            self.learning._put('sharing_batch', identity, batch, conn)
        return {'id': identity, 'published': True}

    def submit(self, value, fields, operator, app_id, allowed):
        aid = value.get('attempt_id')
        if not isinstance(aid, str) or not re.fullmatch(r'[a-f0-9]{32}', aid):
            raise LearningError('答题卡标识无效。', 400)
        with self.learning.transaction() as conn:
            attempt = self.learning._get('sharing_attempt', aid, conn)
            if (not attempt or app_id != attempt['app_id'] or operator.get('union_id') != attempt['union_id']
                    or not isinstance(value.get('ticket'), str) or not re.fullmatch(r'[A-Za-z0-9_-]{43}', value['ticket'])
                    or not hmac.compare_digest(attempt['ticket'], value['ticket']) or not allowed(attempt['open_id'])):
                raise LearningError('仅当前仍有学练权限的参会人本人可提交，转发卡片不能代答。', 403)
            if attempt['submitted_at']:
                return {'ok': True, 'toast': '已提交，保留首次成绩；答案已安排发送。'}
            batch = self.batch(attempt['batch_id'], conn)
            if batch['status'] != 'published': raise LearningError('此批次已关闭，不能继续提交。', 409)
            if not isinstance(fields, dict) or set(fields) != set(attempt['question_ids']):
                raise LearningError('请完成全部5道题后再提交。')
            questions = {q['id']: q for q in batch['questions']}
            answers, correct = {}, 0
            choice_count = 0
            for qid in attempt['question_ids']:
                q, raw = questions[qid], fields[qid]
                if q['type'] == 'interview':
                    if not isinstance(raw, str) or not raw.strip() or len(raw) > 1000:
                        raise LearningError('请填写问答题答案，每题1至1000字。')
                    answers[qid] = raw.strip()
                    continue
                choice_count += 1
                if isinstance(raw, str): raw = [raw]
                if not isinstance(raw, list) or not raw or any(not isinstance(v, str) or v not in [str(i) for i in range(len(q['options']))] for v in raw):
                    raise LearningError('答案选项无效，请重新选择。')
                selected = sorted({int(v) for v in raw})
                if q['type'] == 'single' and len(selected) != 1: raise LearningError('单选题只能选择一项。')
                answers[qid] = selected
                correct += int(selected == q['correct'])
            attempt.update(answers=answers, correct=correct, submitted_at=stamp(), submitted_by=operator['union_id'])
            self.learning._put('sharing_attempt', aid, attempt, conn)
            self.enqueue(conn, 'result', 'result:' + aid, batch_id=batch['id'], attempt_id=aid)
        score = f'选择题答对{correct}/{choice_count}题；' if choice_count else ''
        return {'ok': True, 'toast': f'已完成，{score}参考答案将发送给你。'}

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
                        snapshot.append({'name': p['name'], 'person_id': p['person_id'], 'answered': 5 if answered else 0,
                            'choices': sum(by_id[qid]['type'] != 'interview' for qid in p['question_ids']) if answered else 0,
                            'correct': p['correct'] if answered else 0,
                            'practice': 0, 'papers': 1 if answered else 0, 'scopes': [p['scope']] if p['scope'] else []})
                    for index in range(0, len(snapshot), 50):
                        part, parts = index // 50 + 1, (len(snapshot) + 49) // 50
                        self.enqueue(conn, 'weekly', f"weekly:{start}:{batch['id']}:{part}", batch_id=batch['id'], date=start, end=end,
                                     items=snapshot[index:index + 50], part=part, parts=parts)


def exercise_card(batch, attempt, *, reminder=False, result=False):
    title = ('分享会练习 · 答题结果' if result else '分享会练习 · 未完成提醒' if reminder else '分享会练习 · 5道题')
    elements = [{'tag': 'div', 'text': plain(batch['title'])}, {'tag': 'div', 'text': plain(attempt['name'] + (' · 已完成' if result else ' · 请完成以下5道题'))}]
    form = []
    questions = {q['id']: q for q in batch['questions']}
    choices = sum(questions[qid]['type'] != 'interview' for qid in attempt['question_ids'])
    if result:
        score = f"选择题正确 {attempt['correct']}/{choices} 题 · 正确率 {attempt['correct'] * 100 / choices:g}%" if choices else '本次为问答题，不计算正确率。'
        if choices < 5:
            score += f'\n问答题 {5 - choices} 题已记录，请对照参考答案复习，不计入选择题正确率。'
        elements.append({'tag': 'div', 'text': plain(score)})
    for index, qid in enumerate(attempt['question_ids']):
        q = questions[qid]
        heading = f"{index + 1}. {QUESTION_TYPES[q['type']]} · {q['stem']}"
        target = elements if result else form
        target.append({'tag': 'div', 'text': plain(heading)})
        if q['type'] == 'interview':
            if result:
                raw = attempt['answers'][qid]
                answer = raw[:200] + ('…（完整作答已保存）' if len(raw) > 200 else '')
                target.append({'tag': 'div', 'text': plain('你的作答：' + answer + '\n参考答案：' + q['reference_answer'] + '\n' + q['explanation'] + '\n纪要依据：' + q['evidence'])})
            else:
                target.append({'tag': 'input', 'name': qid, 'input_type': 'multiline_text', 'rows': 3,
                               'max_length': 1000, 'required': True, 'placeholder': plain('请输入文字答案，最多1000字')})
            continue
        target.append({'tag': 'div', 'text': plain('\n'.join(f'{chr(65 + i)}. {text}' for i, text in enumerate(q['options'])))})
        if result:
            target.append({'tag': 'div', 'text': plain('你的选择：' + '、'.join(chr(65 + i) for i in attempt['answers'][qid])
                + '\n正确答案：' + '、'.join(chr(65 + i) for i in q['correct']) + '\n' + q['explanation'] + '\n纪要依据：' + q['evidence'])})
        else:
            target.append({'tag': 'select_static' if q['type'] == 'single' else 'multi_select_static', 'name': qid,
                'required': True, 'placeholder': plain('请选择答案'),
                'options': [{'text': plain(chr(65 + i)), 'value': str(i)} for i in range(len(q['options']))]})
    if not result:
        form.append({'tag': 'button', 'name': 'submit', 'form_action_type': 'submit', 'type': 'primary', 'text': plain('提交5道题'),
                     'behaviors': [{'type': 'callback', 'value': {'kind': 'sharing_exercise', 'attempt_id': attempt['id'], 'ticket': attempt['ticket']}}]})
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
