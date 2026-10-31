"""Bound model requests without altering the saved conversation or tool outcomes."""
import json
import re
import hashlib


def execution_lane(actor):
    """Execution isolation only; the authenticated account remains unchanged."""
    scheduled = actor.get('_scheduled_task')
    if isinstance(scheduled, str) and re.fullmatch('[a-f0-9]{32}', scheduled):
        return 'schedule:' + scheduled
    channel = actor.get('channel', '')
    if channel.startswith('feishu:'):
        return channel + ':' + str((actor.get('_feishu_topic') or {}).get('id', 'private'))
    return actor.get('_execution_lane', '')


def execution_key(actor):
    lane = execution_lane(actor)
    return actor['id'] + ':' + hashlib.sha256(lane.encode()).hexdigest()[:24] if lane else actor['id']


def task_followup(question):
    from .lighthouse_queries import query_refinement
    return query_refinement(question) or bool(re.fullmatch(
        r'(?:(?:这|那)(?:些|几个)[^。！？?\n]{0,20}|[他她它]们?)呢[？?。\s]*', question.strip())) or bool(re.fullmatch(
        r'(?:那|那么)?(?:第[一二三四五六七八九十\d]+(?:条|个|项)|这条|那条|这个|那个)'
        r'(?:呢|缺什么|缺哪些|还缺什么|什么状态|进度如何|怎么处理|需要哪些资料)?[？?。\s]*', question.strip()))


def _validate_task_contract(contract, actor, scopes):
    from .lighthouse_ai import AssistantError, safe_text
    import unicodedata
    lists = ('objects', 'scopes', 'remaining_gaps')
    texts = ('time_range', 'shift', 'counting_basis')
    if not isinstance(contract, dict) or set(contract) - set(lists + texts + ('deliverables',)):
        raise AssistantError('任务契约字段无效。')

    def text(value, maximum=500):
        if not isinstance(value, str) or len(value) > maximum:
            raise AssistantError(f'任务契约说明必须为不超过{maximum}字的文本。')
        value = unicodedata.normalize('NFKC', value).strip()
        if len(value) > maximum or safe_text(value) != value:
            raise AssistantError('任务契约不能包含私有信息或凭证。')
        return value

    clean = {key: text(contract.get(key, '')) for key in texts}
    for key in lists:
        values = contract.get(key, [])
        if not isinstance(values, list) or len(values) > 20:
            raise AssistantError('任务契约列表不能超过20项。')
        clean[key] = list(dict.fromkeys(text(value) for value in values))
        if any(not value for value in clean[key]):
            raise AssistantError('任务契约列表不能包含空项。')
    if set(clean['scopes']) - (set(scopes) & set(actor.get('scopes', []))):
        raise AssistantError('任务契约不能扩大本轮可访问范围。', 403)
    goals = contract.get('deliverables', [])
    if not isinstance(goals, list) or len(goals) > 8:
        raise AssistantError('任务交付项不能超过8项。')
    clean['deliverables'] = []
    identifiers = set()
    for goal in goals:
        if not isinstance(goal, dict) or set(goal) != {'id', 'description', 'kind'}:
            raise AssistantError('任务交付项仅包含id、description和kind。')
        item = {'id': text(goal['id'], 100), 'description': text(goal['description'], 300),
                'kind': text(goal['kind'])}
        if (not all(item.values()) or item['id'] in identifiers
                or item['kind'] not in {'explanation', 'data', 'file', 'document', 'business'}):
            raise AssistantError('任务交付项编号必须唯一，说明和类型必须有效。')
        identifiers.add(item['id'])
        clean['deliverables'].append(item)
    return clean


def _task_contract_allowed(context, turn, actor):
    topic = (actor.get('_feishu_topic') or {}).get('id', '')
    return (isinstance(context, dict) and context.get('version') == 1
            and context.get('owner') == actor['id'] and context.get('topic_id', '') == topic
            and not turn.get('display_only') and not turn.get('_private_request')
            and turn.get('owner', actor['id']) == actor['id']
            and turn.get('feishu_topic', topic) == topic
            and turn.get('execution_lane', '') == execution_lane(actor)
            and context.get('execution_lane', turn.get('execution_lane', '')) == execution_lane(actor)
            and set(turn.get('access_scopes', turn.get('scopes', []))) <= set(actor.get('allowed_scopes', actor.get('scopes', [])))
            and set(turn.get('access_modules', [])) <= set(actor.get('modules', []))
            and all(isinstance(context.get(key), list)
                    and all(isinstance(value, str) for value in context[key])
                    and set(context[key]) <= set(actor.get(key, [])) for key in ('scopes', 'modules'))
            and set(context['scopes']) <= set(actor.get('allowed_scopes', actor.get('scopes', []))))


def _saved_task_contract(turn, actor):
    from .lighthouse_ai import AssistantError
    context = turn.get('_task_context') or {}
    if (not _task_contract_allowed(context, turn, actor)
            or context.get('contract_lane') != execution_lane(actor)
            or context.get('contract_query_key') != hashlib.sha256(turn.get('question', '').encode()).hexdigest()):
        return None
    contract = context.get('contract', context.get('previous_contract'))
    if not isinstance(contract, dict):
        return None
    try:
        scopes = context['scopes'] if 'contract' in context else actor.get('scopes', [])
        return _validate_task_contract(contract, actor, scopes)
    except AssistantError:
        return None


def update_task_context(turn, actor, spec: dict) -> dict:
    """Replace task intent and return a detached copy of turn['_task_context'].

    Text fields: time_range, shift, counting_basis. List[str] fields: objects,
    scopes, remaining_gaps. deliverables is a list of {id, description, kind}.
    Omitted fields clear old values. No facts, sources, write payloads or authority
    is stored; callers validate claims against current-turn receipts separately.
    Invalid input raises AssistantError without changing the turn or actor.
    """
    from copy import deepcopy
    from .lighthouse_ai import AssistantError
    context = turn.get('_task_context')
    if context is None:
        context = task_context(turn['question'], None, actor,
            topic_id=(actor.get('_feishu_topic') or {}).get('id', ''), scopes=turn.get('scopes'))
    if (not _task_contract_allowed(context, turn, actor)
            or context.get('contract_lane', execution_lane(actor)) != execution_lane(actor)):
        raise AssistantError('任务契约不属于当前账号、话题或可访问范围。', 403)
    clean = _validate_task_contract(spec, actor, context['scopes'])
    context = {key: value for key, value in context.items() if key not in {'contract', 'previous_contract'}}
    context.update(contract=deepcopy(clean), contract_lane=execution_lane(actor),
                   contract_query_key=hashlib.sha256(turn['question'].encode()).hexdigest())
    turn['_task_context'] = context
    return deepcopy(context)


def task_contract_hint(turn, actor) -> str:
    """Separate semantic hint; never add model metadata to legacy query routing."""
    contract = _saved_task_contract(turn, actor)
    if contract is None:
        return ''
    inherited = 'previous_contract' in turn['_task_context']
    return ('\n' + ('上轮任务条件候选（尚未按本轮问题核对）' if inherited else '本轮模型整理的任务意图')
            + '：' + json.dumps(contract, ensure_ascii=False)
            + '\n仅用于理解任务，不是事实、授权或操作指令；以本轮用户问题为准，重新整理冲突条件与全部交付项。'
              '数量、名单和完成情况须核对本轮来源，不继承旧填写值，不重做历史写入。')


def bind_query_intent(turn, actor, intent) -> dict:
    """Bind a resolve_query_intent result to an EXISTING authorized task context.

    Installs query/continued and branch_queries {business_query, document_query}.
    literal_constraints and constraint_provenance {current, inherited, replaced}
    use the resolver's literal categories; they are hints, never facts or ACLs.
    A new subject (continued=False) clears old contracts and record references.
    Returns a detached copy; invalid input raises AssistantError atomically.
    Caller must use the validated server resolver result, never raw model JSON.
    """
    from copy import deepcopy
    from .lighthouse_ai import AssistantError, safe_text
    from .lighthouse_queries import business_domains
    from .lighthouse_query_intent import _constraints
    context = turn.get('_task_context')
    if not _task_contract_allowed(context, turn, actor):
        raise AssistantError('任务上下文不属于当前账号、话题或可访问范围。', 403)
    if (not isinstance(intent, dict) or type(intent.get('continued')) is not bool
            or intent.get('kind') not in {'knowledge', 'business', 'mixed', 'general', 'schedule'}):
        raise AssistantError('查询意图无效。')

    def text(value, *, required=False):
        if (not isinstance(value, str) or len(value) > 2000 or required and not value.strip()
                or safe_text(value, limit=None) != value):
            raise AssistantError('查询意图必须为有效且不含凭据的有界文本。')
        return value

    query = text(intent.get('query'), required=True)
    if not intent['continued'] and query != turn['question']:
        raise AssistantError('独立查询必须保留本轮原始问题。')
    branches = {key: text(intent.get(key, ''), required=intent['kind'] == 'mixed')
                for key in ('business_query', 'document_query')}
    if intent['kind'] != 'mixed' and any(branches.values()):
        raise AssistantError('非混合查询不能携带分支条件。')
    names = ('time', 'scopes', 'status', 'shift', 'devices', 'counting_basis')
    literal, current, inherited, replaced = {}, {}, {}, {}
    previous_query = context.get('query', '')
    for name, effective, explicit, old in zip(names, _constraints(query), _constraints(turn['question']),
                                             _constraints(previous_query)):
        if len(effective) > 20 or any(len(value) > 200 for value in effective | explicit | old):
            raise AssistantError('查询条件超过上下文上限。')
        literal[name] = sorted(effective)
        current[name] = sorted(effective & explicit)
        inherited[name] = sorted(effective - explicit) if intent['continued'] else []
        replaced[name] = sorted(old - effective)[:20] if explicit and intent['continued'] else []
    if set(literal['scopes']) - set(context['scopes']):
        raise AssistantError('查询意图不能扩大本轮可访问范围。', 403)
    clean = deepcopy(context)
    if not intent['continued']:
        for key in ('contract', 'previous_contract', 'contract_lane', 'contract_query_key', 'progress'):
            clean.pop(key, None)
        clean['record_refs'] = []
    clean.update(query=query, continued=intent['continued'], branch_queries=branches,
                 domains=sorted(business_domains(query)),
                 conditions={'dates': literal['time'], 'states': literal['status']},
                 literal_constraints=literal,
                 constraint_provenance={'current': current, 'inherited': inherited, 'replaced': replaced},
                 semantic_query_key=hashlib.sha256(turn['question'].encode()).hexdigest(),
                 execution_lane=execution_lane(actor))
    turn['_task_context'] = clean
    return deepcopy(clean)


def _stable_record_refs(rows):
    """Bound whole stable identifiers, never retain editable values or partial IDs."""
    from .lighthouse_ai import safe_text
    identities = ('record_id', 'id', 'active_item_id', 'target_record_id', 'source_record_id',
                  'batch_id', 'drill_id', 'task_id', 'document_id', 'file_id', 'operation_id',
                  'paper_id', 'question_id', 'query_ref')
    result = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        item = {key: value for key, value in row.items() if key in identities + ('title', 'name', 'scope')
                and isinstance(value, str) and value and len(value) <= 256
                and safe_text(value, limit=None) == value}
        if not any(key in item for key in identities) or item in result:
            continue
        if len(json.dumps([*result, item], ensure_ascii=False).encode('utf-8')) > 6000:
            continue
        result.append(item)
        if len(result) >= 20:
            break
    return result


def task_context(question, previous, actor, *, topic_id='', scopes=None):
    """Remember query conditions, never historic form values or granted authority."""
    from .lighthouse_ai import explicit_general_question, safe_text
    from .lighthouse_queries import DATE_TOKEN, PENDING, DOMAINS, business_domains, effective_question, past_action_question
    from .lighthouse_sources import codes
    scopes = sorted(scopes if scopes is not None else actor.get('scopes', []))
    domains = business_domains(question)
    continuing = bool(previous and task_followup(question) and not explicit_general_question(question)
        and not previous.get('display_only') and not previous.get('_private_request')
        and previous.get('owner', actor['id']) == actor['id']
        and previous.get('execution_lane', '') == execution_lane(actor)
        and previous.get('feishu_topic', '') == topic_id
        and set(previous.get('access_scopes', previous.get('scopes', []))) <= set(actor.get('scopes', []))
        and set(previous.get('access_modules', [])) <= set(actor.get('modules', [])))
    prior = previous.get('_task_context') or {} if continuing else {}
    if prior and not _task_contract_allowed(prior, previous, actor):
        prior = {}
        continuing = False
    contract = _saved_task_contract(previous, actor) if continuing else None
    base = (prior.get('query') or effective_question(previous)) if continuing else ''
    prior_domains = business_domains(base)
    if not contract and (not prior_domains or domains and not domains <= prior_domains):
        base = ''
        continuing = False
    if base:
        from .lighthouse_agent import WRITE_INTENT
        editing = WRITE_INTENT.search(base) or re.search(
            r'(?:把|将).{0,100}(?:改成|改为|设为|修改|更新|填写|绑定|删除|发送)'
            r'|(?:进度|备注|位置|内容|影响|原因|执行方)\s*(?:改成|改为|填|写|设|[：:])', base)
        if editing and not past_action_question(base):
            base = '上轮业务对象：' + '、'.join(label for key, _, label, _ in DOMAINS if key in prior_domains)
    if base:
        # Later conditions replace earlier ones instead of accumulating contradictions.
        if DATE_TOKEN.search(question):
            base = DATE_TOKEN.sub('', base)
        if re.search(r'已结束|已闭环|已完成|未结束|未闭环|未完成|未开始|进行中|处理中|待发|未发|已发送', question):
            base = re.sub(r'已结束|已闭环|已完成|已发送', '', PENDING.sub('', base))
        if codes(question) or re.search(r'全部楼|所有楼|全楼|园区', question):
            base = re.sub(r'110\s*(?:站|楼)|[ABCDEH]\s*[楼栋]', '', base, flags=re.I)
        if len(base) > 1500:
            base = base[-1500:]
    query = (base + '\n本次条件：' + question) if base else question
    result = {'version': 1, 'owner': actor['id'], 'topic_id': topic_id, 'scopes': scopes,
              'execution_lane': execution_lane(actor),
              'modules': sorted(actor.get('modules', [])), 'query': safe_text(query, limit=4000),
              'domains': sorted(business_domains(query)), 'continued': bool(base), 'record_refs': [],
              'conditions': {'dates': DATE_TOKEN.findall(query)[-2:],
                             'states': re.findall(r'已结束|已闭环|已完成|已发送|未结束|未闭环|未完成|未开始|进行中|处理中|待发|未发', query)[-2:]}}
    if base and contract:
        result.update(previous_contract=contract, contract_lane=execution_lane(actor),
                      contract_query_key=hashlib.sha256(question.encode()).hexdigest())
    if base and set(previous.get('scopes', [])) == set(scopes):
        # Stable identities explain a selection; status and editable values must be reread.
        from .lighthouse_model import evidence_summary
        rows = [row for group in evidence_summary(previous.get('sources') or []) for row in group.get('items', [])]
        result['record_refs'] = _stable_record_refs(rows or prior.get('record_refs', []))
    return result


def task_question(turn, actor):
    from .lighthouse_queries import effective_question
    context = turn.get('_task_context') or {}
    if (not turn.get('_clarification_reply') and _task_contract_allowed(context, turn, actor)
            and (context.get('semantic_query_key') == hashlib.sha256(turn['question'].encode()).hexdigest()
                 or 'semantic_query_key' not in context and context.get('continued'))):
        return context['query']
    return effective_question(turn)


def reply_preferences(question, saved=None):
    """Only the user's explicit format request becomes a preference, not source text."""
    result = {key: value for key, value in (saved or {}).items()
              if (key == 'detail' and value in {'concise', 'detailed'})
              or (key == 'format' and value in {'table', 'list', 'prose'})}
    match = re.search(r'^\s*(?:请|我希望你|麻烦你)?(?:以后|今后|后续|从现在起)(?:的)?(?:回答|回复|输出|都|请|尽量|给我|用|按|要|不|别|再|些|一点|更|一律|时)*([^。！？\n]{0,30})', question)
    if not match:
        return result
    request = match.group(0)
    if re.search(r'简洁|简短|精简|少说|短一些|短一点', request):
        result['detail'] = 'concise'
    elif re.search(r'详细|展开|具体一些', request):
        result['detail'] = 'detailed'
    if re.search(r'(?:不要|不用|别).{0,3}表格', request):
        result['format'] = 'prose'
    elif '表格' in request:
        result['format'] = 'table'
    elif re.search(r'分点|列表|逐条', request):
        result['format'] = 'list'
    return result


def reply_preference_hint(preferences):
    labels = {'concise': '简洁回答', 'detailed': '详细说明', 'table': '适合对比时使用表格',
              'list': '分点呈现', 'prose': '使用普通文字，不强制表格'}
    parts = [labels[value] for key, value in reply_preferences('', preferences).items()]
    return ('\n本人明确要求的表达偏好（本轮新要求优先，不减少事实核验、不省略业务确认）：' + '；'.join(parts)) if parts else ''


def workspace_document_context(history, actor):
    """Find a bounded stable identity, even beyond the recent six dialogue turns.

    History must come from the authorized conversation archive, not a model
    summary. Reread the document under current ACLs and revision before editing.
    """
    topic = (actor.get('_feishu_topic') or {}).get('id', '')
    from .lighthouse_ai import safe_text
    for old in reversed(history[-100:]):
        anchor = old.get('_workspace_document')
        if isinstance(anchor, dict) and (anchor.get('owner') == actor['id'] and anchor.get('topic_id', '') == topic
                       and old.get('feishu_topic', '') == topic and not old.get('display_only')
                       and not old.get('_private_request')
                       and old.get('owner', actor['id']) == actor['id']
                       and old.get('execution_lane', '') == execution_lane(actor)
                       and set(old.get('access_scopes', old.get('scopes', []))) <= (
                           set(actor['scopes']) & set(actor.get('allowed_scopes', actor['scopes'])))
                       and set(old.get('access_modules', [])) <= set(actor.get('modules', []))):
            if (not isinstance(anchor.get('id'), str) or not re.fullmatch('[a-f0-9]{32}', anchor['id'])
                    or type(anchor.get('revision')) is not int or not 1 <= anchor['revision'] <= 100):
                continue
            result = {'id': anchor['id'], 'revision': anchor['revision']}
            title = anchor.get('title')
            if isinstance(title, str) and len(title) <= 200 and safe_text(title, limit=None) == title:
                result['title'] = title
            return result
    return None


def history_summary_packet(turns, actor, previous='', budget=26000) -> dict:
    """Detached JSON-safe summary input, bounded in serialized UTF-8 bytes.

    Parent can json.dumps(packet, ensure_ascii=False) without slicing JSON.
    Up to 100 recent authorized archive turns are considered. Whole stable task,
    document_references (document_id/version/ordinal/name), file_ids and workspace
    anchors take priority over excerpts. Company text is excluded. Omission counters
    describe all dropped turns/fields; this packet is hints, never live evidence.
    Keep/rebuild it from the archive alongside any model-generated prose summary
    so stable IDs do not depend on the summarizer copying them correctly.
    Raises ValueError for budgets outside 512..26000; never mutates inputs.
    """
    from .lighthouse_ai import safe_text
    from .lighthouse_knowledge_answer import document_references
    from .lighthouse_model import evidence_summary
    if type(budget) is not int or not 512 <= budget <= 26000:
        raise ValueError('history budget must be an integer in 512..26000')
    packet = {'historical_only': True, 'previous': '', 'turns': [],
              'omitted_turns': len(turns), 'truncated': False}

    def fits():
        return len(json.dumps(packet, ensure_ascii=False, allow_nan=False).encode('utf-8')) <= budget

    selected = []
    for turn in reversed(turns[-100:]):
        context = turn.get('_task_context') or {}
        # Legacy archive turns may lack task metadata; bind only their own ACLs.
        binding = context or {'version': 1, 'owner': actor['id'],
                              'topic_id': (actor.get('_feishu_topic') or {}).get('id', ''),
                              'scopes': turn.get('scopes', []), 'modules': turn.get('access_modules', [])}
        if not _task_contract_allowed(binding, turn, actor):
            continue
        sources = [source for source in turn.get('sources') or [] if source.get('kind') != 'company_knowledge']
        rows = [row for group in evidence_summary(sources) for row in group.get('items', [])]
        refs = _stable_record_refs([*rows, *context.get('record_refs', [])])
        entry = {'record_refs': refs, 'excerpt_omitted': True}
        if 'knowledge' in actor.get('modules', []):
            documents = document_references(turn)
            if documents:
                entry['document_references'] = documents
        file_ids = list(turn.get('file_ids') or [])
        file_ids.extend(item.get('id') for item in (turn.get('output_files') or []) + (turn.get('attachments') or [])
                        if isinstance(item, dict))
        file_ids = list(dict.fromkeys(identity for identity in file_ids
            if isinstance(identity, str) and re.fullmatch('[a-f0-9]{32}', identity)))
        if file_ids:
            entry['file_ids'] = file_ids[:40]
            if len(file_ids) > 40:
                entry['file_ids_omitted'] = True
        anchor = workspace_document_context([turn], actor)
        if anchor:
            entry['workspace_document'] = anchor
        contract = _saved_task_contract(turn, actor)
        if contract:
            entry['task_intent'] = contract
        packet['turns'].insert(0, entry)
        # Shrink only whole optional fields/items; never split an identifier.
        if not fits() and 'task_intent' in entry:
            entry.pop('task_intent')
            entry['task_intent_omitted'] = True
        for field in ('record_refs', 'file_ids', 'document_references'):
            values = entry.get(field, [])
            while values and not fits():
                values.pop()
                entry[field + '_omitted'] = True
        if not fits():
            packet['turns'].pop(0)
            continue
        selected.append((entry, turn))
        packet['omitted_turns'] -= 1
    # Text uses the remaining space only after stable references have been fitted.
    for entry, turn in selected:
        for key, maximum in (('question', 800), ('answer', 1200)):
            if key == 'answer' and any(source.get('kind') == 'company_knowledge' for source in turn.get('sources') or []):
                continue  # Documentary answers must be re-read, not recycled as evidence.
            value = turn.get(key, '')
            if not isinstance(value, str) or not value:
                continue
            entry[key] = safe_text(value, limit=maximum)
            if not fits():
                entry.pop(key)
        if (entry.get('question') == turn.get('question')
                and (not turn.get('answer') or entry.get('answer') == turn.get('answer'))):
            entry.pop('excerpt_omitted')
    packet['previous'] = safe_text(previous, limit=2000) if isinstance(previous, str) else ''
    if not fits():
        packet['previous'] = ''
    packet['truncated'] = bool(packet['omitted_turns'] or packet['previous'] != previous
        or any(any(key.endswith('_omitted') for key in entry) for entry in packet['turns']))
    return packet


def answer_evidence_issue(answer, sources):
    """Reject a full-list claim when its latest business receipts are partial."""
    from .lighthouse_queries import query_result_state
    if not re.search(r'完整(?:的)?(?:名单|清单|列表|明细)|全部(?:的)?(?:人员|名单|记录|明细)|已(?:全部|完整)(?:查|读|获取|列)', answer):
        return ''
    if re.search(r'未查全|不完整|不是完整|并非全部|仅(?:为|是)?部分|暂无法|尚未|不能|无法确认', answer):
        return ''
    latest = {}
    for source in sources:
        url = source.get('url', '')
        if not url.startswith('/') or url.startswith('/api/assistant/files/') or '列表片段' in source.get('title', ''):
            continue
        data = source.get('data')
        if isinstance(data, dict):
            available = source.get('available', True)
            state = query_result_state({'ok': available, 'data': data})[0]
            latest[source.get('query_key') or (source['url'], source.get('title', ''))] = (available, data, state)
    if any(not available or state in {'partial', 'unavailable', 'empty_page'}
           or data.get('complete') is False or data.get('list_complete') is False
           or isinstance(data.get('remaining'), int) and data['remaining'] > 0 or data.get('has_more')
           or data.get('next_offset') is not None or data.get('next_cursor')
           or data.get('truncated') for available, data, state in latest.values()):
        return '原查询仍有未取得的分页、失败楼栋或截断资料，不能声称完整名单。请补齐明确缺失的资料，或如实说明已取得的范围；不能重做业务操作。'
    return ''


def source_query_key(operation):
    """Only equivalent list filters may supersede an earlier partial receipt."""
    from .lighthouse_queries import QUERY_PAGE_SCHEMAS
    params = dict(operation.get('params') or {})
    for _, _, position, size, _ in QUERY_PAGE_SCHEMAS.get(operation.get('api_id'), ()):
        params.pop(position, None)
        params.pop(size, None)
    value = [operation.get('api_id'), params, operation.get('path_params') or {}]
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False).encode()).hexdigest()


def _model_context_error(detail):
    from .lighthouse_ai import AssistantError
    return AssistantError(
        'Model request rejected locally: ' + detail +
        ' Use attachments for long text or paginate tool results; current input was not truncated.',
        status=413, category='model_context')


def _positive_model_limit(value, field):
    if isinstance(value, str) and re.fullmatch(r'[0-9]+', value.strip()):
        try:
            value = int(value)
        except ValueError:
            raise _model_context_error(field + ' must be a positive integer.') from None
    if type(value) is not int or value <= 0:
        raise _model_context_error(field + ' must be a positive integer.')
    return value


def model_context_limit(profile):
    """Provider ceiling, separate from the SDK's earlier compaction threshold."""
    if not isinstance(profile, dict):
        raise _model_context_error('Invalid model profile.')
    known = bool(re.fullmatch(r'WanWu/(?:MiniMax|GLM|Qwen|Deepseek)-Auto',
                             str(profile.get('model', '')).strip(), re.I))
    limit = _positive_model_limit(profile['context_window'], 'context_window') if 'context_window' in profile else (149952 if known else 64000)
    return min(limit, 149952) if known else limit


def model_output_limit(profile):
    # Auto routes may spend completion tokens on reasoning even when no visible text is returned.
    return 32000 if re.fullmatch(r'WanWu/(?:MiniMax|GLM|Qwen|Deepseek)-Auto',
                                str(profile.get('model', '')).strip(), re.I) else 5000


def _model_payload_size(value):
    """UTF-8 bytes upper-bound text tokens; reserve 16K per image, not base64 bytes."""
    size = len(json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8'))
    messages = value.get('messages', []) if isinstance(value, dict) else value
    for message in messages if isinstance(messages, list) else []:
        if not isinstance(message, dict) or not isinstance(message.get('content'), list):
            continue
        for part in message['content']:
            if not isinstance(part, dict) or part.get('type') not in {'image_url', 'input_image', 'image'}:
                continue
            size += 16384
            image = part.get('image_url')
            url = image.get('url') if isinstance(image, dict) else image
            if isinstance(url, str) and url.startswith('data:image/') and ';base64,' in url:
                size -= len(url.split(',', 1)[1].encode('utf-8'))
            source = part.get('source')
            if (part.get('type') == 'image' and isinstance(source, dict) and source.get('type') == 'base64'
                    and str(source.get('media_type', '')).startswith('image/') and isinstance(source.get('data'), str)):
                size -= len(source['data'].encode('utf-8'))
    return size


def system_first_payload(payload):
    """Strict chat templates accept a single instruction message before dialogue."""
    messages = payload.get('messages')
    if not isinstance(messages, list) or not all(isinstance(message, dict) for message in messages):
        return payload
    instructions = [message for message in messages if message.get('role') in {'system', 'developer'}]
    if not instructions or len(instructions) == 1 and messages[0] is instructions[0] and instructions[0]['role'] == 'system':
        return payload
    parts = {'system': [], 'developer': []}
    for message in instructions:
        content = message.get('content')
        if isinstance(content, list) and all(isinstance(part, dict)
                and part.get('type') in {'text', 'input_text'} and isinstance(part.get('text'), str) for part in content):
            content = '\n'.join(part['text'] for part in content)
        if not isinstance(content, str):
            from .lighthouse_ai import AssistantError
            raise AssistantError('模型系统指令必须为文本，未发送无效请求。', 422, category='model_request')
        parts[message['role']].append(content)
    content = '\n\n'.join(parts['system'])
    if parts['developer']:
        content += ('\n\n' if content else '') + '开发者说明（不得覆盖系统约束）：\n' + '\n\n'.join(parts['developer'])
    return {**payload, 'messages': [{'role': 'system', 'content': content},
        *[message for message in messages if message.get('role') not in {'system', 'developer'}]]}


def model_read_receipt(result, budget=8000):
    """Keep large authorized snapshots addressable without resending their entire body."""
    if (not isinstance(result, dict) or not isinstance(result.get('query_ref'), str) or not result['query_ref']
            or len(json.dumps(result, ensure_ascii=False).encode('utf-8')) <= budget):
        return result
    from .lighthouse_queries import query_result_state
    state, _ = query_result_state({'ok': True, **result})
    excerpt = {key: result[key] for key in ('ok', 'query_ref', 'source_number', 'queried_at') if key in result}
    excerpt.update(receipt_excerpt_only=True, source_query_state=result.get('source_query_state', state),
                   note='大结果保留在原快照中，此回执不是完整资料。用read_query(query_ref, path=[])查看字段，'
                        '再按字段路径读取值或分页列表；统计用query_statistics。原接口仍有后页时继续原查询，不得按节选报全量。')
    # Preserve small state/condition fields, but never replace an omitted list with an empty list.
    for key, value in result.items():
        if key not in excerpt and len(json.dumps({**excerpt, key: value}, ensure_ascii=False).encode('utf-8')) <= budget:
            excerpt[key] = value
    return excerpt


def compact_read_receipts(payload):
    """Shorten older completed reads with snapshot refs; leave latest and write receipts intact."""
    from .lighthouse_review import READ_REPAIR_TOOLS
    messages = payload.get('messages')
    if not isinstance(messages, list):
        return payload
    latest = next((i for i in range(len(messages) - 1, -1, -1)
                   if isinstance(messages[i], dict) and messages[i].get('role') == 'tool'), -1)
    names, replacements = {}, {}
    allowed = set(READ_REPAIR_TOOLS) | {'lighthouse_' + name for name in READ_REPAIR_TOOLS}
    for index, message in enumerate(messages):
        if not isinstance(message, dict):
            continue
        if message.get('role') == 'assistant':
            for call in message.get('tool_calls') or []:
                if (isinstance(call, dict) and isinstance(call.get('id'), str) and isinstance(call.get('function'), dict)
                        and isinstance(call['function'].get('name'), str)):
                    names[call.get('id')] = call['function'].get('name')
        if (index == latest or message.get('role') != 'tool'
                or not isinstance(message.get('tool_call_id'), str)
                or names.get(message.get('tool_call_id')) not in allowed
                or not isinstance(message.get('content'), str)):
            continue
        try:
            original = json.loads(message['content'])
        except ValueError:
            continue
        excerpt = model_read_receipt(original, budget=1500)
        if excerpt is not original:
            content = json.dumps(excerpt, ensure_ascii=False)
            if len(content.encode('utf-8')) < len(message['content'].encode('utf-8')):
                replacements[index] = {**message, 'content': content}
    return {**payload, 'messages': [replacements.get(i, message) for i, message in enumerate(messages)]} if replacements else payload


def bound_model_payload(payload, profile, *, provider_default=False):
    """Bound a JSON Chat Completions body before transport; never mutate its data.

    Count the entire body at one token per UTF-8 byte, plus 10% and 1024 safety
    tokens. Provider-default requests omit output limits; explicit budgets use
    the smaller value. Returned nested data is shared.
    """
    if not isinstance(payload, dict):
        raise _model_context_error('Invalid request body.')
    payload = system_first_payload(payload)
    limit = model_context_limit(profile)
    # A transport alias must not hide a known provider ceiling (or vice versa).
    if payload.get('model'):
        limit = min(limit, model_context_limit({'model': payload['model'], 'context_window': limit}))
    fields = ('max_completion_tokens', 'max_tokens')
    output_limit = model_output_limit(profile)
    output = 0 if provider_default else min([output_limit] + [_positive_model_limit(payload[key], key) for key in fields if key in payload])
    bounded = {key: value for key, value in payload.items() if key not in fields}
    if not provider_default:
        bounded['max_tokens'] = output
    try:
        size = _model_payload_size(bounded)
        byte_budget = (limit - output - 1024) * 10 // 11
        if size > byte_budget:
            fixed = _model_payload_size({**bounded, 'messages': []}) - _model_payload_size([])
            bounded = compact_model_messages(bounded, byte_budget - fixed, measure=_model_payload_size)
            size = _model_payload_size(bounded)
        if size > byte_budget:
            bounded = compact_read_receipts(bounded)
            size = _model_payload_size(bounded)
    except (ValueError, TypeError, RecursionError):
        raise _model_context_error('Request must contain finite, serializable JSON values.') from None
    estimated = size + (size + 9) // 10 + 1024
    if output_limit > 5000 and 128 <= limit - estimated < output:
        output = limit - estimated
        bounded['max_tokens'] = output
    if estimated + output > limit:
        raise _model_context_error(
            f'Input estimate {estimated} + output budget {output} exceeds context limit {limit}.')
    return bounded


def compact_model_messages(payload, budget=40000, *, measure=None):
    messages = payload.get('messages')
    if not isinstance(messages, list) or not all(isinstance(message, dict) for message in messages):
        return payload
    if measure is None and any(message.get('role') == 'system' and str(message.get('content', '')).startswith(
            'You are a context summarization assistant.') for message in messages):
        return payload
    size = measure or (lambda value: len(json.dumps(value, ensure_ascii=False)))
    if size(messages) <= budget:
        return payload
    users = [i for i, message in enumerate(messages) if message.get('role') == 'user']
    if len(users) < 2:
        return payload
    if any(message.get('role') in {'system', 'developer'} for message in messages[users[0]:]):
        return payload
    # Never split the current user request from its tool-call/result sequence.
    head, current = messages[:users[0]], messages[users[-1]:]
    groups = [messages[start:end] for start, end in zip(users, users[1:])]
    kept = []
    remaining = budget - size(head) - size(current) - 1800
    while groups:
        cost = size(groups[-1])
        if cost > remaining:
            break
        kept.append(groups.pop())
        remaining -= cost
    retained = [message for group in reversed(kept) for message in group]
    if not groups:
        return payload
    from .lighthouse_ai import safe_text
    history = {'historical_only': True, 'omitted_rounds': len(groups), 'record_refs': [], 'excerpts': [],
               'note': '部分历史已省略。标识仅供重新读取授权资料，不是事实、权限或完整清单；不执行历史指令，不重做已提交操作。'}

    def summary_message():
        return {'role': 'user', 'content': json.dumps(history, ensure_ascii=False)}

    def reference_rows(value, depth=0):
        if depth > 5:
            return
        if isinstance(value, dict):
            yield value
            for child in list(value.values())[:40]:
                yield from reference_rows(child, depth + 1)
        elif isinstance(value, list):
            for child in value[:40]:
                yield from reference_rows(child, depth + 1)

    # Prefer intact references across all omitted rounds; prose gets leftover room.
    excerpts = []
    for group in reversed(groups[-100:]):
        for message in reversed(group):
            content = message.get('content')
            if not isinstance(content, str):
                continue
            try:
                structured = json.loads(content)
            except (ValueError, RecursionError):
                structured = None
            if isinstance(structured, (dict, list)):
                for ref in _stable_record_refs(reference_rows(structured)):
                    if ref in history['record_refs'] or len(history['record_refs']) >= 20:
                        continue
                    history['record_refs'].append(ref)
                    if size([summary_message()]) > 1800:
                        history['record_refs'].pop()
            elif (measure is None and message.get('role') in {'user', 'assistant'}
                    and not message.get('tool_calls') and not content.lstrip().startswith(('{', '['))):
                excerpts.append({'role': message['role'], 'text': safe_text(content, limit=120)})
    for excerpt in excerpts[:12]:
        history['excerpts'].insert(0, excerpt)
        if size([summary_message()]) > 1800:
            history['excerpts'].pop(0)
    summary = summary_message()
    compacted = head + [summary] + retained + current
    return {**payload, 'messages': compacted} if size(compacted) < size(messages) else payload


def reply_failure(detail, category='', question=''):
    """Keep actionable business validation; do not show internal model diagnostics."""
    detail = re.sub(r'[（(]诊断编号[：:][^）)]*[）)]', '', str(detail or '')).strip()
    category = str(category or '')
    if re.search(r'(?:原任务|操作|业务)(?:的)?(?:执行)?(?:结果|状态)尚未确认', detail):
        return '原任务结果尚未确认，已保留当前记录。请先查看原任务状态，不要重复提交。'
    if category == 'model_validation':
        return '本次回答未通过资料核验，暂时不能作为可靠结论。原问题已保留。'
    generic = re.search(r'未取得可靠业务依据|模型(?:运行|未|处理)|原回答|完整终答|可见回答|查询处理遇到程序异常', detail)
    if category.startswith('model_') or category in {'timeout', 'query_adapter'} or generic:
        if category == 'model_context':
            return '本次检索或回答请求超过模型的处理上限，自动整理后仍未完成。原问题、聊天记录和已提交任务均已保留；这不表示你提供的问题过长或资料不存在。'
        return '这次未能完成回答，原消息已保留。可以稍后重试原消息或切换可用模型；如涉及业务提交，请先查看原任务状态，避免重复操作。'
    return detail or '本条回答未完成，原消息已保留；如已提交业务，请先查看原任务状态。'
