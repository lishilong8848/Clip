"""Checkpointed question validation and bounded, single-question repair."""
import copy
import json

from .learning import LearningError
from .learning_sharing import question_mix, validate_questions

REPAIR_ATTEMPTS = 2
QUESTION_FIELDS = ('type', 'stem', 'options', 'correct', 'reference_answer', 'explanation', 'evidence', 'image_ids')


def repair_candidate(question):
    if not isinstance(question, dict):
        return {}
    value = {name: question[name] for name in QUESTION_FIELDS if name in question}
    try:
        if len(json.dumps(value, ensure_ascii=False, allow_nan=False)) <= 12000:
            return copy.deepcopy(value)
    except (TypeError, ValueError, RecursionError):
        pass
    return {'stem': str(question.get('stem', ''))[:500]}


def checked_questions(state, source, types):
    return validate_questions([slot['question'] for slot in state['slots'] if slot['status'] == 'valid'],
        source['text'], types, draft=True, images=source.get('images', []))


def check_candidate(question, expected, state, source, types):
    if not isinstance(question, dict):
        raise LearningError('题目格式无效，请按指定题型重新生成这一题。')
    question = {**question, 'origin': 'ai', 'image_confirmed': False}
    checked = validate_questions([*checked_questions(state, source, types), question], source['text'],
        types, draft=True, images=source.get('images', []))[-1]
    if checked['type'] != expected:
        raise LearningError('此题的题型与本次指定题型不一致，请只修订这一题。')
    return checked


async def generate_checked_questions(batch, source, state, generate, checkpoint):
    """Only invalid slots are model inputs for repair; valid slots are never replaced."""
    types = batch.get('question_types') or ['single', 'multiple']
    mix, first = question_mix(40, types), question_mix(5, types)
    for kind in first:
        mix.remove(kind)
    mix = first + mix
    summaries = [{'id': image['id'], 'text': image['text'], 'mode': image['mode']} for image in source.get('images', [])]
    extras = {'source_images': summaries} if summaries else {}

    async def save(progress):
        await checkpoint(checked_questions(state, source, types), state, progress)

    async def repair_pending():
        for index, slot in enumerate(state['slots']):
            if slot['status'] != 'pending':
                continue
            while slot['attempts'] < REPAIR_ATTEMPTS:
                strategy = 'revise' if slot['attempts'] == 0 else 'replace'
                slot['attempts'] += 1
                await save(f'正在自动核对并修订第{index + 1}题（{slot["attempts"]}/{REPAIR_ATTEMPTS}），其余合格题保持不变')
                try:
                    result = await generate(batch['owner'], source['text'],
                        [q['stem'] for q in checked_questions(state, source, types)], 1,
                        question_types=[slot['expected']], repair={'question': slot['candidate'],
                            'reason': slot['error'], 'strategy': strategy}, **extras)
                    if not isinstance(result, list) or len(result) != 1:
                        raise LearningError('未返回一条完整、可核对的修订题目。')
                    value = check_candidate(result[0], slot['expected'], state, source, types)
                except Exception as exc:
                    # Read-only model failures must not discard other checked questions.
                    slot['error'] = str(exc)[:500] if isinstance(exc, LearningError) else '单题修订暂未完成，请仅重新生成这一题并核对依据。'
                else:
                    slot.update(status='valid', question=value, repaired=True)
                    slot.pop('candidate', None)
                    slot.pop('error', None)
                    break
            if slot['status'] == 'pending':
                slot['status'] = 'discarded'
            await save('单题核对进度已保存，继续处理未覆盖知识点')

    await save('已保留合格题，继续核对待处理题目')
    await repair_pending()
    while not state['exhausted'] and state['offset'] < 40:
        offset = state['offset']
        await save(f'已核对{len(checked_questions(state, source, types))}题，继续生成未覆盖知识点（最多40题）')
        additional = await generate(batch['owner'], source['text'],
            [q['stem'] for q in checked_questions(state, source, types)], 5,
            question_types=mix[offset:offset + 5], **extras)
        if not isinstance(additional, list) or len(additional) > 5:
            raise LearningError('模型返回题目数量或格式无效，已保留之前的合格题。', 502)
        # Check the entire page before repairing so later valid questions are also protected.
        for question, expected in zip(additional, mix[offset:offset + 5]):
            slot = {'expected': expected, 'attempts': 0}
            try:
                value = check_candidate(question, expected, state, source, types)
            except LearningError as exc:
                slot.update(status='pending', candidate=repair_candidate(question), error=str(exc)[:500])
            else:
                slot.update(status='valid', question=value)
            state['slots'].append(slot)
        state.update(offset=offset + 5, exhausted=len(additional) < 5)
        await save('本轮合格题已保存，正在核对问题题目')
        await repair_pending()
    questions = checked_questions(state, source, types)
    if len(questions) < 5:
        raise LearningError(f'自动核对和修订后仅有{len(questions)}道合格题，已保存且不会重做；仍不足5题，未发布，可重试剩余题目或补充材料。')
    if set(types) - {q['type'] for q in questions}:
        raise LearningError('合格题已保存，但仍缺少部分已选题型；未发布，可重试剩余题目或补充材料。')
    return questions
