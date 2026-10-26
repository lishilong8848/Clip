"""Import existing questions into the sharing draft with durable, source-checked progress."""
import copy

from .learning import LearningError, canonical, resolve_answer
from .learning_sharing import validate_questions


def imported_question(raw, text, start, images):
    if not isinstance(raw, dict):
        raise LearningError('文件题目提取格式不完整，原文件和已导入题目保留。', 502)
    quote = raw.get('source_quote')
    if not isinstance(quote, str) or not quote.strip() or len(quote) > 5000:
        raise LearningError('导入题目缺少可核验原文，未添加该题。', 502)
    position = text.find(quote, start)
    if position < 0:
        raise LearningError('导入题目原文位置无法核对，未跳过或猜测题目。', 502)
    kind = raw.get('type')
    if kind not in {'single', 'multiple', 'interview'}:
        raise LearningError('文件题型未识别为单选、多选或问答题，请核对原文件。')
    stem, options, answer, explanation = (raw.get(key) for key in ('stem', 'options', 'answer', 'explanation'))
    if (not isinstance(options, list) or any(not isinstance(value, str) for value in options)
            or any(not isinstance(value, str) for value in (stem, answer, explanation))):
        raise LearningError('导入题干、选项或答案格式无效，未使用不完整内容。', 502)
    if not stem.strip() or any(canonical(value) not in canonical(quote) for value in [stem, *options, answer, explanation] if value.strip()):
        raise LearningError('提取的题干、选项或答案与原文不符，未用模型推测内容替代。', 502)
    correct = resolve_answer(answer, [{'id': index, 'text': option} for index, option in enumerate(options)]) if answer and kind != 'interview' else []
    # Unrecognized answers stay blank for explicit correction; never use the model's guessed index.
    if kind == 'single' and len(correct) > 1 or kind == 'multiple' and len(correct) == 1:
        correct = []
    ids = [image['id'] for image in images if quote in image.get('text', '')]
    question = {'origin': 'manual', 'type': kind, 'stem': stem, 'options': options if kind != 'interview' else [],
                'correct': correct, 'reference_answer': answer if kind == 'interview' else '',
                'explanation': explanation, 'evidence': quote[:800], 'image_ids': ids[:3], 'image_confirmed': False}
    return question, position + len(quote)


async def import_questions(batch, source, state, generate, checkpoint):
    images = source.get('images', [])
    text = source['text'] + ''.join('\n\n' + image['text'] for image in images if image.get('mode') in {'ocr', 'vision'})
    if not text.strip() or len(text) > 24000:
        raise LearningError('导入材料须有可读取的题目，正文与识别文字合计最多24000字；请拆分过长题库。')
    questions = [slot['question'] for slot in state['slots'] if slot['status'] == 'valid']
    while not state['exhausted']:
        offset = state['offset']
        await checkpoint(questions, state, f'已导入{len(questions)}题，正在提取原文题目、选项和答案')
        count = max(1, min(5, 100 - len(questions)))
        context = [text[:min(offset, 2000)]] if offset else []
        response = await generate(batch['owner'], text[offset:], context, count, mode='question_import')
        if (not isinstance(response, dict) or not isinstance(response.get('questions'), list)
                or len(response['questions']) > count or type(response.get('has_more')) is not bool
                or not response['questions'] and response['has_more']):
            raise LearningError('题目提取分页未完整返回，已导入题目保留，可重试原任务。', 502)
        if len(questions) + len(response['questions']) > 100:
            raise LearningError('原文件超过每批100题，已保留前100题；请拆分文件后继续导入。')
        added = []
        for value in response['questions']:
            question, offset = imported_question(value, text, offset, images)
            added.append(question)
        checked = validate_questions(questions + added, source['text'], draft=True, images=images)
        # Do not trust the model's end flag while original text remains unexamined.
        exhausted = not text[offset:].strip() or not added and not response['has_more']
        next_state = {**state, 'offset': offset, 'exhausted': exhausted,
                      'slots': state['slots'] + [{'status': 'valid', 'question': q, 'attempts': 0} for q in checked[len(questions):]]}
        await checkpoint(checked, next_state, f'已提取{len(checked)}题，正在保存导入草稿')
        state.update(copy.deepcopy(next_state))
        questions = checked
    if not questions:
        raise LearningError('材料中未识别到已有题目；可换用含题干、选项和答案的文件，或使用AI出题。')
    return questions
