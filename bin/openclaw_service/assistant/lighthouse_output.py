"""Local checks for user-facing narration, not business values or code."""
import re

CHINESE_OUTPUT_NOTICE = '本次答复未通过中文与内容校验，不合格内容未展示。原问题和已读取资料已保留。'
_CJK = re.compile(r'[\u3400-\u9fff]')
_WORD = re.compile(r"[A-Za-z]+(?:['’][A-Za-z]+)?")
_INTERNAL = re.compile(
    r'\b(?:I|we)\s+(?:should|must|need\s+to|have\s+to)\s+(?:give|answer|respond|reply|say|suggest|follow|make\s+sure|provide|tell|explain|be\s+(?:concise|careful))\b'
    r'|\bfollowing\s+(?:the|these|my)\s+(?:system\s+)?instructions\b'
    r'|\blet\s+me\s+(?:think|reason)\b'
    r'|(?:我的(?:内部)?思考过程|根据(?:系统|开发者)(?:提示词|指令).{0,12}(?:我|应该)|我应该(?:给出|回答|回复|告诉用户|建议用户))', re.I)
_TECHNICAL = re.compile(
    r'\b(?:API|HTTP|HTTPS|JSON|CSV|SQL|XML|HTML|CSS|PDF|DOCX|XLSX|PPTX|SOP|EOP|MOP|UPS|HVDC|BMS|ECC|GPU|CPU|VPN|OCR|AI|PC|Qt|VNET|Python|JavaScript|TypeScript|MiniMax|DeepSeek|GLM|Qwen)\b', re.I)


def public_prose(value):
    """Leave filenames, links and quoted/code literals intact in the real reply."""
    text = str(value or '')
    text = re.sub(r'```[^\n]*\n?[\s\S]*?(?:```|$)', '', text)
    text = re.sub(r'`[^`\n]+`', '', text)
    if _CJK.search(text):
        text = re.sub(r'^\s*>[^\n]*(?:\n|$)', '', text, flags=re.M)
        text = re.sub(r'“[^”\n]*”|「[^」\n]*」', '', text)
    text = re.sub(r'!?\[([^\]]*)\]\([^\n)]+\)', r'\1', text)
    text = re.sub(r'https?://[^\s<>]+|(?:[A-Za-z]:[\\/]|/)[\w./\\%+~:@-]+', '', text)
    text = re.sub(r'\b[\w.\-\u3400-\u9fff]+\.(?:xlsx?|xlsm|docx?|pdf|pptx?|csv|json|txt|md|png|jpe?g|webp|heic|zip)\b', '', text, flags=re.I)
    text = re.sub(r'\b(?:rec|tbl|app|cli|ou|oc|om)_[A-Za-z0-9]+\b', '', text)
    return text


def _nontechnical(value):
    text = _TECHNICAL.sub('', value)
    return re.sub(r'\b[A-Za-z][A-Za-z0-9_.-]*\d[A-Za-z0-9_.-]*\b', '', text)


def presentation_issue(value):
    prose = public_prose(value)
    if _INTERNAL.search(prose):
        return '答复包含内部自述或遵循指令的过程。只返回面向用户的中文结论、必要依据和限制，不输出思考、评审或写作计划。'
    for paragraph in re.split(r'\n\s*\n|[\r\n]', prose):
        words = _WORD.findall(_nontechnical(paragraph))
        if not words:
            continue
        letters = sum(len(word) for word in words)
        chinese = len(_CJK.findall(paragraph))
        if not chinese or len(words) >= 5 and letters > max(24, chinese * 2):
            return '面向用户的说明必须使用中文。保留原有数字、姓名、文件名、型号、链接、代码和来源编号，仅修正表达；不补造事实或改变统计口径。'
    return ''


def public_answer(value):
    return CHINESE_OUTPUT_NOTICE if presentation_issue(value) else value


def public_result(result):
    if not presentation_issue(result.get('answer', '')):
        return result
    return {**result, 'answer': CHINESE_OUTPUT_NOTICE,
            'answer_review': {**result.get('answer_review', {}), 'status': 'needs_review',
                              'reason': 'presentation_rejected'}}


def pending_english_start(value):
    """Hold an unfinished English phrase instead of leaking its first words live."""
    match = re.search(r"[A-Za-z][A-Za-z\s'’\",;:!?().-]*$", value)
    if not match or not _WORD.search(_nontechnical(public_prose(match.group()))):
        return None
    # A currently open code fence is an artifact, not explanatory narration.
    if value[:match.start()].count('```') % 2:
        return None
    return match.start()
