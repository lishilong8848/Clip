"""Small local tools. No eval, filesystem, network or business mutations."""
import ast
import datetime as dt
import decimal
import re


class CalculationError(ValueError):
    pass


def current_user_request(question):
    return bool(re.fullmatch(
        r'(?:请问|请告诉我|帮我查一下|查询|查看)?\s*'
        r'(?:(?:当前|现在|本次)(?:的)?\s*(?:登录|登陆)(?:的)?(?:用户|账号|账户|人员|人)'
        r'(?:是谁|是什么|是哪位|信息)?|我(?:当前|现在)?(?:登录|登陆)(?:的)?(?:是谁|是什么账号|哪个账号)'
        r'|我是谁|我的(?:姓名|工号|账号|账户|登录信息)(?:是什么|是多少)?)'
        r'[?？。!！\s]*', question.strip()))


def calculate(expression):
    if not isinstance(expression, str) or not expression.strip() or len(expression) > 400:
        raise CalculationError('请填写不超过400字的算式。')
    text = expression.strip().replace('×', '*').replace('÷', '/').replace('^', '**')
    if not re.fullmatch(r'[0-9eE.\s+*/()%\-]+', text):
        raise CalculationError('仅支持数字、括号和加减乘除、百分比、余数及整数次方。')
    text = re.sub(r'((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*%(?!\s*(?:[-+]?\s*[\d.]|\())', r'(\1/100)', text)
    try:
        tree = ast.parse(text, mode='eval')
    except (SyntaxError, ValueError, RecursionError):
        raise CalculationError('算式格式不正确，请核对括号和运算符。') from None
    if len(list(ast.walk(tree))) > 100:
        raise CalculationError('算式过长，请分步计算。')

    def bounded(value):
        if not value.is_finite() or not value.is_zero() and abs(value.adjusted()) > 100:
            raise CalculationError('数值超出计算范围，请缩小数值或次方。')
        return value

    def number(node):
        if isinstance(node, ast.Constant) and type(node.value) in (int, float):
            literal = ast.get_source_segment(text, node)
            value = decimal.Decimal(literal)
            if len(value.as_tuple().digits) > 40:
                raise CalculationError('单个数字最多40位有效数字。')
            return bounded(value)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.UAdd, ast.USub)):
            value = number(node.operand)
            return value if isinstance(node.op, ast.UAdd) else -value
        if not isinstance(node, ast.BinOp):
            raise CalculationError('不支持该运算。')
        a, b = number(node.left), number(node.right)
        if isinstance(node.op, ast.Add): result = a + b
        elif isinstance(node.op, ast.Sub): result = a - b
        elif isinstance(node.op, ast.Mult): result = a * b
        elif isinstance(node.op, ast.Div): result = a / b
        elif isinstance(node.op, (ast.FloorDiv, ast.Mod)):
            quotient = (a / b).to_integral_value(rounding=decimal.ROUND_FLOOR)
            result = quotient if isinstance(node.op, ast.FloorDiv) else a - quotient * b
        elif isinstance(node.op, ast.Pow):
            if b != b.to_integral_value() or abs(b) > 30:
                raise CalculationError('次方须为-30至30之间的整数。')
            result = a ** int(b)
        else:
            raise CalculationError('不支持该运算。')
        return bounded(result)

    try:
        with decimal.localcontext() as context:
            context.prec, context.Emax, context.Emin = 50, 100, -100
            context.traps[decimal.Underflow] = True
            result = number(tree.body)
            value = '0' if result.is_zero() else format(result.normalize(), 'f')
    except decimal.DivisionByZero:
        raise CalculationError('除数不能为0。') from None
    except decimal.DecimalException:
        raise CalculationError('无法计算该算式，请核对数值和运算范围。') from None
    return {'expression': expression, 'result': value, 'precision': 50}


def calculation_request(question):
    text = question.strip()
    text = re.sub(r'^(?:请|帮我)?\s*(?:计算|算一下|算算|计算一下)\s*[:：]?\s*', '', text)
    text = re.sub(r'\s*(?:等于多少|是多少|等于|=)?[?？。!！\s]*$', '', text)
    if re.fullmatch(r'[0-9.\s+*/()%\-^×÷]+', text) and re.search(r'[+*/%\-^×÷]', text):
        return text
    return None


def date_time(start_date='', end_date=''):
    now = dt.datetime.now(dt.timezone(dt.timedelta(hours=8)))
    result = {'now': now.isoformat(timespec='seconds'), 'date': now.date().isoformat(),
              'weekday': '星期' + '一二三四五六日'[now.weekday()], 'timezone': 'Asia/Shanghai'}
    if start_date or end_date:
        try:
            start, end = dt.date.fromisoformat(start_date), dt.date.fromisoformat(end_date)
        except (ValueError, TypeError):
            raise ValueError('两个日期均须使用YYYY-MM-DD格式。') from None
        result.update(start_date=start.isoformat(), end_date=end.isoformat(), days=(end-start).days)
    return result


def time_request(question):
    return bool(re.fullmatch(r'(?:请问|查询|查看|告诉我)?\s*(?:现在|当前|今天|今日)(?:北京)?'
                            r'(?:时间|日期|是星期几|星期几|几点|几点了|是几月几日|是几号|是哪一天)'
                            r'(?:是什么|是多少|是什么时候)?[?？。!！\s]*', question.strip()))


def public_capability_request(question):
    return bool(re.fullmatch(
        r'(?:请问\s*)?(?:你|灯塔助手|助手)?\s*(?:能不能|能否|能|可不可以|可以|是否支持|支持)\s*'
        r'(?:联网|上网)(?:查询|搜索|查资料|功能|能力)?(?:吗|么)?[?？。!！\s]*'
        r'(?:(?:请)?(?:简单|简要)(?:说明|回答)[?？。!！\s]*)?'
        r'|can\s+you\s+(?:browse\s+(?:the\s+)?web|search\s+(?:the\s+)?internet)[?。!\s]*',
        question.strip(), re.I))
