import re
from ..utils import WHITESPACE_TRANSLATOR

# 通告类型常量

NOTICE_TYPE_WEIBAO = "维保通告"  # 维保通告
NOTICE_TYPE_BIANGENG = "设备变更"  # 设备变更
NOTICE_TYPE_TIAOZHENG = "设备调整"  # 设备调整
NOTICE_TYPE_SHIJIAN = "事件通告"  # 事件通告
NOTICE_TYPE_POWER = "上下电通告"  # 上下电通告
NOTICE_TYPE_POWER_UP = "上电通告"  # 上电通告
NOTICE_TYPE_POWER_DOWN = "下电通告"  # 下电通告
NOTICE_TYPE_POLLING_ALT = "设备轮巡"  # 设备轮巡
NOTICE_TYPE_OVERHAUL = "设备检修"  # 设备检修

# 支持的通告类型列表
SUPPORTED_NOTICE_TYPES = [
    NOTICE_TYPE_WEIBAO,
    NOTICE_TYPE_BIANGENG,
    NOTICE_TYPE_TIAOZHENG,
    NOTICE_TYPE_SHIJIAN,
    NOTICE_TYPE_POWER,
    NOTICE_TYPE_POWER_UP,
    NOTICE_TYPE_POWER_DOWN,
    NOTICE_TYPE_POLLING_ALT,
    NOTICE_TYPE_OVERHAUL,
]

SUPPORTED_NOTICE_MARKERS = tuple(f"【{notice_type}】" for notice_type in SUPPORTED_NOTICE_TYPES)

# 预编译正则表达式
PATTERN_STATUS = re.compile(r"状态[：:]\s*(.*?)(?:\s*[\n【]|$)", re.DOTALL)
PATTERN_TITLE = re.compile(r"【名称】(.*?)【", re.DOTALL)
PATTERN_TITLE_ALT = re.compile(r"【标题】(.*?)【", re.DOTALL)  # 事件通告使用【标题】
PATTERN_TIME = re.compile(r"【时间】(.*?)【", re.DOTALL)
PATTERN_LOCATION = re.compile(r"【位置】(.*?)【", re.DOTALL)
PATTERN_LEVEL = re.compile(r"【等级】(.*?)【", re.DOTALL)  # 设备变更特有


def strip_mentions_from_tail(content: str) -> str:
    """
    删除最后一行中第一个 @ 之后的内容（常用于去除 @人 提醒）。
    """
    if "@" not in content:
        return content
    lines = content.rstrip().splitlines()
    if not lines:
        return content
    last_line = lines[-1]
    at_index = last_line.find("@")
    if at_index == -1:
        return content
    trimmed_last = last_line[:at_index].rstrip()
    if trimmed_last:
        lines[-1] = trimmed_last
    else:
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _prepare_notice_text(content: str) -> str:
    raw_content = str(content or "")
    if not raw_content:
        return ""
    cleaned = raw_content.lstrip("\ufeff \t\r\n")
    if not cleaned:
        return ""
    for marker in SUPPORTED_NOTICE_MARKERS:
        if cleaned.startswith(marker):
            return cleaned
    return ""


def extract_notice_info(content):
    """
    从文本中提取通告信息（支持维保通告、设备变更、设备调整）
    :param content: 文本内容
    :return: dict (notice_type, status, unique_key, title, content) or None
    """
    raw_content = _prepare_notice_text(content)
    if not raw_content:
        return None
    # 检测通告类型
    notice_type = None
    for nt in SUPPORTED_NOTICE_TYPES:
        marker = f"【{nt}】"
        if marker in raw_content:
            notice_type = nt
            break

    if not notice_type:
        return None

    # 提取状态
    status_match = PATTERN_STATUS.search(raw_content)
    status = status_match.group(1).strip() if status_match else ""

    # 提取名称/标题作为唯一标识（事件通告使用【标题】，其他使用【名称】）
    title_match = PATTERN_TITLE.search(raw_content)
    if not title_match:
        title_match = PATTERN_TITLE_ALT.search(raw_content)  # 尝试【标题】
    if not title_match:
        return None
    title = title_match.group(1).replace("\n", " ").strip()

    # 提取时间构建唯一键
    time_match = PATTERN_TIME.search(raw_content)
    time_str = time_match.group(1).strip() if time_match else ""

    # 提取来源（仅事件通告）
    source = ""
    if notice_type == NOTICE_TYPE_SHIJIAN:
        pattern_source = re.compile(r"【来源】(.*?)【", re.DOTALL)
        source_match = pattern_source.search(raw_content)
        if source_match:
            source = source_match.group(1).strip()

    # 唯一键 = 名称 + 时间（确保同一事件的更新能匹配）
    unique_key = f"{title}|{time_str}"

    # 提取等级（仅设备变更）
    level = None
    if notice_type == NOTICE_TYPE_BIANGENG:
        level_match = PATTERN_LEVEL.search(raw_content)
        if level_match:
            raw_level = level_match.group(1).strip()
            # 仅提取有效等级：超低、极低、低、中、高
            # 注意：需优先匹配长词（如'超低'），避免'低'匹配到'超低'的一部分（虽然正则通常按顺序或自动处理，但显式列表更清晰）
            valid_level_match = re.search(r"(超低|极低|低|中|高)", raw_level)
            if valid_level_match:
                level = valid_level_match.group(1)

    cleaned_content = strip_mentions_from_tail(raw_content)

    return {
        "notice_type": notice_type,
        "status": status,
        "unique_key": unique_key,
        "title": title,
        "content": cleaned_content,
        "level": level,  # 新增：等级字段
        "source": source,  # 新增：来源字段
        "time_str": time_str,  # 新增：原始时间字符串
    }


def clean_key(key):
    """清除 Key 中的空白字符，用于模糊匹配"""
    return key.translate(WHITESPACE_TRANSLATOR)


# 保留旧函数名作为别名，便于渐进式迁移
def extract_event_info(content):
    """兼容旧接口，内部调用新函数"""
    return extract_notice_info(content)
