from .change_notice import ChangeNoticeHandler


class GenericNoticeHandler(ChangeNoticeHandler):
    """未显式声明的通告类型的兜底处理器。"""

    notice_types = tuple()
    table_id_attr = ""
