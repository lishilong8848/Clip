"""Typed work-partner actions; identity and credentials never come from the model."""
from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Action(BaseModel):
    model_config = ConfigDict(extra='forbid', str_strip_whitespace=True)
    operation_id: str = Field(min_length=8, max_length=128, pattern=r'^[A-Za-z0-9_-]+$',
        json_schema_extra={'x-ui-hidden': True, 'x-ui-generate': 'uuid'})


class MessageSend(Action):
    text: str = Field(min_length=1, max_length=12000)
    recipient: str = Field(default='', max_length=120, description='优先使用已查询人员的稳定id，也可用完整姓名或工号；发给本人填写“我”。')
    chat_id: str = Field(default='', max_length=120, description='已查询且当前人员在其中的群ID；不能猜测。')
    mention_names: list[str] = Field(default_factory=list, max_length=20, description='需要真正@的人员完整姓名或工号。')
    reply_message_id: str = Field(default='', max_length=120, description='回复已有消息时，使用当前群已读取的消息ID。')

    @model_validator(mode='after')
    def destination(self):
        if bool(self.recipient) == bool(self.chat_id):
            raise ValueError('请选择一个私信收件人或一个群，不能同时填写。')
        if self.reply_message_id and not self.chat_id:
            raise ValueError('话题回复必须同时指定原群。')
        if self.mention_names and not self.chat_id:
            raise ValueError('@人员仅用于群消息。')
        if '<at' in self.text.lower() or '@all' in self.text.lower():
            raise ValueError('请通过人员选择填写@对象，不接受自行拼接@标记或@所有人。')
        return self


class TaskCreate(Action):
    summary: str = Field(min_length=1, max_length=500)
    description: str = Field(default='', max_length=3000)
    assignees: list[str] = Field(min_length=1, max_length=20, description='负责人完整姓名或工号；本人填写“我”。重名时先查询选择。')
    followers: list[str] = Field(default_factory=list, max_length=20)
    due_time: datetime | None = None


class TaskUpdate(Action):
    summary: str | None = Field(default=None, min_length=1, max_length=500)
    description: str | None = Field(default=None, max_length=3000)
    due_time: datetime | None = None
    completed: bool | None = None

    @model_validator(mode='after')
    def changes(self):
        if not any(getattr(self, key) is not None for key in self.model_fields_set - {'operation_id'}):
            raise ValueError('请至少填写一个要修改的字段。')
        return self


class EventCreate(Action):
    calendar_id: str = Field(min_length=1, max_length=256, description='从已授权资源中取得的日历ID。')
    summary: str = Field(min_length=1, max_length=500)
    description: str = Field(default='', max_length=3000)
    start_time: datetime
    end_time: datetime
    location: str = Field(default='', max_length=300)

    @model_validator(mode='after')
    def interval(self):
        # Both local and explicit-offset inputs are normalized by the service.
        from .feishu_workpartner import timestamp
        if timestamp(self.end_time) <= timestamp(self.start_time):
            raise ValueError('日程结束时间须晚于开始时间。')
        return self


class EventUpdate(Action):
    calendar_id: str = Field(min_length=1, max_length=256)
    summary: str | None = Field(default=None, min_length=1, max_length=500)
    description: str | None = Field(default=None, max_length=3000)
    start_time: datetime | None = None
    end_time: datetime | None = None
    location: str | None = Field(default=None, max_length=300)

    @model_validator(mode='after')
    def changes(self):
        if not any(getattr(self, key) is not None for key in self.model_fields_set - {'operation_id', 'calendar_id'}):
            raise ValueError('请至少填写一个要修改的字段。')
        return self


class EventAttendees(Action):
    calendar_id: str = Field(min_length=1, max_length=256)
    attendees: list[str] = Field(min_length=1, max_length=20, description='参与人的完整姓名或工号。添加参与人会向其发送日程邀请。')


class DocumentCreate(Action):
    title: str = Field(min_length=1, max_length=200)
    folder_id: str = Field(default='', max_length=100, description='可选：管理员已授权的云空间文件夹ID。')


class DocumentAppend(Action):
    text: str = Field(min_length=1, max_length=15000, description='以段落追加正文，不覆盖已有内容。')
    expected_revision: int = Field(ge=0, description='先读文档取得的版本，防止覆盖他人修改。')


class FieldValue(BaseModel):
    model_config = ConfigDict(extra='forbid', allow_inf_nan=False)
    name: str = Field(min_length=1, max_length=100)
    value: str | int | float | bool | list[str] | None


class TableRecordWrite(Action):
    fields: list[FieldValue] = Field(min_length=1, max_length=30,
        description='仅已授权的独立协作表，可修改文本、数字、选项、日期、复选框；原灯塔业务表走原业务接口。')
    expected_version_id: str = Field(default='', max_length=32,
        description='更新时必须提供最近读取的version_id；新增时留空。')

    @model_validator(mode='after')
    def unique_fields(self):
        if len({field.name for field in self.fields}) != len(self.fields):
            raise ValueError('字段名称不能重复。')
        return self


class ResourceGrant(Action):
    kind: Literal['calendar', 'document', 'folder', 'table', 'chat'] = Field(json_schema_extra={
        'x-ui-labels': {'calendar': '共享日历', 'document': '云文档', 'folder': '文件夹', 'table': '独立协作表', 'chat': '群聊'}})
    resource_id: str = Field(min_length=1, max_length=256)
    name: str = Field(min_length=1, max_length=200)
    base_id: str = Field(default='', max_length=100)
    building_scopes: list[Literal['110', 'A', 'B', 'C', 'D', 'E', 'H']] = Field(min_length=1, max_length=7)
    writable: bool = False
    enabled: bool = True
    expected_revision: int = Field(default=0, ge=0)

    @model_validator(mode='after')
    def table_base(self):
        if self.kind == 'table' and not self.base_id:
            raise ValueError('授权表格时需要指定所属多维表ID。')
        return self
