import json
import os
import tempfile
import time
from typing import Callable

import lark_oapi as lark
from lark_oapi.api.auth.v3 import (
    InternalTenantAccessTokenRequest,
    InternalTenantAccessTokenRequestBody,
)
from lark_oapi.api.bitable.v1 import (
    AppTableRecord,
    CreateAppTableRecordRequest,
    DeleteAppTableRecordRequest,
    GetAppTableRecordRequest,
    UpdateAppTableRecordRequest,
    ListAppTableFieldRequest,
)
from lark_oapi.api.drive.v1 import UploadAllMediaRequest, UploadAllMediaRequestBody
from lark_oapi.api.wiki.v2 import GetNodeSpaceRequest

from ..config import config
from ..logger import log_error, log_info
from .handlers import NoticePayload, get_notice_handler


# 飞书错误码排查建议映射
FEISHU_ERROR_SUGGESTIONS = {
    1254003: "app_token 错误，请检查配置",
    1254004: "table_id 错误，请检查配置",
    1254006: "record_id 错误，请检查记录ID",
    1254015: "字段类型和值不匹配，请检查字段值格式",
    1254040: "app_token 不存在，请检查配置",
    1254041: "table_id 不存在，请检查配置",
    1254043: "record_id 不存在，请检查记录ID",
    1254045: None,  # 特殊处理
    1254291: "同一个数据表不支持并发调用写接口，请稍后重试",
    1254302: "权限不足，需要为调用身份授予多维表格的高级权限",
    1254304: "权限不足，需要多维表格的可管理权限",
    1254607: "数据未就绪，请稍后重试",
    99991663: "Token 已过期，正在自动刷新",
    99991664: "Token 已过期，正在自动刷新",
    99991665: "Token 已过期，正在自动刷新",
}


def _is_token_error(response) -> bool:
    msg = response.msg.lower() if getattr(response, "msg", None) else ""
    return response.code in [99991663, 99991664, 99991665, 99991677] or "token" in msg


def _build_client() -> lark.Client:
    return (
        lark.Client.builder()
        .enable_set_token(True)
        .log_level(lark.LogLevel.ERROR)
        .build()
    )


def _with_token_retry(request_fn: Callable[[str], object]):
    response = request_fn(config.user_token)
    if response.success():
        return response
    if _is_token_error(response):
        log_info(f"Token 可能过期(code={response.code})，尝试刷新...")
        new_token = refresh_feishu_token()
        if new_token:
            response = request_fn(new_token)
    return response


def _get_bitable_fields(notice_type: str) -> list:
    """
    调用飞书API获取多维表格的字段列表

    Returns:
        字段名称列表
    """
    try:
        handler, table_id, err = _resolve_handler(notice_type)
        if err or not table_id:
            return []

        client = _build_client()
        request = (
            ListAppTableFieldRequest.builder()
            .app_token(config.app_token)
            .table_id(table_id)
            .page_size(200)
            .build()
        )

        def do_list(token: str):
            option = lark.RequestOption.builder().user_access_token(token).build()
            return client.bitable.v1.app_table_field.list(request, option)

        response = _with_token_retry(do_list)

        if not response.success():
            return []

        # 提取字段名称
        field_names = []
        if response.data and response.data.items:
            for field in response.data.items:
                if field.field_name:
                    field_names.append(field.field_name)

        return field_names

    except Exception as e:
        log_error(f"获取多维表格字段失败: {e}")
        return []


def _check_field_mismatch(notice_type: str, fields: dict) -> str:
    """
    获取多维表格的实际字段并与config中的字段对比

    Returns:
        错误提示字符串
    """
    try:
        # 获取多维表格的实际字段列表
        actual_fields = _get_bitable_fields(notice_type)
        if not actual_fields:
            return "字段未找到，请检查多维表格中字段"

        # 对比发送的字段与实际字段
        actual_field_set = set(actual_fields)
        missing_fields = []

        for field_name in fields.keys():
            if field_name not in actual_field_set:
                missing_fields.append(field_name)

        if missing_fields:
            fields_str = "】、【".join(missing_fields)
            return f"config中字段【{fields_str}】在多维表格中不存在，请检查多维表格"

        return "字段未找到，请检查多维表格中字段"

    except Exception as e:
        log_error(f"字段对比失败: {e}")
        return "字段未找到，请检查多维表格中字段"


def _parse_field_error(response, notice_type: str, fields: dict) -> str:
    """
    解析飞书字段错误并返回友好提示

    Args:
        response: 飞书API响应
        notice_type: 通告类型
        fields: 实际发送的字段字典

    Returns:
        友好的错误提示字符串
    """
    error_code = response.code

    # 特殊处理 1254045 字段不存在错误
    if error_code == 1254045:
        return _check_field_mismatch(notice_type, fields)

    # 返回常见错误码的排查建议
    if error_code in FEISHU_ERROR_SUGGESTIONS:
        return FEISHU_ERROR_SUGGESTIONS[error_code]

    # 返回原始错误信息
    return f"{response.code} - {response.msg}"


def _extract_status_from_fields(fields: dict) -> str:
    if not isinstance(fields, dict):
        return ""
    for key, value in fields.items():
        if "状态" not in str(key):
            continue
        if isinstance(value, str) and value.strip():
            return value.strip()
        if isinstance(value, dict):
            name = value.get("name") or value.get("text")
            if isinstance(name, str) and name.strip():
                return name.strip()
        if isinstance(value, list) and value:
            item = value[0]
            if isinstance(item, dict):
                name = item.get("name") or item.get("text")
                if isinstance(name, str) and name.strip():
                    return name.strip()
            if isinstance(item, str) and item.strip():
                return item.strip()
    return ""


def _log_record_action(
    action: str, notice_type: str, record_id: str, fields: dict, text: str
) -> None:
    status = _extract_status_from_fields(fields)
    payload = {
        "action": action,
        "notice_type": notice_type,
        "record_id": record_id,
        "status": status,
        "fields": fields,
        "text": text,
    }
    log_info(f"RecordAction: {json.dumps(payload, ensure_ascii=False)}")


def refresh_feishu_token():
    """刷新飞书 Token (Tenant Access Token)"""
    log_info("正在尝试刷新飞书 Token...")
    try:
        app_id = config.app_id
        app_secret = config.app_secret

        if not app_id or not app_secret:
            log_error("Token 刷新失败: 未配置 App ID 或 App Secret")
            return None

        client = lark.Client.builder().build()

        req = (
            InternalTenantAccessTokenRequest.builder()
            .request_body(
                InternalTenantAccessTokenRequestBody.builder()
                .app_id(app_id)
                .app_secret(app_secret)
                .build()
            )
            .build()
        )

        resp = client.auth.v3.tenant_access_token.internal(req)

        if resp.success():
            data = json.loads(resp.raw.content)
            new_token = data["tenant_access_token"]
            expire_in = data.get("expire", 7200)  # 默认 2 小时

            expire_time = int(time.time()) + expire_in - 300
            config.save(user_token=new_token, token_expire_time=expire_time)

            log_info(f"Token 刷新成功: {new_token[:10]}... (过期时间: {expire_time})")
            return new_token
        else:
            log_error(f"Token 刷新失败: {resp.code} - {resp.msg}")
            return None
    except Exception as e:
        log_error(f"Token 刷新异常: {e}")
        return None


def _get_tenant_access_token_with_credentials(
    app_id: str, app_secret: str
) -> tuple[str, str]:
    try:
        if not app_id or not app_secret:
            return "", "missing app_id/app_secret"

        client = lark.Client.builder().build()
        req = (
            InternalTenantAccessTokenRequest.builder()
            .request_body(
                InternalTenantAccessTokenRequestBody.builder()
                .app_id(app_id)
                .app_secret(app_secret)
                .build()
            )
            .build()
        )
        resp = client.auth.v3.tenant_access_token.internal(req)
        if resp.success():
            data = json.loads(resp.raw.content)
            return data.get("tenant_access_token", ""), ""
        return "", f"{resp.code} - {resp.msg}"
    except Exception as exc:
        return "", str(exc)


def resolve_bitable_app_token(
    app_id: str, app_secret: str, app_token: str
) -> tuple[str, bool]:
    """
    尝试将 Wiki token 转为 obj_token（多维表格 token）。
    若无 obj_token 返回，则认为已是 base token，不做替换。
    """
    if not app_token:
        return app_token, False

    token, err = _get_tenant_access_token_with_credentials(app_id, app_secret)
    if err or not token:
        # 无法解析时，视为 base token，不做替换
        log_info(f"未解析 app_token（视为 base token）：{err}")
        return app_token, False

    try:
        client = (
            lark.Client.builder()
            .enable_set_token(True)
            .log_level(lark.LogLevel.ERROR)
            .build()
        )
        request = (
            GetNodeSpaceRequest.builder().token(app_token).obj_type("wiki").build()
        )
        option = lark.RequestOption.builder().user_access_token(token).build()
        response = client.wiki.v2.space.get_node(request, option)
        if not response.success():
            # 视为 base token，不做替换
            return app_token, False

        payload = json.loads(response.raw.content)
        obj_token = payload.get("data", {}).get("node", {}).get("obj_token", "")
        if obj_token and obj_token != app_token:
            return obj_token, True
        return app_token, False
    except Exception as exc:
        log_error(f"解析 app_token 异常: {exc}")
        return app_token, False


def check_token_status():
    """检查 Token 状态，如果过期或即将过期则刷新"""
    try:
        current_time = int(time.time())
        if current_time >= config.token_expire_time:
            log_info("Token 已过期或即将过期，正在刷新...")
            refresh_feishu_token()
    except Exception as e:
        log_info(f"Token 检查/刷新异常: {e}")


def upload_media_to_feishu(image_bytes, file_name="screenshot.jpg", file_size=None):
    """
    上传图片到飞书，返回 file_token
    :param image_bytes: 图片的字节数据
    :param file_name: 文件名
    :param file_size: 文件大小（字节）
    :return: (success, file_token or error_msg)
    """
    check_token_status()

    if not config.user_token:
        return False, "未配置飞书用户令牌"

    temp_file_path = None

    try:
        suffix = os.path.splitext(file_name)[1] or ".jpg"
        with tempfile.NamedTemporaryFile(
            delete=False, prefix="feishu_upload_", suffix=suffix
        ) as tmp:
            tmp.write(image_bytes)
            temp_file_path = tmp.name

        if file_size is None:
            file_size = len(image_bytes)

        def attempt_upload(token: str):
            with open(temp_file_path, "rb") as file:
                client = _build_client()
                request = (
                    UploadAllMediaRequest.builder()
                    .request_body(
                        UploadAllMediaRequestBody.builder()
                        .file_name(file_name)
                        .parent_type("bitable_image")
                        .parent_node(config.app_token)
                        .size(str(file_size))
                        .file(file)
                        .build()
                    )
                    .build()
                )
                option = lark.RequestOption.builder().user_access_token(token).build()
                return client.drive.v1.media.upload_all(request, option)

        response = _with_token_retry(attempt_upload)

        if not response.success():
            error_msg = f"上传失败: {response.code} - {response.msg}"
            log_error(f"飞书上传: {error_msg}")
            return False, error_msg

        file_token = response.data.file_token
        log_info(f"飞书上传: 成功, file_token={file_token}")
        return True, file_token

    except Exception as e:
        log_error(f"飞书上传: 异常 - {e}")
        return False, str(e)
    finally:
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except Exception:
                pass


def _resolve_handler(notice_type: str):
    handler = get_notice_handler(notice_type)
    table_id = handler.get_table_id(config)
    if not table_id:
        return None, None, f"未配置 {notice_type} 的表格ID"
    return handler, table_id, None


def _send_robot_message(handler, payload: NoticePayload):
    try:
        title, content, notice_type, level = handler.build_robot_message(payload)
        if not title and not content:
            return
        choice = str(getattr(payload, "robot_group_choice", "") or "").strip().lower()
        is_change_notice = notice_type in ("设备变更", "变更通告")
        if choice == "skip":
            log_info("群机器人发送: 按用户选择跳过本次群消息发送")
            return
        if choice == "i2":
            if is_change_notice:
                log_info("群机器人发送: 变更类 I2 群发送已移除，按跳过处理")
                return
            level = "I2"
        elif choice == "i3":
            level = "I3"
        ok, msg = handler.send_group_robot_message(title, content, notice_type, level)
        if not ok and msg != "skip":
            log_error(f"群机器人发送失败: {msg}")
    except Exception as exc:
        log_error(f"群机器人发送异常: {exc}")


def create_bitable_record(
    data_source_text,
    notice_type,
    level=None,
    buildings=None,
    specialty=None,
    event_source=None,
    file_tokens=None,
    extra_file_tokens=None,
    response_time=None,
    occurrence_date=None,
    recover=False,
):
    """
    创建多维表格记录
    :return: (success, record_id or error_msg)
    """
    check_token_status()

    if not config.user_token:
        return False, "未配置飞书用户令牌"

    handler, table_id, err = _resolve_handler(notice_type)
    if err:
        return False, err

    payload = NoticePayload(
        text=data_source_text,
        level=level,
        buildings=buildings,
        specialty=specialty,
        event_source=event_source,
        file_tokens=file_tokens,
        extra_file_tokens=extra_file_tokens,
        response_time=response_time,
        occurrence_date=occurrence_date,
        recover=recover,
    )
    fields = handler.build_create_fields(payload)
    log_info(f"Creating record({notice_type}) with fields: {fields}")

    client = _build_client()
    request = (
        CreateAppTableRecordRequest.builder()
        .app_token(config.app_token)
        .table_id(table_id)
        .request_body(AppTableRecord.builder().fields(fields).build())
        .build()
    )

    def do_create(token: str):
        option = lark.RequestOption.builder().user_access_token(token).build()
        return client.bitable.v1.app_table_record.create(request, option)

    response = _with_token_retry(do_create)

    if not response.success():
        error_msg = f"创建记录失败: {_parse_field_error(response, notice_type, fields)}"
        log_error(f"飞书创建记录: {error_msg}")
        return False, error_msg

    record_id = response.data.record.record_id
    log_info(f"飞书创建记录: 成功, record_id={record_id}")
    _log_record_action("上传", notice_type, record_id, fields, data_source_text)
    _send_robot_message(handler, payload)
    return True, record_id


def create_bitable_record_by_payload(notice_type: str, payload: NoticePayload):
    """
    通过 NoticePayload 创建多维表记录
    """
    check_token_status()

    if not config.user_token:
        return False, "未配置飞书用户令牌"

    handler, table_id, err = _resolve_handler(notice_type)
    if err:
        return False, err

    fields = handler.build_create_fields(payload)
    log_info(f"Creating record({notice_type}) with fields: {fields}")

    client = _build_client()
    request = (
        CreateAppTableRecordRequest.builder()
        .app_token(config.app_token)
        .table_id(table_id)
        .request_body(AppTableRecord.builder().fields(fields).build())
        .build()
    )

    def do_create(token: str):
        option = lark.RequestOption.builder().user_access_token(token).build()
        return client.bitable.v1.app_table_record.create(request, option)

    response = _with_token_retry(do_create)

    if not response.success():
        error_msg = f"创建记录失败: {_parse_field_error(response, notice_type, fields)}"
        log_error(f"飞书创建记录: {error_msg}")
        return False, error_msg

    record_id = response.data.record.record_id
    log_info(f"飞书创建记录: 成功, record_id={record_id}")
    _log_record_action("上传", notice_type, record_id, fields, payload.text)
    _send_robot_message(handler, payload)
    return True, record_id


def query_record_by_id(record_id, notice_type):
    """
    根据 Record ID 查询记录
    :return: (success, record_data or error_msg)
    """
    if not config.user_token:
        return False, "未配置飞书用户令牌"

    handler, table_id, err = _resolve_handler(notice_type)
    if err:
        return False, err

    client = _build_client()
    request = (
        GetAppTableRecordRequest.builder()
        .app_token(config.app_token)
        .table_id(table_id)
        .record_id(record_id)
        .build()
    )

    def do_query(token: str):
        option = lark.RequestOption.builder().user_access_token(token).build()
        return client.bitable.v1.app_table_record.get(request, option)

    response = _with_token_retry(do_query)

    if not response.success():
        error_msg = f"查询记录失败: {response.code} - {response.msg}"
        log_error(f"飞书查询记录: {error_msg}")
        return False, error_msg

    record = response.data.record
    record_data = {"record_id": record.record_id, "fields": record.fields}
    log_info(f"飞书查询记录: 成功, record_id={record.record_id}")
    return True, record_data


def delete_bitable_record(record_id: str, notice_type: str):
    """
    Delete a target bitable record by Record ID.
    Missing records are treated as already deleted so local cleanup can proceed.
    """
    check_token_status()

    record_id = str(record_id or "").strip()
    if not record_id:
        return False, "缺少 record_id"
    if not config.user_token:
        return False, "未配置飞书用户令牌"

    _handler, table_id, err = _resolve_handler(notice_type)
    if err:
        return False, err

    client = _build_client()
    request = (
        DeleteAppTableRecordRequest.builder()
        .app_token(config.app_token)
        .table_id(table_id)
        .record_id(record_id)
        .build()
    )

    def do_delete(token: str):
        option = lark.RequestOption.builder().user_access_token(token).build()
        return client.bitable.v1.app_table_record.delete(request, option)

    response = _with_token_retry(do_delete)

    if not response.success():
        if response.code == 1254043:
            log_info(f"飞书删除记录: 记录已不存在, record_id={record_id}")
            return True, record_id
        error_msg = f"删除记录失败: {response.code} - {response.msg}"
        log_error(f"飞书删除记录: {error_msg}")
        return False, error_msg

    log_info(f"飞书删除记录: 成功, record_id={record_id}")
    _log_record_action("删除", notice_type, record_id, {}, "")
    return True, record_id


def update_bitable_record(
    record_id,
    data_source_text,
    notice_type,
    buildings=None,
    specialty=None,
    event_source=None,
    new_file_tokens=None,
    extra_file_tokens=None,
    existing_file_tokens=None,
    existing_extra_file_tokens=None,
    response_time="",
    existing_response_time="",
    level=None,
    recover=False,
):
    """
    更新多维表格记录
    :return: (success, result or error_msg)
    """
    check_token_status()

    if not config.user_token:
        return False, "未配置飞书用户令牌"

    handler, table_id, err = _resolve_handler(notice_type)
    if err:
        return False, err

    payload = NoticePayload(
        text=data_source_text,
        level=level,
        buildings=buildings,
        specialty=specialty,
        event_source=event_source,
        file_tokens=new_file_tokens,
        existing_file_tokens=existing_file_tokens,
        extra_file_tokens=extra_file_tokens,
        existing_extra_file_tokens=existing_extra_file_tokens,
        response_time=response_time,
        existing_response_time=existing_response_time,
        recover=recover,
    )
    fields = handler.build_update_fields(payload)
    log_info(f"Updating record({notice_type}) {record_id} with fields: {fields}")

    client = _build_client()
    request = (
        UpdateAppTableRecordRequest.builder()
        .app_token(config.app_token)
        .table_id(table_id)
        .record_id(record_id)
        .request_body(AppTableRecord.builder().fields(fields).build())
        .build()
    )

    def do_update(token: str):
        option = lark.RequestOption.builder().user_access_token(token).build()
        return client.bitable.v1.app_table_record.update(request, option)

    response = _with_token_retry(do_update)

    if not response.success():
        error_msg = f"更新记录失败: {_parse_field_error(response, notice_type, fields)}"
        log_error(f"飞书更新记录: {error_msg}")
        return False, error_msg

    log_info(f"飞书更新记录: 成功, record_id={record_id}")
    action = "结束" if _extract_status_from_fields(fields) == "结束" else "更新"
    _log_record_action(action, notice_type, record_id, fields, data_source_text)
    _send_robot_message(handler, payload)
    return True, record_id


def update_bitable_record_by_payload(record_id: str, notice_type: str, payload: NoticePayload):
    """
    通过 NoticePayload 更新多维表记录
    """
    check_token_status()

    if not config.user_token:
        return False, "未配置飞书用户令牌"

    handler, table_id, err = _resolve_handler(notice_type)
    if err:
        return False, err

    fields = handler.build_update_fields(payload)
    log_info(f"Updating record({notice_type}) {record_id} with fields: {fields}")

    client = _build_client()
    request = (
        UpdateAppTableRecordRequest.builder()
        .app_token(config.app_token)
        .table_id(table_id)
        .record_id(record_id)
        .request_body(AppTableRecord.builder().fields(fields).build())
        .build()
    )

    def do_update(token: str):
        option = lark.RequestOption.builder().user_access_token(token).build()
        return client.bitable.v1.app_table_record.update(request, option)

    response = _with_token_retry(do_update)

    if not response.success():
        error_msg = f"更新记录失败: {_parse_field_error(response, notice_type, fields)}"
        log_error(f"飞书更新记录: {error_msg}")
        return False, error_msg

    log_info(f"飞书更新记录: 成功, record_id={record_id}")
    action = "结束" if _extract_status_from_fields(fields) == "结束" else "更新"
    _log_record_action(action, notice_type, record_id, fields, payload.text)
    _send_robot_message(handler, payload)
    return True, record_id


def update_bitable_record_fields(record_id: str, notice_type: str, fields: dict):
    """
    Directly update partial fields on a bitable record without rebuilding NoticePayload.
    :return: (success, record_id or error_msg)
    """
    check_token_status()

    if not config.user_token:
        return False, "未配置飞书用户令牌"

    if not isinstance(fields, dict) or not fields:
        return False, "更新字段不能为空"

    handler, table_id, err = _resolve_handler(notice_type)
    if err:
        return False, err

    log_info(f"Updating record fields({notice_type}) {record_id} with fields: {fields}")

    client = _build_client()
    request = (
        UpdateAppTableRecordRequest.builder()
        .app_token(config.app_token)
        .table_id(table_id)
        .record_id(record_id)
        .request_body(AppTableRecord.builder().fields(fields).build())
        .build()
    )

    def do_update(token: str):
        option = lark.RequestOption.builder().user_access_token(token).build()
        return client.bitable.v1.app_table_record.update(request, option)

    response = _with_token_retry(do_update)

    if not response.success():
        error_msg = f"更新记录失败: {_parse_field_error(response, notice_type, fields)}"
        log_error(f"飞书更新记录字段: {error_msg}")
        return False, error_msg

    log_info(f"飞书更新记录字段: 成功, record_id={record_id}")
    _log_record_action("字段更新", notice_type, record_id, fields, "")
    return True, record_id
