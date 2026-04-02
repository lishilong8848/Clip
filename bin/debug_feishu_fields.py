import sys
import os

# Add current directory to path to allow imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from upload_event_module.config import config
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *


def main():
    print("Loading config...")
    config.load()

    print(f"App ID: {config.app_id}")
    print(f"App Token: {config.app_token}")
    print(f"Table ID (Event Notice): {config.table_id_shijian}")

    if not config.app_token or not config.table_id_shijian:
        print("Error: Missing app_token or table_id_shijian in config.")
        return

    client = (
        lark.Client.builder()
        .app_id(config.app_id)
        .app_secret(config.app_secret)
        .enable_set_token(True)
        .build()
    )

    # Use user token if available (bitable usually requires user token for some ops, but list fields usually works with tenant if app is added)
    # But feishu_service uses user_token.
    # Let's try tenant token (default) first.
    # Or user_token if configured.

    print("Listing fields...")
    request = (
        ListAppTableFieldRequest.builder()
        .app_token(config.app_token)
        .table_id(config.table_id_shijian)
        .page_size(100)
        .build()
    )

    # Use tenant access token (internal app) logic usually handled by client default if app_id/secret provided.
    # But feishu_service uses user_token.

    option = None
    if config.user_token:
        # verify token valid?
        print("Using User Token from config.")
        option = (
            lark.RequestOption.builder().user_access_token(config.user_token).build()
        )

    resp = client.bitable.v1.app_table_field.list(request, option)

    if resp.success():
        print("Success! Fields found:")
        for field in resp.data.items:
            # 使用 repr() 避免编码问题，同时显示原文
            try:
                print(
                    f"- Name: {repr(field.field_name)}, Original: {field.field_name}, Type: {field.ui_type}"
                )
            except:
                print(f"- Name: {repr(field.field_name)}, Type: {field.ui_type}")
    else:
        print(f"Failed to list fields: Code {resp.code}, Msg: {resp.msg}")
        if resp.code == 99991663:  # Token invalid
            print("Token invalid. Please refresh token in app or update config.")


if __name__ == "__main__":
    main()
