import sys
import os
import json

# Add current directory to path to allow imports
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from upload_event_module.config import config
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *


def main():
    config.load()

    if not config.app_token or not config.table_id_shijian:
        print(
            json.dumps(
                {"error": "Missing app_token or table_id_shijian"},
                ensure_ascii=False,
                indent=2,
            )
        )
        return

    client = (
        lark.Client.builder()
        .app_id(config.app_id)
        .app_secret(config.app_secret)
        .enable_set_token(True)
        .build()
    )

    request = (
        ListAppTableFieldRequest.builder()
        .app_token(config.app_token)
        .table_id(config.table_id_shijian)
        .page_size(100)
        .build()
    )

    option = None
    if config.user_token:
        option = (
            lark.RequestOption.builder().user_access_token(config.user_token).build()
        )

    resp = client.bitable.v1.app_table_field.list(request, option)

    if resp.success():
        fields_data = []
        for field in resp.data.items:
            fields_data.append(
                {
                    "name": field.field_name,
                    "type": field.ui_type,
                    "field_id": field.field_id if hasattr(field, "field_id") else None,
                }
            )

        result = {
            "success": True,
            "table_id": config.table_id_shijian,
            "field_count": len(fields_data),
            "fields": fields_data,
        }
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        result = {"success": False, "error_code": resp.code, "error_msg": resp.msg}
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
