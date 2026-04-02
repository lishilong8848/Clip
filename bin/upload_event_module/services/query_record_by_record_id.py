import os
import json
import requests
import sys
import urllib.parse
import time
from typing import Dict, Any, Tuple, Optional

# === input params start
app_id = os.getenv("APP_ID")               # app_id, required, 应用 ID
# 应用唯一标识，创建应用后获得。有关app_id 的详细介绍。请参考通用参数https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology。
app_secret = os.getenv("APP_SECRET")       # app_secret, required, 应用 secret
# 应用秘钥，创建应用后获得。有关 app_secret 的详细介绍，请参考https://open.feishu.cn/document/ukTMukTMukTM/uYTM5UjL2ETO14iNxkTN/terminology。
base_url = os.getenv("BASE_URL")          # string, required, 多维表格 URL
# 多维表格的完整URL，用于解析出app_token和table_id。URL格式可以是feishu.cn/base或feishu.cn/wiki开头。
record_id = os.getenv("RECORD_ID")        # string, required, 记录ID
# 多维表格中一条记录的唯一标识。
user_id_type = os.getenv("USER_ID_TYPE", "open_id")  # string, optional, 用户ID类型
# 指定返回的用户ID类型，可选值：open_id、union_id、user_id。默认为open_id。
# === input params end

REQUEST_TIMEOUT = (5, 15)
REQUEST_RETRY_TIMES = 2


def _request_with_retry(method, url, **kwargs):
    last_error = None
    for attempt in range(REQUEST_RETRY_TIMES + 1):
        try:
            return method(url, timeout=REQUEST_TIMEOUT, **kwargs)
        except Exception as exc:
            last_error = exc
            if attempt >= REQUEST_RETRY_TIMES:
                raise
            time.sleep(0.5 * (attempt + 1))
    raise last_error

def get_tenant_access_token(app_id: str, app_secret: str) -> Tuple[str, Exception]:
    """获取 tenant_access_token

    Args:
        app_id: 应用ID
        app_secret: 应用密钥

    Returns:
        Tuple[str, Exception]: (access_token, error)
    """
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = {
        "app_id": app_id,
        "app_secret": app_secret
    }
    headers = {
        "Content-Type": "application/json; charset=utf-8"
    }
    try:
        print(f"POST: {url}")
        print(f"Request body: {json.dumps(payload)}")
        response = _request_with_retry(
            requests.post, url, json=payload, headers=headers
        )
        response.raise_for_status()

        result = response.json()
        print(f"Response: {json.dumps(result)}")

        if result.get("code", 0) != 0:
            print(f"ERROR: failed to get tenant_access_token: {result.get('msg', 'unknown error')}", file=sys.stderr)
            return "", Exception(f"failed to get tenant_access_token: {response.text}")

        return result["tenant_access_token"], None

    except Exception as e:
        print(f"ERROR: getting tenant_access_token: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"ERROR: Response: {e.response.text}", file=sys.stderr)
        return "", e

def get_wiki_node_info(tenant_access_token: str, node_token: str) -> Dict[str, Any]:
    """获取知识空间节点信息

    Args:
        tenant_access_token: 租户访问令牌
        node_token: 节点令牌

    Returns:
        Dict[str, Any]: 节点信息对象
    """
    url = f"https://open.feishu.cn/open-apis/wiki/v2/spaces/get_node?token={urllib.parse.quote(node_token)}"
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    try:
        print(f"GET: {url}")
        response = _request_with_retry(requests.get, url, headers=headers)
        response.raise_for_status()

        result = response.json()
        print(f"Response: {json.dumps(result)}")
        
        if result.get("code", 0) != 0:
            print(f"ERROR: 获取知识空间节点信息失败 {result}", file=sys.stderr)
            raise Exception(f"failed to get wiki node info: {result.get('msg', 'unknown error')}")

        if not result.get("data") or not result["data"].get("node"):
            raise Exception("未获取到节点信息")

        node_info = result["data"]["node"]
        print("节点信息获取成功:", {
            "node_token": node_info.get("node_token"),
            "obj_type": node_info.get("obj_type"),
            "obj_token": node_info.get("obj_token"),
            "title": node_info.get("title")
        })
        return node_info

    except Exception as e:
        print(f"ERROR: getting wiki node info: {e}", file=sys.stderr)
        raise

def parse_base_url(tenant_access_token: str, base_url_string: str) -> Dict[str, Optional[str]]:
    """解析多维表格参数

    Args:
        tenant_access_token: 租户访问令牌
        base_url_string: 基础URL字符串

    Returns:
        Dict[str, Optional[str]]: 包含appToken、tableID、viewID的字典
    """
    from urllib.parse import urlparse, parse_qs

    parsed_url = urlparse(base_url_string)
    pathname = parsed_url.path
    app_token = pathname.split("/")[-1]

    if "/wiki/" in pathname:
        node_info = get_wiki_node_info(tenant_access_token, app_token)
        app_token = node_info.get("obj_token", app_token)

    query_params = parse_qs(parsed_url.query)
    view_id = query_params.get("view", [None])[0]
    table_id = query_params.get("table", [None])[0]

    return {
        "app_token": app_token,
        "table_id": table_id,
        "view_id": view_id
    }

def get_bitable_record(tenant_access_token: str, app_token: str, table_id: str, record_id: str, user_id_type: str = "open_id") -> Tuple[Dict[str, Any], Exception]:
    """根据record_id查询多维表格记录

    Args:
        tenant_access_token: 租户访问令牌
        app_token: 多维表格App的唯一标识
        table_id: 数据表的唯一标识
        record_id: 记录的唯一标识
        user_id_type: 用户ID类型，默认为open_id

    Returns:
        Tuple[Dict[str, Any], Exception]: (记录数据, 错误)
    """
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
    params = {
        "user_id_type": user_id_type
    }
    headers = {
        "Authorization": f"Bearer {tenant_access_token}",
        "Content-Type": "application/json; charset=utf-8"
    }

    try:
        print(f"GET: {url}")
        print(f"Params: {json.dumps(params)}")
        response = _request_with_retry(
            requests.get, url, headers=headers, params=params
        )
        response.raise_for_status()

        result = response.json()
        print(f"Response: {json.dumps(result)}")

        if result.get("code", 0) != 0:
            print(f"ERROR: 查询记录失败: {result.get('msg', 'unknown error')}", file=sys.stderr)
            return {}, Exception(f"failed to get record: {response.text}")

        return result["data"], None

    except Exception as e:
        print(f"ERROR: getting bitable record: {e}", file=sys.stderr)
        if hasattr(e, 'response') and e.response is not None:
            print(f"ERROR: Response: {e.response.text}", file=sys.stderr)
        return {}, e

if __name__ == "__main__":
    # 验证必要参数
    if not app_id:
        print("ERROR: APP_ID is required", file=sys.stderr)
        exit(1)
    if not app_secret:
        print("ERROR: APP_SECRET is required", file=sys.stderr)
        exit(1)
    if not base_url:
        print("ERROR: BASE_URL is required", file=sys.stderr)
        exit(1)
    if not record_id:
        print("ERROR: RECORD_ID is required", file=sys.stderr)
        exit(1)

    # 获取 tenant_access_token
    tenant_access_token, err = get_tenant_access_token(app_id, app_secret)
    if err:
        print(f"ERROR: getting tenant_access_token: {err}", file=sys.stderr)
        exit(1)

    # 解析多维表格参数
    try:
        bitable_params = parse_base_url(tenant_access_token, base_url)
        app_token = bitable_params["app_token"]
        table_id = bitable_params["table_id"]
        
        if not table_id:
            print("ERROR: 无法从URL中获取table_id，请确保URL包含table参数", file=sys.stderr)
            exit(1)
            
        print(f"解析参数成功: app_token={app_token}, table_id={table_id}")
    except Exception as e:
        print(f"ERROR: 解析多维表格参数失败: {e}", file=sys.stderr)
        exit(1)

    # 查询记录
    record_data, err = get_bitable_record(tenant_access_token, app_token, table_id, record_id, user_id_type)
    if err:
        print(f"ERROR: 查询记录失败: {err}", file=sys.stderr)
        exit(1)

    print("查询记录成功:")
    print(json.dumps(record_data, indent=2, ensure_ascii=False))
