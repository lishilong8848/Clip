"""One SDK long connection, isolated from the portal event loop and Qt."""
import argparse
import hashlib
import json
import logging
from pathlib import Path
import sys


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--callback", required=True)
    args = parser.parse_args()
    log = (args.config.parent / "feishu_assistant.log").open("a", encoding="utf-8", buffering=1)
    sys.stdout = sys.stderr = log
    from urllib.parse import urlsplit
    target = urlsplit(args.callback)
    if target.scheme != "http" or target.hostname != "127.0.0.1" or target.path != "/api/assistant/feishu-event":
        raise ValueError("Invalid private callback")
    from openclaw_service.assistant.lighthouse_ai import unprotect_key
    from openclaw_service.protocol import InstanceLock
    config = json.loads(args.config.read_text(encoding="utf-8"))
    secret, bridge = unprotect_key(config["secret_cipher"]), unprotect_key(config["bridge_cipher"])
    lock = InstanceLock()
    lock.name = "Local\\ClipFlowFeishuAssistant-" + hashlib.sha256(config["app_id"].encode()).hexdigest()[:24]
    with lock:
        import httpx
        # The local callback never leaves loopback. TLS loading is not needed here.
        local = httpx.Client(trust_env=False, verify=False, timeout=5)
        cloud = httpx.Client(timeout=15)
        response = cloud.post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            json={"app_id": config["app_id"], "app_secret": secret}).json()
        if response.get("code") != 0 or not response.get("tenant_access_token"):
            raise RuntimeError("消息应用认证失败")
        bot = cloud.get("https://open.feishu.cn/open-apis/bot/v3/info/", headers={"Authorization": "Bearer " + response["tenant_access_token"]}).json()
        bot_id = (bot.get("bot") or bot.get("data", {}).get("bot") or {}).get("open_id")
        if bot.get("code") != 0 or not bot_id:
            raise RuntimeError("请为飞书应用启用机器人能力并发布应用")
        cloud.close()
        headers = {"Authorization": "Bearer " + bridge}
        local.post(args.callback, headers=headers, json={"kind": "ready", "bot_id": bot_id}).raise_for_status()
        import lark_oapi as lark

        def received(data):
            # Acknowledge only after the portal's durable inbox has accepted the event.
            result = local.post(args.callback, headers=headers, content=lark.JSON.marshal(data).encode("utf-8"))
            result.raise_for_status()

        handler = lark.EventDispatcherHandler.builder("", "").register_p2_im_message_receive_v1(received).build()
        class SafeLogs(logging.Filter):
            def filter(self, record):
                text = record.getMessage().replace(secret, "[redacted]").replace(bridge, "[redacted]")
                import re
                record.msg = re.sub(r"wss://\S+", "[飞书长连接]", text)
                record.args = ()
                return True
        from lark_oapi.core.log import logger
        logger.addFilter(SafeLogs())
        print("[ClipFlow][FeishuAssistant] 开始连接飞书智能体；请订阅 im.message.receive_v1，群聊需 @机器人", flush=True)
        try:
            lark.ws.Client(config["app_id"], secret, event_handler=handler, log_level=lark.LogLevel.INFO).start()
        finally:
            local.close()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("[ClipFlow][FeishuAssistant] 长连接启动未完成，请检查机器人能力、事件订阅及网络；错误类型=" + type(exc).__name__, flush=True)
        sys.exit(1)
