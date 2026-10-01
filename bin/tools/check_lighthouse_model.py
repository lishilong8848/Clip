"""Opt-in model capability probe. Reads encrypted config; sends synthetic math only."""
import asyncio
import json
import os
import sqlite3
import sys
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ.setdefault("PYDANTIC_AI_NO_BANNER", "1")


async def main():
    from lan_bitable_template_portal.lighthouse_ai import CustomModel, unprotect_key
    from lan_bitable_template_portal.lighthouse_model import configured_model
    from pydantic_ai import Agent
    from pydantic_ai.usage import UsageLimits
    path = Path(__file__).resolve().parents[1] / "data" / "lan_portal_state.sqlite3"
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as conn:
        row = conn.execute("SELECT payload_json FROM json_documents WHERE namespace=? AND key=?", ("lighthouse_ai", "model")).fetchone()
    if not row:
        print("Model probe: no configured profile")
        return 2
    saved = json.loads(row[0])
    profile = CustomModel._default(saved)
    if not profile or not profile.get("key_cipher"):
        print("Model probe: credentials not configured")
        return 2
    calls, chunks = [], 0
    try:
        custom = SimpleNamespace(unprotect=unprotect_key, _endpoint=CustomModel._endpoint)
        async with configured_model(custom, profile) as model:
            agent = Agent(model, instructions="This is a compatibility test. Call local_sum once, then give its numeric result.", name="lighthouse_probe")
            @agent.tool_plain
            def local_sum(a: int, b: int) -> int:
                """Add two synthetic numbers. Does not read or write any business data."""
                calls.append((a, b))
                return a + b
            async with asyncio.timeout(90):
                async with agent.run_stream_events("Call local_sum for 23 plus 19.", model_settings={"max_tokens": 500, "parallel_tool_calls": False}, usage_limits=UsageLimits(request_limit=3)) as events:
                    async for event in events:
                        if event.event_kind == "part_delta":
                            chunks += 1
                        if event.event_kind == "agent_run_result":
                            if "42" not in str(event.result.output) or calls != [(23, 19)]:
                                print("Model probe: response did not satisfy the tool contract")
                                return 1
        print("Model probe: streaming and native tool calls OK; chunks=" + str(chunks))
        return 0
    except Exception as exc:
        print("Model probe failed: " + type(exc).__name__ + "; http_status=" + str(getattr(exc, "status_code", "unavailable")))
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
