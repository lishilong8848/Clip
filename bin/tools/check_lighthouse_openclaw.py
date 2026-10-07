"""Opt-in live runtime checks using synthetic credentials and temporary state."""
import asyncio
import copy
import json
import os
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lan_bitable_template_portal.lighthouse_gateway import GatewayClient
from lan_bitable_template_portal.lighthouse_runtime import OpenClawRuntime, account_key, build_configuration, free_port, runtime_files


class ProbeModel:
    def unprotect(self, _):
        return "synthetic-runtime-test-key"

    def settings(self):
        return {"enabled": True, "active_model_id": "probe", "models": [{"id": "probe", "configured": True}]}

    def profile(self, _):
        return {"id": "probe", "model": "probe", "name": "Probe", "endpoint": "http://127.0.0.1:1/v1/chat/completions", "key_cipher": "synthetic"}


class ProbeStore:
    def __init__(self, root):
        self.db_path, self.docs = Path(root) / "fixture.sqlite3", {}

    def get_document(self, space, key):
        return copy.deepcopy(self.docs.get((space, key)))

    def put_document(self, space, key, value):
        self.docs[space, key] = copy.deepcopy(value)


async def measure_responsiveness(stop, samples):
    import httpx
    from fastapi import FastAPI

    app = FastAPI()

    @app.get("/health")
    async def health():
        await asyncio.sleep(0)
        return {"ok": True}

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://probe") as client:
        while not stop.is_set():
            phase = samples["phase"]
            expected = time.monotonic() + .05
            await asyncio.sleep(.05)
            samples["lag"].append((max(0, time.monotonic() - expected), phase))
            started = time.monotonic()
            response = await client.get("/health")
            assert response.status_code == 200 and response.json() == {"ok": True}
            samples["http"].append(time.monotonic() - started)


async def check():
    os.environ["OPENCLAW_DEBUG"] = "1"
    os.environ["LIGHTHOUSE_RUNTIME_PROBE"] = "1"
    runtime_root = Path(sys.argv[sys.argv.index("--runtime-root") + 1]) if "--runtime-root" in sys.argv else None
    probe_root = Path(__file__).resolve().parents[2] / "build_output/agent_probes"
    probe_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lighthouse-openclaw-", dir=probe_root) as directory:
        manager = OpenClawRuntime(directory, max_accounts=1, runtime_root=runtime_root)
        engine = None
        monitor = None
        stop, samples = asyncio.Event(), {"lag": [], "http": [], "phase": "initialization"}
        try:
            if "--initialize" in sys.argv:
                root = Path(directory) / account_key("isolated-runtime-probe")
                root.mkdir()
                (root / "workspace").mkdir()
                profile = {"model": "probe", "name": "Probe", "endpoint": "http://127.0.0.1:1/v1/chat/completions", "key_cipher": "synthetic"}
                config_path = root / "openclaw.json"
                key = account_key('isolated-runtime-probe')
                config_path.write_text(json.dumps(build_configuration(root, {key: {'profile': profile, 'root': root,
                    'agent_id': 'lh-' + key, 'config_fingerprint': 'f' * 64}}, free_port(),
                    model_url='http://127.0.0.1:1/api/assistant/openclaw-models',
                    plugin=Path(__file__).resolve().parents[1] / 'openclaw_service/assistant/openclaw/plugin',
                    tool_names=["lighthouse_probe"])), encoding="utf-8")
                node, entry = runtime_files(runtime_root)
                process = await asyncio.create_subprocess_exec(str(node), str(entry), "doctor", "--fix", "--non-interactive",
                    env={**os.environ, "OPENCLAW_STATE_DIR": str(root), "OPENCLAW_CONFIG_PATH": str(config_path), "OPENCLAW_HOME": str(root),
                         "LIGHTHOUSE_MODEL_KEY": "synthetic-runtime-test-key", "LIGHTHOUSE_GATEWAY_TOKEN": "synthetic-test-token", "OPENCLAW_NO_RESPAWN": "1"},
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
                output, _ = await asyncio.wait_for(process.communicate(), 90)
                print(output.decode("utf-8", errors="replace")[-4500:])
                assert process.returncode == 0
            def diagnostic_config(*args, **kwargs):
                config = build_configuration(*args, **kwargs)
                config["logging"].update(level="info", consoleLevel="info")
                return config
            with patch("lan_bitable_template_portal.lighthouse_runtime.build_configuration", diagnostic_config):
                started = time.monotonic()
                if "--with-tools" in sys.argv:
                    if "--responsiveness" in sys.argv:
                        monitor = asyncio.create_task(measure_responsiveness(stop, samples))
                        while not samples["http"]:
                            if monitor.done():
                                await monitor
                            await asyncio.sleep(.01)
                    def create_engine():
                        # Match ready_streams: framework imports and service construction
                        # run in a worker before account warmup reaches the portal loop.
                        from pydantic_ai.ui.vercel_ai import response_types
                        from lan_bitable_template_portal.lighthouse_ai import LighthouseAssistant
                        from lan_bitable_template_portal.lighthouse_openclaw import LighthouseOpenClaw
                        store = ProbeStore(directory)
                        portal = SimpleNamespace(assistant=LighthouseAssistant(store, None, model=ProbeModel()), store=store)
                        return LighthouseOpenClaw(portal, state_root=directory, runtime_root=runtime_root,
                            bridge_url=lambda: "http://127.0.0.1:1/api/assistant/openclaw-tools")
                    samples["phase"] = "framework_import"
                    engine = await asyncio.to_thread(create_engine)
                    manager = engine.manager
                    actor = {"id": "isolated-runtime-probe", "scopes": ["D"]}
                    async def emit(*_):
                        pass
                    samples["phase"] = "gateway_startup"
                    engine.queue_warmup(actor)
                    await engine.wait_warmup(actor, emit)
                    item = manager.accounts[account_key(actor["id"])]
                    assert not item['busy'] and not engine.bridge.active
                    elapsed = time.monotonic() - started
                    samples["phase"] = "gateway_reuse"
                    started = time.monotonic()
                    await engine._warmup(actor)
                    assert manager.accounts[item['key']] is item
                    print("[LighthouseRuntime] Canonical tools warmup %.2fs; same-process reuse %.3fs." % (elapsed, time.monotonic() - started))
                else:
                    item = await manager.acquire({"id": "isolated-runtime-probe", "scopes": ["D"]}, ProbeModel(),
                        ProbeModel().profile("probe"), tool_names=["lighthouse_probe"])
                    print("[LighthouseRuntime] Cold startup %.2fs." % (time.monotonic() - started))
            async with GatewayClient("ws://127.0.0.1:" + str(item["port"]), item["token"]) as client:
                health = await client.request("health", {}, timeout=15)
                assert isinstance(health, dict) and health.get("ok") is True
                print("[LighthouseRuntime] Gateway v4 and health verified.")
            for file in Path(directory).rglob("*.json"):
                assert "synthetic-runtime-test-key" not in file.read_text(encoding="utf-8"), file.name
            print("[LighthouseRuntime] No plaintext model key in persisted JSON.")
            if monitor:
                stop.set()
                await monitor
                monitor = None
                (maximum_lag, phase), maximum_http = max(samples["lag"]), max(samples["http"])
                p95 = sorted(samples["http"])[int((len(samples["http"]) - 1) * .95)]
                print("[LighthouseRuntime] Same-loop health requests=%d; event-loop max lag=%.1fms (%s); HTTP p95=%.1fms, max=%.1fms." %
                      (len(samples["http"]), maximum_lag * 1000, phase, p95 * 1000, maximum_http * 1000))
                assert maximum_lag < .5, "Gateway startup blocked the portal event loop"
                assert maximum_http < .5, "Other routes stalled during gateway startup"
        except Exception:
            # All values in this probe are synthetic; production diagnostics never dump logs.
            for file in Path(directory).rglob("gateway.log"):
                print(file.read_text(encoding="utf-8", errors="replace")[-5000:])
            raise
        finally:
            stop.set()
            if monitor:
                await monitor
            await (engine.close() if engine else manager.close())


if __name__ == "__main__":
    asyncio.run(check())
