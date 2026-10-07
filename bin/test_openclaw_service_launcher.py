"""Unit tests for bin.openclaw_service.launcher.ManagedService (mocks only).

These tests never touch a real scheduler, real worker process, httpx network
calls, credentials, or cloud storage.  All external side effects are replaced
with mocks and every path uses temporary directories.
"""

import os
import sys as _sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

sys_path_dir = str(Path(__file__).resolve().parent)
if sys_path_dir not in _sys.path:
    _sys.path.insert(0, sys_path_dir)

from upload_event_module.services import process_lifetime as _process_lifetime
from openclaw_service import launcher
from openclaw_service.protocol import ServiceError


class _FakeHTTPError(Exception):
    """Stand-in for ``httpx.HTTPError`` (a plain Exception subclass) that does
    NOT swallow ServiceError, which inherits from RuntimeError."""


def _register_child_patch():
    """Patch the real Windows job-object registration so tests never call it."""
    return patch("upload_event_module.services.process_lifetime.register_child_process",
                 return_value=True)


def _make_response(status=200, ok=True):
    return SimpleNamespace(status_code=status, json=Mock(return_value={"ok": ok}))


def _make_httpx(post_return=None, post_error=None):
    """Return a fake ``httpx`` module and a collector for Client instances.

    ``post_error`` is injected into *every* Client that ``_stop_saved`` actually
    constructs (the launcher builds a fresh ``httpx.Client`` inside the request),
    so ``post`` will raise it and genuinely exercise the ``except httpx.HTTPError``
    path instead of being applied to a throwaway instance.
    """
    instances = []
    if post_return is None:
        post_return = _make_response(200, True)

    class FakeClient:
        def __init__(self, *_args, **_kwargs):
            if post_error is not None:
                self.post = Mock(side_effect=post_error)
            else:
                self.post = Mock(return_value=post_return)
            instances.append(self)

        def __enter__(self):
            return self

        def __exit__(self, *_exc):
            return False

    module = SimpleNamespace(Client=FakeClient, HTTPError=_FakeHTTPError)
    return module, instances


class _ManagedServiceBase(unittest.TestCase):
    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="openclaw_mgmt_")
        base = Path(self._tmp.name)
        self.project = base / "project"
        self.state = base / "state"
        self.project.mkdir(parents=True, exist_ok=True)
        self.state.mkdir(parents=True, exist_ok=True)
        self.addCleanup(self._tmp.cleanup)

    def make_svc(self, project=None, state=None):
        return launcher.ManagedService(project or self.project, state or self.state)

    def make_local_python(self, project=None):
        project = project or self.project
        py = project / "bin" / ".venv" / "Scripts" / "python.exe"
        py.parent.mkdir(parents=True, exist_ok=True)
        py.write_text("", encoding="utf-8")
        return py

    def make_running_proc(self, pid=9001):
        # A lightweight stand-in for a Popen handle: poll() reflects a mutable
        # ``done`` flag so tests can simulate a graceful stop finishing the
        # process (then the launcher should skip terminate()).
        class _FakePopen:
            def __init__(self):
                self.pid = pid
                self.done = False
                self.terminate = Mock()
                self.kill = Mock()
                self.wait = Mock(return_value=0)

            def poll(self):
                return 0 if self.done else None

        return _FakePopen()


class ManagedServiceStartTests(_ManagedServiceBase):
    def test_start_spawns_hidden_low_priority_local_python(self):
        local_python = self.make_local_python()
        svc = self.make_svc()
        proc = self.make_running_proc(pid=4242)
        with _register_child_patch() as register, \
                patch.object(launcher, "descriptor", return_value=None), \
                patch.object(launcher, "control_key", return_value="k"), \
                patch.object(launcher.subprocess, "Popen", return_value=proc) as popen:
            result = svc.start()
        self.assertTrue(result)
        popen.assert_called_once()
        args = popen.call_args.args[0]
        self.assertEqual(args[0], str(local_python))
        self.assertEqual(args[1:5], ["-B", "-u", "-m", "openclaw_service"])
        self.assertIn("--project-root", args)
        self.assertIn("--state-root", args)
        self.assertIn("--parent-pid", args)
        self.assertEqual(args[args.index("--project-root") + 1], str(svc.project))
        self.assertEqual(args[args.index("--state-root") + 1], str(svc.state))
        self.assertEqual(args[args.index("--parent-pid") + 1], str(os.getpid()))
        self.assertEqual(popen.call_args.kwargs["cwd"], svc.project)
        self.assertIs(popen.call_args.kwargs["stdin"], launcher.subprocess.DEVNULL)
        env = popen.call_args.kwargs["env"]
        self.assertEqual(env["PYTHONPATH"], str(svc.project / "bin"))
        self.assertEqual(env["CLIPFLOW_OPENCLAW_LOG_OWNER_PID"], str(os.getpid()))
        if os.name == "nt":
            flags = popen.call_args.kwargs["creationflags"]
            self.assertTrue(flags & launcher.subprocess.CREATE_NO_WINDOW)
            self.assertTrue(flags & launcher.subprocess.BELOW_NORMAL_PRIORITY_CLASS)
        register.assert_called_once_with(4242)

    def test_start_duplicate_does_not_respawn(self):
        self.make_local_python()
        svc = self.make_svc()
        proc = self.make_running_proc()
        with _register_child_patch(), \
                patch.object(launcher, "descriptor", return_value=None), \
                patch.object(launcher.subprocess, "Popen", return_value=proc) as popen:
            self.assertTrue(svc.start())
            self.assertFalse(svc.start())
        popen.assert_called_once()

    def test_start_closed_raises_service_stopping(self):
        self.make_local_python()
        svc = self.make_svc()
        svc.close()
        with patch.object(launcher.subprocess, "Popen") as popen:
            with self.assertRaises(ServiceError) as ctx:
                svc.start()
        self.assertEqual(ctx.exception.code, "service_stopping")
        popen.assert_not_called()

    def test_start_wrong_project_rejected(self):
        self.make_local_python()
        svc = self.make_svc()
        other = Path(self._tmp.name) / "other"
        with patch.object(launcher.subprocess, "Popen") as popen:
            with self.assertRaises(ServiceError) as ctx:
                svc.start(project=other)
        self.assertEqual(ctx.exception.code, "invalid_project")
        popen.assert_not_called()

    def test_start_update_hold_blocks_spawn(self):
        self.make_local_python()
        (self.state / "update-hold.json").write_text("{}", encoding="utf-8")
        svc = self.make_svc()
        with patch.object(launcher.subprocess, "Popen") as popen:
            with self.assertRaises(ServiceError) as ctx:
                svc.start()
        self.assertEqual(ctx.exception.code, "service_updating")
        popen.assert_not_called()

    def test_start_foreign_owner_rejected(self):
        self.make_local_python()
        svc = self.make_svc()
        saved = {
            "identity": "x", "port": 8080, "pid": 111,
            "instance": "a" * 32, "process_stamp": "p",
            "parent_pid": 999, "parent_stamp": "STAMP",
        }

        def stamp(pid):
            return "STAMP" if pid == 999 else None

        with _register_child_patch() as register, \
                patch.object(launcher, "descriptor", return_value=saved), \
                patch.object(launcher.ManagedService, "_stop_saved") as stop_saved, \
                patch.object(launcher, "process_stamp", side_effect=stamp), \
                patch.object(launcher.subprocess, "Popen") as popen:
            with self.assertRaises(ServiceError) as ctx:
                svc.start()
        self.assertEqual(ctx.exception.status, 409)
        self.assertEqual(ctx.exception.code, "service_owner_active")
        popen.assert_not_called()
        stop_saved.assert_not_called()
        register.assert_not_called()

    def test_start_migrates_only_old_detached_then_spawns(self):
        self.make_local_python()
        svc = self.make_svc()
        saved = {
            "identity": "x", "port": 8080, "pid": 111,
            "instance": "a" * 32, "process_stamp": "p",
        }  # no parent_pid/parent_stamp -> old detached host
        proc = self.make_running_proc()
        with _register_child_patch() as register, \
                patch.object(launcher, "descriptor", return_value=saved), \
                patch.object(launcher.ManagedService, "_stop_saved") as stop_saved, \
                patch.object(launcher.subprocess, "Popen", return_value=proc) as popen:
            result = svc.start()
        self.assertTrue(result)
        stop_saved.assert_called_once_with(saved, reason="update")
        popen.assert_called_once()
        register.assert_called_once_with(proc.pid)

    def test_start_frozen_without_local_python_fails(self):
        # No local python file is created on purpose; frozen builds cannot
        # fall back to a bundled interpreter.
        svc = self.make_svc()
        with patch.object(_sys, "frozen", True, create=True), \
                patch.object(launcher, "descriptor", return_value=None), \
                patch.object(launcher.subprocess, "Popen") as popen:
            with self.assertRaises(ServiceError) as ctx:
                svc.start()
        self.assertEqual(ctx.exception.code, "python_unavailable")
        popen.assert_not_called()

    def test_start_falls_back_to_sys_executable_without_local_python(self):
        svc = self.make_svc()
        proc = self.make_running_proc()
        with _register_child_patch(), \
                patch.object(_sys, "frozen", False, create=True), \
                patch.object(launcher, "descriptor", return_value=None), \
                patch.object(launcher.subprocess, "Popen", return_value=proc) as popen:
            result = svc.start()
        self.assertTrue(result)
        args = popen.call_args.args[0]
        self.assertEqual(args[0], str(Path(_sys.executable)))


class ManagedServiceCloseTests(_ManagedServiceBase):
    def test_close_during_startup_without_descriptor(self):
        self.make_local_python()
        svc = self.make_svc()
        proc = self.make_running_proc()
        with _register_child_patch(), \
                patch.object(launcher, "descriptor", return_value=None), \
                patch.object(launcher.subprocess, "Popen", return_value=proc):
            self.assertTrue(svc.start())
        svc.close()
        self.assertTrue(svc.closed)
        self.assertIsNone(svc.process)
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once()

    def test_close_graceful_stop_success_skips_terminate(self):
        self.make_local_python()
        svc = self.make_svc()
        proc = self.make_running_proc()
        with _register_child_patch(), \
                patch.object(launcher, "descriptor", return_value=None), \
                patch.object(launcher.subprocess, "Popen", return_value=proc):
            self.assertTrue(svc.start())
        saved = {
            "port": 8080, "pid": proc.pid,
            "instance": "a" * 32, "process_stamp": "p",
        }

        def graceful_stop(*_a, **_k):
            proc.done = True

        with patch.object(launcher, "descriptor", return_value=saved), \
                patch.object(launcher.ManagedService, "_stop_saved", side_effect=graceful_stop):
            svc.close()
        self.assertTrue(svc.closed)
        proc.terminate.assert_not_called()
        proc.kill.assert_not_called()

    def test_close_graceful_stop_failure_falls_back_to_terminate(self):
        self.make_local_python()
        svc = self.make_svc()
        proc = self.make_running_proc()
        with _register_child_patch(), \
                patch.object(launcher, "descriptor", return_value=None), \
                patch.object(launcher.subprocess, "Popen", return_value=proc):
            self.assertTrue(svc.start())
        saved = {
            "port": 8080, "pid": proc.pid,
            "instance": "a" * 32, "process_stamp": "p",
        }
        with patch.object(launcher, "descriptor", return_value=saved), \
                patch.object(launcher.ManagedService, "_stop_saved", side_effect=RuntimeError("boom")):
            svc.close()  # must not raise
        self.assertTrue(svc.closed)
        proc.terminate.assert_called_once()
        proc.wait.assert_called_once()

    def test_close_is_idempotent(self):
        self.make_local_python()
        svc = self.make_svc()
        proc = self.make_running_proc()
        with _register_child_patch(), \
                patch.object(launcher, "descriptor", return_value=None), \
                patch.object(launcher.subprocess, "Popen", return_value=proc):
            self.assertTrue(svc.start())
        svc.close()
        svc.close()  # second close must be a no-op, not raise
        self.assertTrue(svc.closed)
        self.assertIsNone(svc.process)
        proc.terminate.assert_called_once()
        proc.kill.assert_not_called()

    def test_concurrent_start_close_race_is_safe(self):
        self.make_local_python()
        svc = self.make_svc()
        barrier = threading.Barrier(6)
        errors = []
        lock = threading.Lock()

        def start_worker():
            try:
                barrier.wait()
                svc.start()
            except BaseException as exc:  # noqa: BLE001 - collected for assert
                with lock:
                    errors.append(exc)

        def close_worker():
            try:
                barrier.wait()
                svc.close()
            except BaseException as exc:  # noqa: BLE001 - collected for assert
                with lock:
                    errors.append(exc)

        workers = [threading.Thread(target=start_worker) for _ in range(4)]
        workers += [threading.Thread(target=close_worker) for _ in range(2)]
        with _register_child_patch(), \
                patch.object(launcher, "descriptor", return_value=None), \
                patch.object(launcher.subprocess, "Popen",
                             side_effect=lambda *a, **k: self.make_running_proc()):
            for w in workers:
                w.start()
            for w in workers:
                w.join(timeout=10)

        # The only acceptable failure is a start() racing a close() and observing
        # the already-closed state. Anything else (unexpected exception, state
        # corruption) is a real defect.
        for exc in errors:
            if isinstance(exc, ServiceError) and exc.code == "service_stopping":
                continue
            self.fail(f"unexpected exception during start/close race: {exc!r}")
        self.assertTrue(svc.closed)
        self.assertIsNone(svc.process)


class StopSavedTests(_ManagedServiceBase):
    def _saved(self):
        return {
            "pid": 111, "process_stamp": "p",
            "port": 8080, "instance": "a" * 32,
        }

    def test_stop_saved_success(self):
        svc = self.make_svc()
        saved = self._saved()
        httpx_mod, clients = _make_httpx(_make_response(200, True))
        with patch.dict(_sys.modules, {"httpx": httpx_mod}), \
                patch.object(launcher, "control_key", return_value="KEY"), \
                patch.object(launcher, "process_stamp", return_value=None), \
                patch.object(launcher, "time") as time_mock:
            time_mock.monotonic.side_effect = [0.0, 0.0]
            time_mock.sleep = Mock()
            svc._stop_saved(saved, reason="update")
        client = clients[0]
        client.post.assert_called_once()
        self.assertIn("/shutdown", client.post.call_args.args[0])
        body = client.post.call_args.kwargs["json"]
        # The server only accepts manual/update/system; migration must send update.
        self.assertEqual(body["reason"], "update")

    def test_stop_saved_http_failure_raises(self):
        svc = self.make_svc()
        saved = self._saved()
        httpx_mod, _clients = _make_httpx(_make_response(503, False))
        with patch.dict(_sys.modules, {"httpx": httpx_mod}), \
                patch.object(launcher, "control_key", return_value="KEY"), \
                patch.object(launcher, "process_stamp", return_value=None), \
                patch.object(launcher, "time") as time_mock:
            time_mock.monotonic.side_effect = [0.0, 0.0]
            time_mock.sleep = Mock()
            with self.assertRaises(ServiceError) as ctx:
                svc._stop_saved(saved, reason="update")
        self.assertEqual(ctx.exception.code, "service_stop_failed")

    def test_stop_saved_http_error_then_waits_and_raises(self):
        svc = self.make_svc()
        saved = self._saved()
        # Inject the error through the factory so the Client that _stop_saved
        # actually constructs inside its request will raise, not a throwaway.
        httpx_mod, clients = _make_httpx(post_error=_FakeHTTPError("conn reset"))
        with patch.dict(_sys.modules, {"httpx": httpx_mod}), \
                patch.object(launcher, "control_key", return_value="KEY"), \
                patch.object(launcher, "process_stamp", return_value="p"), \
                patch.object(launcher, "time") as time_mock:
            time_mock.monotonic.side_effect = [0.0, 100.0]
            time_mock.sleep = Mock()
            with self.assertRaises(ServiceError) as ctx:
                svc._stop_saved(saved, reason="update")
        self.assertEqual(ctx.exception.code, "service_stop_failed")
        # Prove the HTTPError path really ran: the real Client.post was attempted
        # and raised, so the launcher entered the except branch before giving up.
        self.assertTrue(clients, "no httpx.Client was constructed by _stop_saved")
        clients[0].post.assert_called_once()

    def test_stop_saved_http_error_but_stamp_gone_allows_stop(self):
        # The old process may close its listener before replying (HTTPError);
        # if its process_stamp has already disappeared, the stop is a success.
        svc = self.make_svc()
        saved = self._saved()
        httpx_mod, clients = _make_httpx(post_error=_FakeHTTPError("conn reset"))
        with patch.dict(_sys.modules, {"httpx": httpx_mod}), \
                patch.object(launcher, "control_key", return_value="KEY"), \
                patch.object(launcher, "process_stamp", return_value=None), \
                patch.object(launcher, "time") as time_mock:
            time_mock.monotonic.side_effect = [0.0, 100.0]
            time_mock.sleep = Mock()
            # Must not raise: stamp gone means the old host already exited.
            svc._stop_saved(saved, reason="update")
        self.assertTrue(clients, "no httpx.Client was constructed by _stop_saved")
        clients[0].post.assert_called_once()


class CredentialScrubbingTests(_ManagedServiceBase):
    def test_no_credentials_in_child_args_env_or_shutdown_payload(self):
        secret = "S3CR3T-DPAPI-KEY"
        self.make_local_python()
        svc = self.make_svc()
        saved = {
            "port": 8080, "pid": 10,
            "instance": "a" * 32, "process_stamp": "old-stamp",
        }  # detached -> triggers a real _stop_saved during start
        proc = self.make_running_proc(pid=1234)
        httpx_mod, clients = _make_httpx(_make_response(200, True))

        with patch.object(_sys, "frozen", False, create=True), \
                _register_child_patch(), \
                patch.dict(_sys.modules, {"httpx": httpx_mod}), \
                patch.object(launcher, "control_key", return_value=secret), \
                patch.object(launcher, "descriptor", return_value=saved), \
                patch.object(launcher, "process_stamp", return_value=None), \
                patch.object(launcher.subprocess, "Popen", return_value=proc) as popen:
            result = svc.start()

        self.assertTrue(result)
        client = clients[0]
        client.post.assert_called_once()
        url = client.post.call_args.args[0]
        body = client.post.call_args.kwargs["json"]
        headers = client.post.call_args.kwargs["headers"]

        # The credential is allowed in exactly one place: the Authorization
        # header used to authenticate the local shutdown call.
        self.assertEqual(headers["Authorization"], "Bearer " + secret)
        self.assertNotIn(secret, url)
        self.assertNotIn(secret, str(body))

        # It must never appear in the child command line or its environment,
        # because those can end up in process listings / child logs.
        args = popen.call_args.args[0]
        env = popen.call_args.kwargs["env"]
        self.assertNotIn(secret, " ".join(args))
        self.assertNotIn(secret, " ".join(str(v) for v in env.values()))


if __name__ == "__main__":
    unittest.main()