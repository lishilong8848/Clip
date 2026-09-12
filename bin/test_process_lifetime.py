import os
import subprocess
import sys
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


BIN_DIR = Path(__file__).resolve().parent
if str(BIN_DIR) not in sys.path:
    sys.path.insert(0, str(BIN_DIR))

from upload_event_module.services.process_lifetime import cleanup_orphaned_processes


def _process_alive(pid: int) -> bool:
    if os.name != "nt":
        return False
    import win32api
    import win32con

    try:
        process = win32api.OpenProcess(win32con.SYNCHRONIZE, False, pid)
    except Exception:
        return False
    try:
        return win32api.WaitForSingleObject(process, 0) == 0x102
    finally:
        process.Close()


def _terminate_process(pid: int) -> None:
    if not _process_alive(pid):
        return
    import win32api
    import win32con

    process = win32api.OpenProcess(win32con.PROCESS_TERMINATE, False, pid)
    try:
        win32api.TerminateProcess(process, 0)
    finally:
        process.Close()


@unittest.skipUnless(os.name == "nt", "Windows process lifetime behavior")
class ProcessLifetimeTests(unittest.TestCase):
    def test_cleanup_only_terminates_matching_orphans(self):
        rows = [
            SimpleNamespace(
                ProcessId=100,
                ParentProcessId=1,
                CreationDate="20260912090000",
                CommandLine="ClipFlow.exe",
            ),
            SimpleNamespace(
                ProcessId=200,
                ParentProcessId=100,
                CreationDate="20260912100000",
                CommandLine="pythonw.exe 剪贴板监听测试.py",
            ),
            SimpleNamespace(
                ProcessId=201,
                ParentProcessId=999,
                CreationDate="20260912100000",
                CommandLine="pythonw.exe 剪贴板监听测试.py",
            ),
            SimpleNamespace(
                ProcessId=202,
                ParentProcessId=999,
                CreationDate="20260912100000",
                CommandLine="pythonw.exe unrelated.py",
            ),
        ]
        service = Mock()
        service.ExecQuery.return_value = rows
        handle = Mock()
        with patch("pythoncom.CoInitialize") as co_initialize, patch(
            "pythoncom.CoUninitialize"
        ) as co_uninitialize, patch(
            "win32com.client.GetObject", return_value=service
        ), patch(
            "win32api.OpenProcess", return_value=handle
        ) as open_process, patch("win32api.TerminateProcess") as terminate:
            self.assertEqual(
                cleanup_orphaned_processes("剪贴板监听测试.py"),
                [201],
            )
        open_process.assert_called_once_with(0x0001, False, 201)
        terminate.assert_called_once_with(handle, 0)
        handle.Close.assert_called_once_with()
        self.assertEqual(co_initialize.call_count, 2)
        self.assertEqual(co_uninitialize.call_count, 2)

    def test_cleanup_does_not_terminate_reused_pid(self):
        original = SimpleNamespace(
            ProcessId=201,
            ParentProcessId=999,
            CreationDate="20260912100000",
            CommandLine="pythonw.exe upload_event_module/ui/剪贴板监听测试.py",
        )
        replacement = SimpleNamespace(
            ProcessId=201,
            ParentProcessId=300,
            CreationDate="20260912110000",
            CommandLine="pythonw.exe upload_event_module/ui/剪贴板监听测试.py",
        )
        service = Mock()
        service.ExecQuery.side_effect = [[original], [replacement]]
        handle = Mock()
        with patch("pythoncom.CoInitialize"), patch(
            "pythoncom.CoUninitialize"
        ), patch("win32com.client.GetObject", return_value=service), patch(
            "win32api.OpenProcess", return_value=handle
        ), patch("win32api.TerminateProcess") as terminate:
            self.assertEqual(
                cleanup_orphaned_processes(
                    "upload_event_module/ui/剪贴板监听测试.py"
                ),
                [],
            )
        terminate.assert_not_called()
        handle.Close.assert_called_once_with()

    def test_job_kills_registered_child_when_owner_exits(self):
        script = (
            "import os,subprocess,sys;"
            "from upload_event_module.services.process_lifetime import register_child_process;"
            "p=subprocess.Popen([sys.executable,'-c','import time;time.sleep(30)']);"
            "print(f'{p.pid}:{int(register_child_process(p.pid))}',flush=True);"
            "os._exit(0)"
        )
        owner = subprocess.Popen(
            [sys.executable, "-c", script],
            cwd=BIN_DIR,
            stdout=subprocess.PIPE,
            text=True,
        )
        child_pid = 0
        try:
            line = owner.stdout.readline().strip()
            owner.stdout.close()
            owner.wait(timeout=5)
            child_pid, registered = (int(value) for value in line.split(":"))
            if not registered:
                self.skipTest("current Windows host does not allow nested Job assignment")
            deadline = time.monotonic() + 5
            while _process_alive(child_pid) and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertFalse(_process_alive(child_pid))
        finally:
            if owner.poll() is None:
                owner.kill()
            if owner.stdout and not owner.stdout.closed:
                owner.stdout.close()
            if child_pid:
                _terminate_process(child_pid)

    def test_parent_watchdog_exits_child(self):
        parent = subprocess.Popen([sys.executable, "-c", "import time;time.sleep(30)"])
        script = (
            "import sys,time;"
            "from upload_event_module.services.process_lifetime import start_parent_exit_watchdog;"
            "print(int(start_parent_exit_watchdog(int(sys.argv[1]))),flush=True);"
            "time.sleep(30)"
        )
        child = subprocess.Popen(
            [sys.executable, "-c", script, str(parent.pid)],
            cwd=BIN_DIR,
            stdout=subprocess.PIPE,
            text=True,
        )
        try:
            self.assertEqual(child.stdout.readline().strip(), "1")
            child.stdout.close()
            parent.terminate()
            parent.wait(timeout=5)
            child.wait(timeout=5)
            self.assertEqual(child.returncode, 0)
        finally:
            if parent.poll() is None:
                parent.kill()
            if child.poll() is None:
                child.kill()
            if child.stdout and not child.stdout.closed:
                child.stdout.close()


if __name__ == "__main__":
    unittest.main()
