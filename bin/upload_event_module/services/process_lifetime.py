# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import threading


_job_lock = threading.Lock()
_job_handle = None
_job_closed = False


def _query_windows_processes(query: str, fields: tuple[str, ...]) -> list[dict]:
    import pythoncom
    import win32com.client

    pythoncom.CoInitialize()
    service = None
    rows = None
    row = None
    try:
        service = win32com.client.GetObject("winmgmts:")
        rows = service.ExecQuery(query)
        return [
            {field: getattr(row, field, None) for field in fields}
            for row in rows
        ]
    finally:
        row = None
        rows = None
        service = None
        pythoncom.CoUninitialize()


def _get_child_job():
    global _job_handle
    if os.name != "nt":
        return None
    with _job_lock:
        if _job_closed:
            return None
        if _job_handle is None:
            import win32job

            job = win32job.CreateJobObject(None, "")
            info = win32job.QueryInformationJobObject(
                job,
                win32job.JobObjectExtendedLimitInformation,
            )
            info["BasicLimitInformation"]["LimitFlags"] |= (
                win32job.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
            )
            win32job.SetInformationJobObject(
                job,
                win32job.JobObjectExtendedLimitInformation,
                info,
            )
            _job_handle = job
        return _job_handle


def register_child_process(pid: int) -> bool:
    """Tie a child PID to this process on Windows."""
    if os.name != "nt":
        return True
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    if pid <= 0 or pid == os.getpid():
        return False
    try:
        import win32api
        import win32con
        import win32job

        job = _get_child_job()
        if job is None:
            return False
        process = win32api.OpenProcess(
            win32con.PROCESS_SET_QUOTA | win32con.PROCESS_TERMINATE,
            False,
            pid,
        )
        try:
            win32job.AssignProcessToJobObject(job, process)
        finally:
            process.Close()
        return True
    except Exception:
        return False


def close_child_process_job() -> None:
    """Close the job once; Windows then terminates any remaining children."""
    global _job_handle, _job_closed
    if os.name != "nt":
        return
    with _job_lock:
        job = _job_handle
        _job_handle = None
        _job_closed = True
    if job is not None:
        try:
            job.Close()
        except Exception:
            pass


def cleanup_orphaned_processes(command_marker: str) -> list[int]:
    """Terminate only matching Windows processes whose original parent is gone."""
    marker = str(command_marker or "").strip().replace("\\", "/").casefold()
    if os.name != "nt" or not marker:
        return []
    terminated: list[int] = []
    try:
        import win32api

        rows = _query_windows_processes(
            "SELECT ProcessId, ParentProcessId, CreationDate, CommandLine "
            "FROM Win32_Process",
            ("ProcessId", "ParentProcessId", "CreationDate", "CommandLine"),
        )
        by_pid = {int(row["ProcessId"]): row for row in rows}
        for row in rows:
            try:
                pid = int(row["ProcessId"])
                command_line = (
                    str(row.get("CommandLine") or "")
                    .replace("\\", "/")
                    .casefold()
                )
                if pid == os.getpid() or marker not in command_line:
                    continue
                parent = by_pid.get(int(row.get("ParentProcessId") or 0))
                child_created = str(row.get("CreationDate") or "")
                parent_created = str((parent or {}).get("CreationDate") or "")
                if parent is not None and not (
                    child_created and parent_created and parent_created > child_created
                ):
                    continue
                process = win32api.OpenProcess(0x0001, False, pid)
                try:
                    current_rows = _query_windows_processes(
                        "SELECT ProcessId, CreationDate, CommandLine "
                        f"FROM Win32_Process WHERE ProcessId={pid}",
                        ("ProcessId", "CreationDate", "CommandLine"),
                    )
                    current = next(
                        (
                            item
                            for item in current_rows
                            if int(item.get("ProcessId") or 0) == pid
                        ),
                        {},
                    )
                    current_command = (
                        str(current.get("CommandLine") or "")
                        .replace("\\", "/")
                        .casefold()
                    )
                    if (
                        not current
                        or marker not in current_command
                        or str(current.get("CreationDate") or "") != child_created
                    ):
                        continue
                    win32api.TerminateProcess(process, 0)
                    terminated.append(pid)
                finally:
                    process.Close()
            except Exception:
                continue
    except Exception:
        pass
    return terminated


def windows_process_command_line(pid: int) -> str:
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return ""
    if os.name != "nt" or pid <= 0:
        return ""
    try:
        rows = _query_windows_processes(
            "SELECT CommandLine FROM Win32_Process "
            f"WHERE ProcessId={pid}",
            ("CommandLine",),
        )
        for row in rows:
            return str(row.get("CommandLine") or "")
    except Exception:
        pass
    return ""


def start_parent_exit_watchdog(parent_pid: int) -> bool:
    """Exit immediately when the exact Windows parent process object exits."""
    if os.name != "nt":
        return True
    try:
        parent_pid = int(parent_pid)
    except (TypeError, ValueError):
        return False
    if parent_pid <= 0 or parent_pid == os.getpid():
        return False
    try:
        import ctypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = [ctypes.c_ulong, ctypes.c_int, ctypes.c_ulong]
        kernel.OpenProcess.restype = ctypes.c_void_p
        kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_ulong]
        kernel.CloseHandle.argtypes = [ctypes.c_void_p]
        parent = kernel.OpenProcess(0x00100000, False, parent_pid)
        if not parent:
            return False
    except Exception:
        return False

    def wait_for_parent() -> None:
        try:
            kernel.WaitForSingleObject(parent, 0xFFFFFFFF)
        finally:
            kernel.CloseHandle(parent)
        close_child_process_job()
        os._exit(0)

    threading.Thread(
        target=wait_for_parent,
        name="ClipFlowParentExitWatchdog",
        daemon=True,
    ).start()
    return True
