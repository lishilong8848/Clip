"""Windows pressure sampling around the isolated native gateway acceptance."""
import asyncio
import ctypes as c
from ctypes import wintypes as w
import json
import os
from pathlib import Path
import statistics
import sys
import threading
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / 'bin/tools'), str(ROOT / 'bin')]
import win32api
import win32process

k = c.WinDLL('kernel32', use_last_error=True)
k.CreateToolhelp32Snapshot.argtypes = [w.DWORD, w.DWORD]
k.CreateToolhelp32Snapshot.restype = w.HANDLE
k.CloseHandle.argtypes = [w.HANDLE]


class ProcessEntry(c.Structure):
    _fields_ = [('size', w.DWORD), ('usage', w.DWORD), ('pid', w.DWORD),
        ('heap', c.c_size_t), ('module', w.DWORD), ('threads', w.DWORD),
        ('parent', w.DWORD), ('priority', w.LONG), ('flags', w.DWORD), ('name', w.WCHAR * 260)]


class IO(c.Structure):
    _fields_ = [(name, c.c_ulonglong) for name in ('reads', 'writes', 'others', 'read_bytes', 'write_bytes', 'other_bytes')]


class Memory(c.Structure):
    _fields_ = [('length', w.DWORD), ('load', w.DWORD)] + [(name, c.c_ulonglong) for name in
        ('total', 'available', 'total_page', 'available_page', 'total_virtual', 'available_virtual', 'extended')]


k.Process32FirstW.argtypes = [w.HANDLE, c.POINTER(ProcessEntry)]
k.Process32NextW.argtypes = k.Process32FirstW.argtypes
k.GetProcessIoCounters.argtypes = [w.HANDLE, c.POINTER(IO)]
k.GetSystemTimes.argtypes = [c.POINTER(w.FILETIME)] * 3
k.GlobalMemoryStatusEx.argtypes = [c.POINTER(Memory)]


def processes():
    snapshot = k.CreateToolhelp32Snapshot(2, 0)
    if snapshot == c.c_void_p(-1).value:
        raise c.WinError(c.get_last_error())
    entry, rows = ProcessEntry(), {}
    entry.size = c.sizeof(entry)
    try:
        okay = k.Process32FirstW(snapshot, c.byref(entry))
        while okay:
            rows[entry.pid] = {'parent': entry.parent, 'name': entry.name}
            okay = k.Process32NextW(snapshot, c.byref(entry))
    finally:
        k.CloseHandle(snapshot)
    return rows


def process_sample(pid):
    handle = win32api.OpenProcess(0x410, False, pid)
    try:
        times = win32process.GetProcessTimes(handle)
        memory = win32process.GetProcessMemoryInfo(handle)
        counters = IO()
        if not k.GetProcessIoCounters(int(handle), c.byref(counters)):
            raise c.WinError(c.get_last_error())
        return {'cpu': (times['KernelTime'] + times['UserTime']) / 10_000_000,
            'private_mib': memory['PagefileUsage'] / 1024**2,
            'working_mib': memory['WorkingSetSize'] / 1024**2,
            'page_faults': memory['PageFaultCount'],
            'read_bytes': counters.read_bytes, 'write_bytes': counters.write_bytes,
            'priority': win32process.GetPriorityClass(handle)}
    finally:
        handle.Close()


class CounterValue(c.Union):
    _fields_ = [('double', c.c_double), ('long', w.LONG), ('large', c.c_longlong)]


class FormattedCounter(c.Structure):
    _fields_ = [('status', w.DWORD), ('value', CounterValue)]


class Counters:
    def __init__(self):
        self.pdh = c.WinDLL('pdh')
        self.pdh.PdhOpenQueryW.argtypes = [w.LPCWSTR, c.c_size_t, c.POINTER(w.HANDLE)]
        self.pdh.PdhAddEnglishCounterW.argtypes = [w.HANDLE, w.LPCWSTR, c.c_size_t, c.POINTER(w.HANDLE)]
        self.pdh.PdhCollectQueryData.argtypes = [w.HANDLE]
        self.pdh.PdhGetFormattedCounterValue.argtypes = [w.HANDLE, w.DWORD, c.POINTER(w.DWORD), c.POINTER(FormattedCounter)]
        self.pdh.PdhCloseQuery.argtypes = [w.HANDLE]
        self.query, self.handles = w.HANDLE(), {}
        if self.pdh.PdhOpenQueryW(None, 0, c.byref(self.query)):
            raise OSError('Cannot open performance counters')
        paths = {'cpu_dpc_pct': r'\Processor(_Total)\% DPC Time',
            'cpu_interrupt_pct': r'\Processor(_Total)\% Interrupt Time',
            'hard_pages_per_sec': r'\Memory\Pages Input/sec',
            'disk_read_bytes_sec': r'\PhysicalDisk(_Total)\Disk Read Bytes/sec',
            'disk_write_bytes_sec': r'\PhysicalDisk(_Total)\Disk Write Bytes/sec',
            'disk_seconds_per_io': r'\PhysicalDisk(_Total)\Avg. Disk sec/Transfer',
            'disk_queue': r'\PhysicalDisk(_Total)\Current Disk Queue Length'}
        for name, path in paths.items():
            handle = w.HANDLE()
            if self.pdh.PdhAddEnglishCounterW(self.query, path, 0, c.byref(handle)) == 0:
                self.handles[name] = handle
        self.pdh.PdhCollectQueryData(self.query)

    def sample(self):
        self.pdh.PdhCollectQueryData(self.query)
        values = {}
        for name, handle in self.handles.items():
            result = FormattedCounter()
            if self.pdh.PdhGetFormattedCounterValue(handle, 0x200, None, c.byref(result)) == 0 and result.status in (0, 1):
                values[name] = result.value.double
        return values

    def close(self):
        self.pdh.PdhCloseQuery(self.query)


phase, samples, finished = 'baseline', [], threading.Event()


def monitor():
    before, prior, started = {}, None, time.monotonic()
    top_before, top_tick, last_top = {}, started, []
    counters = Counters()
    previous_tick = started
    try:
        while not finished.is_set():
            tick = time.monotonic()
            elapsed = max(.001, tick - previous_tick)
            previous_tick = tick
            table = processes()
            owned = {os.getpid()}
            while True:
                children = {pid for pid, row in table.items() if row['parent'] in owned}
                if children <= owned:
                    break
                owned |= children
            snapshot, owned_rows = {}, []
            if tick - top_tick >= 5:
                top_now, busy = {}, []
                for pid, row in table.items():
                    try:
                        handle = win32api.OpenProcess(0x1000, False, pid)
                        try:
                            times = win32process.GetProcessTimes(handle)
                            cpu = (times['KernelTime'] + times['UserTime']) / 10_000_000
                            top_now[pid] = cpu
                            busy.append({'pid': pid, 'name': row['name'], 'cpu_core_pct': round(
                                max(0, cpu-top_before.get(pid, cpu)) / (tick-top_tick)*100, 2)})
                        finally:
                            handle.Close()
                    except Exception:
                        pass
                top_before, top_tick = top_now, tick
                last_top = sorted(busy, key=lambda x:x['cpu_core_pct'], reverse=True)[:5]
            for pid in owned:
                row = table.get(pid)
                if not row:
                    continue
                try:
                    value = process_sample(pid)
                except Exception:
                    continue
                snapshot[pid] = value
                cpu = max(0, value['cpu'] - before.get(pid, value)['cpu']) / elapsed * 100
                if pid in owned:
                    old = before.get(pid, value)
                    owned_rows.append({'pid': pid, 'name': row['name'], **value,
                        'cpu_core_pct': round(cpu, 2),
                        'read_bytes_sec': max(0, value['read_bytes'] - old['read_bytes']) / elapsed,
                        'write_bytes_sec': max(0, value['write_bytes'] - old['write_bytes']) / elapsed})
            before = snapshot
            memory = Memory()
            memory.length = c.sizeof(memory)
            k.GlobalMemoryStatusEx(c.byref(memory))
            times = [w.FILETIME() for _ in range(3)]
            k.GetSystemTimes(*(c.byref(value) for value in times))
            system = [(v.dwHighDateTime << 32) | v.dwLowDateTime for v in times]
            cpu = None
            if prior:
                idle, kernel, user = [value - old for value, old in zip(system, prior)]
                cpu = (kernel + user - idle) / max(1, kernel + user) * 100
            prior = system
            samples.append({'time': round(tick - started, 3), 'phase': phase, 'cpu_total_pct': cpu,
                'available_mib': memory.available / 1024**2, 'committed_mib': (memory.total_page-memory.available_page)/1024**2,
                'sample_gap_ms': elapsed * 1000, 'sample_work_ms': (time.monotonic()-tick)*1000,
                **counters.sample(), 'owned': owned_rows, 'top': last_top})
            finished.wait(max(0, 1 - (time.monotonic() - tick)))
    finally:
        counters.close()


class Output:
    def __init__(self, original):
        self.original, self.buffer, self.queries = original, '', 0
    def write(self, text):
        global phase
        self.original.write(text)
        self.original.flush()
        self.buffer += text
        while '\n' in self.buffer:
            line, self.buffer = self.buffer.split('\n', 1)
            if line.startswith('[ResidentPerformance] samples='):
                self.queries += 1
                if self.queries == 2:
                    phase = 'idle'
            elif line.startswith('[ResidentIdle]'):
                phase = 'validation'
    def flush(self):
        self.original.flush()


def report():
    groups = {}
    def stats(values):
        values = sorted(value for value in values if value is not None)
        return {'mean': round(statistics.mean(values), 2), 'p95': round(values[int((len(values)-1)*.95)], 2),
            'max': round(values[-1], 2)} if values else {}
    for name in sorted({row['phase'] for row in samples}):
        rows = [row for row in samples if row['phase'] == name]
        group = {key: stats([row.get(key) for row in rows]) for key in
            ('cpu_total_pct', 'available_mib', 'hard_pages_per_sec', 'disk_read_bytes_sec', 'disk_write_bytes_sec',
             'disk_seconds_per_io', 'disk_queue', 'cpu_dpc_pct', 'cpu_interrupt_pct', 'sample_gap_ms', 'sample_work_ms')}
        group['seconds'] = len(rows)
        group['processes'] = {}
        for pid in {item['pid'] for row in rows for item in row['owned']}:
            items = [item for row in rows for item in row['owned'] if item['pid'] == pid]
            group['processes'][str(pid)] = {'name': items[0]['name'], **{key: stats([item[key] for item in items])
                for key in ('cpu_core_pct','private_mib','working_mib','read_bytes_sec','write_bytes_sec')}}
        groups[name] = group
    destination = ROOT / '.codex-audit/openclaw-pressure-independent.json'
    destination.write_text(json.dumps({'logical_cpus': os.cpu_count(), 'summary': groups, 'samples': samples}, indent=2), encoding='utf-8')
    print('[PressureSummary]', json.dumps(groups, ensure_ascii=False), flush=True)


async def main():
    global phase
    win32process.SetPriorityClass(win32api.GetCurrentProcess(), win32process.BELOW_NORMAL_PRIORITY_CLASS)
    worker = threading.Thread(target=monitor, daemon=True)
    worker.start()
    try:
        print('[Pressure] baseline 20 seconds', flush=True)
        await asyncio.sleep(20)
        phase = 'startup'
        process = await asyncio.create_subprocess_exec(sys.executable, '-u',
            str(ROOT / 'bin/tools/check_openclaw_backend_native.py'), '--idle-seconds', '180', '--observe-latency',
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        output = Output(sys.stdout)
        async for line in process.stdout:
            output.write(line.decode('utf-8', errors='replace'))
        if await process.wait():
            raise RuntimeError('Native pressure fixture failed')
    finally:
        finished.set()
        worker.join(10)
        report()


if __name__ == '__main__':
    asyncio.run(main())
