"""Isolated local-model/FAISS performance check; no business records or messages."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import ctypes
import json
from pathlib import Path
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from openclaw_service.assistant.lighthouse_knowledge_local import LocalEmbeddings, MemoryIndex, DIMENSIONS


def peak_memory():
    if sys.platform != 'win32':
        return None
    class Counters(ctypes.Structure):
        _fields_ = [('cb', ctypes.c_ulong), ('faults', ctypes.c_ulong),
                    *[(name, ctypes.c_size_t) for name in ('peak', 'working', 'ppp', 'pp', 'pnp', 'np', 'pagefile', 'peakpagefile')]]
    values = Counters()
    values.cb = ctypes.sizeof(values)
    process = ctypes.windll.kernel32.GetCurrentProcess
    process.restype = ctypes.c_void_p
    read = ctypes.windll.psapi.GetProcessMemoryInfo
    read.argtypes = [ctypes.c_void_p, ctypes.POINTER(Counters), ctypes.c_ulong]
    if not read(process(), ctypes.byref(values), values.cb):
        raise ctypes.WinError()
    return round(values.peak / 1024**2, 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--model-root', type=Path, required=True)
    parser.add_argument('--vectors', type=int, default=50_000)
    parser.add_argument('--readers', type=int, default=40)
    args = parser.parse_args()
    if not 3 <= args.vectors <= 100_000 or not 1 <= args.readers <= 64:
        parser.error('vectors must be 3..100000; readers must be 1..64')
    import numpy as np
    engine = LocalEmbeddings(args.model_root)
    try:
        started = time.perf_counter()
        documents = ['公司差旅报销需提交出差申请、交通票据和酒店发票。',
                     '公司年假应提前申请，经主管批准后休假。',
                     '公司访客入场须在前台登记并由接待人陪同。']
        vectors = engine.embed(documents)
        cold_ms = (time.perf_counter() - started) * 1000
        question = '公司出差回来申请报销要准备哪些材料？'
        query = engine.embed([question], query=True)[0]
        reference = np.asarray(vectors) @ np.asarray(query)
        assert int(np.argmax(reference)) == 0, 'actual BGE must retrieve the reimbursement example'
        index = MemoryIndex()
        rng = np.random.default_rng(20261010)
        def rows():
            for number, vector in enumerate(vectors, 1):
                yield number, np.asarray(vector, dtype='<f4').tobytes()
            for number in range(4, args.vectors + 1):
                yield number, rng.standard_normal(DIMENSIONS).astype('<f4').tobytes()
        started = time.perf_counter()
        index.replace(1, rows())
        build_ms = (time.perf_counter() - started) * 1000
        def read(_):
            started = time.perf_counter()
            result = index.search(engine.embed([question], query=True)[0])
            assert result[0][0] == 1
            return (time.perf_counter() - started) * 1000
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=args.readers) as pool:
            times = list(pool.map(read, range(args.readers)))
        elapsed = (time.perf_counter() - started) * 1000
        report = {'ok': True, 'model': 'bge-small-zh-v1.5', 'vectors': args.vectors, 'readers': args.readers,
                  'cold_model_and_three_documents_ms': round(cold_ms, 1), 'index_build_ms': round(build_ms, 1),
                  'cached_query_concurrent_total_ms': round(elapsed, 1),
                  'cached_query_p95_ms': round(sorted(times)[min(len(times)-1, int(len(times)*.95))], 1),
                  'process_peak_working_set_mib': peak_memory(), 'scope': 'synthetic documents, local retrieval only; excludes chat model answer time'}
        assert report['process_peak_working_set_mib'] is None or report['process_peak_working_set_mib'] < 2048
        print(json.dumps(report, ensure_ascii=False))
    finally:
        engine.close()


if __name__ == '__main__':
    main()
