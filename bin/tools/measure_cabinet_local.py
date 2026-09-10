"""Five-process export and hot-read acceptance check on isolated source fixtures."""
import json
import sys
import tempfile
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from bin.test_cabinet_power import fixtures,MemoryStore,FakeFeishu
from bin.lan_bitable_template_portal.cabinet_power import CabinetPowerService
from bin.lan_bitable_template_portal.cabinet_power_excel import Workbook,T


def main():
    _,records,directory,_=fixtures(with_power_baseline=True)
    with tempfile.TemporaryDirectory() as root:
        service=CabinetPowerService(MemoryStore(),FakeFeishu(records),root); service._directory=FakeFeishu(directory)
        report={'reads_ms':{},'exports':{}}
        try:
            service.do_refresh('',{},{})
            for scope in 'ABCDE':
                room=next(r['id'] for r in service.config(scope)['rooms'] if r.get('sheet'))
                service.overview(scope); service.operations(scope,{}); service.layout(scope,room)
                timings=[]
                for _ in range(20):
                    for call in (lambda:service.overview(scope),lambda:service.operations(scope,{}),lambda:service.layout(scope,room)):
                        start=time.perf_counter(); json.dumps(call(),ensure_ascii=False); timings.append((time.perf_counter()-start)*1000)
                report['reads_ms'][scope]={'p95':round(sorted(timings)[int(len(timings)*.95)-1],1),'max':round(max(timings),1)}
                assert report['reads_ms'][scope]['p95']<=500,report
            started=time.perf_counter(); jobs={s:service.job(s,'export','fixture',{}) for s in 'ABCDE'}; during=[]
            while True:
                statuses={s:service.job_status(j['job_id']) for s,j in jobs.items()}
                if all(j['status'] not in ('pending','running') for j in statuses.values()): break
                for s in 'ABCDE':
                    start=time.perf_counter(); json.dumps(service.operations(s,{})); during.append((time.perf_counter()-start)*1000)
                if time.perf_counter()-started>180: raise AssertionError('export timeout')
                time.sleep(.15)
            assert max(j['started_at'] for j in statuses.values())<min(j['finished_at'] for j in statuses.values()),statuses
            for scope,j in statuses.items():
                assert j['status']=='succeeded',j
                exported=service.read('export:'+j['result']['export_id']); book=Workbook(Path(exported['path']).read_bytes())
                assert book.archive.read('xl/vbaProject.bin')
                destination=Path('output/cabinet-audit/exports')/(scope+'.xlsm')
                service.atomic_file(destination.resolve(),Path(exported['path']).read_bytes())
                report['exports'][scope]={'version':j['result']['version'],'started_at':j['started_at'],'finished_at':j['finished_at'],'shared_formula_nodes':sum(f.get('t')=='shared' for n in book.sheets for f in book.sheet(n).iter(T('f')))}
            report['export_seconds']=round(time.perf_counter()-started,2)
            report['during_export_read_p95_ms']=round(sorted(during)[int(len(during)*.95)-1],1)
            assert report['during_export_read_p95_ms']<=500,report
            service.atomic_file(Path('output/cabinet-audit/performance.json').resolve(),json.dumps(report,ensure_ascii=False,indent=2).encode())
            print(json.dumps(report,ensure_ascii=False),flush=True)
        finally: service.shutdown()


if __name__=='__main__': main()
