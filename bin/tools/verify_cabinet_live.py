"""Read-only Feishu verification and five original-template export samples."""
import json
import argparse
import sys
from pathlib import Path
from collections import Counter
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from lan_bitable_template_portal.cabinet_power import CabinetPowerService,INITIAL_TEMPLATES
from lan_bitable_template_portal.cabinet_power_excel import export_workbook,Workbook,T
from lan_bitable_template_portal.state_store import LanPortalStateStore

def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--local',action='store_true'); args=parser.parse_args()
    folder='verified-local-templates' if args.local else 'verified-original-templates'
    service=CabinetPowerService(LanPortalStateStore())
    try:
        if not args.local: service.do_refresh('',{},{})
        report={}
        for scope in 'ABCDE':
            snap=service.snapshot(scope); ops=snap['operations']; config=snap['config']
            imported=[o for o in ops if o['raw_fields'].get('数据标识','').startswith('source_')]
            expected={'A':1031,'B':1087,'C':1220,'D':988,'E':1272}[scope]
            assert len(imported)==expected,(scope,len(imported))
            assert len({o['raw_fields']['数据标识'] for o in imported})==expected
            original=(INITIAL_TEMPLATES/(scope+'.xlsm')).read_bytes()
            generated=export_workbook(original,config,ops)
            before=Workbook(original); after=Workbook(generated)
            assert list(before.sheets)==list(after.sheets)
            assert before.archive.read('xl/vbaProject.bin')==after.archive.read('xl/vbaProject.bin')
            for name in before.sheets:
                assert [x.get('ref') for x in before.sheet(name).iter(T('mergeCell'))]==[x.get('ref') for x in after.sheet(name).iter(T('mergeCell'))]
            path=service.atomic_file(Path('exports')/folder/(scope+'.xlsm'),generated)
            report[scope]={'imported':expected,'records':len(ops),'counts':service.overview(scope)['counts'],'idle_cabinets':sum(o['empty'] for o in ops),'source':'local_committed' if args.local else 'feishu_readback_and_local','original_sheets':list(after.sheets),'export':path,'verified':True}
            print(json.dumps({scope:report[scope]},ensure_ascii=False),flush=True)
        path=service.atomic_file(Path('exports')/folder/'report.json',json.dumps(report,ensure_ascii=False,indent=2).encode('utf-8'))
        print('report: '+path,flush=True)
    finally: service.shutdown(); service.store.shutdown_write_worker()

if __name__=='__main__': main()
