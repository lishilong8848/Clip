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
            notice_summary=service.batches.notice_summary(scope,config)
            generated=export_workbook(original,config,ops,notice_summary)
            before=Workbook(original); after=Workbook(generated)
            expected_sheets=['机柜上电汇总表（邮件）','机柜上电汇总表（通告）',
                             *(name for name in before.sheets if name!='机柜上电汇总表')]
            assert list(after.sheets)==expected_sheets,(scope,list(after.sheets))
            assert before.archive.read('xl/vbaProject.bin')==after.archive.read('xl/vbaProject.bin')
            for name in before.sheets:
                target='机柜上电汇总表（邮件）' if name=='机柜上电汇总表' else name
                source_merges={x.get('ref') for x in before.sheet(name).iter(T('mergeCell'))}
                target_merges={x.get('ref') for x in after.sheet(target).iter(T('mergeCell'))}
                assert source_merges<=target_merges,(scope,name,'mergeCells')
            errors=[]
            for name in after.sheets:
                for ref,cell in after.cells(name).items():
                    value=cell.findtext(T('v'),'')
                    if cell.get('t')=='e' or value in {'#VALUE!','#REF!','#NAME?','#DIV/0!','#N/A','#NUM!','#NULL!'}:
                        errors.append((name,ref,value))
            assert not errors,(scope,errors[:10])
            formats={item['sheet']:item for item in config['template_data']['formats']}
            exported_rows={name:dict(after.rows(name)) for name in formats}
            for operation in ops:
                name=operation.get('source'); row_number=operation.get('source_row')
                if name not in formats or not row_number: continue
                row=exported_rows[name].get(int(row_number),{}); fmt=formats[name]
                assert row.get(fmt['room'])==operation['system_name'],(scope,name,row_number,'room')
                assert row.get(fmt['rack'])==operation['rack'],(scope,name,row_number,'rack')
            path=service.atomic_file(Path('exports')/folder/(scope+'.xlsm'),generated)
            report[scope]={'imported':expected,'records':len(ops),'counts':service.overview(scope)['counts'],
                           'notice_items':len(notice_summary.get('items',[])),'idle_cabinets':sum(o['empty'] for o in ops),
                           'source':'local_committed' if args.local else 'feishu_readback_and_local',
                           'sheets':list(after.sheets),'export':path,'verified':True}
            print(json.dumps({scope:report[scope]},ensure_ascii=False),flush=True)
        path=service.atomic_file(Path('exports')/folder/'report.json',json.dumps(report,ensure_ascii=False,indent=2).encode('utf-8'))
        print('report: '+path,flush=True)
    finally: service.shutdown(); service.store.shutdown_write_worker()

if __name__=='__main__': main()
