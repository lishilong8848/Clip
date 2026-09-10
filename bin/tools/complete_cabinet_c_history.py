"""Approved C-building history repair: existing record IDs only, with resumable readback."""
import argparse
import gzip
import json
import os
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from lan_bitable_template_portal.cabinet_power import CabinetFeishu,CabinetPowerService,INITIAL_TEMPLATES,equivalent
from lan_bitable_template_portal.cabinet_power_data import complete_source_record,from_feishu
from lan_bitable_template_portal.cabinet_power_excel import digest
from lan_bitable_template_portal.state_store import LanPortalStateStore


def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--commit',action='store_true'); parser.add_argument('--journal')
    args=parser.parse_args(); service=CabinetPowerService(LanPortalStateStore()); remote=CabinetFeishu()
    path=Path(args.journal).resolve() if args.journal else service.root/'backups'/('C-history-'+time.strftime('%Y%m%d-%H%M%S')+'.json')
    try:
        if path.exists(): journal=json.loads(path.read_text(encoding='utf-8'))
        else:
            with gzip.open(INITIAL_TEMPLATES/'C.layouts.json.gz','rt',encoding='utf-8') as source: evidence=json.load(source)
            rows=service.list_remote(remote,'C'); updates=[]
            for record in rows:
                completed=complete_source_record(record,evidence['source_rows'])
                if completed:
                    fields={k:v for k,v in completed['fields'].items() if not equivalent({k:v},record['fields'])}
                    updates.append({'record_id':record['record_id'],'before':record,'fields':fields,'verified':False})
            journal={'scope':'C','template_hash':evidence['hash'],'created_at':time.time(),'count':len(rows),'record_ids':sorted(r['record_id'] for r in rows),'updates':updates}
            service.atomic_file(path,json.dumps(journal,ensure_ascii=False,indent=2).encode())
        print(json.dumps({'journal':str(path),'records':journal['count'],'corrections':len(journal['updates']),'continuation_rows':sum(len(from_feishu({'record_id':u['record_id'],'fields':{**u['before']['fields'],**u['fields']}})['meta'].get('continuations',[])) for u in journal['updates'])},ensure_ascii=False),flush=True)
        if not args.commit: return
        os.environ['CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM']='1'; os.environ['CLIPFLOW_REAL_EXTERNAL_CONFIRMED']='1'
        service.ensure_loaded('C')
        if not path.with_suffix('.sqlite3').exists(): service.local.backup('C',path.with_suffix('.sqlite3'))
        with service.local.locked(['C']):
            if service.pending_writes('C'): raise RuntimeError('C楼存在未完成上传')
            remaining=[u for u in journal['updates'] if not u['verified']]
            for start in range(0,len(remaining),10):
                batch=remaining[start:start+10]
                filters='OR('+','.join('CurrentValue.[数据标识]='+json.dumps(item['before']['fields']['数据标识']) for item in batch)+')'
                current={r['record_id']:r for r in remote.list_all(filters=filters)}
                updates=[]
                for item in batch:
                    found=current.get(item['record_id'])
                    if not found: raise RuntimeError('云端原记录不存在: '+item['record_id'])
                    if not equivalent(item['fields'],found['fields']):
                        if digest(found['fields'])!=digest(item['before']['fields']): raise RuntimeError('云端记录冲突，停止补全: '+item['record_id'])
                        updates.append({'record_id':item['record_id'],'fields':item['fields']})
                if updates: remote.request('POST','records/batch_update',{'records':updates})
                current={r['record_id']:r for r in remote.list_all(filters=filters)}
                for item in batch:
                    found=current.get(item['record_id'])
                    if not found or not equivalent(item['fields'],found['fields']): raise RuntimeError('补全回读不一致: '+item['record_id'])
                    item.update(verified=True,after=found)
                service.atomic_file(path,json.dumps(journal,ensure_ascii=False,indent=2).encode())
                print('verified '+str(sum(u['verified'] for u in journal['updates']))+'/'+str(len(journal['updates'])),flush=True)
            actual=service.list_remote(remote,'C')
            if sorted(r['record_id'] for r in actual)!=journal['record_ids']: raise RuntimeError('记录集合发生变化，请核对')
            for item in journal['updates']:
                if not equivalent(item['fields'],next(r for r in actual if r['record_id']==item['record_id'])['fields']): raise RuntimeError('最终回读不一致')
            # Only these original-source events are added to the frozen history baseline.
            saved=service.local.load('C')
            baseline=set(saved['baseline'])
            baseline.update(e['id'] for item in journal['updates'] for e in from_feishu(item['after'])['events'])
            service.local.replace('C',saved['config'],actual,sorted(baseline),extend_baseline=True)
            journal['completed_at']=time.time(); service.atomic_file(path,json.dumps(journal,ensure_ascii=False,indent=2).encode())
            print(json.dumps({'completed':True,'records':len(actual),'updated':len(journal['updates']),'counts':service.overview('C')['counts']},ensure_ascii=False),flush=True)
    finally: service.shutdown(); service.store.shutdown_write_worker()


if __name__=='__main__': main()
