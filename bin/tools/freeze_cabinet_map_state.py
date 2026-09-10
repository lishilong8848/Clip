"""One-time, resumable installation of user-confirmed physical map colour baselines."""
import argparse
import json
import os
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from lan_bitable_template_portal.cabinet_power import CabinetPowerService,INITIAL_TEMPLATES,equivalent
from lan_bitable_template_portal.cabinet_power_excel import map_state_baseline,derive_records,digest
from lan_bitable_template_portal.state_store import LanPortalStateStore


def main():
    parser=argparse.ArgumentParser(); parser.add_argument('--commit',action='store_true'); parser.add_argument('--journal'); args=parser.parse_args()
    service=CabinetPowerService(LanPortalStateStore())
    path=Path(args.journal).resolve() if args.journal else service.root/'backups'/('map-colors-'+time.strftime('%Y%m%d-%H%M%S')+'.json')
    journal=json.loads(path.read_text(encoding='utf-8')) if path.exists() else {'buildings':{}}
    def persist(): service.atomic_file(path,json.dumps(journal,ensure_ascii=False,indent=2).encode())
    try:
        for scope in 'ABCDE':
            service.ensure_loaded(scope)
            with service.local.locked([scope]):
                if service.pending_writes(scope): raise RuntimeError(scope+'楼存在未完成上传')
                if scope not in journal['buildings']:
                    snap=service.snapshot(scope); config=snap['config']
                    if config.get('power_baseline'):
                        print(scope+' baseline already installed',flush=True); continue
                    baseline,colors=map_state_baseline((INITIAL_TEMPLATES/(scope+'.xlsm')).read_bytes(),config,snap['operations'])
                    config['power_baseline']=baseline
                    if scope=='C': next(r for r in config['rooms'] if r['id']=='202')['region']='B1:AX49'
                    records=[{'record_id':o['record_id'],'fields':o['raw_fields']} for o in snap['operations']]
                    journal['buildings'][scope]={'version':snap['version'],'records':records,'config':config,'colors':colors,'counts':derive_records(config,snap['operations'])['counts'],'updates':{},'complete':False}
                    persist()
                plan=journal['buildings'][scope]
                print(json.dumps({scope:{k:plan[k] for k in ('colors','counts','complete')},'journal':str(path)},ensure_ascii=False),flush=True)
                if not args.commit or plan['complete']: continue
                if service.local.version(scope)!=plan['version']:
                    if service.config(scope).get('power_baseline')==plan['config']['power_baseline'] and plan['updates'] and all(u['verified'] for u in plan['updates'].values()):
                        plan['complete']=True; persist(); continue
                    raise RuntimeError(scope+'楼本地版本变化，请重新核对')
                actual=service.list_remote(service.remote_for(scope),scope)
                if {r['record_id']:digest(r['fields']) for r in actual}!={r['record_id']:digest(r['fields']) for r in plan['records']}: raise RuntimeError(scope+'楼云端记录变化，暂停基线安装')
                backup=path.parent/(path.stem+'-'+scope+'.sqlite3')
                if not backup.exists(): service.local.backup(scope,backup)
                os.environ['CLIPFLOW_REQUIRE_REAL_EXTERNAL_CONFIRM']='1'; os.environ['CLIPFLOW_REAL_EXTERNAL_CONFIRMED']='1'
                remote=service.directory(scope)
                for room in plan['config']['rooms']:
                    rid=room['record_id']; current=remote.get(rid); update=plan['updates'].get(rid)
                    if update is None:
                        geometry=json.loads(current['fields'].get('布局资料') or '{}')
                        geometry['region']=room.get('region','')
                        geometry['power_baseline']={'version':1,'template_hash':plan['config']['template_data']['hash'],'racks':{k.split('/')[1]:v for k,v in plan['config']['power_baseline'].items() if k.startswith(room['id']+'/')}}
                        update={'before':current['fields'],'fields':{'布局资料':json.dumps(geometry,ensure_ascii=False,separators=(',',':'))},'verified':False}
                        plan['updates'][rid]=update; persist()
                    if not equivalent(update['fields'],current['fields']):
                        if digest(current['fields'])!=digest(update['before']): raise RuntimeError('基础资料版本冲突：'+rid)
                        remote.update(rid,update['fields'])
                    verified=remote.get(rid)
                    if not equivalent(update['fields'],verified['fields']): raise RuntimeError('基础资料回读不一致：'+rid)
                    update['verified']=True; persist()
                saved=service.local.load(scope)
                service.local.replace(scope,plan['config'],actual,saved['baseline'])
                plan['complete']=True; persist(); print(scope+' installed and verified',flush=True)
    finally: service.shutdown(); service.store.shutdown_write_worker()


if __name__=='__main__': main()
