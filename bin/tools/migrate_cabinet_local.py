"""Back up and migrate cabinet snapshots; export current state before rollback."""
import argparse
import json
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from lan_bitable_template_portal.cabinet_power import CabinetPowerService,NAMESPACE,SNAPSHOT_KEY
from lan_bitable_template_portal.state_store import LanPortalStateStore


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--rollback-export',action='store_true')
    parser.add_argument('--db',default='bin/data/lan_portal_state.sqlite3')
    parser.add_argument('--scopes',default='ABCDE')
    args=parser.parse_args()
    legacy=LanPortalStateStore(args.db); service=CabinetPowerService(legacy)
    backup=service.root/'backups'/time.strftime('%Y%m%d_%H%M%S')
    try:
        docs=legacy.list_documents(NAMESPACE)
        service.atomic_file(backup/'legacy-documents.json',json.dumps(docs,ensure_ascii=False).encode())
        report={}
        if not args.scopes or set(args.scopes)-set('ABCDE'): raise ValueError('楼栋无效')
        for scope in args.scopes:
            service.ensure_loaded(scope)
            with service.local.locked([scope]):
                snapshot=service.snapshot(scope)
                service.local.backup(scope,backup/(scope+'.sqlite3'))
                # Current data plus journals are retained even when reverting application code.
                documents=service.local.documents(scope,'')
                service.atomic_file(backup/(scope+'-current.json'),json.dumps({'snapshot':snapshot,'documents':documents},ensure_ascii=False).encode())
                if args.rollback_export:
                    legacy.put_document(NAMESPACE,SNAPSHOT_KEY+scope,{'saved_at':snapshot['updated_at'],'snapshot':snapshot})
                original=legacy.get_document(NAMESPACE,SNAPSHOT_KEY+scope)
                if not args.rollback_export and original:
                    prior={o['record_id']:o['raw_fields'] for o in original['snapshot']['operations']}
                    current={o['record_id']:o['raw_fields'] for o in snapshot['operations']}
                    report[scope]={'records':len(current),'matches_legacy':prior==current}
                else: report[scope]={'records':len(snapshot['operations'])}
                for item in docs:
                    payload=item.get('payload',{})
                    if payload.get('scope')==scope and item['key'].startswith(('export:','job:')) and service.local.document(scope,item['key']) is None:
                        service.local.document(scope,item['key'],payload)
        print(json.dumps({'backup':str(backup),'buildings':report,'rollback_export':args.rollback_export},ensure_ascii=False),flush=True)
    finally:
        service.shutdown(); legacy.shutdown_write_worker()


if __name__=='__main__': main()
