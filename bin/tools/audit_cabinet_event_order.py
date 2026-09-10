"""Read-only audit of committed cabinet histories, including state/color differences."""
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT))
from bin.lan_bitable_template_portal.cabinet_power_data import from_feishu
from bin.lan_bitable_template_portal.cabinet_power_excel import STATES,completed_state_event,derive_records


def main():
    report={}
    for scope in 'ABCDE':
        path=ROOT/'bin/data/cabinet_power/buildings'/(scope+'.sqlite3')
        with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True) as conn:
            config=json.loads(conn.execute("SELECT payload FROM meta WHERE key='config'").fetchone()[0])
            config['inventory']=[json.loads(r[0]) for r in conn.execute('SELECT payload FROM inventory')]
            ops=[from_feishu(json.loads(r[0])) for r in conn.execute('SELECT payload FROM records')]
        derived=derive_records(config,ops); histories=defaultdict(list)
        for op in ops:
            histories[(op['room'],op['rack'])].extend({**op,**e} for e in op['events'])
        details={'current_history_differences':[],'original_color_differences':[],'expected_actual_order_differences':[],'conflicting_same_actual_time':[],'missing_actual':[],'primary_not_latest':[]}
        def compact(e): return {k:e.get(k) for k in ('record_id','source','source_row','id','action','actual','expected','result')}
        for op in ops:
            valid=[e for e in op['events'] if completed_state_event({**op,**e})]
            latest=max(valid,key=lambda e:e['actual'],default={})
            if latest and latest['group']!=op['meta'].get('primary',0):
                details['primary_not_latest'].append({'room':op['room'],'rack':op['rack'],**compact({**op,**latest})})
        for rack in derived['racks']:
            history=histories[(rack['room'],rack['rack'])]; valid=[e for e in history if completed_state_event(e)]
            latest=max(valid,key=lambda e:(e['actual'],e['id']),default={})
            expected=max((e for e in valid if e.get('expected')),key=lambda e:(e['expected'],e['id']),default={})
            item={'room':rack['room'],'rack':rack['rack'],'state':rack['state'],'state_source':rack.get('state_source'),'latest':compact(latest)}
            if latest and STATES[latest['action']]!=rack['state']: details['current_history_differences'].append(item)
            original=config.get('power_baseline',{}).get(rack['room']+'/'+rack['rack'],{}).get('state')
            if original and original!=rack['state']: details['original_color_differences'].append({**item,'original_color_state':original})
            if latest and expected and latest['actual']!=expected['actual'] and latest['action']!=expected['action']:
                details['expected_actual_order_differences'].append({**item,'latest_expected':compact(expected)})
            if latest and len({STATES[e['action']] for e in valid if e['actual']==latest['actual']})>1: details['conflicting_same_actual_time'].append(item)
            missing=[compact(e) for e in history if e['action'] in STATES and not e['actual']]
            if missing: details['missing_actual'].append({**item,'events':missing})
        report[scope]={'records':len(ops),'events':sum(len(o['events']) for o in ops),'counts':derived['counts'],'audit_counts':{k:len(v) for k,v in details.items()},'details':details}
        print(json.dumps({scope:{k:v for k,v in report[scope].items() if k!='details'}},ensure_ascii=False),flush=True)
    out=ROOT/'output/cabinet-audit/event-order.json'; out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')


if __name__=='__main__': main()
