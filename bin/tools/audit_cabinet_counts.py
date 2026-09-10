"""Read-only reconciliation of the five original workbooks and their merged histories."""
import json
import sys
from collections import Counter
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from lan_bitable_template_portal.cabinet_power import INITIAL_TEMPLATES
from lan_bitable_template_portal.cabinet_power_data import source_rows,source_evidence,complete_source_record,from_feishu
from lan_bitable_template_portal.cabinet_power_excel import Workbook,derive_records,map_state_baseline


def main():
    report={}
    for scope in 'ABCDE':
        content=(INITIAL_TEMPLATES/(scope+'.xlsm')).read_bytes(); model,rows=source_rows(content,scope); book=Workbook(content)
        config={**model,'template_data':{'hash':model['template_hash'],'summary_cells':{n:{ref:book.value(c) for ref,c in book.cells(n).items()} for n in book.sheets if '汇总' in n}}}
        records=[{'record_id':'audit'+scope+str(i),'fields':r['fields']} for i,r in enumerate(rows)]
        evidence=source_evidence(content,model)
        completed=[complete_source_record(r,evidence) or r for r in records]
        derived=derive_records(config,[from_feishu(r) for r in completed])
        baseline,_=map_state_baseline(content,config,[from_feishu(r) for r in completed])
        color_derived=derive_records({**config,'power_baseline':baseline},[from_feishu(r) for r in completed])
        colors=Counter(r.get('template_color','') for r in config['inventory'])
        entry={'source_records':len(records),'color_baseline_counts':color_derived['counts'],'history_counts':derived['counts'],'original_map_colors':dict(colors),'unlocated':{k:v for k,v in derived['unlocated'].items() if v['total']},'issues':derived['issues'],'racks':color_derived['racks']}
        report[scope]=entry
        print(json.dumps({scope:{k:v for k,v in entry.items() if k not in ('issues','racks')}},ensure_ascii=False),flush=True)
    path=Path('output/cabinet-audit/source-counts.json'); path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding='utf-8')


if __name__=='__main__': main()
