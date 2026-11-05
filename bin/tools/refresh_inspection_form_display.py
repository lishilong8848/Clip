"""Regenerate display-only inspection resources from supplied workbooks; sources are never saved."""
import datetime as dt
import hashlib
import json
from pathlib import Path
from openpyxl import load_workbook
from openpyxl.styles.colors import COLOR_INDEX
ROOT=Path(__file__).resolve().parents[2]
TARGET=ROOT/'bin/resources/daily_inspection_templates.json'
SOURCE=Path(r'D:\下载\E楼巡检表')

def color(c,default):
    if c is None:return default
    if c.type=='rgb':return '#'+str(c.rgb)[-6:]
    if c.type=='indexed' and 0<=c.indexed<len(COLOR_INDEX):return '#'+COLOR_INDEX[c.indexed][-6:]
    return default

def css(cell):
    f,a,b,fill=cell.font,cell.alignment,cell.border,cell.fill
    out={'fontFamily':f.name or 'Calibri','fontSize':round((f.sz or 11)*4/3,2),'fontWeight':'700' if f.bold else '400',
         'fontStyle':'italic' if f.italic else 'normal','color':color(f.color,'#000000'),
         'backgroundColor':color(fill.fgColor,'#ffffff') if fill.patternType=='solid' else '#ffffff',
         'textAlign':{'centerContinuous':'center','distributed':'justify','fill':'left','general':'left'}.get(a.horizontal,a.horizontal or 'left'),
         'verticalAlign':{'center':'middle','distributed':'middle','justify':'middle'}.get(a.vertical,a.vertical or 'bottom'),
         'whiteSpace':'pre-wrap' if a.wrap_text else 'pre'}
    for side in ['left','right','top','bottom']:
        edge=getattr(b,side)
        if edge and edge.style:
            weight=2 if edge.style in ('medium','thick','mediumDashDot','mediumDashed') else 1
            style='double' if edge.style=='double' else 'dotted' if edge.style in ('dotted','hair') else 'dashed' if 'dash' in edge.style.lower() else 'solid'
            out['border'+side.title()]=f'{weight}px {style} {color(edge.color,"#000000")}'
        else:out['border'+side.title()]='1px solid #e1e4e8'
    return out

def main():
    data=json.loads(TARGET.read_text(encoding='utf8'))
    for template in data['templates']:
        matches=[p for p in SOURCE.glob('*.xlsx') if template['scope']+'栋'+template['profession'] in p.name]
        if len(matches)!=1:raise ValueError('来源不唯一 '+str(matches))
        source=matches[0];book=load_workbook(source,data_only=False)
        old={s['name']:s for s in template['sheets']};sheets=[]
        base_font=book._fonts[0]
        digit_width=round((base_font.sz or 11)*2/3) if base_font.name in ('宋体','新宋体','SimSun') else 7
        for ws in book:
            values=[c for c in ws._cells.values() if c.value is not None and str(c.value).strip()]
            if not values:continue
            maxrow=max(c.row for c in values);maxcol=max(c.column for c in values)
            for merge in ws.merged_cells.ranges:
                anchor=ws.cell(merge.min_row,merge.min_col)
                if anchor.value is not None:
                    maxrow=max(maxrow,merge.max_row);maxcol=max(maxcol,merge.max_col)
            if maxrow>500 or maxcol>200:raise ValueError(f'有效范围异常 {ws.title} {maxrow} {maxcol}')
            merges=[str(m) for m in ws.merged_cells.ranges if m.max_row<=maxrow and m.max_col<=maxcol]
            widths=[]
            for i in range(1,maxcol+1):
                dim=next((d for d in ws.column_dimensions.values() if (d.min or 0)<=i<=(d.max or 0)),None)
                width=dim.width if dim and dim.width is not None else ws.sheet_format.defaultColWidth or 8.43
                widths.append(0 if dim and dim.hidden else round(width*digit_width+5,2))
            heights=[0 if ws.row_dimensions[i].hidden else round((ws.row_dimensions[i].height or ws.sheet_format.defaultRowHeight or 15)*4/3,2) for i in range(1,maxrow+1)]
            rows=[];style_ids=[];styles=[];seen={}
            for ri in range(1,maxrow+1):
                row=[];ids=[]
                for ci in range(1,maxcol+1):
                    cell=ws.cell(ri,ci);value=cell.value
                    if isinstance(value,(dt.datetime,dt.date,dt.time)):value=value.isoformat()
                    if template['scope']=='D' and ws.title.strip()=='暖通冷机系统巡检表' and value=='`':value=None
                    row.append(value)
                    style=css(cell);key=json.dumps(style,sort_keys=True)
                    if key not in seen:seen[key]=len(styles);styles.append(style)
                    ids.append(seen[key])
                rows.append(row);style_ids.append(ids)
            name=next((name for name in old if name.strip()==ws.title.strip()),ws.title)
            sheets.append({'excluded_ranges':old.get(name,{}).get('excluded_ranges',[]),'name':name,'rows':rows,'merges':merges,'column_widths':widths,'row_heights':heights,'styles':styles,'style_ids':style_ids,'source_range':f'A1:{ws.cell(maxrow,maxcol).coordinate}'})
            print(template['scope'],name,maxcol,maxrow,flush=True)
        template.update(sheets=sheets,source_name=source.name,source_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
        book.close()
    TARGET.write_text(json.dumps(data,ensure_ascii=False,separators=(',',':')),encoding='utf8')
if __name__=='__main__':main()

