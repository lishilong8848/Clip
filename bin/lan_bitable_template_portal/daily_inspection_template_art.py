"""Preserve original Excel drawings; expose only static pictures and text for the page."""
import base64
import io
import posixpath
import xml.etree.ElementTree as ET
from zipfile import ZipFile, ZIP_DEFLATED

S='http://schemas.openxmlformats.org/spreadsheetml/2006/main'
R='http://schemas.openxmlformats.org/officeDocument/2006/relationships'
P='http://schemas.openxmlformats.org/package/2006/relationships'
X='http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing'
A='http://schemas.openxmlformats.org/drawingml/2006/main'
N={'s':S,'x':X,'a':A}


def resolved(part,target):
    return target.lstrip('/') if target.startswith('/') else posixpath.normpath(posixpath.dirname(part)+'/'+target)


def relations(z,part):
    path=posixpath.dirname(part)+'/_rels/'+posixpath.basename(part)+'.rels'
    return ET.fromstring(z.read(path)) if path in z.namelist() else ET.Element('{'+P+'}Relationships')


def read_artwork(source):
    result={}
    with ZipFile(source) as z:
        workbook=ET.fromstring(z.read('xl/workbook.xml'))
        sheets={r.attrib['Id']:resolved('xl/workbook.xml',r.attrib['Target']) for r in relations(z,'xl/workbook.xml')}
        for sheet in workbook.findall('s:sheets/s:sheet',N):
            path=sheets[sheet.attrib['{'+R+'}id']]
            drawing=next((r for r in relations(z,path) if r.attrib.get('Type','').endswith('/drawing')),None)
            if drawing is None:continue
            part=resolved(path,drawing.attrib['Target']);xml=z.read(part)
            rel=relations(z,part);assets={}
            for r in rel:
                if r.attrib.get('TargetMode')=='External':continue
                filename=resolved(part,r.attrib['Target'])
                if r.attrib.get('Type','').endswith('/image'):
                    assets[r.attrib['Id']]={'suffix':posixpath.splitext(filename)[1], 'data':base64.b64encode(z.read(filename)).decode('ascii')}
            result[sheet.attrib['name']]={'xml':xml.decode('utf8'),'relationships':ET.tostring(rel,encoding='unicode'),'assets':assets}
    return result


def display_artwork(art,widths,heights):
    def point(anchor,tag):
        node=anchor.find('x:'+tag,N)
        if node is None:return None
        def v(name):return int(node.findtext('x:'+name,'0',N))
        return (sum(widths[:v('col')])+v('colOff')/9525,sum(heights[:v('row')])+v('rowOff')/9525)
    result=[]
    for anchor in ET.fromstring(art['xml']):
        start=point(anchor,'from');end=point(anchor,'to')
        if start is None:continue
        if end is None:
            ext=anchor.find('x:ext',N)
            if ext is None:continue
            end=(start[0]+int(ext.attrib['cx'])/9525,start[1]+int(ext.attrib['cy'])/9525)
        if start[1]>=sum(heights) or start[0]>=sum(widths):continue
        entry={'x':start[0],'y':start[1],'width':max(0,end[0]-start[0]),'height':max(0,end[1]-start[1])}
        picture=anchor.find('x:pic',N)
        if picture is not None:
            blip=picture.find('.//a:blip',N);asset=art['assets'].get(blip.attrib.get('{'+R+'}embed','')) if blip is not None else None
            if asset:
                mime={'.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg','.gif':'image/gif'}.get(asset['suffix'].lower())
                if mime:result.append({**entry,'image':'data:'+mime+';base64,'+asset['data']})
            continue
        shape=anchor.find('x:sp',N)
        if shape is None:continue
        # Only actual a:t nodes contain text; style metadata must not enter the display.
        lines=[''.join(t.text or '' for t in p.findall('.//a:t',N)) for p in shape.findall('x:txBody/a:p',N)]
        if not any(lines):continue
        run=shape.find('.//a:rPr',N)
        body=shape.find('x:txBody/a:bodyPr',N)
        font=run.find('a:ea',N) if run is not None else None
        result.append({**entry,'lines':lines,'font':font.attrib.get('typeface','sans-serif') if font is not None else 'sans-serif',
                       'size':int(run.attrib.get('sz','1100'))/75 if run is not None else 14.67,
                       'left':int(body.attrib.get('lIns','91440'))/9525 if body is not None else 9.6,
                       'top':int(body.attrib.get('tIns','45720'))/9525 if body is not None else 4.8})
    return result


def preserve_artwork(content,artworks):
    if not artworks:return content
    with ZipFile(io.BytesIO(content)) as source:
        files={name:source.read(name) for name in source.namelist()}
        wb=ET.fromstring(files['xl/workbook.xml'])
        parts={r.attrib['Id']:resolved('xl/workbook.xml',r.attrib['Target']) for r in relations(source,'xl/workbook.xml')}
        types=ET.fromstring(files['[Content_Types].xml']);ct=types.tag.split('}')[0]+'}'
        for index,sheet in enumerate(wb.findall('s:sheets/s:sheet',N),1):
            art=artworks.get(sheet.attrib['name'])
            if not art:continue
            part=parts[sheet.attrib['{'+R+'}id']]
            drawing=f'xl/drawings/inspection{index}.xml'
            rel=ET.fromstring(art['relationships'])
            for r in rel:
                asset=art['assets'].get(r.attrib['Id'])
                if not asset:continue
                suffix=asset['suffix'].lower();media=f'inspection{index}_{r.attrib["Id"]}{suffix}'
                r.set('Target','../media/'+media)
                files['xl/media/'+media]=base64.b64decode(asset['data'])
                ext=suffix.lstrip('.')
                if not any(x.attrib.get('Extension')==ext for x in types):
                    ET.SubElement(types,ct+'Default',Extension=ext,ContentType={'.png':'image/png','.jpg':'image/jpeg','.jpeg':'image/jpeg','.gif':'image/gif'}.get(suffix,'application/octet-stream'))
            files[drawing]=art['xml'].encode('utf8')
            files['xl/drawings/_rels/'+posixpath.basename(drawing)+'.rels']=ET.tostring(rel,encoding='utf-8')
            sheetrels=relations(source,part);rid='rIdInspectionDrawing'
            for old in list(sheetrels):
                if old.attrib.get('Type','').endswith('/drawing'):sheetrels.remove(old)
            ET.SubElement(sheetrels,'{'+P+'}Relationship',Id=rid,Type=R+'/drawing',Target='../drawings/'+posixpath.basename(drawing))
            files[posixpath.dirname(part)+'/_rels/'+posixpath.basename(part)+'.rels']=ET.tostring(sheetrels,encoding='utf-8')
            body=ET.fromstring(files[part])
            for old in body.findall('s:drawing',N):body.remove(old)
            ET.SubElement(body,'{'+S+'}drawing',{'{'+R+'}id':rid});files[part]=ET.tostring(body,encoding='utf-8')
            ET.SubElement(types,ct+'Override',PartName='/'+drawing,ContentType='application/vnd.openxmlformats-officedocument.drawing+xml')
        files['[Content_Types].xml']=ET.tostring(types,encoding='utf-8')
    output=io.BytesIO()
    with ZipFile(output,'w',ZIP_DEFLATED) as target:
        for name,value in files.items():target.writestr(name,value)
    return output.getvalue()
