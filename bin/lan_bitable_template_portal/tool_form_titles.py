"""Keep Excel header text boxes alongside the images written by openpyxl."""
import colorsys
import io
import re
import xml.etree.ElementTree as ET
from zipfile import ZipFile

from .daily_inspection_template_art import N, P, R, X, display_artwork, preserve_artwork, relations, resolved


def _drawing(package, name):
    workbook = 'xl/workbook.xml'
    parts = {r.get('Id'): resolved(workbook, r.get('Target')) for r in relations(package, workbook)}
    for sheet in ET.fromstring(package.read(workbook)).findall('s:sheets/s:sheet', N):
        if sheet.get('name') == name:
            part = parts[sheet.get('{'+R+'}id')]
            return next((resolved(part, rel.get('Target')) for rel in relations(package, part)
                         if rel.get('Type', '').endswith('/drawing')), None)
    return None


def title_anchors(content, name):
    with ZipFile(io.BytesIO(content)) as package:
        part = _drawing(package, name)
        if not part:
            return {}
        return {f'@title{i}': anchor for i, anchor in enumerate(ET.fromstring(package.read(part)))
                if anchor.find('x:sp/x:txBody/a:p//a:t', N) is not None
                and 0 <= int(anchor.findtext('x:from/x:row', '-1', N)) < 3}


def _drawing_color(fill, theme):
    if fill is None or not len(fill):
        return None
    color = fill[0]
    value = color.get('val', '')
    if color.tag == '{'+N['a']+'}schemeClr':
        names = 'lt1 dk1 lt2 dk2 accent1 accent2 accent3 accent4 accent5 accent6 hlink folHlink'.split()
        fallback = {'lt1': 'ffffff', 'dk1': '000000'}
        value = theme.get(names.index(value), fallback.get(value, '')).lstrip('#') if value in names else ''
    if not re.fullmatch('[0-9a-fA-F]{6}', value):
        return None
    rgb = [int(value[i:i+2], 16)/255 for i in (0, 2, 4)]
    hue, light, saturation = colorsys.rgb_to_hls(*rgb)
    for modifier in color:
        amount = modifier.get('val', '')
        if amount.isdecimal():
            if modifier.tag == '{'+N['a']+'}lumMod':
                light *= int(amount)/100000
            elif modifier.tag == '{'+N['a']+'}lumOff':
                light += int(amount)/100000
    return '#' + ''.join(f'{round(channel*255):02x}' for channel in colorsys.hls_to_rgb(hue, min(1, max(0, light)), saturation))


def display_titles(anchors, sheet, theme):
    boxes = []
    for key, anchor in anchors.items():
        root = ET.Element('root')
        root.append(anchor)
        for box in display_artwork({'xml': ET.tostring(root), 'assets': {}}, sheet['columns'],
                                   [row['height'] for row in sheet['rows']]):
            boxes.append(dict(box, key=key, text='\n'.join(box['lines']),
                              background=_drawing_color(anchor.find('x:sp/x:spPr/a:solidFill', N), theme),
                              color=_drawing_color(anchor.find('.//a:rPr/a:solidFill', N), theme)))
    return boxes


def restore_titles(content, saved, sheet, values):
    anchors = title_anchors(content, sheet)
    if not anchors:
        return saved
    output = io.BytesIO()
    with ZipFile(io.BytesIO(saved)) as book, ZipFile(output, 'w') as result:
        part = _drawing(book, sheet)
        root = ET.fromstring(book.read(part)) if part else ET.Element('{'+X+'}wsDr')
        next_id = max((int(node.get('id')) for node in root.findall('.//x:cNvPr', N)), default=0) + 1
        for key, anchor in anchors.items():
            marker = anchor.find('x:from', N)
            shape_name = anchor.find('x:sp/x:nvSpPr/x:cNvPr', N).get('name')
            for existing in list(root):
                existing_shape = existing.find('x:sp/x:nvSpPr/x:cNvPr', N)
                existing_marker = existing.find('x:from', N)
                if (existing_shape is not None and existing_shape.get('name') == shape_name
                        and existing_marker is not None and list(existing_marker.itertext()) == list(marker.itertext())):
                    root.remove(existing)
            override = values.get(f'{sheet}!{key}')
            if override is not None:
                body = anchor.find('x:sp/x:txBody', N)
                paragraphs = body.findall('a:p', N)
                for extra in paragraphs[1:]:
                    body.remove(extra)
                runs = paragraphs[0].findall('.//a:t', N)
                if not runs:
                    runs = [ET.SubElement(ET.SubElement(paragraphs[0], '{'+N['a']+'}r'), '{'+N['a']+'}t')]
                runs[0].text = override
                for extra in runs[1:]:
                    extra.text = ''
            for node in anchor.iter():
                for attribute in list(node.attrib):
                    if attribute.startswith('{'+R+'}'):
                        del node.attrib[attribute]
            anchor.find('x:sp/x:nvSpPr/x:cNvPr', N).set('id', str(next_id))
            next_id += 1
            root.append(anchor)
        if not part:
            art = {'xml': ET.tostring(root, encoding='unicode'), 'assets': {},
                   'relationships': ET.tostring(ET.Element('{'+P+'}Relationships'), encoding='unicode')}
            return preserve_artwork(saved, {sheet: art})
        for entry in book.infolist():
            result.writestr(entry, ET.tostring(root, encoding='utf-8') if entry.filename == part else book.read(entry))
    return output.getvalue()
