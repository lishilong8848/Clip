"""Compare a read-only D/E Feishu snapshot with the packaged workbook layouts."""

import argparse
import collections
import hashlib
import json
import sys
from pathlib import Path
from openpyxl.utils.cell import get_column_letter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lan_bitable_template_portal.daily_inspection import (
    LOCAL_POINT_ID, LOCAL_POINT_SOURCES, LOCATION_FIELDS, ROUND_LOCATION_FIELDS,
    deferred_catalog_section, deferred_original_location, deferred_point,
    devices_for, original_cell_is_status, plain, state_parameter, supplemental_form_devices, validate_round_locations,
    validate_source_batch,
)
from lan_bitable_template_portal.daily_inspection_original import catalog_locations, cell_excluded, original_templates, round_rows


def positions(template, sheet, cells):
    return {(template, sheet.strip(), address) for address in cells.values() if address}


def suggested_state_id(scope, profession, template, sheet, cells):
    payload = json.dumps([scope, profession, template, sheet.strip(), cells],
                         ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return 'local_' + hashlib.sha256(payload.encode()).hexdigest()[:32]


def audit_building(scope, data):
    configs = collections.defaultdict(list)
    samples = collections.defaultdict(list)
    for row in data['config']:
        fields = row['fields']
        configs[plain(fields.get('专业')), plain(fields.get('测点ID'))].append(row)
    for row in data['source']:
        fields = row['fields']
        if plain(fields.get('记录类型')) == '测点明细' and plain(fields.get('楼栋')) == scope+'楼':
            samples[plain(fields.get('专业')), plain(fields.get('测点ID'))].append(fields)
    if validate_source_batch(scope, data['source']) is None:
        raise ValueError(f'{scope}楼源数据批次未完整封标')
    devices = devices_for(scope, data['source'], data['config'], {})
    occupied, seen, candidates = collections.defaultdict(set), set(), []
    backfill, unresolved, manual, catalog_values, already_configured = [], [], [], [], []
    for device in devices:
        for point in device['points']:
            key = device['profession'], point['point_id']
            seen.add(key)
            cells = point['round_cells']
            targets = positions(point['template'], point['sheet'], cells)
            for target in targets:
                occupied[target].add(key)
            candidates.append((device, point, configs.get(key, []), targets))

    for device, point, rows, targets in candidates:
        profession, point_id = device['profession'], point['point_id']
        readings = samples[profession, point_id]
        identity = {'scope': scope, 'profession': profession, 'point_id': point_id,
                    'name': point['name'], 'device': device['name'],
                    'source_device': plain(readings[0].get('设备编号')).strip() if len(readings) == 1 else ''}
        if point['enabled'] is False:
            continue
        if (len(rows) != 1 or not targets or point.get('mapping_issue') or not identity['source_device']
                or plain(readings[0].get('测点名称')).strip() != identity['name']):
            unresolved.append({**identity, 'reason': point.get('mapping_issue')
                               or '测点配置不存在／重复，或目标位置未核验'})
            continue
        row = rows[0]
        fields = row['fields']
        target_fields = (*LOCATION_FIELDS, *ROUND_LOCATION_FIELDS.values())
        existing = {field: plain(fields.get(field)).strip() for field in target_fields}
        if all(existing[field] for field in ROUND_LOCATION_FIELDS.values()):
            continue
        desired = dict(zip(LOCATION_FIELDS, (point['template'], point['sheet'].strip(), point['cell'])))
        desired.update({field: point['round_cells'][slot] for slot, field in ROUND_LOCATION_FIELDS.items()})
        if (any(len(occupied[target]) != 1 for target in targets) or
                any(value and value != desired[field] for field, value in existing.items())):
            unresolved.append({**identity, 'reason': '与现有配置或其他测点目标冲突'})
            continue
        backfill.append({**identity, 'record_id': row['record_id'], 'expected': existing,
                         'fields': {field: value for field, value in desired.items() if not existing[field]}})

    for (profession, point_id), rows in configs.items():
        if LOCAL_POINT_ID.fullmatch(point_id):
            for row in rows:
                fields = row['fields']
                if plain(fields.get('测点来源')).strip() not in LOCAL_POINT_SOURCES:
                    unresolved.append({'scope': scope, 'profession': profession, 'point_id': point_id,
                                       'reason': '无采集测点的来源类型无效'})
                    continue
                template, sheet, _ = (plain(fields.get(field)).strip() for field in LOCATION_FIELDS)
                try:
                    cells = validate_round_locations(scope, profession, template, sheet,
                        {slot: plain(fields.get(field)) for slot, field in ROUND_LOCATION_FIELDS.items()})
                except (ValueError, TypeError, KeyError):
                    unresolved.append({'scope': scope, 'profession': profession, 'point_id': point_id,
                                       'reason': '手工或固定值测点位置无效'})
                    continue
                if not cells or len(rows) != 1:
                    unresolved.append({'scope': scope, 'profession': profession, 'point_id': point_id,
                                       'reason': '手工或固定值测点未配置位置或身份重复'})
                    continue
                for coord in positions(template, sheet, cells):
                    occupied[coord].add((profession, point_id))
                already_configured.append(point_id)
            continue
        if (profession, point_id) not in seen and any(r['fields'].get('启用') is True for r in rows):
            unresolved.append({'scope': scope, 'profession': profession, 'point_id': point_id,
                               'reason': '当前完整批次尚未包含此测点'})

    catalog = catalog_locations()
    manual_targets = set()

    def add_form_point(profession, name, device_name, target):
        template, sheet = target['template'], target['sheet'].strip()
        valid = validate_round_locations(scope, profession, template, sheet, target['cells'])
        coords = positions(template, sheet, valid)
        if any(occupied[cell] for cell in coords) or coords & manual_targets:
            return
        manual_targets.update(coords)
        manual.append({'scope': scope, 'profession': profession, 'name': name, 'device': device_name,
                       'point_id': suggested_state_id(scope, profession, template, sheet, valid),
                       'target': {'workbook': template, 'sheet': sheet, 'cell': valid['白1'], 'round_cells': valid}})

    def empty_status(profession, name, form):
        return state_parameter(name) or (
            profession == '暖通' and form.strip() == '暖通冷机系统巡检表'
            and name in {'冷机模式', '预冷模式', '自然冷却模式', '自然冷模式', '远程控制', '自动运行', '自动补水'}
        )

    for device in devices:
        for section in device['sections']:
            if deferred_catalog_section(device, section):
                continue
            for name, value in section['values'].items():
                target = catalog.get((scope+'楼', device['name'], device['profession'], section['location'],
                                      section['form'].strip(), name))
                if target is None:
                    continue
                coords = positions(target['template'], target['sheet'], target['cells'])
                if any(occupied[cell] for cell in coords):
                    continue
                if value in ('', None) and empty_status(device['profession'], name, section['form']):
                    add_form_point(device['profession'], name, device['name'], target)
                elif value not in ('', None):
                    catalog_values.append({'device': device['name'], 'name': name, 'sheet': target['sheet'],
                                           'profession': device['profession'], 'cells': target['cells']})
    for device in supplemental_form_devices(scope):
        for point in device['points']:
            if point['value_type'] == '状态' and not deferred_point(device, point):
                add_form_point(device['profession'], point['name'], device['name'],
                               {**point, 'cells': point['round_cells']})
    for template in original_templates():
        if template['scope'] != scope:
            continue
        for sheet in template['sheets']:
            rows_by_slot = round_rows(sheet)
            rounds = tuple(ROUND_LOCATION_FIELDS)
            for row, slot in rows_by_slot.items():
                if slot != '白1' or tuple(rows_by_slot.get(row + i) for i in range(4)) != rounds:
                    continue
                for column in range(1, len(sheet['rows'][row - 1]) + 1):
                    letter = get_column_letter(column)
                    cells = {slot: f'{letter}{row + offset}' for offset, slot in enumerate(rounds)}
                    if (not original_cell_is_status(sheet, cells['白1'])
                            or deferred_original_location(scope, sheet['name'], cells)
                            or any(cell_excluded(sheet, address) for address in cells.values())):
                        continue
                    if not all(column <= len(sheet['rows'][row+i-1]) and
                               sheet['rows'][row+i-1][column-1] in ('', None) for i in range(4)):
                        continue
                    try:
                        add_form_point(template['profession'], f'{sheet["name"].strip()} {cells["白1"]} 状态',
                                       sheet['name'].strip(), {'template': template['name'],
                                       'sheet': sheet['name'], 'cells': cells})
                    except ValueError:
                        continue
    return {'backfill': backfill, 'manual_states': manual, 'catalog_values': catalog_values,
            'unresolved': unresolved, 'already_configured': already_configured}


def verify_migration(scope, data, plan):
    """Verify what is configured and what the most recent sealed export contains."""
    batch = validate_source_batch(scope, data['source'])
    if batch is None:
        raise ValueError(f'{scope}楼源数据批次未完整封标，无法核对迁移结果')
    config, exported = collections.defaultdict(list), collections.defaultdict(list)
    for row in data['config']:
        fields = row['fields']
        config[plain(fields.get('专业')), plain(fields.get('测点ID'))].append(row)
    for row in data['source']:
        fields = row['fields']
        if plain(fields.get('记录类型')) == '测点明细':
            exported[plain(fields.get('专业')), plain(fields.get('测点ID'))].append(fields)

    def matches(fields, targets):
        return all(plain(fields.get(field)).strip() == value for field, value in targets.items())

    result = {}
    for kind in ('backfill', 'manual_states'):
        pending_config, pending_export = [], []
        checked = set()
        for item in plan[kind]:
            key = item['profession'], item['point_id']
            if item['scope'] != scope or key in checked:
                raise ValueError(f'{scope}楼迁移清单含重复或跨楼身份')
            checked.add(key)
            if kind == 'backfill':
                targets = {**item['expected'], **item['fields']}
            else:
                target = item['target']
                targets = dict(zip(LOCATION_FIELDS, (target['workbook'], target['sheet'], target['cell'])))
                targets.update({field: target['round_cells'][slot] for slot, field in ROUND_LOCATION_FIELDS.items()})
                targets.update(测点来源='manual_status', 测点类型='状态')
            expected_config = dict(targets)
            expected_export = dict(targets)
            record_id = item.get('record_id')
            if kind == 'backfill':
                expected_config['测点名称'] = item['name']
                expected_export.update(设备编号=item['source_device'], 测点名称=item['name'])
            configured = config[key]
            if (len(configured) != 1 or configured[0]['fields'].get('启用') is not True
                    or not matches(configured[0]['fields'], expected_config)
                    or record_id and configured[0]['record_id'] != record_id):
                pending_config.append(list(key))
            published = exported[key]
            if len(published) != 1 or not matches(published[0], expected_export):
                pending_export.append(list(key))
        result[kind] = {
            'total': len(checked), 'configured': len(checked) - len(pending_config),
            'exported': len(checked) - len(pending_export),
            'pending_config': pending_config, 'pending_export': pending_export,
        }
    return {'source_batch': batch['upload_time'], **result}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('snapshot', type=Path)
    parser.add_argument('report', type=Path)
    parser.add_argument('--verify-against', type=Path, metavar='PLAN')
    args = parser.parse_args()
    source = json.loads(args.snapshot.read_text(encoding='utf-8-sig'))
    if args.verify_against:
        plan = json.loads(args.verify_against.read_text(encoding='utf-8-sig'))
        result = {scope: verify_migration(scope, source[scope], plan[scope]) for scope in ('D', 'E')}
        args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        statuses = {scope: {kind: {key: batch[kind][key] for key in ('total', 'configured', 'exported')}
                            for kind in ('backfill', 'manual_states')} for scope, batch in result.items()}
        print(json.dumps(statuses, ensure_ascii=False))
        return int(any(group['configured'] != group['total'] or group['exported'] != group['total']
                       for groups in statuses.values() for group in groups.values()))
    result = {scope: audit_building(scope, source[scope]) for scope in ('D', 'E')}
    args.report.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    print(json.dumps({scope: {name: len(items) for name, items in report.items()}
                      for scope, report in result.items()}, ensure_ascii=False))


if __name__ == '__main__':
    sys.exit(main())
