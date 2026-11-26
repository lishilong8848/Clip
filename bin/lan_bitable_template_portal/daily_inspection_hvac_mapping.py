"""Authoritative HVAC switch points and original form cells for D/E inspections."""
import json
from functools import lru_cache
from pathlib import Path


_RESOURCE = Path(__file__).resolve().parents[1] / 'resources' / 'daily_inspection_hvac_switch_mapping.json'
_SLOTS = ('白1', '白2', '夜1', '夜2')


@lru_cache(maxsize=2)
def hvac_switch_map(scope):
    """Read-only lookup by status point ID. Each slot tuple follows the source CSV item order."""
    if scope not in ('D', 'E'):
        raise ValueError('暖通启停映射仅支持 D、E 楼')
    source = json.loads(_RESOURCE.read_text(encoding='utf-8'))
    if source['version'] != 1:
        raise ValueError('暖通启停映射版本不匹配')
    result = {}
    occupied = set()
    for row in source['items']:
        if row['scope'] != scope:
            continue
        point = row['point_id']
        if point in result or not row['targets']:
            raise ValueError('暖通启停测点重复或缺少目标数据项')
        slots = {slot: tuple(target['cells'][slot] for target in row['targets']) for slot in _SLOTS}
        for cells in slots.values():
            for cell in cells:
                address = (row['template'], row['sheet'], cell)
                if address in occupied:
                    raise ValueError('暖通启停目标位置重复')
                occupied.add(address)
        result[point] = {'device': row['device'], 'template': row['template'], 'sheet': row['sheet'], 'slots': slots}
    if len(result) != 166 or len(occupied) != 1144:
        raise ValueError('暖通启停映射不完整')
    return result
