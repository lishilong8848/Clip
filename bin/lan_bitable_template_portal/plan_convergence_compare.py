"""按 Sheet 场景分层核验智航屏蔽配置。"""

import re

ALL = "__ALL__"
KEY_FIELDS = ("设备域", "关联资源", "关联设备", "关联告警规则")
GROUP_FIELDS = ("设备域", "关联资源")
SEPARATOR = re.compile(r"[,，、;；\r\n]+")


def _text(value):
    return "" if value is None else str(value).strip()


def _is_all(value):
    return _text(value).lower() in ("全部", "全部规则", "全部设备", "all", ALL.lower())


def _tokens(value):
    if _is_all(value):
        return ALL
    return {part.strip() for part in SEPARATOR.split(_text(value)) if part.strip()}


def _merge_values(current, incoming):
    if current == ALL or incoming == ALL:
        return ALL
    return current | incoming


def _values(*raw_values):
    result = set()
    for raw in raw_values:
        result = _merge_values(result, _tokens(raw))
    return result


def _shown(values):
    return ["全部"] if values == ALL else sorted(values)


def _shown_text(values):
    return "全部" if values == ALL else "、".join(sorted(values))


def _pick(data, *keys):
    for key in keys:
        value = _text(data.get(key))
        if value:
            return value
    return ""


def _row_number(row, fallback):
    value = row.get("_excel_row", fallback)
    if type(value) is not int or value < 2:
        raise ValueError("Excel 原始行号必须是大于等于 2 的整数")
    return value


def _record_id(row, row_number):
    return _text(row.get("记录标识") or row.get("记录ID") or row.get("编号") or row.get("ID")) or str(row_number)


def _append_unique(values, value):
    if value and value not in values:
        values.append(value)


def _expected_groups(rows, scenario_name):
    groups = {}
    row_count = 0
    for index, row in enumerate(rows, 2):
        row_count += 1
        row_number = _row_number(row, index)
        missing = [field for field in KEY_FIELDS if not _text(row.get(field))]
        if missing:
            raise ValueError(f"场景 {scenario_name} 第 {row_number} 行缺少必填字段：{'、'.join(missing)}")
        for field in GROUP_FIELDS:
            if SEPARATOR.search(_text(row[field])):
                raise ValueError(f"场景 {scenario_name} 第 {row_number} 行的 {field} 只允许填写一个值；多个值请拆成多行")

        devices, rules = _tokens(row["关联设备"]), _tokens(row["关联告警规则"])
        if not devices or not rules:
            field = "关联设备" if not devices else "关联告警规则"
            raise ValueError(f"场景 {scenario_name} 第 {row_number} 行的 {field} 没有有效值")

        domain, resource = _text(row["设备域"]), _text(row["关联资源"])
        key = (domain, resource)
        group = groups.setdefault(key, {
            "scenario_name": _text(scenario_name),
            "设备域": domain,
            "关联资源": resource,
            "devices": set(),
            "rules": set(),
            "row_numbers": [],
            "record_ids": [],
            "notes": [],
        })
        group["devices"] = _merge_values(group["devices"], devices)
        group["rules"] = _merge_values(group["rules"], rules)
        _append_unique(group["row_numbers"], row_number)
        _append_unique(group["record_ids"], _record_id(row, row_number))
        _append_unique(group["notes"], _text(row.get("备注")))

    if not groups:
        raise ValueError(f"场景 {scenario_name} 没有可比对的规则")
    return groups, row_count


def _actual_groups(details):
    groups, domains = {}, {}
    for detail in details:
        domain = _pick(detail, "classifyModel", "classifyModelName", "domainCode")
        resource = _pick(detail, "spaceModel", "spaceModelName")
        detail_id = _text(detail.get("blockDetailId"))
        row = {
            "domain": domain,
            "resource": resource,
            "devices": _values(detail.get("instances"), detail.get("instanceIds")),
            "rules": _values(detail.get("relateConfig"), detail.get("alarmConfigId")),
            "device_names": _tokens(detail.get("instances")),
            "device_ids": _tokens(detail.get("instanceIds")),
            "rule_names": _tokens(detail.get("relateConfig")),
            "rule_ids": _tokens(detail.get("alarmConfigId")),
            "detail_id": detail_id,
        }
        if domain:
            domains.setdefault(domain, []).append(row)
        if not domain or not resource:
            continue

        group = groups.setdefault((domain, resource), {
            "devices": set(), "rules": set(),
            "device_names": set(), "device_ids": set(),
            "rule_names": set(), "rule_ids": set(),
            "detail_ids": [],
        })
        for field in ("devices", "rules", "device_names", "device_ids", "rule_names", "rule_ids"):
            group[field] = _merge_values(group[field], row[field])
        _append_unique(group["detail_ids"], detail_id)
    return groups, domains


def _missing_values(expected, actual):
    if expected == ALL:
        return [] if actual == ALL else ["全部"]
    if actual == ALL:
        return []
    return sorted(expected - actual)


def _actual_display(group, names_field, ids_field):
    names = group.get(names_field, set())
    return names if names == ALL or names else group.get(ids_field, set())


def _field_result(field, expected, actual, missing, detail_ids):
    return {
        "field_name": field,
        "expected": _shown_text(expected) if not isinstance(expected, str) or expected == ALL else expected,
        "actual": _shown_text(actual) if not isinstance(actual, str) or actual == ALL else actual,
        "missing_values": missing,
        "actual_record_id": "、".join(detail_ids),
    }


def _base_result(expected, actual=None):
    expected_devices, expected_rules = expected["devices"], expected["rules"]
    actual = actual or {}
    actual_devices = _actual_display(actual, "device_names", "device_ids")
    actual_rules = _actual_display(actual, "rule_names", "rule_ids")
    return {
        "scenario_name": expected["scenario_name"],
        "row_number": expected["row_numbers"][0],
        "row_numbers": expected["row_numbers"],
        "record_id": expected["record_ids"][0],
        "record_ids": expected["record_ids"],
        "record_key": f"{expected['设备域']}|{expected['关联资源']}",
        "设备域": expected["设备域"],
        "关联资源": expected["关联资源"],
        "关联设备": _shown_text(expected_devices),
        "关联告警规则": _shown_text(expected_rules),
        "expected_devices": _shown(expected_devices),
        "expected_rules": _shown(expected_rules),
        "actual_devices": _shown(actual_devices),
        "actual_rules": _shown(actual_rules),
        "matched_detail_ids": actual.get("detail_ids", []),
        "备注": "；".join(expected["notes"]),
    }


def _full_action(expected, prefix):
    return f"{prefix}，并配置设备“{_shown_text(expected['devices'])}”、规则“{_shown_text(expected['rules'])}”"


def _compare_group(expected, actual_groups, domains):
    domain, resource = expected["设备域"], expected["关联资源"]
    key = (domain, resource)
    if domain not in domains:
        result = _base_result(expected)
        result.update({
            "missing_stage": "设备域",
            "missing_devices": _shown(expected["devices"]),
            "missing_rules": _shown(expected["rules"]),
            "actual_resources": [],
            "missing_fields": [_field_result("设备域", domain, "", [domain], [])],
            "message": _full_action(expected, f"新增设备/系统类型“{domain}”和关联资源“{resource}”"),
        })
        return result

    if key not in actual_groups:
        domain_rows = domains[domain]
        resources = sorted({row["resource"] for row in domain_rows if row["resource"]})
        detail_ids = []
        for row in domain_rows:
            _append_unique(detail_ids, row["detail_id"])
        result = _base_result(expected)
        result.update({
            "missing_stage": "关联资源",
            "missing_devices": _shown(expected["devices"]),
            "missing_rules": _shown(expected["rules"]),
            "actual_resources": resources,
            "matched_detail_ids": detail_ids,
            "missing_fields": [_field_result("关联资源", resource, "、".join(resources), [resource], detail_ids)],
            "message": _full_action(expected, f"在设备/系统类型“{domain}”下新增关联资源“{resource}”"),
        })
        return result

    actual = actual_groups[key]
    missing_devices = _missing_values(expected["devices"], actual["devices"])
    missing_rules = _missing_values(expected["rules"], actual["rules"])
    if not missing_devices and not missing_rules:
        return None

    actual_devices = _actual_display(actual, "device_names", "device_ids")
    actual_rules = _actual_display(actual, "rule_names", "rule_ids")
    fields, actions = [], []
    if missing_devices:
        fields.append(_field_result("关联设备", expected["devices"], actual_devices, missing_devices, actual["detail_ids"]))
        actions.append("将关联设备配置为“全部”" if missing_devices == ["全部"] else f"补充设备“{'、'.join(missing_devices)}”")
    if missing_rules:
        fields.append(_field_result("关联告警规则", expected["rules"], actual_rules, missing_rules, actual["detail_ids"]))
        actions.append("将关联告警规则配置为“全部”" if missing_rules == ["全部"] else f"补充规则“{'、'.join(missing_rules)}”")

    result = _base_result(expected, actual)
    result.update({
        "missing_stage": "覆盖要求",
        "missing_devices": missing_devices,
        "missing_rules": missing_rules,
        "actual_resources": [resource],
        "missing_fields": fields,
        "message": f"在“{domain} / {resource}”下，{'；'.join(actions)}",
    })
    return result


def _actual_result(key, group, scenario_name):
    domain, resource = key
    devices = _actual_display(group, "device_names", "device_ids")
    rules = _actual_display(group, "rule_names", "rule_ids")
    return {
        "scenario_name": _text(scenario_name),
        "设备域": domain,
        "关联资源": resource,
        "关联设备": _shown_text(devices),
        "关联告警规则": _shown_text(rules),
        "actual_devices": _shown(devices),
        "actual_rules": _shown(rules),
        "matched_detail_ids": group["detail_ids"],
        "record_key": f"{domain}|{resource}",
    }


def compare_rows_to_details(rows, details, block_id="", scenario_name=""):
    if not isinstance(rows, list) or any(not isinstance(row, dict) for row in rows):
        raise ValueError("场景 rows 必须是记录对象数组")
    if not isinstance(details, list) or any(not isinstance(detail, dict) for detail in details):
        raise ValueError("内网明细必须是记录对象数组")

    expected_groups, expected_row_count = _expected_groups(rows, scenario_name)
    actual_groups, domains = _actual_groups(details)
    matched, missing = [], []
    for key, expected in expected_groups.items():
        gap = _compare_group(expected, actual_groups, domains)
        if gap:
            missing.append(gap)
        else:
            matched.append(_base_result(expected, actual_groups[key]))

    extra = [_actual_result(key, group, scenario_name) for key, group in actual_groups.items() if key not in expected_groups]
    stats = {
        "scenario_count": 1,
        "expected_row_count": expected_row_count,
        "expected_group_count": len(expected_groups),
        "matched_group_count": len(matched),
        "missing_group_count": len(missing),
        "actual_group_count": len(actual_groups),
        "extra_group_count": len(extra),
        # 兼容现有页面和结果文件中的计数字段。
        "expected_count": len(expected_groups),
        "matched_count": len(matched),
        "missing_count": len(missing),
        "missing_field_count": sum(len(item["missing_fields"]) for item in missing),
        "missing_combination_count": len(missing),
        "actual_count": len(actual_groups),
        "extra_count": len(extra),
    }
    scenario = _text(scenario_name)
    return {
        "passed": not missing,
        "block_id": _text(block_id),
        "scenario_name": scenario,
        "pull_method": {
            "api": "GET https://usability.meta42.indc.vnet.com/api/alarm/alarmBlock/getAlarmBlockDetail/{blockId}",
            "data_path": "data.alarmBlockDetailResultList",
        },
        "primary_key": ["场景(Sheet)", "设备域", "关联资源"],
        "comparison_method": "同一设备域和关联资源下，分别汇总设备与规则并做集合包含判断",
        "missing_output_format": {
            "row_numbers": "该要求组对应的 Excel 原始行号",
            "missing_stage": "首个未满足层级：设备域、关联资源或覆盖要求",
            "missing_devices": "尚未覆盖的设备；全部表示内网也必须配置全部",
            "missing_rules": "尚未覆盖的规则；全部表示内网也必须配置全部",
            "message": "可直接交给配置人员执行的补配说明",
        },
        "stats": stats,
        "scenario_results": [{"scenario_name": scenario, "stats": dict(stats)}],
        "matched": matched,
        "missing": missing,
        "missing_data_list": missing,
        "extra": extra,
    }


def compare_scenarios_to_details(scenarios, details, block_id=""):
    if not isinstance(scenarios, list) or len(scenarios) != 1 or not isinstance(scenarios[0], dict):
        raise ValueError("一次只能选择一个场景 Sheet 进行核验")
    scenario = scenarios[0]
    name = _text(scenario.get("scenario_name") or scenario.get("name") or scenario.get("sheet"))
    if not name:
        raise ValueError("缺少场景 Sheet 名称")
    return compare_rows_to_details(scenario.get("rows") or [], details, block_id, name)


def _demo():
    expected = [{
        "设备域": "冷却塔", "关联资源": "南通数据中心C",
        "关联设备": "设备A；设备B", "关联告警规则": "规则1,规则2", "_excel_row": 8,
    }]
    details = [
        {"blockDetailId": "d1", "classifyModel": "冷却塔", "spaceModel": "南通数据中心C", "instances": "设备A", "relateConfig": "规则1"},
        {"blockDetailId": "d2", "classifyModel": "冷却塔", "spaceModel": "南通数据中心C", "instances": "设备B", "relateConfig": "规则2"},
    ]
    result = compare_rows_to_details(expected, details, "b1", "制冷单元轮巡")
    assert result["passed"] and result["stats"]["matched_group_count"] == 1

    partial = compare_rows_to_details(expected, details[:1], "b1", "制冷单元轮巡")
    assert partial["missing"][0]["missing_devices"] == ["设备B"]
    assert partial["missing"][0]["missing_rules"] == ["规则2"]
    assert partial["missing"][0]["row_numbers"] == [8]
    devices_only = [details[0], {**details[1], "instances": "设备A"}]
    gap = compare_rows_to_details(expected, devices_only)["missing"][0]
    assert gap["missing_devices"] == ["设备B"] and gap["missing_rules"] == []
    rules_only = [details[0], {**details[1], "relateConfig": "规则1"}]
    gap = compare_rows_to_details(expected, rules_only)["missing"][0]
    assert gap["missing_devices"] == [] and gap["missing_rules"] == ["规则2"]

    duplicated_rows = [expected[0], {**expected[0], "关联设备": "设备C", "关联告警规则": "规则3", "_excel_row": 10}]
    completed = details + [{"blockDetailId": "d3", "classifyModel": "冷却塔", "spaceModel": "南通数据中心C", "instances": "设备C", "relateConfig": "规则3"}]
    merged = compare_rows_to_details(duplicated_rows, completed, "b1", "制冷单元轮巡")
    assert merged["passed"] and merged["stats"]["expected_row_count"] == 2
    assert merged["stats"]["expected_group_count"] == 1 and merged["matched"][0]["row_numbers"] == [8, 10]

    no_domain = compare_rows_to_details(expected, [{**details[0], "classifyModel": "冷冻泵"}], "b1", "制冷单元轮巡")
    assert no_domain["missing"][0]["missing_stage"] == "设备域"
    no_resource = compare_rows_to_details(expected, [{**details[0], "spaceModel": "南通数据中心B"}], "b1", "制冷单元轮巡")
    assert no_resource["missing"][0]["missing_stage"] == "关联资源"

    all_detail = [{**details[0], "instances": "全部", "instanceIds": "all", "relateConfig": "全部规则", "alarmConfigId": "all"}]
    assert compare_rows_to_details(expected, all_detail)["passed"]
    expect_all = [{**expected[0], "关联设备": "全部", "关联告警规则": "全部"}]
    assert not compare_rows_to_details(expect_all, details)["passed"]
    assert compare_rows_to_details(expect_all, all_detail)["passed"]

    placeholder = [{**expected[0], "关联资源": "空间位置", "关联设备": "设备编号"}]
    assert not compare_rows_to_details(placeholder, details)["passed"]
    literal = [{**details[0], "spaceModel": "空间位置", "instances": "设备编号", "relateConfig": "全部规则"}]
    assert compare_rows_to_details([{**placeholder[0], "关联告警规则": "全部"}], literal)["passed"]

    with_extra = details + [{**details[0], "blockDetailId": "extra", "classifyModel": "冷冻泵"}]
    extra = compare_rows_to_details(expected, with_extra)
    assert extra["passed"] and extra["stats"]["extra_group_count"] == 1
    extra_values = details + [{**details[0], "blockDetailId": "d4", "instances": "设备C", "relateConfig": "规则3"}]
    assert compare_rows_to_details(expected, extra_values)["passed"]
    assert not compare_rows_to_details(expected, [{**details[0], "classifyModel": "开式冷却塔"}, details[1]])["passed"]

    try:
        compare_rows_to_details([{**expected[0], "关联设备": ""}], details, scenario_name="制冷单元轮巡")
    except ValueError as error:
        assert "第 8 行缺少必填字段：关联设备" in str(error)
    else:
        raise AssertionError("空白必填字段不能参与核验")


if __name__ == "__main__":
    _demo()
    print("compare_core self-check ok")
