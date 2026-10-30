"""Resolve existing notice relations without writes or send-workflow calls.

Warnings mean an explicitly existing required relation could not be verified.
Absent links require nothing; blocked_mappings only block their dirty main-field
names. Mapping destinations may be a string or a list of physical column names.

Callers must persist and validate ALL constraints, even on records without dirty
fields. target_constraints apply to the MAIN record, not the descriptor's row.
Binding constraints are immutable and conflicting edits must be rejected before
writing. On replay, allow_desired constraints accept the original value OR the
exact desired value in the SAME persisted record patch (only if that field is
patched); never derive a new value from today's source/identity. Replay uses the
frozen descriptors, not related_records() again. Schema, scope, revision and
write recovery belong to the caller. snapshot_kind='source' uses source-table
snapshots; snapshot_kind='repair' uses the existing repair/notice snapshots.

Successful corrections may supply a cumulative, success-only local baseline at
notice_history_corrections / baseline:{work_type}:{target_record_id}. Its targets
are keyed by base/table/record, with first-original before and latest desired
fields. Only matching original evidence AND matching live desired values permit
an override; regular identities and drafts are never changed by this resolver.
"""
from __future__ import annotations

from copy import deepcopy
import json
import re

from .identity_utils import is_local_record_id


def _text(value):
    return str(value or "").strip()


def _schema(fields):
    return {item["field_name"]: item for item in fields
            if isinstance(item, dict) and item.get("field_name")}


class _Relations:
    def __init__(self, service, store, work_type, target, fields, identity):
        # Lazy import avoids a portal_service import cycle.
        from . import portal_service as portal

        self.service, self.store, self.portal = service, store, portal
        self.kind, self.target = work_type, target
        self.target_id = _text(target.get("record_id"))
        self.fields = _schema(fields)
        self.identity = identity or {}
        self.descriptors, self.warnings = [], []
        self.schemas = {}

    def ids(self, value):
        if value in (None, "", [], {}):
            return []
        if isinstance(value, dict):
            for key in ("record_ids", "link_record_ids", "record_id", "id", "text"):
                if key in value:
                    return self.ids(value[key])
            raise ValueError("关联字段不是明确记录 ID")
        if isinstance(value, list):
            return list(dict.fromkeys(item for part in value for item in self.ids(part)))
        text = _text(value)
        if text.startswith(("[", "{")):
            try:
                return self.ids(json.loads(text))
            except (ValueError, TypeError):
                raise ValueError("关联字段的记录 ID 列表无效") from None
        ids = re.split(r"[\s,，、;；]+", text)
        if any(not re.fullmatch(r"rec[A-Za-z0-9]+", item) for item in ids):
            raise ValueError("关联字段含非记录 ID 内容，未提取部分文字猜测关联")
        return list(dict.fromkeys(ids))

    def read(self, base, table, record_id):
        if not base or not table or not re.fullmatch(r"rec[A-Za-z0-9]+", record_id):
            raise ValueError("缺少明确表身份或有效记录 ID")
        payload = self.service._request_json(
            f"records/{record_id}", app_token=base, table_id=table)
        row = (payload.get("data") or {}).get("record") or {}
        if row.get("record_id") != record_id or not isinstance(row.get("fields"), dict):
            raise ValueError("未完整读回指定关联记录")
        if (base, table) not in self.schemas:
            self.schemas[base, table] = self.service._load_raw_table_fields(app_token=base, table_id=table)
        return row, self.schemas[base, table]

    def add(self, key, label, base, table, row, schema, mappings, constraints=(), *,
            blocked_mappings=None, snapshot_key="", snapshot_kind="repair", target_constraints=()):
        metas, valid, blocked = _schema(schema), {}, dict(blocked_mappings or {})
        for source, destination in mappings.items():
            destinations = destination if isinstance(destination, list) else [destination]
            missing = [dest for dest in destinations if dest not in metas]
            if source not in self.fields or missing:
                blocked[source] = f"{label}缺少映射字段：" + "、".join(missing or [source])
                continue
            readonly = []
            for meta in (self.fields[source], *(metas[dest] for dest in destinations)):
                flags = (meta, meta.get("property") or {})
                if (int(meta.get("type") or 0) in {19, 20, 1001, 1002, 1003, 1004, 1005}
                        or any(flag.get(name) is True for flag in flags
                               for name in ("is_computed", "is_readonly", "is_read_only", "read_only"))):
                    readonly.append(meta["field_name"])
            if readonly:
                blocked[source] = f"{label}映射字段不可写：" + "、".join(readonly)
            elif source not in blocked:
                valid[source] = deepcopy(destination)
        constraints = deepcopy(list(constraints))
        destinations = {dest for value in valid.values()
                        for dest in (value if isinstance(value, list) else [value])}
        for condition in constraints:
            condition["allow_desired"] = condition["field"] in destinations
        descriptor = dict(key=key, label=label, base_token=base, table_id=table,
                          record_id=row["record_id"], mappings=valid,
                          blocked_mappings=blocked, constraints=constraints,
                          target_constraints=deepcopy(list(target_constraints)),
                          snapshot_key=snapshot_key, snapshot_kind=snapshot_kind,
                          raw_record=deepcopy(row), schema=deepcopy(schema))
        self.descriptors.append(descriptor)
        return descriptor

    def attempt(self, label, action):
        try:
            action()
        except Exception as exc:
            # Do not expose HTTP URLs/base tokens from remote exception messages.
            detail = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
            self.warnings.append(f"{label}未自动联动：{detail}。")

    def original(self):
        identity = self.identity
        payload = identity.get("payload") or {}
        if not isinstance(payload, dict):
            raise ValueError("原持久化身份 payload 缺失")
        payload = dict(payload)
        for name in ("work_type", "target_record_id", "source_record_id", "target_app_token",
                     "target_table_id", "source_app_token", "source_table_id"):
            if not payload.get(name):
                payload[name] = identity.get(name)
        if (identity.get("deleted_at") is not None
                or identity.get("work_type") != self.kind
                or identity.get("target_record_id") != self.target_id
                or payload.get("work_type") != self.kind
                or payload.get("target_record_id") != self.target_id):
            raise ValueError("缺少与本次目标精确一致的原持久化身份")
        cfg = self.portal.config
        attr = {"maintenance": "table_id_weibao", "change": "table_id_biangeng",
                "repair": "table_id_overhaul", "polling": "table_id_polling",
                "adjust": "table_id_tiaozheng", "power": "table_id_power"}[self.kind]
        for key, expected in (("target_app_token", cfg.app_token),
                              ("target_table_id", getattr(cfg, attr))):
            if not expected or identity.get(key) != expected:
                raise ValueError("原目标表身份缺失或与当前配置不同")
            if payload.get(key) and payload[key] != expected:
                raise ValueError("原 payload 的目标表身份与当前配置不同")
        return payload

    def paired(self):
        payload = self.original()
        pair_id = _text(payload.get("paired_maintenance_target_record_id"))
        if not pair_id:
            raise ValueError("原 payload 没有明确的配对维保 ID，未按源记录查询最新配对")
        if pair_id == self.target_id:
            raise ValueError("配对维保 ID 与主目标相同")
        cfg = self.portal.config
        base, table = cfg.app_token, cfg.table_id_weibao
        embedded = payload.get("paired_maintenance_upload") or {}
        if not isinstance(embedded, dict):
            raise ValueError("原配对上下文格式无效")
        for name, value in (("target_record_id", pair_id), ("target_app_token", base),
                            ("target_table_id", table)):
            if embedded.get(name) and embedded[name] != value:
                raise ValueError("原配对上下文与明确配对 ID/当前表配置不一致")
        row, schema = self.read(base, table, pair_id)
        mappings = {name: name for name in ("专业", "楼栋", "执行方", "位置", "内容", "原因", "影响", "进度")}
        mappings["变更开始时间"] = "计划开始时间"
        constraints, target_constraints, blocked = [], [], {}
        end = self.timestamp(self.target["fields"].get("变更结束时间"))
        if (self.finished("change", self.target) and self.finished("maintenance", row)
                and end is not None and self.timestamp(row["fields"].get("实际结束时间")) == end):
            mappings["变更结束时间"] = "实际结束时间"
            constraints = [dict(field=name, value=deepcopy(row["fields"].get(name)))
                           for name in ("实际结束时间", "维保状态")]
            target_constraints = [dict(field="变更状态", value=deepcopy(self.target["fields"].get("变更状态")),
                                       allow_desired=False)]
        else:
            blocked["变更结束时间"] = "须确认两条通告已结束且配对实际结束时间与主目标原结束时间一致"
        self.add("paired_maintenance", "配对维保通告", base, table, row, schema, mappings, constraints,
                 blocked_mappings=blocked, target_constraints=target_constraints,
                 snapshot_key=self.service._notice_target_snapshot_source_key("maintenance"))

    def finished(self, work_type, row):
        life = self.service._target_record_lifecycle(
            work_type=work_type, notice_type=self.service._notice_type_for_work_type(work_type),
            target_record={"record_id": row["record_id"], "display_fields": row["fields"]})
        return life.get("finished") is True

    def timestamp(self, value):
        return self.service._repair_management_datetime_ms(
            self.service._repair_management_plain_text(value))

    def verified_overrides(self, payload, base, table, row, schema, starts, expected):
        # Read only the cumulative baseline, never scan operations or trust a
        # newer pointer that may contain no start/cycle fields.
        from .notice_history import comparable

        baseline = self.store.get_document(
            "notice_history_corrections", f"baseline:{self.kind}:{self.target_id}") or {}
        targets = baseline.get("targets") or {}
        main = targets.get("/".join((payload["target_app_token"], payload["target_table_id"], self.target_id))) or {}
        source = targets.get("/".join((base, table, row["record_id"]))) or {}
        main_before, main_after = main.get("before") or {}, main.get("fields") or {}
        source_before, source_after = source.get("before") or {}, source.get("fields") or {}
        metas, raw, overrides, start_proofs = _schema(schema), row["fields"], {}, []
        main_start = "变更开始时间" if self.kind == "change" else "计划开始时间"
        current_start = next((name for name in starts if raw.get(name) not in (None, "", [], {})), "")
        cycle = lambda value: self.service._normalize_memory_cycle(
            self.service._repair_management_plain_text(value))
        candidates = [("start_time", main_start, name, self.timestamp, self.timestamp(payload.get("start_time")))
                      for name in starts if not current_start or name == current_start]
        candidates.append(("maintenance_cycle", "维保周期", "维护周期", cycle, expected.get("maintenance_cycle")))
        for key, main_name, source_name, normalize, original in candidates:
            if (original in (None, "") or main_name not in self.fields or source_name not in metas
                    or main_name not in main_before or main_name not in main_after
                    or source_name not in source_before or source_name not in source_after):
                continue
            if (normalize(main_before[main_name]) != original or normalize(source_before[source_name]) != original
                    or comparable(self.fields[main_name], self.target["fields"].get(main_name))
                    != comparable(self.fields[main_name], main_after[main_name])
                    or comparable(metas[source_name], raw.get(source_name))
                    != comparable(metas[source_name], source_after[source_name])
                    or normalize(main_after[main_name]) != normalize(source_after[source_name])):
                continue
            overrides[key] = normalize(source_after[source_name])
            if key == "start_time":
                start_proofs.append(source_name)
                overrides["start_field"] = source_name
        if len(start_proofs) != 1:
            overrides.pop("start_time", None)
            overrides.pop("start_field", None)
        return overrides

    def source(self):
        payload = self.original()
        source_id = _text(payload.get("source_record_id"))
        if is_local_record_id(source_id):
            return
        if not source_id or source_id == self.target_id or self.identity.get("source_record_id") != source_id:
            raise ValueError("原身份缺少独立且一致的源计划 ID")
        kind = _text(payload.get("source_work_type") or payload.get("converted_from_work_type") or self.kind)
        if kind == "maintenance":
            base, table = self.service.app_token, self.service.table_id
            starts = ("计划开始维护时间", "计划开始时间")
        elif kind == "change":
            base, table = self.portal.CHANGE_SOURCE_APP_TOKEN, self.portal.CHANGE_SOURCE_TABLE_ID
            starts = ("变更开始日期（阿里）", "计划开始日期（阿里）", "计划开始", "计划开始时间", "计划延迟开始日期")
        else:
            raise ValueError("没有可验证的维保/变更源计划类型")
        for key, value in (("source_app_token", base), ("source_table_id", table)):
            if self.identity.get(key) != value or payload.get(key) != value:
                raise ValueError("原源表身份缺失或与当前源表不同")
        current = self.store.resolve_notice_identity(work_type=self.kind, source_record_id=source_id) or {}
        if current.get("target_record_id") != self.target_id:
            raise ValueError("源计划当前绑定已变化或无法确认，未回写旧通告")
        row, schema = self.read(base, table, source_id)
        raw = row["fields"]
        expected = deepcopy(payload.get("_source_period") or self.service.notice_source_period_snapshot(payload))
        if (not isinstance(expected, dict) or expected.get("source_record_id") != source_id
                or expected.get("source_work_type") != kind):
            raise ValueError("原源计划周期快照缺失或不属于本记录")
        actual = self.service.notice_source_period_snapshot(dict(
            source_record_id=source_id, source_work_type=kind,
            plan_month=self.service._repair_management_plain_text(raw.get("计划维护月份")),
            maintenance_cycle=self.service._repair_management_plain_text(raw.get("维护周期"))))
        overrides = self.verified_overrides(payload, base, table, row, schema, starts, expected)
        if "maintenance_cycle" in overrides:
            expected["maintenance_cycle"] = overrides["maintenance_cycle"]
        constraints = []
        for key, field in (("plan_month", "计划维护月份"), ("maintenance_cycle", "维护周期")):
            if expected.get(key) != actual.get(key) or (kind == "maintenance" and not expected.get(key) and key not in overrides):
                raise ValueError(f"源计划{field}缺少原证据或已变化")
            if actual.get(key) or key in overrides:
                constraints.append(dict(field=field, value=raw.get(field)))
        start_field = overrides.get("start_field") or next((name for name in starts if raw.get(name) not in (None, "", [], {})), "")
        original_start = overrides.get("start_time", self.timestamp(payload.get("start_time")))
        main_start = "变更开始时间" if self.kind == "change" else "计划开始时间"
        if (not start_field or (original_start is None and "start_time" not in overrides)
                or self.timestamp(raw.get(start_field)) != original_start
                or self.timestamp(self.target["fields"].get(main_start)) != original_start):
            raise ValueError("原通告、主目标与源计划完整开始日期不一致，无法排除跨月/跨年复用")
        constraints.append(dict(field=start_field, value=raw.get(start_field)))
        target_constraints = [dict(field=main_start, value=deepcopy(self.target["fields"].get(main_start)),
                                   allow_desired=True)]
        # Explicit read-field aliases only; never treat progress/status or a
        # title assembled from maintenance project components as interchangeable.
        aliases = {
            "位置": ("位置",),
            "执行方": ("执行方",), "影响": ("影响", "影响范围"), "进度": ("进度",),
            "原因": ("原因", "维护原因") if kind == "maintenance" else ("变更原因", "原因"),
            "专业": ("专业类别", "专业") if kind == "maintenance" else ("专业",),
        }
        if kind == "maintenance":
            aliases.update({"内容": ("内容", "维护内容"), "维保周期": ("维护周期",)})
        elif self.kind == "change":
            aliases.update({"名称": ("变更简述",), "阿里-变更等级": ("变更等级（阿里）", "变更等级")})
        metas, mappings, blocked = _schema(schema), {}, {}
        for source, candidates in aliases.items():
            if source not in self.fields:
                continue
            found = [name for name in candidates if name in metas]
            if len(found) == 1:
                mappings[source] = found[0]
            elif len(found) > 1:
                blocked[source] = f"源计划候选字段不唯一：{'、'.join(found)}"
        mappings[main_start] = start_field
        if self.kind != "change":
            mappings.update({"实际开始时间": "实际开始时间", "实际结束时间": "实际结束时间"})
            if kind == "maintenance":
                mappings["计划结束时间"] = "计划结束维护时间"
        elif self.kind == "change":
            end = self.timestamp(self.target["fields"].get("变更结束时间"))
            if self.finished("change", self.target) and end is not None and self.timestamp(raw.get("实际结束时间")) == end:
                mappings["变更结束时间"] = "实际结束时间"
                constraints.append(dict(field="实际结束时间", value=raw["实际结束时间"]))
                target_constraints.append(dict(field="变更状态", value=deepcopy(self.target["fields"].get("变更状态")),
                                               allow_desired=False))
            else:
                blocked["变更结束时间"] = "源计划未确认对应本周期实际结束时间"
        self.add("source_plan", "原源计划", base, table, row, schema, mappings, constraints,
                 blocked_mappings=blocked, snapshot_key=kind, snapshot_kind="source",
                 target_constraints=target_constraints)

    def repair(self):
        p, svc = self.portal, self.service
        parent = p.REPAIR_TARGET_SUMMARY_ID_FIELD_NAME
        ids = self.ids(self.target["fields"].get(parent))
        if not ids:
            return
        if int((self.fields.get(parent) or {}).get("type") or 0) != 1:
            raise ValueError("检修目标缺少文本字段维修汇总记录ID-L，未使用查找引用推断项目")
        if len(ids) != 1:
            raise ValueError("维修汇总记录ID-L为空或不唯一")
        summary_id = ids[0]
        base, table = p.REPAIR_SOURCE_APP_TOKEN, p.REPAIR_MANAGEMENT_TABLE_ID
        row, schema = self.read(base, table, summary_id)
        raw, metas = row["fields"], _schema(schema)
        link = p.REPAIR_MANAGEMENT_REPAIR_LINK_STORAGE_FIELD_NAME
        if int((metas.get(link) or {}).get("type") or 0) != 1 or self.ids(raw.get(link)) != [self.target_id]:
            raise ValueError("项目设备检修关联-L未唯一反向绑定本目标；多目标汇总也不覆盖")
        physical = lambda name: svc._repair_physical_field_name(table, name)
        mappings = {"名称（标题）": physical("检修通告名称"),
                    "进度（完成情况）": physical("维修进展描述"),
                    "故障原因": physical("故障维修原因"),
                    "实际开始时间": physical("维修开始时间"),
                    "实际结束时间": physical("维修结束时间（2026）")}
        descriptor = self.add("repair_project", "关联维修项目", base, table, row, schema,
                              mappings, [dict(field=link, value=raw[link])],
                              blocked_mappings={parent: "历史更正不能重绑维修项目"},
                              snapshot_key=p.REPAIR_SNAPSHOT_SOURCE_PROJECTS,
                              target_constraints=[dict(field=parent,
                                                       value=deepcopy(self.target["fields"][parent]),
                                                       allow_desired=False)])
        event_field = physical("关联事件单")
        self.attempt("关联事件", lambda: self.event(raw, metas, event_field, descriptor))
        self.followups(summary_id, raw, metas, descriptor)

    def event(self, project, metas, field, project_descriptor):
        ids = self.ids(project.get(field))
        if not ids:
            return
        if int((metas.get(field) or {}).get("type") or 0) not in {1, 18, 21}:
            raise ValueError("项目缺少明确事件 ID 字段，未从 lookup19 推断事件")
        if len(ids) != 1:
            raise ValueError("项目关联事件单为空或不唯一")
        base, table, _ = self.service._event_source_config()
        row, schema = self.read(base, table, ids[0])
        self.add("repair_event", "关联事件", base, table, row, schema,
                 {"故障原因": "事件发生原因", "解决方案": "事件解决措施"},
                 snapshot_key=self.portal.REPAIR_SNAPSHOT_SOURCE_EVENTS)
        # Persist this verified edge even when the project itself has no dirty
        # fields. An absent event returned above without requiring a relation.
        project_descriptor["constraints"].append(dict(field=field, value=deepcopy(project[field]),
                                                      allow_desired=False))

    def followups(self, summary_id, project, metas, project_descriptor):
        svc, p = self.service, self.portal
        base, table = p.REPAIR_SOURCE_APP_TOKEN, p.REPAIR_FOLLOWUP_TABLE_ID
        link = svc._repair_physical_field_name(p.REPAIR_MANAGEMENT_TABLE_ID,
                                              p.REPAIR_MANAGEMENT_FOLLOWUP_LINK_FIELD_NAME)
        explicit = []
        if project.get(link) and int((metas.get(link) or {}).get("type") or 0) in {1, 18, 21}:
            try:
                explicit = self.ids(project[link])
            except ValueError as exc:
                self.warnings.append(f"项目已有跟进关联无法解析：{exc}。")
                return
        try:
            _, _, records = svc._load_repair_followups_for_summary(summary_id, limit=None, force_refresh=True)
        except Exception as exc:
            self.warnings.append(f"维修跟进列表读取失败（{type(exc).__name__}），无法确认全部关联记录，未允许保存。")
            if not explicit:
                return
            # Show known rows, but the incomplete list remains a blocking issue.
            records = []
        ids = list(dict.fromkeys([_text(row.get("record_id")) for row in records] + explicit))
        if explicit:
            project_descriptor["constraints"].append(dict(field=link, value=deepcopy(project[link]),
                                                          allow_desired=False))
        parent = svc._repair_physical_field_name(table, p.REPAIR_FOLLOWUP_PARENT_ID_FIELD_NAME)
        physical = lambda name: svc._repair_physical_field_name(table, name)
        for record_id in ids:
            def one():
                row, schema = self.read(base, table, record_id)
                meta = _schema(schema).get(parent) or {}
                if int(meta.get("type") or 0) != 1 or self.ids(row["fields"].get(parent)) != [summary_id]:
                    raise ValueError("跟进的维修汇总记录ID-L未唯一绑定本项目")
                constraints = [dict(field=parent, value=row["fields"][parent])]
                self.add(f"repair_followup:{record_id}", f"维修跟进 {record_id}", base, table,
                         row, schema, {"名称（标题）": physical("维修名称"),
                                       "实际开始时间": physical("维修开始时间"),
                                       "实际结束时间": [physical("维修结束时间"), physical("结束时间")]},
                         constraints, snapshot_key=p.REPAIR_SNAPSHOT_SOURCE_FOLLOWUPS)
            self.attempt(f"维修跟进 {record_id}", one)


def related_records(service, store, work_type, target_record, target_fields, identity):
    """Return (descriptors, warnings); all record/schema reads use current config."""
    resolver = _Relations(service, store, work_type, target_record, target_fields, identity)
    if (not re.fullmatch(r"rec[A-Za-z0-9]+", resolver.target_id)
            or not isinstance(target_record.get("fields"), dict)):
        raise ValueError("主目标记录 ID 或原始字段缺失")
    payload = resolver.identity.get("payload") or {}
    payload = payload if isinstance(payload, dict) else {}
    if work_type == "repair":
        if target_record["fields"].get(resolver.portal.REPAIR_TARGET_SUMMARY_ID_FIELD_NAME):
            resolver.attempt("检修关联项目", resolver.repair)
    elif work_type in {"maintenance", "change"}:
        if work_type == "change" and payload.get("paired_maintenance_target_record_id"):
            resolver.attempt("配对维保", resolver.paired)
        source_id = _text(payload.get("source_record_id") or resolver.identity.get("source_record_id"))
        source_kind = _text(payload.get("source_work_type") or payload.get("converted_from_work_type")
                            or resolver.identity.get("work_type"))
        if (resolver.identity.get("work_type") in {"maintenance", "change"}
                and source_kind in {"maintenance", "change"}
                and not is_local_record_id(source_id) and source_id != resolver.target_id):
            resolver.attempt("原源计划", resolver.source)
    return resolver.descriptors, resolver.warnings
