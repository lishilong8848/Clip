"""Read H-duty Zhihang exercises and update the dedicated Feishu table."""
from __future__ import annotations

import calendar
import datetime as dt
import hashlib
import re

from . import plan_convergence_auth as auth
from . import zhihang_data_sources as source
from .zhihang_data_sync import SyncError, TZ, ZhihangBitable, ZhihangSync

KIND = "h_duty_drill"
CREATOR = "南通H值班账户"
BASE = "D01TwFPyXiJBY6kCBDZcMCGLnSe"
TABLE = "tblo1UuKuBfWQNEm"
VIEW = "vewqNLAo7q"
TABLE_URL = f"https://vnet.feishu.cn/wiki/{BASE}?table={TABLE}&view={VIEW}"
TIMES = [f"{hour:02d}:{minute:02d}" for hour in range(24) for minute in (0, 30)]
COLUMNS = {
    "文本": "eventTitle", "演练编号": "eventNumber", "发生时间": "happenTime",
    "楼栋": "dcName", "班组": "exerciseObjectStr teamName dutyTeamName shiftName groupName",
    "专业": "specialtyName majorName exerciseMajor specialty major eventType",
    "响应时间": "responseTime", "确认时间": "ackTime", "恢复时间": "incidentRecoveryTime solveTime",
    "创建人": "creatorName creator", "录制状态": "recordingStatusName recordStatusName",
    "等级": "eventCurrentLevelName eventCurrentLevel eventFirstLevelName eventFirstLevel",
}
TIME_COLUMNS = ("发生时间", "响应时间", "确认时间", "恢复时间")
H_NAMES = {CREATOR.casefold(), "南通h值班账号", "h楼值班账号", "h楼值班账户"}
LOCATIONS = {
    "5": "南通数据中心A", "6": "南通数据中心B", "7": "南通数据中心C",
    "8": "南通基地数据中心_D", "9": "南通基地数据中心_E",
}
BUILDING_OPTIONS = tuple(source._BUILDINGS.values())
FIELD_TYPES = {**dict.fromkeys((*COLUMNS, "演练状态"), 1), "楼栋": 3, **dict.fromkeys(TIME_COLUMNS, 5)}


def field_spec(name):
    field_type = FIELD_TYPES[name]
    spec = {"field_name": name, "type": field_type}
    if field_type == 3:
        spec["property"] = {"options": [{"name": value} for value in BUILDING_OPTIONS]}
    if field_type == 5:
        spec["property"] = {"auto_fill": False, "date_formatter": "yyyy/MM/dd HH:mm"}
    return spec


def _is_h_name(value):
    return source._text(value).replace(" ", "").casefold() in H_NAMES


def _credentials(config, expected_login):
    token = config["token"]
    claims = auth._jwt(token)
    login = source._text(claims.get("UserName"))
    names = [login, *(claims.get(key) for key in ("RealName", "DisplayName", "FullName", "Name", "name"))]
    if not login or not (any(_is_h_name(name) for name in names)
                         or (expected_login and login.casefold() == expected_login.casefold())):
        raise source.ZhihangSourceError(
            "auth_required", "当前智航会话未核验为 H 楼值班账号。请登录 H 楼值班账号，必要时填写该账号的登录名。")
    try:
        return auth.token_auth(token, config)
    except ValueError:
        raise source.ZhihangSourceError("auth_required", "H 楼值班账号登录已过期，请重新登录智航。") from None


def month_query(now):
    start = f"{now:%Y-%m}-01 00:00:00"
    end = f"{now:%Y-%m}-{calendar.monthrange(now.year, now.month)[1]:02d} 23:59:59"
    query = dict.fromkeys(
        "eventTitle eventCurrentLevel orderStatus eventType eventFirstLevel eventHighestLevel "
        "startResponseDate endResponseDate startAskDate endAskDate startIncidentRecoveryDate endIncidentRecoveryDate".split(), "")
    query.update(creator=CREATOR, drill=True, practice=False, domainCodeList=sorted(source._BUILDINGS),
                 startHappendDate=start, endHappendDate=end)
    return query


def pull_h_drills(client, now, login):
    now = source._local(now)
    client.report("核验智航演练楼栋授权")
    response = client.request("h_drill_buildings", {})
    options = response.get("data")
    if not isinstance(options, list) or not all(isinstance(item, dict) for item in options):
        raise source._incomplete("演练楼栋", "无法读取 H 楼值班账号的授权楼栋。")
    domains = {source._first(item, "domainCode") for item in options}
    if not set(source._BUILDINGS) <= domains:
        raise source._incomplete("演练楼栋", "H 楼值班账号的 A–E 楼授权范围不完整。")
    query = month_query(now)
    client.report("读取 H 楼值班演练",
                  日期范围=f"{query['startHappendDate']} 至 {query['endHappendDate']}", 授权楼栋数=len(source._BUILDINGS))
    rows = source._event_pages(client, "发生时间", query, "startHappendDate", "endHappendDate", endpoint="h_drill_list")
    client.report("核对演练编号与创建人", 源端记录=len(rows))
    result = map_h_drills(rows, now, login, options)
    client.report("核对演练编号与创建人", 唯一演练编号=len(result), 去重条数=len(rows) - len(result))
    return {KIND: result}


def map_h_drills(rows, now, login="", options=()):
    location_ids = {source._first(item, "value"): domain for item in options
                    if (domain := source._first(item, "domainCode")) in LOCATIONS and source._first(item, "value")}
    location_names = {name: domain for domain, name in LOCATIONS.items()}
    def map_row(row):
        fields = source._fields(row, COLUMNS)
        happened = source._date(fields["发生时间"])
        if (not happened or happened.strftime("%Y-%m") != now.strftime("%Y-%m")
                or not fields["文本"] or not fields["演练编号"]):
            raise source._incomplete("H楼值班演练", "有记录缺少发生时间、演练名称或演练编号，或超出当月。")
        if not (_is_h_name(fields["创建人"]) or (login and fields["创建人"].casefold() == login.casefold())):
            raise source._incomplete("H楼值班演练", "接口返回其他账号创建的演练，未纳入本批。")
        if source._text(row.get("drill")).casefold() in {"0", "false"} or source._text(row.get("practice")).casefold() in {"1", "true"}:
            raise source._incomplete("H楼值班演练", "接口返回了非正式演练。")
        domain = source._first(row, "domainCode")
        if not domain:
            domain = location_ids.get(source._first(row, "dcCode"), "") or next(
                (code for label, code in location_names.items() if fields["楼栋"].endswith(label)), "")
        if not domain:
            building = source._building(fields["楼栋"])
            domain = next((code for code, name in source._BUILDINGS.items() if name == building), "")
        if domain not in LOCATIONS:
            raise source._incomplete("H楼值班演练", "接口返回 A–E 楼范围外或楼栋无法核验的演练。")
        fields["楼栋"] = source._BUILDINGS[domain]
        for column in TIME_COLUMNS:
            if fields[column]:
                moment = source._date(fields[column])
                if moment is None:
                    raise source._incomplete("H楼值班演练", f"{column}格式无法核验。")
                fields[column] = f"{moment:%Y-%m-%d %H:%M:%S}"
        fields["创建人"] = CREATOR
        if not fields["班组"] and isinstance(row.get("exerciseObjectList"), list):
            groups = dict.fromkeys(source._first(item, "classGroup") for item in row["exerciseObjectList"]
                                   if isinstance(item, dict))
            fields["班组"] = "、".join(f"{group}班" if re.fullmatch(r"[A-Z]", group) else group
                                     for group in groups if group)
        fields["演练状态"] = source._first(row, "orderStatusName", "eventStatusName") or source._mapped(
            row, "orderStatus", source._ORDER_STATUS | {"5": "待评估", "11": "完成", "12": "评估待审批", "13": "终止"})
        if not fields["录制状态"]:
            recording = source._first(row, "hasRecord", "recording", "recorded").casefold()
            fields["录制状态"] = {"0": "未录制", "false": "未录制", "1": "已录制", "true": "已录制"}.get(recording, "")
        return fields

    unique = {}
    for row in rows:
        fields = map_row(row)
        key = source.identity(KIND, fields)
        if key in unique and fields != unique[key]:
            raise source._incomplete("H楼值班演练", "本批同一演练编号存在不同内容，未上传。")
        unique[key] = fields
    if not unique:
        raise source.ZhihangSourceError("empty", "当月没有 H 楼值班演练记录。")
    return unique


class HDutyDrillSync(ZhihangSync):
    namespace = "zhihang_h_drill"
    tasks = {KIND: ("H楼值班演练", TIMES, (TABLE, VIEW))}

    def __init__(self, store, *, cloud=None, pull=None):
        super().__init__(store, cloud=cloud or (lambda table: ZhihangBitable(table, BASE)), pull=pull)

    def _config(self):
        saved = self.read("credentials")
        return saved if saved.get("token") else auth.load_config(self.store)

    def save_login(self, token):
        config = auth.DEFAULTS | {"token": token}
        with self.lock:
            try:
                _credentials(config, self.settings(KIND).get("login_name", ""))
            except source.ZhihangSourceError as error:
                raise SyncError(str(error)) from None
            self.save("credentials", config)

    def _account(self):
        config = self._config()
        expected = self.settings(KIND).get("login_name", "")
        credentials = _credentials(config, expected)
        return credentials, source._text(auth._jwt(config["token"]).get("UserName"))

    def settings(self, kind):
        return super().settings(kind) | {"confirmed": True, "notify": False}

    def _pull(self, kind, now, *, report, preview):
        credentials, login = self._account()
        return pull_h_drills(source._Source(report, preview=preview, credentials=credentials), now, login)

    def configure_h(self, data):
        with self.lock:
            before = self.settings(KIND)
            enabled = data.get("enabled", before["enabled"])
            raw_login = data.get("login_name", before.get("login_name", ""))
            if type(enabled) is not bool or not isinstance(raw_login, str):
                raise SyncError("H楼值班账号或定时设置格式不正确。")
            login = raw_login.strip()
            if len(login) > 80 or re.search(r"[\x00-\x20\x7f]", login):
                raise SyncError("请输入实际的 H 楼值班登录名。")
            identity_changed = login != before.get("login_name", "")
            if identity_changed and (enabled or before["enabled"] or any(
                    run["status"] in {"queued", "running", "pending_verify"} for run in self.history())):
                raise SyncError("请先关闭定时并等待本批结束，再修改 H 楼值班登录名。")
            if enabled and not before["enabled"]:
                self._account()
                if not self._latest_verified_preview(before.get("account_changed_at", "")):
                    raise SyncError("请先使用当前 H 楼值班账号完成一次拉取预览。")
            stamp = dt.datetime.now(TZ).isoformat()
            value = dict(before, enabled=enabled, login_name=login, confirmed=True)
            if enabled != before["enabled"]:
                value["changed_at"] = stamp
            if identity_changed:
                value["account_changed_at"] = stamp
                self.store.delete_document(self.namespace, "pause:" + KIND)
            self.save("settings:" + KIND, value)
            return self.status()

    def _latest_verified_preview(self, since):
        revision = self._session_revision()
        return next((entry for entry in self.history() if entry["mode"] == "preview" and entry["status"] == "success"
                     and entry["created_at"] >= since and entry.get("source_revision") == revision), None)

    def config_revision(self):
        return hashlib.sha256(self._config()["token"].encode()).hexdigest()

    def _session_revision(self):
        return self.config_revision()

    def execute(self, run):
        # Verify the live account even if a previously checked batch is being resumed.
        if self.pull is None:
            self._account()
        revision = self._session_revision()
        if run.get("preview_id") and self.read("run:" + run["preview_id"]).get("source_revision") != revision:
            if not self.read("journal:" + run["id"]):
                raise SyncError("智航登录会话已变化，请重新拉取预览。")
        if run["mode"] == "preview":
            run["source_revision"] = revision
        try:
            super().execute(run)
        except source.ZhihangSourceError as error:
            if error.code != "empty" or run["mode"] == "sync":
                raise
            run.update(stage="当月无演练记录", counts={"total": 0, "create": 0, "update": 0, "delete": 0})

    def enqueue(self, kind, mode, preview_id="", slot=""):
        if mode == "sync" and (not isinstance(preview_id, str) or
                               not self.read("run:" + preview_id).get("counts", {}).get("total")):
            raise SyncError("请先拉取有记录的演练预览。")
        return super().enqueue(kind, mode, preview_id, slot)

    def diff(self, kind, target_kind, rows, remote, schema, reference, mapper, *, report=None):
        for fields in remote.values():
            key = mapper.identity(kind, fields)
            creator = source._first(fields, "创建人")
            if key in rows and creator and not _is_h_name(creator):
                raise SyncError("原表同一演练编号已有其他创建人的记录，请核对。")
        return super().diff(kind, target_kind, rows, remote, schema, reference, mapper, report=report)

    def prepare_schema(self, run, client, schema):
        wrong = [field for field in ("楼栋", *TIME_COLUMNS)
                 if field in schema and schema[field] != FIELD_TYPES[field]]
        if wrong:
            raise SyncError("请先核对飞书原表的字段类型：" + "、".join(wrong))
        missing = {field: kind for field, kind in FIELD_TYPES.items() if field not in schema}
        if run["mode"] != "preview":
            for field in missing:
                client.request("POST", "fields", field_spec(field))
        return schema | missing

    def status(self):
        cfg = self.settings(KIND)
        try:
            self._account()
            account_error = ""
        except source.ZhihangSourceError as error:
            account_error = str(error)
        history = self.history()
        return {"enabled": cfg["enabled"], "login_name": cfg.get("login_name", ""),
                "account_ready": not account_error, "account_error": account_error, "table_url": TABLE_URL,
                "latest": self.public(history[0]) if history else None,
                "history": [self.public(row) for row in history[:12]]}

    def records(self):
        records = [row["fields"] for row in self.cloud(TABLE).list_all() if source._first(row["fields"], "演练编号")]
        records.sort(key=lambda row: source._date(row.get("发生时间")) or dt.datetime.min.replace(tzinfo=TZ), reverse=True)
        rows = []
        for record in records:
            fields = {field: source._first(record, field) for field in FIELD_TYPES}
            for column in TIME_COLUMNS:
                if moment := source._date(fields[column]):
                    fields[column] = f"{moment:%Y-%m-%d %H:%M:%S}"
            rows.append(fields)
        return {"rows": rows}
