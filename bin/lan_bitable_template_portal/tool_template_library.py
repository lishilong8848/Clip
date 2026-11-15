"""Feishu template catalog with immutable snapshots for local orders."""

from collections import Counter
import datetime as dt
import hashlib
import json
import logging
from threading import RLock

from .risk_inspection import RiskError, text
from .tool_self_check import TZ, dumps
from .tool_workspace_workbook import describe


class ToolTemplateLibrary:
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._template_library_lock = RLock()

    def _library_access(self, actor, *, manage=False):
        self.access(actor)
        if not self.available_to(actor) or manage and not self.administrator(actor):
            raise RiskError('当前账号没有工具模板管理权限。' if manage else '当前账号没有工具管理权限。', 403)

    def _has_feishu_templates(self):
        return callable(getattr(self.cloud, 'templates', None))

    @staticmethod
    def _library_stamp(entry):
        return tuple(entry[key] for key in ('name', 'file_token', 'digest'))

    @staticmethod
    def _library_schema(content):
        try:
            return describe(content)['schema']
        except (OSError, ValueError, TypeError, KeyError, AttributeError, SyntaxError) as exc:
            raise RiskError('无法预览此 XLSX 自检模板，请核对文件。') from exc

    def _library_store(self, db, entry, content, schema):
        identity = entry['id']
        latest = db.execute('SELECT version,active FROM tool_check_templates WHERE id=? ORDER BY version DESC LIMIT 1',
                            (identity,)).fetchone()
        version, active = (latest['version'] + 1, latest['active']) if latest else (1, 1)
        db.execute('INSERT INTO tool_check_templates VALUES(?,?,?,?,?,?)',
                   (identity, version, entry['name'], active, content, dumps(schema)))
        db.execute('''INSERT INTO tool_check_template_catalog VALUES(?,?,?,1)
            ON CONFLICT(id) DO UPDATE SET version=excluded.version,entry=excluded.entry,present=1''',
                   (identity, version, dumps(entry)))
        return version

    def _library_sync(self):
        """Called under the library lock. Failed lists preserve the last complete listing."""
        entries = self.cloud.templates()
        with self.connect() as db:
            cached = {row['id']: row for row in db.execute('SELECT * FROM tool_check_template_catalog')}
        changed, visible, unavailable = [], {}, 0
        for entry in entries:
            identity = entry['id']
            old = cached.get(identity)
            if not old or self._library_stamp(entry) != self._library_stamp(json.loads(old['entry'])):
                try:
                    content = self.cloud.download_template(entry)
                    schema = self._library_schema(content)
                except RiskError:
                    unavailable += 1
                    continue
                changed.append((entry, content, schema))
            visible[identity] = entry
        missing = [identity for identity, row in cached.items() if row['present'] and identity not in visible]
        restored = [identity for identity, row in cached.items() if not row['present'] and identity in visible]
        if changed or missing or restored:
            with self.connect(True) as db:
                for entry, content, schema in changed:
                    # Another process may have saved the same revision while the download ran.
                    found = db.execute('SELECT entry FROM tool_check_template_catalog WHERE id=?', (entry['id'],)).fetchone()
                    if not found or self._library_stamp(json.loads(found['entry'])) != self._library_stamp(entry):
                        self._library_store(db, entry, content, schema)
                db.executemany('UPDATE tool_check_template_catalog SET present=0 WHERE id=?', ((key,) for key in missing))
                db.executemany('UPDATE tool_check_template_catalog SET present=1 WHERE id=?', ((key,) for key in restored))
        warning = f'{unavailable} 份飞书模板附件无法读取或校验，暂不可选。' if unavailable else ''
        return entries, warning

    def refresh_template_library(self, actor):
        """Refresh before reading the order board. Existing drafts need no network."""
        self._library_access(actor)
        if not self._has_feishu_templates():
            return ''
        with self._template_library_lock:
            try:
                return self._library_sync()[1]
            except Exception as exc:
                logging.warning('Tool template library refresh failed (%s)', type(exc).__name__)
                return '飞书模板暂时无法读取，正在显示上次核验的目录。'

    def list_templates(self, db, administrator):
        if not self._has_feishu_templates():
            return super().list_templates(db, administrator)
        return [self.template_info(row) for row in db.execute('''
            SELECT t.id,t.version,t.name,t.active,t.schema FROM tool_check_template_catalog c
            JOIN tool_check_templates t ON t.id=c.id AND t.version=c.version
            WHERE c.present=1 AND NOT EXISTS(SELECT 1 FROM tool_check_template_deletions d WHERE d.id=c.id)
            AND (? OR t.active=1) ORDER BY t.name,t.id''', (administrator,))]

    def _select_template(self, db, identity, version):
        if self._has_feishu_templates() and not db.execute(
                'SELECT 1 FROM tool_check_template_catalog WHERE id=? AND version=? AND present=1',
                (identity, version)).fetchone():
            raise RiskError('该模板不是当前可用的飞书模板，请刷新后重新选用。', 409)
        return super()._select_template(db, identity, version)

    def filter_selectable_feishu_templates(self, templates):
        """Apply local tombstones and availability to a workbench fallback list too."""
        with self.connect() as db:
            active = {row['id'] for row in self.list_templates(db, False)}
        return [entry for entry in templates if entry['id'] in active]

    def selectable_feishu_templates(self, actor):
        self._library_access(actor)
        if not self._has_feishu_templates():
            return []
        with self._template_library_lock:
            entries, _ = self._library_sync()
            return self.filter_selectable_feishu_templates(entries)

    @staticmethod
    def _library_usage(db, actor):
        from .tool_management import can_scope

        usage = Counter()
        # Count a template once per unfinished work order (or workbench session item).
        for query in ("SELECT scope,records FROM tool_check_orders WHERE submitted_at IS NULL AND records<>'[]'",
                      "SELECT scope,records FROM tool_work_orders WHERE submitted_at IS NULL AND records<>'[]'"
                      " AND outcome<>'cancelled'"):
            for row in db.execute(query):
                if can_scope(actor, row['scope']):
                    usage.update({entry['template_id'] for entry in json.loads(row['records'])})
        for row in db.execute('''SELECT i.asset,i.template_id FROM tool_workspace_items i
                JOIN tool_workspace_sessions s ON s.id=i.session_id
                WHERE s.status IN ('opening','draft') AND i.template_id<>'' '''):
            if can_scope(actor, json.loads(row['asset'])['scope']):
                usage[row['template_id']] += 1
        return usage

    def template_library(self, actor):
        from .tool_management import can_scope

        self._library_access(actor)
        admin = self.administrator(actor)
        warning = self.refresh_template_library(actor)
        with self.connect() as db:
            db.execute('BEGIN')
            linked = Counter()
            if admin:
                for row in db.execute('SELECT source FROM tool_inventory WHERE active=1'):
                    asset = json.loads(row['source'])
                    if can_scope(actor, asset['scope']) and asset.get('template_id'):
                        linked[asset['template_id']] += 1
            templates = self.list_templates(db, admin) if self._has_feishu_templates() else []
            usage = self._library_usage(db, actor) if admin else Counter()
            legacy = [row | {'source': 'local', 'linked_tools_count': 0, 'open_usage': usage[row['id']]}
                      for row in super().list_templates(db, True) if not row['id'].startswith('rec')] if admin else []
            inventory_at = self.state(db, 'inventory_at')
        return {'templates': [row | {'source': 'feishu', 'linked_tools_count': linked[row['id']],
                                     'open_usage': usage[row['id']]} for row in templates],
                'legacy_templates': legacy, 'can_manage_templates': admin, 'inventory_at': inventory_at,
                'warning': warning}

    @staticmethod
    def _library_version(version):
        if type(version) is not int or version < 1:
            raise RiskError('模板版本无效。')
        return version

    def _library_head(self, db, identity, version, *, include_deleted=False):
        head = db.execute('SELECT * FROM tool_check_template_catalog WHERE id=? AND present=1', (identity,)).fetchone()
        if not head:
            raise RiskError('未找到当前的飞书模板，请刷新后重试。', 404)
        row = self._template(db, identity, include_deleted=include_deleted)
        if head['version'] != version or row['version'] != version:
            raise RiskError('模板已更新，请刷新后重试。', 409)
        return row, json.loads(head['entry'])

    def save_library_template(self, actor, name, content, identity='', version=None):
        self._library_access(actor, manage=True)
        name = text(name, 100, True)
        if identity:
            self._library_version(version)
        elif version is not None:
            raise RiskError('新模板不应指定旧版本。')
        if not isinstance(content, bytes):
            raise RiskError('请提供 XLSX 模板文件。')
        schema = self._library_schema(content)
        digest = hashlib.sha256(content).hexdigest()
        fingerprint = hashlib.sha256(dumps([name, digest]).encode()).hexdigest()

        def save_pending(token, attempted_at):
            with self.connect(True) as db:
                inserted = db.execute('INSERT OR IGNORE INTO tool_check_template_publications VALUES(?,?,?)',
                                      (fingerprint, token, attempted_at))
                if not inserted.rowcount:
                    raise RiskError('同一模板正在发布，请刷新后重试。', 409)

        with self._template_library_lock:
            entries, _ = self._library_sync()
            previous, pending, existing = None, None, None
            with self.connect() as db:
                if identity:
                    row, previous = self._library_head(db, identity, version)
                    if row['name'] == name and row['content'] == content:
                        raise RiskError('没有检测到模板名称或文件变化。', 409)
                else:
                    pending = db.execute('SELECT file_token,attempted_at FROM tool_check_template_publications WHERE fingerprint=?',
                                         (fingerprint,)).fetchone()
                    candidates = [item for item in entries if (item['name'], item['digest'].lower()) == (name, digest)
                                  or pending and item['file_token'] == pending['file_token']]
                    if len(candidates) > 1:
                        raise RiskError('飞书目录中存在重复模板，请核对后再发布。', 409)
                    if candidates:
                        candidate = candidates[0]
                        if not db.execute('SELECT 1 FROM tool_check_template_catalog WHERE id=? AND present=1',
                                          (candidate['id'],)).fetchone():
                            raise RiskError('原模板附件尚未核验，请刷新后重试。', 503)
                        row = self._template(db, candidate['id'])
                        if row['name'] != name or row['content'] != content:
                            raise RiskError('原模板已变化，请核对飞书目录后再操作。', 409)
                        existing = self.template_info(row)
            if existing:
                if pending:
                    with self.connect(True) as db:
                        db.execute('DELETE FROM tool_check_template_publications WHERE fingerprint=?', (fingerprint,))
                return existing | {'existing': True}
            created = self.cloud.publish_template(name, content, previous=previous, pending=pending, save_pending=save_pending)
            with self.connect(True) as db:
                new_version = self._library_store(db, created, content, schema)
                if not identity:
                    db.execute('DELETE FROM tool_check_template_publications WHERE fingerprint=?', (fingerprint,))
                return self.template_info(self._template(db, created['id'], new_version))

    def clone_library_template(self, actor, identity, body):
        self._library_access(actor, manage=True)
        name = text(body.get('name'), 100, True)
        version = self._library_version(body.get('version'))
        with self._template_library_lock:
            self._library_sync()
            with self.connect() as db:
                self._template(db, identity)
                source = self._template(db, identity, version)
            if 'checks' in body or any(key in body for key in ('description', 'specialty', 'cycle', 'applicable', 'precautions')):
                content = self._clone_template_content(source, body, stable=True)
            else:
                content = source['content']
            if name == source['name'] and identity.startswith('rec'):
                raise RiskError('请为副本填写不同的模板名称。', 409)
            return self.save_library_template(actor, name, content)

    def library_template_availability(self, actor, identity, body):
        self._library_access(actor, manage=True)
        active = body.get('active')
        version = self._library_version(body.get('version'))
        if type(active) is not bool:
            raise RiskError('模板启用状态无效。')
        with self._template_library_lock:
            self._library_sync()
            with self.connect(True) as db:
                row, _ = self._library_head(db, identity, version)
                db.execute('UPDATE tool_check_templates SET active=? WHERE id=? AND version=?', (active, identity, version))
                return self.template_info(row) | {'active': active, 'saved': True}

    def delete_library_template(self, actor, identity, body):
        self._library_access(actor, manage=True)
        version = self._library_version(body.get('version'))
        with self._template_library_lock:
            self._library_sync()
            with self.connect(True) as db:
                self._library_head(db, identity, version, include_deleted=True)
                db.execute('INSERT OR IGNORE INTO tool_check_template_deletions VALUES(?,?)',
                           (identity, dt.datetime.now(TZ).isoformat(timespec='seconds')))
        return {'deleted': True}
