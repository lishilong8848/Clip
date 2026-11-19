"""Read-only tool history and a durable, building-scoped reminder outbox."""

import datetime as dt
import json
import logging
import time
import uuid
from collections import Counter, defaultdict

from .risk_inspection import RiskError, text
from .tool_self_check import BUILDINGS, TZ, dumps


HISTORY_LIMIT = 50
AUDIT_LIMIT = 20
CARD_LIMIT = 10
RETRY_SECONDS = 60
LEASE_SECONDS = 300
REMINDER_PREFIX = 'tool_building_reminder:'


def building_reminder_uuid(scope, day):
    """Stable transport idempotency key; day is the Beijing ISO calendar date."""
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f'tool-management-reminder:{scope}:{day}'))


def _date(value):
    try:
        parsed = dt.date.fromisoformat(value)
        return parsed if parsed.isoformat() == value else None
    except (TypeError, ValueError):
        return None


def _inspection_fields(order):
    if order['kind'] != 'external' or not order['submitted_at'] or order['outcome'] == 'cancelled':
        return {}
    try:
        meta = json.loads(order['inspection_meta']) if 'inspection_meta' in order.keys() else {}
    except (TypeError, ValueError):
        return {}
    if not isinstance(meta, dict) or meta.get('result') not in ('qualified', 'unqualified'):
        return {}
    expires = _date(meta.get('expires_on'))
    actual = _date(order['actual'])
    return {
        'report_no': meta['report_no'] if isinstance(meta.get('report_no'), str) else '',
        'institution': meta['institution'] if isinstance(meta.get('institution'), str) else '',
        'result': meta['result'],
        'expires_on': expires.isoformat() if meta['result'] == 'qualified' and expires and actual and expires >= actual else '',
    }


def _external_lifecycle(asset, orders, today):
    from .tool_management import CYCLES, configured_kind, month_offset

    cycle = CYCLES.get(asset.get('external_cycle'))
    last = _date(asset.get('external_last'))
    latest, open_due = None, None
    for row in orders:
        if row['kind'] != 'external' or row['outcome'] == 'cancelled':
            continue
        if row['submitted_at'] and (actual := _date(row['actual'])):
            rank = (actual, row['submitted_at'], row['id'])
            if latest is None or rank > latest[0]:
                latest = rank, row
        elif not row['submitted_at'] and (due := _date(row['due'])):
            open_due = min(open_due, due) if open_due else due

    certificate, inspection_due = None, None
    if latest and (last is None or latest[0][0] >= last):
        last = latest[0][0]
        order = latest[1]
        meta = _inspection_fields(order)
        inspection_due = _date(meta.get('expires_on')) or (month_offset(last, cycle) if cycle else None)
        if meta.get('result') == 'unqualified':
            inspection_due, valid_until, validity = last, '', 'unqualified'
        elif meta.get('result') == 'qualified' and inspection_due:
            valid_until = inspection_due.isoformat()
            validity = 'expired' if inspection_due < today else 'valid'
        else:
            valid_until, validity = '', 'unknown'
        certificate = {'order_id': order['id'], 'actual': last.isoformat(), **meta,
                       'valid_until': valid_until, 'validity': validity}

    next_due = None
    if configured_kind(asset)[0] == 'external' and not asset.get('retired'):
        if not inspection_due and last:
            inspection_due = month_offset(last, cycle)
        # Once the work order exists its due date applies. An earlier known inspection
        # deadline still applies, including when an unqualified result needs a retest.
        if open_due:
            next_due = min(open_due, inspection_due) if inspection_due else open_due
        else:
            next_due = inspection_due or _date(asset.get('external_first')) or today.replace(day=1)
    return {'next_external_due': next_due.isoformat() if next_due else '', 'certificate': certificate}


def _history_order(db, row):
    from .tool_management import DESTINATIONS

    audit = [dict(entry) for entry in db.execute('''SELECT action,name,at,detail FROM tool_work_audit
        WHERE order_id=? ORDER BY id DESC LIMIT ?''', (row['id'], AUDIT_LIMIT))]
    for entry in audit:
        if entry['action'] == '核验并接续旧工作台归档':
            entry['detail'] = ''  # Internal workspace and remote ledger identifiers.
    review = row['review_status'] or ''
    records = json.loads(row['records'])[:AUDIT_LIMIT] if row['kind'] == 'self' and row['submitted_at'] else []
    names = [row['owner_name'], *(entry['name'] for entry in audit)]
    names.extend(record.get(field) for record in records for field in ('inspector', 'reviewer'))
    sync = [{'label': DESTINATIONS.get(job['dest'], job['dest']),
             'state': 'done' if job['synced'] == job['revision'] else 'pending'} for job in db.execute(
                 'SELECT dest,synced,revision FROM tool_work_sync WHERE order_id=? ORDER BY dest', (row['id'],))]
    return {key: row[key] for key in (
        'id', 'month', 'scope', 'kind', 'due', 'actual', 'submitted_at', 'outcome', 'owner_name',
        'reason', 'resolution', 'scrap_reason', 'scrap_status', 'report_name',
    )} | {
        'status': ('cancelled' if row['outcome'] == 'cancelled' else 'review_pending' if review == 'pending'
                   else 'submitted' if row['submitted_at'] else 'pending'),
        'review_status': review, 'review_note': row['review_note'] or '',
        'notes': [record['notes'] for record in records if record.get('notes')],
        'members': list(dict.fromkeys(filter(None, names))), 'history': audit[::-1], 'sync': sync,
        'inspection': _inspection_fields(row),
    }


def _legacy_workspace_history(service, db, actor, asset_id):
    entries = []
    for row in db.execute("""SELECT s.id,s.kind,s.status,s.created_at,s.submitted_at,s.owner_name,s.report_name,i.asset
        FROM tool_workspace_items i JOIN tool_workspace_sessions s ON s.id=i.session_id
        WHERE i.asset_id=? AND i.order_id='' AND i.deleted_at='' AND s.status IN ('pending','archived')
        ORDER BY s.created_at DESC""", (asset_id,)):
        try:
            service._workspace_session(db, actor, row['id'], status_only=True)
        except RiskError as exc:
            if exc.status == 404:
                continue
            raise
        entries.append({
            'id': row['id'], 'workspace_id': row['id'], 'kind': row['kind'],
            'month': row['created_at'][:7], 'scope': json.loads(row['asset'])['scope'],
            'status': 'submitted', 'due': '', 'actual': '', 'submitted_at': row['submitted_at'],
            'owner_name': row['owner_name'], 'report_name': row['report_name'],
            'sync': [{'state': 'done' if row['status'] == 'archived' else 'pending'}], 'inspection': {},
        })
        if len(entries) > HISTORY_LIMIT:
            break
    return entries


def _reminder_card(scope, day, sections):
    labels = {'review': '待审核', 'late': '逾期未办结（含往月）', 'self_draft': '自检待填写', 'external': '送检待填写',
              'certificate': '送检证书到期（7 天内或已逾期）', 'retest': '检测不合格，待复检'}
    elements = []
    for kind, label in labels.items():
        rows = sections.get(kind, [])
        if not rows:
            continue
        lines = [f'{label} · {len(rows)} 张']
        grouped = Counter(rows)
        for (due, name), count in sorted(grouped.items())[:CARD_LIMIT]:
            clean_name = ' '.join(name.split())[:100] or '未命名工具'
            lines.append(f'• {clean_name}' + (f'（{due}）' if due else '') + (f' × {count} 张' if count > 1 else ''))
        omitted = sum(count for _, count in sorted(grouped.items())[CARD_LIMIT:])
        if omitted:
            lines.append(f'另有 {omitted} 张，请在工具管理查看。')
        elements.append({'tag': 'div', 'text': {'tag': 'plain_text', 'content': '\n'.join(lines)}})
    return {'schema': '2.0', 'header': {'title': {'tag': 'plain_text', 'content': f'{scope}楼工具管理提醒 · {day}'},
                       'template': 'blue'}, 'body': {'elements': elements}}


class ToolManagementLifecycle:
    @staticmethod
    def _tool_ledger_receipts(db, key=''):
        return db.execute("""SELECT * FROM (
            SELECT w.id AS receipt_key, w.id AS order_id, '' AS session_id, w.asset_id,
                   s.dest, s.remote_id, w.id || '/' || s.dest AS flow_key, s.revision
            FROM tool_work_orders w JOIN tool_work_sync s ON s.order_id=w.id
            WHERE w.ledger_deleted_at='' AND s.dest='maintenance' AND s.remote_id<>''
              AND s.synced=s.revision AND s.lease_until=0
            UNION ALL
            SELECT i.session_id || ':' || i.asset_id, '', i.session_id, i.asset_id,
                   CASE WHEN i.ledger_table='unified' OR b.kind='self' THEN 'maintenance' ELSE 'inspection' END,
                   i.remote_id, 'workspace:' || i.session_id || ':' || i.asset_id, b.revision
            FROM tool_workspace_items i JOIN tool_workspace_sessions b ON b.id=i.session_id
            WHERE i.order_id='' AND i.remote_id<>'' AND i.deleted_at='' AND b.status='archived'
              AND b.lease_until=0
            ) WHERE (?='' OR receipt_key=?)""", (key, key)).fetchall()

    @staticmethod
    def _tool_delete_job(db, key):
        return db.execute('SELECT * FROM tool_ledger_deletions WHERE receipt_key=?', (key,)).fetchone()

    def _queue_tool_ledger_delete(self, db, actor, key):
        if not self._tool_delete_job(db, key):
            receipts = self._tool_ledger_receipts(db, key)
            if len(receipts) != 1:
                raise RiskError('尚未取得唯一的飞书台账回执，请先核对归档状态。', 409)
            db.execute('''INSERT INTO tool_ledger_deletions(receipt_key,receipt,actor,name)
                VALUES(?,?,?,?)''', (key, dumps(dict(receipts[0])), actor['id'], actor['name']))
        return {'deleted': False, 'deletion_pending': True}

    def _confirm_tool_ledger_deleted(self, db, receipt, actor):
        from .tool_management import clock

        at = clock().isoformat(timespec='seconds')
        order, batch, asset = (receipt[field] for field in ('order_id', 'session_id', 'asset_id'))
        if order:
            updated = db.execute("""UPDATE tool_work_orders SET ledger_deleted_at=?,revision=revision+1
                WHERE id=? AND ledger_deleted_at=''""", (at, order))
            if not updated.rowcount:
                return
            db.execute('DELETE FROM tool_work_sync WHERE order_id=?', (order,))
            db.execute("""UPDATE tool_work_archives SET needs_refresh=1 WHERE month=
                (SELECT month FROM tool_work_orders WHERE id=?)""", (order,))
            items = db.execute("SELECT session_id,asset_id FROM tool_workspace_items WHERE order_id=?", (order,)).fetchall()
        else:
            items = [{'session_id': batch, 'asset_id': asset}]
        for item in items:
            update = db.execute("""UPDATE tool_workspace_items SET deleted_at=?
                WHERE session_id=? AND asset_id=? AND deleted_at=''""", (at, item['session_id'], item['asset_id']))
            if update.rowcount:
                self._workspace_bump(db, item['session_id'])
        self.audit(db, order or batch, actor, '核验飞书工具台账已删除', asset)

    def tool_ledger_delete_one(self):
        now, lease = int(time.time()), uuid.uuid4().hex
        with self.connect(True) as db:
            job = db.execute('''SELECT * FROM tool_ledger_deletions
                WHERE next_try<=? AND lease_until<=? ORDER BY next_try,receipt_key LIMIT 1''', (now, now)).fetchone()
            if not job:
                return False
            key = job['receipt_key']
            receipt = json.loads(job['receipt'])
            db.execute('UPDATE tool_ledger_deletions SET lease=?,lease_until=? WHERE receipt_key=?',
                       (lease, now + 300, key))
        try:
            self.cloud.delete_ledger_record(receipt['dest'], receipt['remote_id'], receipt['flow_key'])
        except Exception as exc:
            error = str(exc) if isinstance(exc, RiskError) else '飞书删除暂未确认，后台将重试原记录。'
            with self.connect(True) as db:
                db.execute("""UPDATE tool_ledger_deletions SET error=?,next_try=?,lease='',lease_until=0
                    WHERE receipt_key=? AND lease=?""", (error, int(time.time()) + 60, key, lease))
        else:
            with self.connect(True) as db:
                if db.execute('DELETE FROM tool_ledger_deletions WHERE receipt_key=? AND lease=?', (key, lease)).rowcount:
                    self._confirm_tool_ledger_deleted(db, receipt, {'id': job['actor'], 'name': job['name']})
        return True

    def reconcile_tool_ledger_deletions(self):
        with self.connect() as db:
            by_table = defaultdict(list)
            for row in self._tool_ledger_receipts(db):
                by_table[row['dest']].append(dict(row))
        for dest, receipts in by_table.items():
            try:
                live = self.cloud.ledger_record_ids(dest)
                for receipt in receipts:
                    if receipt['remote_id'] in live:
                        continue
                    if self.cloud.ledger_exists(dest, receipt['remote_id'], receipt['flow_key']):
                        continue
                    with self.connect(True) as db:
                        key = receipt['receipt_key']
                        current = self._tool_ledger_receipts(db, key)
                        if len(current) == 1 and dict(current[0]) == receipt and not self._tool_delete_job(db, key):
                            self._confirm_tool_ledger_deleted(db, receipt, {'id': 'system', 'name': '飞书台账'})
            except Exception:
                logging.exception('Tool ledger deletion read-back deferred for %s', dest)

    def tool_history(self, actor, asset_id):
        """Return a capped history, with current scope and per-order permissions enforced."""
        from .tool_management import can_scope, clock, configured_kind

        self.access(actor)
        asset_id = text(asset_id, 120, True)
        missing = RiskError('工具不存在或当前账号无权查看。', 404)
        with self.connect() as db:
            db.execute('BEGIN')
            inventory = db.execute('SELECT * FROM tool_inventory WHERE id=? AND active=1', (asset_id,)).fetchone()
            asset = self._workspace_inventory_asset(inventory) if inventory else None
            if asset and not can_scope(actor, asset['scope']):
                raise missing
            visible = [row for row in db.execute('''SELECT work.id,work.month,work.scope,work.kind,work.due,work.asset,
                work.supervisor,work.manager,work.actual,work.submitted_at,work.outcome,work.owner_name,work.owner,work.workspace_direct,work.records,
                work.reason,work.resolution,work.scrap_reason,work.scrap_status,work.report_name,work.inspection_meta,
                review.status AS review_status,review.note AS review_note
                FROM tool_work_orders work LEFT JOIN tool_work_reviews review ON review.order_id=work.id
                WHERE work.asset_id=? AND work.ledger_deleted_at=''
                ORDER BY work.month DESC,COALESCE(work.submitted_at,work.due) DESC,work.id DESC''', (asset_id,))
                if self.visible(db, actor, row)]
            if asset:
                scope = asset['scope']
                scoped = [row for row in visible if row['scope'] == scope]
                if not scoped and not self._workspace_allowed(db, actor, asset, configured_kind(asset)[0]):
                    raise missing
            elif visible:
                asset = json.loads(visible[0]['asset'])
                scoped = [row for row in visible if row['scope'] == visible[0]['scope']]
            else:
                raise missing
            lifecycle = _external_lifecycle(asset, scoped, clock().date())
            if not inventory:
                lifecycle['next_external_due'] = ''
            orders = [_history_order(db, row) for row in visible[:HISTORY_LIMIT]]
            legacy = _legacy_workspace_history(self, db, actor, asset_id)
            orders.extend(legacy)
            orders.sort(key=lambda row: (row['month'], row['submitted_at'] or row['due'], row['id']), reverse=True)
            return {'asset': {key: asset.get(key) for key in ('id', 'scope', 'name', 'location', 'model', 'quantity')},
                    'orders': orders[:HISTORY_LIMIT], 'has_more': len(visible) + len(legacy) > HISTORY_LIMIT, **lifecycle}

    def _building_reminder_cards(self, db, today):
        from .tool_management import configured_kind

        pending = defaultdict(lambda: defaultdict(list))
        day = today.isoformat()
        limit = (today + dt.timedelta(days=7)).isoformat()
        for row in db.execute('''SELECT work.scope,work.kind,work.due,work.asset,work.workspace_direct,review.status AS review_status
            FROM tool_work_orders work LEFT JOIN tool_work_reviews review ON review.order_id=work.id
            WHERE work.submitted_at IS NULL AND work.outcome=''
            AND EXISTS (SELECT 1 FROM tool_workspace_items item WHERE item.order_id=work.id)
            AND (review.status='pending' OR (work.due<>'' AND
                (work.due<? OR (work.kind='external' AND work.due<=?))))''', (day, limit)):
            if row['scope'] not in BUILDINGS:
                continue
            group = ('review' if row['review_status'] == 'pending' else
                     'external' if row['kind'] == 'external' else 'self_draft' if row['workspace_direct'] else 'late')
            pending[row['scope']][group].append((row['due'], json.loads(row['asset']).get('name') or ''))
        history, scrapped, blocked = defaultdict(list), defaultdict(int), set()
        for row in db.execute('''SELECT id,asset_id,scope,kind,due,actual,submitted_at,outcome,scrap_qty,inspection_meta,ledger_deleted_at
                FROM tool_work_orders WHERE submitted_at IS NOT NULL OR outcome IN ('repair','scrap','scrapped')'''):
            if row['outcome'] in ('repair', 'scrap') and not row['ledger_deleted_at']:
                blocked.add(row['asset_id'])
            elif row['outcome'] == 'scrapped':
                scrapped[row['asset_id']] += row['scrap_qty']
            if row['kind'] == 'external' and row['submitted_at'] and row['outcome'] != 'cancelled' and not row['ledger_deleted_at']:
                history[(row['scope'], row['asset_id'])].append(row)
        for row in db.execute('SELECT source,config FROM tool_inventory WHERE active=1'):
            asset = self._workspace_inventory_asset(row)
            scope, asset_id = asset.get('scope'), asset['id']
            if (scope not in BUILDINGS or asset.get('retired') or configured_kind(asset)[0] != 'external'
                    or asset_id in blocked or scrapped[asset_id] >= (asset.get('quantity') or 0)):
                continue
            records = history[(scope, asset_id)]
            if not (_date(asset.get('external_last')) or _date(asset.get('external_first'))
                    or any(_date(record['actual']) for record in records)):
                continue
            lifecycle = _external_lifecycle(asset, records, today)
            certificate = lifecycle['certificate'] or {}
            due = lifecycle['next_external_due']
            if due and due <= limit:
                group = 'retest' if certificate.get('validity') == 'unqualified' else 'certificate'
                pending[scope][group].append((due, asset['name']))
        return {scope: _reminder_card(scope, day, groups) for scope, groups in pending.items()}

    def send_building_reminders(self, deliver, current=None, *, link_for_scope=None):
        """Call deliver(scope, Beijing ISO day, Feishu card); bool or (bool, message) receipt.

        Use building_reminder_uuid in the delivery adapter. An unconfirmed attempt
        retries with the same UUID while the building still has actionable orders. Claim and receipt
        writes are local and the callback runs outside any database transaction.
        """
        from .tool_management import clock

        live_clock = current is None
        current = current or clock()
        current = current.astimezone(TZ) if current.tzinfo is not None else current.replace(tzinfo=TZ)
        result = {'sent': [], 'failed': []}
        if current.hour < 9:
            return result
        day = current.date().isoformat()
        keys = {scope: f'{REMINDER_PREFIX}{day}:{scope}' for scope in BUILDINGS}
        with self.connect() as db:
            db.execute('BEGIN')
            states = {scope: json.loads(self.state(db, key) or '{}') for scope, key in keys.items()}
            cards = self._building_reminder_cards(db, current.date()) if any(not state.get('sent') for state in states.values()) else {}
        if link_for_scope:
            for scope, card in list(cards.items()):
                if states[scope].get('sent'):
                    continue
                url = link_for_scope(scope)
                if not url:
                    del cards[scope]
                    continue
                card['body']['elements'].append({
                    'tag': 'button', 'text': {'tag': 'plain_text', 'content': '去灯塔办理'}, 'type': 'primary',
                    'behaviors': [{'type': 'open_url', 'default_url': url}],
                })
        for scope, key in keys.items():
            if scope not in cards or states[scope].get('sent'):
                continue
            attempt = clock() if live_clock else current
            if attempt.date() != current.date():
                break
            now = attempt.timestamp()
            lease = uuid.uuid4().hex
            with self.connect(True) as db:
                state = json.loads(self.state(db, key) or '{}')
                if state.get('sent') or max(state.get('lease_until', 0), state.get('retry_at', 0)) > now:
                    continue
                card = cards.get(scope)
                if not card:
                    continue
                self.set_state(db, key, dumps({'card': card, 'lease': lease, 'lease_until': now + LEASE_SECONDS}))
            try:
                receipt = deliver(scope, day, card)
                ok = (receipt[0] if isinstance(receipt, tuple) and receipt else receipt) is True
            except Exception:
                ok = False
            with self.connect(True) as db:
                state = json.loads(self.state(db, key) or '{}')
                if state.get('lease') == lease:
                    self.set_state(db, key, dumps({'sent': True} if ok else {
                        'card': state['card'], 'retry_at': now + RETRY_SECONDS,
                    }))
            result['sent' if ok else 'failed'].append(scope)
        return result
