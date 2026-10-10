"""Building-account learning: local durable work, immutable papers, explicit cloud sync."""
from __future__ import annotations

import copy
import csv
import datetime as dt
import hashlib
import io
import ipaddress
import json
import logging
import mimetypes
import re
import sqlite3
import threading
import time
import unicodedata
import uuid
from contextlib import closing, contextmanager
from pathlib import Path
from urllib.parse import urlsplit

SCOPES = tuple("ABCDEH")
BANKS = {"written": "阿里线上考试", "duty": "值班工程师面试", "professional": "专业工程师面试", "supplemental": "专项题库"}
QUOTAS = {"written": 8, "duty": 1, "professional": 1, "supplemental": 5}
TZ = dt.timezone(dt.timedelta(hours=8))
MAX_FILE = 20 * 1024 * 1024
MAX_TOTAL = 100 * 1024 * 1024
DEFAULT_SETTINGS = {"enabled": False, "publish_time": "08:00", "reminder_enabled": False, "reminder_time": "17:00", "portal_url": ""}
ISSUE_STATES = {"pending", "processing", "needs_info", "resolved", "no_change", "withdrawn"}
RATINGS = {"部分掌握", "需复习"}


class LearningError(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def now():
    return dt.datetime.now(TZ)


def stamp():
    return now().isoformat(timespec="seconds")


def digest(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def text(value):
    if isinstance(value, list):
        return "".join(text(item) for item in value)
    if isinstance(value, dict):
        return str(value.get("text") or "")
    return str(value or "")


def canonical(value):
    # Preserve mathematical signs, decimal points and units when identifying repeated stems.
    return re.sub(r"[\s，,。；;？?！!]+", "", unicodedata.normalize("NFKC", text(value))).lower()


def clean(value, limit=12000):
    if not isinstance(value, str):
        raise LearningError("填写内容必须为文本")
    if len(value) > limit:
        raise LearningError(f"填写内容不能超过 {limit} 字")
    return value.strip()


def parse_options(value):
    result = []
    parts = re.split(r"(?:^|[\n\r])\s*([A-Z])[.．、)）:]\s*", unicodedata.normalize("NFKC", text(value)).strip())
    for index in range(1, len(parts), 2):
        label, content = parts[index], parts[index + 1].strip()
        if content:
            result.append({"id": "o_" + digest([label, content])[:16], "text": content, "label": label})
    return result


def resolve_answer(value, options):
    answer = unicodedata.normalize("NFKC", text(value)).strip()
    answer = re.sub(r"^(?:参考答案|正确答案|答案|答)\s*[:：]\s*", "", answer).strip()
    labels = {item.get("label", chr(65 + index)): item["id"] for index, item in enumerate(options)}
    if re.fullmatch(r"[A-Z\s,、;/，]+", answer) and all(c in labels for c in re.findall("[A-Z]", answer)):
        return list(dict.fromkeys(labels[c] for c in re.findall("[A-Z]", answer)))
    exact = [item["id"] for item in options if canonical(item["text"]) == canonical(answer)]
    if len(exact) == 1:
        return exact
    parts = [part.strip() for part in re.split(r"[\n;；、]+", answer) if part.strip()]
    matches = []
    for part in parts:
        found = [item["id"] for item in options if canonical(item["text"]) == canonical(part)]
        if len(found) != 1:
            return []
        matches.extend(found)
    return list(dict.fromkeys(matches))


def problems(question):
    errors = []
    if not question.get("stem"):
        errors.append("缺少题干")
    if question.get("type") == "interview":
        if not question.get("answer_text") and not any(a.get("kind") == "answer" for a in question.get("attachments", [])):
            errors.append("缺少参考答案")
    else:
        options = question.get("options") or []
        ids = [item.get("id") for item in options]
        if not 2 <= len(options) <= 26 or any(not item.get("text") for item in options):
            errors.append("选项不完整（需2至26项）")
        if len(set(ids)) != len(ids) or len({canonical(o["text"]) for o in options}) != len(options):
            errors.append("选项重复")
        answers = question.get("correct_option_ids") or []
        if not answers or not set(answers) <= set(ids):
            errors.append("答案无法唯一匹配选项")
        if question.get("type") == "single" and len(answers) != 1:
            errors.append("单选题须有一个正确选项")
        if question.get("type") not in {"single", "multiple"}:
            errors.append("题型无法识别")
    return errors


def normalize_question(raw):
    fields = raw.get("fields") or {}
    try:
        meta = json.loads(text(fields.get("学练配置")) or "{}")
    except (ValueError, TypeError):
        meta = {}
    if not isinstance(meta, dict):
        meta = {}
    bank, record_id = raw["bank"], raw["record_id"]
    options = parse_options(fields.get("选项"))
    previous = meta.get("options") or []
    previous_text = "\n".join(f"{item.get('label') or chr(65 + index)}. {item.get('text', '')}" for index, item in enumerate(previous))
    if previous and text(fields.get("选项")).replace("\r\n", "\n").strip() == previous_text.strip():
        options = copy.deepcopy(previous)
    # Keep stable identities when the source option text is unchanged or merely reordered.
    for item in options:
        matches = [old for old in previous if canonical(old.get("text")) == canonical(item["text"])]
        if len(matches) == 1:
            item["id"] = matches[0]["id"]
    type_label = text(fields.get("题型"))
    kind = "interview" if bank in {"duty", "professional"} else "single" if type_label in {"单选", "单选题"} else "multiple" if type_label in {"多选", "多选题", "不定项", "不定项选择题"} else "unknown"
    if bank == 'supplemental':
        answers = resolve_answer(fields.get('答案'), options)
        kind = 'interview' if not text(fields.get('选项')).strip() else ('single' if len(answers) == 1 else 'multiple' if len(answers) > 1 else 'unknown')
        type_label = {'interview': '问答题', 'single': '单选题', 'multiple': '多选题'}.get(kind, '待核对')
    attachments = copy.deepcopy(meta.get("attachments") or [])
    source_attachments = [(item, "answer") for item in fields.get("答案附件" if bank == 'supplemental' else "答案图片") or []] + [(item, "material") for item in fields.get("学练资料") or []]
    for item, attachment_kind in source_attachments:
        token = item.get("file_token")
        if token and token not in meta.get("removed_attachment_tokens", []) and not any(a.get("file_token") == token for a in attachments):
            attachments.append({"id": "f_" + digest(token)[:32], "name": item.get("name", "题目资料"), "file_token": token, "kind": attachment_kind, "size": item.get("size", 0)})
    q = {**meta, "id": meta.get("id") or f"{bank}:{record_id}", "record_id": record_id, "bank": bank,
         "stem": text(fields.get("题目")).strip(), "type": kind, "type_label": type_label or "面试",
         "year": text(fields.get("年份") or fields.get("年度")), "options": options,
         "answer_text": text(fields.get("答案")).strip(), "attachments": attachments,
         "analysis": meta.get("analysis", ""), "hint": meta.get("hint", ""), "topic": meta.get("topic", ""),
         "specialty": text(fields.get('专业')) if bank == 'supplemental' else meta.get("specialty", ""), "difficulty": meta.get("difficulty", "普通")}
    q["correct_option_ids"] = resolve_answer(q["answer_text"], options) if kind != "interview" else []
    if meta.get("source_answer") == q["answer_text"] and meta.get("correct_option_ids") and set(meta["correct_option_ids"]) <= {o["id"] for o in options}:
        q["correct_option_ids"] = meta["correct_option_ids"]
        q["answer_text"] = str(meta.get("answer_text") or q["answer_text"])
    q["family_id"] = meta.get("family_id") or digest(canonical(q["stem"]))
    q["problems"] = problems(q)
    q["status"] = meta.get("status") or ("draft" if q["problems"] else "published")
    q["version"] = str(meta.get("version") or digest([q["stem"], kind, options, q["answer_text"], attachments])[:20])
    return q


class LearningService:
    def __init__(self, root=None, cloud=None, send_message=None, get_portal_url=None, get_people=None):
        if root is None:
            from upload_event_module.utils import get_data_file_path
            root = Path(get_data_file_path("learning"))
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = self.root / "learning.sqlite3"
        self._lock = threading.RLock()
        self._cloud = cloud
        self._sender = send_message
        self._portal_url = get_portal_url
        self._people_reader = get_people
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._thread = None
        self._refresh_requested = False
        self._last_error = ""
        self._refresh_state = "idle"
        self._schema_ready = False
        self._restored = False
        self._restore_requested = False
        self._people_retry_at = 0.0
        self._last_tick = 0.0
        if self.db.exists():
            with closing(sqlite3.connect(self.db)) as source:
                if source.execute('PRAGMA user_version').fetchone()[0] < 2:
                    backup = self.root / 'backups' / 'learning-before-personal.sqlite3'
                    backup.parent.mkdir(exist_ok=True)
                    if not backup.exists():
                        temporary = backup.with_suffix('.tmp')
                        with closing(sqlite3.connect(temporary)) as target:
                            source.backup(target)
                        temporary.replace(backup)
        with closing(self._connect()) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS documents(kind TEXT NOT NULL, key TEXT NOT NULL, payload TEXT NOT NULL, revision INTEGER NOT NULL DEFAULT 1, dirty INTEGER NOT NULL DEFAULT 0, updated TEXT NOT NULL, PRIMARY KEY(kind,key))")
            if "retry_at" not in {row[1] for row in conn.execute("PRAGMA table_info(documents)")}:
                conn.execute("ALTER TABLE documents ADD COLUMN retry_at REAL NOT NULL DEFAULT 0")
            columns = {row[1] for row in conn.execute('PRAGMA table_info(documents)')}
            for column in ('person_id', 'scope', 'day'):
                if column not in columns:
                    conn.execute(f"ALTER TABLE documents ADD COLUMN {column} TEXT NOT NULL DEFAULT ''")
            conn.execute('CREATE INDEX IF NOT EXISTS learning_person_date ON documents(kind,person_id,day)')
            conn.execute('CREATE INDEX IF NOT EXISTS learning_scope_date ON documents(kind,scope,day)')
            conn.execute('CREATE TABLE IF NOT EXISTS learning_usage(paper_id TEXT, family_id TEXT, person_id TEXT, scope TEXT, day TEXT, PRIMARY KEY(paper_id,family_id))')
            conn.execute('CREATE INDEX IF NOT EXISTS learning_usage_person ON learning_usage(person_id,day)')
            conn.execute('CREATE INDEX IF NOT EXISTS learning_usage_family ON learning_usage(family_id,day)')
            conn.execute('CREATE TABLE IF NOT EXISTS learning_results(paper_id TEXT, question_id TEXT, person_id TEXT, scope TEXT, day TEXT, bank TEXT, type TEXT, topic TEXT, stem TEXT, correct INTEGER, assisted INTEGER, needs_review INTEGER, invalid INTEGER, practice_count INTEGER, submitted_at TEXT, self_rating TEXT, PRIMARY KEY(paper_id,question_id))')
            conn.execute('CREATE INDEX IF NOT EXISTS learning_results_person ON learning_results(person_id,day)')
            conn.execute('CREATE INDEX IF NOT EXISTS learning_results_scope ON learning_results(scope,day)')
            conn.execute('CREATE TABLE IF NOT EXISTS learning_practice(paper_id TEXT, question_id TEXT, operation_id TEXT, person_id TEXT, scope TEXT, day TEXT, PRIMARY KEY(paper_id,question_id,operation_id))')
            conn.execute('CREATE INDEX IF NOT EXISTS learning_practice_person ON learning_practice(person_id,day)')
            conn.execute('CREATE INDEX IF NOT EXISTS learning_practice_scope ON learning_practice(scope,day)')
            if conn.execute('PRAGMA user_version').fetchone()[0] < 2:
                conn.execute("UPDATE documents SET person_id=COALESCE(json_extract(payload,'$.person_id'),''),scope=COALESCE(json_extract(payload,'$.scope'),''),day=COALESCE(json_extract(payload,'$.date'),'')")
                for paper in self._all('paper', conn):
                    self._index_paper(paper, conn)
                conn.execute('PRAGMA user_version=2')
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS learning_paper_identity ON documents(person_id,day) WHERE kind='paper' AND person_id<>''")
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS learning_reserve_claim ON documents(person_id,day) WHERE kind='reserve' AND person_id<>''")
            conn.commit()

    def _connect(self):
        conn = sqlite3.connect(self.db, timeout=5)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    @contextmanager
    def transaction(self):
        with self._lock:
            conn = self._connect()
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                conn.close()

    def _get(self, kind, key, conn=None):
        own = conn is None
        conn = conn or self._connect()
        try:
            row = conn.execute("SELECT payload,revision,dirty FROM documents WHERE kind=? AND key=?", (kind, key)).fetchone()
            if row is None:
                return None
            value = json.loads(row["payload"])
            value["_revision"] = row["revision"]
            value["_dirty"] = bool(row["dirty"])
            return value
        finally:
            if own:
                conn.close()

    def _all(self, kind, conn=None):
        own = conn is None
        conn = conn or self._connect()
        try:
            values = []
            for row in conn.execute("SELECT payload,revision,dirty FROM documents WHERE kind=? ORDER BY key", (kind,)).fetchall():
                value = json.loads(row["payload"])
                value.update(_revision=row["revision"], _dirty=bool(row["dirty"]))
                values.append(value)
            return values
        finally:
            if own:
                conn.close()

    def _put(self, kind, key, value, conn, dirty=True):
        value = {k: v for k, v in value.items() if not k.startswith("_")}
        conn.execute("INSERT INTO documents(kind,key,payload,dirty,updated,person_id,scope,day) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(kind,key) DO UPDATE SET payload=excluded.payload,revision=documents.revision+1,dirty=excluded.dirty,updated=excluded.updated,retry_at=0,person_id=excluded.person_id,scope=excluded.scope,day=excluded.day", (kind, key, json.dumps(value, ensure_ascii=False), int(dirty), stamp(), value.get('person_id',''), value.get('scope',''), value.get('date','')))
        if kind in {'paper', 'record'}:
            paper = value if kind == 'paper' else self._get('paper', key, conn)
            if paper:
                self._index_paper(paper, conn)
        if dirty:
            self._wake.set()

    def _documents(self, kind, *, scope='', person_id='', start='', end='', legacy=False, conn=None):
        clauses, args = ['kind=?'], [kind]
        for column, value, op in (('scope', scope, '='), ('person_id', person_id, '='), ('day', start, '>='), ('day', end, '<=')):
            if value:
                clauses.append(column + op + '?'); args.append(value)
        if kind in {'paper', 'record'}:
            clauses.append("person_id=''" if legacy else "person_id<>''")
        owned = conn is None
        conn = conn or self._connect()
        try:
            return [{**json.loads(row['payload']), '_revision': row['revision'], '_dirty': bool(row['dirty'])}
                    for row in conn.execute('SELECT payload,revision,dirty FROM documents WHERE ' + ' AND '.join(clauses) + ' ORDER BY day,key', args)]
        finally:
            if owned: conn.close()

    def _index_paper(self, paper, conn):
        person = paper.get('person_id', '')
        for q in paper.get('questions', []):
            conn.execute('INSERT OR IGNORE INTO learning_usage VALUES(?,?,?,?,?)', (paper['id'], q['family_id'], person, paper['scope'], paper['date']))
        conn.execute('DELETE FROM learning_results WHERE paper_id=?', (paper['id'],))
        conn.execute('DELETE FROM learning_practice WHERE paper_id=?', (paper['id'],))
        if paper.get('deleted_at') or not person:
            return
        record = self._get('record', paper['id'], conn) or {}
        for q in paper.get('questions', []):
            entry = record.get('entries', {}).get(q['id'], {})
            a = entry.get('attempt')
            if a:
                if not q.get('invalid'):
                    for n, attempt in enumerate(entry.get('practice', [])):
                        conn.execute('INSERT OR IGNORE INTO learning_practice VALUES(?,?,?,?,?,?)', (paper['id'], q['id'], attempt.get('operation_id') or str(n), person, paper['scope'], attempt['submitted_at'][:10]))
                conn.execute('INSERT INTO learning_results VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', (
                    paper['id'], q['id'], person, paper['scope'], a['submitted_at'][:10], q['bank'], q['type'],
                    q.get('topic') or q.get('specialty') or '未分类', q['stem'], a.get('correct'), bool(a.get('assisted')),
                    bool(entry.get('needs_review')), bool(q.get('invalid')), len(entry.get('practice', [])), a['submitted_at'],
                    (entry.get('practice') or [a])[-1].get('self_rating', '')))

    @staticmethod
    def _publication_key(date):
        return 'personal:' + date

    @property
    def cloud(self):
        if self._cloud is None:
            from .learning_cloud import LearningCloud
            self._cloud = LearningCloud()
        return self._cloud

    def settings(self):
        return {**DEFAULT_SETTINGS, **{k: v for k, v in (self._get("settings", "main") or {}).items() if not k.startswith("_")}}

    def portal_url(self):
        try:
            # Runtime configuration wins over addresses restored from another host.
            address = self._portal_url() if self._portal_url else self.settings().get("portal_url", "")
            parsed = urlsplit(str(address or "").strip())
            host = (parsed.hostname or "").lower().rstrip(".")
            if (parsed.scheme not in {"http", "https"} or not host or parsed.username or parsed.password
                    or parsed.query or parsed.fragment or parsed.path not in {"", "/"}
                    or host == "localhost" or host.endswith(".localhost")):
                return ""
            if parsed.port is not None and not 1 <= parsed.port <= 65535:
                return ""
            try:
                ip = ipaddress.ip_address(host)
            except ValueError:
                ip = None
            if ip and (ip.is_loopback or ip.is_unspecified or ip.is_multicast):
                return ""
            return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            logging.getLogger(__name__).warning("学练入口地址解析失败，将等待程序访问地址恢复")
            return ""

    def public_settings(self):
        return {**self.settings(), "portal_url": self.portal_url()}

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="LearningWorker", daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._wake.set()
        if self._thread:
            self._thread.join(timeout=2)

    def _run(self):
        from upload_event_module.services.process_lifetime import lower_current_thread_priority
        lower_current_thread_priority()
        while not self._stop.is_set():
            self._wake.wait(30)
            self._wake.clear()
            if self._stop.is_set():
                break
            try:
                self.tick()
            except Exception as exc:
                self._last_error = str(exc)[:600]
                logging.getLogger(__name__).warning("学练后台任务失败: %s", exc)

    def request_refresh(self):
        if self._refresh_state != "syncing":
            with self.transaction() as conn:
                conn.execute("UPDATE documents SET retry_at=0 WHERE dirty=1")
            self._refresh_requested = True
            self._refresh_state = "syncing"
            self._wake.set()
        return self.sync_status()

    def sync_status(self):
        with closing(self._connect()) as conn:
            pending = conn.execute("SELECT count(*) FROM documents WHERE dirty=1").fetchone()[0]
        return {"status": "publishing" if self._manual_publish_pending() else self._refresh_state, "error": self._last_error, "pending": pending,
                "updated_at": (self._get("local", "refresh") or {}).get("at", "")}

    def _manual_publish_pending(self, date=None):
        date = date or now().date().isoformat()
        return ((self._get("local", "manual_publish") or {}).get("date") == date
                and not self._get("publication", self._publication_key(date)))

    def tick(self, current=None):
        current = current or now()
        settings = self.settings()
        manual_mode = bool(self._get("local", "manual_publish"))
        refreshed = False
        if self._restore_requested and not self._restored:
            self.restore()
            self._restore_requested = False
        if self._refresh_requested:
            self._refresh_requested = False
            try:
                self.refresh()
                refreshed = True
            except Exception:
                self._refresh_state = "error"
                raise
        if not settings["enabled"] and not manual_mode:
            local = self._get("settings", "main")
            # Turning scheduling off is itself durable; do not strand the disable write.
            if local and local["_dirty"] and (self._schema_ready or (self._get("local", "disable_sync") or {}).get("required")):
                self.cloud.enabled = True
                self.cloud.ensure_schema()
                self._sync_document(("settings", "main", local["_revision"]))
                self.cloud.enabled = False
                with self.transaction() as conn:
                    self._put("local", "disable_sync", {"required": False}, conn, False)
            return
        self.cloud.enabled = True
        try:
            if not self._schema_ready:
                if not refreshed:
                    self.refresh()
                    refreshed = True
                self.cloud.ensure_schema()
                self._schema_ready = True
            if not self._restored:
                self.restore()
            if time.time() >= self._people_retry_at and time.time() - (self._get("local", "people_sync") or {}).get("checked_at", 0) > 3600:
                from .learning_personal import refresh_people
                self._people_retry_at = time.time() + 300
                try:
                    refresh_people(self)
                except Exception:
                    self._last_error = "人员目录暂未刷新，保留原名单；后台稍后重试。"
            self.sync_pending(limit=40)
            day = current.date().isoformat()
            if not self.settings()["enabled"] and not manual_mode:
                return
            due = (settings["enabled"] and current.strftime("%H:%M") >= settings["publish_time"]
                   or self._manual_publish_pending(day))
            if due and not self._get("publication", self._publication_key(day)):
                if not refreshed:
                    self.refresh()
                if not self.settings()["enabled"] and not manual_mode:
                    return
                self.publish(current.date())
                self.sync_pending(limit=40)
            if settings["enabled"] and self.settings()["enabled"]:
                self.send_notifications(current)
        finally:
            if not settings["enabled"]:
                self.cloud.enabled = False

    def restore(self):
        entities = self.cloud.load_entities()
        with self.transaction() as conn:
            for item in entities:
                kind, key, value = item.get("kind"), item.get("key"), item.get("payload")
                if kind not in {"person", "reserve", "paper", "record", "issue", "settings", "publication", "notification", "audit", "attachment"} or not isinstance(value, dict):
                    continue
                if self._get(kind, key, conn) is None:
                    self._put(kind, key, value, conn, False)
            for paper in self._all("paper", conn):
                if paper.get("person_id"):
                    reserve = self._get("reserve", paper.get("reserve_id", ""), conn)
                    if reserve and not reserve.get("person_id"):
                        self._put("reserve", reserve["id"], {**reserve, "person_id": paper["person_id"], "paper_id": paper["id"]}, conn)
                    self._index_paper(paper, conn)
                if paper.get("deleted_at"):
                    continue
                for q in paper["questions"]:
                    for attachment in q.get("attachments", []):
                        if attachment.get("file_token") and not self._get("attachment", attachment["id"], conn):
                            self._put("attachment", attachment["id"], {**attachment, "question_id": q["id"]}, conn, False)
                record = self._get("record", paper["id"], conn)
                if record and self._reconcile_grades(paper, record):
                    record["version"] += 1
                    self._put("record", paper["id"], record, conn)
            for reserve in self._all("reserve", conn):
                if reserve.get("person_id") and not self._get("paper", reserve.get("paper_id", ""), conn):
                    raise LearningError("个人题单云端恢复尚不完整，暂不分配新题，请稍后重试。", 503)
        self._restored = True

    @staticmethod
    def _reconcile_grades(paper, record):
        changed = False
        for q in paper["questions"]:
            entry = record.get("entries", {}).get(q["id"], {})
            if q.get("invalid") or not entry.get("attempt"):
                continue
            if q["type"] == "interview":
                attempts = [entry["attempt"], *entry.get("practice", [])]
                if q.get("correction") and not any(a.get("question_version") == q["version"] for a in attempts) and not entry.get("needs_review"):
                    entry["needs_review"] = True
                    changed = True
                continue
            for attempt in [entry["attempt"], *entry.get("practice", [])]:
                selected, expected = set(attempt["option_ids"]), set(q["correct_option_ids"])
                grade = {"correct": selected == expected, "missed": sorted(expected - selected), "wrong": sorted(selected - expected)}
                if any(attempt.get(k) != v for k, v in grade.items()):
                    attempt.setdefault("corrections", []).append({"at": stamp(), "reason": "恢复时按已确认题单修正评分", **{k: attempt.get(k) for k in grade}})
                    attempt.update(grade)
                    changed = True
        return changed

    def refresh(self):
        self._refresh_state = "syncing"
        questions = [normalize_question(raw) for raw in self.cloud.fetch_questions()]
        if not self._restored:
            self.restore()
        groups = {}
        for q in questions:
            groups.setdefault(canonical(q["stem"]), []).append(q)
        for group in groups.values():
            if len(group) > 1:
                valid = [q for q in group if not q["problems"] and q["status"] == "published"]
                signatures = {digest(sorted(canonical(o["text"]) for o in q["options"] if o["id"] in q["correct_option_ids"])) if q["type"] != "interview" else canonical(q["answer_text"]) for q in valid}
                if len(signatures) > 1:
                    for q in valid:
                        q["problems"].append("同题干答案不一致，需核实")
                for q in sorted(group, key=lambda q: (bool(q["problems"]), q["status"] != "published", q["id"]))[1:]:
                    q["problems"].append("重复题干候选，需核对合并身份")
        with self.transaction() as conn:
            found = set()
            for q in questions:
                found.add(q["id"])
                old = self._get("question", q["id"], conn)
                if old and old["_dirty"]:
                    continue
                if old and self._question_content(old) != self._question_content(q):
                    q["version"] = digest(self._question_content(q))[:20]
                self._put("question", q["id"], q, conn, False)
                for attachment in q["attachments"]:
                    if not self._get("attachment", attachment["id"], conn):
                        self._put("attachment", attachment["id"], {**attachment, "question_id": q["id"]}, conn, False)
            for old in self._all("question", conn):
                if old.get("record_id") and old["id"] not in found and not old["_dirty"]:
                    old["status"] = "deleted"
                    self._put("question", old["id"], old, conn, False)
            self._put("local", "refresh", {"at": stamp()}, conn, False)
        self._refresh_state = "ready"
        self._last_error = ""
        from .learning_personal import refresh_people
        refresh_people(self)

    @staticmethod
    def _question_content(q):
        return {k: q.get(k) for k in ("stem", "type", "options", "correct_option_ids", "answer_text", "analysis", "hint", "topic", "specialty", "difficulty", "attachments", "status")}

    def sync_pending(self, limit=40, *, force=False):
        with closing(self._connect()) as conn:
            rows = conn.execute("SELECT kind,key,revision FROM documents WHERE dirty=1 AND (? OR retry_at<=?) ORDER BY CASE kind WHEN 'attachment' THEN 0 WHEN 'question' THEN 1 WHEN 'paper' THEN 2 WHEN 'publication' THEN 9 ELSE 3 END,updated LIMIT ?", (int(force), time.time(), limit)).fetchall()
        errors = []
        for row in rows:
            if self._stop.is_set():
                break
            try:
                deferred = self._sync_document(row) is False
            except Exception as exc:
                deferred = True
                errors.append(f"{row[0]}: {str(exc)[:180]}")
            if deferred:
                with self.transaction() as conn:
                    conn.execute("UPDATE documents SET retry_at=? WHERE kind=? AND key=? AND revision=? AND dirty=1", (time.time() + 60, row[0], row[1], row[2]))
        if rows:
            self._last_error = "；".join(errors[:3])
        return {"pending_errors": len(errors)}

    def _sync_document(self, row):
        kind, key, revision = row
        value = self._get(kind, key)
        if not value or value["_revision"] != revision:
            return
        payload = {k: v for k, v in value.items() if not k.startswith("_")}
        operation_id = f"learning:{kind}:{key}:{revision}"
        if kind == "publication" and any((self._get("paper", pid) or {}).get("_dirty", True) for pid in payload.get("paper_ids", [])):
            return False
        if kind == "publication" and any((self._get("reserve", rid) or {}).get("_dirty", True) for rid in payload.get("reserve_ids", [])):
            return False
        if kind == "reserve" and payload.get("paper_id") and (self._get("paper", payload["paper_id"]) or {}).get("_dirty", True):
            return False
        if kind == "record" and (self._get("paper", key) or {}).get("_dirty"):
            return False
        if kind == "notification" and payload.get("kind") == "correction":
            paper = self._get("paper", payload.get("paper_id", ""))
            record = self._get("record", payload.get("paper_id", ""))
            if not paper or paper["_dirty"] or record and record["_dirty"]:
                return False
            if not self._correction_matches(payload, paper):
                payload["status"] = "superseded"
        if kind == "attachment":
            if not payload.get("file_token"):
                path = self.root / "files" / payload["local_file"]
                payload.update(self.cloud.upload_attachment(path, payload["name"]))
                # The upload token survives a later entity-write timeout or restart.
                with self.transaction() as conn:
                    self._put(kind, key, payload, conn)
                    revision = self._get(kind, key, conn)["_revision"]
            self.cloud.upsert_entity(kind, key, {k: v for k, v in payload.items() if k != "local_file"}, operation_id)
        elif kind == "question":
            for attachment in payload.get("attachments", []):
                stored = self._get("attachment", attachment["id"])
                if not stored or not stored.get("file_token"):
                    return False
                attachment.update({k: stored[k] for k in ("file_token", "size") if k in stored})
            result = self.cloud.save_question(payload, operation_id)
            payload["record_id"] = result.get("record_id") or payload.get("record_id")
        else:
            self.cloud.upsert_entity(kind, key, payload, operation_id)
        with self.transaction() as conn:
            current = self._get(kind, key, conn)
            if current and current["_revision"] == revision:
                self._put(kind, key, payload, conn, False)
            elif kind == "question" and current and not current.get("record_id") and payload.get("record_id"):
                current["record_id"] = payload["record_id"]
                self._put(kind, key, current, conn)

    def publish(self, day=None):
        from .learning_personal import publish
        day = day or now().date()
        return publish(self, dt.date.fromisoformat(day) if isinstance(day, str) else day)

    def _send(self, scope, message, identity):
        if self._sender:
            return self._sender(scope, message, identity)
        from .portal_service import BUILDING_OPEN_ID_MAP
        from upload_event_module.services.robot_webhook import send_text_to_open_ids
        return send_text_to_open_ids(message, [BUILDING_OPEN_ID_MAP[scope]], message_uuid=str(uuid.uuid5(uuid.NAMESPACE_URL, identity)))

    @staticmethod
    def _correction_matches(notification, paper):
        if not paper or paper.get("deleted_at"):
            return False
        q = next((q for q in (paper or {}).get("questions", []) if q["id"] == notification.get("question_id")), None)
        return q is not None and notification.get("target_fingerprint") == digest([LearningService._question_content(q), bool(q.get("invalid")), q.get("correction")])

    def send_notifications(self, current):
        settings = self.settings()
        date = current.date().isoformat()
        link = None
        with self.transaction() as conn:
            if settings["reminder_enabled"] and current.strftime("%H:%M") >= settings["reminder_time"]:
                for paper in self._documents("paper", start=date, end=date, conn=conn):
                    key = "reminder:personal:" + date + ":" + paper["scope"]
                    record = self._get("record", paper["id"], conn) or {}
                    if paper["date"] == date and not paper.get("deleted_at") and paper.get("notify", True) and paper["questions"] and not self._completed(paper, record) and not self._get("notification", key, conn):
                        self._put("notification", key, {"id": key, "kind": "reminder", "personal_mode": True, "scope": paper["scope"], "date": date, "status": "pending"}, conn)
        for notification in self._all("notification"):
            if notification.get("status") != "pending" or notification.get("retry_at", 0) > time.time():
                continue
            current_settings = self.settings()
            if not current_settings["enabled"]:
                break
            paper = self._get("paper", notification.get("paper_id", ""))
            if paper and paper.get("deleted_at"):
                notification["status"] = "cancelled"
            elif notification["kind"] == "reminder" and not current_settings["reminder_enabled"]:
                notification["status"] = "cancelled"
            elif notification.get("date") != date:
                notification["status"] = "expired"
            elif notification["kind"] == "correction" and not self._correction_matches(notification, paper):
                notification["status"] = "superseded"
            else:
                if (paper and paper["_dirty"]) or (self._get("publication", self._publication_key(date)) or {}).get("_dirty"):
                    continue
                if notification["kind"] == "correction" and (self._get("record", notification.get("paper_id", "")) or {}).get("_dirty"):
                    continue
                outstanding = [p for p in self._documents("paper", scope=notification["scope"], start=date, end=date)
                               if not p.get("deleted_at") and p["questions"] and not self._completed(p, self._get("record", p["id"]) or {})] if notification["kind"] == "reminder" else []
                if notification["kind"] == "reminder" and not outstanding:
                    notification["status"] = "skipped_completed"
                else:
                    if link is None:
                        link = self.portal_url()
                    if not link:
                        self._last_error = "程序暂未识别到可访问的门户地址，学习提醒保留待发送，请检查程序的网络与访问地址设置"
                        continue
                    message = notification.get("message") or f"【画像学练】{notification['scope']}楼 {date}\n" + ("今日学练已发布" if notification["kind"] == "publish" else "今日学练尚未完成")
                    if paper:
                        message += f"，共{len(paper['questions'])}题。"
                    elif notification["kind"] == "publish" and notification.get("personal_mode"):
                        message += "，请选择人员领取，每人15题。"
                    elif outstanding:
                        message += f"，已领取的{len(outstanding)}份个人题单待完成。"
                    message += f"\n{link}/learning?scope={notification['scope']}"
                    result = self._send(notification["scope"], message, notification["id"])
                    ok = result[0] if isinstance(result, tuple) else bool(result)
                    notification["status"] = "sent" if ok else "pending"
                    notification["error"] = "" if ok else str(result[1] if isinstance(result, tuple) else "消息发送失败")
                    notification["retry_at"] = time.time() + 300
            with self.transaction() as conn:
                self._put("notification", notification["id"], notification, conn)

    @staticmethod
    def _admin(actor):
        if not actor.get("is_admin"):
            raise LearningError("仅管理员可执行此操作", 403)

    @staticmethod
    def _self_person(actor):
        person_id = actor.get("person_id") or ""
        return person_id

    def _access(self, actor, *, scope=None, person_id=None, write=False, target_person=None):
        """Resolve the effective scope/person for a request under the new self-service model.

        Writes are only allowed for the actor's own person; building duty accounts are
        read-only. Reads limit ordinary personal accounts to themselves and duty
        accounts to their own building.
        """
        admin = bool(actor.get("is_admin"))
        # Duty is decided solely by the explicit shared_account flag set by the
        # routes layer; never infer it from scope + missing person_id.
        duty = bool(actor.get("shared_account"))
        self_pid = actor.get("person_id") or ""
        if write:
            if duty:
                raise LearningError("楼栋值班账号仅可查看本楼人员与画像，不能提交或代答。", 403)
            if not self_pid:
                raise LearningError("当前账号未关联本人人员，暂时不能提交答题或质疑。", 403)
            target = target_person if target_person is not None else (person_id or "")
            if target and target != self_pid:
                raise LearningError("当前账号只能操作自己的题单与质疑。", 403)
            return {"scope": "", "person_id": self_pid}
        if duty:
            own = actor.get("scope", "")
            if own not in SCOPES:
                raise LearningError("楼栋值班账号缺少楼栋范围。", 403)
            if scope is not None and scope and scope != own:
                raise LearningError("楼栋值班账号仅可查看本楼人员与画像。", 403)
            if person_id:
                prow = self._get("person", person_id)
                if not prow or own not in set(prow.get("scopes") or []):
                    raise LearningError("楼栋值班账号仅可查看本楼人员与画像。", 403)
            return {"scope": own, "person_id": person_id or ""}
        if admin:
            return {"scope": scope or "", "person_id": person_id or ""}
        # Ordinary personal account read: self-only and MUST be mapped. Without a
        # resolved person an empty scope/person would otherwise expose all records.
        if not self_pid:
            raise LearningError("登录账号与人员名单尚未关联，请管理员先同步人员目录。", 403)
        if person_id not in (None, "", self_pid):
            raise LearningError("普通账号只能查看自己的画像与题单。", 403)
        return {"scope": "", "person_id": self_pid}

    def _claim_context(self, actor, payload):
        """Resolve (scope, self_person) for paper.claim with self-service auto-self.

        Claim is always self-service: the paper is created for the actor's own
        resolved person. An explicit scalar that contradicts the mapped self
        building or person is rejected instead of silently ignored.
        """
        self_pid = actor.get("person_id") or ""
        self._access(actor, scope=payload.get('scope'), person_id=payload.get('person_id'), write=True)
        requested_pid = payload.get("person_id")
        if requested_pid and requested_pid != self_pid:
            raise LearningError("当前账号只能领取自己的题单。", 403)
        person_row = self._get("person", self_pid) if self_pid else None
        if not person_row or not person_row.get("active"):
            raise LearningError("登录账号与人员名单尚未关联，请管理员先同步人员目录。", 403)
        scopes = [s for s in person_row.get("scopes", [])]
        if len(set(scopes) & set(SCOPES)) != 1:
            raise LearningError("当前人员未关联唯一楼栋，请管理员核对人员名单。", 409)
        scope = next(s for s in scopes if s in SCOPES)
        explicit = payload.get("scope")
        if explicit and explicit != scope:
            raise LearningError("楼栋与当前人员所属楼栋不一致，请核对后重试。", 403)
        return scope, person_row

    def _scope(self, actor, scope=None, write=False):
        return self._access(actor, scope=scope, write=write)["scope"]

    def bootstrap(self, scope, actor):
        admin = bool(actor.get("is_admin"))
        duty = bool(actor.get("shared_account"))
        self_pid = actor.get("person_id") or ""
        if admin:
            access = self._access(actor, scope=scope)
        elif duty:
            access = self._access(actor, scope=(scope or actor.get("scope")))
        elif self_pid:
            access = self._access(actor, scope="", person_id=self_pid)
        else:
            # Unmatched ordinary accounts may still load bootstrap so the frontend
            # can surface the identity issue; all content endpoints stay closed.
            access = {"scope": "", "person_id": ""}
        scope = access["scope"]
        if not self._restored:
            self._restore_requested = True
            self._wake.set()
        if not self._get("local", "refresh") and self._refresh_state == "idle":
            self.request_refresh()
        self_pid = self._self_person(actor)
        self_person = self._get("person", self_pid) if self_pid else None
        self_public = None
        if self_person:
            self_public = {k: self_person.get(k) for k in ("id", "name", "employee_no", "scopes")}
        self_scopes = [s for s in (self_person or {}).get("scopes", []) if s in SCOPES]
        self_scope = self_scopes[0] if len(self_scopes) == 1 else (self_scopes[0] if self_scopes else "")
        admin = bool(actor.get("is_admin"))
        duty = bool(actor.get("shared_account"))
        can_answer = bool(actor.get("can_answer"))
        can_view_buildings = bool(admin or duty)
        if duty:
            scopes = [{"value": scope, "label": f"{scope}楼"} for scope in [actor.get("scope", "")] if scope in SCOPES]
        elif admin:
            scopes = [{"value": s, "label": f"{s}楼"} for s in SCOPES]
        else:
            scopes = []
        if admin:
            # Unmatched admin can still view, just not answer.
            can_view_buildings = True
        if not self_pid and not admin and not duty:
            summary = {}  # Unmatched ordinary account: surface identity_issue, no stats yet.
        else:
            summary = self.profile(actor, {"scope": scope})["summary"]
        return {"is_admin": admin, "can_answer": can_answer, "can_view_buildings": can_view_buildings,
                "self_person": self_public, "self_scope": self_scope, "identity_issue": actor.get("identity_issue", ""),
                "scopes": scopes, "scope": scope, "settings": self.public_settings(), "sync": self.sync_status(),
                "today": now().date().isoformat(), "silent_manual_publish": True,
                "question_problem_count": sum(bool(q.get("problems")) and q.get("status") != "deleted" for q in self._all("question")) if admin else 0,
                "summary": summary}

    def _paper(self, paper_id, actor, conn=None, write=False):
        paper = self._get("paper", paper_id, conn)
        if not paper:
            raise LearningError("今日题单尚未发布或题单不存在", 404)
        admin = bool(actor.get("is_admin"))
        duty = bool(actor.get("shared_account"))
        self_pid = actor.get("person_id") or ""
        if write:
            self._access(actor, scope=paper.get("scope"), person_id=paper.get("person_id"),
                         write=True, target_person=paper.get("person_id"))
        elif admin:
            pass
        elif duty:
            # Duty legacy papers only belong to the duty building historically, and
            # person access is still enforced by _access on read.
            if paper.get("scope") not in SCOPES or paper.get("scope") != actor.get("scope"):
                raise LearningError("楼栋值班账号仅可查看本楼人员与画像。", 403)
            self._access(actor, scope=paper.get("scope"), person_id=paper.get("person_id"))
        else:
            # Ordinary personal accounts may only open their own paper. Legacy
            # building-history papers (empty person_id) are excluded entirely.
            if not paper.get("person_id") or paper.get("person_id") != self_pid:
                raise LearningError("普通账号只能查看自己的画像与题单。", 403)
            self._access(actor, scope=paper.get("scope"), person_id=paper.get("person_id"))
        if paper.get("deleted_at"):
            raise LearningError("题单已删除", 404)
        if write and not paper.get("person_id"):
            raise LearningError("旧楼栋题单仅保留只读历史，请选择人员领取今日题单。", 409)
        return paper

    def delete_paper(self, paper_id, actor):
        self._admin(actor)
        with self.transaction() as conn:
            paper = self._get("paper", paper_id, conn)
            if not paper:
                raise LearningError("题单不存在", 404)
            if paper.get("deleted_at"):
                return {"id": paper_id, "deleted": True}
            paper["deleted_at"] = stamp()
            paper["deleted_by"] = actor["id"]
            self._put("paper", paper_id, paper, conn)
            for notification in self._all("notification", conn):
                if notification.get("paper_id") == paper_id and notification.get("status") == "pending":
                    notification["status"] = "cancelled"
                    self._put("notification", notification["id"], notification, conn)
        return {"id": paper_id, "deleted": True}

    @staticmethod
    def _completed(paper, record):
        questions = paper.get("questions", [])
        return bool(questions) and all(q.get("invalid") or (record.get("entries", {}).get(q["id"], {}).get("attempt") and not record.get("entries", {}).get(q["id"], {}).get("needs_review")) for q in questions)

    def _public_attachment(self, attachment):
        return {k: attachment.get(k) for k in ("id", "name", "kind", "size")} | {"url": f"/api/learning/attachments/{attachment['id']}"}

    def public_paper(self, paper, actor, record=None):
        record = record if record is not None else self._get("record", paper["id"]) or {}
        result = {k: paper[k] for k in ("id", "date", "scope", "shortage", "created_at")}
        result.update(person_id=paper.get("person_id", ""), person=paper.get("person"), legacy=not bool(paper.get("person_id")))
        result.update({"version": record.get("version", 0), "status": "completed" if self._completed(paper, record) else "pending", "completed_at": record.get("completed_at", ""),
                       "sync_pending": bool(paper.get("_dirty") or record.get("_dirty")), "questions": []})
        for q in paper["questions"]:
            entry = record.get("entries", {}).get(q["id"], {})
            visible = {k: copy.deepcopy(q.get(k)) for k in ("id", "version", "bank", "stem", "type", "type_label", "options", "topic", "specialty", "difficulty", "invalid", "correction")}
            visible["question_id"] = q["id"]
            visible["has_hint"] = bool(q.get("hint"))
            visible.update({k: copy.deepcopy(entry.get(k)) for k in ("attempt", "practice", "note", "favorite", "needs_review")})
            visible["hinted"] = bool(entry.get("assisted"))
            visible["attachments"] = [self._public_attachment(a) for a in q.get("attachments", []) if a.get("kind") != "answer"]
            if entry.get("attempt") or entry.get("revealed"):
                visible["answer"] = {k: copy.deepcopy(q.get(k)) for k in ("correct_option_ids", "answer_text", "analysis", "hint")}
                visible["answer"]["attachments"] = [self._public_attachment(a) for a in q.get("attachments", []) if a.get("kind") == "answer"]
            elif entry.get("hint_seen"):
                visible["answer"] = {"hint": q.get("hint", "")}
            result["questions"].append(visible)
        valid = [q for q in result["questions"] if not q.get("invalid")]
        result["stats"] = {"total": len(valid), "answered": sum(bool(q.get("attempt")) and not q.get("needs_review") for q in valid), "shortage": sum(paper["shortage"].values())}
        return result

    def paper_action(self, action, payload, actor):
        with self.transaction() as conn:
            paper = self._paper(payload.get("id"), actor, conn, write=True)
            if payload.get("person_id") != paper.get("person_id"):
                raise LearningError("答题人员已变化，请重新打开当前人员题单。", 409)
            qid = payload.get("question_id")
            q = next((q for q in paper["questions"] if q["id"] == qid), None)
            if not q:
                raise LearningError("题目不属于当前题单", 404)
            record = self._get("record", paper["id"], conn) or {"id": paper["id"], "person_id": paper["person_id"], "scope": paper["scope"], "date": paper["date"], "entries": {}, "version": 0}
            entry = record["entries"].setdefault(qid, {})
            if action == "answer":
                if q.get("invalid"):
                    raise LearningError("该题已作废，不再参与评价")
                op = clean(payload.get("operation_id", ""), 150)
                if not op:
                    raise LearningError("缺少提交标识，请重新点击提交")
                values = payload.get("option_ids", [])
                if q["type"] != "interview" and (not isinstance(values, list) or any(not isinstance(v, str) for v in values)):
                    raise LearningError("请选择有效选项")
                fingerprint = digest({**{k: payload.get(k) for k in ("question_id", "answer_text", "self_rating", "practice")}, "option_ids": sorted(set(values)) if q["type"] != "interview" else []})
                prior = [entry.get("attempt") or {}, *entry.get("practice", [])]
                match = next((a for a in prior if a.get("operation_id") == op), None)
                if match:
                    if match.get("fingerprint") != fingerprint:
                        raise LearningError("本次提交标识已用于不同内容，请读取最新作答", 409)
                    return self.public_paper(paper, actor, record)
                if payload.get("version") != record["version"]:
                    raise LearningError("该账号的学练记录已更新，请读取最新记录；当前填写请保留", 409)
                if q["type"] == "interview" and problems(q):
                    raise LearningError("参考答案正在调整，请等待管理员补充后再练习自评。")
                practice = bool(payload.get("practice"))
                if entry.get("attempt") and not practice and not entry.get("needs_review"):
                    raise LearningError("本题已提交，可在复习中重做，首次记录不会覆盖", 409)
                answer = {"operation_id": op, "fingerprint": fingerprint, "submitted_at": stamp(), "assisted": bool(entry.get("assisted")), "question_version": q["version"]}
                answer.update(operator_id=actor["id"], operator_name=actor.get("name", ""))
                if q["type"] == "interview":
                    answer["answer_text"] = clean(payload.get("answer_text", ""))
                    answer["self_rating"] = payload.get("self_rating")
                    if not answer["answer_text"] or answer["self_rating"] not in RATINGS:
                        raise LearningError("请填写面试回答并选择掌握程度")
                else:
                    values = payload.get("option_ids")
                    if not isinstance(values, list) or any(not isinstance(v, str) for v in values):
                        raise LearningError("请选择有效选项")
                    selected = set(values)
                    if not selected or not selected <= {o["id"] for o in q["options"]} or q["type"] == "single" and len(selected) != 1:
                        raise LearningError("请选择有效选项")
                    expected = set(q["correct_option_ids"])
                    answer.update(option_ids=sorted(selected), correct=selected == expected, missed=sorted(expected - selected), wrong=sorted(selected - expected))
                if entry.get("attempt"):
                    history = entry.setdefault("practice", [])
                    if len(history) >= 100:
                        raise LearningError("本题已保留100次复习记录，请在其他题目中继续学习")
                    history.append(answer)
                else:
                    entry["attempt"] = answer
                entry["needs_review"] = False
            elif action == "reveal":
                kind = payload.get("kind", "answer")
                if kind not in {"answer", "analysis", "hint"}:
                    raise LearningError("无效提示类型")
                if not entry.get("attempt"):
                    entry["assisted"] = True
                entry["hint_seen" if kind == "hint" else "revealed"] = stamp()
            elif action == "notes":
                if payload.get("version") != record["version"]:
                    raise LearningError("学习记录已变化，请读取最新内容后保存笔记。", 409)
                if "mastered" in payload:
                    raise LearningError("已掌握标记已停用")
                if "note" in payload:
                    entry["note"] = clean(payload["note"], 5000)
                if "favorite" in payload:
                    if not isinstance(payload["favorite"], bool):
                        raise LearningError("收藏标记必须为布尔值")
                    entry["favorite"] = payload["favorite"]
            entry.update(updated_by=actor["id"], updated_by_name=actor.get("name", ""), updated_at=stamp())
            record["version"] += 1
            if self._completed(paper, record) and not record.get("completed_at"):
                record["completed_at"] = stamp()
                record["late"] = now().date().isoformat() > paper["date"]
            self._put("record", paper["id"], record, conn)
        record["_dirty"] = True
        return self.public_paper(paper, actor, record)

    def list_papers(self, actor, query):
        access = self._access(actor, scope=query.get("scope"), person_id=str(query.get("person_id") or ""))
        scope = access["scope"]
        person_id = access["person_id"]
        legacy = query.get("legacy") == "1"
        if person_id:
            # Admin/duty may query any person and must be validated; an ordinary
            # self-only read already constrains to the actor's own person, so a
            # temporarily absent person row cannot be used to widen the filter.
            if actor.get("is_admin") or actor.get("shared_account"):
                from .learning_personal import person
                person(self, person_id)
            if not actor.get('shared_account'):
                scope = ""
        if query.get("today") == "1":
            query = {**query, "date": now().date().isoformat()}
        query = self._date_filters(query)
        papers = [p for p in self._documents("paper", scope=scope, person_id=person_id,
            start=query.get("date") or query.get("from", ""), end=query.get("date") or query.get("to", ""), legacy=legacy) if not p.get("deleted_at")]
        papers.sort(key=lambda p: (p["date"], p["scope"]), reverse=True)
        result = self._page(papers, query)
        result["items"] = [self.public_paper(p, actor) for p in result["items"]]
        if query.get("today") == "1":
            result["today"] = query["date"]
            result["published"] = bool(self._get("publication", self._publication_key(query["date"])))
            result["personal_mode"] = True
        return result

    @staticmethod
    def _page(items, query):
        try:
            size = max(1, min(100, int(query.get("page_size", 20))))
            page = max(1, min(max(1, (len(items) + size - 1) // size), int(query.get("page", 1))))
        except (TypeError, ValueError):
            raise LearningError("无效分页参数")
        return {"items": items[(page - 1) * size:page * size], "total": len(items), "page": page, "page_size": size}

    def review(self, actor, query):
        access = self._access(actor, scope=query.get("scope"), person_id=str(query.get("person_id") or ""))
        from .learning_personal import person
        learner_pid = access["person_id"]
        if not learner_pid:
            raise LearningError("请选择人员。", 400)
        learner = person(self, learner_pid)
        result = []
        for paper in reversed(self._documents("paper", scope=access['scope'], person_id=learner["id"])):
            if paper.get("deleted_at"):
                continue
            public = self.public_paper(paper, actor)
            for q in public["questions"]:
                a = q.get("attempt") or {}
                if query.get("kind") == "favorites":
                    include = q.get("favorite")
                elif query.get("kind") == "notes":
                    include = bool(q.get("note"))
                elif query.get("kind") == "all":
                    include = bool(a)
                else:
                    include = (a.get("correct") is False or a.get("self_rating") in {"部分掌握", "需复习"} or q.get("needs_review")) and not q.get("invalid")
                if query.get("search") and query["search"].lower() not in (q["stem"] + (q.get("topic") or "") + (q.get("note") or "")).lower():
                    continue
                if query.get("bank") and q["bank"] != query["bank"]:
                    continue
                if include:
                    result.append({"paper_id": paper["id"], "date": paper["date"], "scope": paper["scope"], "question": q})
        return self._page(result, query)

    def attempt_history(self, actor, query):
        """Flat, paginated actual submission history for the authorized person.

        Includes both the retained first attempt and every practice submission,
        deduplicated by operation id so retried/lost responses never repeat rows.
        """
        access = self._access(actor, scope=query.get("scope"), person_id=str(query.get("person_id") or ""))
        person_id = access["person_id"]
        if not person_id:
            raise LearningError("请选择人员。", 400)
        rows = []
        seen = set()
        dates = self._date_filters(query)
        # A later practice belongs to its submission day, not the paper's issue day.
        for paper in self._documents("paper", scope=access['scope'], person_id=person_id):
            if paper.get("deleted_at"):
                continue
            record = self._get("record", paper["id"]) or {}
            for q in paper.get("questions", []):
                entry = record.get("entries", {}).get(q["id"], {})
                for index, attempt in enumerate([entry.get('attempt'), *entry.get('practice', [])]):
                    if not attempt:
                        continue
                    day = attempt.get('submitted_at', '')[:10]
                    if dates.get('from') and day < dates['from'] or dates.get('to') and day > dates['to']:
                        continue
                    key = (paper['id'], q['id'], attempt.get('operation_id') or f'legacy:{index}')
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(self._attempt_row(paper, q, attempt, kind='first' if index == 0 else 'practice'))
        rows.sort(key=lambda row: (row["submitted_at"], row["operation_id"]), reverse=True)
        return self._page(rows, query)

    @staticmethod
    def _attempt_row(paper, q, attempt, *, kind):
        return {
            "operation_id": attempt.get("operation_id", ""),
            "submitted_at": attempt.get("submitted_at", ""),
            "date": paper.get("date", ""),
            "scope": paper.get("scope", ""),
            "paper_id": paper["id"],
            "person_id": paper.get("person_id", ""),
            "question_id": q["id"],
            "question": q.get("stem", ""),
            "kind": kind,
            "option_ids": sorted(attempt.get("option_ids") or []),
            "answer_text": attempt.get("answer_text", ""),
            "correct": attempt.get("correct"),
            "self_rating": attempt.get("self_rating", ""),
        }

    @staticmethod
    def _date_filters(query):
        query = dict(query)
        if query.get("period") in {"day", "week", "month", "7", "30"} and not (query.get("from") or query.get("to")):
            today = now().date()
            start = today if query["period"] == "day" else today - dt.timedelta(days=today.weekday()) if query["period"] == "week" else today.replace(day=1)
            if query["period"] in {"7", "30"}:
                start = today - dt.timedelta(days=int(query["period"]) - 1)
            query.update({"from": start.isoformat(), "to": today.isoformat()})
        try:
            for key in ("from", "to"):
                if query.get(key):
                    query[key] = dt.date.fromisoformat(query[key]).isoformat()
            if query.get("from") and query.get("to") and query["from"] > query["to"]:
                raise ValueError()
        except (TypeError, ValueError):
            raise LearningError("请填写有效日期，开始日期不能晚于结束日期")
        return query

    def profile(self, actor, query):
        from .learning_personal import profile
        return profile(self, actor, query)

    def resolve_self(self, open_id):
        """Resolve a session open id to a stable local person via persised login_ids."""
        from .learning_personal import resolve_self
        return resolve_self(self, open_id)

    def _question(self, qid, conn=None):
        q = self._get("question", qid, conn)
        if not q:
            raise LearningError("题目不存在", 404)
        return q

    def admin_question(self, question):
        return {k: v for k, v in question.items() if not k.startswith("_")} | {"sync_pending": bool(question.get("_dirty")), "attachments": [self._public_attachment(a) for a in question.get("attachments", [])]}

    def questions(self, actor, query):
        self._admin(actor)
        result = self._page(self._filtered_questions(query), query)
        result["items"] = [self.admin_question(q) for q in result["items"]]
        return result

    def _filtered_questions(self, query):
        items = self._all("question")
        for field in ("bank", "status", "specialty", "topic"):
            if query.get(field):
                items = [q for q in items if q.get(field) == query[field]]
        if not query.get("status"):
            items = [q for q in items if q.get("status") != "deleted"]
        if query.get("search"):
            term = query["search"].lower()
            items = [q for q in items if term in (q["stem"] + q.get("topic", "") + q.get("answer_text", "")).lower()]
        if str(query.get("problems", "")).lower() in {"1", "true"}:
            items = [q for q in items if q.get("problems")]
        items.sort(key=lambda q: (q.get("updated_at", ""), q["id"]), reverse=True)
        return items

    def _validated_question(self, payload, old=None):
        old = old or {}
        q = {k: copy.deepcopy(v) for k, v in old.items() if not k.startswith("_")}
        for key, limit in (("stem", 12000), ("answer_text", 12000), ("analysis", 12000), ("hint", 5000), ("topic", 100), ("specialty", 50), ("difficulty", 30), ("year", 30)):
            if key in payload:
                q[key] = clean(payload[key], limit)
            else:
                q.setdefault(key, "")
        q["bank"] = payload.get("bank", old.get("bank", "written"))
        if q["year"] and not re.fullmatch(r"(?:19\d{2}|20\d{2}|2100)年?", q["year"]):
            raise LearningError("年度请填写1900至2100的四位年份，可带“年”后缀")
        if q["bank"] not in BANKS or old.get("bank") and q["bank"] != old["bank"]:
            raise LearningError("题库无效，已存在的题目不能移动到另一来源表；可复制后调整")
        q["type"] = payload.get("type", old.get("type", "single" if q["bank"] == "written" else "interview"))
        if q["type"] not in {"single", "multiple", "interview"} or q["bank"] != "supplemental" and (q["bank"] == "written") == (q["type"] == "interview"):
            raise LearningError("题型与来源题库不一致")
        q["type_label"] = "单选" if q["type"] == "single" else "面试" if q["type"] == "interview" else "不定项" if payload.get("type_label", old.get("type_label")) == "不定项" else "多选"
        q["status"] = payload.get("status", old.get("status", "draft"))
        if q["status"] not in {"draft", "published", "disabled", "deleted"}:
            raise LearningError("题目状态无效")
        raw_options = payload.get("options", old.get("options", []))
        if not isinstance(raw_options, list) or len(raw_options) > 26:
            raise LearningError("选项最多26项")
        q["options"] = []
        for index, item in enumerate(raw_options):
            if not isinstance(item, dict):
                raise LearningError("选项格式无效")
            identity = clean(str(item.get("id") or "o_" + uuid.uuid4().hex), 100)
            q["options"].append({"id": identity, "text": clean(item.get("text", ""), 5000), "label": chr(65 + index)})
        answers = payload.get("correct_option_ids", old.get("correct_option_ids", []))
        if not isinstance(answers, list) or any(not isinstance(i, str) for i in answers):
            raise LearningError("正确选项格式无效")
        q["correct_option_ids"] = list(dict.fromkeys(answers))
        if q["type"] != "interview" and answers:
            q["answer_text"] = "\n".join(o["text"] for o in q["options"] if o["id"] in answers)
        q["source_answer"] = q["answer_text"]
        q["attachments"] = old.get("attachments", [])
        q["id"] = old.get("id") or payload.get("new_id") or "q_" + uuid.uuid4().hex
        q["family_id"] = old.get("family_id") or digest(canonical(q["stem"]))
        q["problems"] = problems(q)
        if q["status"] == "published" and q["problems"]:
            raise LearningError("不能发布：" + "；".join(q["problems"]))
        q["version"] = uuid.uuid4().hex
        q["updated_at"] = stamp()
        return q

    def save_question(self, payload, actor):
        self._admin(actor)
        if not self.settings()["enabled"]:
            raise LearningError("请先在学练设置中启用题库同步")
        with self.transaction() as conn:
            new_id = payload.get("new_id")
            if not payload.get("id") and new_id:
                if not isinstance(new_id, str) or not re.fullmatch(r"q_[A-Za-z0-9_-]{1,100}", new_id):
                    raise LearningError("新增题目标识无效")
                previous = self._get("question", new_id, conn)
                if previous:
                    if previous.get("create_fingerprint") != digest(payload):
                        raise LearningError("该题已创建，当前内容与原提交不同，请读取最新题目后编辑", 409)
                    return self.admin_question(previous)
            old = self._question(payload["id"], conn) if payload.get("id") else None
            if old and payload.get("version") != old["version"]:
                raise LearningError("题目已被修改，请读取最新版本后保存", 409)
            q = self._validated_question(payload, old)
            if not old and new_id:
                q["create_fingerprint"] = digest(payload)
            equivalent = [other for other in self._all("question", conn) if other["id"] != q["id"] and canonical(other["stem"]) == canonical(q["stem"]) and other.get("status") != "deleted"]
            if equivalent:
                q["family_id"] = sorted(equivalent, key=lambda other: other["id"])[0]["family_id"]
                if q["status"] == "published" and any(other["status"] == "published" for other in equivalent):
                    raise LearningError("同题干已有发布题目，请先核对并停用重复记录")
            reason = clean(payload.get("reason", "管理员修改题目"), 2000)
            audit_id = uuid.uuid4().hex
            self._put("audit", audit_id, {"id": audit_id, "question_id": q["id"], "actor": actor["id"], "at": stamp(), "reason": reason, "before": {k: v for k, v in (old or {}).items() if not k.startswith("_")}, "after": copy.deepcopy(q)}, conn)
            if old:
                self._correct_papers(old, q, reason, conn)
            self._put("question", q["id"], q, conn)
        q["_dirty"] = True
        return self.admin_question(q)

    def _correct_papers(self, old, updated, reason, conn):
        before = self._question_content(old)
        after = self._question_content(updated)
        if before == after:
            return
        original_options = {o["id"]: o["text"] for o in old["options"]}
        new_options = {o["id"]: o["text"] for o in updated["options"]}
        structure_changed = old["stem"] != updated["stem"] or old["type"] != updated["type"] or original_options != new_options
        answer_changed = old.get("answer_text") != updated.get("answer_text") if old["type"] == "interview" else set(old["correct_option_ids"]) != set(updated["correct_option_ids"])
        reference_changed = {a["id"] for a in old.get("attachments", []) if a.get("kind") == "answer"} != {a["id"] for a in updated.get("attachments", []) if a.get("kind") == "answer"}
        answer_changed = answer_changed or reference_changed
        if not structure_changed and not answer_changed:
            return
        updated_problems = problems(updated)
        missing_reference = updated_problems == ["缺少参考答案"]
        def grading_basis(question):
            return (question["stem"], question["type"], {o["id"]: o["text"] for o in question["options"]},
                    set(question["correct_option_ids"]), question.get("answer_text") if question["type"] == "interview" else None)
        for paper in self._all("paper", conn):
            if paper.get("deleted_at"):
                continue
            q = next((q for q in paper["questions"] if q["id"] == old["id"] and not q.get("invalid") and grading_basis(q) == grading_basis(old)), None)
            if not q:
                continue
            record = self._get("record", paper["id"], conn)
            entry = (record or {}).get("entries", {}).get(q["id"])
            q.setdefault("correction_history", []).append({"at": stamp(), "reason": reason, "original": copy.deepcopy({k: q.get(k) for k in ("stem", "options", "correct_option_ids", "answer_text", "attachments", "version")})})
            if structure_changed or (updated_problems and not missing_reference):
                q["invalid"] = True
                q["correction"] = "题干或选项已更正，原题不再参与评价：" + reason
            else:
                q["correct_option_ids"] = updated["correct_option_ids"]
                q["answer_text"] = updated["answer_text"]
                if reference_changed:
                    q["attachments"] = [a for a in q.get("attachments", []) if a.get("kind") != "answer"] + copy.deepcopy([a for a in updated.get("attachments", []) if a.get("kind") == "answer"])
                q["version"] = updated["version"]
                q["correction"] = ("参考答案暂不可用，等待补充：" if missing_reference else "参考答案已更正：") + reason
                if entry:
                    if q["type"] == "interview":
                        entry["needs_review"] = True
                    else:
                        for attempt in [entry.get("attempt") or {}, *entry.get("practice", [])]:
                            if not attempt:
                                continue
                            attempt.setdefault("corrections", []).append({"at": stamp(), "reason": reason, "correct": attempt.get("correct"), "missed": attempt.get("missed"), "wrong": attempt.get("wrong")})
                            selected, expected = set(attempt["option_ids"]), set(q["correct_option_ids"])
                            attempt.update(correct=selected == expected, missed=sorted(expected - selected), wrong=sorted(selected - expected))
            if record:
                record["version"] += 1
                self._put("record", paper["id"], record, conn)
            self._put("paper", paper["id"], paper, conn)
            nid = f"correction:{paper['id']}:{updated['version']}"
            self._put("notification", nid, {"id": nid, "kind": "correction", "date": now().date().isoformat(), "scope": paper["scope"], "paper_id": paper["id"], "question_id": q["id"],
                                          "target_fingerprint": digest([self._question_content(q), bool(q.get("invalid")), q.get("correction")]),
                                          "status": "pending", "message": f"【画像学练·答案更正】{paper['scope']}楼\n题目：{q['stem'][:150]}\n{q['correction']}"}, conn)

    def issue(self, issue_id, actor, conn=None, write=False):
        item = self._get("issue", issue_id, conn)
        if not item:
            raise LearningError("质疑记录不存在", 404)
        if actor.get('is_admin'):
            return item
        admin = bool(actor.get("is_admin"))
        duty = bool(actor.get("shared_account"))
        self_pid = actor.get("person_id") or ""
        if not admin and not duty:
            # Ordinary personal accounts: own issues only; legacy person-less
            # issues are excluded.
            if not item.get("person_id") or item.get("person_id") != self_pid:
                raise LearningError("普通账号只能查看自己的画像与题单。", 403)
            self._access(actor, scope=item.get("scope"), person_id=item.get("person_id"),
                         write=write, target_person=item.get("person_id"))
            return item
        self._access(actor, scope=item.get("scope"), person_id=item.get("person_id"),
                     write=write, target_person=item.get("person_id"))
        return item

    def public_issue(self, item, actor):
        value = {k: copy.deepcopy(v) for k, v in item.items() if not k.startswith("_")}
        q = value["question"]
        if not actor.get("is_admin"):
            for key in ("correct_option_ids", "answer_text", "source_answer", "analysis", "hint", "correction_history"):
                q.pop(key, None)
        q["attachments"] = [self._public_attachment(a) for a in q.get("attachments", []) if actor.get("is_admin") or a.get("kind") != "answer"]
        value["attachments"] = [self._public_attachment(a) for a in item.get("attachments", [])]
        value["sync_pending"] = bool(item.get("_dirty"))
        return value

    def create_issue(self, payload, actor):
        description = clean(payload.get("description", ""), 6000)
        if not description:
            raise LearningError("请填写题目存在的问题")
        category = payload.get("category", "题目")
        if category not in {"题目", "题干", "选项", "答案", "解析", "资料", "适用条件", "其他"}:
            raise LearningError("无效质疑类别")
        with self.transaction() as conn:
            paper = self._paper(payload.get("paper_id"), actor, conn, write=True)
            if payload.get("person_id") != paper["person_id"]:
                raise LearningError("请核对当前质疑所属人员。", 409)
            q = next((q for q in paper["questions"] if q["id"] == payload.get("question_id")), None)
            if not q:
                raise LearningError("请选择当前题单中的题目")
            existing = next((i for i in self._documents("issue", person_id=paper["person_id"], conn=conn) if i["question_id"] == q["id"] and i["question_version"] == q["version"] and i["status"] in {"pending", "processing", "needs_info"}), None)
            item = existing or {"id": "i_" + uuid.uuid4().hex, "person_id": paper["person_id"], "person": paper["person"], "date": now().date().isoformat(), "scope": paper["scope"], "paper_id": paper["id"], "question_id": q["id"], "question_version": q["version"], "category": category, "description": description,
                                "suggestion": clean(payload.get("suggestion", ""), 6000), "created_at": stamp(), "question": copy.deepcopy(q),
                                "attempt": copy.deepcopy((self._get("record", paper["id"], conn) or {}).get("entries", {}).get(q["id"], {}).get("attempt")), "status": "pending", "comments": [], "attachments": [], "version": 0}
            item["comments"].append({"actor": actor["id"], "name": actor.get("name", ""), "at": stamp(), "text": description, "is_admin": False})
            item["version"] += 1
            item["updated_at"] = stamp()
            self._put("issue", item["id"], item, conn)
        item["_dirty"] = True
        return self.public_issue(item, actor)

    def update_issue(self, payload, actor):
        with self.transaction() as conn:
            item = self.issue(payload.get("id"), actor, conn, write=True)
            if payload.get("version") != item["version"]:
                raise LearningError("质疑内容已更新，请重新读取", 409)
            status = payload.get("status", item["status"])
            if status not in ISSUE_STATES:
                raise LearningError("无效处理状态")
            remark = clean(payload.get("remark", payload.get("description", "")), 6000)
            if actor.get("is_admin"):
                if not remark:
                    raise LearningError("请填写处理说明")
            else:
                self._scope(actor, item["scope"], write=True)
                if status == "withdrawn":
                    if item["status"] != "pending":
                        raise LearningError("仅待处理质疑可撤回")
                elif status != item["status"] and status != "pending":
                    raise LearningError("只有管理员可设置处理结果", 403)
                if status != "withdrawn" and not remark:
                    raise LearningError("请填写补充说明")
            item["status"] = status
            item["version"] += 1
            item["updated_at"] = stamp()
            item["comments"].append({"actor": actor["id"], "name": actor.get("name", ""), "at": stamp(), "text": remark or "撤回质疑", "is_admin": bool(actor.get("is_admin"))})
            self._put("issue", item["id"], item, conn)
            if actor.get("is_admin"):
                nid = f"issue:{item['id']}:{item['version']}"
                self._put("notification", nid, {"id": nid, "kind": "issue", "date": now().date().isoformat(), "scope": item["scope"], "status": "pending", "message": f"【画像学练·质疑处理】\n题目：{item['question']['stem'][:150]}\n{remark}"}, conn)
        item["_dirty"] = True
        return self.public_issue(item, actor)

    def list_issues(self, actor, query):
        access = self._access(actor, scope=query.get("scope"), person_id=str(query.get("person_id") or ""))
        scope = access["scope"]
        identity = access["person_id"]
        items = self._documents("issue", scope=scope, person_id=identity)
        if query.get("status"):
            items = [i for i in items if i["status"] == query["status"]]
        if query.get("search"):
            term = query["search"].lower()
            items = [i for i in items if term in (i["question"]["stem"] + i["description"]).lower()]
        items.sort(key=lambda i: (i["updated_at"], i["id"]), reverse=True)
        result = self._page(items, query)
        result["items"] = [self.public_issue(i, actor) for i in result["items"]]
        return result

    def _attachment_allowed(self, attachment, actor):
        if actor.get("is_admin"):
            return
        access = self._access(actor)
        scope = access["scope"]
        self_pid = access["person_id"]
        if attachment.get("issue_id"):
            self.issue(attachment["issue_id"], actor)
            return
        for paper in self._all("paper"):
            if paper.get("deleted_at"):
                continue
            if scope and paper.get("scope") != scope:
                continue
            if self_pid and paper.get("person_id") != self_pid:
                continue
            for q in paper["questions"]:
                if not any(a["id"] == attachment["id"] for a in q.get("attachments", [])):
                    continue
                if attachment.get("kind") == "answer":
                    entry = (self._get("record", paper["id"]) or {}).get("entries", {}).get(q["id"], {})
                    if not entry.get("revealed") and not entry.get("attempt"):
                        continue
                return
        raise LearningError("无权查看该附件", 403)

    def attachment(self, identity, actor):
        value = self._get("attachment", identity)
        if not value:
            raise LearningError("附件不存在", 404)
        self._attachment_allowed(value, actor)
        local_name = value.get("local_file", "")
        path = self.root / "files" / local_name if local_name and Path(local_name).name == local_name else None
        if path is None or not path.is_file():
            if not value.get("file_token"):
                raise LearningError("附件尚未同步或本地文件缺失，请稍后重试", 409)
            content = self.cloud.download_attachment(value["file_token"], MAX_FILE)
            local_name = hashlib.sha256(content).hexdigest() + Path(value["name"]).suffix.lower()
            path = self._save_file(local_name, content)
            with self.transaction() as conn:
                latest = self._get("attachment", identity, conn)
                latest["local_file"] = local_name
                self._put("attachment", identity, latest, conn, latest["_dirty"])
        mime = mimetypes.guess_type(value["name"])[0] or "application/octet-stream"
        return path, value["name"], mime

    def _save_file(self, name, content):
        directory = self.root / "files"
        directory.mkdir(exist_ok=True)
        path = directory / name
        if not path.exists():
            temporary = directory / (uuid.uuid4().hex + ".tmp")
            temporary.write_bytes(content)
            temporary.replace(path)
        return path

    @staticmethod
    def _validate_file(name, content):
        name = Path(str(name).replace("\\", "/")).name
        if not name or len(name) > 180 or not content or len(content) > MAX_FILE:
            raise LearningError("文件为空、名称过长或超过20MiB", 413)
        extension = Path(name).suffix.lower()
        if extension not in {".png", ".jpg", ".jpeg", ".webp", ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".pptx", ".txt", ".csv"}:
            raise LearningError("仅支持图片、PDF、Office文档和文本资料")
        if extension in {".png", ".jpg", ".jpeg", ".webp"}:
            try:
                from PIL import Image
                with Image.open(io.BytesIO(content)) as image:
                    if image.width * image.height > 40000000:
                        raise ValueError("图片过大")
                    image.verify()
            except Exception as exc:
                raise LearningError("图片格式无效或尺寸过大") from exc
        elif extension == ".pdf" and not content.startswith(b"%PDF"):
            raise LearningError("PDF格式无效")
        elif extension in {".docx", ".xlsx", ".pptx"} and not content.startswith(b"PK"):
            raise LearningError("Office文档格式无效")
        elif extension in {".doc", ".xls"} and not content.startswith(bytes.fromhex("d0cf11e0a1b11ae1")):
            raise LearningError("Office文档格式无效")
        return name

    def add_attachments(self, files, query, actor):
        if not files or len(files) > 10 or sum(len(content) for _, content in files) > MAX_TOTAL:
            raise LearningError("每次最多10份文件，合计不超过100MiB", 413)
        kind = query.get("kind", "material")
        if kind not in {"question", "answer", "material"}:
            raise LearningError("附件类别无效")
        qid, issue_id = query.get("question_id"), query.get("issue_id")
        if bool(qid) == bool(issue_id):
            raise LearningError("请选择附件所属题目或质疑")
        prepared = [(self._validate_file(name, content), content) for name, content in files]
        with self.transaction() as conn:
            if qid:
                self._admin(actor)
                entity = self._question(qid, conn)
                entity_kind = "question"
            else:
                entity = self.issue(issue_id, actor, conn, write=True)
                entity_kind = "issue"
            if str(query.get("version")) != str(entity["version"]):
                raise LearningError("题目或质疑已更新，请读取最新版本后上传附件；当前填写请保留", 409)
            original = copy.deepcopy(entity) if qid else None
            attachments = entity.setdefault("attachments", [])
            seen = {(a.get("sha256"), a.get("kind")) for a in attachments}
            unique = []
            for name, content in prepared:
                signature = (hashlib.sha256(content).hexdigest(), kind)
                if signature not in seen:
                    seen.add(signature)
                    unique.append((name, content))
            if len(attachments) + len(unique) > 10 or sum(int(a.get("size") or 0) for a in attachments) + sum(len(content) for _, content in unique) > MAX_TOTAL:
                raise LearningError("每题或质疑最多10份附件、合计100MiB", 413)
            additions = []
            for name, content in unique:
                sha = hashlib.sha256(content).hexdigest()
                identity = "f_" + uuid.uuid4().hex
                filename = sha + Path(name).suffix.lower()
                self._save_file(filename, content)
                attachment = {"id": identity, "name": name, "kind": kind, "size": len(content), "sha256": sha, "local_file": filename, "owner": actor["id"], "question_id": qid, "issue_id": issue_id}
                self._put("attachment", identity, attachment, conn)
                attachments.append(attachment)
                additions.append(attachment)
            if entity_kind == "question":
                entity["version"] = uuid.uuid4().hex
                entity["problems"] = problems(entity)
                self._correct_papers(original, entity, "参考答案附件已更新", conn)
            else:
                entity["version"] += 1
            self._put(entity_kind, entity["id"], entity, conn)
        return {"items": [self._public_attachment(a) for a in additions], "attachments": [self._public_attachment(a) for a in attachments], "version": entity["version"]}

    def delete_attachment(self, identity, actor, version=None):
        with self.transaction() as conn:
            value = self._get("attachment", identity, conn)
            if not value:
                raise LearningError("附件不存在", 404)
            if value.get("question_id"):
                self._admin(actor)
                entity_kind = "question"
                entity = self._question(value["question_id"], conn)
            else:
                entity_kind = "issue"
                entity = self.issue(value["issue_id"], actor, conn, write=True)
            if str(version) != str(entity["version"]):
                raise LearningError("题目或质疑已更新，请读取最新版本后删除附件；当前填写请保留", 409)
            original = copy.deepcopy(entity) if entity_kind == "question" else None
            entity["attachments"] = [a for a in entity["attachments"] if a["id"] != identity]
            if entity_kind == "question":
                if value.get("file_token"):
                    entity["removed_attachment_tokens"] = list(dict.fromkeys([*entity.get("removed_attachment_tokens", []), value["file_token"]]))
                entity["problems"] = problems(entity)
                if entity["problems"] and entity["status"] == "published":
                    entity["status"] = "draft"
                entity["version"] = uuid.uuid4().hex
                self._correct_papers(original, entity, "参考答案附件已移除", conn)
            else:
                entity["version"] += 1
            self._put(entity_kind, entity["id"], entity, conn)
        # Keep bytes/token available to already-issued snapshots and their audit trail.
        return {"deleted": True, "version": entity["version"]}

    def export(self, kind, query, actor):
        self._admin(actor)
        if kind == "questions":
            values = [{k: v for k, v in q.items() if not k.startswith("_") and k not in {"record_id", "attachments", "id", "create_fingerprint"}} for q in self._filtered_questions(query)]
            return json.dumps({"format": "clipflow-learning-v1", "questions": values}, ensure_ascii=False, indent=2).encode(), "学练题库.json", "application/json"
        scope = self._access(actor, scope=query.get("scope"), person_id=str(query.get("person_id") or ""))["scope"]
        query = self._date_filters(query)
        output = io.StringIO(newline="")
        writer = csv.writer(output)
        writer.writerow(["日期", "楼栋", "姓名", "工号", "人员标识", "题库", "题目", "首次正确", "使用提示", "自评", "提交时间", "无效题", "更正说明", "操作账号"])
        safe = lambda value: "'" + str(value) if str(value).startswith(("=", "+", "-", "@", "\t", "\r")) else value
        for p in self._documents("paper", person_id=str(query.get("person_id") or ""), scope=scope,
                                 start=query.get("from", ""), end=query.get("to", ""), legacy=query.get("legacy") == "1"):
            if p.get("deleted_at") or scope and p["scope"] != scope or query.get("from") and p["date"] < query["from"] or query.get("to") and p["date"] > query["to"]:
                continue
            record = self._get("record", p["id"]) or {}
            for q in p["questions"]:
                if query.get("bank") and q["bank"] != query["bank"]:
                    continue
                a = record.get("entries", {}).get(q["id"], {}).get("attempt") or {}
                person = p.get("person") or {}
                writer.writerow([safe(value) for value in [p["date"], p["scope"], person.get("name", ""), person.get("employee_no", ""), p.get("person_id", ""), BANKS[q["bank"]], q["stem"], a.get("correct", ""), a.get("assisted", ""), a.get("self_rating", ""), a.get("submitted_at", ""), bool(q.get("invalid")), q.get("correction", ""), a.get("operator_name") or a.get("operator_id", "")]])
        return output.getvalue().encode("utf-8-sig"), "学练记录.csv", "text/csv; charset=utf-8"

    def dispatch(self, action, payload, actor, query):
        if not actor.get("id"):
            raise LearningError("登录身份不完整。", 401)
        if not actor.get("is_admin") and not actor.get("person_id") and actor.get("scope") not in SCOPES:
            raise LearningError("楼栋值班账号、管理员及已关联人员名单的普通账号可使用画像学练。", 403)
        identity = payload.get("id")
        if action in {"papers.list", "history"}:
            return self.list_papers(actor, query)
        if action == "people":
            from .learning_personal import people
            return people(self, actor, query)
        if action == "paper.claim":
            from .learning_personal import claim
            return claim(self, actor, payload)
        if action == "paper.get":
            return self.public_paper(self._paper(identity, actor), actor)
        if action == "paper.delete":
            return self.delete_paper(identity, actor)
        if action in {"paper.answer", "paper.reveal", "paper.notes"}:
            return self.paper_action(action.split(".")[1], payload, actor)
        if action == "review":
            return self.review(actor, query)
        if action == "profile":
            return self.profile(actor, query)
        if action == "attempts":
            return self.attempt_history(actor, query)
        if action == "issues.list":
            return self.list_issues(actor, query)
        if action == "issue.create":
            return self.create_issue(payload, actor)
        if action == "issue.update":
            return self.update_issue(payload, actor)
        if action == "questions.list":
            return self.questions(actor, query)
        if action == "attachment.delete":
            return self.delete_attachment(identity, actor, payload.get("version", query.get("version")))
        if action == "settings.get":
            self._admin(actor)
            return self.public_settings()
        if action in {"question.get", "question.save", "question.status", "question.copy", "settings.save", "refresh", "publish", "import"}:
            self._admin(actor)
        if action == "question.get":
            result = self.admin_question(self._question(identity))
            audit = [a for a in self._all("audit") if a.get("question_id") == identity]
            audit.sort(key=lambda a: (a["at"], a["id"]), reverse=True)
            result["audit"] = [{"at": a["at"], "actor": a["actor"], "reason": a["reason"], "before": self.admin_question(a.get("before") or {}), "after": self.admin_question(a.get("after") or {})} for a in audit[:20]]
            return result
        if action == "question.save":
            return self.save_question(payload, actor)
        if action == "question.status":
            ids = payload.get("ids") or [identity]
            if not isinstance(ids, list) or not 1 <= len(ids) <= 100:
                raise LearningError("每次最多操作100道题")
            results = []
            for qid in ids:
                q = self._question(qid)
                try:
                    expected = (payload.get("versions") or {}).get(qid) if len(ids) > 1 else payload.get("version")
                    results.append(self.save_question({"id": qid, "version": expected, "status": payload.get("status"), "reason": "管理员修改题目状态"}, actor))
                except LearningError as exc:
                    results.append({"id": qid, "error": str(exc), "status": exc.status})
            if len(results) == 1 and results[0].get("error"):
                raise LearningError(results[0]["error"], results[0]["status"])
            return results[0] if len(results) == 1 else {"items": results}
        if action == "question.copy":
            q = self._question(identity)
            clone = {k: v for k, v in q.items() if k not in {"id", "record_id", "attachments", "version", "family_id"} and not k.startswith("_")}
            clone["status"] = "draft"
            return self.save_question(clone, actor)
        if action == "settings.save":
            settings = self.settings()
            was_enabled = settings["enabled"]
            for flag in ("enabled", "reminder_enabled"):
                if flag in payload:
                    if not isinstance(payload[flag], bool):
                        raise LearningError("启用设置必须为布尔值")
                    settings[flag] = payload[flag]
            for field in ("publish_time", "reminder_time"):
                if field in payload:
                    if not isinstance(payload[field], str) or not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", payload[field]):
                        raise LearningError("请填写有效的发布或提醒时间")
                    settings[field] = payload[field]
            settings.pop("portal_url", None)
            with self.transaction() as conn:
                disable_pending = was_enabled and not settings["enabled"] or (self._get("local", "disable_sync", conn) or {}).get("required", False)
                if disable_pending:
                    self._put("local", "disable_sync", {"required": True}, conn, False)
                if was_enabled and not settings["enabled"]:
                    conn.execute("DELETE FROM documents WHERE kind='local' AND key='manual_publish'")
                self._put("settings", "main", settings, conn, bool(settings["enabled"] or disable_pending or self._schema_ready))
            if settings["enabled"]:
                self.request_refresh()
            return self.public_settings()
        if action == "refresh":
            return self.request_refresh()
        if action == "publish":
            if payload.get("date") and payload["date"] != now().date().isoformat():
                raise LearningError("只能发布今天的题单，不能补发往日任务")
            date = now().date().isoformat()
            with self.transaction() as conn:
                if self._get("publication", self._publication_key(date), conn):
                    return {"queued": False, "date": date, "already_published": True}
                self._put("local", "manual_publish", {"date": date}, conn, False)
            self._wake.set()
            return {"queued": True, "date": date, "notify": False}
        if action == "import":
            values = payload.get("questions")
            if not isinstance(values, list) or not 1 <= len(values) <= 500:
                raise LearningError("单次导入须为1至500道题")
            import_id = digest(values)
            prepared, errors = [], []
            for index, value in enumerate(values):
                try:
                    if not isinstance(value, dict):
                        raise LearningError("题目必须为对象")
                    candidate = {**value, "status": "draft", "new_id": f"q_import_{import_id}_{index}"}
                    candidate.pop("id", None)
                    candidate.pop("record_id", None)
                    prepared.append(self._validated_question(candidate))
                except LearningError as exc:
                    errors.append({"row": index + 1, "error": str(exc)})
            if payload.get("preview", False):
                return {"items": prepared, "errors": errors}
            if errors:
                raise LearningError("导入有无效内容，请先预检更正")
            if not self.settings()["enabled"]:
                raise LearningError("请先启用画像学练")
            # Imports are drafts until explicitly reviewed and published.
            with self.transaction() as conn:
                for q in prepared:
                    if not self._get("question", q["id"], conn):
                        self._put("question", q["id"], q, conn)
            return {"imported": len(prepared), "status": "draft"}
        raise LearningError("学练接口不存在", 404)
