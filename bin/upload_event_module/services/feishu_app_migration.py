"""Qt-owned credentials and an authenticated, one-time offline deployment upgrade."""
import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from pathlib import Path

from openclaw_service.assistant.lighthouse_ai import protect_key, unprotect_key
from openclaw_service.protocol import atomic_json
from ..utils import DATA_DIR

TARGET_APP_ID = "cli_aa42dc770a381cb3"
MIGRATION_MARKER = "feishu_app_migration_version"
PROFILE_PATH = Path(__file__).with_name("feishu_app_profile.json")
PACKAGED_CONFIG_PATH = Path(__file__).parents[1] / "config.json"
ITERATIONS = 600000
CONFIG_MAX_BYTES = 4 * 1024 * 1024
CREDENTIAL_FIELDS = ("feishu_app_id", "feishu_app_secret", "feishu_user_token", "token_expire_time")
_AGENT_LOCK = threading.RLock()
_LOG = logging.getLogger(__name__)


class FeishuAppMigrationError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__("Feishu app migration blocked (" + code + "); no credential fallback.")


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate field")
        result[key] = value
    return result


def _read_object(path, code, *, max_bytes=16384):
    try:
        with Path(path).open("rb") as stream:
            raw = stream.read(max_bytes + 1)
        value = json.loads(raw, object_pairs_hook=_unique_object)
        if len(raw) > max_bytes or not isinstance(value, dict):
            raise ValueError()
        return value
    except Exception:
        raise FeishuAppMigrationError(code) from None


def _secret(value, code):
    if not isinstance(value, str) or not 1 <= len(value) <= 500 or any(c.isspace() or ord(c) < 32 for c in value):
        raise FeishuAppMigrationError(code)
    return value


def _fingerprint(secret):
    return hashlib.sha256((TARGET_APP_ID + "\0" + secret).encode()).hexdigest()


def _valid_app(value):
    return isinstance(value, str) and bool(re.fullmatch(r"cli_[A-Za-z0-9]{1,120}", value))


def _validated_profile(value, *, legacy=False):
    fields = {"format", "target_app_id", "target_fingerprint", "algorithm",
              "kdf", "iterations", "salt", "nonce", "ciphertext"}
    fields.add("source_app_id" if legacy else "aad_mode")
    try:
        if (set(value) != fields or type(value["format"]) is not int or value["format"] != (1 if legacy else 2)
                or value["target_app_id"] != TARGET_APP_ID
                or value["algorithm"] != "AES-256-GCM" or value["kdf"] != "PBKDF2-HMAC-SHA256"
                or type(value["iterations"]) is not int or value["iterations"] != ITERATIONS
                or not isinstance(value["target_fingerprint"], str)
                or not re.fullmatch(r"[0-9a-f]{64}", value["target_fingerprint"])):
            raise ValueError()
        if legacy and not _valid_app(value["source_app_id"]):
            raise ValueError()
        if not legacy and value["aad_mode"] not in {"v2", "installed-app-v1"}:
            raise ValueError()
        for field, minimum, maximum in (("salt", 16, 16), ("nonce", 12, 12), ("ciphertext", 17, 8192)):
            if not isinstance(value[field], str) or not minimum <= len(base64.b64decode(value[field], validate=True)) <= maximum:
                raise ValueError()
    except Exception:
        raise FeishuAppMigrationError("profile_format") from None
    return value


def _profile(path):
    return _validated_profile(_read_object(path, "profile_format"))


def _key(secret, salt, *, legacy=False):
    return hashlib.pbkdf2_hmac("sha256", _secret(secret, "source_secret_missing").encode(),
                               (b"ClipFlow Feishu migration v1\0" if legacy else b"ClipFlow Feishu migration v2\0") + salt,
                               ITERATIONS, dklen=32)


def _aad(profile, source_app_id=None):
    metadata = {k: v for k, v in profile.items() if k != "ciphertext"}
    if profile.get("aad_mode") == "installed-app-v1":
        if not _valid_app(source_app_id):
            raise FeishuAppMigrationError("source_app_missing")
        metadata.pop("aad_mode")
        metadata.update(format=1, source_app_id=source_app_id)
    return json.dumps(metadata,
                      sort_keys=True, separators=(",", ":")).encode()


def _decrypt(profile, source_secret, source_app_id=None):
    try:
        from cryptography.hazmat.primitives.ciphers.aead import AESGCM
        legacy = profile["format"] == 1 or profile.get("aad_mode") == "installed-app-v1"
        content = AESGCM(_key(source_secret, base64.b64decode(profile["salt"]), legacy=legacy)).decrypt(
            base64.b64decode(profile["nonce"]), base64.b64decode(profile["ciphertext"]), _aad(profile, source_app_id))
        value = json.loads(content, object_pairs_hook=_unique_object)
        if not isinstance(value, dict) or set(value) != {"app_id", "app_secret"} or value["app_id"] != TARGET_APP_ID:
            raise ValueError()
        secret = _secret(value["app_secret"], "profile_payload")
        if not hmac.compare_digest(_fingerprint(secret), profile["target_fingerprint"]):
            raise ValueError()
        return secret
    except Exception:
        raise FeishuAppMigrationError("profile_authentication") from None


def _agent_secret(config):
    try:
        if config.get("app_id") != TARGET_APP_ID or not isinstance(config.get("secret_cipher"), str):
            raise ValueError()
        return _secret(unprotect_key(config["secret_cipher"]), "agent_secret_unreadable")
    except Exception:
        raise FeishuAppMigrationError("agent_secret_unreadable") from None


def _installed_settings(data_dir):
    # Do not import ConfigManager here: its load hook intentionally migrates credentials.
    database = data_dir / "lan_portal_state.sqlite3"
    try:
        if database.is_file():
            with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
                rows = connection.execute("SELECT key, value_json FROM settings").fetchall()
            if rows:
                return {key: json.loads(value) for key, value in rows}
        return _read_object(data_dir / "config.json", "source_config_unavailable", max_bytes=CONFIG_MAX_BYTES)
    except Exception:
        raise FeishuAppMigrationError("source_config_unavailable") from None


@contextmanager
def qt_settings_transaction(data_root):
    database = Path(data_root) / "lan_portal_state.sqlite3"
    database.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(database, timeout=5)) as connection:
        connection.execute("BEGIN IMMEDIATE")
        with connection:
            if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='settings'").fetchone():
                connection.execute("CREATE TABLE settings (key TEXT PRIMARY KEY, value_json TEXT NOT NULL, updated_at REAL NOT NULL)")
            yield connection


def migration_complete(connection):
    row = connection.execute("SELECT value_json FROM settings WHERE key=?", (MIGRATION_MARKER,)).fetchone()
    return bool(row and json.loads(row[0]) == 2)


def read_credentials(connection, *, include_migration=False):
    keys = (*CREDENTIAL_FIELDS, MIGRATION_MARKER) if include_migration else CREDENTIAL_FIELDS
    rows = connection.execute(
        "SELECT key, value_json FROM settings WHERE key IN (" + ",".join("?" for _ in keys) + ")", keys).fetchall()
    value = {key: json.loads(raw) for key, raw in rows}
    result = {key: value.get(key, 0 if key == "token_expire_time" else "") for key in CREDENTIAL_FIELDS}
    if any(not isinstance(result[key], str) for key in CREDENTIAL_FIELDS[:3]):
        raise FeishuAppMigrationError("settings_credentials_invalid")
    if not isinstance(result["token_expire_time"], (int, float, str)):
        raise FeishuAppMigrationError("settings_credentials_invalid")
    if include_migration:
        result[MIGRATION_MARKER] = value.get(MIGRATION_MARKER)
    return result


def write_settings(connection, values):
    connection.executemany("INSERT OR REPLACE INTO settings(key, value_json, updated_at) VALUES (?, ?, ?)",
        [(key, json.dumps(value, ensure_ascii=False), time.time()) for key, value in values.items()])


def _wrapped_profile(source_secret, target_secret):
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    salt, nonce = secrets.token_bytes(16), secrets.token_bytes(12)
    profile = {"format": 2, "aad_mode": "v2", "target_app_id": TARGET_APP_ID,
               "target_fingerprint": _fingerprint(target_secret), "algorithm": "AES-256-GCM",
               "kdf": "PBKDF2-HMAC-SHA256", "iterations": ITERATIONS,
               "salt": base64.b64encode(salt).decode(), "nonce": base64.b64encode(nonce).decode()}
    content = json.dumps({"app_id": TARGET_APP_ID, "app_secret": target_secret}, separators=(",", ":")).encode()
    profile["ciphertext"] = base64.b64encode(AESGCM(_key(source_secret, salt)).encrypt(nonce, content, _aad(profile))).decode()
    _decrypt(profile, source_secret)
    return profile


def upgrade_deployment_profile(*, profile_path=None, source_secret=None, expected_target_secret=None):
    """Upgrade locally without retaining a deployment's source app identifier.

    Without its wrapping secret, retain the authenticated ciphertext and recover
    the exact original AAD from each updating installation's current Qt app ID.
    """
    path = Path(profile_path or PROFILE_PATH)
    value = _read_object(path, "profile_format")
    if value.get("format") == 2:
        _validated_profile(value)
        return path
    legacy = _validated_profile(value, legacy=True)
    if expected_target_secret is not None and not hmac.compare_digest(
            legacy["target_fingerprint"], _fingerprint(_secret(expected_target_secret, "target_secret_missing"))):
        raise FeishuAppMigrationError("profile_target_mismatch")
    if source_secret:
        target_secret = _decrypt(legacy, source_secret)
        upgraded = _wrapped_profile(source_secret, target_secret)
    else:
        upgraded = {key: value for key, value in legacy.items() if key != "source_app_id"}
        upgraded.update(format=2, aad_mode="installed-app-v1")
        if _aad(upgraded, legacy["source_app_id"]) != _aad(legacy):
            raise FeishuAppMigrationError("profile_upgrade_aad")
    _validated_profile(upgraded)
    atomic_json(path, upgraded)
    return path


def build_migration_profile(source_secret, new_secret, *, profile_path=None):
    """Wrap a new deployment, or reuse its matching published authenticated profile."""
    path = Path(profile_path or PROFILE_PATH)
    secret = _secret(new_secret, "target_secret_missing")
    if path.exists():
        upgrade_deployment_profile(profile_path=path, source_secret=source_secret, expected_target_secret=secret)
        previous = _profile(path)
        if previous["target_fingerprint"] == _fingerprint(secret):
            if source_secret and previous["aad_mode"] == "v2":
                _decrypt(previous, source_secret)
            return path
    if not source_secret:
        raise FeishuAppMigrationError("source_secret_missing")
    try:
        atomic_json(path, _wrapped_profile(source_secret, secret))
    except Exception:
        raise FeishuAppMigrationError("profile_build_failed") from None
    return path


def prepare_deployment_profile(*, data_root=None, profile_path=None):
    """Build-only SQLite-first discovery; no runtime credentials are overwritten."""
    data_dir, path = Path(data_root or DATA_DIR), Path(profile_path or PROFILE_PATH)
    source = _installed_settings(data_dir)
    app_id = source.get("feishu_app_id")
    if not _valid_app(app_id):
        raise FeishuAppMigrationError("profile_target_not_configured")
    if app_id != TARGET_APP_ID:
        _secret(source.get("feishu_app_secret"), "qt_secret_invalid")
        database = data_dir / "lan_portal_state.sqlite3"
        if database.is_file():
            with closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as connection:
                if migration_complete(connection):
                    if not path.is_file():
                        return None
                    try:
                        upgrade_deployment_profile(profile_path=path)
                    except FeishuAppMigrationError:
                        _LOG.warning("Feishu deployment profile skipped (profile_format).")
                        return None
                    return path
        secret = _agent_secret(_read_object(data_dir / "feishu_assistant.json", "agent_config_unavailable"))
    else:
        secret = _secret(source.get("feishu_app_secret"), "target_secret_missing")
    if path.is_file():
        upgrade_deployment_profile(profile_path=path, expected_target_secret=secret)
        if _profile(path)["target_fingerprint"] == _fingerprint(secret):
            return path
    source_secret = source.get("feishu_app_secret") if app_id != TARGET_APP_ID else None
    if not source_secret:
        for backup_path in (data_dir / "config.json", PACKAGED_CONFIG_PATH):
            if backup_path.is_file():
                backup = _read_object(backup_path, "source_config_unavailable", max_bytes=CONFIG_MAX_BYTES)
                if _valid_app(backup.get("feishu_app_id")) and backup["feishu_app_id"] != TARGET_APP_ID:
                    source_secret = _secret(backup.get("feishu_app_secret"), "source_secret_missing")
                    break
    return build_migration_profile(source_secret, secret, profile_path=path)


def sync_agent_config(app_id, app_secret, *, data_root=None):
    """Copy valid Qt credentials to the agent, never derive Qt credentials from it."""
    if not _valid_app(app_id):
        return False  # Non-Feishu offline fixtures do not create an agent configuration.
    data_dir = Path(data_root or DATA_DIR)
    agent_path = data_dir / "feishu_assistant.json"
    # Serialize canonical Qt changes with this mirror update across processes.
    with qt_settings_transaction(data_dir) as connection, _AGENT_LOCK:
        canonical = read_credentials(connection)
        app_id = canonical["feishu_app_id"]
        if not _valid_app(app_id) or app_id != TARGET_APP_ID and not migration_complete(connection):
            return False
        secret = _secret(canonical["feishu_app_secret"], "qt_secret_invalid")
        agent = _read_object(agent_path, "agent_config_invalid") if agent_path.exists() else {}
        try:
            same_secret = unprotect_key(agent.get("secret_cipher") or "") == secret
        except Exception:
            same_secret = False
        try:
            bridge_valid = bool(unprotect_key(agent.get("bridge_cipher") or ""))
        except Exception:
            bridge_valid = False
        changed_app = agent.get("app_id") != app_id
        retired_fields = {"app_secret", "feishu_app_id", "feishu_app_secret", "token", "tenant_access_token",
                          "app_access_token", "user_access_token", "user_token", "feishu_user_token",
                          "token_expire_time", "expires", "expires_at"}
        if changed_app or not same_secret or not bridge_valid or type(agent.get("enabled")) is not bool or retired_fields & agent.keys():
            try:
                # Match save_config's fields, preserving an explicit local disable.
                saved = {**{k: v for k, v in agent.items() if k not in retired_fields},
                         "app_id": app_id, "secret_cipher": protect_key(secret),
                         "enabled": agent.get("enabled") is not False}
                if changed_app or not bridge_valid:
                    saved["bridge_cipher"] = protect_key(secrets.token_urlsafe(32))
                if changed_app and agent.get("app_id"):
                    saved["isolated_inbox"] = True
                atomic_json(agent_path, saved)
            except Exception:
                raise FeishuAppMigrationError("agent_config_persist_failed") from None
            return True
    return False


def effective_credentials(app_id, app_secret, *, data_root=None, profile_path=None, _connection=None):
    """Authenticate the installed secret once, then honor all future Qt choices."""
    if not _valid_app(app_id):
        return app_id, app_secret
    data_dir = Path(data_root or DATA_DIR)
    if _connection is None:
        with qt_settings_transaction(data_dir) as connection:
            if not connection.execute("SELECT 1 FROM settings LIMIT 1").fetchone() and (data_dir / "config.json").is_file():
                write_settings(connection, _read_object(data_dir / "config.json", "source_config_unavailable",
                                                        max_bytes=CONFIG_MAX_BYTES))
            pair = effective_credentials(app_id, app_secret, data_root=data_dir,
                                         profile_path=profile_path, _connection=connection)
        sync_agent_config(*pair, data_root=data_dir)
        return pair
    canonical = read_credentials(_connection)
    qt_app = canonical["feishu_app_id"]
    if qt_app:
        if not _valid_app(qt_app):
            raise FeishuAppMigrationError("qt_app_invalid")
        app_id = qt_app
        app_secret = canonical["feishu_app_secret"]
    secret = _secret(app_secret, "qt_secret_invalid")
    if app_id == TARGET_APP_ID:
        if not migration_complete(_connection):
            write_settings(_connection, {MIGRATION_MARKER: 2})
        return app_id, secret
    if migration_complete(_connection):
        return app_id, secret
    path = Path(profile_path or PROFILE_PATH)
    if not path.is_file():
        raise FeishuAppMigrationError("profile_missing")
    secret = _decrypt(_profile(path), secret, app_id)
    write_settings(_connection, {"feishu_app_id": TARGET_APP_ID, "feishu_app_secret": secret,
                                 "feishu_user_token": "", "token_expire_time": 0, MIGRATION_MARKER: 2})
    return TARGET_APP_ID, secret


def migrate_config(settings, *, data_dir=None, profile_path=None, _connection=None):
    """One authenticated installation upgrade; leave unrelated settings untouched."""
    app_id = settings.get("feishu_app_id")
    if not _valid_app(app_id):
        return settings
    if _connection is not None and migration_complete(_connection):
        return settings
    target, secret = effective_credentials(app_id, settings.get("feishu_app_secret"),
                                           data_root=data_dir, profile_path=profile_path, _connection=_connection)
    if (target, secret) == (app_id, settings.get("feishu_app_secret")):
        return settings
    return {**settings, "feishu_app_id": target, "feishu_app_secret": secret,
            "feishu_user_token": "", "token_expire_time": 0}


def cleanup_legacy_configs(app_id, *, data_root=None, profile_path=None):
    """Best-effort cleanup of superseded auth copies in three known Qt-owned inputs."""
    if not _valid_app(app_id):
        return False
    profile_path = Path(profile_path or PROFILE_PATH)
    try:
        if not profile_path.is_file():
            return False  # Preserve the first build's wrapping input until its profile exists.
        profile = _profile(profile_path)
    except Exception:
        _LOG.warning("Feishu legacy cleanup skipped (profile_unavailable).")
        return False
    data_dir = Path(data_root or DATA_DIR)
    changed = False
    for label, path in (("data_config", data_dir / "config.json"), ("module_config", PACKAGED_CONFIG_PATH),
                        ("plan_convergence", data_dir / "plan_convergence" / "feishu.json")):
        try:
            if not path.is_file():
                continue
            value = _read_object(path, "legacy_config_invalid", max_bytes=CONFIG_MAX_BYTES)
            fields = set()
            for id_key, secret_key, auth_fields in (
                    ("feishu_app_id", "feishu_app_secret", CREDENTIAL_FIELDS),
                    ("app_id", "app_secret", ("app_id", "app_secret", "secret_cipher"))):
                candidate_app = value.get(id_key)
                if not _valid_app(candidate_app):
                    continue
                try:
                    candidate_secret = value.get(secret_key)
                    if not candidate_secret and id_key == "app_id" and value.get("secret_cipher"):
                        candidate_secret = unprotect_key(value["secret_cipher"])
                    candidate_secret = _secret(candidate_secret, "legacy_secret_unreadable")
                    if candidate_app != TARGET_APP_ID or _fingerprint(candidate_secret) != profile["target_fingerprint"]:
                        _decrypt(profile, candidate_secret, candidate_app)
                except FeishuAppMigrationError:
                    continue
                fields.update(auth_fields)
            if not fields:
                continue
            fields.update({"token", "tenant_access_token", "app_access_token", "access_token", "refresh_token",
                           "user_access_token", "user_token", "token_expire_time", "expires", "expires_at"})
            saved = {key: part for key, part in value.items() if key not in fields}
            saved["feishu_credentials_source"] = "qt_sqlite"
            atomic_json(path, saved)
        except Exception:
            _LOG.warning("Feishu legacy cleanup skipped (%s; legacy_config_cleanup_failed).", label)
            continue
        changed = True
    return changed
