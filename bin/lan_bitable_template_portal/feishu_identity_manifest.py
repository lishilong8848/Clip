"""Trusted evidence for a one-time account rewrite, never runtime authorization."""
import json
import re
from collections import defaultdict
from pathlib import Path


TARGET_APP_ID = "cli_aa42dc770a381cb3"
PROFILE_PATH = Path(__file__).with_name("feishu_identity_profile.json")
DUTY_PAIRS = {
    "ou_3e0a9399a037276f8449cc778f18bb8a": "ou_822759df685e2807d538fb9cf5465465",
    "ou_160e991b0fd811cda7260de056d8a363": "ou_ac4e1a554a6cabab4d8eea7e3956bec3",
    "ou_99391b00c1406917930483d8500af400": "ou_f2879fa04399d43bff26f92616678b6d",
    "ou_b9c26b4da3721c2de11ee175c707e756": "ou_18396043dc393fafd3b7f43f0e284396",
    "ou_f0fe7137818e8b422b0594e51eab13f3": "ou_fb53efe356466339f4f3cc22f2c7f9fb",
    "ou_55647926b7fbfe46507ef7e8afa76315": "ou_3e7a45be43f809cd54e277243e9821ab",
    "ou_913ff495ae8f5a0a55ae5670fd148bbd": "ou_ca8103bc15b06b448ab94958d5cfef43",
}
EVIDENCE = frozenset({"personnel_snapshot", "hr_text_open_id", "target_hr_person_field",
                      "sender_record_id", "sender_union_id", "sender_admin", "user_supplied_duty", "shared_rows"})
OPEN_ID = re.compile(r"ou_[A-Za-z0-9]+")
RECORD_ID = re.compile(r"rec[A-Za-z0-9]+")


def _valid_id(value, pattern):
    return isinstance(value, str) and pattern.fullmatch(value) is not None


def _text_ids(value):
    if isinstance(value, list):
        return set().union(*(_text_ids(item) for item in value)) if value else set()
    if isinstance(value, dict):
        return _text_ids(value.get("text", ""))
    return set(re.findall(r"(?<![A-Za-z0-9_])ou_[A-Za-z0-9]+(?![A-Za-z0-9_])", value)) if isinstance(value, str) else set()


def _person_id(value):
    if isinstance(value, list):
        if len(value) != 1:
            return ""
        value = value[0]
    if not isinstance(value, dict):
        return ""
    ids = {value[key] for key in ("open_id", "openId", "openid", "user_id", "userId", "id")
           if _valid_id(value.get(key), OPEN_ID)}
    return next(iter(ids)) if len(ids) == 1 else ""


def _unresolved(reason, records=(), old=(), new=(), evidence=()):
    return {"reason": reason, "record_ids": sorted(set(records)),
            "old_open_ids": sorted(set(old)), "new_open_ids": sorted(set(new)),
            "evidence": sorted(set(evidence))}


def _finish(mappings, unresolved):
    pairs = {}
    for item in mappings:
        key = item["old_open_id"], item["new_open_id"]
        entry = pairs.setdefault(key, {"old_open_id": key[0], "new_open_id": key[1],
                                      "record_ids": set(), "evidence": set()})
        entry["record_ids"].update(item["record_ids"])
        entry["evidence"].update(item["evidence"])
    sources, targets, records = defaultdict(set), defaultdict(set), defaultdict(set)
    for (old, new), item in pairs.items():
        sources[old].add(new)
        targets[new].add(old)
        for rid in item["record_ids"]:
            records[rid].add((old, new))
    denied_old, denied_new = set(), set()
    for item in unresolved:
        if item["reason"] in {"old_evidence_conflict", "old_id_multiple_targets", "target_multiple_old_ids",
                              "invalid_snapshot_identity", "duplicate_hr_record_id", "record_identity_conflict",
                              "identity_chain", "conflicting_saved_evidence"}:
            denied_old.update(item["old_open_ids"])
            denied_new.update(item["new_open_ids"])
    accepted = []
    for (old, new), item in sorted(pairs.items()):
        reason = ("record_identity_conflict" if any(len(records[rid]) > 1 for rid in item["record_ids"]) else
                  "old_id_multiple_targets" if len(sources[old]) > 1 else
                  "target_multiple_old_ids" if len(targets[new]) > 1 else
                  "identity_chain" if old in targets or new in sources else
                  "conflicting_saved_evidence" if old in denied_old or new in denied_new else "")
        if reason:
            unresolved.append(_unresolved(reason, item["record_ids"], [old], [new], item["evidence"]))
        else:
            if len(item["record_ids"]) > 1:
                item["evidence"].add("shared_rows")
            accepted.append({**item, "record_ids": sorted(item["record_ids"]), "evidence": sorted(item["evidence"])})
    unique = {json.dumps(item, sort_keys=True): item for item in unresolved}
    return {"version": 1, "target_app_id": TARGET_APP_ID, "mappings": accepted,
            "unresolved": [unique[key] for key in sorted(unique)]}


def build_identity_manifest(snapshot, sender_bindings, hr_rows, *, target_app_id):
    """Join trusted local records to a complete HR read made by target_app_id.

    Callers supply server-read data, never client names or identity claims.
    Inactive records may identify existing history; mappings grant no access.
    """
    if target_app_id != TARGET_APP_ID:
        raise ValueError("Identity manifest requires the verified target application")
    people = snapshot.get("people") or {}
    saved = defaultdict(list)
    for row in sender_bindings:
        binding = row.get("payload") or {}
        matched_by = binding.get("matched_by")
        rid, oid = binding.get("record_id"), binding.get("open_id")
        if (matched_by in {"record_id", "union_id", "admin"}
                and _valid_id(rid, RECORD_ID) and _valid_id(oid, OPEN_ID)):
            saved[rid].append((oid, "sender_" + matched_by))
    by_record = defaultdict(list)
    unresolved, mappings = [], []
    for row in hr_rows:
        rid = row.get("record_id")
        if not _valid_id(rid, RECORD_ID):
            unresolved.append(_unresolved("invalid_hr_record_id"))
        else:
            by_record[rid].append(row)
    for rid, rows in sorted(by_record.items()):
        sources = defaultdict(set)
        previous = people.get(rid) or {}
        old_ids = previous.get("login_ids") or []
        invalid_saved = (previous.get("id", rid) != rid or not isinstance(old_ids, list)
                         or any(not _valid_id(oid, OPEN_ID) for oid in old_ids))
        if not invalid_saved:
            for oid in old_ids:
                sources[oid].add("personnel_snapshot")
        for oid, evidence in saved[rid]:
            sources[oid].add(evidence)
        fresh = set()
        for row in rows:
            fields = row.get("fields") or {}
            for oid in _text_ids(fields.get("飞书 open_id")):
                sources[oid].add("hr_text_open_id")
            oid = _person_id(fields.get("员工姓名"))
            if oid:
                fresh.add(oid)
        evidence = set().union(*sources.values()) if sources else set()
        if fresh:
            evidence.add("target_hr_person_field")
        reason = ("duplicate_hr_record_id" if len(rows) != 1 else
                  "invalid_snapshot_identity" if invalid_saved else
                  "missing_or_ambiguous_target_person" if len(fresh) != 1 else "")
        if reason:
            unresolved.append(_unresolved(reason, [rid], sources, fresh, evidence))
            continue
        new = next(iter(fresh))
        # A snapshot already rewritten to the target is not contradictory old evidence.
        old = set(sources) - {new}
        if len(old) != 1:
            if not old and new in sources:
                continue
            unresolved.append(_unresolved("old_evidence_conflict" if old else "missing_old_evidence",
                                          [rid], old, [new], evidence))
            continue
        prior = next(iter(old))
        mappings.append({"old_open_id": prior, "new_open_id": new, "record_ids": [rid],
                         "evidence": sorted(sources[prior] | {"target_hr_person_field"})})
    for rid in sorted(set(people) | set(saved)):
        if rid not in by_record and _valid_id(rid, RECORD_ID):
            old = [oid for oid in (people.get(rid) or {}).get("login_ids", []) if _valid_id(oid, OPEN_ID)]
            old.extend(oid for oid, _ in saved[rid])
            unresolved.append(_unresolved("record_missing_from_target_hr", [rid], old))
    mappings.extend({"old_open_id": old, "new_open_id": new, "record_ids": [],
                     "evidence": ["user_supplied_duty"]} for old, new in DUTY_PAIRS.items())
    return _finish(mappings, unresolved)


def merge_identity_manifests(*manifests):
    """Merge packaged and independently built trusted local evidence, without API calls."""
    mappings, unresolved = [], []
    for manifest in manifests:
        if (not isinstance(manifest, dict) or set(manifest) != {"version", "target_app_id", "mappings", "unresolved"}
                or type(manifest["version"]) is not int or manifest["version"] != 1
                or manifest["target_app_id"] != TARGET_APP_ID):
            raise ValueError("Unsupported identity manifest")
        for section in ("mappings", "unresolved"):
            if not isinstance(manifest[section], list):
                raise ValueError("Invalid identity manifest entries")
            for item in manifest[section]:
                fields = ({"old_open_id", "new_open_id", "record_ids", "evidence"} if section == "mappings" else
                          {"reason", "old_open_ids", "new_open_ids", "record_ids", "evidence"})
                if not isinstance(item, dict) or set(item) != fields:
                    raise ValueError("Invalid identity manifest entry")
                if (not isinstance(item["record_ids"], list)
                        or any(not _valid_id(rid, RECORD_ID) for rid in item["record_ids"])
                        or not isinstance(item["evidence"], list) or not item["evidence"] and section == "mappings"
                        or any(not isinstance(source, str) or source not in EVIDENCE for source in item["evidence"])):
                    raise ValueError("Invalid identity manifest evidence")
                if section == "mappings":
                    if (not _valid_id(item["old_open_id"], OPEN_ID) or not _valid_id(item["new_open_id"], OPEN_ID)
                            or item["old_open_id"] == item["new_open_id"]):
                        raise ValueError("Invalid identity manifest mapping")
                    duty = (DUTY_PAIRS.get(item["old_open_id"]) == item["new_open_id"]
                            and "user_supplied_duty" in item["evidence"])
                    old_evidence = EVIDENCE - {"target_hr_person_field", "user_supplied_duty", "shared_rows"}
                    if not duty and (not item["record_ids"] or "target_hr_person_field" not in item["evidence"]
                                     or not old_evidence.intersection(item["evidence"])):
                        raise ValueError("Identity manifest needs a stable HR record")
                else:
                    if not isinstance(item["reason"], str) or not re.fullmatch(r"[a-z_]+", item["reason"]):
                        raise ValueError("Invalid unresolved identity reason")
                    for key in ("old_open_ids", "new_open_ids"):
                        if not isinstance(item[key], list) or any(not _valid_id(oid, OPEN_ID) for oid in item[key]):
                            raise ValueError("Invalid unresolved identity")
                (mappings if section == "mappings" else unresolved).append({**item})
    return _finish(mappings, unresolved)


def validate_manifest(payload):
    """Return a validated document, or raise ValueError; this never reads credentials."""
    result = merge_identity_manifests(payload)
    requested = {(item["old_open_id"], item["new_open_id"]) for item in payload["mappings"]}
    accepted = {(item["old_open_id"], item["new_open_id"]) for item in result["mappings"]}
    if requested != accepted:
        raise ValueError("Conflicting identity manifest mappings")
    return result


def load_identity_manifest(path=None):
    return validate_manifest(json.loads(Path(path or PROFILE_PATH).read_text(encoding="utf-8")))


load_manifest = load_identity_manifest
