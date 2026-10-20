"""Pure, schema-scoped identity migration; never opens stores or sends requests.

Franklin's transaction layer must reject a destination (namespace, newkey)
already occupied by a different document. This single-document API cannot see
that collision and intentionally provides no conversation merge or upsert.
Native state is disposable. Operation IDs, receipts and historical text are not.
"""
from __future__ import annotations

from collections.abc import Mapping
import copy
import hashlib
import json
import uuid

from .feishu_identity_manifest import TARGET_APP_ID


class IdentityMigrationConflict(ValueError):
    """Ambiguous identity mapping, colliding keys, or unverifiable keyed data."""


_IDENTITIES = frozenset({
    'owner', 'owner_id', 'owner_open_id', 'source_owner', 'open_id', 'actor',
    'actor_id', 'operator', 'operator_id', 'user_id', 'sender_id', 'author_id',
    'creator_id', 'created_by', 'updated_by', 'authorized_by', '_auth_open_id',
    'actor_open_id', 'actor_openid', 'created_by_openid', 'created_by_open_id',
    'signer_open_id', 'approver_open_id', 'requested_by_openid', 'requested_by_open_id',
    'submitted_by_open_id', 'updated_by_open_id', 'uploaded_by_open_id', 'uploader_id',
    'operator_open_id', 'recipient_open_id', 'recipient', 'recipient_id', 'reviewer_id',
})
_IDENTITY_LISTS = frozenset({
    'login_ids', 'eligible', 'owners', 'readers', 'writers', 'participants',
    'fixed_recipients', 'recipient_ids', 'reviewer_ids', 'manager_ids',
})
_IDENTITY_OBJECTS = frozenset({
    'actor', 'user', 'owner', 'operator', 'owner_profile', 'operator_profile', 'person', 'reviewer',
})
_PEOPLE_LISTS = frozenset({'people', 'persons', 'recipients', 'reviewers', 'final_managers'})
_RECORDS = frozenset({
    'turns', 'plan', 'plans', 'context', '_task_context', '_workspace_document',
    'topic_access', 'delivery_access', 'access', 'capabilities', '_shared_plan',
    'profile', 'original', 'reply',
    'request', 'retry_request', 'request_payload', 'prepared', 'prepared_payload',
    'payload', 'data', 'data_dict', 'kwargs', 'persons', 'recipients',
    'items', 'answers', 'evidence',
})
_BUCKETS = frozenset({'documents', 'packages', 'memories'})
_DOCUMENTS = frozenset({
    'lighthouse_messages', 'lighthouse_runs', 'lighthouse_agent_plans', 'lighthouse_files',
    'lighthouse_topics', 'lighthouse_topic_grants', 'lighthouse_topic_audit',
    'feishu_assistant_identity', 'feishu_assistant_sender', 'feishu_question_cards',
    'feishu_notice_cards', 'personnel_password_login', 'message_recipient_directory',
    'account_management',
    'auth_permissions', 'auth_sessions', 'permission_requests', 'permissions',
    'notice_action_job', 'notice_upload_attachments', 'qt_active_cache', 'qt_notice_history',
    'runtime_state', 'notice_memory', 'notice_daily_summary', 'notice_work_status', 'notice_hidden_ongoing',
    'auth_oauth_states', 'append_events', 'ongoing_items', 'qt_active_items',
    'clipboard_candidates', 'dialog_sessions', 'event_outbox', 'runtime_task_queue',
    'notice_remote_operations', 'repair_management_operations', 'notice_undo_actions',
    'work_handover_versions', 'work_handover_sources', 'risk_people', 'risk_jobs',
    'notice_handoffs', 'batch_rows', 'batch_images', 'batch_runtime_status', 'batch_list_views',
    'records', 'inventory', 'rosters', 'photo_deliveries', 'deliveries', 'documents',
    'engineer_mop_local_files', 'mop_signature_usage_confirmations',
    'mop_temporary_signature_sessions', 'signature_link_tokens', 'mop_notice_bindings',
    'mop_fill_memory', 'handover_links', 'notice_work_type_overrides',
    'business_operation_audits', 'deletion_audit_outbox', 'water_consumption_operations',
    'water_consumption_record_edits', 'critical_guard_tasks', 'critical_guard_responses',
    'critical_guard_signature_sets', 'critical_guard_memories', 'critical_guard_scope_files',
    'critical_guard_scope_templates', 'critical_guard_weather_jobs', 'batches', 'definitions',
    'tasks', 'files', 'photo_preferences', 'photo_previews', 'work_handovers',
    'work_handover_audit', 'work_handover_outbox', 'risk_releases', 'risk_audit', 'knowledge_groups',
    'learning_person',
})


def _mapping(value):
    if not isinstance(value, Mapping):
        raise ValueError('Identity mapping must be an old-to-new dictionary')
    if any(not isinstance(k, str) or not isinstance(v, str) or not k or not v
           or k.strip() != k or v.strip() != v or ':' in k or ':' in v
           for k, v in value.items()):
        raise ValueError('Identity mapping requires exact nonempty account IDs')
    result = {k: v for k, v in value.items() if k != v}
    if len(set(result.values())) != len(result) or set(result) & set(result.values()):
        raise IdentityMigrationConflict('Identity mapping is not one-to-one and idempotent')
    return result


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False).encode()).hexdigest()


def _account_key(key, mapping):
    account, separator, suffix = key.partition(':')
    return mapping.get(account, account) + separator + suffix


def _rekey(values, key_function, transform):
    result = {}
    for key, value in values.items():
        newkey = key_function(key)
        if newkey in result:
            raise IdentityMigrationConflict('Identity dictionary keys collide')
        result[newkey] = transform(value)
    return result


def _walk(value, mapping, *, identity_object=False):
    """Visit known structural records, never bodies, content, results or receipts."""
    if isinstance(value, list):
        return [_walk(item, mapping, identity_object=identity_object) for item in value]
    if not isinstance(value, dict):
        return value
    original = value
    result = dict(value)
    result.pop('native_history_session', None)
    for field, item in original.items():
        if field in _IDENTITIES or identity_object and field == 'id':
            if isinstance(item, str):
                result[field] = mapping.get(item, item)
        if field in _IDENTITY_LISTS and isinstance(item, list):
            result[field] = list(dict.fromkeys(mapping.get(entry, entry) for entry in item)) if all(
                isinstance(entry, str) for entry in item) else item
        if field in _IDENTITY_OBJECTS and isinstance(item, dict):
            result[field] = _walk(item, mapping, identity_object=True)
        elif field in {'people', 'users', 'removed_identities'} and isinstance(item, dict):
            result[field] = _rekey(item, lambda key: mapping.get(key, key),
                                   lambda row: _walk(row, mapping, identity_object=True))
        elif field in _PEOPLE_LISTS and isinstance(item, list):
            result[field] = [mapping.get(row, row) if isinstance(row, str)
                             else _walk(row, mapping, identity_object=True) for row in item]
        elif field == 'execution_lanes' and isinstance(item, dict):
            result[field] = _rekey(item, lambda key: _account_key(key, mapping), lambda row: _walk(row, mapping))
        elif field in _BUCKETS and isinstance(item, dict):
            result[field] = {key: _walk(row, mapping) for key, row in item.items()}
        elif field in _RECORDS:
            result[field] = _walk(item, mapping)
        elif field == 'events' and isinstance(item, list):
            result[field] = [dict(event, data=_walk(event['data'], mapping)) if isinstance(event, dict)
                and event.get('type') == 'data-turn' and isinstance(event.get('data'), dict) else event for event in item]
    aliases = original.get('feishu_open_ids')
    affected = any(original.get(field) != result.get(field) for field in _IDENTITIES | _IDENTITY_LISTS)
    if isinstance(aliases, dict) and (affected or any(oid in mapping for oid in aliases.values() if isinstance(oid, str))):
        # Canonical login_ids migrate; another app's provider IDs are not login aliases.
        result.pop('feishu_open_ids', None)
    return result


def _metadata(payload):
    metadata = payload.setdefault('identity_migration', {})
    if not isinstance(metadata, dict):
        raise IdentityMigrationConflict('Invalid identity migration metadata')
    metadata.setdefault('version', 1)
    return metadata


def _archive(payload, reason):
    metadata = _metadata(payload)
    metadata.update(archived=True, resume_allowed=False, reason=reason)
    payload['resume_available'] = False


def _directory_snapshot(payload, mapping):
    result = _walk(payload, mapping)
    people = result.get('people')
    if not isinstance(people, dict):
        return result
    target_ids = set(mapping.values())
    verified = all(isinstance(person, dict) and isinstance(person.get('login_ids', []), list)
        and all(isinstance(identity, str) and (identity in target_ids or identity.startswith('person:'))
                for identity in person.get('login_ids', [])) for person in people.values())
    already_current = payload.get('schema') == 4 and payload.get('app_id') == TARGET_APP_ID
    if not verified and not already_current:
        _metadata(result).update(directory_verified=False, reason='unverified_directory_login_ids')
        return result
    if verified:
        if isinstance(payload.get('app_id'), str) and payload['app_id'] != TARGET_APP_ID:
            _metadata(result).setdefault('origin_app_id', payload['app_id'])
        result.update(schema=4, app_id=TARGET_APP_ID)
        # Password revisions, tombstones and all HR properties remain intact.
        for person in people.values():
            person.pop('feishu_open_ids', None)
    return result


def _hashed_checkpoint(key, payload, mapping):
    research = key.startswith('research:')
    metadata = payload.get('identity_migration', {})
    if not isinstance(metadata, dict):
        raise IdentityMigrationConflict('Invalid identity migration metadata')
    inputs = metadata.get('checkpoint_inputs', payload)
    owner = inputs.get('owner') if isinstance(inputs, dict) else None
    topic_field = 'topic_id'
    known = isinstance(owner, str) and isinstance(inputs, dict) and topic_field in inputs
    topic = inputs.get(topic_field) if isinstance(inputs, dict) else None
    operation = inputs.get('operation_id') if isinstance(inputs, dict) else None
    known = known and (isinstance(topic, str) if research else topic is None or isinstance(topic, str))
    known = known and (isinstance(operation, str) and bool(operation) if research else True)
    result = _walk(payload, mapping)
    if metadata.get('archived'):
        return key, result
    if known:
        def make(account):
            if research:
                return 'research:' + hashlib.sha256(json.dumps([account, topic, operation]).encode()).hexdigest()
            encoded = json.dumps([account, topic], ensure_ascii=True, separators=(',', ':'))
            return 'workspace:v1:' + hashlib.sha256(encoded.encode('ascii')).hexdigest()
        if key == make(owner):
            if 'checkpoint_inputs' in metadata:
                _metadata(result)['checkpoint_inputs'] = {**inputs, 'owner': mapping.get(owner, owner)}
            return make(mapping.get(owner, owner)), result
    _archive(result, 'checkpoint_inputs_unavailable' if not known else 'checkpoint_digest_mismatch')
    return key, result


def _message_job(key, payload, mapping):
    result = copy.deepcopy(payload)
    owner = payload.get('owner')
    if isinstance(owner, str):
        result['owner'] = mapping.get(owner, owner)
    recipients = payload.get('recipients', [])
    if not isinstance(recipients, list) or not isinstance(payload.get('parts', {}), dict) or payload.get('id', key) != key:
        raise IdentityMigrationConflict('Invalid message job structure')
    affected = result.get('owner') != owner or any(isinstance(row, dict) and row.get('open_id') in mapping
                                                 for row in recipients)
    if not affected:
        return key, result
    metadata = _metadata(result)
    if result.get('owner') != owner:
        metadata.setdefault('previous_owner', owner)
    labels = (['text'] if isinstance(payload.get('text'), str) and payload['text'].strip() else [])
    labels += ['file:' + row['sha256'] for row in payload.get('files', [])
               if isinstance(row, dict) and isinstance(row.get('sha256'), str)]
    original_parts = payload.get('parts', {})
    changes, known_keys = {}, set()
    new_recipients = []
    seen = set()
    for person in recipients:
        if not isinstance(person, dict) or not isinstance(person.get('open_id'), str):
            raise IdentityMigrationConflict('Invalid message recipient')
        previous = person['open_id']
        current = mapping.get(previous, previous)
        if current in seen:
            raise IdentityMigrationConflict('Message recipients collide')
        seen.add(current)
        new_recipients.append({**person, 'open_id': current})
        for label in labels:
            oldkey = _digest([previous, label])
            if oldkey in original_parts:
                known_keys.add(oldkey)
                newkey = _digest([current, label])
                if newkey != oldkey and newkey in original_parts:
                    raise IdentityMigrationConflict('Message part keys collide')
                part = copy.deepcopy(original_parts[oldkey])
                if not isinstance(part, dict):
                    raise IdentityMigrationConflict('Invalid message part')
                part.setdefault('message_uuid', str(uuid.uuid5(uuid.NAMESPACE_URL, str(payload.get('id', key)) + oldkey)))
                changes[oldkey] = (newkey, part)
    result['recipients'] = new_recipients
    result['parts'] = {changes[old][0] if old in changes else old: changes[old][1] if old in changes else copy.deepcopy(part)
                       for old, part in original_parts.items()}
    uncertain = payload.get('status') not in {'queued', 'completed'} or any(
        not isinstance(part, dict) or part.get('status') not in {'sent', 'failed'} for part in original_parts.values())
    opaque = bool(set(original_parts) - known_keys)
    if payload.get('status') != 'completed' and (uncertain or opaque):
        metadata.update(blocked=True, reason='unconfirmed_or_opaque_message_job')
    if payload.get('status') == 'queued':
        for file in result.get('files', []):
            if isinstance(file, dict):
                file.pop('file_key', None)
    return key, result


def _workpartner(key, payload, mapping):
    result = _walk(payload, mapping)
    app = payload.get('app')
    operation = key.startswith('operation:')
    fields = [app, payload.get('owner'), payload.get('operation_id')] if operation else [app, payload.get('kind'), payload.get('resource_id')]
    if not all(isinstance(field, str) and field for field in fields) or key != ('operation:' if operation else 'resource:') + _digest(fields):
        _metadata(result).update(blocked=True, opaque_key=True, reason='ledger_digest_unverifiable')
        return key, result
    newfields = [TARGET_APP_ID, result.get('owner'), fields[2]] if operation else [TARGET_APP_ID, *fields[1:]]
    if fields != newfields:
        _metadata(result).setdefault('origin_app_id', app)
        if operation and result.get('owner') != fields[1]:
            _metadata(result).setdefault('previous_owner', fields[1])
    result['app'] = TARGET_APP_ID
    if operation and payload.get('status') not in {'completed', 'rejected'}:
        result['status'] = 'uncertain'
        _metadata(result).update(blocked=True, reason='unconfirmed_workpartner_operation')
    elif operation and payload.get('status') == 'rejected' and payload.get('prior_unconfirmed'):
        result['status'] = 'uncertain'
        _metadata(result).update(blocked=True, reason='unconfirmed_workpartner_operation')
    return ('operation:' if operation else 'resource:') + _digest(newfields), result


def _public_delivery(key, payload, mapping):
    result = copy.deepcopy(payload)
    metadata = _metadata(result)
    if isinstance(payload.get('owner'), str) and payload['owner'] in mapping:
        metadata.setdefault('previous_owner', payload['owner'])
        result['owner'] = mapping[payload['owner']]
    if isinstance(payload.get('app_id'), str):
        metadata.setdefault('origin_app_id', payload['app_id'])
    receipt = payload.get('receipt') if isinstance(payload.get('receipt'), dict) else {}
    message_uuid = payload.get('message_uuid') or receipt.get('message_uuid')
    verified = False
    if isinstance(message_uuid, str) and message_uuid:
        for previous, current in mapping.items():
            if key == hashlib.sha256(json.dumps(['open_id', previous, message_uuid]).encode()).hexdigest():
                metadata.update(previous_receive_id=previous, new_receive_id=current,
                    receive_type='open_id', message_uuid=message_uuid, route_digest_verified=True)
                verified = True
                break
    if not verified:
        metadata['opaque_key'] = True
    if payload.get('status') in {'posting', 'pending', 'sending', 'uncertain'}:
        metadata.update(blocked=True, reason='unconfirmed_public_delivery')
    return key, result


def transform_document(namespace, key, payload, mapping):
    """Return independent transformed data; the caller owns collision checks/commit.

    mapping is exactly {old_open_id: new_open_id}. A many-to-one map or chain
    raises IdentityMigrationConflict. Unknown namespaces/keys are unchanged.
    Archived checkpoints keep their original opaque key and must be excluded
    from resume admission by the migration coordinator, never regenerated.
    """
    mapping = _mapping(mapping)
    value = copy.deepcopy(payload)
    if not mapping:
        return key, value
    if namespace == 'workspace_layout':
        try:
            parts = json.loads(key)
        except (ValueError, TypeError):
            return key, value
        if isinstance(parts, list) and len(parts) == 3 and isinstance(parts[0], str) and parts[0] in mapping:
            parts[0] = mapping[parts[0]]
            key = json.dumps(parts, ensure_ascii=False, separators=(',', ':'))
        return key, value
    if namespace == 'lighthouse_appearance':
        return mapping.get(key, key), value
    if namespace == 'personnel_password_login' and isinstance(value, dict):
        return key, _directory_snapshot(value, mapping)
    if namespace == 'message_delivery' and isinstance(value, dict):
        return _message_job(key, value, mapping)
    if namespace == 'feishu_public_delivery' and isinstance(value, dict):
        return _public_delivery(key, value, mapping)
    if namespace == 'feishu_workpartner' and isinstance(value, dict) and key.startswith(('operation:', 'resource:')):
        return _workpartner(key, value, mapping)
    if namespace == 'lighthouse_ai':
        if key.startswith(('research:', 'workspace:v1:')) and isinstance(value, dict):
            return _hashed_checkpoint(key, value, mapping)
        if key.startswith(('conversation:', 'model:')):
            prefix, suffix = key.split(':', 1)
            return prefix + ':' + _account_key(suffix, mapping), _walk(value, mapping)
        if key.startswith('topic-conversation:'):
            prefix, separator, owner = key.rpartition(':')
            return prefix + separator + mapping.get(owner, owner), _walk(value, mapping)
        return key, value
    if namespace not in _DOCUMENTS:
        return key, value
    original_key = key
    if namespace == 'feishu_question_cards' and not key.startswith('ticket:'):
        key = _account_key(key, mapping)
    elif namespace == 'feishu_assistant_sender':
        prefix, separator, owner = key.rpartition(':')
        key = prefix + separator + mapping.get(owner, owner)
    # RiskInspection.brief stores an account ID here, unlike stable HR/person IDs.
    result = _walk(value, mapping, identity_object=namespace == 'risk_people')
    if namespace == 'risk_jobs' and isinstance(result, dict):
        for field in ('person_id', 'subject_id'):
            if isinstance(result.get(field), str):
                result[field] = mapping.get(result[field], result[field])
    if namespace == 'feishu_question_cards' and isinstance(result, dict) and isinstance(result.get('key'), str):
        result['key'] = _account_key(result['key'], mapping)
    if namespace == 'feishu_question_cards' and isinstance(result, dict):
        affected = key != original_key or result.get('owner') != value.get('owner') or result.get('key') != value.get('key')
        source_app = value.get('app_id')
        if affected or isinstance(source_app, str) and source_app != TARGET_APP_ID:
            # Historical card IDs/tickets belong to their original app, not the new bot.
            _archive(result, 'question_card_from_previous_identity')
            if 'expires' in value:
                _metadata(result).setdefault('previous_expires', value['expires'])
            result.update(status='archived', expires=0)
    return key, result
