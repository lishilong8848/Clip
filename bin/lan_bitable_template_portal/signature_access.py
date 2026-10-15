"""Signature ownership and management reuse trusted account levels and scopes."""
from .account_policy import account_allowed
from .signature_management import SignatureManagementError


class SignatureAccess:
    def __init__(self, manager, auth, actor):
        self.manager, self.auth, self.actor = manager, auth, actor
        self.profile = auth.account_profile(actor)
        if 'signatures' not in self.profile.get('modules', []):
            raise SignatureManagementError('当前账号没有签名管理权限。', 403)
        self.data = None
        self.owners = {}
        self.profiles = {actor: self.profile}

    def profile_for(self, identity):
        if identity not in self.profiles:
            self.profiles[identity] = self.auth.account_profile(identity)
        return self.profiles[identity]

    def bind(self, data):
        self.data = data
        self.owners = {}
        for row in data['staff']:
            identity = str(row.get('open_id') or '')
            if identity:
                effective = data['resolved'].get('staff:' + str(row['record_id']))
                if effective:
                    self.owners.setdefault(effective['person_key'], set()).add(identity)

    def ensure_directory(self):
        if self.data is None:
            self.bind(self.manager.directory())

    @property
    def global_manager(self):
        return self.profile.get('level') in {'L0', 'L1'} and self.profile.get('can_manage_accounts', False)

    @property
    def scopes(self):
        return [s for s in self.profile.get('scope_rules', []) if s in self.manager.d.BUILDING_OPEN_ID_MAP]

    def target(self, person):
        self.ensure_directory()
        effective = self.data['resolved'].get(person['person_key'], person)
        identities = set(self.owners.get(effective['person_key'], set()))
        if person.get('open_id'):
            identities.add(str(person['open_id']))
        if len(identities) != 1:
            return None
        identity = next(iter(identities))
        if identity in self.manager.d.BUILDING_OPEN_ID_MAP.values() or self.auth.directory_block_reason(identity):
            return None
        profile = self.profile_for(identity)
        return {**profile, 'open_id': identity, 'is_self': identity == self.actor,
                'own_scopes': [profile['home_scope']] if profile.get('home_scope') else [],
                'blocked_reason': self.auth.directory_block_reason(identity)}

    def allowed(self, person, action):
        if action == 'request':
            return self.request_allowed(person)
        target = self.target(person)
        if target:
            if target['is_self']:
                return True
            return account_allowed(self.profile, target, action)
        # Unlinked temporary visitors have no login account or management level.
        return bool(self.global_manager and person.get('source') == 'external'
                    and not person.get('origin_staff_record_id')
                    and not self.owners.get(person['person_key']))

    def request_allowed(self, person):
        if self.allowed(person, 'edit'):
            return True
        superior = self.target(person)
        subordinate = {**self.profile, 'own_scopes': [self.profile['home_scope']] if self.profile.get('home_scope') else []}
        return bool(superior and account_allowed(superior, subordinate, 'edit'))

    def decorate(self, person):
        row = dict(person)
        target = self.target(person)
        row.update(is_self=bool(target and target['is_self']),
                   can_preview=bool(person.get('has_signature') and self.allowed(person, 'view')),
                   can_request=self.request_allowed(person),
                   can_receive_message=bool(target and target.get('open_id')))
        row['invite_only'] = row['can_request'] and not self.allowed(person, 'view') and not self.allowed(person, 'edit')
        if row['invite_only']:
            # An invitation exposes the recipient, never their handwriting or signing history.
            row = {key: row.get(key) for key in ('source', 'record_id', 'person_key', 'name', 'employee_no',
                    'building', 'position', 'specialty', 'is_self', 'can_preview', 'can_request',
                    'can_receive_message', 'invite_only')}
            row['signature_status'] = 'restricted'
        return row

    def require(self, source, record_id, action):
        person = self.manager.person(source, record_id)
        if not self.allowed(person, action):
            raise SignatureManagementError('当前账号无权查看或请求此人员签名，请由其所属管理人员处理。', 403)
        return person

    def account(self, identity=''):
        identity = str(identity or self.actor).strip()
        if identity != self.actor:
            profile = self.profile_for(identity)
            target = {**profile, 'own_scopes': [profile['home_scope']] if profile.get('home_scope') else []}
            if not any(account_allowed(self.profile, target, action) for action in ('view', 'edit')):
                raise SignatureManagementError('当前账号无权查看此人员的签名情况。', 403)
        if identity in self.manager.d.BUILDING_OPEN_ID_MAP.values():
            return {'person': None, 'reason': '值班账号为共享账号，没有个人签名。请使用本人账号签名。'}
        if self.auth.directory_block_reason(identity):
            return {'person': None, 'reason': '该人员已停用、离职或身份待核对。'}
        self.ensure_directory()
        if any(not source['ok'] for source in self.data['sources'].values()):
            raise SignatureManagementError('签名目录读取失败，请稍后重试。', 503)
        candidates = {row['person_key']: row for row in self.data['resolved'].values()
                      if self.owners.get(row['person_key']) == {identity}}
        if len(candidates) != 1:
            return {'person': None, 'reason': '尚未唯一关联到本人的签名记录，请管理员核对人员关联。'}
        person = self.decorate(next(iter(candidates.values())))
        latest = self.manager.store.get_document('signature_management_person', person['person_key']) or {}
        request = self.manager.store.get_document('signature_management_request', latest.get('request_id', '')) if latest.get('request_id') else None
        person['request'] = self.manager._public_request(request) if request else None
        return {'person': person, 'reason': ''}

    def recipient(self, value):
        value = str(value or '').strip()
        if not value:
            return
        if value.lower().startswith('staff:'):
            self.require('staff', value.split(':', 1)[1], 'view')
        elif value.split(':', 1)[-1].upper() not in self.scopes:
            raise SignatureManagementError('不能把签名链接发送到管理范围之外的值班账号。', 403)

    def temporary(self, payload):
        scopes = set(self.manager.s._building_codes_from_value(payload.get('building')))
        if not self.global_manager:
            raise SignatureManagementError('新增或关联临时人员请联系全楼管理员。', 403)
        if not scopes or not scopes.issubset(self.scopes):
            raise SignatureManagementError('人员楼栋超出当前管理范围。', 403)

