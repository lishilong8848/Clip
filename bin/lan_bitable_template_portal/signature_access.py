"""Signature ownership and management reuse trusted account levels and scopes."""
from .account_policy import account_allowed
from .signature_management import SignatureManagementError, public_person


class SignatureAccess:
    def __init__(self, manager, auth, actor):
        self.manager, self.auth, self.actor = manager, auth, actor
        self.profile = auth.account_profile(actor)
        if 'signatures' not in self.profile.get('modules', []) or auth.directory_block_reason(actor):
            raise SignatureManagementError('当前账号没有签名管理权限。', 403)
        self.data = None
        self.owners = {}
        self.profiles = {actor: self.profile}
        self._targets = None

    def profile_for(self, identity):
        if identity not in self.profiles:
            self.profiles[identity] = self.auth.account_profile(identity)
        return self.profiles[identity]

    def bind(self, data, *, cache_targets=False):
        self.data = data
        self._targets = {} if cache_targets else None
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

    @property
    def can_create_temporary(self):
        return bool(self.scopes)

    def shared_temporary(self, person):
        self.ensure_directory()
        effective = self.data['resolved'].get(person['person_key'], person)
        # Account-linked signatures keep account permissions, including merged aliases.
        return bool(person.get('source') == 'external' and effective.get('source') == 'external'
                    and not person.get('origin_staff_record_id')
                    and not effective.get('origin_staff_record_id')
                    and not self.owners.get(effective['person_key']))

    def target(self, person):
        self.ensure_directory()
        if self._targets is None:
            return self._target(person)
        key = person['person_key']
        if key not in self._targets:
            self._targets[key] = self._target(person)
        return self._targets[key]

    def _target(self, person):
        effective = self.data['resolved'].get(person['person_key'], person)
        identities = set(self.owners.get(effective['person_key'], set()))
        if person.get('open_id'):
            identities.add(str(person['open_id']))
        if len(identities) != 1:
            return None
        identity = next(iter(identities))
        if identity in self.manager.d.BUILDING_OPEN_ID_MAP.values():
            return None
        blocked = self.auth.directory_block_reason(identity)
        if blocked:
            return None
        profile = self.profile_for(identity)
        return {**profile, 'open_id': identity, 'is_self': identity == self.actor,
                'own_scopes': [profile['home_scope']] if profile.get('home_scope') else [],
                'blocked_reason': blocked}

    def allowed(self, person, action):
        if action == 'request':
            return self.request_allowed(person)
        target = self.target(person)
        if target:
            if target['is_self']:
                return True
            return account_allowed(self.profile, target, action)
        return action in {'view', 'edit'} and self.shared_temporary(person)

    def request_allowed(self, person):
        if self.allowed(person, 'edit'):
            return True
        superior = self.target(person)
        subordinate = {**self.profile, 'own_scopes': [self.profile['home_scope']] if self.profile.get('home_scope') else []}
        return bool(superior and account_allowed(superior, subordinate, 'edit'))

    def presentation(self, person):
        row = dict(person)
        target = self.target(person)
        if target:
            lookup = getattr(self.auth, 'directory_person', None)
            personnel = lookup(target['open_id']) if callable(lookup) else {}
            scopes = personnel.get('scopes', []) if personnel else [target.get('home_scope')]
            # A person's home is not the buildings they can administer or receive for.
            scopes = dict.fromkeys(s for s in scopes if s in self.manager.d.BUILDING_OPEN_ID_MAP)
            row['building'] = '、'.join('110站' if s == '110' else s + '楼' for s in scopes)
            row['scope_text'] = row['building']
        return row

    def decorate(self, person):
        row = self.presentation(person)
        target = self.target(person)
        row.update(is_self=bool(target and target['is_self']),
                   can_preview=bool(person.get('has_signature') and self.allowed(person, 'view')),
                   can_request=self.request_allowed(person),
                   can_receive_message=bool(target and target.get('open_id')))
        row['directory_only'] = not self.allowed(person, 'view') and not self.allowed(person, 'edit')
        row['invite_only'] = row['can_request'] and row['directory_only']
        if row['directory_only']:
            # The shared directory exposes search metadata, never protected ink/history.
            row = {key: row.get(key) for key in ('source', 'record_id', 'person_key', 'name', 'employee_no',
                    'building', 'position', 'specialty', 'team', 'shift', 'account_nature',
                    'is_self', 'can_preview', 'can_request', 'can_receive_message', 'invite_only', 'directory_only')}
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

    def recipient_people(self, person):
        shared = self.shared_temporary(person)
        return [row for row in self.manager.message_recipients(self.auth)
                if shared or self.allowed(public_person(row, 'staff'), 'view')]

    def recipient(self, value, *, person=None):
        value = str(value or '').strip()
        if not value:
            return
        if value.lower().startswith('staff:'):
            record_id = value.split(':', 1)[1]
            if person is not None and self.shared_temporary(person):
                if not any(row['record_id'] == record_id for row in self.recipient_people(person)):
                    raise SignatureManagementError('所选人员无法接收签名链接，请重新选择。', 403)
            else:
                self.require('staff', record_id, 'view')
        elif value.split(':', 1)[-1].upper() not in self.scopes:
            raise SignatureManagementError('不能把签名链接发送到管理范围之外的值班账号。', 403)

    def temporary(self, payload):
        scopes = set(self.manager.s._building_codes_from_value(payload.get('building')))
        if not self.can_create_temporary:
            raise SignatureManagementError('当前账号未配置可新增临时人员的楼栋。', 403)
        if not scopes or not scopes.issubset(self.scopes):
            raise SignatureManagementError('人员楼栋超出当前管理范围。', 403)

