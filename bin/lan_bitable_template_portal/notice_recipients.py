"""Current-app personnel lookup for background notice notifications."""
from contextlib import contextmanager
import time

from .account_policy import categorize_position
from .portal_service import BUILDING_SCOPE_CODES, WORK_TYPE_BY_NOTICE_TYPE


@contextmanager
def notification_directory(auth):
    app_id = auth.current_feishu_app_id()
    provider = getattr(auth, '_account_directory_provider', None)

    def valid(snapshot):
        return (isinstance(snapshot, dict) and snapshot.get('schema') == 4
                and snapshot.get('app_id') == app_id and bool(app_id)
                and isinstance(snapshot.get('people'), dict)
                and 0 <= time.time() - float(snapshot.get('loaded_at') or 0) < 300
                and not getattr(auth, '_account_directory_refresh_failed', False))

    snapshot = provider() if provider else {}
    if not valid(snapshot):
        loader = getattr(auth, '_account_directory_loader', None)
        if loader:
            loader()
            snapshot = provider() if provider else {}
    if not valid(snapshot):
        raise RuntimeError('当前应用人员目录暂不可核验')
    yield app_id, snapshot['people'].values()
    if provider() is not snapshot or auth.current_feishu_app_id() != app_id or not valid(snapshot):
        raise RuntimeError('人员目录已变化，稍后重新核对接收人')


def current_supervisors(auth, job):
    scopes = set(job.get('scopes', [])).intersection(BUILDING_SCOPE_CODES)
    module = WORK_TYPE_BY_NOTICE_TYPE.get(job.get('notice_type'))
    if not scopes or not module:
        return [], '通告楼栋或类型待核对，未扩大主管通知范围'
    recipients, unresolved = [], 0
    with notification_directory(auth) as (app_id, people):
        for person in people:
            homes = scopes.intersection(person.get('scopes', []))
            if (not homes or person.get('inactive') or not person.get('selectable')
                    or categorize_position(person.get('position')) != 'supervisor'):
                continue
            identities = person.get('login_ids', [])
            if len(identities) != 1 or not auth.current_feishu_open_id(identities[0], app_id):
                unresolved += 1
                continue
            profile = auth.account_profile(identities[0])
            allowed = set(profile.get('scope_rules', []))
            if module in profile.get('modules', []) and ('ALL' in allowed or homes & allowed):
                recipients.append(identities[0])
    warning = f'{unresolved} 位主管身份待核对，未向不确定账号发送' if unresolved else ''
    return list(dict.fromkeys(recipients)), warning
