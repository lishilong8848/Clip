"""Resolve post-check engineers from the shared current-app personnel snapshot."""
import re

from .account_policy import categorize_position
from .plan_convergence import PlanConvergenceService
from .plan_convergence_maintenance import excluded_buildings
from .notice_recipients import notification_directory
from .portal_service import BUILDING_SCOPE_CODES, PERMISSION_DIRECTORY_SPECIALTY_LABELS


def current_engineers(auth, record, scopes):
    majors = set(filter(None, re.split(r'[\s,，、/;；]+', str(record.get('major') or ''))))
    if not majors or not majors <= PERMISSION_DIRECTORY_SPECIALTY_LABELS:
        return [], '通告专业缺失或待核对，仅通知原接收人'
    scopes = set(scopes).intersection(BUILDING_SCOPE_CODES)
    scopes = {scope for scope in scopes if not excluded_buildings({'building': scope + '楼'})}
    if not scopes:
        return [], ''

    recipients, unresolved = [], 0
    with notification_directory(auth) as (app_id, people):
        for person in people:
            if (person.get('inactive') or not person.get('selectable')
                    or categorize_position(person.get('position')) != 'engineer'
                    or not scopes.intersection(person.get('scopes', []))
                    or not majors.intersection(person.get('specialties', []))):
                continue
            identities = person.get('login_ids', [])
            if len(identities) != 1 or not auth.current_feishu_open_id(identities[0], app_id):
                unresolved += 1
                continue
            profile = auth.account_profile(identities[0])
            if ('plan_convergence' not in profile.get('modules', [])
                    or not PlanConvergenceService._visible_notice_records([record], profile.get('scope_rules', []))):
                continue
            recipients.append(identities[0])
    warning = f'{unresolved} 位工程师身份待核对，未向不确定账号发送' if unresolved else ''
    return list(dict.fromkeys(recipients)), warning
