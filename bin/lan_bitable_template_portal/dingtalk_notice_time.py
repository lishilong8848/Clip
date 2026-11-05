"""Reconcile the first acknowledged DingTalk start time without re-sending."""
from dataclasses import fields
from datetime import datetime, timezone, timedelta
from upload_event_module.config import get_field_config
from upload_event_module.services.handlers import get_notice_handler, NoticePayload
from upload_event_module.services.service_registry import query_record_by_id, update_bitable_record_fields


def sync_start_time(runtime,operation,acknowledged_at):
    request=operation.get('request') or {};result=operation.get('result') or {}
    if request.get('action',operation.get('operation_type')) not in ('start','upload'):return 'unchanged'
    if result.get('dingtalk_start_time_state') in ('synced','conflict'):return result['dingtalk_start_time_state']
    target=operation.get('target_record_id') or result.get('record_id')
    if not target:return 'pending'
    notice_type=result.get('robot_notice_type') or ''
    payload=result.get('robot_payload') or {}
    field_config=get_field_config(notice_type)
    field=field_config.get('actual_start') or field_config.get('response_time' if notice_type=='事件通告' else 'start_time')
    if not field:return 'unchanged'
    source=NoticePayload(**{key:value for key,value in payload.items() if key in {item.name for item in fields(NoticePayload)}})
    expected=get_notice_handler(notice_type).build_create_fields(source).get(field)
    timestamp=int(acknowledged_at*1000)
    targets=[(target,notice_type,field,expected)]
    job_id=operation['operation_id'].removeprefix('notice_action:') if operation['operation_id'].startswith('notice_action:') else ''
    job=runtime.service.get_job(job_id) or {} if job_id else {}
    prepared=job.get('prepared') or {}
    pair=result.get('dingtalk_paired_target') or prepared.get('paired_maintenance_target_record_id')
    if pair and pair!=target:
        previous=runtime.service._repair_management_datetime_ms(result.get('dingtalk_paired_start') or prepared.get('paired_maintenance_actual_start_time') or prepared.get('response_time'))
        targets.append((pair,'维保通告',get_field_config('维保通告')['actual_start'],previous))
    with runtime._notice_robot_impersonation(operation):
        for record,kind,column,baseline in targets:
            ok,read=query_record_by_id(record,kind)
            if not ok or not isinstance(read,dict):return 'pending'
            current=(read.get('fields') or {}).get(column)
            if current==timestamp:continue
            # Do not overwrite a later manual history correction.
            if baseline is None or current!=baseline:return 'conflict'
            ok,_=update_bitable_record_fields(record,kind,{column:timestamp})
            if not ok:return 'pending'
            ok,read=query_record_by_id(record,kind)
            if not ok or not isinstance(read,dict) or (read.get('fields') or {}).get(column)!=timestamp:return 'pending'
    if job_id:
        runtime.service.mark_job(job_id,dingtalk_actual_start_time=datetime.fromtimestamp(acknowledged_at,timezone(timedelta(hours=8))).isoformat(),_persist=True)
    runtime.clear_payload_cache()
    return 'synced'
