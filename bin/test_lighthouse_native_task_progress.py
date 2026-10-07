"""Assistant observes the original job; it never creates a second write."""
import asyncio
import copy
from unittest.mock import patch

from bin.test_lighthouse_notice_command_regression import NoticeCommandRegressionBase, ACTOR


class NativeTaskProgressTests(NoticeCommandRegressionBase):
    async def test_clearing_conversation_discards_original_notice_retry(self):
        from lan_bitable_template_portal.lighthouse_ai import AssistantError
        plan = self._prepare_start('maintenance', patch_extra={'title': 'A楼测试维保'})
        saved = self._stored(plan['id'])
        saved.update(status='failed', results=[{'ok': False, 'data': {'job_id': 'old-job'}, 'job_result': {
            'ok': True, 'data': {'phase': 'failed', 'error_retryable': True}}}])
        self.agent._save_plan(ACTOR, saved)
        self.assertTrue(self.agent.public_plan(saved, ACTOR)['can_retry'])
        cleared = self.assistant.clear(ACTOR)
        self.assertEqual(cleared['turns'], [])
        with patch.object(self.agent, '_invoke') as invoke:
            with self.assertRaisesRegex(AssistantError, '会话已清空'):
                await self.agent.retry_notice(ACTOR, plan['id'], {'version': saved['version']}, self.request)
        invoke.assert_not_called()
        # Only the conversation is cleared; the audit receipt remains available.
        self.assertEqual(self._stored(plan['id'])['results'][0]['data']['job_id'], 'old-job')

    async def test_latest_start_supersedes_failed_attempt_but_original_can_retry_first(self):
        from lan_bitable_template_portal.portal_service import PortalError
        first = self._prepare_start('maintenance', patch_extra={'title': 'A楼第一条维保'})
        await self._amend_unbound(first)
        body = self._body_after_submit(first['id'])
        job_id, _ = self.isolated.create_job(body)
        self.isolated.service.mark_job(job_id, phase='failed', error='connection reset')
        reused, should_start = self.isolated.create_job(body)
        self.assertEqual(reused, job_id)
        self.assertTrue(should_start)
        self.assertEqual(len(self.isolated._jobs), 1)
        self.isolated.service.mark_job(job_id, phase='failed', error='request timeout')
        latest = {**body, 'operation_id': 'new-operation',
                  'patch': {**body['patch'], 'content': 'latest fields', 'end_time': '2026-09-30 12:00'}}
        replacement, should_start = self.isolated.create_job(latest)
        self.assertTrue(should_start)
        self.assertNotEqual(replacement, job_id)
        old = self.isolated.service.get_job(job_id)
        new = self.isolated.service.get_job(replacement)
        self.assertEqual(old['superseded_by_job_id'], replacement)
        self.assertFalse(old['error_retryable'])
        self.assertEqual(new['request']['content'], 'latest fields')
        self.assertEqual(new['request']['manual_id'], old['retry_request']['manual_id'])
        self.assertEqual(new['replacement_create_operation_id'], 'notice_action:' + job_id)
        with self.assertRaisesRegex(PortalError, '不可重试'):
            self.isolated.create_job(body)
        with self.assertRaisesRegex(PortalError, '不可重试'):
            self.isolated.service.retry_action_job(job_id)
        self.isolated.service.mark_job(replacement, phase='failed', error='request timeout')
        third, _ = self.isolated.create_job({**latest, 'operation_id': 'third-operation'})
        self.assertEqual(self.isolated.service.get_job(third)['replacement_create_operation_id'], 'notice_action:' + job_id)
        # Compaction and another failure cannot resurrect the retired task.
        self.isolated.service.mark_job(job_id, phase='failed', error='late timeout', error_retryable=True)
        self.assertEqual(self.isolated.service.get_job(job_id)['superseded_by_job_id'], replacement)
        with self.assertRaisesRegex(PortalError, '不可重试'):
            self.isolated.create_job(body)

    async def test_replacement_local_save_failure_keeps_original_retryable(self):
        plan = self._prepare_start('maintenance', patch_extra={'title': 'A楼测试维保'})
        body = await self._amend_unbound(plan)
        original, _ = self.isolated.create_job(body)
        self.isolated.service.mark_job(original, phase='failed', error='request timeout')
        baseline = self.isolated.service.get_job(original)
        with patch.object(self.isolated.service._state_store, 'put_documents', side_effect=OSError('local disk failure')):
            with self.assertRaises(OSError):
                self.isolated.create_job({**body, 'operation_id': 'latest'})
        self.assertEqual(self.isolated.service.get_job(original), baseline)
        self.assertEqual(len(self.isolated._jobs), 1)

    async def test_replacement_does_not_merge_different_building_or_start_time(self):
        plan = self._prepare_start('maintenance', patch_extra={'title': 'A楼测试维保'})
        body = await self._amend_unbound(plan)
        original, _ = self.isolated.create_job(body)
        self.isolated.service.mark_job(original, phase='failed', error='request timeout')
        changed = {**body, 'operation_id': 'other-date', 'patch': {**body['patch'], 'start_time': '2026-10-01 09:00', 'end_time': '2026-10-01 11:00'}}
        other, _ = self.isolated.create_job(changed)
        self.assertNotEqual(original, other)
        self.assertFalse(self.isolated.service.get_job(original).get('superseded_by_job_id'))

    async def test_latest_start_does_not_duplicate_an_active_modified_notice(self):
        from lan_bitable_template_portal.portal_service import PortalError
        plan = self._prepare_start('maintenance', patch_extra={'title': 'A楼测试维保'})
        body = await self._amend_unbound(plan)
        self.isolated.create_job(body)
        with self.assertRaisesRegex(PortalError, '仍在上传'):
            self.isolated.create_job({**body, 'operation_id': 'changed-active',
                'patch': {**body['patch'], 'content': 'changed content'}})
        self.assertEqual(len(self.isolated._jobs), 1)

    async def test_retry_uses_original_operation_and_duplicate_retry_does_not_resend(self):
        plan = self._prepare_start('maintenance', patch_extra={'title': 'A楼测试维保'})
        await self._amend_unbound(plan)
        phase = 'failed'

        async def invoke(actor, operation, request, **kwargs):
            if operation['api_id'] == 'GET /api/jobs/{job_id}':
                return {'ok': True, 'data': {'job_id': 'original-job', 'phase': phase,
                    'error': 'unconfirmed', 'error_retryable': True}}
            self.notice_writes.append(copy.deepcopy(operation['body']))
            return {'ok': True, 'api_id': operation['api_id'], 'data': {'job_id': 'original-job'}}

        with patch.object(self.agent, '_invoke', side_effect=invoke):
            failed = await self._confirm_and_complete(plan['id'])
            self.assertTrue(self.agent.public_plan(failed, ACTOR)['can_retry'])
            phase = 'success'
            # An already-successful remote job is adopted without another POST.
            completed = await self.agent.retry_notice(ACTOR, plan['id'], {'version': failed['version']}, self.request)
            self.assertEqual(completed['status'], 'completed')
            self.assertEqual(len(self.notice_writes), 1)

        # A separate failed plan can retry the SAME accepted request.
        second = self.agent.prepare(ACTOR, self._start_decision('maintenance', patch_extra={'title': 'retry second'}), 'retry-second', [])
        await self._amend_unbound(second)
        phase = 'failed'
        with patch.object(self.agent, '_invoke', side_effect=invoke):
            failed = await self._confirm_and_complete(second['id'])
            original = copy.deepcopy(self.notice_writes[-1])
            await asyncio.gather(*(self.agent.retry_notice(ACTOR, second['id'], {'version': failed['version']}, self.request) for _ in range(2)))
            phase = 'success'
            await asyncio.wait_for(asyncio.gather(*tuple(self.agent.tasks)), 8)
        self.assertEqual(self.notice_writes[-1], original)
        self.assertEqual(len(self.notice_writes), 3)
        self.assertEqual(self._stored(second['id'])['status'], 'completed')

    async def test_retry_superseded_job_never_posts_and_preserves_original_results(self):
        plan = self._prepare_start('maintenance', patch_extra={'title': 'A楼测试维保'})
        await self._amend_unbound(plan)
        superseded = ''

        async def invoke(actor, operation, request, **kwargs):
            if operation['api_id'] == 'GET /api/jobs/{job_id}':
                return {'ok': True, 'data': {'job_id': 'original-job', 'phase': 'failed',
                    'superseded_by_job_id': superseded, 'error_retryable': True}}
            self.notice_writes.append(copy.deepcopy(operation['body']))
            return {'ok': True, 'api_id': operation['api_id'], 'data': {'job_id': 'original-job'}}

        with patch.object(self.agent, '_invoke', side_effect=invoke):
            failed = await self._confirm_and_complete(plan['id'])
            superseded = 'new-job'
            retired = await self.agent.retry_notice(ACTOR, plan['id'], {'version': failed['version']}, self.request)
        self.assertEqual(retired['status'], 'superseded')
        self.assertFalse(retired['can_retry'])
        self.assertEqual(len(self.notice_writes), 1)
        self.assertEqual(self._stored(plan['id'])['results'][0]['data']['job_id'], 'original-job')

    async def test_parallel_notices_keep_separate_jobs_and_execution_ownership(self):
        first = self._prepare_start('maintenance', patch_extra={'title': 'A楼第一条维保'})
        second = self.agent.prepare(ACTOR, self._start_decision('maintenance',
            patch_extra={'title': 'A楼第二条维保'}), 'parallel_notice_second', [])
        duplicate = self.agent.prepare(ACTOR, self._start_decision('maintenance',
            patch_extra={'title': 'A楼第一条维保'}),
            'parallel_notice_duplicate', [])
        phases = {}
        for plan in (first, second, duplicate):
            await self._amend_unbound(plan)

        async def invoke(actor, operation, request, **kwargs):
            if operation['api_id'] == 'GET /api/jobs/{job_id}':
                job_id = operation['path_params']['job_id']
                return {'ok': True, 'data': {'job_id': job_id, 'phase': phases[job_id]}}
            self.assertEqual(operation['api_id'], 'POST /api/workbench-actions')
            self.notice_writes.append(copy.deepcopy(operation['body']))
            try:
                job_id, _ = self.isolated.create_job(operation['body'])
            except Exception as exc:
                return {'ok': False, 'error': repr(exc)}
            phases.setdefault(job_id, 'uploading')
            return {'ok': True, 'api_id': operation['api_id'], 'data': {'job_id': job_id}}

        async def submit(plan):
            for stage in ('review', 'execute'):
                stored = self._stored(plan['id'])
                await self.agent.confirm(ACTOR, plan['id'],
                    {'version': stored['version'], 'stage': stage}, self.request)
            for _ in range(200):
                if self._stored(plan['id'])['status'] == 'submitted':
                    return
                await asyncio.sleep(.01)
            self.fail('native notice was not submitted: ' + str(self._stored(plan['id']).get('error')))

        with patch.object(self.agent, '_invoke', side_effect=invoke):
            try:
                await submit(first)
                await submit(second)
                await submit(duplicate)
                self.assertEqual(len(self.isolated._jobs), 2)
                first_job = self._stored(first['id'])['results'][0]['data']['job_id']
                second_job = self._stored(second['id'])['results'][0]['data']['job_id']
                self.assertNotEqual(first_job, second_job)
                self.assertEqual(self._stored(duplicate['id'])['results'][0]['data']['job_id'], first_job)
                await self.agent.confirm(ACTOR, second['id'],
                    {'version': self._stored(second['id'])['version'], 'stage': 'execute'}, self.request)
                self.assertEqual(len(self.notice_writes), 3)
                phases[first_job] = 'success'
                for _ in range(300):
                    if self._stored(first['id'])['status'] == 'completed':
                        break
                    await asyncio.sleep(.01)
                self.assertEqual(self._stored(first['id'])['status'], 'completed')
                self.assertIn((ACTOR['id'], second['id']), self.agent.executing)
                refreshed = await self.agent.refresh(ACTOR, self._stored(second['id']), self.request)
                self.assertEqual(refreshed['status'], 'submitted')
                phases[second_job] = 'success'
                await asyncio.wait_for(asyncio.gather(*tuple(self.agent.tasks)), 8)
                self.assertTrue(all(self._stored(plan['id'])['status'] == 'completed'
                                    for plan in (first, second, duplicate)))
            finally:
                for task in tuple(self.agent.tasks):
                    task.cancel()
                await asyncio.gather(*tuple(self.agent.tasks), return_exceptions=True)

    async def test_non_notice_plan_still_blocks_parallel_writes(self):
        from lan_bitable_template_portal.lighthouse_ai import AssistantError
        first = self._prepare_start('maintenance')
        second = self._prepare_start('adjust')
        await self._amend_unbound(second)
        pending = self._stored(first['id'])
        pending['operations'] = [{'api_id': 'POST /api/other-business'}]
        pending['status'] = 'submitted'
        self.agent.store.put_document('lighthouse_agent_plans', first['id'], pending)
        self.agent.executing.add((ACTOR['id'], first['id']))
        try:
            stored = self._stored(second['id'])
            with self.assertRaises(AssistantError):
                await self.agent.confirm(ACTOR, second['id'],
                    {'version': stored['version'], 'stage': 'review'}, self.request)
            self.assertEqual(self.notice_writes, [])
        finally:
            self.agent.executing.discard((ACTOR['id'], first['id']))

    async def test_intermediate_progress_is_persisted_and_duplicate_confirmation_reuses_job(self):
        plan = self._prepare_start('repair')
        await self._amend_unbound(plan)
        phases = iter(['uploading', 'remote_written', 'success'])
        reads = []

        async def invoke(actor, operation, request, **kwargs):
            if operation['api_id'] == 'GET /api/jobs/{job_id}':
                phase = next(phases)
                reads.append(copy.deepcopy(operation))
                return {'ok': True, 'data': {'job_id': 'native-job', 'phase': phase}}
            self.assertEqual(operation['api_id'], 'POST /api/workbench-actions')
            self.notice_writes.append(copy.deepcopy(operation['body']))
            return {'ok': True, 'api_id': operation['api_id'], 'data': {'job_id': 'native-job'}}

        with patch.object(self.agent, '_invoke', side_effect=invoke):
            stored = self._stored(plan['id'])
            await self.agent.confirm(ACTOR, plan['id'], {'version': stored['version'], 'stage': 'review'}, self.request)
            stored = self._stored(plan['id'])
            await self.agent.confirm(ACTOR, plan['id'], {'version': stored['version'], 'stage': 'execute'}, self.request)
            for _ in range(200):
                await asyncio.sleep(.02)
                stored = self._stored(plan['id'])
                if stored.get('results') and stored['results'][-1].get('job_result'):
                    break
            self.assertEqual(stored['status'], 'submitted')
            self.assertEqual(stored['results'][-1]['job_result']['data']['phase'], 'uploading')
            await self.agent.confirm(ACTOR, plan['id'], {'version': stored['version'], 'stage': 'execute'}, self.request)
            await asyncio.wait_for(asyncio.gather(*tuple(self.agent.tasks)), 8)
        self.assertEqual(len(self.notice_writes), 1)
        self.assertEqual({read['path_params']['job_id'] for read in reads}, {'native-job'})
        stored = self._stored(plan['id'])
        self.assertEqual(stored['status'], 'completed')
        self.assertEqual(stored['results'][-1]['job_result']['data']['phase'], 'success')

    async def test_failed_original_job_keeps_exact_failure_and_never_resends(self):
        plan = self._prepare_start('repair')
        await self._amend_unbound(plan)

        async def invoke(actor, operation, request, **kwargs):
            if operation['api_id'] == 'GET /api/jobs/{job_id}':
                return {'ok': True, 'data': {'job_id': 'native-job', 'phase': 'failed', 'error': 'Cloud result unconfirmed'}}
            self.assertEqual(operation['api_id'], 'POST /api/workbench-actions')
            self.notice_writes.append(copy.deepcopy(operation['body']))
            return {'ok': True, 'api_id': operation['api_id'], 'data': {'job_id': 'native-job'}}

        with patch.object(self.agent, '_invoke', side_effect=invoke):
            await self._confirm_and_complete(plan['id'])
        stored = self._stored(plan['id'])
        self.assertEqual(stored['status'], 'failed')
        self.assertEqual(stored['results'][-1]['error'], 'Cloud result unconfirmed')
        self.assertEqual(stored['results'][-1]['job_result']['data']['phase'], 'failed')
        self.assertEqual(len(self.notice_writes), 1)
