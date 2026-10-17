"""Watermarked handover evidence and private image delivery; no external writes."""
import asyncio
import datetime as dt
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
import unittest

from PIL import Image

from .test_pre_shift_extensions import Base, DAY, at, checks, cloudmod
from lan_bitable_template_portal.pre_shift_photo import render_handover, watermark_details


class WatermarkTests(unittest.TestCase):
    def test_building_time_and_weather_are_sourced_not_copied_from_example(self):
        current = at(DAY + 'T09:00:00')
        for scope in checks.SCOPES:
            stamp = watermark_details(scope, current)
            label = '110站' if scope == '110' else scope + '楼'
            self.assertEqual(stamp['title'], f'EA118南通机房{label}交接班')
            self.assertEqual(stamp['area'], label)
            self.assertEqual(stamp['time_label'], '上传时间')
            self.assertEqual(stamp['time'], '2026.10.14 09:00')
            self.assertEqual(stamp['weather'], '未获取')
            self.assertIn(label, stamp['location'])
        weather = {'available': True, 'city': '南通', 'date': DAY, 'fetched_at': current.timestamp(),
                   'description': '多云', 'minimum': 19, 'maximum': 25}
        stamp = watermark_details('B', current, weather, {36867: '2026:10:14 08:56:00'})
        self.assertEqual(stamp['time_label'], '拍摄时间')
        self.assertEqual(stamp['time'], '2026.10.14 08:56')
        self.assertEqual(stamp['weather'], '今日预报 多云 19～25℃')
        for bad in ({'city': '北京'}, {'date': '2026-10-13'}, {'fetched_at': current.timestamp()-3600}):
            self.assertEqual(watermark_details('B', current, {**weather, **bad})['weather'], '未获取')
        self.assertEqual(watermark_details('B', current, exif={36867: '2099:01:01 01:00:00'})['time_label'], '上传时间')

    def test_pixels_orientation_format_and_readable_small_images(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            source, target = Path(folder)/'raw.jpg', Path(folder)/'out.jpg'
            image = Image.new('RGB', (1600, 1200), '#708090')
            exif = Image.Exif()
            exif[274], exif[36867] = 6, '2026:10:14 08:56:00'
            image.save(source, exif=exif)
            details = render_handover(source, target, 'C', at(DAY+'T09:00:00'))
            with Image.open(target) as out:
                self.assertEqual(out.format, 'JPEG')
                self.assertEqual(out.size, (1200, 1600))
                self.assertFalse(out.getexif(), 'strip original location/EXIF before sharing')
                self.assertGreater(out.getpixel((50, 1530))[0], 140, 'white translucent stamp at bottom-left')
            self.assertEqual(details['area'], 'C楼')
            for size in ((2, 2), (1, 20000), (20000, 1)):
                Image.new('RGB', size).save(source)
                render_handover(source, target, 'A', at(DAY+'T09:00:00'))
                with Image.open(target) as out:
                    self.assertGreaterEqual(out.width, 700)
                    self.assertLessEqual(max(out.size), 2560)
            Image.new('RGB', (5001, 5000)).save(source)
            with self.assertRaisesRegex(ValueError, '像素'):
                render_handover(source, target, 'A', at(DAY+'T09:00:00'))


class DeliveryTests(Base):
    def upload(self):
        self.save('A', 0, [{'id': 'x', 'text': '检查'}])
        self.prepare(at(DAY+'T09:00:00'))
        return self.upload_again()

    def upload_again(self):
        session = self.service.stage_upload(self.worker, DAY+':A:白', '', 'handover', '交接班.png')
        Image.new('RGB', (2, 2)).save(session['path'], format='PNG')
        return self.service.commit_upload(self.worker, session, None)

    def row(self, file_id):
        with self.service.connection() as conn:
            return dict(conn.execute('SELECT * FROM photo_deliveries WHERE file_id=?', (file_id,)).fetchone())

    def test_same_photo_is_saved_previewed_synced_and_delivered_once_to_uploader(self):
        photo = self.upload()
        self.assertEqual(self.upload_again()['id'], photo['id'])
        self.assertEqual(photo['delivery_status'], 'pending')
        meta = self.service.get_file(self.worker, photo['id'])
        content = Path(meta['path']).read_bytes()
        self.assertEqual(Image.open(io.BytesIO(content)).format, 'JPEG')
        def send(recipient, payload, identity, checkpoint):
            self.assertEqual(recipient, self.worker['id'])
            self.assertEqual(Path(payload['path']).read_bytes(), content)
            self.assertEqual(identity, 'pre-shift-photo:'+photo['id'])
            checkpoint('img_saved', 'cli_agent')
            return 'om_receipt'
        self.service.photo_sender = Mock(side_effect=send)
        self.service.deliver_photos(at(DAY+'T09:00:00'))
        self.service.deliver_photos(at(DAY+'T09:01:00'))
        self.service.photo_sender.assert_called_once()
        self.assertEqual(self.row(photo['id'])['message_id'], 'om_receipt')
        state = self.service.state(self.worker, 'A', at(DAY+'T09:01:00'))
        self.assertEqual(state['tasks'][0]['handover_photo']['delivery_status'], 'sent')
        peer = self.service.state(self.peer, 'A', at(DAY+'T09:01:00'))
        self.assertNotIn('delivery_status', peer['tasks'][0]['handover_photo'])
        self.service.submit(self.worker, DAY+':A:白', 1, [{'id':'x','status':'normal','note':''}], at(DAY+'T09:02:00'))
        self.service.sync(at(DAY+'T09:02:00'))
        tasks = self.cloud.upsert.call_args.args[1]
        self.assertEqual(Path(tasks[0]['_files'][0]['path']).read_bytes(), content)

    def test_lost_receipt_restart_reuses_image_key_and_identity_then_stops(self):
        photo = self.upload()
        identities = []
        def uncertain(recipient, payload, identity, checkpoint):
            identities.append(identity)
            checkpoint('img_saved', 'cli_agent')
            raise TimeoutError('lost response')
        self.service.photo_sender = Mock(side_effect=uncertain)
        current = at(DAY+'T09:00:00')
        self.service.deliver_photos(current)
        row = self.row(photo['id'])
        self.assertEqual(row['sent'], 0)
        self.assertTrue(Path(self.service.get_file(self.worker, photo['id'])['path']).is_file())
        def finish(recipient, payload, identity, checkpoint):
            identities.append(identity)
            self.assertEqual(payload['image_key'], 'img_saved')
            self.assertEqual(payload['sender_app_id'], 'cli_agent')
            return 'om_original'
        restarted = checks.CheckService(self.temp, photo_sender=Mock(side_effect=finish))
        restarted.deliver_photos(current+dt.timedelta(seconds=299))
        restarted.photo_sender.assert_not_called()
        restarted.deliver_photos(current+dt.timedelta(seconds=301))
        restarted.deliver_photos(current+dt.timedelta(seconds=600))
        self.assertEqual(identities, [identities[0]]*2)
        self.assertEqual(self.row(photo['id'])['sent'], 1)
        restarted.photo_sender.assert_called_once()

    def test_bad_receipt_is_not_success_and_blind_retry_stops_before_uuid_expiry(self):
        photo = self.upload()
        def send(recipient, payload, identity, checkpoint):
            checkpoint('img_saved', 'cli_agent')
            return ''
        self.service.photo_sender = Mock(side_effect=send)
        current = at(DAY+'T09:00:00')
        self.service.deliver_photos(current)
        self.assertEqual(self.row(photo['id'])['sent'], 0)
        self.service.deliver_photos(current+dt.timedelta(minutes=50))
        self.service.photo_sender.assert_called_once()
        self.assertIn('未重复发送', self.row(photo['id'])['error'])

    def test_delete_cancels_unsent_photo_and_never_backfills_old_files(self):
        photo = self.upload()
        with self.service.connection(True) as conn:
            conn.execute('UPDATE photo_deliveries SET lease=?', (at(DAY+'T09:01:00').timestamp(),))
        with self.assertRaises(checks.CheckError) as caught:
            self.service.delete_file(self.peer, photo['id'])
        self.assertEqual(caught.exception.status, 403)
        with self.assertRaises(checks.CheckError) as caught:
            self.service.delete_file(self.worker, photo['id'])
        self.assertEqual(caught.exception.status, 409)
        with self.service.connection(True) as conn:
            conn.execute('UPDATE photo_deliveries SET lease=0')
        self.service.delete_file(self.worker, photo['id'])
        self.service.photo_sender = Mock()
        self.service.deliver_photos(at(DAY+'T09:00:00'))
        self.service.photo_sender.assert_not_called()
        old = self.upload_again()
        with self.service.connection(True) as conn:
            conn.execute('DELETE FROM photo_deliveries')
        restarted = checks.CheckService(self.temp, photo_sender=Mock())
        restarted.deliver_photos(at(DAY+'T09:00:00'))
        restarted.photo_sender.assert_not_called()
        self.assertTrue(Path(restarted.get_file(self.worker, old['id'])['path']).is_file())

    def test_render_failure_keeps_db_clean_and_removes_stage(self):
        self.save('A', 0, [{'id': 'x', 'text': '检查'}])
        self.prepare(at(DAY+'T09:00:00'))
        with patch('lan_bitable_template_portal.pre_shift_photo.render_handover', side_effect=ValueError('字体缺失')):
            with self.assertRaisesRegex(checks.CheckError, '水印生成失败'):
                self.upload_again()
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM files').fetchone()[0], 0)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM photo_deliveries').fetchone()[0], 0)
        self.assertEqual(list(self.service.staging_dir.iterdir()), [])

    def test_transaction_failure_rolls_back_file_and_delivery_together(self):
        import sqlite3
        self.save('A', 0, [{'id': 'x', 'text': '检查'}])
        self.prepare(at(DAY+'T09:00:00'))
        with patch.object(self.service,'_bump_shift_cloud',side_effect=sqlite3.OperationalError('disk write')):
            with self.assertRaises(sqlite3.OperationalError):
                self.upload_again()
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM files').fetchone()[0],0)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM photo_deliveries').fetchone()[0],0)
        self.assertEqual(list(self.service.files_dir.glob('*.jpg')),[])
        self.assertEqual(list(self.service.staging_dir.iterdir()),[])

    def test_concurrent_workers_only_claim_one_delivery(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor
        photo = self.upload()
        entered, finish = threading.Event(), threading.Event()
        def send(*args):
            entered.set()
            self.assertTrue(finish.wait(5))
            args[3]('img_saved','cli_agent')
            return 'om_one'
        self.service.photo_sender = Mock(side_effect=send)
        other = checks.CheckService(self.temp,photo_sender=self.service.photo_sender)
        with ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.service.deliver_photos,at(DAY+'T09:00:00'))
            try:
                self.assertTrue(entered.wait(5))
                pool.submit(other.deliver_photos,at(DAY+'T09:00:00')).result(timeout=5)
            finally:
                finish.set()
            first.result(timeout=5)
        self.service.photo_sender.assert_called_once()
        self.assertEqual(self.row(photo['id'])['message_id'],'om_one')


class TransportTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.guard = patch('lan_bitable_template_portal.portal_service.external_real_write_guard',
                           return_value={'real_write_allowed': True})
        self.guard.start()
        self.addCleanup(self.guard.stop)
        self.events = []
        self.client = SimpleNamespace(post=AsyncMock(side_effect=self.post))
        self.bot = SimpleNamespace(config={'app_id':'cli_agent'},
            messenger=SimpleNamespace(client=AsyncMock(return_value=self.client), headers=AsyncMock(return_value={})),
            handover_cards=SimpleNamespace(recipient_id=Mock(return_value=('open_id','ou_agent_person'))))
        self.runtime = SimpleNamespace(feishu_assistant_channel=self.bot,
            auth_manager=SimpleNamespace(directory_person=Mock(return_value={'id':'person_stable','selectable':True}),
                account_profile=Mock(return_value={'modules':['maintenance']})))
        self.sender = cloudmod.CheckSender(self.runtime)

    async def post(self, url, **kwargs):
        if url.endswith('/images'):
            self.assertEqual(kwargs['data'], {'image_type':'message'})
            self.assertEqual(kwargs['files']['image'][2], 'image/jpeg')
            self.events.append('image')
            return SimpleNamespace(status_code=200, json=lambda:{'code':0,'data':{'image_key':'img_key'}})
        self.events.append('message')
        self.assertEqual(kwargs['json']['msg_type'], 'image')
        self.assertEqual(json.loads(kwargs['json']['content']), {'image_key':'img_key'})
        self.assertEqual(kwargs['json']['receive_id'], 'ou_agent_person')
        return SimpleNamespace(status_code=200, json=lambda:{'code':0,'data':{'message_id':'om_received'}})

    async def test_upload_checkpoint_before_send_and_retry_only_sends_original_image_key(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'watermarked.jpg'
            Image.new('RGB', (20,20)).save(path)
            payload = {'path':str(path),'name':'A楼交接班.jpg'}
            def checkpoint(key, app):
                self.assertEqual((key,app), ('img_key','cli_agent'))
                self.events.append('checkpoint')
            result = await self.sender.send('ou_logged_in', None, 'stable-photo', photo=payload, checkpoint=checkpoint)
            self.assertEqual(result, 'om_received')
            self.assertEqual(self.events, ['image','checkpoint','message'])
            first_uuid = self.client.post.call_args.kwargs['json']['uuid']
            self.events.clear()
            await self.sender.send('ou_logged_in', None, 'stable-photo',
                photo={**payload,'image_key':'img_key','sender_app_id':'cli_agent'}, checkpoint=checkpoint)
            self.assertEqual(self.events, ['checkpoint','message'])
            self.assertEqual(self.client.post.call_args.kwargs['json']['uuid'], first_uuid)
            self.runtime.auth_manager.directory_person.assert_called_with('ou_logged_in')
            with self.assertRaisesRegex(checks.CheckError, '原发送应用已变化'):
                await self.sender.send('ou_logged_in', None, 'stable-photo',
                    photo={**payload,'image_key':'img_key','sender_app_id':'cli_other'}, checkpoint=checkpoint)

    async def test_image_permission_failure_or_revoked_person_never_sends_message(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder)/'watermarked.jpg'
            Image.new('RGB', (20,20)).save(path)
            self.client.post.side_effect = None
            self.client.post.return_value = SimpleNamespace(status_code=403, json=lambda:{'code':99991672})
            checkpoint = Mock()
            with self.assertRaisesRegex(checks.CheckError, '图片上传未确认'):
                await self.sender.send('ou_logged_in', None, 'photo', photo={'path':str(path),'name':'x.jpg'}, checkpoint=checkpoint)
            checkpoint.assert_not_called()
            self.assertEqual(self.client.post.call_count, 1)
            self.runtime.auth_manager.directory_person.return_value = {'id':'person_stable','selectable':False}
            with self.assertRaisesRegex(checks.CheckError, '身份已变化'):
                await self.sender.send('ou_logged_in', None, 'photo', photo={'path':str(path),'name':'x.jpg'}, checkpoint=checkpoint)
            self.assertEqual(self.client.post.call_count, 1)

    async def test_fixed_duty_account_without_hr_record_can_receive_its_own_photo(self):
        from lan_bitable_template_portal.portal_auth import BUILDING_OPEN_ID_MAP
        self.runtime.auth_manager.directory_person.return_value = {}
        recipient = BUILDING_OPEN_ID_MAP['H']
        result = await self.sender.send(recipient, None, 'fixed-duty-photo',
            photo={'image_key':'img_key','sender_app_id':'cli_agent'},checkpoint=Mock())
        self.assertEqual(result,'om_received')
        self.bot.handover_cards.recipient_id.assert_called_once_with({'id':recipient,'open_id':recipient})
        self.runtime.auth_manager.account_profile.return_value = {'modules':[]}
        with self.assertRaisesRegex(checks.CheckError,'权限已变化'):
            await self.sender.send(recipient, None, 'fixed-duty-photo',
                photo={'image_key':'img_key','sender_app_id':'cli_agent'},checkpoint=Mock())
        self.assertEqual(self.client.post.call_count,1)


class PhotoRouteTests(Base):
    def test_shared_weather_download_and_permission_refresh_after_weather_read(self):
        from fastapi import FastAPI
        import httpx
        from lan_bitable_template_portal import pre_shift_routes
        self.save('A', 0, [{'id':'x','text':'检查'}])
        current = at(DAY+'T09:00:00')
        self.prepare(current)
        user = {**self.worker, 'open_id':self.worker['id'], 'modules':['maintenance']}
        controller = SimpleNamespace(_current_session=lambda _: {'user':user},
            _request_base_url=lambda _: 'http://fixture')
        runtime = SimpleNamespace(service=Mock(),auth_manager=SimpleNamespace(public_user=lambda _:user))
        app = FastAPI()
        weather = AsyncMock(return_value={'available':True,'city':'南通','date':DAY,
            'fetched_at':current.timestamp(),'description':'多云','minimum':19,'maximum':25})
        app.state.lighthouse_icon_weather = SimpleNamespace(get=weather)
        raw = io.BytesIO()
        Image.new('RGB',(800,600),'#708090').save(raw,format='PNG')
        params = {'task_id':DAY+':A:白','kind':'handover','name':'现场.png'}
        with patch.object(pre_shift_routes, 'CheckService', return_value=self.service):
            pre_shift_routes.install_pre_shift_routes(app,controller,runtime)
            async def exercise():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://fixture') as client:
                    response = await client.post('/api/pre-shift-checks/files',params=params,content=raw.getvalue())
                    self.assertEqual(response.status_code,200,response.text)
                    photo = response.json()['data']
                    self.assertEqual(photo['watermark']['area'],'A楼')
                    self.assertIn('今日预报 多云',photo['watermark']['weather'])
                    weather.assert_awaited_once_with('南通')
                    download = await client.get(photo['url'])
                    self.assertEqual(download.headers['content-type'],'image/jpeg')
                    self.assertEqual(Image.open(io.BytesIO(download.content)).format,'JPEG')
                    self.service.delete_file(self.worker,photo['id'])
                    weather.side_effect = TimeoutError('forecast unavailable')
                    response = await client.post('/api/pre-shift-checks/files',params=params,content=raw.getvalue())
                    self.assertEqual(response.status_code,200,response.text)
                    photo = response.json()['data']
                    self.assertEqual(photo['watermark']['weather'],'未获取')
                    self.service.delete_file(self.worker,photo['id'])
                    async def revoke(_):
                        user['modules'] = []
                        return {}
                    weather.side_effect = revoke
                    response = await client.post('/api/pre-shift-checks/files',params=params,content=raw.getvalue())
                    self.assertEqual(response.status_code,403,response.text)
                    self.assertEqual(list(self.service.staging_dir.iterdir()),[])
            asyncio.run(exercise())


if __name__ == '__main__':
    unittest.main()
