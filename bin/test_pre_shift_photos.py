"""Watermarked handover evidence and private image delivery; no external writes."""
import asyncio
import base64
import datetime as dt
import io
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
import unittest

from PIL import Image

from .test_pre_shift_extensions import Base, DAY, at, checks, cloudmod
from lan_bitable_template_portal.pre_shift_photo import render_handover, watermark_details, inspect_photo, photo_options


class PhotoPreviewTests(Base):
    def setUp(self):
        super().setUp()
        self.current = at(DAY+'T09:00:00')
        self.save('A', 0, [{'id':'x','text':'检查'}]); self.prepare(self.current)
        self.ocr = patch('openclaw_service.assistant.lighthouse_files.recognize_text', return_value='')
        self.ocr.start(); self.addCleanup(self.ocr.stop)

    def preview(self, actor=None, kind='item', exif=True):
        session = self.service.stage_upload(actor or self.worker, DAY+':A:白', 'x' if kind=='item' else '', kind, '现场.jpg')
        tags = Image.Exif()
        if exif:
            tags[36867] = '2026:10:13 08:31:00'
        Image.new('RGB', (900,700), '#708090').save(session['path'], format='JPEG', exif=tags)
        info = self.service.retain_photo_preview(actor or self.worker, session, Path(session['path']).stat().st_size, self.current)
        return session, info

    def test_valid_photo_with_wrong_suffix_preserves_capture_time_and_final_format(self):
        session=self.service.stage_upload(self.worker,DAY+':A:白','x','item','手机照片.png')
        tags=Image.Exif(); tags[36867]='2026:10:13 08:31:00'
        Image.new('RGB',(900,700)).save(session['path'],format='JPEG',exif=tags)
        info=self.service.retain_photo_preview(self.worker,session,Path(session['path']).stat().st_size,self.current)
        self.assertTrue(session['name'].endswith('.jpg'))
        self.assertEqual(info['original_time'],'2026-10-13 08:31')
        result=self.service.commit_photo_preview(self.worker,info['preview_id'],{**info['options'],'enabled':False},current=self.current)
        self.assertTrue(result['name'].endswith('.jpg'))
        self.assertEqual(self.counts(),(1,1))

    def test_48mp_phone_jpeg_is_reduced_before_full_decode_and_can_be_confirmed(self):
        session=self.service.stage_upload(self.worker,DAY+':A:白','x','item','相机.jpg')
        tags=Image.Exif(); tags[36867]='2026:10:13 08:31:00'; tags[274]=6
        with Image.new('RGB',(8000,6000),'#708090') as photo:
            photo.save(session['path'],format='JPEG',exif=tags)
        original=Image.MAX_IMAGE_PIXELS
        size=Path(session['path']).stat().st_size
        info=self.service.retain_photo_preview(self.worker,session,size,self.current)
        self.assertEqual(info['original_time'],'2026-10-13 08:31')
        self.assertGreater(info['height'],info['width'])
        self.assertEqual(Image.MAX_IMAGE_PIXELS,original)
        with Image.open(session['path']) as photo:
            self.assertLess(photo.width*photo.height,25_000_000)
        result=self.service.commit_photo_preview(self.worker,info['preview_id'],self.stamped(info),current=self.current)
        again=self.service.commit_photo_preview(self.worker,info['preview_id'],self.stamped(info),current=self.current)
        self.assertEqual(result['id'],again['id'])
        self.assertEqual(self.counts(),(1,1))

    def test_corrupt_photos_do_not_save_and_size_errors_are_not_masked(self):
        session=self.service.stage_upload(self.worker,DAY+':A:白','x','item','broken.jpg')
        Path(session['path']).write_bytes(b'not a photo')
        with self.assertRaisesRegex(checks.CheckError,'无法读取文件'):
            self.service.commit_upload(self.worker,session,len(b'not a photo'))
        session=self.service.stage_upload(self.worker,DAY+':A:白','x','item','too-big.png')
        Path(session['path']).write_bytes(b'placeholder')
        with patch('lan_bitable_template_portal.pre_shift_photo.prepare_upload',side_effect=ValueError('图片像素过大，请缩小照片后上传。')):
            with self.assertRaisesRegex(checks.CheckError,'像素过大'):
                self.service.commit_upload(self.worker,session,len(b'placeholder'))
        self.assertEqual(self.counts(),(0,0))

    def test_common_album_formats_decode_preview_and_preserve_capture_metadata(self):
        from lan_bitable_template_portal.pre_shift_photo import prepare_upload
        for suffix, fmt in [('heic','HEIF'),('heif','HEIF'),('avif','AVIF'),('png','PNG'),
                            ('webp','WEBP'),('gif','GIF'),('tiff','TIFF'),('bmp','BMP'),('ico','ICO'),('jp2','JPEG2000')]:
            with self.subTest(format=fmt,suffix=suffix):
                session=self.service.stage_upload(self.worker,DAY+':A:白','x','item','相册.'+suffix)
                tags=Image.Exif();tags[36867]='2026:10:13 08:31:00'
                with Image.new('RGB',(96,80),'#708090') as photo:
                    photo.save(session['path'],format=fmt,exif=tags)
                info=self.service.retain_photo_preview(self.worker,session,Path(session['path']).stat().st_size,self.current)
                self.assertTrue(info['image'].startswith('data:image/jpeg'))
                self.assertIn(Path(session['name']).suffix,{'.jpg','.png','.webp'})
                if fmt in {'HEIF','AVIF','PNG','WEBP','TIFF'}:
                    self.assertEqual(info['original_time'],'2026-10-13 08:31')
                self.service.discard_photo_preview(self.worker,info['preview_id'],self.current)

    def test_multiframe_photos_use_first_frame_and_do_not_import_all_pages(self):
        from lan_bitable_template_portal.pre_shift_photo import prepare_upload
        for fmt in ('GIF','TIFF','WEBP'):
            path=Path(self.temp)/('animated.'+fmt.lower())
            with Image.new('RGB',(60,40),'red') as first, Image.new('RGB',(60,40),'blue') as second:
                first.save(path,format=fmt,save_all=True,append_images=[second])
            name=prepare_upload(path,path.name)
            with Image.open(path) as actual:
                self.assertEqual(getattr(actual,'n_frames',1),1)
                r,g,b=actual.convert('RGB').getpixel((20,20))
                self.assertGreater(r,b)
            self.assertIn(Path(name).suffix,{'.jpg','.png'})

    def test_photo_codec_is_part_of_packaging_and_startup_requirements(self):
        import package_portable
        from upload_event_module.services.dependency_bootstrap import DEFAULT_MODULE_TO_PACKAGE
        self.assertEqual(package_portable.RUNTIME_MODULE_TO_PACKAGE['pillow_heif'],'pillow-heif==1.8.1')
        self.assertEqual(DEFAULT_MODULE_TO_PACKAGE['pillow_heif'],'pillow-heif==1.8.1')
        self.assertEqual(DEFAULT_MODULE_TO_PACKAGE['PIL.AvifImagePlugin'],'Pillow>=11.3.0')
        self.assertEqual(package_portable.RUNTIME_MODULE_TO_PACKAGE['PIL.AvifImagePlugin'],'Pillow>=11.3.0')
        self.assertTrue({'pillow_heif','PIL.AvifImagePlugin'} <= set(package_portable.RUNTIME_PACKAGE_INSTALL_ORDER))

    def test_unavailable_heif_codec_does_not_break_standard_photo_uploads(self):
        with patch('lan_bitable_template_portal.pre_shift_photo.register_heif_opener',None):
            session,info=self.preview()
            self.assertTrue(info['preview_id'])
            self.service.discard_photo_preview(self.worker,info['preview_id'],self.current)
            session=self.service.stage_upload(self.worker,DAY+':A:白','x','item','photo.heic')
            Path(session['path']).write_bytes(b'heic')
            with self.assertRaisesRegex(checks.CheckError,'解码组件尚未就绪'):
                self.service.commit_upload(self.worker,session,4)

    def counts(self):
        with self.service.connection() as conn:
            return tuple(conn.execute('SELECT COUNT(*) FROM '+name).fetchone()[0] for name in ('files','photo_deliveries'))

    def stamped(self, info, **fields):
        return {**info['options'], 'enabled':True, **fields}

    def test_supervisor_photo_upload_confirm_and_submission_keep_actual_actor_and_ownership(self):
        supervisor={**self.worker,'id':'ou_supervisor','name':'本楼主管','level':'L3','category':'supervisor'}
        for kind in ('item','handover'):
            _,info=self.preview(supervisor,kind)
            photo=self.service.commit_photo_preview(supervisor,info['preview_id'],info['options'],current=self.current)
            with self.service.connection() as conn:
                delivery=conn.execute('SELECT recipient FROM photo_deliveries WHERE file_id=?',(photo['id'],)).fetchone()
                file=conn.execute('SELECT uploader_id FROM files WHERE id=?',(photo['id'],)).fetchone()
            self.assertEqual(delivery['recipient'],supervisor['id']);self.assertEqual(file['uploader_id'],supervisor['id'])
            with self.assertRaises(checks.CheckError) as denied:
                self.service.delete_file(self.worker,photo['id'])
            self.assertEqual(denied.exception.status,403)
        self.service.submit(supervisor,DAY+':A:白',1,[{'id':'x','status':'normal','note':''}],self.current)
        task=next(t for t in self.service.state(self.worker,'A',self.current)['tasks'] if t['id']==DAY+':A:白')
        self.assertFalse(task['can_submit']);self.assertEqual(task['actor_name'],supervisor['name'])

    def test_supervisor_authority_revoked_after_preview_cannot_commit(self):
        supervisor={**self.worker,'id':'ou_supervisor','level':'L3','category':'supervisor'}
        for kind in ('item','handover'):
            _,info=self.preview(supervisor,kind)
            with self.assertRaises(checks.CheckError) as denied:
                self.service.commit_photo_preview({**supervisor,'category':'engineer','level':'L4'},info['preview_id'],info['options'],current=self.current)
            self.assertEqual(denied.exception.status,403)
        self.assertEqual(self.counts(),(0,0))

    def test_cancel_owner_expiry_and_quota_do_not_write_business_files(self):
        session, info = self.preview()
        self.assertEqual(self.counts(), (0,0))
        with self.assertRaises(checks.CheckError) as caught:
            self.service.commit_photo_preview(self.peer,info['preview_id'],info['options'],current=self.current)
        self.assertEqual(caught.exception.status,404)
        with self.assertRaises(checks.CheckError) as caught:
            self.service.discard_photo_preview({**self.worker,'scope_rules':['B'],'home_scope':'B'},info['preview_id'],self.current)
        self.assertEqual(caught.exception.status,403)
        self.assertTrue(Path(session['path']).is_file())
        self.assertTrue(self.service.discard_photo_preview(self.worker,info['preview_id'],self.current)['cancelled'])
        self.assertFalse(Path(session['path']).exists())
        session, info = self.preview()
        with self.assertRaises(checks.CheckError) as caught:
            self.service.commit_photo_preview(self.worker,info['preview_id'],info['options'],current=self.current+dt.timedelta(hours=2))
        self.assertEqual(caught.exception.status,410)
        self.service.cleanup_photo_previews(self.current+dt.timedelta(hours=2))
        self.assertFalse(Path(session['path']).exists())
        with patch.object(checks,'MAX_PREVIEW_BYTES',1), self.assertRaisesRegex(checks.CheckError,'待确认照片'):
            self.preview()
        self.assertEqual(self.counts(),(0,0))

    def test_commit_restart_replay_and_metadata_only_preferences(self):
        session, info = self.preview()
        options = self.stamped(info, logo='vnet', scale=.5, position={'x':.8,'y':.2}, weather='多云',location='本次定位')
        result = self.service.commit_photo_preview(self.worker,info['preview_id'],options,current=self.current)
        self.assertEqual(self.counts(),(1,1))
        self.assertFalse(Path(session['path']).exists())
        restarted = checks.CheckService(self.temp)
        replay = restarted.commit_photo_preview(self.worker,info['preview_id'],options,current=self.current)
        self.assertEqual(replay['id'],result['id']); self.assertEqual(self.counts(),(1,1))
        style = restarted.photo_preferences(self.worker)
        self.assertEqual(style,{'version':1,'style':{'scale':.5,'logo':'vnet','position':{'x':.8,'y':.2}}})
        self.assertEqual(restarted.photo_preferences(self.peer),{'version':0,'style':{}})
        self.assertFalse(self.service.discard_photo_preview(self.worker,info['preview_id'],self.current)['cancelled'])
        with self.assertRaisesRegex(checks.CheckError,'其他设置'):
            restarted.commit_photo_preview(self.worker,info['preview_id'],{**options,'scale':.6},current=self.current)
        self.service.delete_file(self.worker,result['id'])
        with self.assertRaisesRegex(checks.CheckError,'已被移除'):
            restarted.commit_photo_preview(self.worker,info['preview_id'],options,current=self.current)

    def test_concurrent_preferences_keep_newer_device_and_opt_out_is_honored(self):
        _, older = self.preview(); _, newer = self.preview()
        self.service.commit_photo_preview(self.worker,newer['preview_id'],self.stamped(newer,scale=.6),current=self.current)
        result = self.service.commit_photo_preview(self.worker,older['preview_id'],self.stamped(older,scale=.3),current=self.current)
        self.assertIn('另一设备',result['preference_warning'])
        self.assertEqual(self.service.photo_preferences(self.worker)['style']['scale'],.6)
        _, other = self.preview()
        self.service.commit_photo_preview(self.worker,other['preview_id'],self.stamped(other,scale=.45),False,self.current)
        self.assertEqual(self.service.photo_preferences(self.worker)['version'],1)
        with self.service.connection(True) as conn:
            conn.execute('UPDATE photo_preferences SET style=?', ('[]',))
        self.assertIn('无法读取',self.service.photo_preferences(self.worker)['warning'])

    def test_saved_photo_survives_receipt_failure_and_retry_never_duplicates(self):
        import sqlite3
        session, info = self.preview(); options = self.stamped(info)
        with patch.object(self.service,'_complete_photo_preview',side_effect=sqlite3.OperationalError('disk write')):
            result = self.service.commit_photo_preview(self.worker,info['preview_id'],options,current=self.current)
        self.assertIn('照片已上传',result['preference_warning'])
        self.assertTrue(Path(session['path']).is_file()); self.assertEqual(self.counts(),(1,1))
        with self.assertRaisesRegex(checks.CheckError,'已提交上传'):
            self.service.discard_photo_preview(self.worker,info['preview_id'],self.current)
        replay = checks.CheckService(self.temp).commit_photo_preview(self.worker,info['preview_id'],options,current=self.current)
        self.assertEqual(result['id'],replay['id']); self.assertEqual(self.counts(),(1,1))
        self.assertEqual(self.service.photo_preferences(self.worker)['version'],1)

    def test_failed_write_preserves_original_preview_and_same_request_can_retry(self):
        session, info = self.preview(); options = self.stamped(info)
        with patch.object(self.service,'commit_upload',side_effect=OSError('disk write')):
            with self.assertRaises(checks.CheckError) as caught:
                self.service.commit_photo_preview(self.worker,info['preview_id'],options,current=self.current)
        self.assertEqual(caught.exception.status,503)
        self.assertTrue(Path(session['path']).is_file()); self.assertEqual(self.counts(),(0,0))
        self.assertEqual(self.service.photo_preferences(self.worker)['version'],0)
        self.service.commit_photo_preview(self.worker,info['preview_id'],options,current=self.current)
        self.assertEqual(self.counts(),(1,1))

    def test_cleanup_failure_after_success_does_not_report_upload_failure(self):
        session, info = self.preview(); real_unlink = Path.unlink
        def unlink(path, *args, **kwargs):
            if str(path)==session['path']:
                raise PermissionError('busy file')
            return real_unlink(path,*args,**kwargs)
        with patch.object(Path,'unlink',unlink):
            result = self.service.commit_photo_preview(self.worker,info['preview_id'],self.stamped(info),current=self.current)
        self.assertTrue(result['id']); self.assertEqual(self.counts(),(1,1))
        self.service.cleanup_photo_previews(self.current+dt.timedelta(hours=2))
        self.assertFalse(Path(session['path']).exists())

    def test_concurrent_commit_and_expired_worker_cannot_release_new_lease(self):
        import threading
        from concurrent.futures import ThreadPoolExecutor
        session, info = self.preview(); options = self.stamped(info)
        entered, finish = threading.Event(), threading.Event()
        original = self.service.commit_upload
        def pause(*args,**kwargs):
            entered.set(); self.assertTrue(finish.wait(8)); return original(*args,**kwargs)
        with patch.object(self.service,'commit_upload',side_effect=pause),ThreadPoolExecutor(max_workers=2) as pool:
            first = pool.submit(self.service.commit_photo_preview,self.worker,info['preview_id'],options,True,self.current)
            try:
                self.assertTrue(entered.wait(5))
                with self.assertRaisesRegex(checks.CheckError,'正在上传'):
                    self.service.commit_photo_preview(self.worker,info['preview_id'],options,current=self.current)
                with self.service.connection(True) as conn:
                    conn.execute('UPDATE photo_previews SET lease=?',(self.current.timestamp()+999,))
            finally:
                finish.set()
            self.assertIn('preference_warning',first.result(timeout=10))
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT lease FROM photo_previews').fetchone()[0],self.current.timestamp()+999)
        self.assertTrue(Path(session['path']).exists()); self.assertEqual(self.counts(),(1,1))

    def test_cleanup_ignores_corrupt_metadata_and_never_deletes_external_file(self):
        session, info = self.preview()
        outside = Path(self.temp)/'preserve.txt'; outside.write_text('preserve')
        with self.service.connection(True) as conn:
            conn.execute('UPDATE photo_previews SET session=?,expires=?', (json.dumps({'path':str(outside)}),0))
            conn.execute("INSERT INTO photo_previews(id,actor_id,scope,session,size,content_hash,expires) VALUES('corrupt','x','A','{',1,'x',0)")
        self.service.cleanup_photo_previews(self.current)
        self.assertEqual(outside.read_text(),'preserve')
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM photo_previews').fetchone()[0],0)
        # An expired but still-leased preview must not be removed by maintenance.
        _, info = self.preview()
        with self.service.connection(True) as conn:
            conn.execute('UPDATE photo_previews SET expires=0,lease=?',(self.current.timestamp()+100,))
        self.service.cleanup_photo_previews(self.current)
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM photo_previews').fetchone()[0],1)

    def test_manual_capture_time_has_server_audit_and_does_not_leak_actor_into_jpeg(self):
        _, info = self.preview()
        self.assertEqual(info['time_origin'],'exif')
        options = self.stamped(info,time='2026-10-13 08:45')
        result = self.service.commit_photo_preview(self.worker,info['preview_id'],options,current=self.current)
        audit = result['watermark']['audit']
        self.assertEqual(audit['original_time'],'2026-10-13 08:31')
        self.assertEqual(audit['display_time'],'2026-10-13 08:45'); self.assertTrue(audit['modified'])
        self.assertEqual(audit['actor_id'],self.worker['id']); self.assertIn('手动修改',result['watermark']['time_label'])
        path = self.service.get_file(self.worker,result['id'])['path']
        with Image.open(path) as image:
            saved = json.loads(image.info['comment'].split(b':',1)[1])
            self.assertNotIn('actor_id',saved['audit']); self.assertNotIn('actor_name',saved['audit'])
        _, blank = self.preview(exif=False)
        result = self.service.commit_photo_preview(self.worker,blank['preview_id'],self.stamped(blank,time='2026-10-12 08:00'),current=self.current)
        self.assertEqual(result['watermark']['audit']['origin'],'unknown')
        self.assertIn('手动填写',result['watermark']['time_label'])

    def test_existing_platform_stamp_is_not_applied_twice_or_remembered_as_new_style(self):
        raw=Path(self.temp)/'raw.jpg';Image.new('RGB',(900,700)).save(raw)
        session=self.service.stage_upload(self.worker,DAY+':A:白','x','item','已有水印.jpg')
        render_handover(raw,session['path'],'A',self.current,capture={'captured_at':self.current.timestamp()*1000})
        content=Path(session['path']).read_bytes()
        info=self.service.retain_photo_preview(self.worker,session,len(content),self.current)
        self.assertTrue(info['existing_locked'])
        result=self.service.commit_photo_preview(self.worker,info['preview_id'],self.stamped(info,existing=False,scale=.7),current=self.current)
        self.assertFalse(result['watermark']['applied']);self.assertTrue(result['watermark']['existing'])
        self.assertEqual(Path(self.service.get_file(self.worker,result['id'])['path']).read_bytes(),content)
        self.assertEqual(self.service.photo_preferences(self.worker)['version'],0)


class WatermarkTests(unittest.TestCase):
    def test_positions_are_validated_and_corner_pixels_stay_inside_photo(self):
        import tempfile
        options = {'enabled':True,'existing':False,'title':'检查','area':'A楼','time':'2026-10-13 08:31','scale':.4}
        self.assertEqual(photo_options(options),photo_options({**options,'position':{'x':0,'y':1}}))
        for value in ({'x':2,'y':0},{'x':float('nan'),'y':0},{'x':True,'y':0},[],{'x':0},None):
            with self.subTest(value=value),self.assertRaisesRegex(ValueError,'位置'):
                photo_options({**options,'position':value})
        with tempfile.TemporaryDirectory() as folder:
            raw, out = Path(folder)/'raw.jpg',Path(folder)/'out.jpg'
            Image.new('RGB',(1600,1200),'#708090').save(raw)
            for x,y in ((0,0),(0,1),(1,0),(1,1),(.5,.5)):
                render_handover(raw,out,'A',at(DAY+'T09:00:00'),capture={'watermark':{**options,'position':{'x':x,'y':y}}})
                with Image.open(out) as image:
                    left,top = 24+round((1600-48-640)*x),24+round((1200-48-177)*y)
                    r,g,b = image.getpixel((left+300,min(top+15,1170)))
                    self.assertGreater(b,r+50,'header follows the selected position')
            Image.new('RGB',(320,1600),'#708090').save(raw)
            with patch('openclaw_service.assistant.lighthouse_files.recognize_text',return_value=''):
                info=inspect_photo(raw,'A',at(DAY+'T09:00:00'))
            render_handover(raw,out,'A',at(DAY+'T09:00:00'),capture={'watermark':options})
            with Image.open(out) as image:
                self.assertAlmostEqual(image.width/image.height,info['width']/info['height'],places=3,msg='editable tall photos must not gain side padding')

    def test_delivery_copy_is_bounded_and_preserves_large_original(self):
        import tempfile, hashlib
        from lan_bitable_template_portal.pre_shift_photo import prepare_delivery_image,DELIVERY_IMAGE_BYTES,_photo_time
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'noise.png'
            Image.effect_noise((2300,2300),100).convert('RGB').save(path,compress_level=0)
            before=hashlib.sha256(path.read_bytes()).hexdigest()
            self.assertGreater(path.stat().st_size,DELIVERY_IMAGE_BYTES)
            result=prepare_delivery_image(path,'原始照片.png')
            self.assertTrue(0<len(result['content'])<=DELIVERY_IMAGE_BYTES)
            self.assertEqual(result['mime'],'image/jpeg')
            self.assertEqual(Image.open(io.BytesIO(result['content'])).format,'JPEG')
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),before)
            Image.new('RGB',(20,20)).save(path)
            self.assertIsNone(prepare_delivery_image(path,'small.png')['content'])
        self.assertEqual(_photo_time({306:'2026:10:13 08:00:00'}),'','file modification is not capture time')
        self.assertEqual(_photo_time({36867:'2099:12:31 00:00:00'},at(DAY+'T09:00:00')),'','future EXIF is not auto-filled as capture time')
    def test_logo_validation_and_pixels_in_final_watermark(self):
        import tempfile
        options={'enabled':True,'existing':False,'scale':.4,'title':'测试Logo水印','area':'A楼',
                 'time':'2026-10-13 08:31','weather':'','location':''}
        self.assertEqual(photo_options({**options,'logo':''}),photo_options(options),'empty logo must preserve pre-upgrade dedup identity')
        self.assertNotIn('logo',photo_options(options))
        content=io.BytesIO();Image.new('RGBA',(128,64),'#ed2020').save(content,format='PNG')
        logo='data:image/png;base64,'+base64.b64encode(content.getvalue()).decode()
        for invalid in ('https://other.example/logo.png','data:image/svg+xml;base64,PHN2Zy8+',
                        'data:image/png;base64,%%%', 'data:image/jpeg;base64,'+logo.split(',')[1],
                        'data:image/png;base64,'+'A'*2804, [], True):
            with self.subTest(logo=invalid),self.assertRaisesRegex(ValueError,'Logo'):
                photo_options({**options,'logo':invalid})
        transparent=io.BytesIO();Image.new('RGBA',(20,20)).save(transparent,format='PNG')
        with self.assertRaisesRegex(ValueError,'Logo'):
            photo_options({**options,'logo':'data:image/png;base64,'+base64.b64encode(transparent.getvalue()).decode()})
        with tempfile.TemporaryDirectory() as folder:
            source,target=Path(folder)/'raw.jpg',Path(folder)/'stamped.jpg'
            Image.new('RGB',(1600,1200),'#708090').save(source)
            for choice in ('', 'vnet', logo):
                details=render_handover(source,target,'A',at(DAY+'T09:00:00'),capture={'watermark':{**options,'logo':choice}})
                self.assertEqual(details.get('logo',''),'vnet' if choice=='vnet' else 'custom' if choice else '')
                with Image.open(target) as image:
                    self.assertEqual(image.size,(1600,1200))
                    self.assertNotIn(b'data:image',image.info['comment'],'do not embed uploaded logo data in photo metadata')
                    if choice==logo:
                        self.assertGreater(sum(1 for r,g,b in image.crop((20,980,160,1060)).getdata() if r>180 and g<80 and b<80),200)

    def test_inspection_reads_exif_and_existing_stamp_without_business_write(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            source,target=Path(folder)/'raw.jpg',Path(folder)/'stamped.jpg'
            exif=Image.Exif();exif[36867]='2026:10:12 17:32:00'
            Image.new('RGB',(1200,800),'#708090').save(source,exif=exif)
            with patch('openclaw_service.assistant.lighthouse_files.recognize_text',return_value='施工区域：H楼\n拍摄时间：2026.10.11 18:00'):
                info=inspect_photo(source,'A',at(DAY+'T09:00:00'))
            self.assertEqual(info['options']['time'],'2026-10-12 17:32')
            self.assertTrue(info['existing_watermark'])
            self.assertFalse(info['options']['enabled'])
            render_handover(source,target,'A',at(DAY+'T09:00:00'))
            with patch('openclaw_service.assistant.lighthouse_files.recognize_text') as ocr:
                info=inspect_photo(target,'A',at(DAY+'T09:00:00'))
                ocr.assert_not_called()
            self.assertTrue(info['existing_watermark'])
            self.assertEqual(info['options']['time'],'2026-10-12 17:32')

    def test_ocr_time_and_missing_metadata_require_real_selection_not_upload_time(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            source=Path(folder)/'raw.jpg';Image.new('RGB',(800,600)).save(source)
            with patch('openclaw_service.assistant.lighthouse_files.recognize_text',return_value='拍照时间：2026.10.13 08:31\n施工区域：A楼'):
                info=inspect_photo(source,'A',at(DAY+'T09:00:00'))
            self.assertEqual(info['options']['time'],'2026-10-13 08:31')
            with patch('openclaw_service.assistant.lighthouse_files.recognize_text',return_value='施 工 区 域 ： H 楼\n拍 摄 时 间 ： 2026 ． 1 0 ． 1 7 1 8 ： 00'):
                info=inspect_photo(source,'H',at('2026-10-18T09:00:00'))
            self.assertTrue(info['existing_watermark'])
            self.assertEqual(info['options']['time'],'2026-10-17 18:00')
            with patch('openclaw_service.assistant.lighthouse_files.recognize_text',return_value='拍 摄 时 间 ： 2026 ． 1 0 ． 2 1 3 ： 00'):
                info=inspect_photo(source,'H',at('2026-10-18T09:00:00'))
            self.assertEqual(info['options']['time'],'','ambiguous OCR day/hour must be chosen by the user')
            with patch('openclaw_service.assistant.lighthouse_files.recognize_text',return_value='拍摄时间：2026.10.2 13:00'):
                info=inspect_photo(source,'H',at('2026-10-18T09:00:00'))
            self.assertEqual(info['options']['time'],'2026-10-02 13:00')
            with patch('openclaw_service.assistant.lighthouse_files.recognize_text',side_effect=TimeoutError()):
                info=inspect_photo(source,'A',at(DAY+'T09:00:00'))
            self.assertEqual(info['options']['time'],'')
            self.assertNotIn('未获取',json.dumps(info,ensure_ascii=False))
            with self.assertRaisesRegex(ValueError,'拍照时间'):
                photo_options({**info['options'],'enabled':True})

    def test_edited_fields_size_and_option_validation(self):
        import tempfile
        options={'enabled':True,'existing':False,'scale':.18,'title':'自定义标题','area':'B楼施工区域',
                 'time':'2026-10-13 08:31','weather':'多云','location':'用户确认地点'}
        for bad in ({'scale':.17},{'scale':.76},{'scale':float('nan')},{'time':''},{'title':'x'*101},{'other':'x'},{'enabled':1}):
            with self.subTest(bad=bad),self.assertRaises(ValueError):photo_options({**options,**bad})
        with tempfile.TemporaryDirectory() as folder:
            source,target=Path(folder)/'raw.jpg',Path(folder)/'out.jpg'
            Image.new('RGB',(1600,1200),'#708090').save(source)
            for scale in (.18,.75):
                result=render_handover(source,target,'A',at(DAY+'T09:00:00'),capture={'watermark':{**options,'scale':scale}})
                self.assertEqual([result[k] for k in ('title','area','time','weather','location')],[options[k] for k in ('title','area','time','weather','location')])
                with Image.open(target) as image:
                    self.assertEqual(image.size,(1600,1200))
                    expected_width=1600*scale
                    self.assertGreater(image.getpixel((int(expected_width*.8),1160))[0],140)
                    self.assertLess(image.getpixel((int(expected_width+80),1160))[0],140)
            render_handover(source,target,'A',at(DAY+'T09:00:00'),capture={'watermark':{
                **options,'title':'标题'*50,'area':'区域'*40,'weather':'天气'*50,'location':'地点'*100}})
            with Image.open(target) as image:
                self.assertEqual(image.size,(1600,1200))
                self.assertEqual(image.getpixel((400,1100)),(112,128,144),'long edited text stays inside the watermark panel')

    def test_building_time_and_weather_are_sourced_not_copied_from_example(self):
        current = at(DAY + 'T09:00:00')
        for scope in checks.SCOPES:
            stamp = watermark_details(scope, current)
            label = '110站' if scope == '110' else scope + '楼'
            self.assertEqual(stamp['title'], f'EA118南通机房{label}交接班')
            self.assertEqual(stamp['area'], label)
            self.assertEqual(stamp['time_label'], '拍照时间')
            self.assertEqual(stamp['time'], '未获取')
            self.assertEqual(stamp['weather'], '')
            self.assertEqual(stamp['location'], '')
        capture = {'captured_at': current.timestamp()*1000, 'position':{'latitude':32.0167,'longitude':120.8667,'accuracy':15,'timestamp':current.timestamp()*1000}}
        weather = {'source_state':'fresh','weather':{'observed_at':current.isoformat(),'condition':'多云','temperature':25}}
        stamp = watermark_details('B', current, weather, {36867: '2026:10:14 08:56:00'},capture=capture)
        self.assertEqual(stamp['time_label'], '拍照时间')
        self.assertEqual(stamp['time'], '2026.10.14 08:56')
        self.assertEqual(stamp['weather'], '多云 25℃')
        self.assertEqual(stamp['location'], '', 'unresolved GPS must not become watermark text')
        self.assertEqual(watermark_details('B',current,capture={**capture,'_location_name':'江苏省南通市崇川区中央路'})['location'],
                         '江苏省南通市崇川区中央路')
        self.assertEqual(watermark_details('B',current,capture={'_location_name':'未定位的地址'})['location'], '')
        self.assertEqual(watermark_details('B',current,capture=capture)['time'],'2026.10.14 09:00')
        for bad in ({'source_state':'stale'},{'weather':{**weather['weather'],'observed_at':(current-dt.timedelta(hours=1)).isoformat()}},
                    {'weather':{**weather['weather'],'temperature':None}}, {'weather':{}}):
            self.assertEqual(watermark_details('B',current,{**weather,**bad},capture=capture)['weather'],'')
        for bad in ({'timestamp':0},{'latitude':float('nan')},{'accuracy':2000}):
            stamp=watermark_details('B',current,weather,capture={**capture,'position':{**capture['position'],**bad}})
            self.assertEqual((stamp['location'],stamp['weather']),('',''))
        self.assertEqual(watermark_details('B',current,{'available':True,'description':'多云','minimum':19,'maximum':25},capture=capture)['weather'],'')
        self.assertEqual(watermark_details('B', current, exif={36867: '2099:01:01 01:00:00'})['time'], '未获取')
        forecast={'forecast':{'available':True,'city':'南通','date':DAY,'fetched_at':current.timestamp(),
                              'description':'多云','minimum':19,'maximum':25}}
        self.assertEqual(watermark_details('B',current,forecast)['weather'],'南通预报 多云 19～25℃')
        self.assertEqual(watermark_details('B',current,forecast)['location'],'')
        away={**capture,'position':{**capture['position'],'latitude':39.9,'longitude':116.4}}
        self.assertEqual(watermark_details('B',current,forecast,capture=away)['weather'],'')

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
                self.assertGreater(out.getpixel((600, 1550))[0], 140, 'white translucent stamp at bottom-left, away from text')
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
    def test_logo_is_part_of_upload_identity_and_existing_stamp_is_not_overlaid(self):
        self.save('A',0,[{'id':'x','text':'检查'}]);current=at(DAY+'T09:00:00');self.prepare(current)
        options={'enabled':True,'existing':False,'scale':.4,'title':'测试','area':'A楼','time':'2026-10-13 08:31','weather':'','location':''}
        raw=io.BytesIO();Image.new('RGB',(1000,800),'#708090').save(raw,format='PNG')
        def upload(logo,content=None,name='现场.png'):
            session=self.service.stage_upload(self.worker,DAY+':A:白','x','item',name)
            Path(session['path']).write_bytes(content or raw.getvalue());session['photo_capture']={'watermark':{**options,'logo':logo}}
            return self.service.commit_upload(self.worker,session,None,current)
        stamped=upload('vnet');self.assertEqual(upload('vnet')['id'],stamped['id'])
        plain=upload('');self.assertNotEqual(plain['id'],stamped['id'])
        original=Path(self.service.get_file(self.worker,stamped['id'])['path']).read_bytes()
        preserved=upload('vnet',original,'已有水印.jpg')
        self.assertEqual(Path(self.service.get_file(self.worker,preserved['id'])['path']).read_bytes(),original)
        self.assertEqual(list(self.service.staging_dir.iterdir()),[])

    def test_optional_stamp_preserves_original_and_uses_option_aware_dedup(self):
        self.save('A',0,[{'id':'x','text':'检查'}]);current=at(DAY+'T09:00:00');self.prepare(current)
        options={'enabled':False,'existing':False,'scale':.4,'title':'自定义','area':'A楼','time':'2026-10-13 08:31','weather':'','location':''}
        original=io.BytesIO();Image.new('RGB',(1000,800),'#708090').save(original,format='PNG')
        def upload(opts,kind='item',content=None,name='现场.png'):
            session=self.service.stage_upload(self.worker,DAY+':A:白','x' if kind=='item' else '',kind,name)
            Path(session['path']).write_bytes(content or original.getvalue())
            session['photo_capture']={'watermark':opts}
            return self.service.commit_upload(self.worker,session,None,current)
        plain=upload(options)
        self.assertFalse(plain['watermarked'])
        self.assertEqual(Path(self.service.get_file(self.worker,plain['id'])['path']).read_bytes(),original.getvalue())
        self.assertEqual(upload(options)['id'],plain['id'])
        stamped=upload({**options,'enabled':True,'scale':.18})
        self.assertNotEqual(stamped['id'],plain['id'])
        changed=upload({**options,'enabled':True,'scale':.75,'title':'改后标题'})
        self.assertNotEqual(changed['id'],stamped['id'])
        result=upload({**options,'enabled':True},content=Path(self.service.get_file(self.worker,stamped['id'])['path']).read_bytes(),name='已加水印.jpg')
        self.assertEqual(Path(self.service.get_file(self.worker,result['id'])['path']).read_bytes(),Path(self.service.get_file(self.worker,stamped['id'])['path']).read_bytes())
        handover=upload({**options,'existing':True,'enabled':True},kind='handover')
        self.assertEqual(handover['name'],'现场.png')
        self.assertEqual(Path(self.service.get_file(self.worker,handover['id'])['path']).read_bytes(),original.getvalue())
        self.assertTrue(handover['watermarked'])
        with self.service.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM photo_deliveries').fetchone()[0],5)
        self.assertEqual(list(self.service.staging_dir.iterdir()),[])

    def test_item_camera_also_stamps_persists_and_queues_only_once(self):
        self.save('A', 0, [{'id':'x','text':'检查'}])
        current = at(DAY+'T09:00:00')
        self.prepare(current)
        def upload(captured):
            session = self.service.stage_upload(self.worker,DAY+':A:白','x','item','现场.png',captured=captured)
            Image.new('RGB',(800,600),'#708090').save(session['path'],format='PNG')
            return self.service.commit_upload(self.worker,session,None,current)
        ordinary = upload(False)
        camera = upload(True)
        self.assertNotEqual(ordinary['id'],camera['id'],'a prior unwatermarked file cannot replace the camera result')
        self.assertTrue(camera['watermarked'])
        self.assertEqual(camera['watermark']['title'],'EA118南通机房A楼班前检查')
        self.assertEqual(upload(True)['id'],camera['id'])
        meta = self.service.get_file(self.worker,camera['id'])
        with Image.open(meta['path']) as photo:
            self.assertEqual(photo.format,'JPEG')
            self.assertGreater(photo.getpixel((40,560))[0],140)
        state = self.service.state(self.worker,'A',current)
        file = next(f for f in state['tasks'][0]['files'] if f['id']==camera['id'])
        self.assertEqual(file['delivery_status'],'pending')
        with self.service.connection() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM photo_deliveries').fetchone()[0],1)
        self.assertEqual(list(self.service.staging_dir.iterdir()),[])
        self.service.submit(self.worker,DAY+':A:白',1,[{'id':'x','status':'normal','note':''}],current)
        self.service.sync(current)
        cloud_files = self.cloud.upsert.call_args.args[1][0]['_files']
        self.assertEqual(Path(next(f['path'] for f in cloud_files if f['file_id']==camera['id'])).read_bytes(),Path(meta['path']).read_bytes())

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
        # Inject a storage error only after both file and delivery inserts ran.
        with self.service.connection(True) as conn:
            conn.execute("""CREATE TRIGGER fail_photo_delivery AFTER INSERT ON photo_deliveries
                            BEGIN SELECT RAISE(ABORT, 'injected storage failure'); END""")
        with self.assertRaisesRegex(sqlite3.IntegrityError, 'injected storage failure'):
            self.upload_again()
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM files').fetchone()[0],0)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM photo_deliveries').fetchone()[0],0)
        self.assertEqual(list(self.service.files_dir.glob('*.jpg')),[])
        self.assertEqual(list(self.service.staging_dir.iterdir()),[])
        with self.service.connection(True) as conn:
            conn.execute('DROP TRIGGER fail_photo_delivery')
        photo = self.upload_again()
        with self.service.connection() as conn:
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM files').fetchone()[0], 1)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM photo_deliveries').fetchone()[0], 1)
        self.assertTrue(Path(self.service.get_file(self.worker, photo['id'])['path']).is_file())

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

    async def test_preserved_png_is_uploaded_with_its_actual_mime(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'original.png';Image.new('RGB',(20,20)).save(path)
            self.client.post.side_effect=[SimpleNamespace(status_code=200,json=lambda:{'code':0,'data':{'image_key':'img_key'}}),
                                         SimpleNamespace(status_code=200,json=lambda:{'code':0,'data':{'message_id':'om_png'}})]
            result=await self.sender.send('ou_logged_in',None,'png',photo={'path':str(path),'name':'original.png'},checkpoint=Mock())
            self.assertEqual(result,'om_png')
            self.assertEqual(self.client.post.call_args_list[0].kwargs['files']['image'][2],'image/png')

    async def test_large_original_uses_small_send_copy_and_retry_reuses_image_key(self):
        import tempfile
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'original.png'
            Image.effect_noise((2300,2300),100).convert('RGB').save(path,compress_level=0)
            original=path.read_bytes()
            async def post(url, **kwargs):
                if url.endswith('/images'):
                    name,stream,mime=kwargs['files']['image'];data=stream.read()
                    self.assertTrue(0<len(data)<=10*1024*1024);self.assertEqual(mime,'image/jpeg')
                    self.assertIn('发送副本',name)
                    return SimpleNamespace(status_code=200,json=lambda:{'code':0,'data':{'image_key':'img_large'}})
                return SimpleNamespace(status_code=200,json=lambda:{'code':0,'data':{'message_id':'om_large'}})
            self.client.post.side_effect=post
            result=await self.sender.send('ou_logged_in',None,'large',photo={'path':str(path),'name':'original.png'},checkpoint=Mock())
            self.assertEqual(result,'om_large');self.assertEqual(path.read_bytes(),original)
            with patch('lan_bitable_template_portal.pre_shift_photo.prepare_delivery_image',side_effect=AssertionError('must not reupload')):
                await self.sender.send('ou_logged_in',None,'large',photo={'path':str(path),'name':'original.png','image_key':'img_large','sender_app_id':'cli_agent'},checkpoint=Mock())
            self.assertEqual(self.client.post.call_count,3)

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
    def test_location_settings_are_admin_only_private_and_persistent(self):
        from fastapi import FastAPI
        import httpx
        from lan_bitable_template_portal import pre_shift_routes
        from lan_bitable_template_portal.pre_shift_location import PhotoLocation
        self.save('A',0,[{'id':'x','text':'检查'}]);self.prepare(at(DAY+'T09:00:00'))
        user={**self.worker,'open_id':self.worker['id'],'modules':['maintenance']}
        async def read_json(request,**kwargs):
            return await request.json()
        controller=SimpleNamespace(_current_session=lambda _: {'user':user},_request_base_url=lambda _:'http://fixture',_read_json_request=read_json)
        runtime=SimpleNamespace(service=Mock(),auth_manager=SimpleNamespace(public_user=lambda _:user));app=FastAPI()
        with patch.object(pre_shift_routes,'CheckService',return_value=self.service):
            pre_shift_routes.install_pre_shift_routes(app,controller,runtime)
            async def exercise():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://fixture') as client:
                    endpoint='/api/pre-shift-checks/location-settings'
                    for level in ('L4','L3','L2'):
                        user['level']=level
                        self.assertEqual((await client.get(endpoint)).status_code,403)
                        self.assertEqual((await client.put(endpoint,json={})).status_code,403)
                    user['level']='L1'
                    settings=(await client.get(endpoint)).json()['data']
                    key='0'*32;body={'key':key,'revision':settings['revision']}
                    self.assertFalse(settings['configured'])
                    self.assertEqual((await client.put(endpoint,json=body,headers={'Origin':'https://other.example'})).status_code,403)
                    self.assertEqual((await client.put(endpoint,json={**body,'key':'wrong'})).status_code,400)
                    response=await client.put(endpoint,json=body)
                    self.assertEqual(response.status_code,200,response.text)
                    self.assertTrue(response.json()['data']['configured'])
                    self.assertNotIn(key,response.text)
                    self.assertEqual((await client.put(endpoint,json=body)).status_code,409)
                    restarted=PhotoLocation(self.service.db.parent/'location_settings.json')
                    self.assertEqual(restarted.settings(),response.json()['data'])
                    self.assertNotIn(key,restarted.path.read_text())
                    user['modules']=[]
                    self.assertEqual((await client.get(endpoint)).status_code,403)
            asyncio.run(exercise())

    def test_retained_preview_confirm_cancel_and_permissions_via_real_routes(self):
        from fastapi import FastAPI
        import httpx
        from lan_bitable_template_portal import pre_shift_routes
        self.save('A',0,[{'id':'x','text':'检查'}]);self.prepare(at(DAY+'T09:00:00'))
        user={**self.worker,'open_id':'ou_supervisor','name':'本楼主管','category':'supervisor','level':'L3','modules':['maintenance']}
        async def read_json(request, **kwargs):
            return await request.json()
        controller=SimpleNamespace(_current_session=lambda _: {'user':user},_request_base_url=lambda _:'http://fixture',_read_json_request=read_json)
        runtime=SimpleNamespace(service=Mock(),auth_manager=SimpleNamespace(public_user=lambda _:user));app=FastAPI()
        raw=io.BytesIO();Image.new('RGB',(900,700)).save(raw,format='JPEG')
        params={'task_id':DAY+':A:白','item_id':'x','kind':'item','name':'现场.jpg','retain':'1'}
        def resolve(capture):
            self.assertNotIn('_location_name',capture,'untrusted browser address is discarded before geocoding')
            return '江苏省南通市崇川区中央路'
        with patch.object(pre_shift_routes,'CheckService',return_value=self.service),patch(
            'openclaw_service.assistant.lighthouse_files.recognize_text',return_value=''),patch(
            'lan_bitable_template_portal.pre_shift_location.PhotoLocation.resolve',side_effect=resolve):
            pre_shift_routes.install_pre_shift_routes(app,controller,runtime)
            async def exercise():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://fixture') as client:
                    current=at(DAY+'T09:00:00')
                    position={'latitude':32.0167,'longitude':120.8667,'accuracy':15,'timestamp':current.timestamp()*1000}
                    from urllib.parse import quote
                    response=await client.post('/api/pre-shift-checks/photo-preview',params=params,content=raw.getvalue(),
                        headers={'X-Photo-Context':quote(json.dumps({'position':position,'_location_name':'伪造地址'}))})
                    self.assertEqual(response.status_code,200,response.text);info=response.json()['data']
                    self.assertEqual(info['options']['location'],'江苏省南通市崇川区中央路')
                    self.assertEqual(len(list(self.service.staging_dir.glob('*.upload'))),1)
                    body={'preview_id':info['preview_id'],'watermark':info['options'],'remember_style':True}
                    denied=await client.post('/api/pre-shift-checks/files',json=body,headers={'Origin':'https://other.example'})
                    self.assertEqual(denied.status_code,403)
                    response=await client.post('/api/pre-shift-checks/files',json={**body,'task_id':'another'})
                    self.assertEqual(response.status_code,400,'metadata confirmation cannot rebind task')
                    response=await client.post('/api/pre-shift-checks/files',json=body)
                    self.assertEqual(response.status_code,200,response.text);result=response.json()['data']
                    replay=await client.post('/api/pre-shift-checks/files',json=body)
                    self.assertEqual(replay.json()['data']['id'],result['id'])
                    photo=await client.get(result['url']);self.assertEqual(photo.content,raw.getvalue())
                    response=await client.post('/api/pre-shift-checks/photo-preview',params=params,content=raw.getvalue())
                    key=response.json()['data']['preview_id']
                    response=await client.request('DELETE','/api/pre-shift-checks/photo-preview',json={'preview_id':key})
                    self.assertTrue(response.json()['data']['cancelled'])
                    self.assertEqual(list(self.service.staging_dir.glob('*.upload')),[])
                    response=await client.post('/api/pre-shift-checks/photo-preview',params=params,content=raw.getvalue())
                    key=response.json()['data']['preview_id'];user['modules']=[]
                    denied=await client.post('/api/pre-shift-checks/files',json={**body,'preview_id':key})
                    self.assertEqual(denied.status_code,403)
                    with self.service.connection() as conn:
                        self.assertEqual(conn.execute('SELECT COUNT(*) FROM files').fetchone()[0],1)
                        self.assertEqual(conn.execute('SELECT COUNT(*) FROM photo_deliveries').fetchone()[0],1)
                    user['modules']=['maintenance']
                    response=await client.post('/api/pre-shift-checks/submit',json={'task_id':DAY+':A:白','version':1,'answers':[{'id':'x','status':'normal','note':''}]})
                    self.assertEqual(response.status_code,200,response.text)
                    self.assertEqual(response.json()['data']['actor_name'],'本楼主管')
            asyncio.run(exercise())


class PhotoLocationTests(Base):
    def mapper(self,client=None):
        from lan_bitable_template_portal.pre_shift_location import PhotoLocation
        mapper=PhotoLocation(Path(self.temp)/'location_settings.json',client=client)
        mapper.save({'key':'0'*32,'revision':mapper.settings()['revision']})
        self.addCleanup(mapper.close)
        return mapper

    def position(self,**values):
        return {'position':{'latitude':32.0167,'longitude':120.8667,'accuracy':15,
                            'timestamp':at(DAY+'T09:00:00').timestamp()*1000,**values}}

    def test_convert_then_geocode_and_bounded_cached_lookup(self):
        client=Mock();client.request_json.side_effect=[{'status':'1','locations':'120.870000,32.019000'},
            {'status':'1','regeocode':{'formatted_address':'江苏省南通市崇川区中央路'}}]
        mapper=self.mapper(client);current=at(DAY+'T09:00:00')
        address=mapper.resolve(self.position(),current)
        self.assertEqual(address,'江苏省南通市崇川区中央路')
        first,second=client.request_json.call_args_list
        self.assertTrue(first.args[1].endswith('/coordinate/convert'))
        self.assertEqual(first.kwargs['params']['locations'],'120.866700,32.016700')
        self.assertEqual(first.kwargs['params']['coordsys'],'gps')
        self.assertTrue(second.args[1].endswith('/geocode/regeo'))
        self.assertEqual(second.kwargs['params']['location'],'120.870000,32.019000')
        self.assertEqual(mapper.resolve(self.position(),current),address)
        self.assertEqual(client.request_json.call_count,2)
        for values in ({'accuracy':2000},{'latitude':float('nan')},{'longitude':181},{'timestamp':0}):
            self.assertEqual(mapper.resolve(self.position(**values),current),'')
        self.assertEqual(client.request_json.call_count,2)
        client.request_json.side_effect=TimeoutError('transient failure')
        with patch('lan_bitable_template_portal.pre_shift_location.time.monotonic',return_value=1_000_000_000):
            for number in range(260):
                mapper.resolve(self.position(longitude=121+number/10000),current)
        self.assertLessEqual(len(mapper.cache),256)
        mapper.save({'key':'1'*32,'revision':mapper.settings()['revision']})
        self.assertEqual(len(mapper.cache),0,'key replacement clears previously cached addresses')

    def test_provider_errors_never_become_coordinates_or_block_upload(self):
        client=Mock();mapper=self.mapper(client);current=at(DAY+'T09:00:00')
        cases=[TimeoutError('key=private'),[{'status':'0','info':'INVALID_USER_KEY'}],
            [{'status':'1','locations':'999,999'}],[{'status':'1','locations':'120.8,32;121,32'}],
            [{'status':'1','locations':'120.8,32'},{'status':'0','regeocode':{'formatted_address':'错误'}}],
            [{'status':'1','locations':'120.8,32'},{'status':'1','regeocode':{'formatted_address':'GPS 32.01670, 120.86670'}}],
            [{'status':'1','locations':'120.8,32'},{'status':'1','regeocode':{'formatted_address':[]}}]]
        for result in cases:
            with self.subTest(result=str(result)),mapper.lock:
                mapper.cache.clear();client.request_json.side_effect=result
                self.assertEqual(mapper.resolve(self.position(),current),'')
        mapper.path.write_text('{broken')
        self.assertEqual(mapper.resolve(self.position(),current),'')

    def test_requests_redact_key_and_coordinates_from_normal_http_logging(self):
        import httpx
        from upload_event_module.services.http_client import FeishuHttpClient
        def response(request):
            data=({'status':'1','locations':'120.870000,32.019000'} if request.url.path.endswith('/convert')
                  else {'status':'1','regeocode':{'formatted_address':'江苏省南通市崇川区中央路'}})
            return httpx.Response(200,json=data)
        mapper=self.mapper(FeishuHttpClient(transport=httpx.MockTransport(response),retries=0))
        with self.assertLogs('httpx',level='INFO') as logs:
            self.assertTrue(mapper.resolve(self.position(),at(DAY+'T09:00:00')))
        for message in logs.output:
            self.assertNotIn('0'*32,message)
            self.assertNotIn('120.866700',message)
            self.assertNotIn('32.019000',message)

class PhotoUploadRouteTests(Base):
    def test_photo_editor_preview_never_commits_and_no_stamp_preserves_original(self):
        from fastapi import FastAPI
        import httpx
        from urllib.parse import quote
        from lan_bitable_template_portal import pre_shift_routes
        self.save('A',0,[{'id':'x','text':'检查'}]);current=at(DAY+'T09:00:00');self.prepare(current)
        user={**self.worker,'open_id':self.worker['id'],'modules':['maintenance']}
        controller=SimpleNamespace(_current_session=lambda _: {'user':user},_request_base_url=lambda _:'http://fixture')
        runtime=SimpleNamespace(service=Mock(),auth_manager=SimpleNamespace(public_user=lambda _:user));app=FastAPI()
        raw=io.BytesIO();exif=Image.Exif();exif[36867]='2026:10:12 17:32:00';Image.new('RGB',(800,600)).save(raw,format='JPEG',exif=exif)
        params={'task_id':DAY+':A:白','kind':'handover','name':'现场.jpg'}
        with patch.object(pre_shift_routes,'CheckService',return_value=self.service),patch(
            'openclaw_service.assistant.lighthouse_files.recognize_text',return_value=''):
            pre_shift_routes.install_pre_shift_routes(app,controller,runtime)
            async def exercise():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://fixture') as client:
                    result=await client.post('/api/pre-shift-checks/photo-preview',params=params,content=raw.getvalue())
                    self.assertEqual(result.status_code,200,result.text);info=result.json()['data']
                    self.assertEqual(info['options']['time'],'2026-10-12 17:32')
                    with self.service.connection() as db:
                        self.assertEqual(db.execute('SELECT COUNT(*) FROM files').fetchone()[0],0)
                        self.assertEqual(db.execute('SELECT COUNT(*) FROM photo_deliveries').fetchone()[0],0)
                    self.assertEqual(list(self.service.staging_dir.iterdir()),[])
                    denied=await client.post('/api/pre-shift-checks/photo-preview',params=params,content=raw.getvalue(),headers={'Origin':'https://other.example'})
                    self.assertEqual(denied.status_code,403)
                    headers={'X-Photo-Context':quote(json.dumps({'watermark':{**info['options'],'enabled':False}},ensure_ascii=False))}
                    result=await client.post('/api/pre-shift-checks/files',params=params,content=raw.getvalue(),headers=headers)
                    self.assertEqual(result.status_code,200,result.text);photo=result.json()['data']
                    self.assertFalse(photo['watermarked'])
                    fetched=await client.get(photo['url'])
                    self.assertEqual(fetched.content,raw.getvalue())
                    self.assertIn('inline',fetched.headers['content-disposition'])
                    self.assertEqual(photo['delivery_status'],'pending')
            asyncio.run(exercise())

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
        weather = Mock(return_value={'snapshot':{'source_state':'fresh','weather':{
            'observed_at':current.isoformat(),'condition':'多云','temperature':25}}})
        runtime.service._state_store.get_critical_guard_weather_state = weather
        capture = {'captured_at':current.timestamp()*1000,'position':{
            'latitude':32.0167,'longitude':120.8667,'accuracy':15,'timestamp':current.timestamp()*1000}}
        raw = io.BytesIO()
        Image.new('RGB',(800,600),'#708090').save(raw,format='PNG')
        params = {'task_id':DAY+':A:白','kind':'handover','name':'现场.png','capture':'1'}
        with patch.object(pre_shift_routes, 'CheckService', return_value=self.service):
            pre_shift_routes.install_pre_shift_routes(app,controller,runtime)
            async def exercise():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://fixture',headers={'X-Photo-Context':json.dumps(capture)}) as client:
                    response = await client.post('/api/pre-shift-checks/files',params=params,content=raw.getvalue())
                    self.assertEqual(response.status_code,200,response.text)
                    photo = response.json()['data']
                    self.assertEqual(photo['watermark']['area'],'A楼')
                    self.assertEqual(photo['watermark']['weather'],'多云 25℃')
                    self.assertEqual(photo['watermark']['time_label'],'拍照时间')
                    self.assertEqual(photo['watermark']['time'],'2026.10.14 09:00')
                    weather.assert_called_once_with()
                    download = await client.get(photo['url'])
                    self.assertEqual(download.headers['content-type'],'image/jpeg')
                    self.assertEqual(Image.open(io.BytesIO(download.content)).format,'JPEG')
                    self.service.delete_file(self.worker,photo['id'])
                    weather.side_effect = TimeoutError('observation unavailable')
                    response = await client.post('/api/pre-shift-checks/files',params=params,content=raw.getvalue())
                    self.assertEqual(response.status_code,200,response.text)
                    photo = response.json()['data']
                    self.assertEqual(photo['watermark']['weather'],'')
                    self.service.delete_file(self.worker,photo['id'])
                    def revoke():
                        user['modules'] = []
                        return {}
                    weather.side_effect = revoke
                    response = await client.post('/api/pre-shift-checks/files',params=params,content=raw.getvalue())
                    self.assertEqual(response.status_code,403,response.text)
                    self.assertEqual(list(self.service.staging_dir.iterdir()),[])
            asyncio.run(exercise())


if __name__ == '__main__':
    unittest.main()
