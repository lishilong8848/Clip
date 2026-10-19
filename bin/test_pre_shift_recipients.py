"""Fixed item recipients override, but never fall back to, shift roster recipients."""
import datetime as dt
import tempfile
import unittest
from pathlib import Path
import sys
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal.pre_shift_checks import CheckService, CheckError, TZ
from test_pre_shift_checks import receipt

NOW = dt.datetime(2026, 10, 19, 8, 40, tzinfo=TZ)
ADMIN = {'id':'ou_admin','name':'管理员','level':'L1','category':'manager','scope_rules':['ALL']}
FIXED = [{'open_id':'ou_fixed','name':'固定检查人'}]


class FixedRecipientsTests(unittest.TestCase):
    def setUp(self):
        folder = tempfile.TemporaryDirectory()
        self.addCleanup(folder.cleanup)
        self.directory = Mock(return_value=FIXED)
        self.roster = Mock(return_value={'people':[{'scope':'A','shift':s,'open_id':'ou_roster','name':'排班人员'} for s in ('白','夜')], 'warnings':{}})
        self.sender = Mock(side_effect=receipt)
        self.service = CheckService(folder.name, roster=self.roster, recipient_directory=self.directory,
                                    sender=self.sender, portal_url=lambda:'https://fixture.invalid')

    def save(self, items, version=0):
        with patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=NOW):
            return self.service.save_definition(ADMIN, 'A', version, items)

    @staticmethod
    def item(key='fixed', recipients=None):
        return {'id':key,'text':key+'检查', 'fixed_recipients':['ou_fixed'] if recipients is None else recipients}

    def text(self, call):
        return call.args[1]['elements'][0]['text']['content']

    def test_fixed_only_skips_roster_and_repeats_only_to_fixed_people_across_shifts_and_days(self):
        self.save([self.item()])
        self.service.tick(NOW)
        self.roster.assert_not_called()
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_fixed'])
        self.service.tick(NOW+dt.timedelta(minutes=2))
        self.assertEqual(self.sender.call_count,1)
        self.service.tick(NOW+dt.timedelta(hours=2,minutes=10))
        self.service.tick(NOW.replace(hour=17,minute=40))
        self.service.tick(NOW+dt.timedelta(days=1))
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_fixed']*5)
        self.assertEqual(self.service.configured_scopes(roster_only=True), [])
        self.assertEqual(self.service.state(ADMIN,'A',NOW)['roster_error'], '')

    def test_mixed_items_notify_each_recipient_only_about_assigned_items(self):
        self.save([self.item(),self.item('rostered',[])])
        self.service.tick(NOW)
        sent={c.args[0]:self.text(c) for c in self.sender.call_args_list}
        self.assertEqual(set(sent), {'ou_fixed','ou_roster'})
        self.assertIn('fixed检查', sent['ou_fixed'])
        self.assertNotIn('rostered检查', sent['ou_fixed'])
        self.assertIn('rostered检查', sent['ou_roster'])
        self.assertNotIn('fixed检查', sent['ou_roster'])
        task=self.service.state(ADMIN,'A',NOW)['tasks'][0]
        self.service.submit(ADMIN,task['id'],task['definition_version'],
            [{'id':i['id'],'status':'normal','note':''} for i in task['items']],NOW)
        self.service.tick(NOW+dt.timedelta(hours=2,minutes=10))
        self.assertEqual(self.sender.call_count,2, 'authorized completion stops both recipient groups')

    def test_same_person_receives_one_combined_message_for_mixed_items(self):
        self.directory.return_value=[{'open_id':'ou_roster','name':'排班人员'}]
        self.save([self.item(recipients=['ou_roster']),self.item('rostered',[])])
        self.service.tick(NOW)
        self.assertEqual(self.sender.call_count,1)
        text=self.text(self.sender.call_args)
        self.assertIn('fixed检查',text)
        self.assertIn('rostered检查',text)

    def test_unavailable_fixed_person_is_not_silently_replaced_by_roster(self):
        self.save([self.item(),self.item('rostered',[])])
        self.directory.return_value=[]
        self.service.tick(NOW)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_roster'])
        self.assertNotIn('fixed检查',self.text(self.sender.call_args))
        self.assertIn('固定接收人',self.service.last_error)
        self.directory.return_value=FIXED
        self.service.tick(NOW+dt.timedelta(minutes=5))
        self.assertEqual([c.args[0] for c in self.sender.call_args_list], ['ou_roster','ou_fixed'])

    def test_validation_permissions_legacy_edits_and_clearing_back_to_roster(self):
        for ids in (['ou_stranger'],['bad-id'],'ou_fixed'):
            with self.subTest(ids=ids), self.assertRaises(CheckError):
                self.save([self.item(recipients=ids)])
        self.save([self.item()])
        self.save([{'id':'fixed','text':'修改检查文案'}],1)
        with self.service.connection() as db:
            self.assertEqual(self.service.definition('A',db)['items'][0]['fixed_recipients'],['ou_fixed'])
        self.save([self.item(recipients=[])],2)
        self.service.tick(NOW)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list],['ou_roster'])
        with self.assertRaises(CheckError):
            self.service.save_definition({'id':'ou_worker','level':'L4','category':'operator','home_scope':'A'},'A',3,[self.item()])

    def test_update_and_restart_preserve_fixed_recipients_and_delivery_dedup(self):
        self.save([self.item()])
        self.service.tick(NOW)
        restarted=CheckService(self.service.db.parent, roster=self.roster, recipient_directory=self.directory,
                               sender=self.sender,portal_url=lambda:'https://fixture.invalid')
        restarted.tick(NOW+dt.timedelta(minutes=5))
        self.assertEqual(self.sender.call_count,1)
        restarted.tick(NOW+dt.timedelta(hours=2,minutes=10))
        self.assertEqual(self.sender.call_count,2)
        self.assertEqual(self.sender.call_args.args[0],'ou_fixed')

    def test_weekly_item_uses_fixed_recipient_on_each_next_occurrence(self):
        item={**self.item(), 'frequency':{'kind':'weekly','weekday':0,'time':'08:50','start_date':'2026-10-19'}}
        self.save([item])
        self.service.tick(NOW)
        self.service.tick(NOW+dt.timedelta(days=7))
        self.assertEqual([c.args[0] for c in self.sender.call_args_list],['ou_fixed','ou_fixed'])
        self.roster.assert_not_called()

    def test_fixed_recipient_directory_failure_keeps_rostered_items_deliverable(self):
        self.save([self.item(),self.item('rostered',[])])
        self.directory.side_effect=CheckError('目录加载中',503)
        self.service.tick(NOW)
        self.assertEqual([c.args[0] for c in self.sender.call_args_list],['ou_roster'])
        self.assertNotIn('fixed检查',self.text(self.sender.call_args))

    def test_completing_one_building_does_not_suppress_other_buildings_fixed_reminder(self):
        self.save([self.item()])
        with patch('lan_bitable_template_portal.pre_shift_checks.now', return_value=NOW):
            self.service.save_definition(ADMIN,'B',0,[self.item()])
        self.service.tick(NOW)
        self.assertEqual(self.sender.call_count,2)
        task=self.service.state(ADMIN,'A',NOW)['tasks'][0]
        self.service.submit(ADMIN,task['id'],task['definition_version'],[{'id':'fixed','status':'normal','note':''}],NOW)
        self.service.tick(NOW+dt.timedelta(hours=2,minutes=10))
        self.assertEqual(self.sender.call_count,3)
        self.assertIn('B楼',self.text(self.sender.call_args))


class RecipientOptionsTests(unittest.TestCase):
    def test_only_active_existing_check_permissions_are_selectable(self):
        from types import SimpleNamespace
        from lan_bitable_template_portal.pre_shift_cloud import fixed_recipient_options
        people={key:{'id':'rec'+key,'name':key,'login_ids':['ou_'+key],'selectable':True}
                for key in ('own','other','manager','disabled','departed','unclear')}
        people['departed']['inactive']=True
        people['unclear']['selectable']=False
        own={'level':'L4','category':'operator','home_scope':'A','scope_rules':['A'],'modules':['maintenance']}
        profiles={'ou_own':own,'ou_other':{**own,'home_scope':'B','scope_rules':['B']},
                  'ou_manager':{**ADMIN,'modules':['maintenance']}}
        auth=SimpleNamespace(_account_directory_provider=lambda:{'loaded_at':1,'people':people},
            account_profile=lambda oid:profiles.get(oid,{}),_open_id_explicitly_disabled=lambda oid:oid=='ou_disabled')
        options=fixed_recipient_options(SimpleNamespace(auth_manager=auth),'A')
        self.assertEqual({p['open_id'] for p in options},{'ou_own','ou_manager'})
        self.assertTrue(all(set(p)=={'name','open_id','employee_no'} for p in options))

    def test_recipient_endpoint_requires_building_management_permission(self):
        import asyncio
        import httpx
        from fastapi import FastAPI
        from types import SimpleNamespace
        from lan_bitable_template_portal import pre_shift_routes
        user={**ADMIN,'open_id':ADMIN['id'],'level':'L3','category':'supervisor',
              'scope_rules':['A'],'modules':['maintenance']}
        runtime=SimpleNamespace(service=Mock(),auth_manager=SimpleNamespace(public_user=lambda _:user))
        controller=SimpleNamespace(_current_session=lambda _: {'user':user})
        app=FastAPI()
        with patch.object(pre_shift_routes,'CheckService',return_value=Mock()), patch('lan_bitable_template_portal.pre_shift_cloud.fixed_recipient_options',return_value=FIXED) as options:
            pre_shift_routes.install_pre_shift_routes(app,controller,runtime)
            async def run():
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app),base_url='http://fixture') as client:
                    url='/api/pre-shift-checks/recipients'
                    self.assertEqual((await client.get(url+'?scope=A')).json()['data']['items'],FIXED)
                    self.assertEqual((await client.get(url+'?scope=B')).status_code,403)
                    user.update(level='L4',category='operator',home_scope='A')
                    self.assertEqual((await client.get(url+'?scope=A')).status_code,403)
            asyncio.run(run())
            options.assert_called_once_with(runtime,'A')


if __name__ == '__main__':
    unittest.main()
