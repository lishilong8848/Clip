"""Adjustment SOP and non-blocking notice preparation regression tests."""
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

BIN=Path(__file__).parent
if str(BIN) not in sys.path: sys.path.insert(0,str(BIN))
from lan_bitable_template_portal.polling_work_orders import PollingWorkOrderService
from lan_bitable_template_portal.polling_work_order_relay import PollingWorkOrderRelayConnector
from lan_bitable_template_portal.state_store import LanPortalStateStore
from lan_bitable_template_portal.portal_service import MaintenancePortalService
from lan_bitable_template_portal.server import PortalRuntime
from lan_bitable_template_portal.workbench_lite import render_workbench_lite
from upload_event_module.services.handlers.device_adjust_notice import AdjustNoticeHandler
from upload_event_module.services.handlers.base import NoticePayload
from clipflow_backend.main import FastAPIPortalController


class AdjustWorkOrderTests(unittest.TestCase):
    def test_adjust_selector_and_single_generic_order(self):
        html=render_workbench_lite(payload={"records":[],"ongoing":[]},session={"role":"admin"},scope="A",work_type="adjust")
        for text in ("SOP步骤填写","选择操作步骤","本次调整不使用工单","设备调整工单"):
            self.assertIn(text,html)
        for text in ("制冷单元模式切换 SOP","调整制冷单元","当前运行模式","切换后运行模式","停机状态","板换模式","预冷模式","制冷模式"):
            self.assertIn(text,html)
        self.assertIn("'from_unit', 'to_unit', 'other_unit'",html)
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); service=PollingWorkOrderService(LanPortalStateStore(root/"state.sqlite3"))
            service.sop_root=root/"sops"; service.work_order_root=root/"orders"
            sop=service.save_sop({"work_type":"adjust","scope":"A","name":"调整作业","steps":[{"content":"核对设定值","photo_required":False,"operator_required":True,"reviewer_required":True}]})
            sop=service.add_sop_attachment(sop["sop_id"],file_name="操作说明.txt",content=b"test",expected_version=sop["version"])
            request={"scope":"A","work_type":"adjust","action":"start","_web_action_request":True,"polling_sop_id":sop["sop_id"],"polling_sop_version":sop["version"],"polling_operator_record_id":"op","polling_reviewer_record_id":"rev"}
            people=[{"record_id":"op","name":"操作人"},{"record_id":"rev","name":"审核人"}]
            prepared=service.prepare_start(request,job_id="adjust-test",people=people)
            self.assertEqual(prepared["polling_work_order_spec"]["runs"],[{"run_index":1,"label":"设备调整作业"}])
            group=service.create_group(prepared,target_record_id="recAdjust",title="调整设定值",public_base_url="http://127.0.0.1:18766")
            self.assertEqual(group["work_type"],"adjust")
            self.assertEqual(group["notice_type"],"设备调整")
            with self.assertRaises(Exception): service.prepare_start({**request,"polling_reviewer_record_id":"op"},job_id="bad",people=people)
            self.assertEqual(service.prepare_start({**request,"polling_work_order_exempt":True},job_id="exempt",people=[]),{"polling_work_order_exempt":True})

    def test_adjust_cooling_mode_uses_existing_run_fields_and_resolves_unit_text(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder); service=PollingWorkOrderService(LanPortalStateStore(root/"state.sqlite3"))
            service.sop_root=root/"sops"; service.work_order_root=root/"orders"
            sop=service.save_sop({"work_type":"adjust","scope":"A","name":"制冷单元切换","steps":[
                {"content":"确认{{from}}当前运行参数","operator_required":True,"reviewer_required":True},
                {"content":"执行{{from}}模式切换","operator_required":True,"reviewer_required":True},
            ]})
            sop=service.add_sop_attachment(sop["sop_id"],file_name="切换说明.txt",content=b"test",expected_version=sop["version"])
            base={"scope":"A","work_type":"adjust","action":"start","_web_action_request":True,"polling_sop_id":sop["sop_id"],"polling_sop_version":sop["version"],"polling_operator_record_id":"op","polling_reviewer_record_id":"rev"}
            people=[{"record_id":"op","name":"操作人"},{"record_id":"rev","name":"审核人"}]
            with self.assertRaisesRegex(Exception,"请选择本次需要调整"):
                service.prepare_start(base,job_id="missing-unit",people=people)
            with self.assertRaisesRegex(Exception,"请选择本次需要调整"):
                service.prepare_start({**base,"polling_run_count":1,"polling_runs":[{"from_unit":"1#","to_unit":"2#","other_unit":"7#"}]},job_id="bad-unit",people=people)
            with self.assertRaisesRegex(Exception,"不能相同"):
                service.prepare_start({**base,"polling_run_count":1,"polling_runs":[{"from_unit":"2#","to_unit":"2#","other_unit":"2#"}]},job_id="same-mode",people=people)
            prepared=service.prepare_start({**base,"polling_run_count":1,"polling_runs":[{"from_unit":"1#","to_unit":"2#","other_unit":"2#"}]},job_id="cooling-unit",people=people)
            self.assertEqual(prepared["polling_work_order_spec"]["runs"],[{"run_index":1,"label":"2#制冷单元由停机状态指向板换模式","from_unit":"1#","to_unit":"2#","other_unit":"2#"}])
            self.assertEqual([step["content"] for step in prepared["polling_work_order_spec"]["steps"]],["确认2#制冷单元当前运行参数","执行2#制冷单元模式切换"])
            group=service.create_group(prepared,target_record_id="recCoolingAdjust",title="制冷单元调整",public_base_url="http://127.0.0.1:18766",public_relay=True)
            self.assertEqual(group["runs"],[{"run_index":1,"label":"2#制冷单元由停机状态指向板换模式","from_unit":"1#","to_unit":"2#","other_unit":"2#"}])
            self.assertEqual([step["content"] for step in group["steps"]],["确认2#制冷单元当前运行参数","执行2#制冷单元模式切换"])
            self.assertNotIn("{{from}}",str(group["steps"]))
            prepared_local=service.prepare_start({**base,"polling_run_count":1,"polling_runs":[{"from_unit":"1#","to_unit":"2#","other_unit":"2#"}]},job_id="cooling-unit-local",people=people)
            local_group=service.create_group(prepared_local,target_record_id="recCoolingAdjustLocal",title="制冷单元调整",public_base_url="http://127.0.0.1:18766")
            self.assertEqual(local_group["runs"],group["runs"])
            self.assertEqual(
                [(step["content"],step["photo_required"]) for step in local_group["steps"]],
                [(step["content"],step["photo_required"]) for step in group["steps"]],
            )
            token=service.role_token("recCoolingAdjust","operator")
            service.activate(token,run_index=1,expected_version=group["version"])
            relay=object.__new__(PollingWorkOrderRelayConnector); relay.work_orders=service
            projection=relay._role_projection("recCoolingAdjust","operator")
            self.assertEqual(projection["work_orders"][0]["from_unit"],"1#")
            self.assertEqual(projection["work_orders"][0]["to_unit"],"2#")
            self.assertEqual(projection["work_orders"][0]["label"],"2#制冷单元由停机状态指向板换模式")
            self.assertEqual(projection["steps"][0]["content"],"确认2#制冷单元当前运行参数")
            self.assertTrue(all(step["photo_required"] for step in projection["steps"]))
            self.assertNotIn("other_unit",projection["work_orders"][0])

            invalid=service.save_sop({"work_type":"adjust","scope":"A","name":"错误占位符","steps":[{"content":"普通步骤 {{to}}","operator_required":True}]})
            invalid=service.add_sop_attachment(invalid["sop_id"],file_name="说明.txt",content=b"test",expected_version=invalid["version"])
            with self.assertRaisesRegex(Exception,"只允许使用"):
                service.prepare_start({**base,"polling_sop_id":invalid["sop_id"],"polling_sop_version":invalid["version"],"polling_run_count":1,"polling_runs":[{"from_unit":"1#","to_unit":"2#","other_unit":"2#"}]},job_id="bad-placeholder",people=people)

    def test_adjust_target_fields_and_end_guard_routing(self):
        fields=AdjustNoticeHandler().build_create_fields(NoticePayload(
            text="【设备调整】\n【名称】调整设定值\n【状态】开始\n【时间】2026-09-01 09:00至2026-09-01 10:00",
            polling_work_order_required=True,polling_operator_name="操作人",polling_reviewer_name="审核人",
        ))
        self.assertTrue(fields["是否涉及重要操作"])
        self.assertEqual(fields["操作人"],"操作人")
        self.assertEqual(fields["现场复核人"],"审核人")
        self.assertEqual(PortalRuntime._work_order_notice_type({"work_type":"adjust"}),"设备调整")
        self.assertEqual(PortalRuntime._work_order_field_config({"work_type":"adjust"})["work_order_attachments"],"工单附件")
        manager=MagicMock(); manager.get_group.return_value={"work_type":"adjust","state":"active"}
        with patch.object(PortalRuntime,"polling_work_orders",return_value=manager):
            self.assertTrue(PortalRuntime._work_order_end_error(target_record_id="recAdjust",work_type="adjust",fields={"是否涉及重要操作":True,"操作人":"op","现场复核人":"rev"}))

    def test_update_end_and_exemption_do_not_load_signature_directory(self):
        service=MagicMock(spec=MaintenancePortalService)
        service._state_store=MagicMock()
        service._truthy_flag.side_effect=lambda v:bool(v)
        service._synchronize_prepared_notice_text.side_effect=lambda p:p
        for work in ("maintenance","polling","adjust"):
            service._manual_payload_notice_work_type.return_value=work
            for action in ("update","end","start"):
                service.prepare_simple_manual_notice_action.return_value={}
                service.prepare_maintenance_action.return_value={}
                MaintenancePortalService.prepare_workbench_action(service,{"work_type":work,"action":action,"_web_action_request":True,"polling_work_order_exempt":True},job_id="timing-test")
        service._load_signature_people.assert_not_called()

    def test_local_notifications_are_retried_by_existing_worker(self):
        controller=object.__new__(FastAPIPortalController)
        manager=MagicMock(); group={"target_record_id":"recAdjust","operator_link":"http://lan/operator","reviewer_link":"http://lan/reviewer"}
        manager.open_groups.return_value=[group]; manager.group_with_links.return_value=group
        with patch("clipflow_backend.main._mock_external_enabled",return_value=False),patch.object(PortalRuntime,"polling_work_order_public_relay_should_run",return_value=False),patch.object(PortalRuntime,"polling_work_orders",return_value=manager),patch.object(PortalRuntime,"_polling_work_order_public_base_url",return_value="http://lan"),patch.object(PortalRuntime,"_send_polling_work_order_links") as send:
            controller._run_scheduled_polling_relay()
        send.assert_called_once_with(group)


if __name__=="__main__": unittest.main()
