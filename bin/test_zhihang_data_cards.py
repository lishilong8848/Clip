import json
import os
import unittest
from copy import deepcopy
from unittest.mock import patch

from lan_bitable_template_portal import zhihang_data_cards as cards


class Store:
    def __init__(self):
        self.docs = {}

    def get_document(self, namespace, key):
        return deepcopy(self.docs.get((namespace, key)))

    def put_documents(self, namespace, documents):
        for key, value in documents.items():
            self.docs[namespace, key] = deepcopy(value)


def inspection(name, status, start="2026/11/12 09:00", end="2026/11/13 00:00", **fields):
    return {"楼栋": "C楼", "巡检人": name, "工单状态": status, "计划区间": f"{start}-{end}",
            "异常标": "异常", "巡检结果": "总:40 正常:38 异常:2", "提交时间": "2026-11-12 10:00", **fields}


class ZhihangDataCardsTests(unittest.TestCase):
    def setUp(self):
        env = patch.dict(os.environ, clear=True)
        env.start()
        self.addCleanup(env.stop)
        sender = patch("upload_event_module.services.robot_webhook.send_interactive_to_chat_id", return_value=(True, "ok"))
        self.send = sender.start()
        self.addCleanup(sender.stop)
        delay = patch.object(cards.time, "sleep")
        self.sleep = delay.start()
        self.addCleanup(delay.stop)
        self.store = Store()

    def notify(self, kind, rows, slot="2026-11-12T11:00:00+08:00", run_id="run1", *, created=None):
        if created:
            self.store.put_documents(cards.NAMESPACE, {"run:" + run_id: {"created_at": created}})
        return cards.notify(kind, rows, slot, run_id, store=self.store)

    def test_normal_completion_excluded_from_details_and_overflow_without_mutating_rows(self):
        rows = {f"normal:{i}": inspection(f"正常人员{i}", "正常完成") for i in range(35)}
        rows.update({f"abnormal:{i}": inspection(f"异常人员{i}", "逾期完成") for i in range(22)})
        saved = deepcopy(rows)
        receipt = self.notify("inspection", rows)
        self.assertEqual(saved, rows)
        self.assertEqual(receipt["abnormal_count"], 22)
        card, chat = self.send.call_args.args
        content = json.dumps(card, ensure_ascii=False)
        self.assertEqual(chat, cards.DEFAULT_CHAT_ID)
        self.assertEqual(card["header"]["title"]["content"], "巡检异常提醒（11:00班次）")
        self.assertNotIn("正常人员", content)
        self.assertNotIn("正常完成", content)
        self.assertIn("另有 2 项未展开", content)
        self.assertIn("异常点 2", content)

    def test_normal_only_manual_and_disabled_are_silent(self):
        normal = {"normal": inspection("用户", "正常完成", 提交时间="", 逾期原因="上游残留原因")}
        abnormal = {"late": inspection("用户", "逾期完成")}
        self.assertEqual(self.notify("inspection", normal)["abnormal_count"], 0)
        self.assertEqual(self.notify("inspection", abnormal, "", "manual")["status"], "skipped")
        self.store.put_documents(cards.NAMESPACE, {"settings:inspection": {"notify": False}})
        self.assertEqual(self.notify("inspection", abnormal, run_id="disabled")["status"], "skipped")
        with patch.dict(os.environ, {"INSPECT_AUTO_SYNC_NOTIFY_ENABLED": "false"}):
            self.assertEqual(cards.notification_targets("inspection"), [])
        self.send.assert_not_called()

    def test_scheduled_range_and_overdue_deadline_use_beijing_time(self):
        rows = {
            "from": inspection("包含开始", "待巡检", start="2026/11/12 17:00", 提交时间="", 巡检结果="", 异常标=""),
            "to": inspection("排除结束", "逾期完成", end="2026/11/12 17:00"),
            "late": inspection("已逾班", "待巡检", start="2026/11/12 15:00", 提交时间="", 巡检结果="", 异常标=""),
            "invalid": inspection("无效区间", "待巡检", end="2026/11/12 09:00"),
        }
        receipt = self.notify("inspection", rows, "2026-11-12T09:00:00Z", created="2026-11-12T20:00:01+08:00")
        self.assertEqual((receipt["abnormal_count"], receipt["invalid_plan_range_count"]), (2, 1))
        content = json.dumps(self.send.call_args.args[0], ensure_ascii=False)
        self.assertIn("已逾班", content)
        self.assertIn("包含开始", content)
        self.assertNotIn("排除结束", content)
        self.assertIn("逾班未提交", content)

    def test_original_targets_and_retry_reuse_card_and_message_uuid(self):
        os.environ.update(INSPECT_FEISHU_NOTIFY_CHAT_ID="oc_primary", EXTRA_ABNORMAL_NOTIFY_CHAT_ID="oc_extra")
        failure = (False, "mock failure")
        self.send.side_effect = [failure] * 3 + [(True, "ok")] + [failure] * 3 + [(True, "ok")]
        rows = {"late": inspection("原明细", "逾期完成")}
        self.assertEqual(self.notify("inspection", rows)["status"], "failed")
        self.assertEqual(self.send.call_count, 3)
        self.assertEqual(self.sleep.call_count, 2)
        with patch.dict(os.environ, {"INSPECT_FEISHU_NOTIFY_CHAT_ID": "oc_changed"}):
            self.assertEqual(self.notify("inspection", rows)["status"], "partial")
            self.assertEqual(self.notify("inspection", rows)["status"], "sent")
            self.assertEqual(self.notify("inspection", rows)["status"], "sent")
        primary, extra = self.send.call_args_list[:4], self.send.call_args_list[4:]
        self.assertEqual(primary, [primary[0]] * 4)
        self.assertEqual(extra, [extra[0]] * 4)
        self.assertEqual([call.args[1] for call in self.send.call_args_list], ["oc_primary"] * 4 + ["oc_extra"] * 4)
        self.assertNotEqual(primary[0].kwargs["message_uuid"], extra[0].kwargs["message_uuid"])

    def test_name_target_uses_current_app_chat_list_and_deduplicates_extra(self):
        os.environ.update(INSPECT_FEISHU_NOTIFY_CHAT_NAME="巡检群", EXTRA_ABNORMAL_NOTIFY_CHAT_ID="oc_group")
        rows = {"late": inspection("巡检人", "逾期完成")}
        with patch("upload_event_module.services.robot_webhook._get_chat_cache", return_value=(
            [{"name": "巡检群", "chat_id": "oc_group"}], "",
        )) as lookup:
            self.assertEqual(self.notify("inspection", rows)["status"], "sent")
        lookup.assert_called_once()
        self.send.assert_called_once()
        self.assertEqual(self.send.call_args.args[1], "oc_group")

    def test_change_focus_quiet_final_and_important_target_cadence(self):
        os.environ.update(CHANGE_FEISHU_NOTIFY_CHAT_ID="oc_change", CHANGE_IMPORTANT_NOTIFY_CHAT_ID="oc_important",
                          EXTRA_ABNORMAL_NOTIFY_CHAT_ID="oc_extra")
        self.store.put_documents(cards.NAMESPACE, {"settings:change": {"times": ["08:07", "15:38"]}})
        quiet = {"change:1": {"状态": "审批中", "标题": "无临期"}}
        important = {"change:2": {"状态": "审批中", "标题": "待办变更", "当前节点": "客服审核"}}
        self.assertEqual(self.notify("change", quiet, "2026-11-12T08:07:00+08:00")["status"], "skipped")
        self.assertEqual(self.notify("change", important, "2026-11-12T08:07:00+08:00", "focus")["status"], "sent")
        self.assertEqual([call.args[1] for call in self.send.call_args_list], ["oc_change", "oc_extra"])
        self.assertEqual(self.notify("change", quiet, "2026-11-12T15:38:00+08:00", "quiet")["status"], "skipped")
        self.send.reset_mock()
        self.notify("change", important, "2026-11-13T15:38:00+08:00", "important")
        self.assertEqual([call.args[1] for call in self.send.call_args_list], ["oc_change", "oc_important", "oc_extra"])

    def test_event_delta_and_final_slot_without_duplicate_quiet_card(self):
        rows = {"event:1": {"事件标题": "已结案", "事件状态": "完成", "同步关注类型": "重要"}}
        self.assertEqual(self.notify("event", rows, "2026-11-12T08:37:00+08:00")["status"], "sent")
        self.assertEqual(self.notify("event", rows, "2026-11-12T16:08:00+08:00", "quiet")["status"], "skipped")
        self.assertEqual(self.notify("event", rows, "2026-11-13T08:37:00+08:00", "catchup",
                                     created="2026-11-13T09:00:00+08:00")["status"], "skipped")
        self.assertEqual(self.notify("event", rows, "2026-11-13T16:08:00+08:00", "final")["status"], "sent")

    def test_risk_and_drill_original_calendar_rules(self):
        os.environ.update(AUTO_SYNC_MESSAGE_ENABLED="true", AUTO_SYNC_MESSAGE_SCHEDULES="08:30,08:33")
        risk = {"risk:1": {"楼栋": "A楼", "排查时间": "2026-11-12", "审核时间": "2026-11-12"}}
        drill = {"drill:1": {"实施区域": "A楼", "演练年份": "2026", "演练月份": "11", "演练状态": "待评估"}}
        self.assertEqual(self.notify("risk", risk, "2026-11-12T08:30:00+08:00")["status"], "sent")
        self.assertEqual(self.notify("risk", risk, "2026-11-19T08:30:00+08:00", "repeat")["status"], "skipped")
        self.assertEqual(self.notify("risk", risk, "2026-11-30T08:30:00+08:00", "month-end")["status"], "sent")
        self.assertEqual(self.notify("drill", drill, "2026-11-12T08:33:00+08:00", "thursday")["status"], "sent")
        self.assertEqual(self.notify("drill", drill, "2026-11-13T08:33:00+08:00", "friday")["status"], "skipped")
        self.assertEqual(self.notify("drill", drill, "2026-11-26T08:33:00+08:00", "after-25")["status"], "sent")


if __name__ == "__main__":
    unittest.main()
