import copy
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parent))
from lan_bitable_template_portal import feishu_assistant_questions as forms
from openclaw_service.assistant.lighthouse_ai import AssistantError
from bin.test_feishu_assistant import Memory


class QuestionForms(unittest.TestCase):
    def setUp(self):
        self.bot = SimpleNamespace(runtime=SimpleNamespace(state_store=Memory(Path('unused'))))
        self.actor = {'id': 'ou_primary', 'scopes': ['E']}
        self.message = {'message_id': 'om_original', 'chat_id': 'oc_group', 'union_id': 'on_person'}
        self.turn = {'operation_id': 'question_fixture_0001', 'question': '写篇文章',
                     'answer': '选择读者', 'clarification': {'options': ['新员工', '有经验人员']}}
        self.conversation = {'conversation_id': 'conversation', 'turns': [self.turn]}

    def make(self, values):
        card = forms.prepare_question_card(self.bot, self.message, self.actor, self.conversation, self.turn)
        button = card['body']['elements'][0]['elements'][-1]
        event = {'header': {'app_id': 'app', 'event_type': 'card.action.trigger', 'event_id': 'event'},
                 'event': {'operator': {'open_id': 'ou_botapp', 'union_id': 'on_person'},
                           'context': {'open_message_id': 'om_card', 'open_chat_id': 'oc_group'},
                           'action': {'value': button['behaviors'][0]['value'], 'form_value': values}}}
        return card, event, forms.parse_question_event(event, 'app')

    def test_card_is_editable_and_selection_returns_to_original_question(self):
        card, event, message = self.make({'choice': '0', 'answer': '重点写实际例子'})
        self.assertEqual(card['schema'], '2.0')
        elements = card['body']['elements'][0]['elements']
        self.assertEqual(elements[1]['tag'], 'select_static')
        self.assertEqual(elements[-1]['form_action_type'], 'submit')
        reply, prior = forms.read_question_reply(self.bot, message, self.actor, self.conversation)
        self.assertEqual(reply, '新员工\n重点写实际例子')
        self.assertEqual(prior, self.turn['operation_id'])
        self.assertEqual(message['reply_to_message_id'], 'om_card')
        self.assertEqual(message, forms.parse_question_event(event, 'app'))

    def test_multiple_choices_and_free_text(self):
        self.turn['clarification']['multiple'] = True
        card, _, message = self.make({'choice': ['1', '0'], 'answer': ''})
        self.assertEqual(card['body']['elements'][0]['elements'][1]['tag'], 'multi_select_static')
        self.assertEqual(forms.read_question_reply(self.bot, message, self.actor, self.conversation)[0], '有经验人员、新员工')
        self.turn['clarification']['options'] = []
        _, _, message = self.make({'answer': '详细描述'})
        self.assertEqual(forms.read_question_reply(self.bot, message, self.actor, self.conversation)[0], '详细描述')

    def test_owner_clear_expiry_and_changed_question_cannot_be_bypassed(self):
        _, _, message = self.make({'answer': '回答'})
        invalid = [({'id': 'ou_other', 'is_admin': True}, self.conversation),
                   (self.actor, {**self.conversation, 'conversation_id': 'cleared'}),
                   (self.actor, {**self.conversation, 'turns': [{'operation_id': 'new', 'question': '另一个问题'}]})]
        for actor, conversation in invalid:
            with self.assertRaises(AssistantError):
                forms.read_question_reply(self.bot, message, actor, conversation)
        stored = self.bot.runtime.state_store.get_document(forms.NAMESPACE, self.actor['id'])
        stored['expires'] = 1
        self.bot.runtime.state_store.put_document(forms.NAMESPACE, self.actor['id'], stored)
        with self.assertRaises(AssistantError):
            forms.read_question_reply(self.bot, message, self.actor, self.conversation)

    def test_unknown_options_empty_and_foreign_callback_rejected(self):
        for values in ({'choice': '999'}, {'choice': ['0', '1']}, {'answer': ''}):
            _, _, message = self.make(values)
            with self.assertRaises(AssistantError):
                forms.read_question_reply(self.bot, message, self.actor, self.conversation)
        _, event, own = self.make({'answer': '填写'})
        self.assertIsNone(forms.parse_question_event(event, 'wrong-app'))
        foreign = copy.deepcopy(event)
        foreign['event']['operator']['union_id'] = 'on_other'
        other = forms.parse_question_event(foreign, 'app')
        self.assertNotEqual(own['message_id'], other['message_id'])
        with self.assertRaises(AssistantError):
            forms.read_question_reply(self.bot, other, self.actor, self.conversation)
        event['event']['action']['form_value']['forged_owner'] = 'admin'
        self.assertIsNone(forms.parse_question_event(event, 'app'))


if __name__ == '__main__':
    unittest.main()
