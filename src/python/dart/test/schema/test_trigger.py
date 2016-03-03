import unittest

from dart.model.trigger import Trigger
from dart.model.trigger import TriggerData
from dart.model.exception import DartValidationException
from dart.schema.base import default_and_validate
from dart.schema.trigger import trigger_schema
from dart.trigger.workflow import workflow_completion_trigger


class TestTriggerSchema(unittest.TestCase):

    def test_trigger_schema(self):
        args = {'completed_workflow_id': 'ABC123'}
        state = None
        tr = Trigger(data=TriggerData('test-trigger', 'workflow_completion', ['ABC123'], args, state=state))
        obj_before = tr.to_dict()
        tr = default_and_validate(tr, trigger_schema(workflow_completion_trigger.params_json_schema))
        # state should be defaulted to INACTIVE
        self.assertNotEqual(obj_before, tr.to_dict())

    def test_trigger_schema_invalid(self):
        with self.assertRaises(DartValidationException) as context:
            name = None
            args = {'completed_workflow_id': 'ABC123'}
            tr = Trigger(data=TriggerData(name, 'workflow_completion', ['ABC123'], args))
            # should fail because the name is missing
            default_and_validate(tr, trigger_schema(workflow_completion_trigger.params_json_schema))

        self.assertTrue(isinstance(context.exception, DartValidationException))


if __name__ == '__main__':
    unittest.main()
