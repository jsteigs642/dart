import unittest

from dart.model.workflow import Workflow
from dart.model.workflow import WorkflowData
from dart.model.exception import DartValidationException
from dart.schema.base import default_and_validate
from dart.schema.workflow import workflow_schema


class TestWorkflowSchema(unittest.TestCase):

    def test_workflow_schema(self):
        state = None
        wf = Workflow(data=WorkflowData('test-workflow', 'ABC123', state=state))
        obj_before = wf.to_dict()
        wf = default_and_validate(wf, workflow_schema())
        # state should be defaulted to INACTIVE
        self.assertNotEqual(obj_before, wf.to_dict())

    def test_workflow_schema_invalid(self):
        with self.assertRaises(DartValidationException) as context:
            name = None
            wf = Workflow(data=WorkflowData(name, 'ABC123'))
            # should fail because the name is missing
            default_and_validate(wf, workflow_schema())

        self.assertTrue(isinstance(context.exception, DartValidationException))


if __name__ == '__main__':
    unittest.main()
