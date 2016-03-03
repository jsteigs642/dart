import unittest

from dart.client.python.dart_client import Dart
from dart.engine.no_op.metadata import NoOpActionTypes
from dart.model.action import ActionData, Action, ActionState
from dart.model.datastore import Datastore, DatastoreData, DatastoreState
from dart.model.workflow import Workflow, WorkflowState, WorkflowInstanceState
from dart.model.workflow import WorkflowData


class TestWorkflowFailure(unittest.TestCase):
    def setUp(self):
        dart = Dart(host='localhost', port=5000)
        """ :type dart: dart.client.python.dart_client.Dart """
        self.dart = dart

        dst_args = {'action_sleep_time_in_seconds': 0}
        dst = Datastore(data=DatastoreData('test-datastore', 'no_op_engine', args=dst_args, state=DatastoreState.ACTIVE))
        self.datastore = self.dart.save_datastore(dst)

        wf = Workflow(data=WorkflowData('test-workflow', self.datastore.id, state=WorkflowState.ACTIVE))
        self.workflow = self.dart.save_workflow(wf, self.datastore.id)

        a = Action(data=ActionData(NoOpActionTypes.action_that_fails.name, NoOpActionTypes.action_that_fails.name, state=ActionState.TEMPLATE))
        self.dart.save_actions([a], workflow_id=self.workflow.id)

    def tearDown(self):
        for a in self.dart.get_actions(workflow_id=self.workflow.id):
            self.dart.delete_action(a.id)
        self.dart.delete_workflow_instances(self.workflow.id)
        self.dart.delete_workflow(self.workflow.id)
        self.dart.delete_datastore(self.datastore.id)

    def test_workflow_failure(self):
        self.dart.manually_trigger_workflow(self.workflow.id)
        wf_instances = self.dart.await_workflow_completion(self.workflow.id)
        for wfi in wf_instances:
            self.assertEqual(wfi.data.state, WorkflowInstanceState.FAILED)


if __name__ == '__main__':
    unittest.main()
