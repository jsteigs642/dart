import unittest
import time

from dart.client.python.dart_client import Dart
from dart.engine.no_op.metadata import NoOpActionTypes
from dart.model.exception import DartRequestException
from dart.model.action import ActionData, Action, ActionState
from dart.model.datastore import Datastore, DatastoreData, DatastoreState
from dart.model.workflow import WorkflowData, WorkflowState, WorkflowInstanceState
from dart.model.workflow import Workflow

"""
-------------------------------- IMPORTANT NOTE --------------------------------

This test requires at least 2 running instances of engine-worker-local to pass!!!

---------------------------------------------------------------------------------
"""

# uncomment the line below to use the test.  unittest.skip() prevents pycharm from giving a green bar :-P
# class TestConcurrency(unittest.TestCase):
class TestConcurrency(object):
    def setUp(self):
        dart = Dart(host='localhost', port=5000)
        """ :type dart: dart.client.python.dart_client.Dart """
        self.dart = dart

        dst_args = {'action_sleep_time_in_seconds': 5}
        dst = Datastore(data=DatastoreData('test-datastore', 'no_op_engine', args=dst_args, state=DatastoreState.TEMPLATE))
        self.datastore = self.dart.save_datastore(dst)

        wf = Workflow(data=WorkflowData('test-workflow', self.datastore.id, state=WorkflowState.ACTIVE, concurrency=2))
        self.workflow = self.dart.save_workflow(wf, self.datastore.id)

        a = Action(data=ActionData(NoOpActionTypes.action_that_succeeds.name, NoOpActionTypes.action_that_succeeds.name, state=ActionState.TEMPLATE))
        self.action = self.dart.save_actions([a], workflow_id=self.workflow.id)[0]

    def tearDown(self):
        for a in self.dart.get_actions(workflow_id=self.workflow.id):
            self.dart.delete_action(a.id)
        for wfi in self.dart.get_workflow_instances(self.workflow.id):
            self.dart.delete_datastore(wfi.data.datastore_id)
        self.dart.delete_workflow_instances(self.workflow.id)
        self.dart.delete_workflow(self.workflow.id)
        self.dart.delete_datastore(self.datastore.id)

    def test_concurrency(self):
        self.dart.manually_trigger_workflow(self.workflow.id)
        time.sleep(1)
        self.dart.manually_trigger_workflow(self.workflow.id)
        time.sleep(1)

        exception_handled = False
        try:
            self.dart.manually_trigger_workflow(self.workflow.id)
        except DartRequestException as e:
            self.assertEqual(e.response.status_code, 400)
            exception_handled = True
        if not exception_handled:
            self.fail('3rd workflow trigger should have failed')

        wf_instances = self.dart.await_workflow_completion(self.workflow.id, num_instances=2)
        for wfi in wf_instances:
            self.assertEqual(wfi.data.state, WorkflowInstanceState.COMPLETED)

        self.dart.delete_action(self.action.id)
        actions = self.dart.get_actions(workflow_id=self.workflow.id)
        self.assertEqual(len(actions), 2)
        a0, a1 = actions
        actions_ran_concurrently = a0.data.start_time < a1.data.end_time and a1.data.start_time < a0.data.end_time
        self.assertTrue(actions_ran_concurrently, 'Did you remember to start an extra engine worker?')


if __name__ == '__main__':
    unittest.main()
