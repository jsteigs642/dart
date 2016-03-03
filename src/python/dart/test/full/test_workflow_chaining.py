import unittest

from dart.client.python.dart_client import Dart
from dart.engine.no_op.metadata import NoOpActionTypes
from dart.model.action import ActionData, Action, ActionState
from dart.model.datastore import Datastore, DatastoreData, DatastoreState
from dart.model.trigger import Trigger, TriggerState
from dart.model.trigger import TriggerData
from dart.model.workflow import WorkflowData, WorkflowState, WorkflowInstanceState
from dart.model.workflow import Workflow


class TestWorkflowChaining(unittest.TestCase):
    def setUp(self):
        dart = Dart(host='localhost', port=5000)
        """ :type dart: dart.client.python.dart_client.Dart """
        self.dart = dart

        dst_args = {'action_sleep_time_in_seconds': 0}
        dst0 = Datastore(data=DatastoreData('test-datastore0', 'no_op_engine', args=dst_args, state=DatastoreState.TEMPLATE))
        self.datastore0 = self.dart.save_datastore(dst0)
        dst1 = Datastore(data=DatastoreData('test-datastore1', 'no_op_engine', args=dst_args, state=DatastoreState.TEMPLATE))
        self.datastore1 = self.dart.save_datastore(dst1)

        wf0 = Workflow(data=WorkflowData('test-workflow0', self.datastore0.id, state=WorkflowState.ACTIVE))
        self.workflow0 = self.dart.save_workflow(wf0, self.datastore0.id)
        wf1 = Workflow(data=WorkflowData('test-workflow1', self.datastore1.id, state=WorkflowState.ACTIVE))
        self.workflow1 = self.dart.save_workflow(wf1, self.datastore1.id)

        a00 = Action(data=ActionData(NoOpActionTypes.action_that_succeeds.name, NoOpActionTypes.action_that_succeeds.name, state=ActionState.TEMPLATE))
        a01 = Action(data=ActionData(NoOpActionTypes.action_that_succeeds.name, NoOpActionTypes.action_that_succeeds.name, state=ActionState.TEMPLATE))
        self.action00, self.action01 = self.dart.save_actions([a00, a01], workflow_id=self.workflow0.id)

        a10 = Action(data=ActionData(NoOpActionTypes.action_that_succeeds.name, NoOpActionTypes.action_that_succeeds.name, state=ActionState.TEMPLATE))
        a11 = Action(data=ActionData(NoOpActionTypes.action_that_succeeds.name, NoOpActionTypes.action_that_succeeds.name, state=ActionState.TEMPLATE))
        self.action10, self.action11 = self.dart.save_actions([a10, a11], workflow_id=self.workflow1.id)

        tr_args = {'completed_workflow_id': self.workflow0.id}
        tr = Trigger(data=TriggerData('test-trigger', 'workflow_completion', [self.workflow1.id], tr_args, TriggerState.ACTIVE))
        self.trigger = self.dart.save_trigger(tr)

    def tearDown(self):
        for a in self.dart.get_actions(workflow_id=self.workflow0.id):
            self.dart.delete_action(a.id)
        for a in self.dart.get_actions(workflow_id=self.workflow1.id):
            self.dart.delete_action(a.id)
        for wfi in self.dart.get_workflow_instances(self.workflow0.id):
            self.dart.delete_datastore(wfi.data.datastore_id)
        for wfi in self.dart.get_workflow_instances(self.workflow1.id):
            self.dart.delete_datastore(wfi.data.datastore_id)
        self.dart.delete_trigger(self.trigger.id)
        self.dart.delete_workflow_instances(self.workflow0.id)
        self.dart.delete_workflow_instances(self.workflow1.id)
        self.dart.delete_workflow(self.workflow0.id)
        self.dart.delete_workflow(self.workflow1.id)
        self.dart.delete_datastore(self.datastore0.id)
        self.dart.delete_datastore(self.datastore1.id)

    def test_workflow_chaining(self):
        self.dart.manually_trigger_workflow(self.workflow0.id)
        wf_instances = self.dart.await_workflow_completion(self.workflow1.id)
        for wfi in wf_instances:
            self.assertEqual(wfi.data.state, WorkflowInstanceState.COMPLETED)


if __name__ == '__main__':
    unittest.main()
