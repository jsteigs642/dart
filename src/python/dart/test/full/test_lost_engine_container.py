import unittest

from dart.client.python.dart_client import Dart
from dart.engine.no_op.metadata import NoOpActionTypes
from dart.model.action import ActionData, Action, ActionState
from dart.model.datastore import Datastore, DatastoreData, DatastoreState

"""
-------------------------------- IMPORTANT NOTE --------------------------------

This test requires:
    - setting this worker config entry:
          dart:
              app_context:
                  message_service:
                      options:
                          ecs_task_status_override: STOPPED
    - putting a breakpoint in each worker in order to kill it in action
    - starting the worker back up to detect the lost container

---------------------------------------------------------------------------------
"""

# uncomment the line below to use the test.  unittest.skip() prevents pycharm from giving a green bar :-P
# class TestLostEngineContainer(unittest.TestCase):
class TestLostEngineContainer(object):
    def setUp(self):
        dart = Dart(host='localhost', port=5000)
        """ :type dart: dart.client.python.dart_client.Dart """
        self.dart = dart

        dst_args = {'action_sleep_time_in_seconds': 0}
        dst = Datastore(data=DatastoreData('test-datastore', 'no_op_engine', args=dst_args, state=DatastoreState.ACTIVE))
        self.datastore = self.dart.save_datastore(dst)

    def tearDown(self):
        self.dart.delete_datastore(self.datastore.id)

    def test_lost_engine_container(self):
        a = Action(data=ActionData(NoOpActionTypes.action_that_succeeds.name, NoOpActionTypes.action_that_succeeds.name, state=ActionState.HAS_NEVER_RUN))
        action = self.dart.save_actions([a], datastore_id=self.datastore.id)[0]

        action = self.dart.await_action_completion(action.id)
        self.assertEqual(action.data.state, ActionState.FAILED)

        datastore = self.dart.get_datastore(self.datastore.id)
        self.assertEqual(datastore.data.state, DatastoreState.INACTIVE)

        self.dart.delete_action(action.id)


if __name__ == '__main__':
    unittest.main()
