import os
import unittest

from dart.client.python.dart_client import Dart
from dart.model.dataset import DatasetData, Column, Dataset, DataType, DataFormat, FileFormat, RowFormat
from dart.model.subscription import Subscription, SubscriptionState
from dart.model.subscription import SubscriptionData

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
# class TestLostSubscriptionContainer(unittest.TestCase):
class TestLostSubscriptionContainer(object):
    def setUp(self):
        dart = Dart(host='localhost', port=5000)
        """ :type dart: dart.client.python.dart_client.Dart """
        self.dart = dart

        cs = [Column('c1', DataType.VARCHAR, 50), Column('c2', DataType.BIGINT)]
        df = DataFormat(FileFormat.TEXTFILE, RowFormat.DELIMITED)
        dataset_data = DatasetData('test-dataset', 'test_dataset_table', 's3://' + os.environ['DART_TEST_BUCKET'] + '/impala', df, cs)
        self.dataset = self.dart.save_dataset(Dataset(data=dataset_data))

        start = 's3://' + os.environ['DART_TEST_BUCKET'] + '/impala/impala'
        end = 's3://' + os.environ['DART_TEST_BUCKET'] + '/impala/install'
        regex = '.*\\.rpm'
        ds = Subscription(data=SubscriptionData('test-subscription', self.dataset.id, start, end, regex))
        self.subscription = self.dart.save_subscription(ds)

    def tearDown(self):
        self.dart.delete_dataset(self.dataset.id)
        self.dart.delete_subscription(self.subscription.id)

    def test_lost_subscription_container(self):
        subscription = self.dart.await_subscription_generation(self.subscription.id)
        self.assertEqual(subscription.data.state, SubscriptionState.FAILED)



if __name__ == '__main__':
    unittest.main()
