import unittest

from dart.model.exception import DartValidationException
from dart.model.subscription import Subscription
from dart.model.subscription import SubscriptionData
from dart.schema.base import default_and_validate
from dart.schema.subscription import subscription_schema


class TestSubscriptionSchema(unittest.TestCase):

    def test_subscription_schema(self):
        start = 's3://my-test-bucket/impala/impala'
        end = 's3://my-test-bucket/impala/install'
        regex = '.*\\.rpm'
        state = None
        sub = Subscription(data=SubscriptionData('test-subscription', 'ABC123', start, end, regex, state=state))
        obj_before = sub.to_dict()
        sub = default_and_validate(sub, subscription_schema())
        # state should be defaulted to INACTIVE
        self.assertNotEqual(obj_before, sub.to_dict())

    def test_subscription_schema_invalid(self):
        with self.assertRaises(DartValidationException) as context:
            start = 's3://my-test-bucket/impala/impala'
            end = 's3://my-test-bucket/impala/install'
            regex = '.*\\.rpm'
            name = None
            sub = Subscription(data=SubscriptionData(name, 'ABC123', start, end, regex))
            # should fail because the name is missing
            default_and_validate(sub, subscription_schema())

        self.assertTrue(isinstance(context.exception, DartValidationException))


if __name__ == '__main__':
    unittest.main()
