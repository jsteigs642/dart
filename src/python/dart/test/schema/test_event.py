import unittest

from dart.model.event import Event
from dart.model.event import EventData
from dart.model.exception import DartValidationException
from dart.schema.base import default_and_validate
from dart.schema.event import event_schema


class TestEventSchema(unittest.TestCase):

    def test_event_schema(self):
        state = None
        e = Event(data=EventData('test-event', state=state))
        obj_before = e.to_dict()
        e = default_and_validate(e, event_schema())
        # state should be defaulted to INACTIVE
        self.assertNotEqual(obj_before, e.to_dict())

    def test_event_schema_invalid(self):
        with self.assertRaises(DartValidationException) as context:
            name = None
            e = Event(data=EventData(name))
            # should fail because the name is missing
            default_and_validate(e, event_schema())

        self.assertTrue(isinstance(context.exception, DartValidationException))


if __name__ == '__main__':
    unittest.main()
