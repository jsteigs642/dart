import unittest

from dart.engine.no_op.metadata import NoOpActionTypes
from dart.model.action import ActionData, Action
from dart.model.exception import DartValidationException
from dart.schema.action import action_schema
from dart.schema.base import default_and_validate


class TestActionSchema(unittest.TestCase):

    def test_action_schema(self):
        last_in_workflow = None
        a = Action(data=ActionData('copy_hdfs_to_s3', 'copy_hdfs_to_s3', {
            'source_hdfs_path': 'hdfs:///user/hive/warehouse/dtest4',
            'destination_s3_path': 's3://fake-bucket/dart_testing',
        }, engine_name='no_op_engine', last_in_workflow=last_in_workflow))
        obj_before = a.to_dict()
        obj_after = default_and_validate(a, action_schema(NoOpActionTypes.copy_hdfs_to_s3_action.params_json_schema)).to_dict()
        # many fields should have been defaulted, making these unequal
        self.assertNotEqual(obj_before, obj_after)

    def test_action_schema_invalid(self):
        with self.assertRaises(DartValidationException) as context:
            a = Action(data=ActionData('copy_hdfs_to_s3', 'copy_hdfs_to_s3', {
                'source_hdfs_path': 'hdfs:///user/hive/warehouse/dtest4',
                # 'destination_s3_path': 's3://fake-bucket/dart_testing',
            }, engine_name='no_op_engine'))
            # should fail because destination_s3_path is required
            default_and_validate(a, action_schema(NoOpActionTypes.copy_hdfs_to_s3_action.params_json_schema)).to_dict()

        self.assertTrue(isinstance(context.exception, DartValidationException))


if __name__ == '__main__':
    unittest.main()
