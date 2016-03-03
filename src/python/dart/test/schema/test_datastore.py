import unittest

from dart.model.datastore import Datastore, DatastoreData
from dart.model.exception import DartValidationException
from dart.schema.base import default_and_validate
from dart.schema.datastore import datastore_schema
from dart.util.secrets import purge_secrets


class TestDatastoreSchema(unittest.TestCase):

    options_json_schema = {
        'type': 'object',
        'properties': {
            'release_label': {'type': 'string', 'pattern': '^emr-[0-9].[0-9].[0-9]+$', 'default': 'emr-4.2.0', 'description': 'desired EMR release label'},
            'instance_type': {'readonly': True, 'type': ['string', 'null'], 'default': 'm3.2xlarge', 'description': 'The ec2 instance type of master/core nodes'},
            'instance_count': {'type': ['integer', 'null'], 'default': None, 'minimum': 1, 'maximum': 30, 'description': 'The total number of nodes in this cluster (overrides data_to_freespace_ratio)'},
            'data_to_freespace_ratio': {'type': ['number', 'null'], 'default': 0.5, 'minimum': 0.0, 'maximum': 1.0, 'description': 'Desired ratio of HDFS data/free-space'},
            'dry_run': {'type': ['boolean', 'null'], 'default': False, 'description': 'write extra_data to actions, but do not actually run'},
            'secret': {'type': ['string', 'null'], 'x-dart-secret': True},
        },
        'additionalProperties': False,
        'required': ['release_label'],
    }

    def test_datastore_schema(self):
        dst = Datastore(data=DatastoreData('test-datastore', 'fake_engine', args={'data_to_freespace_ratio': 0, 'secret': 'hi'}))
        obj_before = dst.to_dict()
        schema = datastore_schema(self.options_json_schema)
        dst = default_and_validate(dst, schema)
        obj_after = dst.to_dict()
        self.assertNotEqual(obj_before, obj_after)

        self.assertEqual(obj_after['data']['args']['secret'], 'hi')
        secrets = {}
        purge_secrets(obj_after, schema, secrets)
        self.assertEqual(obj_after['data']['args'].get('secret'), None)
        self.assertEqual(secrets, {'secret': 'hi'})

    def test_datastore_schema_invalid(self):
        with self.assertRaises(DartValidationException) as context:
            dst = Datastore(data=DatastoreData('test-datastore', 'fake_engine', args={'data_to_freespace_ratio_yo': 0}))
            default_and_validate(dst, datastore_schema(self.options_json_schema))

        self.assertTrue(isinstance(context.exception, DartValidationException))


if __name__ == '__main__':
    unittest.main()
