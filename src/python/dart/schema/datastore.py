from dart.model.datastore import DatastoreState
from dart.schema.base import base_schema, tag_list_schema


def datastore_schema(engine_data_options_schema):
    return base_schema({
        'type': 'object',
        'properties': {
            'name': {'type': 'string', 'pattern': '^[a-zA-Z0-9_-]+$', 'maxLength': 50},
            'engine_name': {'type': 'string', 'pattern': '^[a-zA-Z0-9_]+$', 'readonly': True},
            'workflow_datastore_id': {'type': ['string', 'null'], 'default': None, 'readonly': True},
            'host': {'type': ['string', 'null'], 'default': None, 'readonly': True},
            'port': {'type': ['integer', 'null'], 'default': None, 'readonly': True},
            'connection_url': {'type': ['string', 'null'], 'default': None, 'readonly': True},
            's3_artifacts_path': {'type': ['string', 'null'], 'default': None, 'readonly': True},
            's3_logs_path': {'type': ['string', 'null'], 'default': None, 'readonly': True},
            'tags': tag_list_schema(),
            'state': {'type': 'string', 'enum': DatastoreState.all(), 'default': DatastoreState.INACTIVE},
            'concurrency': {'type': 'integer', 'default': 1, 'minimum': 1, 'maximum': 10},
            'args': engine_data_options_schema or {'type': 'null'},
            'extra_data': {'type': ['object', 'null'], 'default': None, 'readonly': True},
        },
        'additionalProperties': False,
        'required': ['name', 'engine_name', 'args'] if engine_data_options_schema else ['name', 'engine_name']
    })
