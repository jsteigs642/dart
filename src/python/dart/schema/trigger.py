from dart.model.trigger import TriggerState
from dart.schema.base import base_schema, tag_list_schema


def trigger_type_schema():
    return {
        'type': 'object',
        'readonly': True,
        'properties': {
            'name': {'type': 'string'},
            'description': {'type': 'string'},
            'params_json_schema': {'type': 'object'},
        }
    }


def trigger_schema(trigger_params_schema):
    return base_schema({
        'type': 'object',
        'properties': {
            'name': {'type': 'string', 'pattern': '^[a-zA-Z0-9_-]+$', 'maxLength': 50},
            'trigger_type_name': {'type': 'string', 'readonly': True},
            'workflow_ids': {
                'x-schema-form': {'type': 'tabarray', 'title': "{{ value || 'workflow_id ' + $index }}"},
                'type': 'array',
                'default': [],
                'items': {'type': 'string'},
                'minItems': 0,
            },
            'tags': tag_list_schema(),
            'args': trigger_params_schema or {'type': 'null'},
            'state': {'type': 'string', 'enum': TriggerState.all(), 'default': TriggerState.INACTIVE},
            'extra_data': {'type': ['object', 'null'], 'default': None, 'readonly': True},
        },
        'additionalProperties': False,
        'required': ['name', 'trigger_type_name', 'args'] if trigger_params_schema else ['name', 'trigger_type_name']
    })
