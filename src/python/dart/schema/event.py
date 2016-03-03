from dart.model.event import EventState
from dart.schema.base import base_schema, tag_list_schema


def event_schema():
    return base_schema({
        'type': 'object',
        'properties': {
            'name': {'type': 'string', 'pattern': '^[a-zA-Z0-9_-]+$', 'maxLength': 50},
            'description': {'type': ['string', 'null']},
            'state': {'type': 'string', 'enum': EventState.all(), 'default': EventState.INACTIVE},
            'tags': tag_list_schema(),
        },
        'additionalProperties': False,
        'required': ['name']
    })
