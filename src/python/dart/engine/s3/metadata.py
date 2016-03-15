import logging

from dart.model.action import ActionType

_logger = logging.getLogger(__name__)


class S3ActionTypes(object):
    copy = ActionType(name='copy',
                      description='Accomplishes s3 source to s3 destination copy, giving the destination bucket owner full control',
                      params_json_schema={
                          'type': 'object',
                          'properties': {
                              'from_path': {'type': 'string', 'pattern': '^s3://.+$',
                                            'description': 'The source s3 file path'},
                              'to_path': {'type': 'string', 'pattern': '^s3://.+$',
                                          'description': 'The destination s3 file path'},
                              'recursive': {'type': ['boolean', 'null'], 'default': True,
                                            'description': 'Performs recursive copy of source to destination'},
                              'additionalProperties': False,
                              'required': ['from_path', 'to_path']
                          }
                      })
