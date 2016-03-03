import argparse
import base64
import copy
import logging

import boto3

from dart.config.config import dart_root_relative_path

_logger = logging.getLogger(__name__)


class PutStack(object):
    def __init__(self, mode, stack_name, stack_version, dart_config=None, dart_stack_name_override=None):
        assert mode in ['create', 'update']
        self.mode = mode
        self.stack_name = stack_name
        self.stack_version = stack_version
        self.dart_config = dart_config
        self.dart_stack_name_override = dart_stack_name_override
        self.dart_stack_name = self.dart_stack_name_override
        self.set_dart_stack_name()

    def put_stack(self, template_body_replacements=None):
        self.set_dart_stack_name()
        _logger.info('putting stack: %s' % self.dart_stack_name)

        cfn = self.dart_config['cloudformation_stacks'][self.stack_name]
        boto_args = copy.deepcopy(cfn.get('boto_args', {}))

        with open(dart_root_relative_path(cfn['template_body_path'])) as t:
            template_body = t.read()
            if cfn.get('user_data_script'):
                with open(dart_root_relative_path(cfn['user_data_script'])) as u:
                    template_body = template_body.replace('{USER_DATA_CONTENTS}', base64.b64encode(u.read()))
            if template_body_replacements:
                for k, v in template_body_replacements.iteritems():
                    template_body = template_body.replace(k, v)
            boto_args['TemplateBody'] = template_body

        boto_args['StackName'] = self.dart_stack_name

        op = 'create_stack'
        if self.mode == 'update':
            op = 'update_stack'
            if 'Tags' in boto_args:
                del boto_args['Tags']

        client = boto3.client('cloudformation')
        create_or_update = getattr(client, op)
        create_or_update(**boto_args)
        return self.dart_stack_name

    def set_dart_stack_name(self):
        if self.dart_config and not self.dart_stack_name:
            values = (self.dart_config['dart']['env_name'], self.stack_name, self.stack_version)
            self.dart_stack_name = 'dart-%s-%s-%s' % values


class PutStackTool(object):
    def __init__(self, put_stack_instance):
        assert isinstance(put_stack_instance, PutStack)
        self.put_stack_instance = put_stack_instance

    def run(self):
        self.put_stack_instance.put_stack()


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', action='store', dest='mode', choices=['create', 'update'])
    parser.add_argument('-n', '--stack-name', action='store', dest='stack_name', help='from your config file')
    parser.add_argument('-v', '--stack-version', action='store', dest='stack_version')
    parser.add_argument('-d', '--dart-stack-name-override', action='store', dest='dart_stack_name_override')
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    PutStackTool(
        PutStack(
            args.mode,
            args.stack_name,
            args.stack_version,
            None,
            args.dart_stack_name_override
        )
    ).run()
