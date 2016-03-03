import json
import os
import re
import time

import boto3
import sys
import objectpath
import yaml
from dart.service.secrets import Secrets

from dart.util.s3 import get_bucket_name, get_key_name
from dart.util.shell import call


def print_config(config_path):
    config = configuration(config_path)
    print yaml.dump(config, default_flow_style=False)


def print_config_value_from_stdin(object_path):
    config = yaml.load(''.join(sys.stdin.readlines()))
    tree = objectpath.Tree(config)
    print tree.execute(object_path)


def configuration(config_path, suppress_decryption=False):
    def env(loader, node):
        pattern = re.compile(r'\$\{(.+?)\}')
        raw_value = loader.construct_scalar(node)
        result = str(raw_value)
        try:
            for env_var in re.findall(pattern, raw_value):
                value = os.environ.get(env_var)
                if not value:
                    raise Exception('empty environment variable found (%s) in config file: %s' % (env_var, config_path))
                result = result.replace('${%s}' % env_var, os.environ[env_var])
            return str(result)
        except:
            raise Exception('could not interpolate !env value (%s) in config file: %s' % (raw_value, config_path))
    yaml.add_constructor('!env', env)

    raw_file_data = _get_raw_config_data(config_path)

    # this first load has a pass-through decryption function, which allows us to find the secrets_s3_path
    def decrypt(loader, node):
        return '!decrypt %s' % loader.construct_scalar(node)
    yaml.add_constructor('!decrypt', decrypt)

    config = yaml.load(raw_file_data)
    if not suppress_decryption:
        secrets_config = get_secrets_config(config)
        secrets_service = Secrets(secrets_config['kms_key_arn'], secrets_config['secrets_s3_path'])

        # now we can instantiate the secrets service to perform any necessary decryption
        def decrypt(loader, node):
            return secrets_service.get(loader.construct_scalar(node))
        yaml.add_constructor('!decrypt', decrypt)

    return yaml.load(raw_file_data)


def dart_root_relative_path(*args):
    this_dir = os.path.dirname(os.path.abspath(__file__))
    arg_list = list(args) if args else []
    return os.path.join(*([this_dir] + ['..', '..', '..', '..'] + arg_list))


def get_secrets_config(config):
    for e in config['dart']['app_context']:
        if e['name'] == 'secrets':
            return e['options']
    raise Exception('could not find "secrets" in the config app_context')


def _get_raw_config_data(config_path):
    if config_path.startswith('s3://'):
        response = boto3.client('s3').get_object(Bucket=get_bucket_name(config_path), Key=get_key_name(config_path))
        return response["Body"].read()

    # use as is for absolute paths, else make it relative to the project root
    path = config_path if config_path.startswith('/') else dart_root_relative_path(config_path)
    with open(path) as f:
        return f.read()


def set_dart_environment_variables(ecs_agent_data_path, container_id=None):
    ecs_data_path = ecs_agent_data_path or ''
    if not os.path.isfile(ecs_data_path):
        os.environ['DART_INSTANCE_ID'] = 'local-instance'
        os.environ['DART_CONTAINER_ID'] = 'local-container'
        os.environ['DART_ECS_CLUSTER'] = 'local-cluster'
        os.environ['DART_ECS_CONTAINER_INSTANCE_ARN'] = 'local-containerinstancearn'
        os.environ['DART_ECS_FAMILY'] = 'local-family'
        os.environ['DART_ECS_TASK_ARN'] = 'local-task'
        return

    cmd = """ cat /proc/self/cgroup | grep "cpu:/" | sed 's/\([0-9]\):cpu:\/docker\///g' """
    container_id = container_id if container_id else call(cmd).strip()

    # ECS sometimes takes a bit to write the state to the ecs_agent_data file, so we will pause for a moment
    time.sleep(10)

    with open(ecs_data_path) as f:
        data = json.load(f)['Data']
        task_arn = data['TaskEngine']['IdToTask'][container_id]
        os.environ['DART_ECS_TASK_ARN'] = task_arn
        os.environ['DART_INSTANCE_ID'] = data['EC2InstanceID']
        os.environ['DART_CONTAINER_ID'] = container_id
        os.environ['DART_ECS_CLUSTER'] = data['Cluster']
        os.environ['DART_ECS_CONTAINER_INSTANCE_ARN'] = data['ContainerInstanceArn']

        for task in data['TaskEngine']['Tasks']:
            if task['Arn'] == task_arn:
                os.environ['DART_ECS_FAMILY'] = task['Family']

