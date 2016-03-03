import argparse
import logging

import boto3

_logger = logging.getLogger(__name__)


class PartialEnvironmentDestroyTool(object):
    """
    This tool shuts down a previously created partial environment
    """
    def __init__(self, environment_name):
        self.environment_name = environment_name

    def run(self):
        _logger.info('deleting stacks...')
        cfn_client = boto3.client('cloudformation')
        cfn_client.delete_stack(StackName='dart-%s-subscription-worker-v1' % self.environment_name)
        cfn_client.delete_stack(StackName='dart-%s-trigger-worker-v1' % self.environment_name)
        cfn_client.delete_stack(StackName='dart-%s-engine-worker-v1' % self.environment_name)
        cfn_client.delete_stack(StackName='dart-%s-web-internal-v1' % self.environment_name)
        cfn_client.delete_stack(StackName='dart-%s-web-v1' % self.environment_name)
        cfn_client.delete_stack(StackName='dart-%s-engine-taskrunner-v1' % self.environment_name)
        cfn_client.delete_stack(StackName='dart-%s-elb-internal-v1' % self.environment_name)
        cfn_client.delete_stack(StackName='dart-%s-elb-v1' % self.environment_name)
        cfn_client.delete_stack(StackName='dart-%s-rds-v1' % self.environment_name)
        cfn_client.delete_stack(StackName='dart-%s-events-v1' % self.environment_name)
        _logger.info('done')


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--environment-name', action='store', dest='environment_name', required=True)
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    PartialEnvironmentDestroyTool(args.environment_name).run()
