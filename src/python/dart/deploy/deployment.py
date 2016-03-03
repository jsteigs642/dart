import logging
import time

import boto3

_logger = logging.getLogger(__name__)


class DeploymentTool(object):

    def _get_dart_host(self, config):
        elb_params = config['cloudformation_stacks']['elb']['boto_args']['Parameters']
        rs_name_param = self._get_element(elb_params, 'ParameterKey', 'RecordSetName')
        dart_host = rs_name_param['ParameterValue']
        return dart_host

    @staticmethod
    def _wait_for_stack_completion_and_get_outputs(stack_name, assert_outputs_len=None):
        _logger.info('waiting for stack to finish: %s' % stack_name)
        response = None
        while True:
            response = boto3.client('cloudformation').describe_stacks(StackName=stack_name)
            status = response['Stacks'][0]['StackStatus']
            if status == 'CREATE_IN_PROGRESS':
                time.sleep(7)
                continue
            if status in ['CREATE_COMPLETE', 'UPDATE_COMPLETE']:
                break
            raise Exception('stack creation for %s did not complete successfully: %s' % (stack_name, status))

        _logger.info('done')
        if assert_outputs_len:
            outputs = response['Stacks'][0]['Outputs']
            assert len(outputs) == assert_outputs_len
            return outputs
        return None

    @staticmethod
    def _wait_for_web_app(load_balancer_name):
        while True:
            response = boto3.client('elb').describe_instance_health(LoadBalancerName=load_balancer_name)
            for instance_state in response.get('InstanceStates', []):
                if instance_state['State'] == 'InService':
                    _logger.info('done')
                    return
            time.sleep(7)

    @staticmethod
    def _get_element(l, k, v):
        for e in l:
            if e[k] == v:
                return e
        raise Exception('element with %s: %s not found' % (k, v))
