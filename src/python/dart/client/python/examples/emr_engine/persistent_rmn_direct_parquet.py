from dart.client.python.dart_client import Dart
from dart.model.action import Action, ActionState
from dart.model.action import ActionData
from dart.model.dataset import FileFormat, Compression, RowFormat
from dart.model.datastore import Datastore, DatastoreData, DatastoreState
from dart.model.subscription import Subscription, SubscriptionState
from dart.model.subscription import SubscriptionData
from dart.model.trigger import TriggerData
from dart.model.trigger import Trigger
from dart.model.workflow import WorkflowState, Workflow
from dart.model.workflow import WorkflowData

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    subscription = dart.save_subscription(Subscription(data=SubscriptionData(
        name='rmn_direct_subscription_DW-3307',
        dataset_id='34HWJLF5N9',
        s3_path_start_prefix_inclusive='s3://example-bucket/prd/inbound/overlord/raw/rmndirect/2015/08/18/',
        on_success_email=['daniel@email.com'],
        on_failure_email=['daniel@email.com'],
    )))
    print 'created subscription: %s' % subscription.id

    print 'awaiting subscription generation...'
    subscription = dart.await_subscription_generation(subscription.id)
    assert subscription.data.state == SubscriptionState.ACTIVE
    print 'done.'

    datastore = dart.save_datastore(Datastore(
        data=DatastoreData(
            name='rmn_direct_adhoc',
            engine_name='emr_engine',
            state=DatastoreState.ACTIVE,
            args={
                # 'data_to_freespace_ratio': 0.10,
                'instance_count': 2,
            }
        )
    ))
    print 'created datastore: %s' % datastore.id

    actions = dart.save_actions(
        actions=[
            Action(data=ActionData('start_datastore', 'start_datastore')),
            Action(data=ActionData('load_dataset', 'load_dataset', args={
                'dataset_id': '34HWJLF5N9',
                's3_path_end_prefix_exclusive': 's3://example-bucket/prd/inbound/overlord/raw/rmndirect/2015/08/18/',
                'target_file_format': FileFormat.PARQUET,
                'target_row_format': RowFormat.NONE,
                'target_compression': Compression.SNAPPY,
            })),
        ],
        datastore_id=datastore.id
    )
    print 'created action: %s' % actions[0].id
    print 'created action: %s' % actions[1].id

    workflow = dart.save_workflow(
        workflow=Workflow(
            data=WorkflowData(
                name='rmn_direct_workflow_DW-3307',
                datastore_id=datastore.id,
                state=WorkflowState.ACTIVE,
                on_failure_email=['daniel@email.com'],
                on_success_email=['daniel@email.com'],
                on_started_email=['daniel@email.com'],
            )
        ),
        datastore_id=datastore.id
    )
    print 'created workflow: %s' % workflow.id

    wf_actions = dart.save_actions(
        actions=[
            Action(data=ActionData('consume_subscription', 'consume_subscription', state=ActionState.TEMPLATE, args={
                'subscription_id': subscription.id,
                'target_file_format': FileFormat.PARQUET,
                'target_row_format': RowFormat.NONE,
                'target_compression': Compression.SNAPPY,
            })),
        ],
        workflow_id=workflow.id
    )
    print 'created workflow action: %s' % wf_actions[0].id

    trigger = dart.save_trigger(Trigger(data=TriggerData(
        name='rmn_direct_trigger_DW-3307',
        trigger_type_name='subscription_batch',
        workflow_ids=[workflow.id],
        args={
            'subscription_id': subscription.id,
            'unconsumed_data_size_in_bytes': 16000000
        }
    )))
    print 'created trigger: %s' % trigger.id
