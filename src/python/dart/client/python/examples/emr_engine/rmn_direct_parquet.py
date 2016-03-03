from dart.client.python.dart_client import Dart
from dart.model.action import Action, ActionState
from dart.model.action import ActionData
from dart.model.dataset import Column, DatasetData, Dataset, DataFormat, FileFormat, DataType, Compression, RowFormat, \
    LoadType
from dart.model.datastore import Datastore, DatastoreData, DatastoreState
from dart.model.subscription import Subscription, SubscriptionState
from dart.model.subscription import SubscriptionData
from dart.model.trigger import TriggerData, Trigger, TriggerState
from dart.model.workflow import Workflow, WorkflowData, WorkflowState

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    dataset = dart.save_dataset(Dataset(data=(DatasetData(
        name='rmn_direct_v02',
        table_name='rmn_direct',
        location='s3://example-bucket/prd/inbound/overlord/raw/rmndirect',
        load_type=LoadType.MERGE,
        data_format=DataFormat(
            file_format=FileFormat.TEXTFILE,
            row_format=RowFormat.JSON,
        ),
        compression=Compression.GZIP,
        partitions=[
            Column('year', DataType.STRING),
            Column('month', DataType.STRING),
            Column('day', DataType.STRING),
        ],
        columns=[
            Column('host', DataType.STRING, path='metadata.host'),
            Column('referrer', DataType.STRING, path='owen.context.referrer'),
            Column('userAgent', DataType.STRING, path='owen.context.userAgent'),
            Column('ipAddress', DataType.STRING, path='owen.context.ipAddress'),
            Column('osVersion', DataType.STRING, path='owen.context.osVersion'),
            Column('osFamily', DataType.STRING, path='owen.context.osFamily'),
            Column('osName', DataType.STRING, path='owen.context.osName'),
            Column('browserFamily', DataType.STRING, path='owen.context.browserFamily'),
            Column('browserVersion', DataType.STRING, path='owen.context.browserVersion'),
            Column('latitude', DataType.STRING, path='owen.context.latitude'),
            Column('longitude', DataType.STRING, path='owen.context.longitude'),
            Column('dma', DataType.STRING, path='owen.context.dma'),
            Column('environment', DataType.STRING, path='owen.context.environment'),
            Column('campaign', DataType.STRING, path='owen.context.marketing.campaign'),
            Column('channel', DataType.STRING, path='owen.context.marketing.channel'),
            Column('content', DataType.STRING, path='owen.context.marketing.content'),
            Column('medium', DataType.STRING, path='owen.context.marketing.medium'),
            Column('source', DataType.STRING, path='owen.context.marketing.source'),
            Column('term', DataType.STRING, path='owen.context.marketing.term'),
            Column('sku_0', DataType.STRING, path='owen.context.custom.basket.items[0].sku'),
            Column('listPrice_0', DataType.FLOAT, path='owen.context.custom.basket.items[0].listPrice'),
            Column('quantity_0', DataType.INT, path='owen.context.custom.basket.items[0].quantity'),
            Column('items', DataType.STRING, path='owen.context.custom.basket.items'),
            Column('orderDiscountAmount', DataType.STRING, path='owen.context.custom.basket.orderDiscountAmount'),
            Column('orderId', DataType.STRING, path='owen.context.custom.basket.orderId'),
            Column('inventoryUuid_0', DataType.STRING, path='owen.context.inventory[0].inventoryUuid'),
            Column('inventoryType_0', DataType.STRING, path='owen.context.inventory[0].inventoryType'),
            Column('outclickUuid_0', DataType.STRING, path='owen.context.inventory[0].outclickUuid'),
            Column('key', DataType.STRING, path='schema.key'),
            Column('version', DataType.STRING, path='schema.version'),
            Column('eventInstanceUuid', DataType.STRING, path='owen.event.eventInstanceUuid'),
            Column('eventTimestamp', DataType.STRING, path='owen.event.eventTimestamp'),
            Column('eventPlatform', DataType.STRING, path='owen.event.eventPlatform'),
            Column('eventCategory', DataType.STRING, path='owen.event.eventCategory'),
            Column('eventAction', DataType.STRING, path='owen.event.eventAction'),
            Column('eventName', DataType.STRING, path='owen.event.eventName'),
            Column('eventTarget', DataType.STRING, path='owen.event.eventTarget'),
            Column('eventVersion', DataType.STRING, path='owen.event.eventVersion'),
        ],
    ))))
    print 'created dataset: %s' % dataset.id

    subscription = dart.save_subscription(Subscription(data=SubscriptionData(
        name='rmn_direct_subscription_DW-3307',
        dataset_id=dataset.id,
        s3_path_start_prefix_inclusive='s3://example-bucket/prd/inbound/overlord/raw/rmndirect/2015/09/04/',
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
            name='rmn_direct_adhoc_DW-3307',
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
                'dataset_id': dataset.id,
                's3_path_end_prefix_exclusive': 's3://example-bucket/prd/inbound/overlord/raw/rmndirect/2015/09/04/',
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
            'unconsumed_data_size_in_bytes': 1000000
        },
        state=TriggerState.ACTIVE
    )))
    print 'created trigger: %s' % trigger.id
