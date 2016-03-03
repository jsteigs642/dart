from dart.client.python.dart_client import Dart
from dart.model.action import Action, ActionState
from dart.model.action import ActionData
from dart.model.dataset import Column, DatasetData, Dataset, DataFormat, FileFormat, RowFormat, DataType, Compression, \
    LoadType
from dart.model.datastore import Datastore, DatastoreData, DatastoreState
from dart.model.subscription import Subscription, SubscriptionState
from dart.model.subscription import SubscriptionData
from dart.model.trigger import Trigger
from dart.model.trigger import TriggerData
from dart.model.workflow import Workflow, WorkflowState
from dart.model.workflow import WorkflowData

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    dataset = dart.save_dataset(Dataset(data=(DatasetData(
        name='owen_eu_DW-3213_v3',
        table_name='owen_eu',
        location='s3://example-bucket/prd/inbound/overlord/eu-all-events',
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
            Column('referer', DataType.STRING, path='metadata.referer'),
            Column('userAgent', DataType.STRING, path='owen.context.userAgent'),
            Column('ipAddress', DataType.STRING, path='owen.context.ipAddress'),
            Column('session', DataType.STRING, path='owen.context.session'),
            Column('propertyName', DataType.STRING, path='owen.context.propertyName'),
            Column('pageName', DataType.STRING, path='owen.context.pageName'),
            Column('previousPageName', DataType.STRING, path='owen.context.previousPageName'),
            Column('viewInstanceUuid', DataType.STRING, path='owen.context.viewInstanceUuid'),
            Column('previousViewInstanceUuid', DataType.STRING, path='owen.context.previousViewInstanceUuid'),
            Column('pageType', DataType.STRING, path='owen.context.pageType'),
            Column('udid', DataType.STRING, path='owen.context.udid'),
            Column('advertiserUuid', DataType.STRING, path='owen.context.advertiserUuid'),
            Column('osFamily', DataType.STRING, path='owen.context.osFamily'),
            Column('latitude', DataType.STRING, path='owen.context.latitude'),
            Column('longitude', DataType.STRING, path='owen.context.longitude'),
            Column('userId', DataType.STRING, path='owen.context.custom.legacy.userId'),
            Column('geofenceId', DataType.STRING, path='owen.context.custom.legacy.geofenceId'),
            Column('userUuid', DataType.STRING, path='owen.context.userUuid'),
            Column('offerId', DataType.STRING, path='owen.context.inventory[0].inventoryUuid'),
            Column('inventorySource', DataType.STRING, path='owen.context.inventory[0].inventorySource'),
            Column('expirationDate', DataType.STRING, path='owen.context.inventory[0].expirationDate'),
            Column('position', DataType.STRING, path='owen.context.inventory[0].position'),
            Column('offerType', DataType.STRING, path='owen.context.inventory[0].inventoryType'),
            Column('eventInstanceUuid', DataType.STRING, path='owen.event.eventInstanceUuid'),
            Column('eventTimestamp', DataType.TIMESTAMP, path='owen.event.eventTimestamp', date_pattern="yyyy-MM-dd'T'HH:mm:ss'Z'"),
            Column('eventPlatform', DataType.STRING, path='owen.event.eventPlatform'),
            Column('eventCategory', DataType.STRING, path='owen.event.eventCategory'),
            Column('eventAction', DataType.STRING, path='owen.event.eventAction'),
            Column('eventName', DataType.STRING, path='owen.event.eventName'),
            Column('eventTarget', DataType.STRING, path='owen.event.eventTarget'),
            Column('eventVersion', DataType.STRING, path='owen.event.eventVersion'),
            Column('userQualifier', DataType.STRING, path='owen.context.userQualifier'),
            Column('outclickUuid', DataType.STRING, path='owen.context.inventory[0].outclickUuid'),
            Column('inventoryName', DataType.STRING, path='owen.context.inventory[0].inventoryName'),
            Column('enviroment', DataType.STRING, path='owen.context.environment'),
            Column('loggedInFlag', DataType.STRING, path='owen.context.loggedInFlag'),
            Column('eventPlatformVersion', DataType.STRING, path='owen.event.eventPlatformVersion'),
            Column('appForegroundFlag', DataType.BOOLEAN, path='owen.context.appForegroundFlag'),
            Column('bluetoothEnabledFlag', DataType.BOOLEAN, path='owen.context.bluetoothEnabledFlag'),
            Column('favoriteFlag', DataType.BOOLEAN, path='owen.context.favoriteFlag'),
            Column('locationEnabledFlag', DataType.BOOLEAN, path='owen.context.locationEnabledFlag'),
            Column('notificationEnabledFlag', DataType.BOOLEAN, path='owen.context.notificationEnabledFlag'),
            Column('personalizationFlag', DataType.BOOLEAN, path='owen.context.personalizationFlag'),
            Column('macAddress', DataType.STRING, path='owen.context.macAddress'),
            Column('osVersion', DataType.STRING, path='owen.context.osVersion'),
            Column('osName', DataType.STRING, path='owen.context.osName'),
            Column('browserFamily', DataType.STRING, path='owen.context.browserFamily'),
            Column('deviceCategory', DataType.STRING, path='owen.context.deviceCategory'),
            Column('deviceMake', DataType.STRING, path='owen.context.mobileDeviceMake'),
            Column('deviceModel', DataType.STRING, path='owen.context.mobileDeviceModel'),
            Column('connectionType', DataType.STRING, path='owen.context.connectionType'),
            Column('browserVersion', DataType.STRING, path='owen.context.browserVersion'),
            Column('city', DataType.STRING, path='owen.context.city'),
            Column('country', DataType.STRING, path='owen.context.country'),
            Column('region', DataType.STRING, path='owen.context.region'),
            Column('partialSearchTerm', DataType.STRING, path='owen.context.partialSearchTerm'),
            Column('outclickURL', DataType.STRING, path='owen.context.inventory[0].outRedirectUrl'),
            Column('clickLocation', DataType.STRING, path='owen.context.inventory[0].clickLocation'),
            Column('inventoryChannel', DataType.STRING, path='owen.context.inventory[0].inventoryChannel'),
            Column('brand', DataType.STRING, path='owen.context.inventory[0].brand'),
            Column('commentsCount', DataType.INT, path='owen.context.inventory[0].commentsCount'),
            Column('legacyOfferId', DataType.STRING, path='owen.context.custom.legacy.offerIds.offerId'),
            Column('pageViewHash', DataType.STRING, path='owen.context.custom.legacy.pageViewHash'),
            Column('vIdInt', DataType.STRING, path='owen.context.custom.legacy.vIdInt'),
            Column('merchantId', DataType.STRING, path='owen.context.custom.legacy.merchantId'),
            Column('facebookConnect', DataType.STRING, path='owen.context.custom.facebookConnect'),
            Column('schemaKey', DataType.STRING, path='schema.key'),
        ],
    ))))
    print 'created dataset: %s' % dataset.id

    subscription = dart.save_subscription(Subscription(data=SubscriptionData(
        name='owen_eu_subscription_DW-3213_v3',
        dataset_id=dataset.id,
        s3_path_start_prefix_inclusive='s3://example-bucket/prd/inbound/overlord/eu-all-events/2015/08/05/',
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
            name='owen_eu_parquet_DW-3213_v3',
            engine_name='emr_engine',
            state=DatastoreState.ACTIVE,
            args={
                # 'data_to_freespace_ratio': 0.05,
                'instance_count': 3,
            }
        )
    ))
    print 'created datastore: %s' % datastore.id

    a0, a1 = dart.save_actions(
        actions=[
            Action(data=ActionData('start_datastore', 'start_datastore')),
            Action(data=ActionData('load_dataset', 'load_dataset', args={
                'dataset_id': dataset.id,
                's3_path_end_prefix_exclusive': 's3://example-bucket/prd/inbound/overlord/eu-all-events/2015/08/05/',
                'target_file_format': FileFormat.PARQUET,
                'target_row_format': RowFormat.NONE,
                'target_compression': Compression.SNAPPY,
            })),
        ],
        datastore_id=datastore.id
    )
    print 'created action: %s' % a0.id
    print 'created action: %s' % a1.id

    workflow = dart.save_workflow(
        workflow=Workflow(
            data=WorkflowData(
                name='owen_eu_parquet_workflow_DW-3213_v3',
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

    a2 = dart.save_actions(
        actions=[
            Action(data=ActionData('consume_subscription', 'consume_subscription', state=ActionState.TEMPLATE, args={
                'subscription_id': subscription.id,
                'target_file_format': FileFormat.PARQUET,
                'target_row_format': RowFormat.NONE,
                'target_compression': Compression.SNAPPY,
            })),
        ],
        workflow_id=workflow.id
    )[0]
    print 'created workflow action: %s' % a2.id

    trigger = dart.save_trigger(Trigger(data=TriggerData(
        name='owen_eu_parquet_trigger_DW-3213_v3',
        trigger_type_name='subscription_batch',
        workflow_ids=[workflow.id],
        args={
            'subscription_id': subscription.id,
            'unconsumed_data_size_in_bytes': 16000000
        }
    )))
    print 'created trigger: %s' % trigger.id
