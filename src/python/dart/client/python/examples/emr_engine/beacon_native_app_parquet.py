from dart.client.python.dart_client import Dart
from dart.model.action import Action, ActionState
from dart.model.action import ActionData
from dart.model.dataset import Column, DatasetData, Dataset, DataFormat, DataType, Compression, LoadType
from dart.model.datastore import Datastore, DatastoreData, DatastoreState
from dart.model.event import Event, EventState
from dart.model.event import EventData
from dart.model.trigger import Trigger, TriggerData
from dart.model.workflow import Workflow, WorkflowState
from dart.model.workflow import WorkflowData

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    dataset = dart.save_dataset(Dataset(data=(DatasetData(
        name='beacon_native_app_v02',
        table_name='beacon_native_app',
        location='s3://example-bucket/prd/beacon/native_app/v2/parquet/snappy',
        hive_compatible_partition_folders=True,
        load_type=LoadType.INSERT,
        data_format=DataFormat('parquet'),
        columns=[
            Column('logFileId', DataType.BIGINT),
            Column('lineNumber', DataType.INT),
            Column('created', DataType.BIGINT),
            Column('remoteip', DataType.STRING),
            Column('useragent', DataType.STRING),
            Column('eventType', DataType.STRING),
            Column('appVersion', DataType.STRING),
            Column('advertiserID', DataType.STRING),
            Column('couponsOnPage', DataType.INT),
            Column('coupons', DataType.STRING),
            Column('channel', DataType.STRING),
            Column('geoCouponCount', DataType.STRING),
            Column('geofence', DataType.STRING),
            Column('geofenceTimeSpent', DataType.STRING),
            Column('loginStatus', DataType.STRING),
            Column('products', DataType.STRING),
            Column('session', DataType.STRING),
            Column('systemName', DataType.STRING),
            Column('systemVersion', DataType.STRING),
            Column('udid', DataType.STRING),
            Column('userQualifier', DataType.STRING),
            Column('url', DataType.STRING),
            Column('user_uuid', DataType.STRING),
            Column('userId', DataType.STRING),
            Column('searchType', DataType.STRING),
            Column('searchListTerm', DataType.STRING),
            Column('searchTerm', DataType.STRING),
            Column('emailUUId', DataType.STRING),
            Column('userFingerprint', DataType.STRING),
            Column('locationStatus', DataType.STRING),
            Column('pushNotificationStatus', DataType.BOOLEAN),
            Column('placement', DataType.STRING),
            Column('loc', DataType.STRING),
            Column('ppoi0', DataType.STRING),
            Column('ppoi1', DataType.STRING),
            Column('ppoi2', DataType.STRING),
            Column('ppoi3', DataType.STRING),
            Column('ppoi4', DataType.STRING),
            Column('appLaunchNotificationType', DataType.STRING),
            Column('scenarioName', DataType.STRING),
            Column('behaviorName', DataType.STRING),
            Column('couponType', DataType.STRING),
            Column('couponPosition', DataType.STRING),
            Column('hasQSRContent', DataType.BOOLEAN),
            Column('promptName', DataType.STRING),
            Column('locationPermissionChanage', DataType.STRING),
            Column('couponProblemType', DataType.STRING),
            Column('storeTitle', DataType.STRING),
            Column('mallName', DataType.STRING),
            Column('restaurantName', DataType.STRING),
            Column('milesAway', 'float'),
            Column('menuItem', DataType.STRING),
            Column('toolName', DataType.STRING),
            Column('toolAction', DataType.STRING),
            Column('toolStep', DataType.STRING),
            Column('mallPosition', DataType.INT),
            Column('recommendStoreName', DataType.STRING),
            Column('recommendStorePosition', DataType.INT),
            Column('favoriteStoreName', DataType.STRING),
            Column('favoriteStoreAction', DataType.STRING),
            Column('favoriteStorePosition', DataType.INT),
            Column('favoriteSiteId', DataType.STRING),
            Column('receiverName', DataType.STRING),
            Column('outclickButtonPrompt', DataType.STRING),
            Column('dataSource', DataType.STRING),
            Column('searchResultCount', DataType.INT),
            Column('searchResultPosition', DataType.INT),
            Column('shareType', DataType.STRING),
            Column('daysUntilExpiration', DataType.INT),
            Column('fireDate', DataType.BIGINT),
            Column('settingsChangeValue', DataType.STRING),
            Column('settingsChangeType', DataType.STRING),
            Column('settingsChangeLocation', DataType.STRING),
            Column('clickAction', DataType.STRING),
            Column('tnt', DataType.STRING),
            Column('previousPage', DataType.STRING),
            Column('clickPage', DataType.STRING),
            Column('launchReason', DataType.STRING),
            Column('taplyticsData', DataType.STRING),
            Column('appCampaign', DataType.STRING),
            Column('accountMethod', DataType.STRING),
            Column('appState', DataType.STRING),
            Column('btStatus', DataType.BOOLEAN),
            Column('btBeaconId', DataType.STRING),
            Column('btBeaconFactoryId', DataType.STRING),
            Column('btBeaconName', DataType.STRING),
            Column('btTimeSpent', DataType.STRING),
            Column('purchaseId', DataType.STRING),
            Column('transactionId', DataType.STRING),
            Column('outclickLink', DataType.STRING),
            Column('outclickPage', DataType.STRING),
            Column('featuredCouponPosition', DataType.INT),
            Column('commentCount', DataType.INT),
            Column('mallCount', DataType.INT),
            Column('clickCount', DataType.INT),
            Column('merchantName', DataType.STRING),
            Column('merchantPosition', DataType.INT),
        ],
        compression=Compression.SNAPPY,
        partitions=[Column('createdpartition', DataType.STRING)],
    ))))
    print 'created dataset: %s' % dataset.id

    datastore = dart.save_datastore(Datastore(
        data=DatastoreData(
            'beacon_native_app_impala',
            'emr_engine',
            state=DatastoreState.TEMPLATE,
            args={'data_to_freespace_ratio': 0.25}
        )
    ))
    print 'created datastore: %s' % datastore.id

    workflow = dart.save_workflow(Workflow(
        data=WorkflowData(
            'load_beacon_native_app_impala',
            datastore.id,
            state=WorkflowState.ACTIVE,
            on_failure_email=['daniel@email.com'],
            on_success_email=['daniel@email.com'],
            on_started_email=['daniel@email.com'],
        )
    ), datastore.id)
    print 'created workflow: %s' % workflow.id

    a0, a1 = dart.save_actions([
        Action(data=ActionData('start_datastore', 'start_datastore', state=ActionState.TEMPLATE)),
        Action(data=ActionData('load_dataset', 'load_dataset', state=ActionState.TEMPLATE, args={
            'dataset_id': dataset.id,
            's3_path_start_prefix_inclusive': 's3://example-bucket/prd/beacon/native_app/v2/parquet/snappy/createdpartition=2015-06-27',
        })),
    ], workflow_id=workflow.id)
    print 'created action: %s' % a0.id
    print 'created action: %s' % a1.id

    event = dart.save_event(Event(data=EventData('beacon_native_app_to_parquet_emr_job_completion', state=EventState.ACTIVE)))
    print 'created event: %s' % event.id

    trigger = dart.save_trigger(Trigger(data=TriggerData(
        'beacon_native_app_to_parquet_emr_job_completion_trigger',
        'event',
        [workflow.id],
        {'event_id': event.id}))
    )
    print 'created trigger: %s' % trigger.id

