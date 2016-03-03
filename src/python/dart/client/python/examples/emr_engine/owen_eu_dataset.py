from dart.client.python.dart_client import Dart
from dart.model.action import Action, ActionState
from dart.model.action import ActionData
from dart.model.dataset import Column, DatasetData, Dataset, DataFormat, FileFormat, RowFormat, DataType, Compression, \
    LoadType
from dart.model.datastore import Datastore, DatastoreData, DatastoreState
from dart.model.subscription import Subscription, SubscriptionState
from dart.model.subscription import SubscriptionData
from dart.model.trigger import Trigger, TriggerState
from dart.model.trigger import TriggerData
from dart.model.workflow import Workflow, WorkflowState
from dart.model.workflow import WorkflowData

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    dataset = dart.save_dataset(Dataset(data=(DatasetData(
        name='owen_eu_DW-3411_v1',
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
            Column('eventName', DataType.STRING, path='owen.event.eventName'),
            Column('eventVersion', DataType.STRING, path='owen.event.eventVersion'),
            Column('eventPlatform', DataType.STRING, path='owen.event.eventPlatform'),
            Column('eventInstanceUuid', DataType.STRING, path='owen.event.eventInstanceUuid'),
            Column('eventCategory', DataType.STRING, path='owen.event.eventCategory'),
            Column('eventTimestamp', DataType.TIMESTAMP, path='owen.event.eventTimestamp', date_pattern="yyyy-MM-dd'T'HH:mm:ss'Z'"),
            Column('eventTarget', DataType.STRING, path='owen.event.eventTarget'),
            Column('eventAction', DataType.STRING, path='owen.event.eventAction'),
            Column('eventPlatformVersion', DataType.STRING, path='owen.event.eventPlatformVersion'),
            Column('osName', DataType.STRING, path='owen.context.osName'),
            Column('loggedInFlag', DataType.BOOLEAN, path='owen.context.loggedInFlag'),
            Column('custom', DataType.STRING, path='owen.context.custom'),
            Column('browserVersion', DataType.STRING, path='owen.context.browserVersion'),
            Column('referrer', DataType.STRING, path='owen.context.referrer'),
            Column('previousPageName', DataType.STRING, path='owen.context.previousPageName'),
            Column('screenHeight', DataType.STRING, path='owen.context.screenHeight'),
            Column('breadCrumb', DataType.STRING, path='owen.context.breadCrumb'),
            Column('pageName', DataType.STRING, path='owen.context.pageName'),
            Column('country', DataType.STRING, path='owen.context.country'),
            Column('propertyName', DataType.STRING, path='owen.context.propertyName'),
            Column('launchCount', DataType.STRING, path='owen.context.launchCount'),
            Column('viewInstanceUuid', DataType.STRING, path='owen.context.viewInstanceUuid'),
            Column('osVersion', DataType.STRING, path='owen.context.osVersion'),
            Column('connectionType', DataType.STRING, path='owen.context.connectionType'),
            Column('partialSearchTerm', DataType.STRING, path='owen.context.partialSearchTerm'),
            Column('carrier', DataType.STRING, path='owen.context.carrier'),
            Column('longitude', DataType.STRING, path='owen.context.longitude'),
            Column('productSectionPosition_0', DataType.STRING, path='owen.context.inventory[0].productSectionPosition'),
            Column('savedFlag_0', DataType.BOOLEAN, path='owen.context.inventory[0].savedFlag'),
            Column('position_0', DataType.STRING, path='owen.context.inventory[0].position'),
            Column('brand_0', DataType.STRING, path='owen.context.inventory[0].brand'),
            Column('affiliateNetwork_0', DataType.STRING, path='owen.context.inventory[0].affiliateNetwork'),
            Column('deepLinkUrl_0', DataType.STRING, path='owen.context.inventory[0].deepLinkUrl'),
            Column('conquestingFlag_0', DataType.BOOLEAN, path='owen.context.inventory[0].conquestingFlag'),
            Column('originalPrice_0', DataType.STRING, path='owen.context.inventory[0].originalPrice'),
            Column('adUnitUuid_0', DataType.STRING, path='owen.context.inventory[0].adUnitUuid'),
            Column('startDate_0', DataType.TIMESTAMP, path='owen.context.inventory[0].startDate', date_pattern="yyyy-MM-dd'T'HH:mm:ss'Z'"),
            Column('proximityUnit_0', DataType.STRING, path='owen.context.inventory[0].proximityUnit'),
            Column('commentsCount_0', DataType.STRING, path='owen.context.inventory[0].commentsCount'),
            Column('outRedirectUrl_0', DataType.STRING, path='owen.context.inventory[0].outRedirectUrl'),
            Column('productCardPosition_0', DataType.STRING, path='owen.context.inventory[0].productCardPosition'),
            Column('productSectionUuid_0', DataType.STRING, path='owen.context.inventory[0].productSectionUuid'),
            Column('lastVerifiedDate_0', DataType.TIMESTAMP, path='owen.context.inventory[0].lastVerifiedDate', date_pattern="yyyy-MM-dd'T'HH:mm:ss'Z'"),
            Column('productCardUuid_0', DataType.STRING, path='owen.context.inventory[0].productCardUuid'),
            Column('redemptionChannel_0', DataType.STRING, path='owen.context.inventory[0].redemptionChannel'),
            Column('noVotes_0', DataType.STRING, path='owen.context.inventory[0].noVotes'),
            Column('retailCategory_0', DataType.STRING, path='owen.context.inventory[0].retailCategory'),
            Column('couponRank_0', DataType.STRING, path='owen.context.inventory[0].couponRank'),
            Column('inventoryChannel_0', DataType.STRING, path='owen.context.inventory[0].inventoryChannel'),
            Column('yesVotes_0', DataType.STRING, path='owen.context.inventory[0].yesVotes'),
            Column('inventorySource_0', DataType.STRING, path='owen.context.inventory[0].inventorySource'),
            Column('inventoryName_0', DataType.STRING, path='owen.context.inventory[0].inventoryName'),
            Column('monetizableFlag_0', DataType.BOOLEAN, path='owen.context.inventory[0].monetizableFlag'),
            Column('recommendedFlag_0', DataType.BOOLEAN, path='owen.context.inventory[0].recommendedFlag'),
            Column('expirationDate_0', DataType.TIMESTAMP, path='owen.context.inventory[0].expirationDate', date_pattern="yyyy-MM-dd'T'HH:mm:ss'Z'"),
            Column('clickLocation_0', DataType.STRING, path='owen.context.inventory[0].clickLocation'),
            Column('finalPrice_0', DataType.STRING, path='owen.context.inventory[0].finalPrice'),
            Column('usedByCount_0', DataType.STRING, path='owen.context.inventory[0].usedByCount'),
            Column('proximity_0', DataType.STRING, path='owen.context.inventory[0].proximity'),
            Column('inventoryUuid_0', DataType.STRING, path='owen.context.inventory[0].inventoryUuid'),
            Column('siteUuid_0', DataType.STRING, path='owen.context.inventory[0].siteUuid'),
            Column('outclickUuid_0', DataType.STRING, path='owen.context.inventory[0].outclickUuid'),
            Column('adUnitType_0', DataType.STRING, path='owen.context.inventory[0].adUnitType'),
            Column('exclusivityFlag_0', DataType.BOOLEAN, path='owen.context.inventory[0].exclusivityFlag'),
            Column('inventoryType_0', DataType.STRING, path='owen.context.inventory[0].inventoryType'),
            Column('successPercentage_0', DataType.STRING, path='owen.context.inventory[0].successPercentage'),
            Column('claimUuid_0', DataType.STRING, path='owen.context.inventory[0].claimUuid'),
            Column('region', DataType.STRING, path='owen.context.region'),
            Column('session', DataType.STRING, path='owen.context.session'),
            Column('content', DataType.STRING, path='owen.context.marketing.content'),
            Column('marketingVendor', DataType.STRING, path='owen.context.marketing.vendor'),
            Column('campaign', DataType.STRING, path='owen.context.marketing.campaign'),
            Column('adGroup', DataType.STRING, path='owen.context.marketing.adGroup'),
            Column('campaignUuid', DataType.STRING, path='owen.context.marketing.campaignUuid'),
            Column('campaignSendCount', DataType.STRING, path='owen.context.marketing.campaignSendCount'),
            Column('source', DataType.STRING, path='owen.context.marketing.source'),
            Column('term', DataType.STRING, path='owen.context.marketing.term'),
            Column('channel', DataType.STRING, path='owen.context.marketing.channel'),
            Column('medium', DataType.STRING, path='owen.context.marketing.medium'),
            Column('cdRank', DataType.STRING, path='owen.context.marketing.cdRank'),
            Column('notificationUuid', DataType.STRING, path='owen.context.marketing.notificationUuid'),
            Column('inventoryCount', DataType.STRING, path='owen.context.inventoryCount'),
            Column('favoriteFlag', DataType.BOOLEAN, path='owen.context.favoriteFlag'),
            Column('pageType', DataType.STRING, path='owen.context.pageType'),
            Column('bluetoothBeaconType', DataType.STRING, path='owen.context.bluetoothBeaconType'),
            Column('variation_0', DataType.STRING, path='owen.context.experiment[0].variation'),
            Column('campaign_0', DataType.STRING, path='owen.context.experiment[0].campaign'),
            Column('locationEnabledFlag', DataType.BOOLEAN, path='owen.context.locationEnabledFlag'),
            Column('macAddress', DataType.STRING, path='owen.context.macAddress'),
            Column('browserFamily', DataType.STRING, path='owen.context.browserFamily'),
            Column('geofenceUuid', DataType.STRING, path='owen.context.geofenceUuid'),
            Column('mobileDeviceMake', DataType.STRING, path='owen.context.mobileDeviceMake'),
            Column('vendor_0', DataType.STRING, path='owen.context.vendor[0].vendor'),
            Column('vendorClickUuid_0', DataType.STRING, path='owen.context.vendor[0].vendorClickUuid'),
            Column('udid', DataType.STRING, path='owen.context.udid'),
            Column('latitude', DataType.STRING, path='owen.context.latitude'),
            Column('bluetoothEnabledFlag', DataType.BOOLEAN, path='owen.context.bluetoothEnabledFlag'),
            Column('environment', DataType.STRING, path='owen.context.environment'),
            Column('city', DataType.STRING, path='owen.context.city'),
            Column('userUuid', DataType.STRING, path='owen.context.userUuid'),
            Column('dma', DataType.STRING, path='owen.context.dma'),
            Column('testUuid', DataType.STRING, path='owen.context.test.testUuid'),
            Column('userAgent', DataType.STRING, path='owen.context.userAgent'),
            Column('previousViewInstanceUuid', DataType.STRING, path='owen.context.previousViewInstanceUuid'),
            Column('language', DataType.STRING, path='owen.context.language'),
            Column('deviceCategory', DataType.STRING, path='owen.context.deviceCategory'),
            Column('bluetoothBeaconId', DataType.STRING, path='owen.context.bluetoothBeaconId'),
            Column('screenWidth', DataType.STRING, path='owen.context.screenWidth'),
            Column('personalizationFlag', DataType.BOOLEAN, path='owen.context.personalizationFlag'),
            Column('appForegroundFlag', DataType.BOOLEAN, path='owen.context.appForegroundFlag'),
            Column('mobileDeviceModel', DataType.STRING, path='owen.context.mobileDeviceModel'),
            Column('userQualifier', DataType.STRING, path='owen.context.userQualifier'),
            Column('deviceFingerprint', DataType.STRING, path='owen.context.deviceFingerprint'),
            Column('ipAddress', DataType.STRING, path='owen.context.ipAddress'),
            Column('osFamily', DataType.STRING, path='owen.context.osFamily'),
            Column('advertiserUuid', DataType.STRING, path='owen.context.advertiserUuid'),
            Column('notificationEnabledFlag', DataType.BOOLEAN, path='owen.context.notificationEnabledFlag'),
            Column('inventory', DataType.STRING, path='owen.context.inventory'),
            Column('vendor', DataType.STRING, path='owen.context.vendor'),
            Column('experiment', DataType.STRING, path='owen.context.experiment'),
        ],
    ))))
    print 'created dataset: %s' % dataset.id

    subscription = dart.save_subscription(Subscription(data=SubscriptionData(
        name='owen_eu_subscription_DW-3411_v1',
        dataset_id=dataset.id,
        s3_path_start_prefix_inclusive='s3://example-bucket/prd/inbound/overlord/eu-all-events/2015/09/07/',
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
            name='owen_eu_parquet_DW-3411_v1',
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
                's3_path_end_prefix_exclusive': 's3://example-bucket/prd/inbound/overlord/eu-all-events/2015/09/07/',
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
                name='owen_eu_parquet_workflow_DW-3411_v1',
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
        name='owen_eu_parquet_trigger_DW-3411_v1',
        trigger_type_name='subscription_batch',
        workflow_ids=[workflow.id],
        args={
            'subscription_id': subscription.id,
            'unconsumed_data_size_in_bytes': 16000000
        },
        state=TriggerState.ACTIVE,
    )))
    print 'created trigger: %s' % trigger.id
