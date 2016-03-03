from dart.client.python.dart_client import Dart
from dart.model.action import Action, ActionState
from dart.model.action import ActionData
from dart.model.dataset import Column, DatasetData, Dataset, DataFormat, FileFormat, RowFormat, DataType, Compression
from dart.model.datastore import Datastore, DatastoreData, DatastoreState
from dart.model.subscription import SubscriptionState, Subscription, SubscriptionData
from dart.model.trigger import Trigger
from dart.model.trigger import TriggerData
from dart.model.workflow import Workflow, WorkflowData, WorkflowState

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    dataset = dart.save_dataset(Dataset(data=DatasetData(
        name='weblogs_v01',
        table_name='weblogs',
        location='s3://example-bucket/weblogs/www.retailmenot.com/ec2/',
        data_format=DataFormat(
            file_format=FileFormat.TEXTFILE,
            row_format=RowFormat.REGEX,
            regex_input="(?<ip>^(?:(?:unknown(?:,\\s)?|(?:\\d+\\.\\d+\\.\\d+\\.\\d+(?:,\\s)?))+)|\\S*)\\s+\\S+\\s+(?<userIdentifier>(?:[^\\[]+|\\$\\S+\\['\\S+'\\]|\\[username\\]))\\s*\\s+\\[(?<requestDate>[^\\]]+)\\]\\s+\"(?<httpMethod>(?:GET|HEAD|POST|PUT|DELETE|TRACE))\\s(?<urlPath>(?:[^ ?]+))(?:\\?(?<queryString>(?:[^ ]+)))?\\sHTTP/(?<httpVersion>(?:[\\d\\.]+))\"\\s+(?<statusCode>[0-9]+)\\s+(?<bytesSent>\\S+)\\s+\"(?<referrer>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+\"(?<userAgent>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+(?<responseTime>[-0-9]*)\\s+\"(?<hostName>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+\"(?<userFingerprint>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+\"(?<userId>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+\"(?<sessionId>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+\"(?<requestId>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+\"(?<visitorId>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+\"(?<vegSlice>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+\"(?<fruitSlice>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s+\"(?<cacheHitMiss>(?:[^\"\\\\]*(?:\\\\.[^\"\\\\]*)*))\"\\s*\\Z",
            regex_output="%1$s %2$s %3$s %4$s %5$s %6$s %7$s %8$s %9$s %10$s %11$s %12$s %13$s %14$s %15$s %16$s %17$s %18$s %19$s %20$s %21s",
        ),
        columns=[
            Column('ip', DataType.STRING),
            Column('user', DataType.STRING),
            Column('requestDate', DataType.TIMESTAMP, date_pattern='dd/MMM/yyyy:HH:mm:ss Z'),
            Column('httpMethod', DataType.STRING),
            Column('urlPath', DataType.STRING),
            Column('queryString', DataType.STRING),
            Column('httpVersion', DataType.STRING),
            Column('statusCode', DataType.STRING),
            Column('bytesSent', DataType.INT),
            Column('referrer', DataType.STRING),
            Column('userAgent', DataType.STRING),
            Column('responseTime', DataType.BIGINT),
            Column('hostname', DataType.STRING),
            Column('userFingerprint', DataType.STRING),
            Column('userId', DataType.STRING),
            Column('sessionId', DataType.STRING),
            Column('requestId', DataType.STRING),
            Column('visitorId', DataType.STRING),
            Column('vegSlice', DataType.STRING),
            Column('fruitSlice', DataType.STRING),
            Column('cacheHitMiss', DataType.STRING),
        ],
        compression=Compression.BZ2,
        partitions=[
            Column('year', DataType.STRING),
            Column('week', DataType.STRING),
        ],
    )))
    print 'created dataset: %s' % dataset.id

    subscription = dart.save_subscription(Subscription(data=SubscriptionData(
        name='weblogs_rmn_subscription',
        dataset_id=dataset.id,
        on_failure_email=['daniel@email.com', 'john@email.com'],
        on_success_email=['daniel@email.com', 'john@email.com'],
    )))
    print 'created subscription: %s' % subscription.id

    print 'awaiting subscription generation...'
    subscription = dart.await_subscription_generation(subscription.id)
    assert subscription.data.state == SubscriptionState.ACTIVE
    print 'done.'

    datastore = dart.save_datastore(Datastore(
        data=DatastoreData(
            name='weblogs_rmn_legacy',
            engine_name='emr_engine',
            state=DatastoreState.TEMPLATE,
            args={
                'data_to_freespace_ratio': 0.50,
            }
        )
    ))
    print 'created datastore: %s' % datastore.id

    workflow = dart.save_workflow(
        workflow=Workflow(
            data=WorkflowData(
                name='weblogs_rmn_legacy_parse_to_delimited',
                datastore_id=datastore.id,
                state=WorkflowState.ACTIVE,
                on_failure_email=['daniel@email.com', 'john@email.com'],
                on_success_email=['daniel@email.com', 'john@email.com'],
                on_started_email=['daniel@email.com', 'john@email.com'],
            )
        ),
        datastore_id=datastore.id
    )
    print 'created workflow: %s' % workflow.id

    a2 = dart.save_actions(
        actions=[
            Action(data=ActionData('consume_subscription', 'consume_subscription', state=ActionState.TEMPLATE, args={
                'subscription_id': subscription.id,
                'target_file_format': FileFormat.TEXTFILE,
                'target_row_format': RowFormat.DELIMITED,
                'target_compression': Compression.GZIP,
                'target_delimited_by': '\t',
                'target_quoted_by': '"',
                'target_escaped_by': '\\',
                'target_null_string': 'NULL',
            })),
        ],
        workflow_id=workflow.id
    )[0]
    print 'created workflow action: %s' % a2.id

    trigger = dart.save_trigger(Trigger(data=TriggerData(
        name='weblogs_DW-3213_v3',
        trigger_type_name='subscription_batch',
        workflow_ids=[workflow.id],
        args={
            'subscription_id': subscription.id,
            'unconsumed_data_size_in_bytes': 16000000
        }
    )))
    print 'created trigger: %s' % trigger.id