from dart.client.python.dart_client import Dart
from dart.model.action import Action
from dart.model.action import ActionData
from dart.model.dataset import Column, DatasetData, Dataset, DataFormat, FileFormat, RowFormat, DataType, Compression
from dart.model.datastore import Datastore, DatastoreData, DatastoreState

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    # dart = Dart()
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

    datastore = dart.save_datastore(Datastore(
        data=DatastoreData(
            name='weblogs_DW-3503',
            engine_name='emr_engine',
            state=DatastoreState.ACTIVE,
            args={
                'data_to_freespace_ratio': 0.30,
            }
        )
    ))
    print 'created datastore: %s' % datastore.id

    actions = dart.save_actions(
        actions=[
            Action(data=ActionData('start_datastore', 'start_datastore')),
            Action(data=ActionData('load_dataset', 'load_dataset', args={
                'dataset_id': dataset.id,
                's3_path_start_prefix_inclusive': 's3://example-bucket/weblogs/www.retailmenot.com/ec2/2014/50',
                's3_path_end_prefix_exclusive': 's3://example-bucket/weblogs/www.retailmenot.com/ec2/2015/00',
                's3_path_regex_filter': 's3://example-bucket/weblogs/www.retailmenot.com/ec2/2014/../www\\.retailmenot\\.com.*',
                'target_file_format': FileFormat.TEXTFILE,
                'target_row_format': RowFormat.DELIMITED,
                'target_compression': Compression.GZIP,
                'target_delimited_by': '\t',
                'target_quoted_by': '"',
                'target_escaped_by': '\\',
                'target_null_string': 'NULL',
            })),
        ],
        datastore_id=datastore.id
    )
    print 'created datastore action: %s' % actions[0].id
