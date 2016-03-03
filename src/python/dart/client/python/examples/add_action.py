from dart.client.python.dart_client import Dart
from dart.model.action import Action
from dart.model.action import ActionData
from dart.model.dataset import FileFormat

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    action = dart.save_actions([
        Action(data=ActionData('load_dataset', 'load_dataset', args={
            'dataset_id': 'NVVLBI7CWB',
            's3_path_start_prefix_inclusive': 's3://example-bucket/weblogs/www.retailmenot.com/ec2/2014/52',
            's3_path_end_prefix_exclusive': 's3://example-bucket/weblogs/www.retailmenot.com/ec2/2015/00',
            's3_path_regex_filter': 's3://example-bucket/weblogs/www.retailmenot.com/ec2/2014/../www\\.retailmenot\\.com.*',
            'target_file_format': FileFormat.PARQUET,
        })),
    ], datastore_id='IOMUQ5L8AX')[0]
    print 'created action: %s' % action.id
