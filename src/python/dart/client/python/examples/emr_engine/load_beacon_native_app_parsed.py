from dart.client.python.dart_client import Dart
from dart.model.action import Action
from dart.model.action import ActionData
from dart.model.dataset import FileFormat, Compression
from dart.model.dataset import RowFormat
from dart.model.datastore import Datastore
from dart.model.datastore import DatastoreData
from dart.model.datastore import DatastoreState

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    datastore = dart.save_datastore(Datastore(
        data=DatastoreData(
            name='amaceiras_beacon_native_app_null_coupons_issue',
            engine_name='emr_engine',
            state=DatastoreState.ACTIVE,
            args={
                # 'data_to_freespace_ratio': 0.05,
                'instance_count': 5,
            }
        )
    ))
    print 'created datastore: %s' % datastore.id

    actions = dart.save_actions(
        actions=[
            Action(data=ActionData('start_datastore', 'start_datastore')),
            Action(data=ActionData('load_dataset', 'load_dataset', args={
                'dataset_id': 'URBA9XEQEF',
                's3_path_start_prefix_inclusive': 's3://example-bucket/nb.retailmenot.com/parsed_logs/2015/33/beacon-v2-2015-08-18',
                # 's3_path_end_prefix_exclusive': 's3://example-bucket/nb.retailmenot.com/parsed_logs/2015/31/beacon-v2-2015-08-01',
                's3_path_regex_filter': '.*\\.tsv',
                'target_file_format': FileFormat.PARQUET,
                'target_row_format': RowFormat.NONE,
                'target_compression': Compression.SNAPPY,
            })),
        ],
        datastore_id=datastore.id
        # datastore_id='JK4PVO2KZA'
    )
    print 'created action: %s' % actions[0].id
