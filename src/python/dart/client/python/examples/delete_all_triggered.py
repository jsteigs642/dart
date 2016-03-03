from dart.client.python.dart_client import Dart

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    trigger = ''
    dataset_id = ''
    datastore_id = ''

    trigger = dart.get_trigger(trigger)
    for workflow_id in trigger.data.workflow_ids:
        for wfi in dart.get_workflow_instances(workflow_id):
            datastore_id = wfi.data.datastore_id
            for a in dart.get_actions(datastore_id=datastore_id):
                dart.delete_action(a.id)

        dart.delete_workflow_instances(workflow_id)
        dart.delete_workflow(workflow_id)

    if datastore_id:
        dart.delete_datastore(datastore_id)

    if dataset_id:
        dart.delete_dataset(dataset_id)
