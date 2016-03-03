from dart.client.python.dart_client import Dart
from dart.model.datastore import Datastore, DatastoreState

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    datastore = dart.get_datastore('KNMUGQWTHT')
    assert isinstance(datastore, Datastore)

    datastore.data.state = DatastoreState.ACTIVE
    dart.save_datastore(datastore)
