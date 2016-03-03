from dart.client.python.dart_client import Dart

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    dart.delete_dataset('4S5DC2VGED')
    dart.delete_datastore('N4ILZV77D0')
    dart.delete_action('WS8PXAVIK2')
    dart.delete_action('SZWQV8MUXB')
