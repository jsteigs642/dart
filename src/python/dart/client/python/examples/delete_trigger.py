from dart.client.python.dart_client import Dart

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    trigger = dart.delete_trigger('P4WIS506ZW')
