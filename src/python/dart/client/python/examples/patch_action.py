from dart.client.python.dart_client import Dart

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    action = dart.get_action('8U7H6OLHC5')
    action = dart.patch_action(action, order_idx=5)
    print 'patched action: %s' % action.id
