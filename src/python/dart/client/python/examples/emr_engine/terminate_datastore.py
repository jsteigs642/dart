from dart.client.python.dart_client import Dart
from dart.model.action import Action
from dart.model.action import ActionData

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    action = dart.save_actions(
        actions=[
            Action(data=ActionData('terminate_datastore', 'terminate_datastore')),
        ],
        datastore_id='80WJRQDHXK'
    )[0]
    print 'created action: %s' % action.id
