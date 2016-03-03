from dart.client.python.dart_client import Dart
from dart.model.workflow import Workflow, WorkflowState

if __name__ == '__main__':
    dart = Dart('localhost', 5000)
    assert isinstance(dart, Dart)

    workflow = dart.get_workflow('86KJA8TAA9')
    assert isinstance(workflow, Workflow)

    workflow.data.state = WorkflowState.ACTIVE
    dart.save_workflow(workflow)
