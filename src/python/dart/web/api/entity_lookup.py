from functools import wraps
from flask import abort, current_app
from dart.context.locator import injectable


@injectable
class EntityLookupService(object):
    def __init__(self, engine_service, dataset_service, datastore_service, action_service, trigger_service,
                 workflow_service, subscription_service, event_service):
        self._services = {
            'engine': engine_service.get_engine,
            'subgraph_definition': engine_service.get_subgraph_definition,
            'dataset': dataset_service.get_dataset,
            'datastore': datastore_service.get_datastore,
            'action': action_service.get_action,
            'trigger': trigger_service.get_trigger,
            'workflow': workflow_service.get_workflow,
            'workflow_instance': workflow_service.get_workflow_instance,
            'subscription': subscription_service.get_subscription,
            'event': event_service.get_event,
        }

    def unsupported_entity_type(self, entity_type):
        return self._services.get(entity_type) is None

    def get_entity(self, entity_type, id):
        get_func = self._services[entity_type]
        return get_func(id, raise_when_missing=False)


def fetch_model(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        lookup_service = current_app.dart_context.get(EntityLookupService)
        entities_by_type = {}
        for url_param_name, value in kwargs.iteritems():
            if lookup_service.unsupported_entity_type(url_param_name):
                continue
            model = lookup_service.get_entity(url_param_name, value)
            if not model:
                abort(404)
            entities_by_type[url_param_name] = model
        kwargs.update(entities_by_type)
        return f(*args, **kwargs)
    return wrapper
