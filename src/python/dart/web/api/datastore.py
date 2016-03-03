import json

from flask import Blueprint, request, current_app
from flask.ext.jsontools import jsonapi
from jsonpatch import JsonPatch

from dart.model.datastore import Datastore, DatastoreState
from dart.service.action import ActionService
from dart.service.datastore import DatastoreService
from dart.service.filter import FilterService
from dart.web.api.entity_lookup import fetch_model

api_datastore_bp = Blueprint('api_datastore', __name__)


@api_datastore_bp.route('/datastore', methods=['POST'])
@jsonapi
def post_datastore():
    return {'results': datastore_service().save_datastore(Datastore.from_dict(request.get_json())).to_dict()}


@api_datastore_bp.route('/datastore/<datastore>', methods=['GET'])
@fetch_model
@jsonapi
def get_datastore(datastore):
    return {'results': datastore.to_dict()}


@api_datastore_bp.route('/datastore', methods=['GET'])
@jsonapi
def find_datastores():
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    filters = [filter_service().from_string(f) for f in json.loads(request.args.get('filters', '[]'))]
    datastores = datastore_service().query_datastores(filters, limit, offset)
    return {
        'results': [d.to_dict() for d in datastores],
        'limit': limit,
        'offset': offset,
        'total': datastore_service().query_datastores_count(filters)
    }


@api_datastore_bp.route('/datastore/<datastore>', methods=['PUT'])
@fetch_model
@jsonapi
def put_datastore(datastore):
    """ :type datastore: dart.model.datastore.Datastore """
    updated_datastore = Datastore.from_dict(request.get_json())
    if datastore.data.state == DatastoreState.TEMPLATE and updated_datastore.data.state != DatastoreState.TEMPLATE:
        return {'results': 'ERROR', 'error_message': 'TEMPLATE state cannot be changed'}, 400, None
    if updated_datastore.data.state not in [DatastoreState.ACTIVE, DatastoreState.INACTIVE, DatastoreState.DONE]:
        return {'results': 'ERROR', 'error_message': 'state must be ACTIVE, INACTIVE, or DONE'}, 400, None

    datastore = datastore_service().update_datastore_extra_data(datastore, updated_datastore.data.extra_data)
    return {'results': datastore_service().update_datastore_state(datastore, updated_datastore.data.state).to_dict()}


@api_datastore_bp.route('/datastore/<datastore>', methods=['PATCH'])
@fetch_model
@jsonapi
def patch_datastore(datastore):
    """ :type datastore: dart.model.datastore.Datastore """
    p = JsonPatch(request.get_json())
    sanitized_datastore = datastore.copy()
    patched_datastore = Datastore.from_dict(p.apply(datastore.to_dict()))

    # only allow updating fields that are editable
    sanitized_datastore.data.name = patched_datastore.data.name
    sanitized_datastore.data.host = patched_datastore.data.host
    sanitized_datastore.data.port = patched_datastore.data.port
    sanitized_datastore.data.connection_url = patched_datastore.data.connection_url
    sanitized_datastore.data.state = patched_datastore.data.state
    sanitized_datastore.data.concurrency = patched_datastore.data.concurrency
    sanitized_datastore.data.args = patched_datastore.data.args
    sanitized_datastore.data.extra_data = patched_datastore.data.extra_data
    sanitized_datastore.data.tags = patched_datastore.data.tags

    # revalidate
    sanitized_datastore = datastore_service().default_and_validate_datastore(sanitized_datastore)

    return {'results': datastore_service().patch_datastore(datastore, sanitized_datastore).to_dict()}


@api_datastore_bp.route('/datastore/<datastore>', methods=['DELETE'])
@fetch_model
@jsonapi
def delete_datastore(datastore):
    datastore_service().delete_datastore(datastore.id)
    return {'results': 'OK'}


def filter_service():
    """ :rtype: dart.service.filter.FilterService """
    return current_app.dart_context.get(FilterService)


def datastore_service():
    """ :rtype: dart.service.datastore.DatastoreService """
    return current_app.dart_context.get(DatastoreService)


def action_service():
    """ :rtype: dart.service.action.ActionService """
    return current_app.dart_context.get(ActionService)
