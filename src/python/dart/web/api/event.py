import json
from flask import Blueprint, request, current_app
from flask.ext.jsontools import jsonapi

from dart.model.event import Event
from dart.service.event import EventService
from dart.service.filter import FilterService
from dart.web.api.entity_lookup import fetch_model

api_event_bp = Blueprint('api_event', __name__)


@api_event_bp.route('/event', methods=['POST'])
@jsonapi
def post_event():
    event = event_service().save_event(Event.from_dict(request.get_json()))
    return {'results': event.to_dict()}


@api_event_bp.route('/event/<event>', methods=['GET'])
@fetch_model
@jsonapi
def get_event(event):
    return {'results': event.to_dict()}


@api_event_bp.route('/event', methods=['GET'])
@jsonapi
def find_events():
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    filters = [filter_service().from_string(f) for f in json.loads(request.args.get('filters', '[]'))]
    events = event_service().query_events(filters, limit, offset)
    return {
        'results': [d.to_dict() for d in events],
        'limit': limit,
        'offset': offset,
        'total': event_service().query_events_count(filters)
    }


@api_event_bp.route('/event/<event>', methods=['PUT'])
@fetch_model
@jsonapi
def put_event(event):
    e = Event.from_dict(request.get_json())
    event = event_service().update_event(event, e.data.name, e.data.description, e.data.state)
    return {'results': event.to_dict()}


@api_event_bp.route('/event/<event>', methods=['DELETE'])
@fetch_model
@jsonapi
def delete_event(event):
    event_service().delete_event(event.id)
    return {'results': 'OK'}


def filter_service():
    """ :rtype: dart.service.filter.FilterService """
    return current_app.dart_context.get(FilterService)


def event_service():
    """ :rtype: dart.service.event.EventService """
    return current_app.dart_context.get(EventService)
