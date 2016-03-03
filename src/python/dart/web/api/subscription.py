import json
from flask import Blueprint, request, current_app

from flask.ext.jsontools import jsonapi

from dart.model.subscription import Subscription, SubscriptionState, SubscriptionElementState
from dart.service.filter import FilterService
from dart.service.subscription import SubscriptionService, SubscriptionElementService
from dart.web.api.entity_lookup import fetch_model


api_subscription_bp = Blueprint('api_subscription', __name__)


@api_subscription_bp.route('/dataset/<dataset>/subscription', methods=['POST'])
@fetch_model
@jsonapi
def post_subscription(dataset):
    subscription = Subscription.from_dict(request.get_json())
    subscription.data.dataset_id = dataset.id
    subscription = subscription_service().save_subscription(subscription)
    return {'results': subscription.to_dict()}


@api_subscription_bp.route('/subscription/<subscription>', methods=['GET'])
@fetch_model
@jsonapi
def get_subscription(subscription):
    return {'results': subscription.to_dict()}


@api_subscription_bp.route('/subscription', methods=['GET'])
@jsonapi
def find_subscriptions():
    limit = int(request.args.get('limit', 20))
    offset = int(request.args.get('offset', 0))
    filters = [filter_service().from_string(f) for f in json.loads(request.args.get('filters', '[]'))]
    subscriptions = subscription_service().query_subscriptions(filters, limit, offset)
    return {
        'results': [d.to_dict() for d in subscriptions],
        'limit': limit,
        'offset': offset,
        'total': subscription_service().query_subscriptions_count(filters)
    }


@api_subscription_bp.route('/subscription/<subscription>/element_stats', methods=['GET'])
@fetch_model
@jsonapi
def get_subscription_element_stats(subscription):
    stats = subscription_element_service().get_subscription_element_stats(subscription.id)
    return {'results': [s.to_dict() for s in stats]}


@api_subscription_bp.route('/subscription/<subscription>/elements', methods=['GET'])
@fetch_model
@jsonapi
def find_subscription_elements(subscription):
    """ :type subscription: dart.model.subscription.Subscription """
    state = request.args.get('state')
    processed_after_s3_path = request.args.get('processed_after_s3_path')
    gte_processed = None
    if processed_after_s3_path:
        se = subscription_element_service().get_subscription_element(subscription.id, processed_after_s3_path)
        gte_processed = se.processed
    return subscription_elements(None, state, subscription.id, gte_processed, processed_after_s3_path)


@api_subscription_bp.route('/action/<action>/subscription/elements', methods=['GET'])
@fetch_model
@jsonapi
def find_action_subscription_elements(action):
    """ :type action: dart.model.action.Action """
    if 'subscription_id' not in action.data.args:
        error_message = 'action (id=%s) does not appear to consume a subscription: %s' % action.id
        return {'results': 'ERROR', 'error_message': error_message}, 400, None
    action_id = action.id
    subscription_id = action.data.args['subscription_id']
    state = SubscriptionElementState.ASSIGNED
    return subscription_elements(action_id, state, subscription_id)


def subscription_elements(action_id, state, subscription_id, gte_processed=None, gt_s3_path=None):
    limit = int(request.args.get('limit', 10000))
    offset = int(request.args.get('offset', 0))
    elements = subscription_element_service().find_subscription_elements(
        subscription_id=subscription_id,
        state=state,
        limit=limit,
        offset=offset,
        action_id=action_id,
        gt_s3_path=gt_s3_path,
        gte_processed=gte_processed
    )
    return {
        'results': [e.to_dict() for e in elements],
        'limit': limit,
        'offset': offset,
        'total': subscription_element_service().find_subscription_elements_count(
            subscription_id=subscription_id,
            state=state,
            action_id=action_id,
            gt_s3_path=gt_s3_path,
            gte_processed=gte_processed
        )
    }


@api_subscription_bp.route('/subscription/<subscription>', methods=['PUT'])
@fetch_model
@jsonapi
def put_subscription(subscription):
    s = Subscription.from_dict(request.get_json())
    state = SubscriptionState.INACTIVE if s.data.state == SubscriptionState.INACTIVE else subscription.data.state
    subscription = subscription_service().update_subscription_name(subscription, s.data.name)
    subscription = subscription_service().update_subscription_state(subscription, state)
    return {'results': subscription.to_dict()}


@api_subscription_bp.route('/subscription/<subscription>', methods=['DELETE'])
@fetch_model
@jsonapi
def delete_subscription(subscription):
    subscription_service().delete_subscription(subscription.id)
    return {'results': 'OK'}


def filter_service():
    """ :rtype: dart.service.filter.FilterService """
    return current_app.dart_context.get(FilterService)


def subscription_service():
    """ :rtype: dart.service.subscription.SubscriptionService """
    return current_app.dart_context.get(SubscriptionService)


def subscription_element_service():
    """ :rtype: dart.service.subscription.SubscriptionElementService """
    return current_app.dart_context.get(SubscriptionElementService)
