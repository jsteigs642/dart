from dart.context.locator import injectable
from dart.message.call import SubscriptionCall
from dart.model.subscription import SubscriptionState


@injectable
class SubscriptionProxy(object):
    def __init__(self, subscription_broker):
        self._subscription_broker = subscription_broker

    def generate_subscription_elements(self, subscription):
        """ :type subscription: dart.model.subscription.Subscription """
        sid = subscription.id
        state = subscription.data.state
        assert state == SubscriptionState.QUEUED, 'expected subscription (id=%s) to be in QUEUED state' % sid
        self._subscription_broker.send_message({'call': SubscriptionCall.GENERATE, 'subscription_id': sid})
