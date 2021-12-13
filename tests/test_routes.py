from unittest import mock

from pydrinker.routes import DrinkerRoute

from pydrinker_gcp.message_translators import SubscriptionMessageTranslator
from pydrinker_gcp.routes import SubscriptionRoute


@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_subscription_route_instance(mocked_subscriber_client, mocked_get_subscriber):
    def fake_function(message, *args):
        pass

    subscription_route = SubscriptionRoute(
        project_id="xablau-xebleu-123456",
        subscription_id="sample-sub",
        handler=fake_function,
    )

    assert isinstance(subscription_route, DrinkerRoute)
    assert isinstance(subscription_route.message_translator, SubscriptionMessageTranslator)
    assert subscription_route.name == "xablau-xebleu-123456/sample-sub"
    assert subscription_route.handler == fake_function
