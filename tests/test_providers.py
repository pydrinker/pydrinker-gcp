from unittest import mock

import pytest
from google.api_core.exceptions import DeadlineExceeded
from google.cloud.pubsub_v1.types import ReceivedMessage
from pydrinker.exceptions import ProviderError

from pydrinker_gcp.providers import SubscriptionProvider


@pytest.mark.asyncio
@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
@mock.patch("pydrinker_gcp.providers.BaseSubscriber.get_messages")
async def test_subscription_provider_fetch_messages_with_messages(
    mocked_get_messages, mocked_subscriber_client, mocked_get_subscriber, received_message
):
    mocked_get_messages.return_value = (message for message in [received_message, received_message])

    subscription_provider = SubscriptionProvider(
        project_id="xablau-xebleu-123456", subscription_id="sample-sub", options={"some": "parameter"}
    )
    messages = await subscription_provider.fetch_messages()
    assert len(messages) == 2
    assert isinstance(messages[0], ReceivedMessage)
    assert isinstance(messages[1], ReceivedMessage)
    mocked_get_messages.assert_called_once_with(some="parameter")


@pytest.mark.asyncio
@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
@mock.patch("pydrinker_gcp.providers.BaseSubscriber.get_messages")
async def test_subscription_provider_fetch_messages_without_messages(
    mocked_get_messages, mocked_subscriber_client, mocked_get_subscriber, received_message
):
    subscription_provider = SubscriptionProvider(
        project_id="xablau-xebleu-123456", subscription_id="sample-sub", options={"some": "parameter"}
    )
    messages = await subscription_provider.fetch_messages()
    assert len(messages) == 0
    mocked_get_messages.assert_called_once_with(some="parameter")


@pytest.mark.asyncio
@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
@mock.patch("pydrinker_gcp.providers.BaseSubscriber.get_messages")
async def test_subscription_provider_fetch_messages_with_timeout(
    mocked_get_messages, mocked_subscriber_client, mocked_get_subscriber, received_message
):
    mocked_get_messages.side_effect = DeadlineExceeded(message="Deadline Exceeded")

    subscription_provider = SubscriptionProvider(
        project_id="xablau-xebleu-123456", subscription_id="sample-sub", options={"some": "parameter"}
    )

    with pytest.raises(ProviderError) as exc:
        await subscription_provider.fetch_messages()

    assert "504 Deadline Exceeded" in str(exc)
    mocked_get_messages.assert_called_once_with(some="parameter")


@pytest.mark.asyncio
@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
@mock.patch("pydrinker_gcp.providers.BaseSubscriber.acknowledge_messages")
async def test_subscription_provider_confirm_message_success(
    mocked_acknowledge_messages,
    mocked_subscriber_client,
    mocked_get_subscriber,
    received_message,
):
    subscription_provider = SubscriptionProvider(
        project_id="xablau-xebleu-123456", subscription_id="sample-sub", options={"some": "parameter"}
    )
    assert await subscription_provider.confirm_message(received_message) is None
    mocked_acknowledge_messages.assert_called_once_with(ack_ids=["123abc"], some="parameter")


@pytest.mark.asyncio
@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
@mock.patch("pydrinker_gcp.providers.BaseSubscriber.acknowledge_messages")
async def test_subscription_provider_confirm_message_with_timeout(
    mocked_acknowledge_messages, mocked_subscriber_client, mocked_get_subscriber, received_message
):
    mocked_acknowledge_messages.side_effect = DeadlineExceeded(message="Deadline Exceeded")

    subscription_provider = SubscriptionProvider(
        project_id="xablau-xebleu-123456", subscription_id="sample-sub", options={"some": "parameter"}
    )
    with pytest.raises(ProviderError) as exc:
        await subscription_provider.confirm_message(received_message)

    assert "504 Deadline Exceeded" in str(exc)
    mocked_acknowledge_messages.assert_called_once_with(ack_ids=["123abc"], some="parameter")


@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
@mock.patch("pydrinker_gcp.providers.BaseSubscriber.close")
def test_subscription_provider_stop_success(mocked_close, mocked_subscriber_client, mocked_get_subscriber):
    subscription_provider = SubscriptionProvider(
        project_id="xablau-xebleu-123456", subscription_id="sample-sub", options={"some": "parameter"}
    )
    assert subscription_provider.stop() is None
    mocked_close.assert_called_once_with()
