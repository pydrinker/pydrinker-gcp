from unittest import mock

import pytest
from google.cloud.pubsub_v1.types import PubsubMessage, PullResponse, ReceivedMessage

from pydrinker_gcp.base import BaseSubscriber


@pytest.fixture
def pubsub_message():
    return PubsubMessage(data=b"{'xablau': 'xebleu'}")


@pytest.fixture
def received_message(pubsub_message):
    return ReceivedMessage(
        ack_id="123abc",
        message=pubsub_message,
    )


@pytest.fixture
def pull_response(received_message):
    return PullResponse(received_messages=[received_message])


@mock.patch("pydrinker_gcp.base.retry.Retry")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_get_messages_with_messages(mocked_subscriber_client, mocked_retry, pull_response):
    expected_deadline = 123
    expected_max_messages = 3
    mocked_subscriber_client.return_value.pull.return_value = pull_response

    base_sub = BaseSubscriber(project_id="xablau-xebleu-123456", subscription_id="sample-sub")
    messages = list(base_sub.get_messages(deadline=expected_deadline, max_messages=expected_max_messages))

    mocked_retry.assert_called_once_with(deadline=expected_deadline)
    mocked_subscriber_client.return_value.pull.assert_called_once_with(
        request={"subscription": base_sub.subscription_path, "max_messages": expected_max_messages},
        retry=mocked_retry(deadline=expected_deadline),
    )

    received_message = messages.pop()
    assert len(messages) == 0
    assert received_message.message.data == b"{'xablau': 'xebleu'}"


@mock.patch("pydrinker_gcp.base.retry.Retry")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_get_messages_without_messages(mocked_subscriber_client, mocked_retry, pull_response):
    expected_deadline = 123
    expected_max_messages = 3

    base_sub = BaseSubscriber(project_id="xablau-xebleu-123456", subscription_id="sample-sub")
    messages = list(base_sub.get_messages(deadline=expected_deadline, max_messages=expected_max_messages))

    mocked_retry.assert_called_once_with(deadline=expected_deadline)
    mocked_subscriber_client.return_value.pull.assert_called_once_with(
        request={"subscription": base_sub.subscription_path, "max_messages": expected_max_messages},
        retry=mocked_retry(deadline=expected_deadline),
    )

    assert len(messages) == 0


@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_acknowledge_messages_call(mocked_subscriber_client):
    base_sub = BaseSubscriber(project_id="xablau-xebleu-123456", subscription_id="sample-sub")
    base_sub.acknowledge_messages(["xablau123"])

    mocked_subscriber_client.return_value.acknowledge.assert_called_once_with(
        request={"subscription": base_sub.subscription_path, "ack_ids": ['xablau123']},
    )


@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_close_call(mocked_subscriber_client):
    base_sub = BaseSubscriber(project_id="xablau-xebleu-123456", subscription_id="sample-sub")
    base_sub.close()

    mocked_subscriber_client.return_value.close.assert_called_once_with()
