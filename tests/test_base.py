import json
import os
from unittest import mock

from pydrinker_gcp.base import SUB_AUDIENCE, BaseSubscriber, _get_subscriber


@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.retry.Retry")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_get_messages_with_messages(
    mocked_subscriber_client, mocked_retry, mocked_get_subscriber, pull_response
):
    expected_deadline = 123
    expected_max_messages = 3
    mocked_subscriber_client.return_value.pull.return_value = pull_response
    mocked_get_subscriber.return_value = mocked_subscriber_client()

    base_sub = BaseSubscriber(project_id="xablau-xebleu-123456", subscription_id="sample-sub")
    messages = list(base_sub.get_messages(deadline=expected_deadline, max_messages=expected_max_messages))

    mocked_retry.assert_called_once_with(deadline=expected_deadline)
    mocked_subscriber_client.return_value.pull.assert_called_once_with(
        request={"subscription": base_sub.subscription_path, "max_messages": expected_max_messages},
        retry=mocked_retry(deadline=expected_deadline),
        timeout=None,
    )

    received_message = messages.pop()
    assert len(messages) == 0
    assert received_message.message.data == b'{"xablau": "xebleu"}'


@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.retry.Retry")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_get_messages_without_messages(
    mocked_subscriber_client, mocked_retry, mocked_get_subscriber, pull_response
):
    expected_deadline = 123
    expected_max_messages = 3
    mocked_get_subscriber.return_value = mocked_subscriber_client()

    base_sub = BaseSubscriber(project_id="xablau-xebleu-123456", subscription_id="sample-sub")
    messages = list(base_sub.get_messages(deadline=expected_deadline, max_messages=expected_max_messages))

    mocked_retry.assert_called_once_with(deadline=expected_deadline)
    mocked_subscriber_client.return_value.pull.assert_called_once_with(
        request={"subscription": base_sub.subscription_path, "max_messages": expected_max_messages},
        retry=mocked_retry(deadline=expected_deadline),
        timeout=None,
    )

    assert len(messages) == 0


@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.retry.Retry")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_acknowledge_messages_call(mocked_subscriber_client, mocked_retry, mocked_get_subscriber):
    expected_deadline = 123
    mocked_get_subscriber.return_value = mocked_subscriber_client()

    base_sub = BaseSubscriber(project_id="xablau-xebleu-123456", subscription_id="sample-sub")
    base_sub.acknowledge_messages(["xablau123"])

    mocked_subscriber_client.return_value.acknowledge.assert_called_once_with(
        request={"subscription": base_sub.subscription_path, "ack_ids": ["xablau123"]},
        retry=mocked_retry(deadline=expected_deadline),
        timeout=None,
    )


@mock.patch("pydrinker_gcp.base._get_subscriber")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_close_call(mocked_subscriber_client, mocked_get_subscriber):
    mocked_get_subscriber.return_value = mocked_subscriber_client()

    base_sub = BaseSubscriber(project_id="xablau-xebleu-123456", subscription_id="sample-sub")
    base_sub.close()

    mocked_subscriber_client.return_value.close.assert_called_once_with()


@mock.patch.dict(os.environ, {"GOOGLE_APPLICATION_CREDENTIALS": "credential.json"})
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
@mock.patch("pydrinker_gcp.base.os.path.isfile")
def test_get_subscriber_with_credential_file(mocked_isfile, mocked_subscriber_client):
    mocked_isfile.return_value = True
    subscriber_client = _get_subscriber()

    mocked_subscriber_client.assert_called_once_with()
    assert subscriber_client == mocked_subscriber_client()


@mock.patch("pydrinker_gcp.base.jwt.Credentials.from_service_account_info")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_get_subscriber_with_service_account_value(
    mocked_subscriber_client, mocked_from_service_account_info
):
    google_service_account = (
        '{"type": "service_account", "project_id": "fake-project-123456", "private_key_id": "123"}'
    )
    with mock.patch.dict(os.environ, {"GOOGLE_SERVICE_ACCOUNT": google_service_account}):
        subscriber_client = _get_subscriber()

        mocked_from_service_account_info.assert_called_once_with(
            json.loads(google_service_account), audience=SUB_AUDIENCE
        )
        mocked_subscriber_client.assert_called_once_with(credentials=mocked_from_service_account_info())
        assert subscriber_client == mocked_subscriber_client()


@mock.patch("pydrinker_gcp.base.jwt.Credentials.from_service_account_info")
@mock.patch("pydrinker_gcp.base.pubsub_v1.SubscriberClient")
def test_get_subscriber_without_any_environment_variable(
    mocked_subscriber_client, mocked_from_service_account_info
):
    google_service_account = (
        '{"type": "service_account", "project_id": "fake-project-123456", "private_key_id": "123"}'
    )
    with mock.patch.dict(os.environ, {"GOOGLE_SERVICE_ACCOUNT": google_service_account}):
        subscriber_client = _get_subscriber()

        mocked_from_service_account_info.assert_called_once_with(
            json.loads(google_service_account), audience=SUB_AUDIENCE
        )
        mocked_subscriber_client.assert_called_once_with(credentials=mocked_from_service_account_info())
        assert subscriber_client == mocked_subscriber_client()
