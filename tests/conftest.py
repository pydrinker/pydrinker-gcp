import pytest
from google.cloud.pubsub_v1.types import PubsubMessage, PullResponse, ReceivedMessage


@pytest.fixture
def pubsub_message():
    return PubsubMessage(
        data=b'{"xablau": "xebleu"}',
        message_id="3175906331341274",
        publish_time={"seconds": 1633986169, "nanos": 951000000},
    )


@pytest.fixture
def received_message(pubsub_message):
    return ReceivedMessage(
        ack_id="123abc",
        message=pubsub_message,
    )


@pytest.fixture
def pull_response(received_message):
    return PullResponse(received_messages=[received_message])
