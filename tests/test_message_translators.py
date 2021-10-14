import datetime
from unittest import mock

from proto.datetime_helpers import DatetimeWithNanoseconds

from pydrinker_gcp.message_translators import SubscriptionMessageTranslator


@mock.patch("pydrinker_gcp.message_translators.logger.error")
def test_subscription_message_translator_translate(mocked_logger_error, received_message):
    translator = SubscriptionMessageTranslator()
    assert translator.translate(received_message) == {
        "content": {"xablau": "xebleu"},
        "metadata": {
            "ack_id": "123abc",
            "attributes": {},
            "ordering_key": "",
            "message_id": "3175906331341274",
            "publish_time": DatetimeWithNanoseconds(
                2021, 10, 11, 21, 2, 49, 951000, tzinfo=datetime.timezone.utc
            ),
        },
    }
    mocked_logger_error.assert_not_called()


@mock.patch("pydrinker_gcp.message_translators.logger.error")
def test_subscription_message_translator_without_bytes_content(
    mocked_logger_error, pubsub_message, received_message
):
    pubsub_message.data = "olokinho meu!"
    received_message.message = pubsub_message

    translator = SubscriptionMessageTranslator()
    assert translator.translate(received_message) == {
        "content": None,
        "metadata": {
            "ack_id": "123abc",
            "attributes": {},
            "ordering_key": "",
            "message_id": "3175906331341274",
            "publish_time": DatetimeWithNanoseconds(
                2021, 10, 11, 21, 2, 49, 951000, tzinfo=datetime.timezone.utc
            ),
        },
    }

    error_log = (
        "error=UnicodeDecodeError('utf-8', b'\\xa2Z$\\x8axh\\x99\\xeb', 0, 1, 'invalid start byte'), "
        'message=ack_id: "123abc"\nmessage {\n  data: "\\242Z$\\212xh\\231\\353"\n  '
        'message_id: "3175906331341274"\n  publish_time {\n    seconds: 1633986169\n    '
        "nanos: 951000000\n  }\n}\n"
    )
    mocked_logger_error.assert_called_once_with(error_log)


@mock.patch("pydrinker_gcp.message_translators.logger.error")
def test_subscription_message_translator_without_json_content(
    mocked_logger_error, pubsub_message, received_message
):
    pubsub_message.data = b"olokinho meu!"
    received_message.message = pubsub_message

    translator = SubscriptionMessageTranslator()
    assert translator.translate(received_message) == {
        "content": None,
        "metadata": {
            "ack_id": "123abc",
            "attributes": {},
            "ordering_key": "",
            "message_id": "3175906331341274",
            "publish_time": DatetimeWithNanoseconds(
                2021, 10, 11, 21, 2, 49, 951000, tzinfo=datetime.timezone.utc
            ),
        },
    }
    error_log = (
        "error=JSONDecodeError('Expecting value: line 1 column 1 (char 0)'), "
        'message=ack_id: "123abc"\nmessage {\n  data: "olokinho meu!"\n  '
        'message_id: "3175906331341274"\n  publish_time {\n    seconds: 1633986169\n    '
        "nanos: 951000000\n  }\n}\n"
    )
    mocked_logger_error.assert_called_once_with(error_log)
