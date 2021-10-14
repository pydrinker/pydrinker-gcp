import json
import logging

from google.cloud.pubsub_v1.types import ReceivedMessage
from pydrinker.message_translators import AbstractMessageTranslator

logger = logging.getLogger(__name__)


class SubscriptionMessageTranslator(AbstractMessageTranslator):
    def translate(self, message: ReceivedMessage):
        """Translate a given message to an appropriate format to message processing.

        This method should return a `dict` instance with two keys: `content`
        and `metadata`.
        The `content` should contain the translated message and, `metadata` a
        dictionary with translation metadata.
        """
        translated_message = {"content": None}
        pubsub_message = message.message
        translated_message["metadata"] = {
            "ack_id": message.ack_id,
            "message_id": pubsub_message.message_id,
            "publish_time": pubsub_message.publish_time,
            "ordering_key": pubsub_message.ordering_key,
            "attributes": pubsub_message.attributes,
        }

        try:
            decoded_message_data = pubsub_message.data.decode("utf-8")
        except UnicodeDecodeError as exc:
            logger.error(f"error={exc!r}, message={message!r}")
            return translated_message

        try:
            translated_message["content"] = json.loads(decoded_message_data)
        except json.decoder.JSONDecodeError as exc:
            logger.error(f"error={exc!r}, message={message!r}")
            return translated_message

        return translated_message
