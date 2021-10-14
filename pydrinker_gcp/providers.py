import logging

from google import api_core
from pydrinker.exceptions import ProviderError
from pydrinker.providers import AbstractProvider

from .base import BaseSubscriber

logger = logging.getLogger(__name__)

GOOGLE_CORE_EXCEPTIONS = (
    api_core.exceptions.Aborted,
    api_core.exceptions.DeadlineExceeded,
    api_core.exceptions.InternalServerError,
    api_core.exceptions.ResourceExhausted,
    api_core.exceptions.ServiceUnavailable,
    api_core.exceptions.Unknown,
    api_core.exceptions.Cancelled,
)


class SubscriptionProvider(AbstractProvider, BaseSubscriber):
    def __init__(self, project_id: str, subscription_id: str, options=None, **kwargs):
        self.project_id = project_id
        self.subscription_id = subscription_id
        self._options = options or {}
        super().__init__(project_id, subscription_id, **kwargs)

    async def fetch_messages(self):
        """Return a sequence of messages to be processed.

        If no messages are available, this coroutine should return an empty list.
        """
        logger.debug(f"fetching messages on {self.subscription_id}")
        try:
            messages = list(self.get_messages(**self._options))
        except GOOGLE_CORE_EXCEPTIONS as exc:
            raise ProviderError(
                f"error to fetch messages from subscriber_id={self.subscription_id!r}: {exc}"
            ) from exc

        return messages or []

    async def confirm_message(self, message):
        """Confirm the message processing.

        After the message confirmation we should not receive the same message again.
        This usually means we need to delete/acknowledge the message in the provider.
        """
        ack_id = message.ack_id
        logger.info(f"confirm message (ack/deletion), ack_id={ack_id}")
        try:
            self.acknowledge_messages(ack_ids=[ack_id], **self._options)
        except GOOGLE_CORE_EXCEPTIONS as exc:
            raise ProviderError(
                f"error to confirm messages from subscriber_id={self.subscription_id!r}: {exc}"
            ) from exc

    def stop(self):
        """Stop the provider.

        If needed, the provider should perform clean-up actions.
        This method is called whenever we need to shutdown the provider.
        """
        logger.info(f"stopping {self}")
        self.close()
        return super().stop()
