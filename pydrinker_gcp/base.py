import json
import os

from google.api_core import retry
from google.auth import jwt
from google.cloud import pubsub_v1
from pydrinker.exceptions import ProviderError

SUB_AUDIENCE = "https://pubsub.googleapis.com/google.pubsub.v1.Subscriber"


def _get_subscriber():
    credential_file = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if credential_file and os.path.isfile(credential_file):
        return pubsub_v1.SubscriberClient()

    google_service_account = os.environ.get("GOOGLE_SERVICE_ACCOUNT")
    if google_service_account:
        if isinstance(google_service_account, str):
            google_service_account = json.loads(google_service_account)
        credentials = jwt.Credentials.from_service_account_info(
            google_service_account,
            audience=SUB_AUDIENCE,
        )
        return pubsub_v1.SubscriberClient(credentials=credentials)

    raise ProviderError(
        "cannot set subscriber without GOOGLE_APPLICATION_CREDENTIALS or "
        "GOOGLE_SERVICE_ACCOUNT environment variables"
    )


class BaseSubscriber:
    def __init__(self, project_id: str, subscription_id: str, *args, **kwargs):
        self.subscriber = _get_subscriber()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_id)

    def get_messages(
        self, deadline: float = 300, max_messages: int = 1, timeout: float = None, *args, **kwargs
    ):
        response = self.subscriber.pull(
            request={"subscription": self.subscription_path, "max_messages": max_messages},
            retry=retry.Retry(deadline=deadline),
            timeout=timeout,
        )
        for received_message in response.received_messages:
            yield received_message

    def acknowledge_messages(
        self, ack_ids: list, deadline: float = 300, timeout: float = None, *args, **kwargs
    ) -> None:
        self.subscriber.acknowledge(
            request={"subscription": self.subscription_path, "ack_ids": ack_ids},
            retry=retry.Retry(deadline=deadline),
            timeout=timeout,
        )

    def close(self):
        self.subscriber.close()
