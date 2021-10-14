from google.api_core import retry
from google.cloud import pubsub_v1


class BaseSubscriber:
    def __init__(self, project_id: str, subscription_id: str, *args, **kwargs):
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_id)

    def get_messages(self, deadline: int = 300, max_messages: int = 1):
        response = self.subscriber.pull(
            request={"subscription": self.subscription_path, "max_messages": max_messages},
            retry=retry.Retry(deadline=deadline),
        )
        for received_message in response.received_messages:
            yield received_message

    def acknowledge_messages(self, ack_ids: list) -> None:
        self.subscriber.acknowledge(request={"subscription": self.subscription_path, "ack_ids": ack_ids})

    def close(self):
        self.subscriber.close()
