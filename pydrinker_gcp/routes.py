from pydrinker.routes import DrinkerRoute

from .message_translators import SubscriptionMessageTranslator
from .providers import SubscriptionProvider


class SubscriptionRoute(DrinkerRoute):
    def __init__(
        self,
        project_id,
        subscription_id,
        provider_options=None,
        message_translator=None,
        name=None,
        *args,
        **kwargs,
    ):
        provider_options = provider_options or {}
        provider = SubscriptionProvider(
            project_id=project_id, subscription_id=subscription_id, **provider_options
        )
        kwargs["provider"] = provider
        kwargs["message_translator"] = message_translator or SubscriptionMessageTranslator()
        kwargs["name"] = name or f"{project_id}/{subscription_id}"

        super().__init__(*args, **kwargs)
