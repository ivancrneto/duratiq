"""Optional Dramatiq broker used to enqueue a tick when retrying a run.

The admin doesn't hold the workflow registry, so it can't run a tick itself. It
just drops a ``duratiq_tick`` message on the broker the duratiq workers consume;
a worker (which has the registry) picks it up and resumes the run.
"""

from __future__ import annotations

from functools import lru_cache

import dramatiq

from .core.config import settings


class BrokerNotConfigured(Exception):
    """Retry was attempted but DURATIQ_BROKER_URL is not set."""


@lru_cache(maxsize=1)
def get_broker() -> dramatiq.Broker | None:
    url = settings.broker_url
    if not url:
        return None
    if url.startswith("redis"):
        from dramatiq.brokers.redis import RedisBroker

        return RedisBroker(url=url)
    if url.startswith(("amqp", "amqps")):
        from dramatiq.brokers.rabbitmq import RabbitmqBroker

        return RabbitmqBroker(url=url)
    raise ValueError(f"unsupported DURATIQ_BROKER_URL scheme: {url!r}")


def enqueue_tick(run_id: str) -> None:
    """Send a ``duratiq_tick`` message for ``run_id`` to the configured broker."""
    broker = get_broker()
    if broker is None:
        raise BrokerNotConfigured("DURATIQ_BROKER_URL is not set")
    broker.declare_queue(settings.broker_queue)
    message = dramatiq.Message(
        queue_name=settings.broker_queue,
        actor_name="duratiq_tick",
        args=(run_id,),
        kwargs={},
        options={},
    )
    broker.enqueue(message)
