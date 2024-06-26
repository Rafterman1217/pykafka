
import json
import time
from typing import Callable
from logging import getLogger
from pykafka.runtime import PyProducer
from pykafka.config import ProducerConfig

logger = getLogger("pykafka")


def periodic_producer(topic: str, interval_s: int, config: ProducerConfig) -> Callable[[Callable], Callable]:
    """
    Decorator function that creates a periodic producer.

    Args:
        topic (str): The Kafka topic to produce messages to.
        interval_s (int): The interval in seconds between producing messages.
        config (ProducerConfig): The configuration for the producer.

    Returns:
        Callable: The decorated function.

    """
    producer = PyProducer(config)

    def inner(func: Callable[[], dict]):
        def wrapper(*args, **kwargs):
            res = func(*args, **kwargs)
            producer.produce(topic=topic, key=str(time.time()),
                             value=json.dumps(res).encode("utf-8"))
            producer.flush()
            return res
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper.topic = topic
        wrapper.pykafka_type = "periodic_producer"
        wrapper.interval_s = interval_s
        return wrapper
    return inner
