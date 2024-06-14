
import logging
from typing import Callable, List
from pykafka.types import Message

logger = logging.getLogger("pykafka")


def consumer(topic: str, group_id: str, auto_offset_reset: str) -> Callable[[Callable], Callable]:
    """
    Decorator for defining a Kafka consumer.

    Args:
        topic (str): The topic to consume messages from.
        group_id (str): The consumer group ID.
        auto_offset_reset (str): The strategy to use for resetting the offset when there is no initial offset in Kafka or if the current offset does not exist anymore on the server.

    Returns:
        Callable: The decorated function.

    Example:
        @consumer(topic='my_topic', group_id='my_group', auto_offset_reset='earliest')
        def process_message(message):
            # Process the Kafka message
            pass
    """
    def inner(func: Callable[[Message], None]):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper.topic = topic
        wrapper.pykafka_type = "consumer"
        wrapper.group_id = group_id
        wrapper.auto_offset_reset = auto_offset_reset
        return wrapper
    return inner
