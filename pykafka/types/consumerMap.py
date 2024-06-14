from typing import Callable
from pykafka.types import Message


class ConsumerMap:
    topic: str
    func: Callable[[Message], None]
    """
    Represents a mapping between a Kafka topic and a consumer function.

    Attributes:
        topic (str): The Kafka topic to consume from.
        func (Callable[[Message], None]): The consumer function to process the messages.
    """

    def __init__(self, topic: str, func: Callable[[Message], None]) -> None:
        """
        Initializes a new instance of the ConsumerMap class.

        Args:
            topic (str): The Kafka topic to consume from.
            func (Callable[[Message], None]): The consumer function to process the messages.
        """
        self.topic = topic
        self.func = func
