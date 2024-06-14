

from typing import List
from pykafka.config import ConsumerConfig
from confluent_kafka import Consumer
from pykafka.types import Message

import logging

logger = logging.getLogger("pykafka")


class PyConsumer(Consumer):
    """
    A wrapper class for the Kafka Consumer that provides additional functionality.
    """

    def __init__(self, conf: ConsumerConfig):
        """
        Initializes a new instance of the PyConsumer class.

        Args:
            conf (ConsumerConfig): The configuration for the consumer.
        """
        super().__init__(conf.__dict__())

    def close(self) -> None:
        """
        Closes the consumer.

        Returns:
            None
        """
        super().close()
        logger.info("Closed consumer")

    def subscribe(self, topics: List[str]) -> None:
        """
        Subscribes to the specified topics.

        Args:
            topics (List[str]): The topics to subscribe to.

        Returns:
            None
        """
        super().subscribe(topics)
        logger.info(f"Subscribed to topics {topics}")

    def poll(self, timeout: float = -1) -> Message:
        """
        Polls for new messages.

        Args:
            timeout (float, optional): The maximum time to wait for new messages, in seconds.
                If set to -1 (default), it will block until a message is available.

        Returns:
            Message: The next available message, or None if no message is available within the timeout.
        """
        return super().poll(timeout)

    def commit(self) -> None:
        """
        Commits the current offset for the consumer.

        Returns:
            None
        """
        super().commit()

    def __del__(self):
        """
        Destructor that automatically closes the consumer when the object is deleted.

        Returns:
            None
        """
        self.close()
