
        

from pykafka.config.producerConfig import ProducerConfig
from confluent_kafka import Producer

import logging

logger = logging.getLogger("pykafka")


class PyProducer(Producer):
    """
    A custom producer class that extends the base Producer class.
    """

    def __init__(self, conf: ProducerConfig):
        """
        Initializes a new instance of the PyProducer class.

        Args:
            conf (ProducerConfig): The configuration object for the producer.
        """
        super().__init__(conf.__dict__())

    def produce(self, topic: str, key: str, *args, **kwargs) -> None:
        """
        Produces a message to the specified topic.

        Args:
            topic (str): The name of the topic to produce the message to.
            key (str): The key associated with the message.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.

        Returns:
            None
        """
        super().produce(*args, topic=topic, key=key, **kwargs)
        logger.info(f"Produced message to topic {topic}")

    def flush(self) -> None:
        """
        Flushes the producer.

        Returns:
            None
        """
        super().flush()
        logger.info("Flushed producer")
