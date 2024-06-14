from typing import Callable


class PeriodicProducerMap:
    topic: str
    func: Callable[[], None]
    interval: int

    """
    Represents a mapping between a topic, a function, and an interval for periodic execution.

    Attributes:
        topic (str): The topic associated with the producer.
        func (Callable[[], None]): The function to be executed periodically.
        interval (int): The interval (in seconds) at which the function should be executed.
    """

    def __init__(self, topic: str, func: Callable[[], None], interval: int) -> None:
        self.topic = topic
        self.func = func
        self.interval = interval
