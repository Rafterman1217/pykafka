

from dataclasses import dataclass


@dataclass
class ProducerConfig:
    """
    Configuration class for Kafka producer.
    """

    bootstrap_servers: str

    def __dict__(self):
        """
        Returns the configuration as a dictionary.
        """
        return {
            "bootstrap.servers": self.bootstrap_servers
        }
