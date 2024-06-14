


from dataclasses import dataclass


@dataclass
class ProducerConfig:
    
    bootstrap_servers: str

    def __dict__(self):
        return {
            "bootstrap.servers": self.bootstrap_servers
        }