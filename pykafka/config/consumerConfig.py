

from dataclasses import dataclass
from typing import List

@dataclass
class ConsumerConfig:
    
    bootstrap_servers: List[str]
    group_id: str
    auto_offset_reset: str
    
    
    def __dict__(self):
        return {
            "bootstrap.servers": ",".join(self.bootstrap_servers),
            "group.id": self.group_id,
            "auto.offset.reset": self.auto_offset_reset
        }
    