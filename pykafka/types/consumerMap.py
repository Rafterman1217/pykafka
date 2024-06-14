from typing import Callable
from pykafka.types import Message

class ConsumerMap:
    topic:str
    func:Callable[[Message],None]
    
    def __init__(self,topic:str,func:Callable[[Message],None]) -> None:
        self.topic = topic
        self.func = func    
    