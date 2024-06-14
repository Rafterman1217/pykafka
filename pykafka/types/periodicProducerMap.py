from typing import Callable


class PeriodicProducerMap:
    topic:str
    func:Callable[[],None]
    interval:int
    
    def __init__(self,topic:str,func:Callable[[],None],interval:int) -> None:
        self.topic = topic
        self.func = func
        self.interval = interval
