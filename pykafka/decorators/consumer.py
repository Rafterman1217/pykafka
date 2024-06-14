
import logging
from typing import Callable, List
from pykafka.types import Message

logger = logging.getLogger("pykafka")



def consumer(topic:str,group_id:str,auto_offset_reset:str) -> Callable[[Callable],Callable]:
    def inner(func:Callable[[Message],None]):
        def wrapper(*args,**kwargs):
            return func(*args,**kwargs)
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper.topic = topic
        wrapper.pykafka_type = "consumer"
        wrapper.group_id = group_id
        wrapper.auto_offset_reset = auto_offset_reset
        return wrapper
    return inner

