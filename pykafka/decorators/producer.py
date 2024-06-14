
import json
import time
from typing import Callable
from logging import getLogger
from pykafka.runtime import PyProducer
from pykafka.config import ProducerConfig

logger = getLogger("pykafka")



def producer(topic:str,config:ProducerConfig) -> Callable[[Callable],Callable]:
    py_producer = PyProducer(config)
    def inner(func:Callable[[],dict]):
        def wrapper(*args,**kwargs):
            res = func(*args,**kwargs)
            py_producer.produce(topic=topic,key=str(time.time()),value=json.dumps(res).encode("utf-8"))
            py_producer.flush()
            return res
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper.topic = topic
        wrapper.pykafka_type = "producer"
        return wrapper

    return inner