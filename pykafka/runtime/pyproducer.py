
        

from pykafka.config.producerConfig import ProducerConfig
from confluent_kafka import Producer
from pykafka.types import Message

import logging

logger = logging.getLogger("pykafka")


class PyProducer(Producer):

    def __init__(self,conf:ProducerConfig):
        super().__init__(conf.__dict__())
        
    def produce(self,topic:str,key:str,*args,**kwargs)->None:
        super().produce(*args,topic=topic,key=key,**kwargs)
        logger.info(f"Produced message to topic {topic}")
        
    def flush(self)->None:
        super().flush()
        logger.info("Flushed producer")
        