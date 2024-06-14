
        

from typing import List
from pykafka.config import ConsumerConfig
from confluent_kafka import Consumer
from pykafka.types import Message

import logging

logger = logging.getLogger("pykafka")

class PyConsumer(Consumer):
    
    def __init__(self,conf:ConsumerConfig):
        super().__init__(conf.__dict__())
        
    def close(self)->None:
        super().close()
        logger.info("Closed consumer")
        
    def subscribe(self,topics:List[str])->None:
        super().subscribe(topics)
        logger.info(f"Subscribed to topics {topics}")
        
    def poll(self,timeout:float=-1)-> Message:
        return super().poll(timeout)
        
    def commit(self)->None:
        super().commit()
        
    def __del__(self):
        self.close()
        

