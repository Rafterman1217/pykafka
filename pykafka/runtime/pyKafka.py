
from pathlib import Path
from types import ModuleType
from typing import Callable, List
from pykafka.config.consumerConfig import ConsumerConfig
from pykafka.runtime.pyconsumer import PyConsumer
from pykafka.types import Message,ConsumerMap,PeriodicProducerMap
from logging import getLogger
import threading
from ischedule import schedule,run_loop
import os

logger = getLogger("pykafka")

class PyKafka:
    consumer_map : List[ConsumerMap] = []
    periodic_producer_map : List[PeriodicProducerMap] = []
    registered_groups : List[str] = []
    modules : List[ModuleType] = []
    consumer_threads = {}
    periodic_producer_threads = {}
    consumers = {}
    bootstrap_servers:List[str]
    
    def __init__(self,modules:List[ModuleType],bootstrap_servers:List[str]) -> None:
        self.modules = modules
        self.bootstrap_servers = bootstrap_servers
        self.discover_decorated()
        self.make_thread_map()
    
    def add_consumer_function(self,cmt:ConsumerMap)->None:
        if cmt.topic in [cmt.topic for cmt in self.consumer_map] and cmt.func.group_id in [cmt.func.group_id for cmt in self.consumer_map]:
            logger.error(f"Consumer function for topic {cmt.topic} already exists in group {cmt.func.group_id}")
            raise Exception(f"Consumer function for topic {cmt.topic} already exists in group {cmt.func.group_id}")
        self.consumer_map.append(cmt)
        logger.info(f"Added consumer function for topic {cmt.topic}, group {cmt.func.group_id}, function {cmt.func.__name__}")
        
    def add_periodic_producer_function(self,ppmt:PeriodicProducerMap)->None:
        self.periodic_producer_map.append(ppmt)
        logger.info(f"Added periodic producer function for topic {ppmt.topic}, interval {ppmt.interval}, function {ppmt.func.__name__}")

    def discover_decorated(self) -> None:
        for module in self.modules:
            for attr in dir(module):
                if hasattr(module,attr) and callable(getattr(module,attr)) and hasattr(getattr(module,attr),"pykafka_type"):
                    if getattr(module,attr).pykafka_type == "consumer":
                        self.add_consumer_function(ConsumerMap(getattr(module,attr).topic,getattr(module,attr)))
                    elif getattr(module,attr).pykafka_type == "periodic_producer":
                        self.add_periodic_producer_function(PeriodicProducerMap(getattr(module,attr).topic,getattr(module,attr),getattr(module,attr).interval_s))
                    elif getattr(module,attr).pykafka_type == "producer":
                        pass
                    else:
                        raise Exception(f"Unknown pykafka type {getattr(module,attr).pykafka_type}")
                    
    def make_thread_map(self) -> None:
        for cmt in self.consumer_map:
            if cmt.func.group_id not in self.consumer_threads:
                self.consumer_threads[cmt.func.group_id] = []
            self.consumer_threads[cmt.func.group_id].append(cmt.func)
            logger.info(f"Added consumer function to thread map for group {cmt.func.group_id}")
        for ppmt in self.periodic_producer_map:
            if ppmt.topic not in self.periodic_producer_threads:
                self.periodic_producer_threads[ppmt.topic] = []
            self.periodic_producer_threads[ppmt.topic].append(ppmt.func)
            logger.info(f"Added periodic producer function to thread map for topic {ppmt.topic}")
        logger.info(f"Thread map: {self.periodic_producer_threads}")
    
    def process_consumer(self,functions:List[Callable[[Message],None]]) -> None:
        conf = ConsumerConfig(bootstrap_servers=self.bootstrap_servers,group_id=functions[0].group_id,auto_offset_reset=functions[0].auto_offset_reset)
        consumer = PyConsumer(conf)
        logger.info(f"Starting consumer for group {functions[0].group_id} on topic {functions[0].topic} based on config {conf}")
        topics = [func.topic for func in functions]
        consumer.subscribe(topics)
        logger.info(f"Subscribed to topics {topics}")
        self.consumers[functions[0].group_id] = consumer
        while True:
            msg = consumer.poll()
            if msg is not None:
                for func in functions:
                    if msg.topic() == func.topic:
                        func(msg)
            consumer.commit()
            
    def process_periodic_producer(self,func:Callable[[],dict]) -> None:
        schedule(func,interval=func.interval_s)
        run_loop()
            
    def run(self) -> None:
        tmap = []
        for funcs in self.consumer_threads.values():
            tmap.append(threading.Thread(target=self.process_consumer,args=(funcs,),daemon=False))

        for funcs in self.periodic_producer_threads.values():
            for func in funcs:
                tmap.append(threading.Thread(target=self.process_periodic_producer,args=(func,),daemon=False))
     
        for t in tmap:
            t.start()
            logger.info(f"Started thread {t}")
            
                     
    def __del__(self):
        for consumer in self.consumers.values():
            consumer.close()         
            logger.info(f"Closed consumer for group {consumer.group_id}")
            
    
    @staticmethod
    def get_folders(base_dir: Path) -> List[str]:
        folders = []
        for entry in os.scandir(base_dir):
            if entry.is_dir():
                folders.append(entry.name)
        return folders
        
    @classmethod
    def django_init(cls,base_dir:Path,bootstrap_servers:List[str])->'PyKafka':
        folders = cls.get_folders(base_dir)
        modules = []
        for folder in folders:
            try:
                modules.append(__import__(f"{folder}.services",fromlist=[""]))
                logger.info(f"Imported module {folder}")
            except Exception as e:
                logger.error(f"Failed to import module {folder}: {e}")
        return cls(modules,bootstrap_servers)
        