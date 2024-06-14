
from pathlib import Path
from types import ModuleType
from typing import Callable, List
from pykafka.config.consumerConfig import ConsumerConfig
from pykafka.runtime.pyconsumer import PyConsumer
from pykafka.types import Message, ConsumerMap, PeriodicProducerMap
from logging import getLogger
import threading
from ischedule import schedule, run_loop
import os

logger = getLogger("pykafka")


class PyKafka:
    """
    A class representing a PyKafka instance.

    Attributes:
        consumer_map (List[ConsumerMap]): A list of consumer mappings.
        periodic_producer_map (List[PeriodicProducerMap]): A list of periodic producer mappings.
        registered_groups (List[str]): A list of registered consumer groups.
        modules (List[ModuleType]): A list of modules to be imported.
        consumer_threads (dict): A dictionary mapping consumer group IDs to consumer threads.
        periodic_producer_threads (dict): A dictionary mapping topics to periodic producer threads.
        consumers (dict): A dictionary mapping consumer group IDs to consumer instances.
        bootstrap_servers (List[str]): A list of bootstrap servers.

    Methods:
        __init__(modules: List[ModuleType], bootstrap_servers: List[str]) -> None:
            Initializes a PyKafka instance.
        add_consumer_function(cmt: ConsumerMap) -> None:
            Adds a consumer function to the consumer map.
        add_periodic_producer_function(ppmt: PeriodicProducerMap) -> None:
            Adds a periodic producer function to the periodic producer map.
        discover_decorated() -> None:
            Discovers decorated functions in the imported modules.
        make_thread_map() -> None:
            Creates a thread map based on the consumer and periodic producer maps.
        process_consumer(functions: List[Callable[[Message], None]]) -> None:
            Processes messages for the specified consumer functions.
        process_periodic_producer(func: Callable[[], dict]) -> None:
            Processes periodic producer functions.
        run() -> None:
            Runs the PyKafka instance by starting the consumer and periodic producer threads.
        __del__() -> None:
            Closes the consumer instances when the PyKafka instance is deleted.
        get_folders(base_dir: Path) -> List[str]:
            Returns a list of folders in the specified base directory.
        django_init(base_dir: Path, bootstrap_servers: List[str]) -> PyKafka:
            Initializes a PyKafka instance for Django projects.

    """

    consumer_map: List[ConsumerMap] = []
    periodic_producer_map: List[PeriodicProducerMap] = []
    registered_groups: List[str] = []
    modules: List[ModuleType] = []
    consumer_threads = {}
    periodic_producer_threads = {}
    consumers = {}
    bootstrap_servers: List[str]

    def __init__(self, modules: List[ModuleType], bootstrap_servers: List[str]) -> None:
        """
        Initializes a PyKafka instance.

        Args:
            modules (List[ModuleType]): A list of modules to be imported.
            bootstrap_servers (List[str]): A list of bootstrap servers.

        Returns:
            None
        """
        self.modules = modules
        self.bootstrap_servers = bootstrap_servers
        self.discover_decorated()
        self.make_thread_map()

    def add_consumer_function(self, cmt: ConsumerMap) -> None:
        """
        Adds a consumer function to the consumer map.

        Args:
            cmt (ConsumerMap): The consumer mapping to be added.

        Returns:
            None
        """
        if cmt.topic in [cmt.topic for cmt in self.consumer_map] and cmt.func.group_id in [cmt.func.group_id for cmt in self.consumer_map]:
            logger.error(f"Consumer function for topic {
                         cmt.topic} already exists in group {cmt.func.group_id}")
            raise Exception(f"Consumer function for topic {
                            cmt.topic} already exists in group {cmt.func.group_id}")
        self.consumer_map.append(cmt)
        logger.info(f"Added consumer function for topic {cmt.topic}, group {
                    cmt.func.group_id}, function {cmt.func.__name__}")

    def add_periodic_producer_function(self, ppmt: PeriodicProducerMap) -> None:
        """
        Adds a periodic producer function to the periodic producer map.

        Args:
            ppmt (PeriodicProducerMap): The periodic producer mapping to be added.

        Returns:
            None
        """
        self.periodic_producer_map.append(ppmt)
        logger.info(f"Added periodic producer function for topic {
                    ppmt.topic}, interval {ppmt.interval}, function {ppmt.func.__name__}")

    def discover_decorated(self) -> None:
        """
        Discovers decorated functions in the imported modules.

        Returns:
            None
        """
        for module in self.modules:
            for attr in dir(module):
                if hasattr(module, attr) and callable(getattr(module, attr)) and hasattr(getattr(module, attr), "pykafka_type"):
                    if getattr(module, attr).pykafka_type == "consumer":
                        self.add_consumer_function(ConsumerMap(
                            getattr(module, attr).topic, getattr(module, attr)))
                    elif getattr(module, attr).pykafka_type == "periodic_producer":
                        self.add_periodic_producer_function(PeriodicProducerMap(getattr(
                            module, attr).topic, getattr(module, attr), getattr(module, attr).interval_s))
                    elif getattr(module, attr).pykafka_type == "producer":
                        pass
                    else:
                        raise Exception(f"Unknown pykafka type {
                                        getattr(module, attr).pykafka_type}")

    def make_thread_map(self) -> None:
        """
        Creates a thread map based on the consumer and periodic producer maps.

        Returns:
            None
        """
        for cmt in self.consumer_map:
            if cmt.func.group_id not in self.consumer_threads:
                self.consumer_threads[cmt.func.group_id] = []
            self.consumer_threads[cmt.func.group_id].append(cmt.func)
            logger.info(f"Added consumer function to thread map for group {
                        cmt.func.group_id}")
        for ppmt in self.periodic_producer_map:
            if ppmt.topic not in self.periodic_producer_threads:
                self.periodic_producer_threads[ppmt.topic] = []
            self.periodic_producer_threads[ppmt.topic].append(ppmt.func)
            logger.info(
                f"Added periodic producer function to thread map for topic {ppmt.topic}")
        logger.info(f"Thread map: {self.periodic_producer_threads}")

    def process_consumer(self, functions: List[Callable[[Message], None]]) -> None:
        """
        Process the consumer by subscribing to the specified topics and invoking the provided functions for each message received.

        Args:
            functions (List[Callable[[Message], None]]): A list of functions to be invoked for each message received. Each function should take a single argument of type `Message` and return `None`.

        Returns:
            None
        """
        conf = ConsumerConfig(bootstrap_servers=self.bootstrap_servers,
                              group_id=functions[0].group_id, auto_offset_reset=functions[0].auto_offset_reset)
        consumer = PyConsumer(conf)
        logger.info(f"Starting consumer for group {functions[0].group_id} on topic {
                    functions[0].topic} based on config {conf}")
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

    def process_periodic_producer(self, func: Callable[[], dict]) -> None:
        """
        Executes the given function periodically.

        Args:
            func (Callable[[], dict]): The function to be executed periodically.

        Returns:
            None
        """
        schedule(func, interval=func.interval_s)
        run_loop()

    def run(self) -> None:
        """
        Starts and runs the consumer and producer threads.

        This method starts the consumer and producer threads by creating and starting
        the corresponding `threading.Thread` objects. It first creates and starts the
        consumer threads for processing consumer functions, and then creates and starts
        the producer threads for processing periodic producer functions.

        Note:
        - The consumer threads are created based on the `consumer_threads` dictionary.
        - The producer threads are created based on the `periodic_producer_threads` dictionary.

        Returns:
        None
        """
        tmap = []
        for funcs in self.consumer_threads.values():
            tmap.append(threading.Thread(
                target=self.process_consumer, args=(funcs,), daemon=False))

        for funcs in self.periodic_producer_threads.values():
            for func in funcs:
                tmap.append(threading.Thread(
                    target=self.process_periodic_producer, args=(func,), daemon=False))

        for t in tmap:
            t.start()
            logger.info(f"Started thread {t}")

    def __del__(self):
        """
        Clean up method that is automatically called when the object is about to be destroyed.
        Closes all consumers and logs the group ID of each closed consumer.
        """
        for consumer in self.consumers.values():
            consumer.close()
            logger.info(f"Closed consumer for group {consumer.group_id}")

    @staticmethod
    def get_folders(base_dir: Path) -> List[str]:
        """
        Retrieves a list of folders in the specified base directory.

        Args:
            base_dir (Path): The base directory to search for folders.

        Returns:
            List[str]: A list of folder names found in the base directory.
        """
        folders = []
        for entry in os.scandir(base_dir):
            if entry.is_dir():
                folders.append(entry.name)
        return folders

    @classmethod
    def django_init(cls, base_dir: Path, bootstrap_servers: List[str]) -> 'PyKafka':
        """
        Initializes PyKafka with Django-specific configuration.

        Args:
            base_dir (Path): The base directory of the Django project.
            bootstrap_servers (List[str]): A list of Kafka bootstrap servers.

        Returns:
            PyKafka: An instance of the PyKafka class.

        """
        folders = cls.get_folders(base_dir)
        modules = []
        for folder in folders:
            try:
                modules.append(__import__(f"{folder}.services", fromlist=[""]))
                logger.info(f"Imported module {folder}")
            except Exception as e:
                logger.error(f"Failed to import module {folder}: {e}")
        return cls(modules, bootstrap_servers)
