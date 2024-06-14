# PyKafka

What is it?

It mainly consists of 3 decorators for the [Confluent_kafka](https://github.com/confluentinc/confluent-kafka-python) client and an django integration



```
pykafka = PyKafka([services],bootstrap_servers=["localhost:9092"])
```

creates a PyKafka app,
```
pykafka.run()
```
starts the app


---
```
pykafka = PyKafka([services],bootstrap_servers=["localhost:9092"])
```
services -> a module that exports functions (in its init!)



## Consumers

```
@consumer(topic="test",group_id="test",auto_offset_reset="earliest")
def test(msg:Message):
    print(msg.value(),msg.topic())
    
```

is an example function, the runtime will now start a thread that listens to Kafka messages in the "test" topic. The test() function will be called for each received message.

```
@consumer(topic="testQueue",group_id="test",auto_offset_reset="earliest")
def test2(msg:Message):
    print(msg.value(),msg.topic())
```
If another function belongs to the same group, its messages will be processed in the same way, but also in the same thread. Each topic is unique per group, so if you want to consume a topic from multiple functions, new groups need to be created.

```
@consumer(topic="testQueue",group_id="test",auto_offset_reset="earliest")
def test2(msg:Message):
    print(msg.value(),msg.topic())

@consumer(topic="testQueue",group_id="test2",auto_offset_reset="earliest")
def test3(msg:Message):
    print(msg.value(),msg.topic(),__name__)
```

This would start two threads and call both functions for a message.

## Periodic producers

```
@periodic_producer(topic="test",interval_s=1, config=ProducerConfig(bootstrap_servers="localhost:9092"))
def test4():
    return {"value":"el-magical"}

```
Each periodic producer gets its own thread, where the decorated function is called every interval_s seconds. The returned dict (!) is published on the specified topic.

## Producers

```
@producer(topic="test",config=ProducerConfig(bootstrap_servers="localhost:9092"))
def inputtery(val):
    return {"value":val}
```
Producers are the only decorators that do not have to be in the "Service" module (but can be). Each call to the function will publish the returned dict (!) on the specified topic.

## Django Integration

you'll need to the following to your settings

````
from pykafka.config import ProducerConfig

KAFKA_BOOTSTRAP_SERVERS = ["localhost:9092"]
PYKAFKA_PRODUCER_CONFIG = ProducerConfig(bootstrap_servers="localhost:9092")
````

also you need to add *pykafka* to your installed apps

```

INSTALLED_APPS = [
    "...",
    "pykafka"
    "...",
]

```

after that every app can have a 'service' module which works like above.

the kafka process can be started via

```
poetry run python manage.py kafka

or

python manage.py kafka
```

this process however doesnt react to code changes in the services without restart.