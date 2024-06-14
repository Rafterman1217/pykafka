import unittest
from pykafka.config.consumerConfig import ConsumerConfig

class TestConsumerConfig(unittest.TestCase):
    
    def test_dict_representation(self):
        bootstrap_servers = ["localhost:9092", "localhost:9093"]
        group_id = "test-group"
        auto_offset_reset = "earliest"

        config = ConsumerConfig(bootstrap_servers, group_id, auto_offset_reset)
        expected_dict = {
            "bootstrap.servers": "localhost:9092,localhost:9093",
            "group.id": "test-group",
            "auto.offset.reset": "earliest"
        }

        self.assertEqual(config.__dict__(), expected_dict)

if __name__ == "__main__":
    unittest.main()