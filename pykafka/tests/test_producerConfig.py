import unittest
from pykafka.config.producerConfig import ProducerConfig

class TestProducerConfig(unittest.TestCase):
    
    def test_dict_representation(self):
        bootstrap_servers = "localhost:9092"
        config = ProducerConfig(bootstrap_servers)
        expected_dict = {
            "bootstrap.servers": "localhost:9092"
        }
        self.assertEqual(config.__dict__(), expected_dict)

if __name__ == "__main__":
    unittest.main()