from unittest.mock import patch

from django.test import TestCase

from huey import RedisHuey
from huey.consumer import Consumer
from huey.contrib.djhuey.configuration import SingleConfReader, MultiConfReader


class SingleConfReaderTest(TestCase):
    def test_huey(self):
        config = {
            'name': 'myapp',
            'connection': {'host': 'localhost', 'port': 6378},
            'consumer': {
                'workers': 4,
                'worker_type': 'process',  # "thread" or "greenlet" are other options
            },
        }
        huey = SingleConfReader(config).huey
        self.assertIsInstance(huey, RedisHuey)
        self.assertTrue('myapp' in huey.storage.queue_key)
        self.assertEqual(huey.storage.connection_params['host'], 'localhost')
        self.assertEqual(huey.storage.connection_params['port'], 6378)

    def test_read_consumer_options(self):
        config = {
            'name': 'myapp',
            'connection': {'host': 'localhost', 'port': 6379},
            'consumer': {
                'workers': 4,
                'worker_type': 'process',  # "thread" or "greenlet" are other options
            },
        }
        consumer_config = SingleConfReader(config).consumer_options
        self.assertEqual(consumer_config['workers'], 4)
        self.assertEqual(consumer_config['worker_type'], 'process')

    @patch('huey.consumer_options.ConsumerConfig.setup_logger')
    def test_consumer(self, setup_logger):
        config = {
            'name': 'myapp',
            'connection': {'host': 'localhost', 'port': 6379},
            'consumer': {
                'workers': 4,
                'worker_type': 'process',  # "thread" or "greenlet" are other options
            },
        }
        reader = SingleConfReader(config)
        consumer = reader.consumer
        self.assertIsInstance(consumer, Consumer)
        self.assertEqual(consumer.huey, reader.huey)
        self.assertEqual(consumer.workers, 4)


class MultiConfReaderTest(TestCase):
    config = {
        'my-app': {
            'backend': 'huey.backends.redis_backend',
            'connection': {'host': 'localhost', 'port': 6378},
                'consumer': {
                    'workers': 4,
                    'worker_type': 'process',
            }
        },
        'my-app2': {
            'backend': 'huey.backends.sqlite_backend',
            'connection': {'location': 'sqlite filename'},
                'consumer': {
                    'workers': 4,
                    'worker_type': 'process',
            }
        },
    }

    def test_configurations_splitting(self):
        reader = MultiConfReader(self.config)
        self.assertEqual(len(reader.configurations), 2)

    def test_huey(self):
        reader = MultiConfReader(self.config)
        huey = reader.configurations[0].huey
        self.assertIsInstance(huey, RedisHuey)
        self.assertTrue('myapp' in huey.storage.queue_key)

    def test_get_item(self):
        reader = MultiConfReader(self.config)
        conf = reader['my-app']
        self.assertEqual(conf.name, 'my-app')



