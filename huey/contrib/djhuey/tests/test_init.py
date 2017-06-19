from django.test import TestCase

from huey.contrib.djhuey.configuration import DjangoHuey


class DjangoHueyTest(TestCase):
    def test_read_config(self):
        config = {
            'my-app': {
                'default': True,
                'backend': 'huey.backends.redis_backend',
                'connection': {'host': 'localhost', 'port': 6379},
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
        dh = DjangoHuey(config)
        dh.start()
        self.assertEqual(dh.huey.name, 'my-app')
        self.assertEqual(len(dh.hueys), 2)

    def test_read_config_legacy(self):
        config = {
            'name': 'my-app',
            'connection': {'host': 'localhost', 'port': 6379},
            'consumer': {
                'workers': 4,
                'worker_type': 'process',  # "thread" or "greenlet" are other options
            },
        }
        dh = DjangoHuey(config)
        dh.start()
        self.assertEqual(dh.huey.name, 'my-app')
        self.assertEqual(len(dh.hueys), 1)

    def test_task_equal(self):
        def sample_task():
            return 'bla'

        config = {
            'name': 'my-app',
            'connection': {'host': 'localhost', 'port': 6379},
            'always_eager': True,
            'consumer': {
                'workers': 4,
                'worker_type': 'process',  # "thread" or "greenlet" are other options
            },
        }
        dh = DjangoHuey(config)
        dh.start()
        dh_task = dh.task()(sample_task)
        huey_task = dh.huey.task()(sample_task)
        dh_ret = dh_task()
        huey_ret = huey_task()
        self.assertEqual(dh_ret, huey_ret)

    def test_task_equal_choose_name(self):
        def sample_task():
            return 'bla'

        config = {
            'my-app': {
                'default': True,
                'backend': 'huey.backends.redis_backend',
                'always_eager': True,
                'connection': {'host': 'localhost', 'port': 6379},
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
        dh = DjangoHuey(config)
        dh.start()
        dh_task = dh.task('my-app')(sample_task)
        huey_task = dh.huey.task()(sample_task)
        dh_ret = dh_task()
        huey_ret = huey_task()
        self.assertEqual(dh_ret, huey_ret)

