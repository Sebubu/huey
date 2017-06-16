from functools import wraps
import sys

from django.conf import settings
from django.db import connection

from huey.contrib.djhuey.configuration import SingleConfReader, MultiConfReader


configuration_message = """
Configuring Huey for use with Django
====================================

Huey was designed to be simple to configure in the general case.  For that
reason, huey will "just work" with no configuration at all provided you have
Redis installed and running locally.

On the other hand, you can configure huey manually using the following
setting structure.

The following example uses Redis on localhost, and will run four worker
processes:

HUEY = {
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

Additionally the old configuration variant is still usable:

HUEY = {
    'name': 'my-app',
    'connection': {'host': 'localhost', 'port': 6379},
    'consumer': {
        'workers': 4,
        'worker_type': 'process',  # "thread" or "greenlet" are other options
    },
}

If you would like to configure Huey's logger using Django's integrated logging
settings, the logger used by consumer is named "huey.consumer".

Alternatively you can simply assign `settings.HUEY` to an actual `Huey`
object instance:

from huey import RedisHuey
HUEY = RedisHuey('my-app')
"""


def default_queue_name():
    try:
        return settings.DATABASE_NAME
    except AttributeError:
        try:
            return settings.DATABASES['default']['NAME']
        except KeyError:
            return 'huey'


def config_error(msg):
    print(configuration_message)
    print('\n\n')
    print(msg)
    sys.exit(1)


class DjangoHuey:
    def __init__(self, huey_settings):
        self.huey_settings = huey_settings
        self.hueys = {}
        self.huey = None

    def task(self, name=None, **kwargs):
        if name is None:
            huey = self.huey
        else:
            huey = self.hueys[name]

        return huey.task(**kwargs)



    def periodic_task(self, name=None, **kwargs):
        huey = self.hueys[name]

        def decorator(func):
            return huey.periodic_task(**kwargs)(func)

        return decorator

    def start(self):
        if self.huey_settings is None:
            try:
                from huey import RedisHuey
            except ImportError:
                config_error('Error: Huey could not import the redis backend. '
                             'Install `redis-py`.')
            else:
                self.huey = RedisHuey(default_queue_name())
                self.hueys = {default_queue_name():self.huey}

        if isinstance(self.huey_settings, dict):
            single_reader = SingleConfReader(self.huey_settings)
            is_legacy_configuration = single_reader.is_valid()
            if is_legacy_configuration:
                self.huey = single_reader.huey
                self.hueys = {single_reader.name: self.huey}
            else:
                multi_reader = MultiConfReader(self.huey_settings)
                if multi_reader.is_valid():
                    self.huey = multi_reader.default_configuration.huey
                    for single_reader in multi_reader.configurations:
                        self.hueys[single_reader.name] = single_reader.huey



