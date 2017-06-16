from django.conf import settings

from huey import RedisHuey
from huey.consumer import Consumer
from huey.consumer_options import ConsumerConfig


class Reader:
    """
    This reader supports two different configurations.


    """
    def __init__(self, huey_settings):
        self.huey_settings = huey_settings


class MultiConfReader:
    """
    Supports the multi queue configuration.
    This configuration style aligns with the django database configuration in the django settings file.
    The reader is lazy. It only creates RedisHuey and Consumer if the properties are accessed.

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

    The MultiConfiguration adds a additional property: default.
    It indicates the default queue if only the decorator @huey.task is used.
    The default property can only be used once.
    """

    def __init__(self, huey_settings, handle_options={}):
        self.huey_settings = huey_settings
        self.handle_options = handle_options
        self._single_conf_readers = []

        self._create_single_confs()

    def _create_single_confs(self):
        huey_config = self.huey_settings.copy()

        for name, config in huey_config.items():
            config['name'] = name
            reader = SingleConfReader(config, self.handle_options)
            self._single_conf_readers.append(reader)

    def __getitem__(self, item):
        for conf in self._single_conf_readers:
            if conf.name == item:
                return conf
        raise KeyError

    def is_valid(self):
        for key, value in self.huey_settings:
            if not isinstance(value, {}):
                return False
        return True

    @property
    def configurations(self):
        return self._single_conf_readers

    @property
    def default_configuration(self):
        for conf in self._single_conf_readers:
            if 'default' in conf.huey_settings:
                if conf.huey_settings['default']:
                    return conf
        return self._single_conf_readers[0]





class SingleConfReader:
    """
    Supports the old legacy configuration.
    The reader is lazy. It only creates RedisHuey and Consumer if the properties are accessed.

    HUEY = {
    'name': 'my-app',
    'connection': {'host': 'localhost', 'port': 6379},
        'consumer': {
            'workers': 4,
            'worker_type': 'process',  # "thread" or "greenlet" are other options
        },
    }
    """
    def __init__(self, huey_settings, handle_options={}):
        self.huey_settings = huey_settings
        self.handle_options = handle_options
        self._huey = None
        self._consumer = None

    @property
    def name(self):
        return self.huey_settings['name']

    @property
    def huey(self):
        if self._huey is not None:
            return self._huey
        huey_config = self.huey_settings.copy()
        name = huey_config.pop('name', self.default_queue_name())
        conn_kwargs = huey_config.pop('connection', {})
        try:
            del huey_config['consumer']  # Don't need consumer opts here.
        except KeyError:
            pass
        if 'always_eager' not in huey_config:
            huey_config['always_eager'] = settings.DEBUG
        huey_config.update(conn_kwargs)
        self._huey = RedisHuey(name, **huey_config)
        return self._huey

    @property
    def consumer_options(self):
        consumer_options = {}
        huey_config = self.huey_settings.copy()
        if isinstance(huey_config, dict):
            consumer_options.update(huey_config.get('consumer', {}))

        for key, value in self.handle_options.items():
            if value is not None:
                consumer_options[key] = value

        consumer_options.setdefault('verbose',
                                    consumer_options.pop('huey_verbose', None))
        return consumer_options

    @property
    def consumer(self):
        if self._consumer is not None:
            return self._consumer
        config = ConsumerConfig(**self.consumer_options)
        config.validate()
        config.setup_logger()
        self._consumer = Consumer(self.huey, **config.values)
        return self._consumer

    @staticmethod
    def default_queue_name():
        try:
            return settings.DATABASE_NAME
        except AttributeError:
            try:
                return settings.DATABASES['default']['NAME']
            except KeyError:
                return 'huey'

    def is_valid(self):
        for key, value in self.huey_settings:
            if not isinstance(value, {}):
                return True
        return False

    def __str__(self):
        return str(self.huey_settings)



