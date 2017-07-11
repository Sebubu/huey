import imp

import logging
from django.apps import apps as django_apps
from django.conf import settings
from django.core.management.base import BaseCommand

from huey.consumer import Consumer
from huey.consumer_options import ConsumerConfig
from huey.consumer_options import OptionParserHandler


class Command(BaseCommand):
    """
    Queue consumer. Example usage::

    To start the consumer (note you must export the settings module):

    django-admin.py run_huey
    """
    help = "Run the queue consumer"
    _type_map = {'int': int, 'float': float}

    def add_arguments(self, parser):
        option_handler = OptionParserHandler()
        groups = (
            option_handler.get_logging_options(),
            option_handler.get_worker_options(),
            option_handler.get_scheduler_options(),
        )
        for option_list in groups:
            for short, full, kwargs in option_list:
                if short == '-v':
                    full = '--huey-verbose'
                    short = '-V'
                if 'type' in kwargs:
                    kwargs['type'] = self._type_map[kwargs['type']]
                parser.add_argument(full, short, **kwargs)
        parser.add_argument('-qu', '--queue', type=str, help='Select the queue to listen on.')

    def autodiscover(self):
        """Use Django app registry to pull out potential apps with tasks.py module."""
        module_name = 'tasks'
        for config in django_apps.get_app_configs():
            app_path = config.module.__path__
            try:
                fp, path, description = imp.find_module(module_name, app_path)
            except ImportError:
                continue
            else:
                import_path = '%s.%s' % (config.name, module_name)
                imp.load_module(import_path, fp, path, description)

    def handle(self, *args, **options):
        queue_defined = 'queue' in options and options['queue'] is not None
        if queue_defined:
            from huey.contrib.djhuey import consumers
            queue = options['queue']
            consumer = consumers[queue]
        else:
            from huey.contrib.djhuey import consumer

        self.autodiscover()

        print('Run huey on ' + str(consumer.huey.name))
        logging.info('Running huey on ' + str(consumer.huey.name))
        consumer.run()
