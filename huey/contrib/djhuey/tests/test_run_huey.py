from django.test import TestCase

from huey.contrib.djhuey.management.commands.run_huey import Command as run_huey


class RunHueyTest(TestCase):
    def test_run_huey(self):
        run_huey('run_huey', queue='my-app')