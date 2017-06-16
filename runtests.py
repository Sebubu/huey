#!/usr/bin/env python
import os
import sys
import unittest

from huey import tests


def run_tests(*test_args):
    suite = unittest.TestLoader().loadTestsFromModule(tests)
    result = unittest.TextTestRunner(verbosity=1).run(suite)
    if result.failures:
        sys.exit(1)
    elif result.errors:
        sys.exit(2)


def run_django_tests(*test_args):
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "huey.contrib.djhuey.tests.settings")
    from django.core.management import execute_from_command_line
    args = sys.argv
    args.insert(1, "test")
    execute_from_command_line(args)


if __name__ == '__main__':
    run_django_tests(*sys.argv[1:])
    sys.exit(0)
