# -*- coding: utf-8 -*-
import logging
import pickle

from django.core.management import call_command
from django.test import TestCase
from django.core.cache import cache
from django_gearman_commands import GearmanServerInfo

import django_gearman_commands.settings


log = logging.getLogger(__name__)


print 'test'

class GearmanCommandsTest(TestCase):

    def test_server_info(self):
        server_info = GearmanServerInfo(django_gearman_commands.settings.GEARMAN_SERVERS[0])
        server_info.get_server_info()
        self.assertTrue(server_info.server_version.startswith('OK'), 'Unexpected server version string')
        self.assertTrue(type(server_info.tasks) is list, 'Unexpected server tasks type')

        # verify command is callable
        overview = call_command('gearman_server_info')

    def test_server_info_task_filter(self):
        # submit job to queue so we have something shown up in get_server_info()
        call_command('gearman_submit_job', 'footest')

        server_info = GearmanServerInfo(django_gearman_commands.settings.GEARMAN_SERVERS[0])

        server_info.get_server_info("footest")
        self.assertTrue(len(server_info.tasks) > 0, 'Unexpected server_info.tasks')

        server_info.get_server_info("NON_EXISTING!!")
        self.assertTrue(len(server_info.tasks) == 0, 'Unexpected server_info.tasks')

        # let the worker process the job
        call_command('gearman_worker_footest')

    def test_worker_simple(self):
        # submit job
        call_command('gearman_submit_job', 'footest')

        # let the worker process the job
        call_command('gearman_worker_footest')

        # verify job was processed
        self.assertEqual(cache.get('footest'), 'I AM FOO !', 'Unexpected footest worker result')

    def test_worker_task_data_string(self):
        # submit job
        call_command('gearman_submit_job', 'footest', 'DATA')

        # let the worker process the job
        call_command('gearman_worker_footest')

        # verify job was processed and processed task data
        self.assertEqual(cache.get('footest'), 'DATA', 'Unexpected footest worker result (data string)')

    def test_worker_task_data_pickled(self):
        # submit job
        call_command('gearman_submit_job', 'footest', pickle.dumps(u'DATA'))

        # let the worker process the job
        call_command('gearman_worker_footest')

        # verify job was processed and processed task data
        self.assertEqual(pickle.loads(cache.get('footest')), u'DATA', 'Unexpected footest worker result (data pickled)')