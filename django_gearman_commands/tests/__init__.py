# -*- coding: utf-8 -*-

import logging
import pickle

from django.core.management import call_command
from django.conf import settings
from django.test import TestCase
from django.core.cache import cache
from django_gearman_commands import GearmanServerInfo


log = logging.getLogger(__name__)


class GearmanCommandsTest(TestCase):

    def test_server_info(self):
        server_info = GearmanServerInfo(settings.GEARMAN_SERVERS[0])
        server_info.get_server_info()
        self.assertTrue(server_info.server_version.startswith('OK'), 'Unexpected server version string')
        self.assertTrue(type(server_info.tasks) is tuple, 'Unexpected server tasks type')
        
        # verify command is callable
        overview = call_command('gearman_server_info')
    
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
