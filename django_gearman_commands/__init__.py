# -*- coding: utf-8 -*-

import time
import logging
from datetime import datetime

import gearman

from django.core.management.base import BaseCommand, CommandError
from django.conf import settings

import django_gearman_commands.settings

__version__ = '0.1'


log = logging.getLogger(__name__)


class HookedGearmanWorker(gearman.GearmanWorker):
    """GearmanWorker with hooks support."""
    
    def __init__(self, exit_after_job, host_list=None):
        super(HookedGearmanWorker, self).__init__(host_list=host_list)
        self.exit_after_job = exit_after_job
        
    def after_job(self):
        return not self.exit_after_job

    
class GearmanWorkerBaseCommand(BaseCommand):
    """Base command for Gearman workers.

    Subclass this class in your gearman worker commands.
    
    """
    @property
    def task_name(self):
        """Override task_name property in worker to indicate what task should be registered in Gearman."""
        raise NotImplementedError, 'task_name should be implemented in worker'

    @property
    def exit_after_job(self):
        """Return True if worker should exit after processing job. False by default.

        You do not need to override this in standard case, except in case
        you want to control and terminate worker after processing jobs.
        Used by test worker 'footest'.

        """
        return False

    def do_job(self, job_data):
        """Gearman job execution logic.
        
        Override this in worker to perform job.
        
        """
        raise NotImplementedError, 'do_job() should be implemented in worker'
    
    def handle(self, *args, **options):
        try:
            worker = HookedGearmanWorker(exit_after_job=self.exit_after_job,
                                         host_list=django_gearman_commands.settings.GEARMAN_SERVERS)
            task_name = '{0}@{1}'.format(self.task_name, get_namespace()) if get_namespace() else self.task_name
            log.info('Registering gearman task: %s', self.task_name)
            worker.register_task(task_name, self._invoke_job)
        except:
            log.exception('Problem with registering gearman task')
            raise
        
        worker.work()

    def _invoke_job(self, worker, job):
        """Invoke gearman job.
        
        Honestly, wrapper for do_job().
        
        """
        try:
            # represent default job data '' as None
            job_data = job.data if job.data else None
            self.stdout.write('Invoking gearman job, task: %s.\n' % self.task_name)

            result = self.do_job(job_data)

            log.info('Job finished, task: %s', self.task_name)
            self.stdout.write('Job finished, task: %s\n' % self.task_name)
            
            if result is not None:
                log.info(result)
                self.stdout.write('%s\n' % result)

            return 'OK'
        except:
            log.exception('Error occured when invoking job, task: %s', self.task_name)
            raise


class GearmanServerInfo():
    """Administration informations about Gearman server.

    See GearmanAdminClient for reference: http://packages.python.org/gearman/admin_client.html

    """

    def __init__(self, host):
        self.host = host
        self.server_version = None
        self.tasks = None
        self.workers = None

    def get_server_info(self):
        """Read Gearman server info - status, workers and and version."""
        result = ''

        # read server status info
        client = gearman.GearmanAdminClient([self.host])
        
        self.server_version = client.get_version()
        self.tasks = client.get_status()
        self.workers = client.get_workers()

        # use prettytable if available, otherwise raw output
        try:
            from prettytable import PrettyTable
            use_prettytable = True
        except ImportError:
            use_prettytable = False

        if use_prettytable:
            # use PrettyTable for output
            # version
            table = PrettyTable(['Gearman Server Host', 'Gearman Server Version'])
            table.add_row([self.host, self.server_version])
            result += '%s.\n\n' % str(table)

            # tasks
            table = PrettyTable(['Task Name', 'Total Workers', 'Running Jobs', 'Queued Jobs'])
            for r in self.tasks:
                table.add_row([r['task'], r['workers'], r['running'], r['queued']])
                
            result += '%s.\n\n' % str(table)

            # workers
            table = PrettyTable(['Worker IP', 'Registered Tasks', 'Client ID', 'File Descriptor'])
            for r in self.workers:
                if r['tasks']: # ignore workers with no registered task
                    table.add_row([r['ip'], ','.join(r['tasks']), r['client_id'], r['file_descriptor']])

            result += '%s.\n\n' % str(table)

        else:
            # raw output without PrettyTable
            result += 'Gearman Server Host:%s\n' % self.host
            result += 'Gearman Server Version:%s.\n' % self.server_version
            result += 'Tasks:\n%s\n' % str(self.tasks)
            result += 'Workers:\n%s\n' % str(self.workers)
            
        return result


def get_namespace():
    """Namespace to suffix function on a mutialized gearman."""
    return django_gearman_commands.settings.GEARMAN_CLIENT_NAMESPACE
