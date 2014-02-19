# -*- coding: utf-8 -*-
import sys
import time
import logging
import multiprocessing

import gearman
from django.core.management.base import BaseCommand

import django_gearman_commands.settings as dgc_settings


log = logging.getLogger(__name__)


class HookedGearmanWorker(gearman.GearmanWorker):
    """GearmanWorker with hooks support."""

    def __init__(self, exit_after_job, host_list=None):
        super(HookedGearmanWorker, self).__init__(host_list=host_list)
        self.exit_after_job = exit_after_job

    def after_job(self):
        return not self.exit_after_job


class HaltIfInactiveWorker(HookedGearmanWorker):
    @staticmethod
    def stopwatch(seconds=dgc_settings.WORKER_HALT_TIMEOUT_IN_SECONDS):
        for i in range(seconds):
            time.sleep(1)
        sys.exit(1)

    def start_watch(self):
        self.watch = multiprocessing.Process(target=self.stopwatch, name='Worker timer')
        self.watch.start()

    def work(self, poll_timeout=1):
        """Loop indefinitely, complete tasks from all connections."""
        continue_working = True
        worker_connections = []
        had_job = []

        def continue_while_connections_alive(any_activity):
            if had_job and not self.has_job_lock():
                self.watch.terminate()
                del self.watch
                self.start_watch()

                del had_job[:]
                return self.after_poll(any_activity) and self.after_job()

            del had_job[:]
            if self.has_job_lock():
                had_job.append(True)
            elif self.watch.exitcode:
                return False

            return self.after_poll(any_activity)

        self.start_watch()

        # Shuffle our connections after the poll timeout
        while continue_working and not self.watch.exitcode:
            worker_connections = self.establish_worker_connections()
            continue_working = self.poll_connections_until_stopped(worker_connections, continue_while_connections_alive, timeout=poll_timeout)

        # If we were kicked out of the worker loop, we should shutdown all our connections
        for current_connection in worker_connections:
            current_connection.close()


class GearmanWorkerBaseCommand(BaseCommand):
    """Base command for Gearman workers.

    Subclass this class in your gearman worker commands.

    """
    worker_class = HookedGearmanWorker

    def __init__(self):
        super(GearmanWorkerBaseCommand, self).__init__()
        self.gearman_worker = None

    @property
    def task_name(self):
        """Override task_name property in worker to indicate what task should be registered in Gearman."""
        raise NotImplementedError('task_name should be implemented in worker')

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
        raise NotImplementedError('do_job() should be implemented in worker')

    def handle(self, *args, **options):
        try:
            self.gearman_worker = self.worker_class(
                exit_after_job=self.exit_after_job,
                host_list=dgc_settings.GEARMAN_SERVERS
            )
            task_name = '{0}@{1}'.format(self.task_name, get_namespace()) if get_namespace() else self.task_name
            log.info('Registering gearman task: %s', self.task_name)
            self.gearman_worker.register_task(task_name, self._invoke_job)
        except Exception:
            log.exception('Problem with registering gearman task')
            raise

        self.gearman_worker.work()

    def _invoke_job(self, worker, job):
        """Invoke gearman job."""
        try:
            self.stdout.write('Invoking gearman job, task: {0:s}.\n'.format(self.task_name))
            result = self.do_job(job.data if job.data else None)

            log.info('Job finished, task: %s, result %s', self.task_name, result)
            self.stdout.write('Job finished, task: {0:s}\n'.format(self.task_name))

            if result is not None:
                self.stdout.write('{0}\n'.format(result))

            return 'OK' if not isinstance(result, basestring) else result
        except Exception:
            log.exception('Error occurred when invoking job, task: %s', self.task_name)
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
        self.ping_time = None
        self.ping_time_str = None

    def get_server_info(self, task_filter=None):
        """Read Gearman server info - status, workers and and version."""
        result = ''

        # Read server status info.
        client = gearman.GearmanAdminClient([self.host])

        self.server_version = client.get_version()
        self.tasks = client.get_status()
        self.workers = client.get_workers()
        self.ping_time = client.ping_server()
        self.ping_time_str = '{0:0.016f}'.format(self.ping_time)

        # if task_filter is set, filter list of tasks and workers by regex pattern task_filter
        if task_filter:
            # filter tasks
            self.tasks = [item for item in self.tasks if task_filter in item['task']]

            # filter workers by registered task name
            self.workers = [item for item in self.workers if item['tasks'] and task_filter in [t for t in item['tasks']]]

        # sort tasks by task name
        self.tasks = sorted(self.tasks, key=lambda item: item['task'])

        # sort registered workers by task name
        self.workers = sorted(self.workers, key=lambda item: item['tasks'])

        # Use prettytable if available, otherwise raw output.
        try:
            from prettytable import PrettyTable
        except ImportError:
            PrettyTable = None

        if PrettyTable is not None:
            # Use PrettyTable for output.
            # server
            table = PrettyTable(['Gearman Server Host', 'Gearman Server Version', 'Ping Response Time'])
            table.add_row([self.host, self.server_version, self.ping_time_str])
            result += '{0:s}.\n\n'.format(table)

            # tasks
            table = PrettyTable(['Task Name', 'Total Workers', 'Running Jobs', 'Queued Jobs'])
            for r in self.tasks:
                table.add_row([r['task'], r['workers'], r['running'], r['queued']])

            result += '{0:s}.\n\n'.format(table)

            # workers
            table = PrettyTable(['Worker IP', 'Registered Tasks', 'Client ID', 'File Descriptor'])
            for r in self.workers:
                if r['tasks']: # ignore workers with no registered task
                    table.add_row([r['ip'], ','.join(r['tasks']), r['client_id'], r['file_descriptor']])

            result += '{0:s}.\n\n'.format(table)

        else:
            # raw output without PrettyTable
            result += 'Gearman Server Host:{0:s}\n'.format(self.host)
            result += 'Gearman Server Version:{0:s}.\n'.format(self.server_version)
            result += 'Gearman Server Ping Response Time:{0:s}.\n'.format(self.ping_time_str)
            result += 'Tasks:\n{0:s}\n'.format(self.tasks)
            result += 'Workers:\n{0:s}\n'.format(self.workers)

        return result


def get_namespace():
    """Namespace to suffix function on a mutialized gearman."""
    return dgc_settings.GEARMAN_CLIENT_NAMESPACE


def submit_job(task_name, data='', client=None, **options):
    """Shortcut util for submitting job in standard way.

    We will revert the default behaviour of GermanClient.submit_job, so that
    job are executed in background by default and client doesn't wait for task completion.

    """
    background = options.pop('background', True)
    wait_until_complete = options.pop('wait_until_complete', False)
    client = client or gearman.GearmanClient(dgc_settings.GEARMAN_SERVERS)
    task_name = '{0}@{1}'.format(task_name, get_namespace()) if get_namespace() else task_name

    return client.submit_job(task_name, data=data, background=background, wait_until_complete=wait_until_complete,
                             **options)
