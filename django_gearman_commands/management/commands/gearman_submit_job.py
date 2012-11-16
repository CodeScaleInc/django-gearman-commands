# -*- coding: utf-8 -*-

import logging

import gearman

from django.core.management.base import BaseCommand, CommandError

import django_gearman_commands.settings
from django_gearman_commands import get_namespace


log = logging.getLogger(__name__)


class Command(BaseCommand):
    """Submit specific gearman job with job data as an arguments."""

    args = '<task_name> [job_data]'
    help = 'Submit gearman job with specified task, optionally with job data'

    def handle(self, *args, **options):
        try:
            job_data = ''

            if len(args) == 0:
                raise CommandError('At least task name must be provided.')

            task_name = '{0}@{1}'.format(args[0], get_namespace()) if get_namespace() else args[0]
            if len(args) > 1:
                job_data = args[1]

            self.stdout.write('Submitting job: {0:s}, job data: {1:s}.\n'.format(task_name, job_data if job_data else '(empty)'))

            client = gearman.GearmanClient(django_gearman_commands.settings.GEARMAN_SERVERS)
            result = client.submit_job(task_name, job_data, wait_until_complete=options.get('wait_unit_complete', False),
                                       background=options.get('background', True))
            
            self.stdout.write('Job submission done, result: {0:s}.\n'.format(result))
        except:
            log.exception('Error when submitting gearman job')
            raise
