# -*- coding: utf-8 -*-

import logging

from django.core.management.base import BaseCommand

import django_gearman_commands.settings
from django_gearman_commands import GearmanServerInfo


log = logging.getLogger(__name__)


class Command(BaseCommand):
    """Pprint overview of Gearman server status and workers."""

    args = '[task_filter]'
    help = 'Print overview of Gearman server status and workers.'

    def handle(self, *args, **options):
        result = ''

        task_filter = ''
        if len(args) > 0:
            task_filter = args[0]

        for server in django_gearman_commands.settings.GEARMAN_SERVERS:
            server_info = GearmanServerInfo(server)
            result += server_info.get_server_info(task_filter)

        self.stdout.write(result)
