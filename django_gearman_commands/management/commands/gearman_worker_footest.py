# -*- coding: utf-8 -*-

from django.core.cache import cache

from django_gearman_commands import GearmanWorkerBaseCommand


class Command(GearmanWorkerBaseCommand):
    """Gearman worker performing 'footest' job."""
    
    @property
    def task_name(self):
        return 'footest'

    @property
    def exit_after_job(self):
        return True # terminate after job is handled. Do not do this for standard workers !
    
    def do_job(self, job_data):
        # set data to cache
        cache.set('footest', 'I AM FOO !' if not job_data else job_data)
        

    

    
