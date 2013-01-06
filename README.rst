=========================
 Django Gearman Commands
=========================

django-gearman-commands is set of Django management commands
and base classes aiming to simplify developing and submitting
tasks to Gearman job server from Django.

About Gearman
=============

Gearman, as stated on project website, provides 'a generic application framework to farm out work to other machines or processes that are better suited to do the work'.
Practically, Gearman is a daemon, service, running on TCP port and waiting for Clients wishing to get job done and Workers who handle and process the jobs.
Gearman - anagram for "Manager" itself does exactly what manager does - accepts requests from Clients and distribute them to Workers.

Illustration how it works with one Gearman server::

 ----------     ----------     ----------     ----------
 | Client |     | Client |     | Client |     | Client |
 ----------     ----------     ----------     ----------
      \             |             |                 /               
       \            |             |                /
        \           |             |               /
         \          |             |              /
          \         |             |             /
	   \        |             |            /
	    \       |             |           /
	     \      |             |          /
              \     |             |         / 
	       ------------------------------
               |      Gearman Job Server    |
               ------------------------------
                             |       
       ----------------------------------------------
       |              |              |              |
   ----------     ----------     ----------     ----------
   | Worker |     | Worker |     | Worker |     | Worker |
   ----------     ----------     ----------     ----------


**Clients** request job to be done. This is your (web)application.

**Gearman** servers inserts job requests into the queue and notifies workers.

**Workers** asks for jobs and process them. Worker is your standalone running script/program.

Note that single Gearman Job Server in illustration above is only meant to simplify illustration.
Even Gearman itself is really stable, in production it is recommended to run at least 2 job servers
for redundancy.

Gearman has API for writing Clients and Workers for a lot of environments - Python, Ruby, PHP, Java, Perl, C and probably some others.
In practice, you can write workers in same or different languages.
For example, if you have PHP application, you can use Gearman PHP API but your workers can be written in Python.
Gearman is designed with flexibility in mind.

You can read more about Gearman itself on http://gearman.org/index.php

Why django-gearman-commands
===========================

For Python, there is a great python-gearman 2.x API by Yelp - https://github.com/Yelp/python-gearman.
python-gearman API allows you to write Clients, Workers and perform some additional administration tasks
in Python without assuming you use some specific web application framework.

The main issue solved by django-gearman-commands is writing Workers in a way they are aware of your Django application.

Gearman Worker is standalone script running on background and waiting for job requests.
In theory, you write Python script implementing worker and logic of job and that's it.
In practice, your worker needs to use Django facilities.
When you use virtualenv (which you should), you also need to think of imports, DJANGO_SETTINGS_MODULE
and all that jazz in your Worker script.

Django provides a way to write standalone, console-based scripts - Commands - https://docs.djangoproject.com/en/dev/howto/custom-management-commands/
Similar to './manage.py syncdb' or './manage.py migrate', it is possible to write your own commands invoked by './manage.py mycommand'.
This is recommended way to write scripts in Django project which are not run from web application itself.
Django Commands can be called from console or from Django application itself, which makes them quite flexible
for both manual command-line invocation or programmatic invocation.

What django-gearman-commands provides
-------------------------------------

**Base Class for your Workers**

django_gearman_commands.GearmanWorkerBaseCommand is base class for writing Gearman Workers
as Django commands. No need to hassle with low-level python-gearman API, just inherit GearmanWorkerBaseCommand,
override task_name and do_job and you are ready to go.

**Submitting Jobs Easily**

django-gearman-commands provides 'gearman_submit_job' command that can be used to submit new jobs
to gearman. Instead of writing your own class to submit jobs and handle job arguments,
invoke ./manage.py gearman_submit_job task_name job_data

**Gearman Administration Overview**

Gearman itself provides administration functions returning version of Gearman server, active workers
and list of registered tasks with their relations to workers, running and pending jobs.
Simply run ./manage.py gearman_server_info to get current status of Gearman servers.
If you want to output that information yourself, you can use django_gearman_commands.GearmanServerInfo class.

Getting started
===============

Setup
-----

So you have your Django application and want to install django-gearman-commands.
django-gearman-commands is standard Django app which provides no models, views or urls,
only few classes, custom management commands and tests.

There is only one new dependency to add to your app (except Django) - python-gearman API::

 $ pip install -e git+https://github.com/Yelp/python-gearman.git@2ed9d88941e31e3358a0b80787254d0c2cfaa78a#egg=gearman-dev

We install python-gearman from git, because there are some fixes on master used by django-gearman-commands.

There is one optional depedency - prettytable, used by gearman_server_info to output server status in pretty tables::

 $ pip install prettytable==0.5

If you do not install prettytable, ./manage.py gearman_server_info will output server info 'as-is' raw form.

To install django-gearman-commands itself::

 $ pip install django-gearman-commands


Finally, add django-gearman-commands to your INSTALLED_APPS::

 INSTALLED_APPS = (
        # ...installed apps...
        'django_gearman_commands',
 )

And add list of Gearman job servers to settings.py::

 GEARMAN_SERVERS = ['127.0.0.1:4730']

Writing workers
---------------

**django_gearman_commands.GearmanWorkerBaseCommand** is base class for your custom django commands acting like Gearman workers.
You should write custom command for each specific task.

Suppose we want to write worker to import some complex data which can take a long time.
Create file 'gearman_worker_data_import.py' in your Django app management/commands directory
with following content::

 import django_gearman_commands

 class Command(django_gearman_commands.GearmanWorkerBaseCommand):
    """Gearman worker performing 'data_import' job."""
    
    @property
    def task_name(self):
        return 'data_import'

    def do_job(self, job_data):
        # perform complex data import
        your_code_performing_job_logic()
        

As you see, you need to do three things:

* create file 'management/commands/gearman_worker_MY_TASK_NAME.py' in your django app
* create Command class inheriting from django_gearman_commands.GearmanWorkerBaseCommand class
* override task_name property and do_job() method

**task_name** is unique identification of task, which your worker is supposed to do. Submitting jobs is done via sending task name and optional job parameters. Task name can be also easily name-spaced.

**do_job()** is method which will be invoked when job is submitted. If job was submitted with arguments, 'job_data' is not None.


Run your worker::

 $ ./manage.py gearman_worker_data_import

Worker will start, register itself to Gearman server(s) and wait for jobs. 

Submitting jobs
---------------

So now you have your first worker up and running.
You can submit first job easily with 'gearman_submit_job' commands::

 $ ./manage.py gearman_submit_job data_import

 Submitting job: data_import, job data: (empty).
 Job submission done, result: <GearmanJobRequest task='data_import', unique='8e610a031cef8aaf50c30f451d77808d', priority=None, background=True, state='CREATED', timed_out=False>.

By default, jobs are submitted in background and 'gearman_submit_job' does not wait for job to finish.
You can override this with '--foreground' CLI option. See './manage.py gearman_submit_job --help'.
If you did everything right, your worker method 'your_code_performing_job_logic()' should be now running in background.

This method is fine if you want to run job manually or from cron.
For example, if you want to run data_import for cron every 5 minutes, you can add something like this to cron::

 */5 * * * * /path-to-your-virtualenv/bin/python /path-to-your-project/manage.py gearman_submit_job data_import

However, in lot of cases, you want to run job on-demand, for example in some Django view, user makes some action
and you want to run job immediately - sending email, importing data or anything else you need and don't want to block
user's web request until task is completed.
Django can call custom management commands programatically, via django.core.management.call_command method::

 from django.core.management import call_command
 
 def some_view(request):
     # ....process your view logic....
     # submit job to queue
     call_command('gearman_submit_job', 'data_import')    

By using job submission wrapper Command 'gearman_submit_job',
you are now able submit jobs from console, cron and your app with same API.

Task namespaces
---------------

Sometimes it can be useful to distinguish between jobs submitted with same task name from several applications connected
to same gearman servers. For example, you may have several instances of same django project deployed for individual
clients.
In that case, you can add GEARMAN_CLIENT_NAMESPACE to your django settings to uniquely identify tasks
submitted by project::

 GEARMAN_CLIENT_NAMESPACE = 'MyCustomer1'


Gearman server info
-------------------

gearman_server_info outputs current status of Gearman servers.
If you installed prettytable dependency, here is how output looks like::

 $ ./manage.py gearman_server_info
 +---------------------+------------------------+--------------------+
 | Gearman Server Host | Gearman Server Version | Ping Response Time |
 +---------------------+------------------------+--------------------+
 |    127.0.0.1:4730   |        OK 1.1.3        | 0.0006051063537598 |
 +---------------------+------------------------+--------------------+.

 +---------------+---------------+--------------+-------------+
 |   Task Name   | Total Workers | Running Jobs | Queued Jobs |
 +---------------+---------------+--------------+-------------+
 | data_unlock   |       1       |      0       |      0      |
 | data_import   |       1       |      1       |      0      |
 | cache_cleanup |       1       |      0       |      0      |
 +---------------+---------------+--------------+-------------+.

 +-----------+------------------+-----------+-----------------+
 | Worker IP | Registered Tasks | Client ID | File Descriptor |
 +-----------+------------------+-----------+-----------------+
 | 127.0.0.1 |   data_unlock    |     -     |        35       |
 | 127.0.0.1 |   data_import    |     -     |        36       |
 | 127.0.0.1 |  cache_cleanup   |     -     |        37       |
 +-----------+------------------+-----------+-----------------+


If you have a lot of workers, you can filter output using command argument (case-sensitive)::

 $ ./manage.py gearman_server_info cleanup
 +---------------------+------------------------+--------------------+
 | Gearman Server Host | Gearman Server Version | Ping Response Time |
 +---------------------+------------------------+--------------------+
 |    127.0.0.1:4730   |        OK 1.1.3        | 0.0006871223449707 |
 +---------------------+------------------------+--------------------+.

 +---------------+---------------+--------------+-------------+
 |   Task Name   | Total Workers | Running Jobs | Queued Jobs |
 +---------------+---------------+--------------+-------------+
 | cache_cleanup |       1       |      0       |      0      |
 +---------------+---------------+--------------+-------------+.

 +-----------+------------------+-----------+-----------------+
 | Worker IP | Registered Tasks | Client ID | File Descriptor |
 +-----------+------------------+-----------+-----------------+
 | 127.0.0.1 |  cache_cleanup   |     -     |        37       |
 +-----------+------------------+-----------+-----------------+


Practical production deployment with Supervisor
===============================================

In production, you need to be sure about two things:

 * your Gearman server is running
 * your Workers are running

Supervisor -  http://supervisord.org/ is babysitter for processes.
It allows you to launch, restart and monitor running processes.

Supervisor + Gearman
--------------------

Assuming you have supervisor installed and know the basics,
you can create 'gearman.conf' in /etc/supervisor/conf.d with following content::

 [program:gearman]
 command=/home/gearman/gearmand/gearmand/gearmand -q libsqlite3 --libsqlite3-db=/home/gearman/gearmand-sqlite.db -L 127.0.0.1
 numprocs=1
 directory=/home/gearman
 stdout_logfile=/var/log/gearman.log
 autostart=true
 autorestart=true
 user=gearman

This will start Gearman server compiled in /home/gearman/earmand/gearmand/gearmand with SQLite persistent queue on localhost.
Of course, your variables may vary.

Supervisor + Workers
--------------------

You can create single .conf file for all workers relevant to single application.
This will create process 'group' and allows you to reload of all workers related to some application
at once when you redeploy new code.

For example, create 'myapp.conf' in /etc/supervisor/conf.d with all workers relevant to 'myapp':::

 [program:myapp_data_import]
 command=/path-to-your-virtualenv/bin/python /path-to-your-project/manage.py gearman_worker_data_import
 numprocs=1
 directory=/home/myapp/
 stdout_logfile=/var/log/myapp_data_import.log
 autostart=true
 autorestart=true
 user=myapp

 [program:myapp_send_invoices]
 command=/path-to-your-virtualenv/bin/python /path-to-your-project/manage.py gearman_worker_send_invoices
 numprocs=1
 directory=/home/myapp/
 stdout_logfile=/var/log/myapp_send_invoices.log
 autostart=true
 autorestart=true
 user=myapp

 [group:myapp]
 programs=myapp_data_import, myapp_send_invoices

After redeployment, you can restart all workers:::

 $ supervisorctl reread
 $ supervisorctl update
 $ supervisorctl restart myapp:*

I recommend automating this with Fabric - http://docs.fabfile.org/

Contributing to django-gearman-commands
=======================================

Contributions are welcome.
If possible, please use following workflow:

 * find out what is bothering you
 * check Issues page if problem is not already discussed
 * fork django-gearman-commands
 * fix it in your fork and add test to tests/__init__.py
 * add yourself as Contributor to 'Authors and Contributors'
 * and make Pull Request with description what change is supposed to do

Running tests
-------------

Tests are located in tests/__init__.py file.
There is a wrapper 'runtests.py' in root directory to setup Django environment with minimal dependencies and run tests.
The point is to allow testing of django-gearman-commands during development without full-blown Django application::

 $ python runtests.py

As you can read from runtests.py, tests expect Gearman server running on localhost on standard port 4730.

License
=======

BSD, see LICENSE for details

Authors and Contributors
========================

Author: Jozef Ševčík, sevcik@codescale.net

Contributors:

 * Vladimír Gorej (gorej@codescale.net)

