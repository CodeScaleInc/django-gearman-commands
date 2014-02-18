"""
This code provides a mechanism for running django_gearman_commands internal
test suite without having a full Django project.  It sets up the
global configuration, then dispatches out to `call_command` to
kick off the test suite.

"""

# Setup and configure the minimal settings necessary to
# run the test suite.  Note that Django requires that the
# `DATABASES` value be present and configured in order to
# do anything.

from django.conf import settings
import logging

logging.basicConfig(level=logging.DEBUG, format='%(levelname)s %(asctime)s %(module)s: %(message)s')

settings.configure(
    DEBUG=True,
    
    INSTALLED_APPS=[
        "django_gearman_commands",
    ],
    
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": ":memory:",
        }
    },
    
    # tests dependency - run gearman server on localhost on default port
    GEARMAN_SERVERS=[
        'localhost:4730'
    ],
    
    # LocMemCache used as a verification storage for tests
    CACHES={
        'default': {
            'BACKEND': 'django.core.cache.backends.locmem.LocMemCache',
            'LOCATION': 'django-gearman-commands-tests'
        }
    },

    GEARMAN_CLIENT_NAMESPACE='django-gearman-commands-tests'
)

# Start the test suite now that the settings are configured.
from django.core.management import call_command
call_command("test", "django_gearman_commands")