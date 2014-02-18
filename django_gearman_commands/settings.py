from django.conf import settings


# gearman jobs servers
GEARMAN_SERVERS = getattr(settings, 'GEARMAN_SERVERS', ['127.0.0.1:4730'])

# Namespacing support
GEARMAN_CLIENT_NAMESPACE = getattr(settings, 'GEARMAN_CLIENT_NAMESPACE', '')

# Timeout after worker is terminated.
WORKER_HALT_TIMEOUT_IN_SECONDS = getattr(settings, 'GEARMAN_WORKER_HALT_TIMEOUT_IN_SECONDS', 30)