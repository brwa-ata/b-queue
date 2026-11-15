import os

SECRET_KEY = 'tests'
USE_TZ = True
TIME_ZONE = 'UTC'
ROOT_URLCONF = 'tests.urls'
DEFAULT_AUTO_FIELD = 'django.db.models.BigAutoField'

INSTALLED_APPS = [
    'django.contrib.contenttypes',
    'django.contrib.auth',
    'b_queue',
    'tests',
]

if os.environ.get('BQ_TEST_POSTGRES'):
    # e.g. BQ_TEST_POSTGRES=postgres://user:pass@localhost:5432/postgres
    from urllib.parse import urlparse

    url = urlparse(os.environ['BQ_TEST_POSTGRES'])
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql',
            'NAME': url.path.lstrip('/'),
            'USER': url.username,
            'PASSWORD': url.password,
            'HOST': url.hostname,
            'PORT': url.port or 5432,
        }
    }
else:
    DATABASES = {
        'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}
    }

TASKS = {
    'default': {'BACKEND': 'b_queue.backends.DatabaseBackend'},
    'redis': {
        'BACKEND': 'b_queue.backends.RedisBackend',
        'OPTIONS': {
            'URL': os.environ.get('BQ_TEST_REDIS', 'redis://localhost:6379/15'),
            'PREFIX': 'b_queue_test',
            # tests run inside a transaction that never commits
            'ENQUEUE_ON_COMMIT': False,
        },
    },
    'redis_on_commit': {
        'BACKEND': 'b_queue.backends.RedisBackend',
        'OPTIONS': {
            'URL': os.environ.get('BQ_TEST_REDIS', 'redis://localhost:6379/15'),
            'PREFIX': 'b_queue_test',
        },
    },
}

B_QUEUE = {'SCHEDULE_MODULES': []}
