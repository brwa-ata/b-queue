from django.apps import AppConfig


class BQueueConfig(AppConfig):
    name = 'b_queue'
    verbose_name = 'Queue'
    default_auto_field = 'django.db.models.BigAutoField'
