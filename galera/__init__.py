import django.conf

if django.VERSION[:2] < (3, 2):
    default_app_config = 'galera.apps.GaleraConfig'
