import django
import django.conf

if django.VERSION[:2] < (3, 2):
    default_app_config = 'galera.apps.GaleraConfig'


class DefaultSettings:
    GALERA_META_KEY_RETRIES = 'django.galera_middleware.retries'
    GALERA_META_KEY_WAITED = 'django.galera_middleware.waited'

    GALERA_RETRY_COUNT = 5
    GALERA_RETRY_MIN_SLEEP_MS = 100
    GALERA_RETRY_MAX_SLEEP_MS = 500
    GALERA_RETRY_EXCEPTIONS = {
        1213: [
            'Deadlock found when trying to get lock; try restarting transaction'
        ]
    }


class Settings:
    def __getattr__(self, item):
        try:
            return getattr(django.conf.settings, item)
        except AttributeError:
            return getattr(DefaultSettings, item)


settings = Settings()
