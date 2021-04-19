import logging
from random import randint
from time import sleep

from django import db
from django.db.utils import OperationalError

from galera import settings


class GaleraMiddleware(object):
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        return self.get_response(request)

    def process_exception(self, request, exception):
        if type(exception) == OperationalError:
            code, msg = exception.args
            if code in settings.GALERA_RETRY_EXCEPTIONS and msg in settings.GALERA_RETRY_EXCEPTIONS[code]:
                retry = request.META.get(settings.GALERA_META_KEY_RETRIES, 0)
                waited = request.META.get(settings.GALERA_META_KEY_WAITED, 0)

                if retry < settings.GALERA_RETRY_COUNT:
                    logging.getLogger('django').warning('reprocessing request after deadlock', exc_info=True)

                    db.connections.close_all()

                    wait = randint(settings.GALERA_RETRY_MIN_SLEEP_MS, settings.GALERA_RETRY_MAX_SLEEP_MS) / 1000.0

                    request.META[settings.GALERA_META_KEY_RETRIES] = retry + 1
                    request.META[settings.GALERA_META_KEY_WAITED] = round(waited + wait, 4)

                    sleep(wait)
                    return self.get_response(request)
