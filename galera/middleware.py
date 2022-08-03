import warnings


class GaleraMiddleware(object):
    def __init__(self, get_response):
        warnings.warn('GaleraMiddleware is deprecated and can be removed '
                      'since deadlocks are now handled by the database backend')
        self.get_response = get_response

    def __call__(self, request):
        return self.get_response(request)
