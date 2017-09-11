import logging

from fan.sync import get_discovery
from fan.context import Context
from django.conf import settings


class FanMiddleware(object):
    '''
    Enable tracing handling
    Adds `request.ctx`
    '''
    log = logging.getLogger('FanMiddleware')

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        '''
        We're performing initialization when request.ctx is called
        So set property here
        '''
        if hasattr(request, 'ctx'):
            return self.get_response(request)
        discovery = get_discovery(is_django=True, name=getattr(settings, 'FAN_SERVICE', None))
        tracer = discovery.tracer

        span_context = tracer.extract('http', request.META)
        name = '{} {}'.format(request.method, request.path)
        if span_context:
            ctx = Context(discovery, None, span_context, name)
            request.ctx = ctx
            with ctx:
                return self.get_response(request)
        return self.get_response(request)
