import logging

from fan.sync import get_discovery
from fan.context import Context


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
        if hasattr(request, '_ctx'):
            return request._ctx
        discovery = get_discovery(is_django=True)
        tracer = discovery.tracer

        span_context = tracer.extract('http', request.META)
        if span_context:
            ctx = Context(discovery, None, span_context)
            request.ctx = ctx
            with ctx.span:
                return self.get_response(request)
        return self.get_response(request)
