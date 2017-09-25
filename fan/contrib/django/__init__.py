import logging

from django.conf import settings

from fan.context import Context
from fan.sync import get_discovery


VARS = {}


def update_vars(ctx):
    span = ctx.span
    _ctx = ctx.span.context
    VARS['TRACE_ID'] = hex(_ctx.trace_id)[2:]
    VARS['SPAN_ID'] = hex(_ctx.span_id)[2:]
    VARS['PARENT_SPAN_ID'] = span.parent_id and hex(span.parent_id)[2:]


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
        else:
            ctx = Context(discovery, None, None, name)
        request.ctx = ctx
        with ctx:
            update_vars(ctx)
            return self.get_response(request)
