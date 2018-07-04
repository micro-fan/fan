import logging

from django.conf import settings
from django.http import HttpResponse

from fan.context import Context
from fan.exceptions import RPCHttpError
from fan.sync import get_discovery

VARS = {}


def update_vars(ctx, request):
    span = ctx.span
    _ctx = ctx.span.context
    VARS['TRACE_ID'] = hex(_ctx.trace_id)[2:]
    VARS['SPAN_ID'] = hex(_ctx.span_id)[2:]
    VARS['PARENT_SPAN_ID'] = span.parent_id and hex(span.parent_id)[2:]
    # HTTP_INSTALLATION_ID
    # HTTP_SESSION_ID
    for k, v in _ctx.baggage.items():
        VARS[k.upper()] = v


class FanMiddleware(object):
    """
    Enable tracing handling
    Adds `request.ctx`
    """
    log = logging.getLogger('FanMiddleware')

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        """
        We're performing initialization when request.ctx is called
        So set property here
        """
        VARS.clear()
        if hasattr(request, 'ctx'):
            return self.get_response(request)
        discovery = get_discovery(is_django=True, name=getattr(settings, 'FAN_SERVICE', None))
        tracer = discovery.tracer

        span_context = tracer.extract('http', request.META)
        name = '{} {}'.format(request.method, request.path)
        if span_context:
            ctx = Context(discovery, None, span_context, name)
        else:
            # TODO: should be configurable
            baggage = {}
            for k in ['HTTP_INSTALLATION_ID', 'HTTP_SESSION_ID']:
                if k in request.META:
                    baggage[k.strip('HTTP_')] = request.META[k]
            ctx = Context(discovery, None, None, name, baggage)
        request.ctx = ctx
        with ctx:
            update_vars(ctx, request)
            return self.get_response(request)

    def process_exception(self, request, exception):
        if isinstance(exception, RPCHttpError):
            resp = exception.response
            return HttpResponse(
                content=resp.content,
                status=resp.status_code,
                content_type=resp.headers['Content-Type']
            )
