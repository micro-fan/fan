import logging
import requests

from basictracer.propagator import Propagator
from basictracer.context import SpanContext

from fan.remote import Transport


def hex_string(i):
    return '{:x}'.format(i)


def from_hex_string(s):
    return int(s, 16)


class HTTPPropagator(Propagator):
    mapping = {
        'span_id': ('ot-span-id', hex_string, from_hex_string),
        'trace_id': ('ot-trace-id', hex_string, from_hex_string),
        # 'baggage': 'ot-baggage',
        'sampled': ('ot-sampled', str, bool),
    }

    def inject(self, span_context, carrier):
        for k, v in self.mapping.items():
            name, pack, _ = v
            carrier[name] = pack(getattr(span_context, k))
        return carrier

    def extract(self, carrier):
        # TODO: support ot-debug => override sampled to true
        # generate custom label
        try:
            kwargs = {'baggage': {}}
            for k, v in self.mapping.items():
                name, _, extract = v
                kwargs[k] = extract(carrier[name])
            return SpanContext(**kwargs)
        except:
            pass


class DjangoPropagator(HTTPPropagator):
    mapping = {
        'span_id': ('HTTP_OT_SPAN_ID', hex_string, from_hex_string),
        'trace_id': ('HTTP_OT_TRACE_ID', hex_string, from_hex_string),
        # 'baggage': 'ot-baggage',
        'sampled': ('HTTP_OT_SAMPLED', str, bool),
    }

    def extract(self, carrier):
        try:
            kwargs = {'baggage': {}}
            for k, v in self.mapping.items():
                name, _, extract = v
                kwargs[k] = extract(carrier[name])
            return SpanContext(**kwargs)
        except:
            pass


class HTTPTransport(Transport):
    log = logging.getLogger('HTTPTransport')

    def __init__(self, discovery, endpoint, params):
        super().__init__(discovery, endpoint, params)
        self.base_url = '{transport}://{host}:{port}'.format(**params)
        self.methods = {}
        for method in params['methods']:
            self.methods[method['name']] = method

    def get_headers(self, ctx):
        hdrs = {}
        tracer = ctx.discovery.tracer
        tracer.inject(ctx.span.context, 'http', hdrs)
        return hdrs

    def prepare_get_params(self, params):
        out = {}
        for k, v in params.items():
            if isinstance(v, list):
                out[k] = ','.join([str(x) for x in v])
            else:
                out[k] = v
        return out

    def rpc_call(self, method_name, ctx, **kwargs):
        method = self.methods[method_name]
        url = ''.join([self.base_url, method['url']])
        if '{' in url:
            url = url.format(**kwargs)
        m = method.get('method', 'get').lower()
        req = getattr(requests, m)
        if m in ('get', 'delete'):
            kw = {'params': self.prepare_get_params(kwargs)}
        else:
            kw = {'json': kwargs}
        self.log.debug('Url: {} Params: {}'.format(url, kw))
        kw['headers'] = self.get_headers(ctx)
        resp = req(url, **kw)
        if resp.status_code in (200, 201):
            ret = resp.json()
        elif resp.status_code in (204,):
            ret = True
        else:
            # TODO: howto return error
            self.log.error('Resp: {} : {}'.format(resp.status_code, resp))
            raise Exception('HttpError: {}'.format(resp))
        return ret
