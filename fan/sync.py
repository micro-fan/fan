'''
Shortlived sync helpers. Primary target is creating short-lived fast context with get_context
'''
import os
import time
import json

from basictracer import BasicTracer
from basictracer.recorder import InMemoryRecorder

from fan.context import Context
from fan.contrib.kazoo.discovery import KazooDiscovery
from fan.transport import HTTPTransport, HTTPPropagator, DjangoPropagator


discovery = None
tracer = None


class FanRecorder(InMemoryRecorder):

    def record_span(self, span):
        with open('/tmp/trace_{}'.format(time.time()*1000000), 'w') as f:
            ctx = span.context
            ctx_row = {'trace_id': ctx.trace_id,
                       'span_id': ctx.span_id,
                       'sampled': ctx.sampled,
                       'parent_id': span.parent_id}
            json.dump(ctx_row, f)

        return super().record_span(span)


def get_tracer():
    # TODO: get tracer configuration from ENV
    global tracer
    if tracer:
        return tracer
    recorder = FanRecorder()
    tracer = BasicTracer(recorder)
    return tracer


def get_discovery(is_django=False):
    # TODO: get root
    global discovery
    if discovery:
        return discovery
    discovery = KazooDiscovery(os.environ.get('ZK_HOST', 'zk'))
    discovery.on_start()
    discovery.transport_classes = {
        'http': HTTPTransport,
    }
    discovery.tracer = get_tracer()
    if is_django:
        discovery.tracer.register_propagator('http', DjangoPropagator())
    else:
        discovery.tracer.register_propagator('http', HTTPPropagator())
    return discovery


def get_context():
    return Context(get_discovery())
