'''
Shortlived sync helpers. Primary target is creating short-lived fast context with get_context
'''
from functools import wraps
import json
import logging
import os
import socket
import time

import requests
from basictracer import BasicTracer
from basictracer.recorder import InMemoryRecorder
from py_zipkin.zipkin import ZipkinAttrs
from py_zipkin.thrift import (annotation_list_builder, create_endpoint,
                              binary_annotation_list_builder,
                              create_span, thrift_objs_in_bytes)

from fan.context import Context
from fan.contrib.kazoo.discovery import KazooDiscovery
from fan.exceptions import DiscoveryConnectionError
from fan.transport import HTTPTransport, HTTPPropagator, DjangoPropagator


discovery = None
tracer = None
ZIPKIN = os.environ.get('ZIPKIN')


def http_transport(encoded_span):
    # copypasted from: https://github.com/Yelp/py_zipkin#transport
    # The collector expects a thrift-encoded list of spans. Instead of
    # decoding and re-encoding the already thrift-encoded message, we can just
    # add header bytes that specify that what follows is a list of length 1.
    requests.post(
        'http://{}/api/v1/spans'.format(ZIPKIN),
        data=encoded_span,
        headers={'Content-Type': 'application/x-thrift'},
    )


MY_IP = socket.gethostbyname(socket.gethostname())
EP = None
log = logging.getLogger('fan.sync')


def logger_log_span(span_id, parent_span_id, trace_id, span_name, annotations,
                    binary_annotations, **kwargs):
    log.info('Log span: {}'.format(locals()))


def zipkin_log_span(span_id, parent_span_id, trace_id, span_name, annotations,
                    binary_annotations, timestamp_s, duration_s, **kwargs):
    span = create_span(span_id, parent_span_id, trace_id, span_name,
                       annotations, binary_annotations,
                       timestamp_s, duration_s)
    http_transport(thrift_objs_in_bytes([span]))

    params = {'timestamp_s': timestamp_s,
              'duration_s': duration_s,
              **kwargs}
    logger_log_span(span_id, parent_span_id, trace_id, span_name, annotations,
                    binary_annotations, **params)


class FanRecorder(InMemoryRecorder):
    def __init__(self, name, log_span):
        super().__init__()
        self.name = name
        self.log_span = log_span

    def record_span(self, span):
        global EP
        if not EP:
            EP = create_endpoint(port=80, service_name=self.name, host=MY_IP)

        ctx = span.context
        timing = {'ss': span.start_time,
                  'sr': span.start_time+span.duration}
        annotations = annotation_list_builder(timing, EP)
        attrs = ZipkinAttrs(trace_id=hex(ctx.trace_id)[2:],
                            span_id=hex(ctx.span_id)[2:],
                            parent_span_id=span.parent_id and hex(span.parent_id)[2:],
                            flags=0,
                            is_sampled=True)
        params = {
            'zipkin_attrs': attrs,
            'trace_id': hex(ctx.trace_id)[2:],
            'span_id': hex(ctx.span_id)[2:],
            'parent_span_id': span.parent_id and hex(span.parent_id)[2:],
            'service_name': self.name,
            'span_name': span.operation_name or 'no_name',
            'annotations': annotations,
            'binary_annotations': binary_annotation_list_builder({**span.context.baggage,
                                                                  **span.tags}, EP),
            'transport_handler': http_transport,
            'timestamp_s': span.start_time,
            'duration_s': span.duration,
        }

        self.log_span(**params)

        with open('/tmp/trace_{}'.format(time.time()*1000000), 'w') as f:
            ctx_row = {'trace_id': ctx.trace_id,
                       'span_id': ctx.span_id,
                       'sampled': ctx.sampled,
                       'parent_id': span.parent_id}
            json.dump(ctx_row, f)

        return super().record_span(span)


def get_tracer(name=None):
    global tracer
    if tracer:
        return tracer
    if not name:
        name = 'no_name'

    if ZIPKIN:
        log_span = zipkin_log_span
    else:
        log_span = logger_log_span
    recorder = FanRecorder(name, log_span)
    tracer = BasicTracer(recorder)
    return tracer


def cache_discovery(fun):
    @wraps(fun)
    def wrapped(*args, **kwargs):
        global discovery
        if discovery:
            return discovery
        discovery = fun(*args, **kwargs)
        return discovery
    return wrapped


@cache_discovery
def get_discovery(is_django=False, name=None):
    # TODO: get root
    discovery = KazooDiscovery(os.environ.get('ZK_HOST', 'zk'))
    discovery.transport_classes = {
        'http': HTTPTransport,
    }

    if not name and is_django:
        name = 'django'

    discovery.tracer = get_tracer(name)
    if is_django:
        discovery.tracer.register_propagator('http', DjangoPropagator())
    else:
        discovery.tracer.register_propagator('http', HTTPPropagator())
    return discovery


def get_context(name=None, service_name=None):
    if not service_name and name:
        service_name = name
    return Context(get_discovery(is_django=False, name=service_name), name=name)
