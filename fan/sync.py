'''
Shortlived sync helpers. Primary target is creating short-lived fast context with get_context
'''
import os
import time
import json
import socket

import requests
from basictracer import BasicTracer
from basictracer.recorder import InMemoryRecorder
from py_zipkin.logging_helper import log_span
from py_zipkin.thrift import (annotation_list_builder, create_endpoint,
                              binary_annotation_list_builder)

from fan.context import Context
from fan.contrib.kazoo.discovery import KazooDiscovery
from fan.transport import HTTPTransport, HTTPPropagator, DjangoPropagator


discovery = None
tracer = None


def http_transport(encoded_span):
    # copypasted from: https://github.com/Yelp/py_zipkin#transport
    # The collector expects a thrift-encoded list of spans. Instead of
    # decoding and re-encoding the already thrift-encoded message, we can just
    # add header bytes that specify that what follows is a list of length 1.
    body = b'\x0c\x00\x00\x00\x01' + encoded_span
    r = requests.post(
        'http://zipkin:9411/api/v1/spans',
        data=body,
        headers={'Content-Type': 'application/x-thrift'},
    )


MY_IP = socket.gethostbyname(socket.gethostname())
EP = None


class FanRecorder(InMemoryRecorder):
    def __init__(self, name):
        super().__init__()
        self.name = name

    def record_span(self, span):
        global EP
        if not EP:
            EP = create_endpoint(port=80, service_name=self.name, host=MY_IP)

        ctx = span.context
        timing = {'ss': span.start_time,
                  'sr': span.start_time+span.duration}
        annotations = annotation_list_builder(timing, EP)
        params = {
            'span_id': hex(ctx.span_id),
            'parent_span_id': span.parent_id and hex(span.parent_id),
            'trace_id': hex(ctx.trace_id),
            'span_name': span.operation_name or 'no_name',
            'annotations': annotations,
            'binary_annotations': binary_annotation_list_builder(span.tags, EP),
            'transport_handler': http_transport,
        }
        log_span(**params)
        with open('/tmp/trace_{}'.format(time.time()*1000000), 'w') as f:
            ctx_row = {'trace_id': ctx.trace_id,
                       'span_id': ctx.span_id,
                       'sampled': ctx.sampled,
                       'parent_id': span.parent_id}
            json.dump(ctx_row, f)

        return super().record_span(span)


def get_tracer(name=None):
    # TODO: get tracer configuration from ENV
    global tracer
    if tracer:
        return tracer
    recorder = FanRecorder(name or 'no_name')
    tracer = BasicTracer(recorder)
    return tracer


def get_discovery(is_django=False, name=None):
    # TODO: get root
    global discovery
    if discovery:
        return discovery
    discovery = KazooDiscovery(os.environ.get('ZK_HOST', 'zk'))
    discovery.on_start()
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


def get_context(name=None):
    return Context(get_discovery(is_django=False, name=name))
