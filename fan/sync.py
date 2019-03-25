'''
Shortlived sync helpers. Primary target is creating short-lived fast context with get_context
'''
import logging
import os
import socket
from functools import wraps

import requests
from basictracer import BasicTracer
from basictracer.recorder import SpanRecorder
from py_zipkin.thrift import (annotation_list_builder, create_endpoint,
                              binary_annotation_list_builder, create_span, span_to_bytes,
                              encode_bytes_list)
from py_zipkin.zipkin import ZipkinAttrs

from fan.context import Context
from fan.contrib.kazoo.discovery import KazooDiscovery
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


log = logging.getLogger('fan.sync')


def zipkin_log_span(span_id, parent_span_id, trace_id, span_name, annotations, binary_annotations,
                    timestamp_s, duration_s, **kwargs):
    span = create_span(span_id, parent_span_id, trace_id, span_name, annotations,
                       binary_annotations, timestamp_s, duration_s)
    http_transport(encode_bytes_list([span_to_bytes(span)]))


class BaseFanRecorder(SpanRecorder):
    _endpoint = None
    my_ip = socket.gethostbyname(socket.gethostname())

    def __init__(self, name, send_to_zipkin):
        super().__init__()
        self.name = name
        self.send_to_zipkin = send_to_zipkin

    @property
    def endpoint_info(self):
        return {
            'port': 80,
            'service_name': self.name,
            'ipv4': self.my_ip,
        }

    @property
    def endpoint(self):
        if not self._endpoint:
            self._endpoint = create_endpoint(**self.endpoint_info)
        return self._endpoint

    def zipkin_span_params(self, span):
        span_params = self.log_span_params(span)

        timing = {
            'ss': span.start_time,
            'sr': span.start_time + span.duration
        }
        annotations = annotation_list_builder(timing, self.endpoint)
        zipkin_attrs = ZipkinAttrs(
            trace_id=span_params['trace_id'],
            span_id=span_params['span_id'],
            parent_span_id=span_params['parent_span_id'],
            flags=span_params['flags'],
            is_sampled=span_params['is_sampled']
        )

        return {
            **span_params,
            'zipkin_attrs': zipkin_attrs,
            'annotations': annotations,
            'binary_annotations': binary_annotation_list_builder({
                **span.context.baggage,
                **span.tags
            }, self.endpoint),
        }

    def log_span_params(self, span):
        ctx = span.context
        return {
            'trace_id': hex(ctx.trace_id)[2:],
            'span_id': hex(ctx.span_id)[2:],
            'parent_span_id': span.parent_id and hex(span.parent_id)[2:],
            'service_name': self.name,
            'span_name': span.operation_name or 'no_name',

            'flags': 0,  # for ZipkinAttrs
            'is_sampled': True,  # for ZipkinAttrs
            'endpoint': self.endpoint_info,  # for annotation

            'timestamp_s': span.start_time,
            'duration_s': span.duration,
            'span_context_baggage': span.context.baggage,
            'span_context_tags': span.tags,
        }


class FanRecorder(BaseFanRecorder):
    def record_span(self, span):
        if self.send_to_zipkin:
            zipkin_log_span(**self.zipkin_span_params(span))

        log.info('Log span: {}'.format(self.log_span_params(span)))

        return super().record_span(span)


def get_tracer(name=None):
    global tracer
    if tracer:
        return tracer

    recorder = FanRecorder(name or 'no_name', send_to_zipkin=ZIPKIN)
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
    discovery = KazooDiscovery(os.environ.get('ZK_HOST', 'zk'), os.environ.get('ZK_CHROOT', '/'))
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
