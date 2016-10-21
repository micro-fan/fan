'''
Shortlived sync helpers. Primary target is creating short-lived fast context with get_context
'''
import os

from basictracer import BasicTracer
from basictracer.recorder import InMemoryRecorder

from fan.context import Context
from fan.contrib.kazoo.discovery import KazooDiscovery
from fan.transport import HTTPTransport, HTTPPropagator


discovery = None
tracer = None


def get_tracer():
    # TODO: get tracer configuration from ENV
    global tracer
    if tracer:
        return tracer
    recorder = InMemoryRecorder()
    tracer = BasicTracer(recorder)
    return tracer


def get_discovery():
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
    discovery.tracer.register_propagator('http', HTTPPropagator())
    return discovery


def get_context():
    return Context(get_discovery())
