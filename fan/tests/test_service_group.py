from unittest import TestCase

from basictracer import BasicTracer
from basictracer.recorder import InMemoryRecorder
from fan.context import Context
from fan.discovery import (SimpleDictDiscovery,
                           LocalDiscovery, CompositeDiscovery)
from fan.service import ServiceGroup, Service, endpoint
from fan.process import Process
from fan.remote import LocalEndpoint, RemoteEndpoint
from fan.tests import DummyTracer, DummyTransport


class FanTestServiceGroup(ServiceGroup):
    def __init__(self, discovery):
        super().__init__(discovery)
        self.endpoints = []

    def start(self):
        for data in self.services:
            service = data['service']()
            service.on_start()

            self.instances.append(service)
            for ep_conf in data['endpoints']:
                ep = ep_conf['endpoint'](self.discovery, service, ep_conf['params'])
                ep.on_start()
                self.endpoints.append(ep)
                self.discovery.register(ep)


class DummyServiceGroup(FanTestServiceGroup):
    services = [{'service': DummyTracer,
                 'endpoints': [{'endpoint': RemoteEndpoint,
                                'params': {'id': 1, 'transport': 'dummy'}}]}]


class ChainedEchoService(Service):
    name = 'chained_echo'

    @endpoint('echo')
    def echo(self, ctx, word):
        return ctx.rpc.dummy_tracer.echo(word, 0)[0]


class ChainedServiceGroup(FanTestServiceGroup):
    services = [{'service': ChainedEchoService,
                 'endpoints': [{'endpoint': RemoteEndpoint,
                                'params': {'id': 2, 'transport': 'dummy'}}]}]


class FanTestProcess(Process):
    def create_context(self):
        return Context(self.discovery)


class FanTestRemoteDiscovery(SimpleDictDiscovery):
    def __init__(self, conf):
        super().__init__(conf)
        self.conf = conf
        self.remote = None

    def link(self, other_remote):
        if not self.remote:
            self.remote = other_remote
            self.remote.link(self)

    def find_local_endpoint(self, service_name):
        self.log.debug('Remote Lookup: {} {}'.format(service_name, self.cached_endpoints))
        if service_name in self.cached_endpoints:
            return self.cached_endpoints[service_name]
        assert False, 'Cannot find: {} {}'.format(service_name, self.cached_endpoints)

    def find_remote_endpoint(self, service_name, version_filter):
        pass


class FanTestLocalDiscovery(LocalDiscovery):
    transports = {'dummy': DummyTransport}

    def get_transport_class(self, name):
        return self.transports[name]


class ProcessTestCase(TestCase):
    def setUp(self):
        self.recorder = InMemoryRecorder()
        d = CompositeDiscovery(LocalDiscovery(), SimpleDictDiscovery({}))
        d.tracer = BasicTracer(self.recorder)

        self.process = FanTestProcess(d)

    def test_call(self):
        context = self.process.create_context()

        context.discovery.register(LocalEndpoint(DummyTracer()))
        with context:
            response = context.rpc.dummy_tracer.echo('hello', 7)

        self.assertEquals(len(self.recorder.get_spans()), 9)
        assert response == ('hello', 0), response


class MultiProcessTestCase(TestCase):
    def setUp(self):
        self.recorder = InMemoryRecorder()

        self.remote = FanTestRemoteDiscovery({})

        self.p1 = self.create_process(self.recorder, ChainedServiceGroup)
        self.p2 = self.create_process(self.recorder, DummyServiceGroup)
        self.p1.start()
        self.p2.start()

    def create_process(self, recorder, sg):
        discovery = CompositeDiscovery(FanTestLocalDiscovery(), self.remote)
        discovery.tracer = BasicTracer(recorder)
        proc = FanTestProcess(discovery)
        proc.service_groups = [sg]
        return proc

    def test_call(self):
        context = self.p1.create_context()
        with context:
            result = context.rpc.chained_echo.echo('hello')
        self.assertEqual(result, 'hello')
        self.assertEquals(len(self.recorder.get_spans()), 3)
