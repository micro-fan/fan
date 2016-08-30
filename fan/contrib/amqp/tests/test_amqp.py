import asyncio
import os
from unittest import TestCase

from basictracer import BasicTracer
from basictracer.recorder import InMemoryRecorder

from fan.context import TracedContext
from fan.remote import ProxyEndpoint
from fan.contrib.amqp import AMQPEndpoint, AMQPTransport
from fan.discovery import SimpleDictDiscovery, LocalDiscovery
from fan.contrib.aio.discovery import AIOCompositeDiscovery
from fan.service import Service, endpoint


class DummyDiscovery(SimpleDictDiscovery):
    pass


class TestLocalDiscovery(LocalDiscovery):
    transports = {'amqp': AMQPTransport}

    def get_transport_class(self, name):
        return self.transports[name]


class DummyService(Service):
    service_name = 'dummy'

    @endpoint
    def ping(self, ctx):
        print('Call ping endpoint {}'.format(ctx.span.context.trace_id))
        return 'pong'


class AMQPCase(TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        self.recorder = InMemoryRecorder()
        self.dict_discovery = DummyDiscovery({})
        self.discovery = AIOCompositeDiscovery(TestLocalDiscovery(), self.dict_discovery)
        self.discovery.tracer = BasicTracer(self.recorder)
        self.svc = DummyService()
        self.svc.on_start()

    def test_remote_register(self):
        ep = AMQPEndpoint(self.discovery, self.svc, {})
        ep.on_start()
        self.discovery.register(ep)

        l = self.discovery.local
        r = self.discovery.remote
        assert l.cached_endpoints[('dummy',)] == ep
        assert r.data['dummy'] == {}

    @property
    def ctx(self):
        discovery = AIOCompositeDiscovery(TestLocalDiscovery(), self.dict_discovery)
        discovery.tracer = BasicTracer(self.recorder)
        return TracedContext(discovery)

    async def _test_remote_call(self):
        params = {'host': os.environ.get('AMQP_HOST', 'rabbitmq'),
                  'queue': 'dummy',
                  'exchange': 'tipsi',
                  'exchange_type': 'topic',
                  'routing_key': 'dummy',
                  'transport': 'amqp'}
        ep = AMQPEndpoint(self.discovery, self.svc, params)
        print('EP: {} {}'.format(ep.on_start(), ep))
        await ep.on_start()
        self.discovery.register(ep)

        pep = ProxyEndpoint(self.discovery, 'dummy', params)
        pep.on_start()

        ctx = self.ctx
        res = await ctx.rpc.dummy.ping()
        ctx.span.finish()
        self.assertEqual(res, 'pong')

    def test_remote_call(self):
        self.loop.run_until_complete(self._test_remote_call())
        self.assertEquals(len(self.recorder.get_spans()), 2)
        print('SPANS: {}'.format([(x.context.span_id, x.context.trace_id) for x in self.recorder.get_spans()]))

    def tearDown(self):
        self.loop.close()
        asyncio.set_event_loop(None)
