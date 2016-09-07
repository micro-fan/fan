import asyncio
import os
import logging
from unittest import TestCase

from basictracer import BasicTracer
from basictracer.recorder import InMemoryRecorder

# TODO separate
from aiozk.test.aio_test import AIOTestCase

from fan.context import TracedContext
from fan.contrib.aio.remote import AIOProxyEndpoint
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
        self.log.debug('Call ping endpoint {}'.format(ctx.span.context.trace_id))
        return 'pong'


class AMQPCase(AIOTestCase):
    log = logging.getLogger('AMQPCase')

    async def setUp(self):
        self.recorder = InMemoryRecorder()
        self.dict_discovery = DummyDiscovery({})
        self.discovery = AIOCompositeDiscovery(TestLocalDiscovery(), self.dict_discovery)
        self.discovery.tracer = BasicTracer(self.recorder)
        self.svc = DummyService()
        await self.ensure_future(self.svc.on_start())

    async def test_remote_register(self):
        params = {'host': os.environ.get('AMQP_HOST', 'rabbitmq'),
                  'queue': 'dummy',
                  'exchange': 'tipsi',
                  'exchange_type': 'topic',
                  'routing_key': 'dummy',
                  'transport': 'amqp'}
        ep = AMQPEndpoint(self.discovery, self.svc, params)
        await ep.on_start()
        self.discovery.register(ep)

        l = self.discovery.local
        r = self.discovery.remote
        assert l.cached_endpoints[('dummy',)] == ep
        assert r.data['dummy'] == params

    @property
    def ctx(self):
        discovery = AIOCompositeDiscovery(TestLocalDiscovery(), self.dict_discovery)
        discovery.tracer = BasicTracer(self.recorder)
        return TracedContext(discovery)

    async def test_remote_call(self):
        params = {'host': os.environ.get('AMQP_HOST', 'rabbitmq'),
                  'queue': 'dummy',
                  'exchange': 'tipsi',
                  'exchange_type': 'topic',
                  'routing_key': 'dummy',
                  'transport': 'amqp'}
        ep = AMQPEndpoint(self.discovery, self.svc, params)
        await ep.on_start()
        self.discovery.register(ep)

        pep = AIOProxyEndpoint(self.discovery, 'dummy', params)
        await pep.on_start()

        ctx = self.ctx
        res = await ctx.rpc.dummy.ping()
        ctx.span.finish()
        self.assertEqual(res, 'pong')
        self.assertEquals(len(self.recorder.get_spans()), 2)
        spans = [(x.context.span_id, x.context.trace_id) for x in self.recorder.get_spans()]
        self.log.debug('SPANS: {}'.format(spans))
