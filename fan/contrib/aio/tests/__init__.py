import asyncio
import logging

from basictracer import BasicTracer
from basictracer.recorder import InMemoryRecorder
from tipsi_tools.testing.aio import AIOTestCase

from fan.context import Context
from fan.contrib.aio.remote import AIOProxyEndpoint, AIOTransport
from fan.remote import RemoteEndpoint

from fan.discovery import SimpleDictDiscovery, LocalDiscovery
from fan.contrib.aio.discovery import AIOCompositeDiscovery
from fan.service import Service, endpoint
from fan.tests import TEST_TIMEOUT


class AIODummyTransport(AIOTransport):
    requests = {}

    async def on_start(self):
        if self.remote:
            self.channel = asyncio.Queue()
            self.requests[self.params['id']] = self.channel
        else:
            self.channel = asyncio.Queue()
        self._read = asyncio.ensure_future(self.read_loop())

    async def on_stop(self):
        self._read.cancel()

    async def rpc_inner_call(self, msg, future):
        msg['response'] = self.channel
        await self.requests[self.params['id']].put(msg)
        return await future


class AIODummyEndpoint(RemoteEndpoint):
    async def on_start(self):
        await self.transport.on_start()

    async def on_stop(self):
        await self.transport.on_stop()


class TestLocalDiscovery(LocalDiscovery):
    transports = {'aiodummy': AIODummyTransport}

    def get_transport_class(self, name):
        return self.transports[name]


class DummyService(Service):
    name = 'dummy'

    @endpoint
    def ping(self, ctx):
        self.log.debug('Call ping endpoint {}'.format(ctx.span.context.trace_id))
        return 'pong'


class AIOEndpointCase(AIOTestCase):
    endpoint_class = AIODummyEndpoint
    transport_class = AIODummyTransport
    endpoint_params = {'id': 'test', 'transport': 'aiotest'}

    def __init__(self, *args):
        super().__init__(*args)
        self.log = logging.getLogger(self.__class__.__name__)

    def get_local_discovery(self):
        ld = TestLocalDiscovery()
        TestLocalDiscovery.transports = {self.endpoint_params['transport']: self.transport_class}
        if not hasattr(self, 'ctx_local'):
            self.ctx_local = []
        self.ctx_local.append(ld)
        return ld

    async def setUp(self):
        self.recorder = InMemoryRecorder()
        self.dict_discovery = SimpleDictDiscovery({})
        self.discovery = AIOCompositeDiscovery(self.get_local_discovery(),
                                               self.dict_discovery)
        self.discovery.tracer = BasicTracer(self.recorder)
        self.svc = DummyService()
        await self.ensure_future(self.svc.on_start())

    async def _test_remote_register(self):
        ep = self.endpoint_class(self.discovery, self.svc, self.endpoint_params)
        await asyncio.wait_for(ep.on_start(), TEST_TIMEOUT)
        self.discovery.register(ep)

        l = self.discovery.local
        r = self.discovery.remote
        assert l.cached_endpoints[('dummy',)] == ep
        assert r.data['dummy'] == self.endpoint_params

    @property
    def ctx(self):
        ld = TestLocalDiscovery()
        self.ctx_local.append(ld)
        discovery = AIOCompositeDiscovery(self.get_local_discovery(),
                                          self.dict_discovery)
        discovery.tracer = BasicTracer(self.recorder)
        return Context(discovery, self.svc)

    async def _test_remote_call(self):
        params = self.endpoint_params
        ep = self.endpoint_class(self.discovery, self.svc, params)
        await asyncio.wait_for(ep.on_start(), TEST_TIMEOUT)
        self.discovery.register(ep)

        with self.ctx as ctx:
            res = await asyncio.wait_for(ctx.rpc.dummy.ping(), TEST_TIMEOUT)
        self.assertEqual(res, 'pong')
        self.assertEquals(len(self.recorder.get_spans()), 2)
        spans = [(x.context.span_id, x.context.trace_id) for x in self.recorder.get_spans()]
        self.log.debug('SPANS: {}'.format(spans))

    async def tearDown(self):
        for ld in self.ctx_local:
            for v in ld.cached_endpoints.values():
                await v.on_stop()
