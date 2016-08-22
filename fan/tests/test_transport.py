from unittest import TestCase
from fan.remote import RemoteEndpoint, ProxyEndpoint, Transport
from fan.discovery import SimpleDictDiscovery, CompositeDiscovery, LocalDiscovery
from fan.service import Service, endpoint


class DummyDiscovery(SimpleDictDiscovery):
    pass


class DummyTransport(Transport):
    def on_start(self):
        super().on_start()
        self.test_started = True


class DummyService(Service):
    service_name = 'dummy'

    @endpoint
    def ping(self, ctx):
        return 'pong'


class TransportCase(TestCase):
    def setUp(self):
        self.dict_discovery = DummyDiscovery({})
        self.discovery = CompositeDiscovery(LocalDiscovery(), self.dict_discovery)
        self.svc = DummyService()
        self.svc.on_start()

    def test_remote_register(self):
        ep = RemoteEndpoint(self.discovery, self.svc, {'transport': DummyTransport})
        ep.on_start()
        self.discovery.register(ep)

        assert ep.transport.test_started
        l = self.discovery.local
        r = self.discovery.remote
        assert l.cached_endpoints[('dummy',)] == ep
        assert r.cached_endpoints['dummy']['transport'] == DummyTransport
