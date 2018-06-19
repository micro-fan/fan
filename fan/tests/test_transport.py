from unittest import TestCase
from fan.remote import RemoteEndpoint, Transport
from fan.discovery import SimpleDictDiscovery, CompositeDiscovery, LocalDiscovery
from fan.service import Service, endpoint


class DummyTransport(Transport):
    def on_start(self):
        super().on_start()
        self.test_started = True


class DummyDiscovery(SimpleDictDiscovery):
    pass


class FanTestLocalDiscovery(LocalDiscovery):
    transports = {'dummy': DummyTransport}

    def get_transport_class(self, name):
        return self.transports[name]


class DummyService(Service):
    name = 'dummy'

    @endpoint
    def ping(self, ctx):
        return 'pong'


class DummyRemoteEndpoint(RemoteEndpoint):
    transportClass = DummyTransport


class TransportCase(TestCase):
    def setUp(self):
        self.dict_discovery = DummyDiscovery({})
        self.discovery = CompositeDiscovery(FanTestLocalDiscovery(), self.dict_discovery)
        self.svc = DummyService()
        self.svc.on_start()

    def test_remote_register(self):
        ep = DummyRemoteEndpoint(self.discovery, self.svc, {'transport': 'dummy'})
        ep.on_start()
        self.discovery.register(ep)

        assert ep.transport.test_started
        l = self.discovery.local
        r = self.discovery.remote
        assert l.cached_endpoints[('dummy',)] == ep
        assert r.data['dummy'] == {'transport': 'dummy'}
