from unittest import TestCase

from fan.context import Context
from fan.discovery import LocalDiscovery
from fan.remote import LocalEndpoint
from fan.service import Service, endpoint


class DummyService(Service):
    service_name = 'dummy'

    @endpoint('echo')
    def method(self, context, message):
        print('SELF: {} MSG: {}'.format(self, message))
        return message


class D2Service(DummyService):
    service_name = 'tree.dummy'


class FanTest(TestCase):
    def __init__(self, *args, **kwargs):
        super(FanTest, self).__init__(*args, **kwargs)
        discovery = LocalDiscovery()

        for service in [DummyService, D2Service]:
            s = service()
            discovery.register(LocalEndpoint(s))

        self.context = Context(discovery)


class ServiceTest(FanTest):
    def test_01_simple(self):
        msg = 'test_message'
        self.assertEqual(self.context.rpc.dummy.echo(msg), msg)

    def test_02_tree(self):
        msg = 'test_message'
        self.assertEqual(self.context.rpc.tree.dummy.echo(msg), msg)
