from unittest import TestCase

from basictracer import BasicTracer
from basictracer.recorder import InMemoryRecorder

from fan.context import Context
from fan.discovery import LocalDiscovery
from fan.remote import LocalEndpoint
from fan.tests import DummyService, NestedService


class FanTest(TestCase):
    def __init__(self, *args, **kwargs):
        super(FanTest, self).__init__(*args, **kwargs)

        self.recorder = InMemoryRecorder()
        discovery = LocalDiscovery()
        discovery.tracer = BasicTracer(self.recorder)

        for service in [DummyService, NestedService]:
            s = service()
            discovery.register(LocalEndpoint(s))

        self.context = Context(discovery)


class ServiceTest(FanTest):
    def test_01_simple(self):
        msg = 'test_message'
        with self.context:
            self.assertEqual(self.context.rpc.dummy.echo(msg), msg)

    def test_02_tree(self):
        msg = 'test_message'
        with self.context:
            self.assertEqual(self.context.rpc.nested.tree.dummy.echo(msg), msg)
