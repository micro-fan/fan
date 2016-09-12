import asyncio
import os

# TODO separate
from aiozk.test.aio_test import AIOTestCase

from fan.contrib.aio.discovery import ZKDiscovery
from fan.tests import TEST_TIMEOUT


class StubEndpoint:
    name = 'this.is.stub.endpoint'
    version = '1.0.0'
    config = '{"stub": "config"}'


class TestZK(AIOTestCase):
    async def setUp(self):
        self.remote = ZKDiscovery(os.environ.get('ZK_HOST', 'zk:2181'), chroot='/test_fan')
        await asyncio.wait_for(self.remote.on_start(), TEST_TIMEOUT)

    async def test_init(self):
        pass

    async def test_register_function(self):
        data = b'{"test": "data"}'
        await self.remote.recursive_create('/test/sub1/sub2', '1.0.0', data)
        base_path = '/endpoints/test/sub1/sub2/1.0.0'
        names = await self.remote.zk.get_children(base_path)
        out = await self.remote.zk.get_data('{}/{}'.format(base_path, names[0]))
        self.assertEqual(data, out)
        await self.remote.zk.deleteall('/endpoints/test')

    async def test_register_endpoint(self):
        ep = StubEndpoint()
        await self.remote.register(ep)
