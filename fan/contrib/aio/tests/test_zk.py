import asyncio
import os

from tipsi_tools.testing.aio import AIOTestCase

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
        await self.remote.recursive_create('/endpoints/test/sub1/sub2/1.0.0', data)
        base_path = '/endpoints/test/sub1/sub2/1.0.0'
        names = await self.remote.zk.get_children(base_path)
        out = await self.remote.zk.get_data('{}/{}'.format(base_path, names[0]))
        self.assertEqual(data, out)
        await self.remote.zk.deleteall('/endpoints/test')

    async def test_register_endpoint(self):
        ep = StubEndpoint()
        await self.remote.register(ep)

    async def test_register_barrier(self):
        ep = StubEndpoint()
        zk = self.remote.zk
        barrier_name = '{}/{}/barrier'.format(ep.name, ep.version)
        barrier = zk.recipes.Barrier(barrier_name)
        await barrier.create()
        fut = asyncio.ensure_future(self.remote.register(ep))
        try:
            await asyncio.wait_for(fut, 0.12)
        except asyncio.TimeoutError:
            pass
        await barrier.lift()
        await self.remote.register(ep)
