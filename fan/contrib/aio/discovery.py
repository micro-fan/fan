import json

from aiozk import ZKClient, exc

from fan.discovery import CompositeDiscovery, RemoteDiscovery
from fan.contrib.aio.remote import AIOProxyEndpoint


class ZKDiscovery(RemoteDiscovery):
    def __init__(self, zk_path, chroot=None):
        super().__init__()
        self.zk = ZKClient(zk_path, chroot)

    async def create(self, path, **kwargs):
        try:
            return await self.zk.create(path, **kwargs)
        except exc.NodeExists:
            pass

    async def recursive_create(self, path, data) -> str:
        if not isinstance(path, list):
            path = path.split('/')
        curr = '/'
        await self.create(curr)
        for sub in path:
            curr += '/{}'.format(sub)
            await self.create(curr, container=True)
        curr += '/config_'
        if not (isinstance(data, str) or isinstance(data, bytes)):
            data = json.dumps(data)
        out = await self.create(curr, data=data, ephemeral=True, sequential=True)
        return out

    async def on_start(self):
        await self.zk.start()
        if not await self.zk.exists('/endpoints'):
            await self.zk.create('/endpoints')

    async def register(self, endpoint) -> str:
        name = endpoint.name
        path = name.split('.')
        path.append(endpoint.version)
        barrier_path = '/'.join(path + ['barrier'])
        return await self.register_raw(path, endpoint.config, barrier_path)

    async def register_raw(self, path, data, barrier_path=None) -> str:
        if barrier_path:
            barrier = self.zk.recipes.Barrier(barrier_path)
            await barrier.wait()
        return await self.recursive_create(path, data)

    def find_endpoint(self, service_name, version_filter):
        pass

    def watch(self, path, callback):
        pass

    def unwatch(self, path, callback):
        pass


class AIOCompositeDiscovery(CompositeDiscovery):
    def create_proxy(self, name, proxy_cfg):
        return AIOProxyEndpoint(self, name, proxy_cfg)
