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

    async def recursive_create(self, path, version, data):
        if not isinstance(path, list):
            path = path.split('/')
        curr = '/'
        await self.create(curr)
        for sub in path:
            curr += '/{}'.format(sub)
            await self.create(curr, container=True)
        curr += '/{}'.format(version)
        await self.create(curr, container=True)

        curr += '/config_'
        await self.create(curr, data=data, ephemeral=True, sequential=True)

    async def on_start(self):
        await self.zk.start()
        if not await self.zk.exists('/endpoints'):
            await self.zk.create('/endpoints')

    async def register(self, endpoint):
        name = endpoint.name
        path = name.split('.')
        barrier = self.zk.recipes.Barrier('{}/{}/barrier'.format(name, endpoint.version))
        await barrier.wait()
        await self.recursive_create(path, endpoint.version, endpoint.config)

    def find_endpoint(self, service_name, version_filter):
        pass

    def watch(self, path, callback):
        pass

    def unwatch(self, path, callback):
        pass


class AIOCompositeDiscovery(CompositeDiscovery):
    def create_proxy(self, name, proxy_cfg):
        return AIOProxyEndpoint(self, name, proxy_cfg)
