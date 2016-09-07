import asyncio

from aiozk import ZKClient

from fan.discovery import CompositeDiscovery, RemoteDiscovery
from fan.contrib.aio.remote import AIOProxyEndpoint


class ZKDiscovery(RemoteDiscovery):
    def __init__(self, zk_path, chroot=None):
        super().__init__()
        self.zk = ZKClient(zk_path, chroot)

    async def on_start(self):
        await self.zk.start()
        if not await self.zk.exists('/endpoints'):
            await self.zk.create('/endpoints')

    def register(self, endpoint):
        name = endpoint.name

    def find_endpoint(self, service_name):
        pass

    def watch(self, path, callback):
        pass

    def unwatch(self, path, callback):
        pass


class AIOCompositeDiscovery(CompositeDiscovery):
    def create_proxy(self, name, proxy_cfg):
        return AIOProxyEndpoint(self, name, proxy_cfg)
