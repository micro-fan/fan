from copy import copy
from fan.contrib.aio.discovery import ZKDiscovery


class SyncHelper:
    # url: viewset class
    # 'service_name': {'url', 'url_kwargs', 'args_names'}
    def __init__(self, zk_config, chroot, config):
        self.d = ZKDiscovery(zk_config, chroot)
        self.config = config
        self.registered = {}

    async def on_start(self):
        await self.d.on_start()
        await self.register_all()

    async def register_all(self):
        for service in self.config['services']:
            await self.register_endpoint(service)

    async def register_endpoint(self, service):
        service = copy(service)
        name = ['/endpoints'] + service.pop('name').split('.')
        version = service.pop('version')
        path = name + [version]
        barrier_path = '/'.join(path + ['barrier'])

        out = await self.d.register_raw(path, service, barrier_path)
        self.registered[out] = (path, service, barrier_path)
