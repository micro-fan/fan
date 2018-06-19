import logging
import os
from copy import copy

from fan.contrib.aio.discovery import ZKDiscovery

logging.basicConfig(level=logging.DEBUG)


class SyncHelper:
    # url: viewset class
    # 'service_name': {'url', 'url_kwargs', 'args_names'}
    log = logging.getLogger('SyncHelper')

    def __init__(self, zk_config, chroot, config):
        self.d = ZKDiscovery(zk_config, chroot)
        self.config = config
        self.registered = {}

    async def on_start(self):
        await self.d.on_start()
        await self.register_all()

    async def on_stop(self):
        await self.d.stop()

    async def register_all(self):
        for service in self.config['services']:
            zk_path, data = await self.register_endpoint(service)

    def gen_callback(self, path, data, service):
        async def updated(new_data):
            self.log.debug('New data: {!r}'.format(new_data))
            if new_data == data:
                return
            self.log.debug('Updated node: {} {} => {}'.format(path, data, new_data))
            self.d.unwatch(path, updated)
            await self.register_endpoint(service)

        return updated

    async def register_endpoint(self, service):
        service_original = service
        service = copy(service)
        name = ['/endpoints'] + service.pop('name').split('.')
        version = service.pop('version')
        path = name + [version]
        barrier_path = '/'.join(path + ['barrier'])

        zk_path, data = await self.d.register_raw(path, service, barrier_path)
        self.registered[zk_path] = (path, service, barrier_path)
        self.log.debug('Registered: {!r}'.format(zk_path))
        self.d.watch(zk_path, self.gen_callback(zk_path, data, service_original))
        return zk_path, data
