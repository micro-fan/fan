import json
import logging

from aiozk import ZKClient, exc

from fan.contrib.kazoo.discovery import VSN_RE
from fan.discovery import CompositeDiscovery, RemoteDiscovery
from fan.contrib.aio.remote import AIOProxyEndpoint


class ZKDiscovery(RemoteDiscovery):
    log = logging.getLogger('fan.aio.ZKDiscovery')

    def __init__(self, zk_path, chroot='/', with_data_watcher=True, loop=None):
        super().__init__()
        self.chroot = chroot
        self.with_data_watcher = with_data_watcher
        self.closing = False
        self.loop = loop
        self.check_zk_task = None
        self.zk = ZKClient(zk_path, chroot, loop=loop)

    async def create(self, path, **kwargs):
        try:
            return await self.zk.create(path, **kwargs)
        except exc.NodeExists:
            pass

    async def recursive_create(self, path, data) -> tuple:
        if not isinstance(path, list):
            path = path.split('/')
        curr = '/'
        await self.create(curr)
        for sub in path:
            curr += '/{}'.format(sub)
            await self.create(curr, container=True)
        curr += '/config_'
        if not (isinstance(data, str) or isinstance(data, bytes)):
            data = json.dumps(data).encode()
        out = await self.create(curr, data=data, ephemeral=True, sequential=True)
        return out, data

    async def on_start(self):
        await self.zk.start()
        if not await self.zk.exists('/endpoints'):
            await self.zk.create('/endpoints')
        if self.with_data_watcher:
            self.data_watcher = self.zk.recipes.DataWatcher()
            self.data_watcher.set_client(self.zk)

    async def register(self, endpoint) -> tuple:
        name = endpoint.name
        path = name.split('.')
        path.append(endpoint.version)
        barrier_path = '/'.join(path + ['barrier'])
        return await self.register_raw(path, endpoint.config, barrier_path)

    async def register_raw(self, path, data, barrier_path=None) -> tuple:
        if barrier_path:
            barrier = self.zk.recipes.Barrier(barrier_path)
            await barrier.wait()
        return await self.recursive_create(path, data)

    async def find_endpoint(self, service_name, version_filter):
        path = ['/endpoints'] + service_name.split('.')
        path = '/'.join(path)

        if not await self.zk.exists(path):
            return
        childs = await self.zk.get_children(path)
        if len(childs) == 0:
            return

        childs = [x for x in childs if VSN_RE.match(x)]
        version = sorted(childs)[-1]
        vpath = '{}/{}'.format(path, version)
        configs = await self.zk.get_children(vpath)
        # keep only config_ paths
        configs = filter(lambda x: x.startswith('config_'), configs)
        return await self.create_endpoint(service_name, vpath, configs)

    async def create_endpoint(self, name, path, configs):
        # TODO: pass all configs
        configs = sorted(configs)
        assert len(configs), (name, path, configs)
        config = configs[-1]
        dpath = '{}/{}'.format(path, config)
        data = await self.zk.get_data(dpath)
        self.log.debug('GET DATA: {!r}'.format(data))
        params = json.loads(data)
        return AIOProxyEndpoint(self, name, params)

    def watch(self, path, callback):
        self.data_watcher.add_callback(path, callback)

    def unwatch(self, path, callback):
        self.data_watcher.remove_callback(path, callback)

    def remove_all_wathers(self):
        for path, callbacks in list(self.data_watcher.callbacks.items()):
            for callback in list(callbacks):
                self.data_watcher.remove_callback(path, callback)

    async def stop(self):
        self.closing = True
        self.remove_all_wathers()
        await self.zk.session.close()


class AIOCompositeDiscovery(CompositeDiscovery):
    def create_proxy(self, name, proxy_cfg):
        return AIOProxyEndpoint(self, name, proxy_cfg)


def ensure_started(fn):
    async def _inner(self, *args, **kwargs):
        if not getattr(self, '_started', False):
            await self.on_start()
            setattr(self, '_started', True)
        return await fn(self, *args, **kwargs)

    return _inner


class LazyAiozkDiscovery(ZKDiscovery):
    @ensure_started
    async def find_endpoint(self, service_tuple, version_filter):
        service_name = '.'.join(service_tuple)
        return await super().find_endpoint(service_name, version_filter)

    @ensure_started
    async def create_endpoint(self, *args, **kwargs):
        return await super().create_endpoint(*args, **kwargs)

    def get_transport_class(self, name):
        return self.transport_classes[name]
