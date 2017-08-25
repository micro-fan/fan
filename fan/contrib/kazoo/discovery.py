import json
import logging

from kazoo.client import KazooClient

from fan.discovery import RemoteDiscovery
from fan.remote import ProxyEndpoint


class KazooDiscovery(RemoteDiscovery):
    log = logging.getLogger('KazooDiscovery')

    def __init__(self, zk_path, chroot=None):
        super().__init__()
        self.zk = KazooClient(hosts=zk_path)

    def on_start(self):
        self.zk.start()

    def on_stop(self):
        self.zk.stop()

    def find_endpoint(self, service_tuple, version_filter):
        path = ['/endpoints'] + list(service_tuple)
        path = '/'.join(path)

        if not self.zk.exists(path):
            return
        childs = self.zk.get_children(path)
        if len(childs) == 0:
            return

        version = sorted(childs)[-1]
        vpath = '{}/{}'.format(path, version)
        configs = self.zk.get_children(vpath)
        # keep only config_ paths
        configs = filter(lambda x: x.startswith('config_'), configs)
        return self.create_endpoint(service_tuple, vpath, configs)

    def get_transport_class(self, name):
        return self.transport_classes[name]

    def create_endpoint(self, name, path, configs):
        # TODO: pass all configs
        configs = sorted(configs)
        assert len(configs), configs
        config = configs[-1]
        dpath = '{}/{}'.format(path, config)
        data = self.zk.get(dpath)[0].decode('utf8')
        self.log.debug('GET DATA: {!r}'.format(data))
        params = json.loads(data)
        return ProxyEndpoint(self, name, params)
