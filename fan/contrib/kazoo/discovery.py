import json
import logging
import re

from kazoo.client import KazooClient

from fan.discovery import RemoteDiscovery
from fan.remote import ProxyEndpoint


VSN_RE = re.compile(r'^\d+(\.\d+){,2}$')


class KazooWrapper:
    """
    Wrapper that ensure that we connect to zookeeper only when need it.
    Prevents connection on error request.
    """
    log = logging.getLogger('KazooWrapper')

    def __init__(self, chroot=None, *args, **kwargs):
        self.zk = KazooClient(*args, **kwargs)
        self._started = False
        self.chroot = chroot

    def start(self):
        self.log.debug('Connect')
        self.zk.start()
        if self.chroot:
            self.zk.ensure_path(self.chroot)
            self.zk.chroot = self.chroot
        self._started = True

    def stop(self):
        if self._started:
            self.zk.stop()

    def __getattr__(self, name):
        if not self._started:
            self.start()
        return getattr(self.zk, name)


class KazooDiscovery(RemoteDiscovery):
    log = logging.getLogger('KazooDiscovery')
    timeout = 5

    def __init__(self, zk_path, chroot='/'):
        super().__init__()
        self.zk = KazooWrapper(chroot=chroot, hosts=zk_path, timeout=self.timeout)

    def on_start(self):
        self.zk.start(timeout=self.timeout)

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
        childs = [x for x in childs if VSN_RE.match(x)]
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
        assert len(configs), (name, path, configs)
        config = configs[-1]
        dpath = '{}/{}'.format(path, config)
        data = self.zk.get(dpath)[0].decode('utf8')
        self.log.debug('GET DATA: {!r}'.format(data))
        params = json.loads(data)
        return ProxyEndpoint(self, name, params)
