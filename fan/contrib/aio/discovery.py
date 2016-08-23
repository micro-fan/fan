from fan.discovery import CompositeDiscovery
from fan.contrib.aio.remote import AIOProxyEndpoint


class AIOCompositeDiscovery(CompositeDiscovery):
    def create_proxy(self, name, proxy_cfg):
        return AIOProxyEndpoint(self, name, proxy_cfg)
