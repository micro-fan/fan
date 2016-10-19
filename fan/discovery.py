import logging

from fan.remote import LocalEndpoint, Endpoint, ProxyEndpoint, RemoteEndpoint


def path_set(obj, path, what):
    curr = obj
    for name in path[:-1]:
        if name not in curr:
            curr[name] = {}
        curr = curr[name]
    curr[path[-1]] = what


def path_unset(obj, path):
    curr = obj
    for name in path[:-1]:
        if name not in path:
            return
        curr = curr[name]
    del curr[path[-1]]


def path_get(obj, path):
    curr = obj
    for name in path:
        if name not in path:
            return
        curr = curr[name]
    return curr


class LocalDiscovery:
    """
    Service types:
        singleton - only a single service allowed
        round_robin - any worker can handle request
        shard - only a concrete worker can handle request
    """
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.cached_endpoints = {}

    def register(self, endpoint: Endpoint):
        if isinstance(endpoint, LocalEndpoint):
            service = endpoint.service
            path = tuple(service.name.split('.'))
            self.cached_endpoints[path] = endpoint
        else:
            path = tuple(endpoint.name)
            self.cached_endpoints[path] = endpoint

    def find_endpoint(self, service_name, version_filter=None):
        if service_name in self.cached_endpoints:
            return self.cached_endpoints[service_name]


class RemoteDiscovery:
    '''
    Doesn't contain any objects, only hierarchical data with primitive datatypes
    May cache data, but should invalidate cached in distributed deployments
    '''
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)
        self.watchers = {}

    def register(self, endpoint):
        raise NotImplementedError

    def find_endpoint(self, service_name, version_filter):
        return self.find_remote_endpoint(service_name, version_filter)

    def find_remote_endpoint(self, service_name, version_filter):
        raise NotImplementedError

    def watch(self, path, callback):
        raise NotImplementedError

    def unwatch(self, path, callback):
        raise NotImplementedError


def version_filter(constraints):
    def filter_versions(versions):
        if len(versions):
            # TODO: add actual filter
            return versions[0]
    return filter_versions


class CompositeDiscovery:
    def __init__(self, local, remote):
        self.log = logging.getLogger(self.__class__.__name__)
        self.local = local
        self.remote = remote

    def find_endpoint(self, name, version_filter=[]):
        local = self.local.find_endpoint(name, version_filter)
        if local:
            return local
        proxy_cfg = self.remote.find_endpoint(name, version_filter)
        if proxy_cfg:
            ep = self.create_proxy(name, proxy_cfg)
            self.local.register(ep)
            return ep

    def create_proxy(self, name, proxy_cfg):
        return ProxyEndpoint(self, name, proxy_cfg)

    def register(self, endpoint):
        self.local.register(endpoint)
        if isinstance(endpoint, RemoteEndpoint):
            self.remote.register(endpoint)

    # TODO: transport info should go from process
    def get_transport_class(self, name):
        return self.local.get_transport_class(name)


class SimpleDictDiscovery(RemoteDiscovery):
    def __init__(self, conf):
        super().__init__()
        self.data = conf

    def register(self, endpoint):
        path, data = endpoint.service.name.split('.'), endpoint.remote_params
        path_set(self.data, path, data)

    def find_endpoint(self, path, version_filter):
        return path_get(self.data, path)

    def watch(self, path, cb):
        pass

    def unwatch(self, path, cb):
        pass
