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
        self.cached_endpoints = {}

    def register(self, endpoint: Endpoint):
        if isinstance(endpoint, LocalEndpoint):
            service = endpoint.service
            path = tuple(service.service_name.split('.'))
            self.cached_endpoints[path] = endpoint
        else:
            path = tuple(endpoint.name)
            self.cached_endpoints[path] = endpoint

    def find_endpoint(self, service_name):
        if service_name in self.cached_endpoints:
            return self.cached_endpoints[service_name]


class RemoteDiscovery:
    '''
    Doesn't contain any objects, only hierarchical data with primitive datatypes
    May cache data, but should invalidate cached in distributed deployments
    '''
    def __init__(self):
        self.watchers = {}

    def register(self, path, config):
        raise NotImplementedError

    def find_endpoint(self, service_name):
        return self.find_remote_endpoint(service_name)

    def find_remote_endpoint(self, service_name):
        raise NotImplementedError

    def watch(self, path, callback):
        raise NotImplementedError

    def unwatch(self, path, callback):
        raise NotImplementedError


class CompositeDiscovery:
    def __init__(self, local, remote):
        self.local = local
        self.remote = remote

    def find_endpoint(self, name):
        local = self.local.find_endpoint(name)
        if local:
            return local
        proxy_cfg = self.remote.find_endpoint(name)
        if proxy_cfg:
            ep = self.create_proxy(name, proxy_cfg)
            self.local.register(ep)
            return ep

    def create_proxy(self, name, proxy_cfg):
        return ProxyEndpoint(self, name, proxy_cfg)

    def register(self, endpoint):
        self.local.register(endpoint)
        if isinstance(endpoint, RemoteEndpoint):
            self.remote.register(endpoint.service.service_name.split('.'), endpoint.remote_params)

    # TODO: transport info should go from process
    def get_transport_class(self, name):
        return self.local.get_transport_class(name)


class SimpleDictDiscovery(RemoteDiscovery):
    def __init__(self, conf):
        super().__init__()
        self.data = conf

    def register(self, path, data):
        path_set(self.data, path, data)

    def find_endpoint(self, path):
        return path_get(self.data, path)

    def watch(self, path, cb):
        pass

    def unwatch(self, path, cb):
        pass
