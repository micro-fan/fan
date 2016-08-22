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


class BaseDiscovery:
    """
    Service types:
        singleton - only a single service allowed
        round_robin - any worker can handle request
        shard - only a concrete worker can handle request
    """
    def get_connection_params(self, conn_name: str) -> dict:
        raise NotImplementedError

    def register(self, endpoint: Endpoint):
        raise NotImplementedError

    def subscribe_queue(self, queue: str, obj):
        raise NotImplementedError

    def unsubscribe_queue(self, queue: str, obj):
        raise NotImplementedError


class LocalDiscovery(BaseDiscovery):
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


class RemoteDiscovery(BaseDiscovery):
    '''
    Doesn't contain any objects, only hierarchical data with primitive datatypes
    May cache data, but should invalidate cached in distributed deployments
    '''
    def __init__(self):
        self.cached_endpoints = {}

    def register(self, path, config):
        raise NotImplementedError

    def watch(self, path, callback):
        raise NotImplementedError

    def find_endpoint(self, service_name):
        if service_name in self.cached_endpoints:
            return self.cached_endpoints[service_name]
        return self.find_remote_endpoint(service_name)

    def find_remote_endpoint(self, service_name):
        raise NotImplementedError


class CompositeDiscovery(BaseDiscovery):
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


class SimpleDictDiscovery(RemoteDiscovery):
    def __init__(self, conf):
        super().__init__()
        self.connections = {}

        for conn_dict in conf.get('connections', []):
            assert 'transport' in conn_dict, '"transport" param is required in connection dict'
            assert 'connection' in conn_dict, '"connection" param is required in connection dict'
            assert 'params' in conn_dict, '"param" param is required in connection dict'
            connection = conn_dict['connection']
            self.connections[connection] = conn_dict

        for proxy_dict in conf.get('proxy_endpoints', []):
            assert 'transport' in proxy_dict, '"transport" is required in proxy dict'
            assert 'endpoint' in proxy_dict, '"endpoint" param is required in proxy dict'
            assert 'params' in proxy_dict, '"params" param is required in proxy dict'

            self.register_remote(proxy_dict['endpoint'], proxy_dict['transport'],
                                 proxy_dict['params'])

    def register(self, path, data):
        path_set(self.cached_endpoints, path, data)

    def find_endpoint(self, path):
        return path_get(self.cached_endpoints, path)

    def get_connection_params(self, conn_name: str):
        if conn_name in self.connections:
            return self.connections
        else:
            raise RuntimeError('"{}" connection is missing '
                               'in simple dict discovery'.format(conn_name))
