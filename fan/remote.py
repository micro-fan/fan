class Transport:
    def __init__(self, discovery, endpoint, params):
        self.discovery = discovery
        self.params = params
        self.endpoint = endpoint
        self.started = False
        self.stopped = False

    def on_start(self):
        self.started = True

    def on_stop(self):
        self.stopped = True

    def rpc_call(self, ctx, *args, **kwargs):
        raise NotImplementedError

    def handle_call(self, method, ctx, *args, **kwargs):
        return getattr(self.endpoint, method)(ctx, *args, **kwargs)


class Endpoint:

    def __init__(self, service):
        self.service = service
        self.started = False
        self.stopped = False

    def on_start(self):
        self.started = True

    def on_stop(self):
        self.stopped = True


class ProxyEndpoint(Endpoint):
    def __init__(self, discovery, name, params):
        self.name = name
        self.params = params
        self.discovery = discovery
        transportClass = discovery.get_transport_class(params['transport'])
        self.transport = transportClass(discovery, self, params)

    def __getattr__(self, name):
        if name in ('name', 'params', 'discovery', 'transport'):
            return object.__getattribute__(self, name)

        def callable(ctx, *args, **kwargs):
            ret = self.transport.rpc_call(name, ctx, *args, **kwargs)
            return ret
        return callable

    def on_start(self):
        self.transport.on_start()

    def on_stop(self):
        return self.transport.on_stop()


class LocalEndpoint(Endpoint):
    """
    For in-process communications only
    """
    def __getattr__(self, name):
        print('Obj: {} {}'.format(self.service, name))
        return getattr(self.service, self.service._rpc[name])


class RemoteEndpoint(LocalEndpoint):
    """
    Listen some remote transport for incoming messages
    """

    def __init__(self, discovery, service, params):
        self.service = service
        self.params = params
        self.remote_params = params
        if 'transport' in params:
            self.transportClass = discovery.get_transport_class(params['transport'])
        else:
            self.transportClass = Transport
        self.transport = self.transportClass(discovery, self, params)

    def on_start(self):
        self.transport.on_start()

    def on_stop(self):
        return self.transport.on_stop()
