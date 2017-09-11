import logging


class Transport:
    def __init__(self, discovery, endpoint, params):
        self.log = logging.getLogger(self.__class__.__name__)
        self.discovery = discovery
        self.params = params
        self.endpoint = endpoint
        self.started = False
        self.stopped = False

    def on_start(self):
        self.started = True

    def on_stop(self):
        self.stopped = True

    def rpc_call(self, method, ctx, *args, **kwargs):
        raise NotImplementedError

    def handle_call(self, method, ctx, *args, **kwargs):
        return getattr(self.endpoint, method)(ctx, *args, **kwargs)


class Endpoint:
    name = None  # type: str

    def __init__(self, service):
        self.log = logging.getLogger(self.__class__.__name__)
        self.service = service
        self.started = False
        self.stopped = False

    def on_start(self):
        self.started = True

    def on_stop(self):
        self.stopped = True


class ProxyEndpoint(Endpoint):
    def __init__(self, discovery, name, params):
        self.log = logging.getLogger(self.__class__.__name__)
        self.name = name
        self.params = params
        self.discovery = discovery
        transportClass = discovery.get_transport_class(params['transport'])
        self.transport = transportClass(discovery, self, params)

    def perform_call(self, ctx, method_name, *args, **kwargs):
        if not self.transport.started:
            self.transport.on_start()
        # ctx.span.operation_name = method_name
        return self.transport.rpc_call(method_name, ctx, *args, **kwargs)

    def on_start(self):
        self.transport.on_start()

    def on_stop(self):
        return self.transport.on_stop()


class LocalEndpoint(Endpoint):
    """
    For in-process communications only
    """
    def __init__(self, service):
        super().__init__(service)
        self.name = service.name.split('.')
        self.log = logging.getLogger(self.__class__.__name__)

    def perform_call(self, ctx, method_name, *args, **kwargs):
        return getattr(self.service, self.service._rpc[method_name])(ctx, *args, **kwargs)


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

    @property
    def name(self):
        return self.service.name

    def on_start(self):
        self.transport.on_start()

    def on_stop(self):
        return self.transport.on_stop()

    def __getattr__(self, name):
        return getattr(self.service, self.service._rpc[name])
