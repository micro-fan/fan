from fan.service import Service, endpoint
from fan.remote import Transport, RemoteEndpoint


TEST_TIMEOUT = 5


class DummyService(Service):
    name = 'dummy'

    @endpoint('echo')
    def method(self, context, message):
        self.log.debug('SELF: {} MSG: {}'.format(self, message))
        return message


class NestedService(DummyService):
    name = 'nested.tree.dummy'


class DummyTracer(Service):
    '''
    Call itself given number of times
    '''
    name = 'dummy_tracer'

    @endpoint('echo')
    def echo(self, ctx, word, count):
        if count > 0:
            return ctx.rpc.dummy_tracer.echo(word, count-1)
        else:
            return word, 0


class DummyTransport(Transport):
    storage = {}  # type: dict

    def __init__(self, discovery, endpoint, params):
        super().__init__(discovery, endpoint, params)
        if isinstance(endpoint, RemoteEndpoint):
            self.storage[params['id']] = self

    def rpc_call(self, method, ctx, *args, **kwargs):
        self.log.debug('Storage: {}'.format(self.storage))
        remote_ep = self.storage[self.params['id']]
        return remote_ep.handle_call(method, ctx, *args, **kwargs)
