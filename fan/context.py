from fan.rpc import RPC
from fan.service import Service


class Context:
    def __init__(self, discovery, service=None, parent=None, name=None):
        self.discovery = discovery
        if service:
            assert isinstance(service, Service), service
        self.service = service
        self.parent = parent

        if isinstance(parent, Context):
            parent_context = parent.span.context
        else:
            parent_context = parent
        self.span = discovery.tracer.start_span(child_of=parent_context,
                                                operation_name=name)
        self._entered = False

    def create_child_context(self, name=None):
        return Context(self.discovery, self.service, self, name)

    @property
    def rpc(self):
        assert self._entered, 'You must enter context before call .rpc'
        return RPC(self)

    def pre_call(self):
        pass

    def post_call(self):
        self.span.finish()

    def __enter__(self, *args):
        self._entered = True
        self.pre_call()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.post_call()
