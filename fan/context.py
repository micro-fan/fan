from fan.rpc import RPC
from fan.service import Service


class Context:
    def __init__(self, discovery, service=None, parent=None):
        self.discovery = discovery
        if service:
            assert isinstance(service, Service), service
        self.service = service
        self.parent = parent

    def create_child_context(self):
        return Context(self.discovery, self.service, self)

    @property
    def rpc(self):
        return RPC(self.create_child_context())

    def pre_call(self):
        pass

    def post_call(self):
        pass


class TracedContext(Context):
    def __init__(self, discovery, service=None, parent=None, name=None):
        super().__init__(discovery, service, parent)
        if isinstance(parent, Context):
            parent_context = parent.span.context
        else:
            parent_context = parent
        self.span = discovery.tracer.start_span(child_of=parent_context,
                                                operation_name=name)

    def create_child_context(self, name=None):
        return TracedContext(self.discovery, self.service, self, name)

    def pre_call(self):
        pass

    def post_call(self):
        self.span.finish()
