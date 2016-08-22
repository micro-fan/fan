from fan.rpc import RPC


class Context:
    def __init__(self, discovery, parent=None):
        self.discovery = discovery
        self.parent = parent

    def create_child_context(self):
        return Context(self.discovery, self)

    @property
    def rpc(self):
        return RPC(self.create_child_context())

    def pre_call(self):
        pass

    def post_call(self):
        pass


class TracedContext(Context):
    def __init__(self, discovery, parent=None):
        super().__init__(discovery, parent)
        if parent:
            self.span = discovery.tracer.start_span(child_of=parent.span.context)
        else:
            self.span = discovery.tracer.start_span()

    def create_child_context(self):
        return TracedContext(self.discovery, self)

    def pre_call(self):
        pass

    def post_call(self):
        self.span.finish()
