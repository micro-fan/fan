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
    def __init__(self, discovery, parent=None, name=None):
        super().__init__(discovery, parent)
        if isinstance(parent, Context):
            parent_context = parent.span.context
        else:
            parent_context = parent
        self.span = discovery.tracer.start_span(child_of=parent_context,
                                                operation_name=name)

    def create_child_context(self, name=None):
        return TracedContext(self.discovery, self, name)

    def pre_call(self):
        pass

    def post_call(self):
        self.span.finish()
