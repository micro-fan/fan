class Caller:
    def __init__(self, context, name):
        self.context = context
        self.call_path = [name]

    def __getattr__(self, name):
        self.call_path.append(name)
        return self

    def __call__(self, *args, **kwargs):
        print('Generate call: {} {} {}'.format(self.call_path, args, kwargs))
        endpoint = self.context.discovery.find_endpoint(tuple(self.call_path[:-1]))
        print('Endpoint: {}'.format(endpoint))
        if endpoint:
            handler = self.find_handler(endpoint)
            try:
                self.context.pre_call()
                return handler(self.context, *args, **kwargs)
            finally:
                self.context.post_call()

    def find_handler(self, ep):
        print('EP: {}'.format(ep))
        return getattr(ep, self.call_path[-1])


class RPC:
    def __init__(self, context):
        self.context = context

    def __getattr__(self, name):
        return Caller(self.context, name)
