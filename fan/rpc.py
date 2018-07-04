import logging

from fan.exceptions import RPCException


class Caller:
    log = logging.getLogger('Caller')

    def __init__(self, parent_context, name, call_path=[]):
        self.parent_context = parent_context
        self.call_path = call_path + [name]

    def __getattr__(self, name):
        '''
        Make copy of self, usefull for caches, like:
        >>> app = ctx.rpc.app
        >>> author = app.author
        >>> author.list()
        >>> author.delete(id=1)
        '''
        return self.__class__(self.parent_context, name, self.call_path)

    def __call__(self, *args, **kwargs):
        self.log.debug('Generate call: {} {} {}'.format(self.call_path, args, kwargs))
        endpoint = self.parent_context.discovery.find_endpoint(
            tuple(self.call_path[:-1]), version_filter=None
        )
        self.log.debug('RPCEndpoint: {}'.format(endpoint))
        if endpoint:
            name = '.'.join(self.call_path)
            with self.parent_context.create_child_context(name=name) as ctx:
                method_name = self.call_path[-1]
                return endpoint.perform_call(ctx, method_name, *args, **kwargs)
        else:
            raise RPCException('No such endpoint')


class AsyncCaller(Caller):
    async def __call__(self, *args, **kwargs):
        self.log.debug('Generate call: {} {} {}'.format(self.call_path, args, kwargs))
        endpoint = await self.parent_context.discovery.find_endpoint(
            tuple(self.call_path[:-1]), version_filter=None
        )
        self.log.debug('RPCEndpoint: {}'.format(endpoint))
        if endpoint:
            name = '.'.join(self.call_path)
            async with self.parent_context.create_child_context(name=name) as ctx:
                method_name = self.call_path[-1]
                return await endpoint.perform_call(ctx, method_name, *args, **kwargs)
        else:
            raise RPCException('No such endpoint')


class RPC:
    def __init__(self, context, async_caller=False):
        self.context = context
        self.async_call = async_caller

    def __getattr__(self, name):
        return (AsyncCaller if self.async_call else Caller)(self.context, name)
