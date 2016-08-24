import asyncio

from fan.remote import ProxyEndpoint, Transport, RemoteEndpoint


class AIOTransport(Transport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = asyncio.get_event_loop()
        self.remote = isinstance(self.endpoint, RemoteEndpoint)

    async def rpc_call(self, name, ctx, *args, **kwargs):
        # span_id, trace_id, sampled, baggage, with_baggage_item

        c = ctx.span.context
        context_headers = {'span_id': c.span_id,
                           'trace_id': c.trace_id,
                           'sampled': c.sampled}
        print('CTX: {}'.format(context_headers))
        msg = {'context_headers': context_headers,
               'method': name,
               'args': args,
               'kwargs': kwargs}
        return await self._inner_call(msg)

    async def _inner_call(self, msg):
        raise NotImplementedError


class AIOProxyEndpoint(ProxyEndpoint):

    def __getattr__(self, name):
        async def callable(ctx, *args, **kwargs):
            if not self.transport.started:
                await self.transport.on_start()
            ret = await self.transport.rpc_call(name, ctx, *args, **kwargs)
            print('RPC resp: {}'.format(ret))
            return ret
        return callable

    async def on_start(self):
        await self.transport.on_start()

    async def on_stop(self):
        await self.transport.on_stop()
