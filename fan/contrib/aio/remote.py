import asyncio

from fan.remote import ProxyEndpoint, Transport, RemoteEndpoint


class AIOTransport(Transport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = asyncio.get_event_loop()
        self.remote = isinstance(self.endpoint, RemoteEndpoint)
        self.terminate_future = asyncio.Future()
        self.responses = {}

    def terminate(self, reason):
        for v in self.responses.values():
            v.set_exception(reason)
        if not self.terminate_future.done():
            self.terminate_future.set_exception(reason)

    async def wait_or_terminate(self, coro):
        f = asyncio.ensure_future(coro)
        ret = await asyncio.wait([f, self.terminate_future], return_when=asyncio.FIRST_COMPLETED)
        return await ret[0].pop()

    async def rpc_call(self, name, ctx, *args, **kwargs):
        # span_id, trace_id, sampled, baggage, with_baggage_item

        c = ctx.span.context
        context_headers = {'span_id': c.span_id,
                           'trace_id': c.trace_id,
                           'sampled': c.sampled}
        self.log.debug('CTX: {}'.format(context_headers))
        msg = {'context_headers': context_headers,
               'method': name,
               'args': args,
               'kwargs': kwargs}
        f = self.responses[str(c.span_id)] = asyncio.Future()
        return await self.rpc_inner_call(msg, f)

    def send_response(self, msg):
        span_id = str(msg['context_headers']['span_id'])
        self.responses[span_id].set_result(msg)

    async def _inner_call(self, msg):
        """
        implemente here concrete interaction with pub/sub transport level
        """
        raise NotImplementedError


class AIOQueueBasedTransport:
    """
    Mixin to build queue based RPC
    """
    async def on_start(self):
        await self.sub_prepare()
        await self.pub_prepare()

    async def on_stop(self):
        await self.pub_stop()
        await self.sub_stop()

    def sub_prepare(self):
        raise NotImplementedError

    def sub_stop(self):
        raise NotImplementedError

    def pub_prepare(self):
        raise NotImplementedError

    def pub_stop(self):
        raise NotImplementedError


class AIOProxyEndpoint(ProxyEndpoint):

    def __getattr__(self, name):
        async def callable(ctx, *args, **kwargs):
            if not self.transport.started:
                await self.transport.on_start()
            ret = await self.transport.rpc_call(name, ctx, *args, **kwargs)
            self.log.debug('RPC resp: {}'.format(ret))
            return ret
        return callable

    async def on_start(self):
        await self.transport.on_start()

    async def on_stop(self):
        await self.transport.on_stop()
