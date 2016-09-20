import asyncio
from types import CoroutineType

from basictracer.context import SpanContext

from fan.context import Context
from fan.remote import ProxyEndpoint, Transport, RemoteEndpoint


class AIOTransport(Transport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = asyncio.get_event_loop()
        self.remote = isinstance(self.endpoint, RemoteEndpoint)
        self.responses = {}

    def terminate(self, reason):
        for v in self.responses.values():
            v.set_exception(reason)

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
        resp = await self.rpc_inner_call(msg, f)
        return resp['response']

    async def read_loop(self, *args, **kwargs):
        try:
            while not self.stopped:
                msg = await self.inner_read_message(*args, **kwargs)
                assert type(msg) == dict, 'Msg is: {!r}'.format(msg)
                ctx_headers = msg['context_headers']
                if self.remote:
                    parent_ctx = SpanContext(**ctx_headers)
                    method = msg['method']
                    ctx = Context(self.discovery, self.endpoint.service,
                                  parent_ctx, method)
                    self.log.debug('CTX: {}'.format(ctx.span.context.trace_id))
                    call_args = msg.get('args', ())
                    call_kwargs = msg.get('kwargs', {})
                    hc = self.handle_call(method, ctx, *call_args, **call_kwargs)
                    if isinstance(hc, CoroutineType):
                        resp = await hc
                    else:
                        resp = hc
                    self.log.debug('Remote send resp ==> {}'.format(resp))
                    response = {'context_headers': msg['context_headers'],
                                'method': msg['method'],
                                'response': resp}
                    await self.remote_send_response(msg, response)
                else:
                    self.log.debug('Proxy return resp: {}'.format(msg))
                    self.proxy_send_response(msg)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.log.exception('In read loop')
            self.terminate(e)

    async def remote_send_response(self, request, response):
        raise NotImplementedError

    async def inner_read_message(self, *args, **kwargs):
        raise NotImplementedError

    def proxy_send_response(self, msg):
        span_id = str(msg['context_headers']['span_id'])
        self.responses.pop(span_id).set_result(msg)

    async def rpc_inner_call(msg, future):
        """
        implemente here concrete interaction with pub/sub transport level
        """
        raise NotImplementedError

    async def on_start(self):
        raise NotImplementedError

    async def on_stop(self):
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

    async def perform_call(self, ctx, method_name, *args, **kwargs):
        if not self.transport.started:
            await self.transport.on_start()
        return await self.transport.rpc_call(method_name, ctx, *args, **kwargs)

    async def on_start(self):
        await self.transport.on_start()

    async def on_stop(self):
        await self.transport.on_stop()
