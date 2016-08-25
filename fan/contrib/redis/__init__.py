import asyncio
import uuid
import aioredis
from types import CoroutineType

from basictracer.context import SpanContext

from fan.context import TracedContext as Context
from fan.remote import RemoteEndpoint
from fan.contrib.aio.remote import AIOTransport, AIOQueueBasedTransport


class RedisTransport(AIOQueueBasedTransport, AIOTransport):

    def new_connection(self):
        params = self.params
        return aioredis.create_redis((params.get('host', 'localhost'),
                                      params.get('port', 6379)),
                                     loop=self.loop)

    async def sub_prepare(self):
        self.sub = await self.new_connection()
        print('Subscribe...')
        if self.remote:
            route = self.params['queue']
        else:
            self.responses = asyncio.Queue()
            route = self.back_route = str(uuid.uuid4())

        res = await self.sub.subscribe(route)
        self.loop.create_task(self.read_loop(res[0]))

    async def pub_prepare(self):
        self.pub = await self.new_connection()

    async def sub_stop(self):
        await self.sub.close()

    async def pub_stop(self):
        await self.pub.close()

    async def on_start(self):
        print('Start redis')
        await super().on_start()

    async def rpc_inner_call(self, msg):
        msg['back_route'] = self.back_route
        is_ok = await self.pub.publish_json(self.params['queue'], msg)
        # TODO: not clear what this code actually mean
        assert is_ok in (1, 2, 3), 'Not ok: {} => {}'.format(is_ok, self.endpoint)
        rep = await self.responses.get()
        return rep['response']

    async def read_loop(self, chan):
        while not self.stopped:
            while (await chan.wait_message()):
                msg = await chan.get_json()
                ctx_headers = msg['context_headers']
                parent_ctx = SpanContext(**ctx_headers)
                method = msg['method']
                ctx = Context(self.discovery, parent_ctx, method)
                print('CTX: {}'.format(ctx.span.context.trace_id))
                args = msg.get('args', ())
                kwargs = msg.get('kwargs', {})
                if self.remote:
                    hc = self.handle_call(method, ctx, *args, **kwargs)
                    if isinstance(hc, CoroutineType):
                        resp = await hc
                    else:
                        resp = hc
                    print('Send resp ==> : {}'.format(resp))
                    await self.pub.publish_json(msg['back_route'],
                                                {'context_headers': msg['context_headers'],
                                                 'method': msg['method'],
                                                 'response': resp})
                else:
                    print('Put into responses {}'.format(self.responses))
                    await self.responses.put(msg)


class RedisEndpoint(RemoteEndpoint):
    transportClass = RedisTransport

    async def on_start(self):
        await self.transport.on_start()

    async def on_stop(self):
        await self.transport.on_stop()
