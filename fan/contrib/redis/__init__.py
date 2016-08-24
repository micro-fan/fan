import asyncio
import uuid
import aioredis
from types import CoroutineType

from basictracer.context import SpanContext

from fan.context import TracedContext as Context
from fan.remote import RemoteEndpoint
from fan.contrib.aio.remote import AIOTransport


class RedisTransport(AIOTransport):

    async def on_start(self):
        print('Start redis')
        super().on_start()
        params = self.params

        self.sub = await aioredis.create_redis((params.get('host', 'localhost'),
                                                params.get('port', 6379)),
                                               loop=self.loop)
        self.pub = await aioredis.create_redis((params.get('host', 'localhost'),
                                                params.get('port', 6379)),
                                               loop=self.loop)
        print('Subscribe...')
        if self.remote:
            route = params['queue']
        else:
            self.responses = asyncio.Queue()
            route = self.back_route = str(uuid.uuid4())

        res = await self.sub.subscribe(route)
        self.loop.create_task(self.read_loop(res[0]))

    async def _inner_call(self, msg):
        msg['back_route'] = self.back_route

        is_ok = await self.pub.publish_json(self.params['queue'], msg)
        assert is_ok in (1, 2), 'Not ok: {} => {}'.format(is_ok, self.endpoint)
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

    async def on_stop(self):
        if hasattr(self, 'sub'):
            await self.sub.close()
        if hasattr(self, 'pub'):
            await self.pub.close()


class RedisEndpoint(RemoteEndpoint):
    transportClass = RedisTransport

    async def on_start(self):
        await self.transport.on_start()

    async def on_stop(self):
        await self.transport.on_stop()
