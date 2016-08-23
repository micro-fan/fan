import asyncio
import uuid
import aioredis

from fan.remote import RemoteEndpoint, Transport


class RedisTransport(Transport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.loop = asyncio.get_event_loop()
        self.remote = isinstance(self.endpoint, RemoteEndpoint)

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
            route = self.back_rk = str(uuid.uuid4())

        res = await self.sub.subscribe(route)
        self.loop.create_task(self.read_loop(res[0]))

    async def read_loop(self, chan):
        print('read loop call')
        while not self.stopped:
            print('wait message...')
            while (await chan.wait_message()):
                print('Got message')
                msg = await chan.get_json()
                print('MSG: {}'.format(msg))
                ctx = None
                method = msg['method']
                args = ()
                kwargs = {}
                if self.remote:
                    print('Call: {} {}'.format(method, ctx))
                    hc = self.handle_call(method, ctx, *args, **kwargs)
                    print('HC U: {}'.format(hc))
                    # resp = await hc
                    resp = hc
                    print('Send resp ==> : {}'.format(resp))
                    await self.pub.publish_json(msg['back_route'],
                                                {'context_headers': msg['context_headers'],
                                                 'method': msg['method'],
                                                 'response': resp})
                else:
                    print('Put into responses {}'.format(self.responses))
                    await self.responses.put(msg)

    async def rpc_call(self, name, ctx, *args, **kwargs):
        # span_id, trace_id, sampled, baggage, with_baggage_item
        c = ctx.span.context
        context_headers = {'span_id': c.span_id,
                           'trace_id': c.trace_id,
                           'sampled': c.sampled}

        print('CTX: {}'.format(context_headers))
        msg = {'context_headers': context_headers,
               'back_route': self.back_rk,
               'method': name,
               'args': args,
               'kwargs': kwargs}
        is_ok = await self.pub.publish_json(self.params['queue'], msg)
        assert is_ok in (1, 2), 'Not ok: {} => {}'.format(is_ok, self.endpoint)
        rep = await self.responses.get()
        print('get resp: {}'.format(rep))
        return rep['response']

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
