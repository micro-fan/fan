import asyncio
import uuid
import aioredis

from fan.remote import RemoteEndpoint
from fan.contrib.aio.remote import AIOTransport, AIOQueueBasedTransport


class RedisStop(Exception):
    pass


class RedisTransport(AIOQueueBasedTransport, AIOTransport):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        import logging
        self.log = logging.getLogger(str(self))

    def new_connection(self):
        params = self.params
        return aioredis.create_redis((params.get('host', 'localhost'),
                                      params.get('port', 6379)),
                                     loop=self.loop)

    async def sub_prepare(self):
        self.sub = await self.new_connection()
        self.log.debug('Subscribe...')
        if self.remote:
            route = self.params['queue']
        else:
            route = self.back_route = str(uuid.uuid4())

        res = await self.sub.subscribe(route)
        self._read = asyncio.ensure_future(self.read_loop(res[0]))

    async def pub_prepare(self):
        self.pub = await self.new_connection()

    async def sub_stop(self):
        self.sub.close()

    async def pub_stop(self):
        self._read.cancel()
        self.pub.close()
        self.terminate(RedisStop())

    async def on_start(self):
        self.log.debug('Start redis')
        await super().on_start()

    async def rpc_inner_call(self, msg, resp):
        msg['back_route'] = self.back_route
        is_ok = await self.pub.publish_json(self.params['queue'], msg)
        # TODO: not clear what this code actually mean
        assert is_ok in (1, 2, 3), 'Not ok: {} => {}'.format(is_ok, self.endpoint)
        rep = await resp
        return rep['response']

    async def inner_read_message(self, chan):
        await chan.wait_message()
        return await chan.get_json()

    async def remote_send_response(self, msg, response):
        await self.pub.publish_json(msg['back_route'],
                                    {'context_headers': msg['context_headers'],
                                     'method': msg['method'],
                                     'response': response})


class RedisEndpoint(RemoteEndpoint):

    async def on_start(self):
        await self.transport.on_start()

    async def on_stop(self):
        await self.transport.on_stop()
