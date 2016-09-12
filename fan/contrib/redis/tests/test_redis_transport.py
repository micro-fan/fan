import os

from fan.contrib.redis import RedisEndpoint, RedisTransport
from fan.contrib.aio.tests import AIOEndpointCase


class RedisCase(AIOEndpointCase):
    endpoint_class = RedisEndpoint
    transport_class = RedisTransport
    endpoint_params = {'host': os.environ.get('REDIS_HOST', 'redis'),
                       'port': 6379,
                       'transport': 'redis',
                       'queue': 'test'}

    async def test_call(self):
        await self._test_remote_call()

    async def test_remote_register(self):
        await self._test_remote_register()
