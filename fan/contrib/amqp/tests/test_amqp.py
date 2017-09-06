import os

from fan.contrib.amqp import AMQPEndpoint, AMQPTransport
from fan.contrib.aio.tests import AIOEndpointCase


class AMQPCase(AIOEndpointCase):
    endpoint_class = AMQPEndpoint
    transport_class = AMQPTransport
    endpoint_params = {'host': os.environ.get('AMQP_HOST', 'rabbitmq'),
                       'queue': 'dummy',
                       'exchange': 'tipsi',
                       'exchange_type': 'topic',
                       'routing_key': 'dummy',
                       'transport': 'amqp'}

    async def test_call(self):
        await self._test_remote_call()

    async def test_remote_register(self):
        await self._test_remote_register()
