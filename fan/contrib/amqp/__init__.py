import asyncio
import asynqp
import re
import time
from types import CoroutineType

from basictracer.context import SpanContext

from fan.context import Context
from fan.remote import RemoteEndpoint
from fan.contrib.aio.remote import AIOTransport, AIOQueueBasedTransport


asynqp.queue.VALID_QUEUE_NAME_RE = re.compile('')


class AMQPTransport(AIOQueueBasedTransport, AIOTransport):
    async def ensure_connection(self):
        if hasattr(self, 'conn'):
            return
        params = self.params
        while True:
            try:
                self.conn = await asynqp.connect(params.get('host', 'localhost'),
                                                 params.get('port', 5672),
                                                 username=params.get('user', 'guest'),
                                                 password=params.get('password', 'guest'))
                break
            except OSError:
                time.sleep(0.2)

    async def sub_prepare(self):
        params = self.params
        await self.ensure_connection()
        self.sub = await self.conn.open_channel()
        self.routing_key = params['routing_key']
        self.default_exchange = await self.sub.declare_exchange('', 'direct')
        self.exchange = await self.sub.declare_exchange(params.get('exchange', ''),
                                                        params.get('exchange_type', 'direct'))
        self.log.debug('Subscribe...')
        if self.remote:
            queue_name = params['queue']
            queue = await self.sub.declare_queue(queue_name)
            await queue.bind(self.exchange, self.routing_key)
        else:
            queue_name = 'amq.rabbitmq.reply-to'
            queue = await self.sub.declare_queue(queue_name, nowait=True)
        # self.loop.create_task(self.read_loop(queue))
        self.queue = queue
        await queue.consume(self.deliver, no_ack=not self.remote)

    async def pub_prepare(self):
        self.pub = await self.conn.open_channel()

    async def sub_stop(self):
        await self.sub.close()
        await self.conn.close()

    async def pub_stop(self):
        await self.pub.close()

    async def on_start(self):
        self.log.info('Start amqp')
        await super().on_start()

    async def rpc_inner_call(self, msg, resp):
        ctx = msg['context_headers']
        for k, v in ctx.items():
            ctx[k] = str(v)
        body = msg
        span_id = str(ctx['span_id'])
        amqp_msg = asynqp.Message(body,
                                  headers=ctx,
                                  reply_to='amq.rabbitmq.reply-to',
                                  correlation_id=span_id)
        self.log.debug('Publish message: {} {}'.format(self.exchange, self.routing_key))
        self.exchange.publish(amqp_msg, self.routing_key)
        rep = await resp
        return rep

    def deliver(self, msg):
        self.log.debug('DELIVERED: {}'.format(msg))
        self.loop.create_task(self.read_loop(msg))

    async def read_loop(self, raw_msg):
        try:
            self.log.debug('Got message: {} Q[{}]'.format(raw_msg, self.queue.name))
            msg = raw_msg.json()
            ctx_headers = msg['context_headers']
            if self.remote:
                parent_ctx = SpanContext(**ctx_headers)
                method = msg['method']
                ctx = Context(self.discovery, self.endpoint.service, parent_ctx, method)
                self.log.debug('CTX: {} {}'.format(ctx.span.context.trace_id, raw_msg.correlation_id))
                args = msg.get('args', ())
                kwargs = msg.get('kwargs', {})
                hc = self.handle_call(method, ctx, *args, **kwargs)
                if isinstance(hc, CoroutineType):
                    resp = await hc
                else:
                    resp = hc
                self.log.debug('Send resp ==> : {}'.format(resp))
                resp = asynqp.Message({'context_headers': msg['context_headers'],
                                       'method': msg['method'],
                                       'response': resp},
                                      correlation_id=raw_msg.correlation_id)
                self.default_exchange.publish(resp, raw_msg.reply_to, mandatory=False)
                raw_msg.ack()
            else:
                self.proxy_send_response(msg)
        except Exception as e:
            self.terminate(e)


class AMQPEndpoint(RemoteEndpoint):
    async def on_start(self):
        await self.transport.on_start()

    async def on_stop(self):
        await self.transport.on_stop()
