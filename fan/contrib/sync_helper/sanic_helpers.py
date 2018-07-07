# TODO: Move sanic service helper to separate module

import asyncio
import logging
import os
from typing import Type

from sanic import Sanic

from fan.asynchronous import get_discovery, AsyncContext
from fan.contrib.sync_helper import SyncHelper


class SanicRegister:
    log = logging.getLogger('EndpointRegister')

    def __init__(self):
        self.sync_helper = None

    def get_local_ip(self):
        if not os.path.exists('/.dockerenv'):
            return '127.0.0.1'
        with open('/etc/hosts') as f:
            lines = f.readlines()
        return lines[-1].strip().split()[0]

    def __init__(self, name, port, transport='http', version='1.0.0'):
        self.zk_config = os.environ.get('ZK_HOST', 'zk')
        self.zk_chroot = os.environ.get('ZK_CHROOT', '/')
        self.service = {
            'methods': [],
            'name': name,
            'host': self.get_local_ip(),
            'port': port,
            'transport': transport,
            'version': version,
        }
        self.methods = []

    def add(self, name, url, method='GET', content_type='application/json'):
        self.service['methods'].append({
            'name': name,
            'method': method,
            'url': url,
            'content_type': content_type,
        })

    async def register(self, app=None, loop=None):
        assert app or loop, 'Either app nor loop must be provided as keyword argument'
        conf = {
            'services': [self.service]
        }
        self.sync_helper = SyncHelper(self.zk_config, self.zk_chroot, conf)
        self.loop = app.loop if app else loop
        await self.sync_helper.on_start()

    async def stop(self):
        await self.sync_helper.on_stop()


class AbstractTaskWorker:
    def __init__(self, loop):
        self.idle_timeout = 1
        self.loop = loop
        self.stopping = False
        self.task = self.loop.create_task(self.worker_loop())

    async def worker_loop(self):
        while not self.stopping:
            await asyncio.sleep(self.idle_timeout, loop=self.loop)

    def stop(self):
        self.stopping = True
        if not self.task.done():
            self.task.cancel()

    @classmethod
    def task(cls, app=None, loop=None):
        assert app or loop, 'Either app nor loop must be defined'
        return cls(app.loop if app else loop)

    @classmethod
    async def deferred_task(cls, app):
        # This method is used with sanic_app.add_task to retrieve loop after app started.
        return cls(app.loop)


class SanicServiceHelper:
    DEFAULT_BAGGAGE_HEADERS = {
        'HTTP_INSTALLATION_ID': 'INSTALLATION_ID',
        'HTTP_SESSION_ID': 'SESSION_ID',
    }

    def __init__(self, name, host, port, **kwargs):
        self._host = host
        self._port = port
        self.sanic_server = None
        self.app = Sanic(name)
        self.fan_reg = SanicRegister(name, port=self._port)
        self.task_classes = []
        self.workers = []
        self.baggage_headers = kwargs.get('baggage_headers', self.DEFAULT_BAGGAGE_HEADERS)

    def add_endpoint(self, handler, name, url, method, **kwargs):  # TODO: url arguments
        self.app.add_route(handler, url, methods=[method])
        self.fan_reg.add(name=name, url=url, method=method, **kwargs)

    def add_task(self, task_class: Type[AbstractTaskWorker]):
        self.task_classes.append(task_class)

    def add_exception_handler(self, handler):
        self.app.error_handler.add(Exception, handler)

    def prepare_app(self, loop):
        def fan_exception_handler(request, exception):
            return _inner_handler(request, exception)

        def _inner_handler(request, exception):
            request['fan_exc_info'] = exception

            # Temporally remove fan_handler from handlers
            # TODO: Think about better way to override error handling
            new_handlers = []
            for exc, hdl in self.app.error_handler.handlers:
                if hdl != fan_exception_handler:
                    new_handlers.append((exc, hdl))
            self.app.error_handler.handlers[:] = new_handlers

            result = self.app.error_handler.response(request=request, exception=exception)

            self.add_exception_handler(fan_exception_handler)
            return result

        self.add_exception_handler(fan_exception_handler)

        @self.app.middleware('request')
        async def fan_sanic_on_request(request):
            if request.get('fan_ctx'):
                return

            discovery = await get_discovery(name=self.app.name, loop=loop)
            tracer = discovery.tracer

            span_context = tracer.extract('http', request.headers)
            name = '{} {}'.format(request.method, request.path)
            if span_context:
                 ctx = AsyncContext(discovery, None, span_context, name)
            else:
                # TODO: Extract same code for django and sanic
                baggage = {}
                for k, v in self.baggage_headers.items():
                    h = request.headers.get(k)
                    if h is not None:
                        baggage[v] = h
                ctx = AsyncContext(discovery, None, None, name, baggage)

            await ctx.__aenter__()
            request['fan_ctx'] = ctx

        @self.app.middleware('response')
        async def fan_sanic_on_response(request, response):
            ctx = request.get('fan_ctx')
            if ctx:
                exc_info = request.get('fan_exc_info')
                await ctx.__aexit__(exc_info and type(exc_info), exc_info and str(exc_info), None)

    def run(self, **kwargs):
        loop = kwargs.get('loop') or asyncio.get_event_loop()
        asyncio.ensure_future(self.async_run(loop=loop, **kwargs), loop=loop)
        loop.run_forever()

    async def async_run(self, loop=None, **kwargs):
        loop = loop or asyncio.get_event_loop()
        for task_class in self.task_classes:
            self.workers.append(task_class.task(loop=loop))
        server = self.app.create_server(host=self._host, port=self._port, **kwargs)
        self.prepare_app(loop=loop)
        self.sanic_server = asyncio.ensure_future(server, loop=loop)
        await self.fan_reg.register(loop=loop)

    async def stop(self):
        await self.fan_reg.stop()
        if not self.sanic_server.done():
            self.sanic_server.cancel()
        for worker in self.workers:
            worker.stop()
