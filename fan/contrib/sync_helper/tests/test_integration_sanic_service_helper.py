import asyncio

import aiohttp
import pytest
from sanic import response

from fan.asynchronous import get_context
from fan.contrib.sync_helper.sanic_helpers import AbstractTaskWorker, SanicServiceHelper

# Global state
is_test_task_running = False


async def status(request):
    return response.json({'status': is_test_task_running})


async def ping(request):
    return response.json(['pong'])


class FunTestTask(AbstractTaskWorker):
    async def worker_loop(self):
        global is_test_task_running
        while not self.stopping:
            is_test_task_running = True
            await asyncio.sleep(self.idle_timeout)


@pytest.fixture
def global_status():
    global is_test_task_running
    is_test_task_running = False
    yield
    is_test_task_running = False


@pytest.fixture
def fan_test_service_port(unused_tcp_port_factory):
    yield unused_tcp_port_factory()


@pytest.fixture
def fan_test_service_url(fan_test_service_port):
    yield f'http://127.0.0.1:{fan_test_service_port}'


@pytest.fixture
def fan_test_service(fan_test_service_port, global_status):
    service = SanicServiceHelper('test_service', host='0.0.0.0', port=fan_test_service_port)
    service.add_endpoint(ping, name='ping', url='/ping/', method='GET')
    service.add_endpoint(status, name='status', url='/status/', method='GET')
    service.add_task(FunTestTask)
    yield service


@pytest.fixture
async def running_service(event_loop, fan_test_service):
    await fan_test_service.async_run(loop=event_loop)
    yield fan_test_service
    await fan_test_service.stop()


@pytest.fixture
async def fan_async_context(event_loop):
    async with await get_context(loop=event_loop) as ctx:
        yield ctx
        await ctx.stop()


@pytest.mark.asyncio
async def test_server_is_running(running_service, fan_test_service_url):
    global is_test_task_running
    assert is_test_task_running, 'Task is not running'

    conn = aiohttp.TCPConnector(verify_ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        async with session.get(f'{fan_test_service_url}/status/') as response:
            response_data = await response.json()
            assert response_data.get('status'), 'Api status: task is not running'


@pytest.mark.asyncio
async def test_rpc_async_call(event_loop, running_service, fan_test_service_url, fan_async_context):
    resp = await fan_async_context.rpc.test_service.status()
    assert resp.get('status'), 'Api status: task is not running'
