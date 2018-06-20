import asyncio

import aiohttp
import pytest
from sanic import response

from fan.asynchronous import get_context
from fan.contrib.sync_helper.sanic_helpers import AbstractTaskWorker, SanicServiceHelper


async def ping(request):
    return response.json(['pong'])


@pytest.fixture
def ft_task_is_running():
    yield asyncio.Future()


@pytest.fixture
def fun_test_task(ft_task_is_running):
    class FunTestTask(AbstractTaskWorker):
        async def worker_loop(self):
            while not self.stopping:
                await asyncio.sleep(0.1)
                ft_task_is_running.set_result(True)
                await asyncio.sleep(self.idle_timeout)

    return FunTestTask


@pytest.fixture
def fan_test_service_port(unused_tcp_port_factory):
    yield unused_tcp_port_factory()


@pytest.fixture
def fan_test_service_url(fan_test_service_port):
    yield f'http://127.0.0.1:{fan_test_service_port}'


@pytest.fixture
def fan_test_service(fan_test_service_port, fun_test_task):
    service = SanicServiceHelper('test_service', host='0.0.0.0', port=fan_test_service_port)
    service.add_endpoint(ping, name='ping', url='/ping/', method='GET')
    service.add_task(fun_test_task)
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
async def test_server_is_running(running_service, fan_test_service_url, ft_task_is_running):
    await asyncio.wait_for(ft_task_is_running, timeout=1)
    assert ft_task_is_running.result(), 'Task is not running'


@pytest.mark.asyncio
async def test_server_http_ok(running_service, fan_test_service_url, ft_task_is_running):
    conn = aiohttp.TCPConnector()
    async with aiohttp.ClientSession(connector=conn) as session:
        async with session.get(f'{fan_test_service_url}/ping/') as response:
            response_data = await response.json()
            assert 'pong' in response_data, response_data


@pytest.mark.asyncio
async def test_get_async_context_two_times(event_loop):
    async with await get_context(loop=event_loop) as ctx:
        await ctx.stop()

    async with await get_context(loop=event_loop) as ctx:
        await ctx.stop()


@pytest.mark.asyncio
async def test_rpc_async_call(event_loop, running_service, fan_test_service_url, fan_async_context):
    resp = await fan_async_context.rpc.test_service.ping()
    assert 'pong' in resp, resp


@pytest.mark.asyncio
async def test_call_async_ctx_second_type(event_loop, running_service, fan_test_service_url,
                                          fan_async_context):
    # Test that different event_loop is cached in cache discovery
    resp = await fan_async_context.rpc.test_service.ping()
    assert 'pong' in resp, resp
