import asyncio

from fan.asynchronous import get_context


async def run(loop):
    async with await get_context(name='service_client', loop=loop) as ctx:
        # Normal request example
        resp = await ctx.rpc.service_one.ok()
        print(resp)

        # Error example
        resp = await ctx.rpc.service_one.error()
        print(resp)


loop = asyncio.get_event_loop()
task = loop.create_task(run(loop=loop))
loop.run_until_complete(task)
