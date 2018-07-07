from sanic.response import text, json

from fan.contrib.sync_helper.sanic_helpers import SanicServiceHelper


async def f_ok(request):
    resp = await request['fan_ctx'].rpc.service_two.ping()
    return json([f"OK 2: {resp}"])


async def f_error(request):
    resp = await request['fan_ctx'].rpc.service_two.ping()
    1 / 0
    return text(f'OK 1: {resp}')


service = SanicServiceHelper('fan_test', host='127.0.0.1', port=31415)

service.add_endpoint(f_ok, name='ok', url='/ok/', method='GET')
service.add_endpoint(f_error, name='error', url='/error/', method='GET')

service.run()
