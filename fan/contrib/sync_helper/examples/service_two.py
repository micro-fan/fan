from sanic.response import json

from fan.contrib.sync_helper.sanic_helpers import SanicServiceHelper


async def ping(request):
    return json(['pong'])


service = SanicServiceHelper('service_two', host='127.0.0.1', port=31416)

service.add_endpoint(ping, name='ping', url='/ping/', method='GET')

service.run()
