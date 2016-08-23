from fan.remote import ProxyEndpoint


class AIOProxyEndpoint(ProxyEndpoint):

    def __getattr__(self, name):
        if name in ('name', 'params', 'discovery', 'transport'):
            return object.__getattribute__(self, name)

        async def callable(ctx, *args, **kwargs):
            if not self.transport.started:
                await self.transport.on_start()
            ret = await self.transport.rpc_call(name, ctx, *args, **kwargs)
            print('RPC resp: {}'.format(ret))
            return ret
        return callable

    async def on_start(self):
        await self.transport.on_start()

    async def on_stop(self):
        await self.transport.on_stop()
