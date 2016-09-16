from fan.lifecycle import (Terminated, MaxRestarts, State,
                           Spec, Lifecycle, Supervisor)


def async_terminate_on_error(fun):
    async def wrapped(self, *args, **kwargs):
        try:
            res = await fun(self, *args, **kwargs)
            return res
        except Exception as e:
            self.log.debug('Catch exception: {!r}'.format(e))
            try:
                if hasattr(self, 'lifecycle'):
                    await self.lifecycle.terminate(e)
                else:
                    await self.terminate(e)
            except Exception as term_e:
                self.log.exception('during terminate')
            raise e
    return wrapped


class AIOSpec(Spec):
    async def run(self, obj):
        await obj.lifecycle.on_start()
        return obj


class AIOLifecycle(Lifecycle):

    @async_terminate_on_error
    async def on_start(self):
        assert self.state == State.INITIALIZED, self.state
        self.state = State.STARTING
        if self.start_method:
            await getattr(self.obj, self.start_method)()
        self.state = State.RUNNING

    async def safe_call(self, fun, *args, **kwargs):
        try:
            return await fun(*args, **kwargs)
        except Exception as e:
            self.log.info('during call {} [{} {}]'.format(fun, args, kwargs))

    async def terminate(self, reason=Terminated):
        if self.state >= State.STOPPING:
            return
        self.state = State.STOPPING
        self.log.info('Terminate reason: {!r} {}'.format(reason, self.obj))
        for link in self.links:
            await self.safe_call(link.terminate, reason, self.obj)
        for watch in self.watches:
            await self.safe_call(watch.notify, reason, self.obj)
        self.state = State.STOPPED
        if self.terminate_method:
            try:
                self.state = State.TERMINATING
                await self.safe_call(getattr(self.obj, self.terminate_method), reason)
            except Exception as e:
                self.log.exception('during terminate: {}.{}'.format(self.obj,
                                                                    self.terminate_method))
        self.state = State.TERMINATED

    @async_terminate_on_error
    async def notify(self, reason, obj):
        await getattr(self.obj, self.notify_method)(reason, obj)


class AIOSupervisor(Supervisor):
    def __init__(self, specs=[]):
        super().__init__(specs)
        self.lifecycle = AIOLifecycle(self, 'on_start', 'terminate', 'notify')

    @async_terminate_on_error
    async def notify(self, reason, obj):
        if self.lifecycle.state > State.RUNNING:
            return
        try:
            spec = self.instances.pop(obj)
        except KeyError as e:
            self.log.exception('Get notify without from obj not in self.instances')
            raise e
        await self.start_child(spec)

    async def add_child(self, spec):
        assert self.lifecycle.state == State.RUNNING, \
          'Supervisor should be started before add child {}'.format(spec)
        self.specs.append(spec)
        await self.start_child(spec)

    @async_terminate_on_error
    async def start_child(self, spec):
        while True:
            try:
                child = spec.get_obj()
                await spec.run(child)
                self.lifecycle.watch(child)
                self.instances[child] = spec
                return child
            except MaxRestarts as e:
                raise e
            except Exception:
                self.log.info('Restart worker: {}'.format(spec))

    async def on_start(self):
        self.log.debug('Start supervisor')
        assert self.lifecycle.state == State.STARTING, "don't call on_start directly"
        for spec in self.specs:
            await self.start_child(spec)

    async def terminate(self, reason):
        assert self.lifecycle.state == State.TERMINATING, "don't call terminate directly"
        self.log.info('Supervisor terminate reason: {}'.format(reason))
        while True:
            try:
                obj, spec = self.instances.popitem()
                await obj.lifecycle.terminate(reason)
            except KeyError:
                break
