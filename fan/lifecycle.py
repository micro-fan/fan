import time
import weakref
import logging
from enum import IntEnum
from collections import OrderedDict


class LifecycleReason(Exception):
    pass


class SupervisorStop(LifecycleReason):
    pass


class Terminated(LifecycleReason):
    pass


class MaxRestarts(LifecycleReason):
    pass


class RestartType(IntEnum):
    one_for_one = 1
    rest_for_one = 2
    one_for_all = 3


class State(IntEnum):
    INITIALIZED = 0
    STARTING = 1
    RUNNING = 2
    STOPPING = 3
    STOPPED = 4
    TERMINATING = 5
    TERMINATED = 6


class RestartStrategy:
    def __init__(self, restart_type=RestartType.one_for_one, max_r=10, max_t=1):
        self.restart_type = RestartType
        self.max_r = max_r
        self.max_t = max_t


def terminate_on_error(fun):
    def wrapped(self, *args, **kwargs):
        try:
            res = fun(self, *args, **kwargs)
            return res
        except Exception as e:
            self.log.debug('Catch exception: {!r}'.format(e))
            try:
                if hasattr(self, 'lifecycle'):
                    self.lifecycle.terminate(e)
                else:
                    self.terminate(e)
            except Exception as term_e:
                self.log.exception('during terminate')
            raise e
    return wrapped


class Spec:
    def __init__(self, cls, *args, strategy=RestartStrategy(), **kwargs):
        self.cls = cls
        self.args = args
        self.strategy = strategy
        self.kwargs = kwargs
        self.last_create = 0
        self.restarts = 0

    def get_obj(self):
        curr = time.time()
        if curr - self.last_create < self.strategy.max_t:
            self.restarts += 1
            if self.restarts >= self.strategy.max_r:
                raise MaxRestarts
        else:
            self.restarts = 0
        self.last_create = curr
        return self.cls(*self.args, *self.kwargs)

    def run(self, obj):
        obj.lifecycle.on_start()
        return obj


class Lifecycle:
    '''
    We assume that you will set self.lifecycle = Lifecycle()
    '''
    log = logging.getLogger('Lifecycle')

    def __init__(self, obj, start_method='', terminate_method='', notify_method=''):
        self.obj = obj
        self.start_method = start_method
        self.terminate_method = terminate_method
        self.notify_method = notify_method
        self.links = weakref.WeakSet()
        self.watches = weakref.WeakSet()
        self.state = State.INITIALIZED

    @terminate_on_error
    def on_start(self):
        assert self.state == State.INITIALIZED, self.state
        self.state = State.STARTING
        if self.start_method:
            getattr(self.obj, self.start_method)()
        self.state = State.RUNNING

    def safe_call(self, fun, *args, **kwargs):
        try:
            return fun(*args, **kwargs)
        except Exception as e:
            self.log.info('during call {} [{} {}]'.format(fun, args, kwargs))

    def terminate(self, reason=Terminated):
        if self.state >= State.STOPPING:
            return
        self.state = State.STOPPING
        self.log.info('Terminate reason: {!r} {}'.format(reason, self.obj))
        for link in self.links:
            self.safe_call(link.terminate, reason, self.obj)
        for watch in self.watches:
            self.safe_call(watch.notify, reason, self.obj)
        self.state = State.STOPPED
        if self.terminate_method:
            try:
                self.state = State.TERMINATING
                self.safe_call(getattr(self.obj, self.terminate_method), reason)
            except Exception as e:
                self.log.exception('during terminate: {}.{}'.format(self.obj,
                                                                    self.terminate_method))
        self.state = State.TERMINATED

    @terminate_on_error
    def notify(self, reason, obj):
        getattr(self.obj, self.notify_method)(reason, obj)

    def link(self, obj):
        assert self.state <= State.RUNNING
        obj.lifecycle.links.add(self)

    def watch(self, obj):
        assert self.state <= State.RUNNING
        obj.lifecycle.watches.add(self)


class Supervisor:
    log = logging.getLogger('Supervisor')

    def __init__(self, specs=[]):
        assert isinstance(specs, list), 'incompatible specs: {!r}'.format(specs)
        self.specs = specs
        self.instances = OrderedDict()
        self.lifecycle = Lifecycle(self, 'on_start', 'terminate', 'notify')

    @terminate_on_error
    def notify(self, reason, obj):
        if self.lifecycle.state > State.RUNNING:
            return
        try:
            spec = self.instances.pop(obj)
        except KeyError as e:
            self.log.exception('Get notify without from obj not in self.instances')
            raise e
        self.start_child(spec)

    def add_child(self, spec):
        assert self.lifecycle.state == State.RUNNING, \
          'Supervisor should be started before add child {}'.format(spec)
        self.specs.append(spec)
        self.start_child(spec)

    @terminate_on_error
    def start_child(self, spec):
        while True:
            try:
                child = spec.get_obj()
                spec.run(child)
                self.lifecycle.watch(child)
                self.instances[child] = spec
                return child
            except MaxRestarts as e:
                raise e
            except Exception:
                self.log.info('Restart worker: {}'.format(spec))

    def on_start(self):
        self.log.debug('Start supervisor')
        assert self.lifecycle.state == State.STARTING, "don't call on_start directly"
        for spec in self.specs:
            self.start_child(spec)

    def terminate(self, reason):
        assert self.lifecycle.state == State.TERMINATING, "don't call terminate directly"
        self.log.info('Supervisor terminate reason: {}'.format(reason))
        while True:
            try:
                obj, spec = self.instances.popitem()
                obj.lifecycle.terminate(reason)
            except KeyError:
                break
