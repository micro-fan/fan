import logging
from functools import wraps
from types import FunctionType
from typing import List, Set, Any  # noqa


class ServiceGroup:
    '''
    define services
    define routes
    encapsulate discovery and config
    start and stop services
    '''
    services = []  # type: List[Any]

    def __init__(self, discovery):
        self.log = logging.getLogger(self.__class__.__name__)
        self.discovery = discovery
        self.instances = []

    def start(self):
        for service_class in self.services:
            service = service_class()
            service.on_start()
            self.instances.append(service)

    def stop(self):
        for service in self.instances:
            service.on_stop()

    def get_connections(self) -> Set[str]:
        connections = set()
        for srv_dict in self.services:
            for e_dict in srv_dict.get('endpoints', []):
                assert 'connection' in e_dict, '"connection" key must be defined in endpoint config'  # noqa
                connections.add(e_dict['connection'])
        return connections


class ServiceMeta(type):
    log = logging.getLogger('ServiceMeta')

    def run_asserts(name, attrs):
        if name == 'Service':
            return
        assert attrs['name'], 'No service name for {} given'.format(name)

    def process_rpc(svc, bases, attrs):
        if svc.__name__ == 'Service':
            return

        r = svc._rpc = {}

        for k, v in attrs.items():
            if hasattr(v, 'rpc'):
                fun = getattr(svc, v.__name__)
                r[v.rpc['name']] = fun.__name__

        for base in bases:
            if hasattr(base, '_rpc'):
                for k, v in base._rpc.items():
                    if k not in r:
                        r[k] = v
        r['_meta'] = {'name': tuple(svc.name.split('.'))}

    def __new__(cls, name, bases, attrs, **kwargs):
        cls.log.debug('cls: {} n: {} b: {} A: {}'.format(cls, name, bases, attrs, **kwargs))
        cls.run_asserts(name, attrs)
        obj = super().__new__(cls, name, bases, attrs, **kwargs)
        cls.process_rpc(obj, bases, attrs)
        return obj


class Service(metaclass=ServiceMeta):
    name = None  # type: str
    version = None  # type: tuple

    def __init__(self):
        self.log = logging.getLogger(self.name)

    def on_start(self):
        pass

    def on_stop(self):
        pass


def endpoint(name):
    """
    you can just wrap function with @endpoint and it set current function name as name
    or you can pass name - @endpoint('custom_name')
    """
    if type(name) == FunctionType:
        name_string = name.__name__
        return endpoint(name_string)(name)

    def set_rpc(func):
        func.rpc = {
            'name': name or func.__name__
        }

    def wrapped(func):
        set_rpc(func)
        @wraps(func)
        def ret(svc, *args, **kwargs):
            assert isinstance(svc, Service), '{} is not subclass of Service'.format(svc)
            return func(svc, *args, **kwargs)
        return ret
    return wrapped
