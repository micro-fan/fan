import requests

from fan.remote import Transport


class HTTPTransport(Transport):

    def __init__(self, discovery, endpoint, params):
        super().__init__(discovery, endpoint, params)

    def rpc_call(self, method, ctx, **kwargs):
        pass
