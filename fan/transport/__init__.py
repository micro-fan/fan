import requests

from fan.remote import Transport


class HTTPTransport(Transport):

    def __init__(self, discovery, endpoint, params):
        super().__init__(discovery, endpoint, params)
        self.base_url = '{transport}://{host}:{port}'.format(**params)
        self.methods = {}
        for method in params['methods']:
            self.methods[method['name']] = method

    def rpc_call(self, method_name, ctx, **kwargs):
        method = self.methods[method_name]
        url = ''.join([self.base_url, method['url']])
        if '{' in url:
            url = url.format(**kwargs)
        m = method.get('method', 'get').lower()
        req = getattr(requests, m)
        if m in ('get', 'delete'):
            kw = {'params': kwargs}
        else:
            kw = {'json': kwargs}
        resp = req(url, **kw)
        if resp.status_code in (200, 201):
            ret = resp.json()
        elif resp.status_code in (204,):
            ret = True
        return ret
