class RPCException(Exception):
    pass


class RPCHttpError(RPCException):
    def __init__(self, response):
        self.response = response
        super().__init__(self.response)

    def __str__(self):
        return '<RPCHttpError {} {}'.format(self.response.status_code,
                                            self.response.content.decode('utf8'))


class AioRPCHttpError(RPCException):
    def __init__(self, status, content):
        self.status = status
        self.content = content
        super().__init__(content)

    def __str__(self):
        return '<RPCHttpError {} {}'.format(self.status, self.content.decode('utf8'))


class DiscoveryConnectionError(Exception):
    pass
