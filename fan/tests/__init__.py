from fan.service import Service, endpoint


TEST_TIMEOUT = 5


class DummyService(Service):
    name = 'dummy'

    @endpoint('echo')
    def method(self, context, message):
        self.log.debug('SELF: {} MSG: {}'.format(self, message))
        return message


class NestedService(DummyService):
    name = 'nested.tree.dummy'
