import asyncio
from tornado.platform.asyncio import AsyncIOMainLoop, to_asyncio_future

import os
from unittest import TestCase

from fan.contrib.aio.discovery import ZKDiscovery


class TestZK(TestCase):
    def setUp(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        AsyncIOMainLoop().install()

        self.remote = ZKDiscovery(os.environ.get('ZK_HOST', 'zk:2181'))

    async def _init_inner(self):
        await self.remote.on_start()
        self.ok = True

    def test_init(self):
        self.loop.run_until_complete(self._init_inner())
        assert self.ok
        pass
