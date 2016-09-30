import os

import yaml
from tipsi_tools.testing.aio import AIOTestCase
from aiozk import ZKClient

from fan.contrib.sync_helper import SyncHelper


CURR_DIR = os.path.dirname(os.path.abspath(__file__))
CONF = yaml.load(open(os.path.join(CURR_DIR, 'conf.yaml')))


class TestSyncHelper(AIOTestCase):

    async def dump_tree(self, base='/'):
        out = list(await self.get_tree(base))
        print('Tree dump: {}'.format(out))
        return out

    @property
    def zk_path(self):
        return os.environ.get('ZK_HOST', 'zk')

    async def setUp(self):
        self.zk = ZKClient(self.zk_path)
        await self.zk.start()

    async def get_tree(self, curr='/'):
        out = [curr]
        childs = await self.zk.get_children(curr)
        for c in childs:
            # eliminate double slash: //root = '/'.join('/', 'root')
            if curr == '/':
                curr = ''
            out.append(await self.get_tree('/'.join([curr, c])))
        return out

    async def test_00_simple(self):
        conf = CONF
        s = SyncHelper(self.zk_path, '/', conf)
        await s.on_start()
