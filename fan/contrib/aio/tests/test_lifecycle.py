import asyncio

from tipsi_tools.testing.aio import AIOTestCase
from fan.contrib.aio.lifecycle import (AIOSupervisor, AIOLifecycle, AIOSpec, MaxRestarts,
                                       State)


class Worker:
    def __init__(self):
        self.lifecycle = AIOLifecycle(self, 'on_start', 'terminate')
        self.started = False
        self.terminated = False

    async def on_start(self):
        self.started = True

    async def on_stop(self):
        await self.lifecycle.terminate()

    async def terminate(self, reason):
        self.started = False
        self.terminated = True


class DamagedWorker(Worker):
    async def new_on_start(self):
        self.started = True

    async def on_start(self):
        DamagedWorker.on_start = DamagedWorker.new_on_start
        raise Exception('damaged worker')


class BrokenWorkerError(Exception):
    pass


class BrokenWorker(Worker):
    def on_start(self):
        raise BrokenWorkerError


class FanTestSupervisor(AIOSupervisor):

    def __init__(self):
        self.specs = [AIOSpec(Worker)]
        super().__init__(self.specs)

    @property
    def state(self):
        return self.lifecycle.state


class AIOLifecycleTest(AIOTestCase):

    async def get_sup(self):
        if hasattr(self, '_s'):
            return self._s
        self._s = FanTestSupervisor()
        await self._s.lifecycle.on_start()
        return self._s

    async def test_01_simple_start(self):
        s = await self.get_sup()
        self.assertEqual(s.state, State.RUNNING)
        self.assertEqual(len(s.instances), 1)
        await s.lifecycle.terminate()
        self.assertEqual(s.state, State.TERMINATED)

    async def test_02_simple_add(self):
        s = await self.get_sup()
        worker = list(s.instances)[0]
        self.assertEqual(worker.lifecycle.state, State.RUNNING)

    async def test_03_worker_restart(self):
        s = await self.get_sup()
        worker = list(s.instances)[0]
        self.assertEqual(worker.lifecycle.state, State.RUNNING)
        await worker.lifecycle.terminate()
        self.assertEqual(worker.lifecycle.state, State.TERMINATED)

        worker = list(s.instances)[0]
        self.assertEqual(worker.lifecycle.state, State.RUNNING)

    async def test_04_damaged_restart(self):
        s = await self.get_sup()
        await s.add_child(AIOSpec(DamagedWorker))
        workers = list(s.instances)
        self.assertEqual(len(workers), 2)
        worker2 = workers[1]
        self.assertEqual(s.state, State.RUNNING)
        self.assertEqual(worker2.lifecycle.state, State.RUNNING)

        await s.lifecycle.terminate()
        self.assertEqual(s.lifecycle.state, State.TERMINATED)
        self.assertEqual(len(s.instances), 0)
        self.assertEqual(workers[0].lifecycle.state, State.TERMINATED)
        self.assertEqual(workers[1].lifecycle.state, State.TERMINATED)

    async def test_05_add_broken_worker(self):
        s = await self.get_sup()
        await self.assertRaises(MaxRestarts, s.add_child, AIOSpec(BrokenWorker))
        self.assertEqual(s.lifecycle.state, State.TERMINATED)

    async def test_06_broken_supervisor(self):
        s = AIOSupervisor([AIOSpec(BrokenWorker)])
        await self.assertRaises(MaxRestarts, s.lifecycle.on_start)
        self.assertEqual(list(s.instances), [])
        self.assertEqual(s.lifecycle.state, State.TERMINATED)

    async def test_07_supervisor_of_supervisor(self):
        sup_spec = AIOSpec(AIOSupervisor, [AIOSpec(BrokenWorker)])
        s = AIOSupervisor([sup_spec])
        await self.assertRaises(MaxRestarts, s.lifecycle.on_start)
        self.assertEqual(s.lifecycle.state, State.TERMINATED)

    async def test_08_nested_add(self):
        sup_spec = AIOSpec(AIOSupervisor, [AIOSpec(DamagedWorker)])
        s = AIOSupervisor([sup_spec])
        await s.lifecycle.on_start()
        self.assertEqual(s.lifecycle.state, State.RUNNING)

        s2 = list(s.instances)[0]
        self.assertEqual(s2.lifecycle.state, State.RUNNING)

        await self.assertRaises(MaxRestarts, s2.add_child, AIOSpec(BrokenWorker))
        self.assertEqual(s2.lifecycle.state, State.TERMINATED)
        self.assertEqual(s.lifecycle.state, State.TERMINATED)


class LaggyWorker:
    def __init__(self):
        self.lifecycle = AIOLifecycle(self, 'on_start', 'terminate')
        self.started = False
        self.terminated = False

    async def on_start(self):
        self.started = True
        asyncio.ensure_future(self.on_stop())

    async def on_stop(self):
        await self.lifecycle.terminate()

    async def terminate(self, reason):
        self.started = False
        self.terminated = True


class Linked(AIOLifecycle):
    def __init__(self):
        self.lifecycle = AIOLifecycle(self, notify_method='notify')
        self.notified = False

    def notify(self, reason, obj):
        self.notified = True

class AIOOnlyTest(AIOTestCase):
    async def test_01_die_after_start(self):
        s = AIOSupervisor([AIOSpec(LaggyWorker)])
        await s.lifecycle.on_start()
        self.assertEqual(s.lifecycle.state, State.RUNNING)
        await asyncio.sleep(0.01)
        self.assertEqual(s.lifecycle.state, State.TERMINATED)

    async def test_02_restart(self):
        s = AIOSupervisor([AIOSpec(Worker)])
        await s.lifecycle.on_start()
        self.assertEqual(s.lifecycle.state, State.RUNNING)
        w = list(s.instances)[0]
        l = Linked()
        l.lifecycle.watch(w)
        self.assertEqual(l.notified, False)
        self.ensure_future(w.lifecycle.terminate())
        await asyncio.sleep(0.001)
        self.assertEqual(l.notified, True)
        self.assertEqual(w.lifecycle.state, State.TERMINATED)

        self.assertEqual(s.lifecycle.state, State.RUNNING)
