from unittest import TestCase
from fan.lifecycle import Supervisor, Lifecycle, Spec, MaxRestarts, State


class Worker:
    def __init__(self):
        self.lifecycle = Lifecycle(self, 'on_start', 'terminate')
        self.started = False
        self.terminated = False

    def on_start(self):
        self.started = True

    def on_stop(self):
        self.lifecycle.terminate()

    def terminate(self, reason):
        self.started = False
        self.terminated = True


class DamagedWorker(Worker):
    def new_on_start(self):
        self.started = True

    def on_start(self):
        DamagedWorker.on_start = DamagedWorker.new_on_start
        raise Exception('damaged worker')


class BrokenWorkerError(Exception):
    pass


class BrokenWorker(Worker):
    def on_start(self):
        raise BrokenWorkerError


class FanTestSupervisor(Supervisor):

    def __init__(self):
        self.specs = [Spec(Worker)]
        super().__init__(self.specs)

    @property
    def state(self):
        return self.lifecycle.state


class LifecycleTest(TestCase):

    @property
    def s(self):
        if hasattr(self, '_s'):
            return self._s
        self._s = FanTestSupervisor()
        self._s.lifecycle.on_start()
        return self._s

    def test_01_simple_start(self):
        self.assertEqual(self.s.state, State.RUNNING)
        self.assertEqual(len(self.s.instances), 1)

    def test_02_simple_add(self):
        worker = list(self.s.instances)[0]
        self.assertEqual(worker.lifecycle.state, State.RUNNING)

    def test_03_worker_restart(self):
        worker = list(self.s.instances)[0]
        self.assertEqual(worker.lifecycle.state, State.RUNNING)
        worker.lifecycle.terminate()
        self.assertEqual(worker.lifecycle.state, State.TERMINATED)

        worker = list(self.s.instances)[0]
        self.assertEqual(worker.lifecycle.state, State.RUNNING)

    def test_04_damaged_restart(self):
        self.s.add_child(Spec(DamagedWorker))
        workers = list(self.s.instances)
        self.assertEqual(len(workers), 2)
        worker2 = workers[1]
        self.assertEqual(self.s.state, State.RUNNING)
        self.assertEqual(worker2.lifecycle.state, State.RUNNING)

        self.s.lifecycle.terminate()
        self.assertEqual(self.s.lifecycle.state, State.TERMINATED)
        self.assertEqual(len(self.s.instances), 0)
        self.assertEqual(workers[0].lifecycle.state, State.TERMINATED)
        self.assertEqual(workers[1].lifecycle.state, State.TERMINATED)

    def test_05_add_broken_worker(self):
        self.assertRaises(MaxRestarts, self.s.add_child, Spec(BrokenWorker))
        self.assertEqual(self.s.lifecycle.state, State.TERMINATED)

    def test_06_broken_supervisor(self):
        s = Supervisor([Spec(BrokenWorker)])
        self.assertRaises(MaxRestarts, s.lifecycle.on_start)
        self.assertEqual(list(s.instances), [])
        self.assertEqual(s.lifecycle.state, State.TERMINATED)

    def test_07_supervisor_of_supervisor(self):
        sup_spec = Spec(Supervisor, [Spec(BrokenWorker)])
        s = Supervisor([sup_spec])
        self.assertRaises(MaxRestarts, s.lifecycle.on_start)
        self.assertEqual(s.lifecycle.state, State.TERMINATED)

    def test_08_nested_add(self):
        sup_spec = Spec(Supervisor, [Spec(DamagedWorker)])
        s = Supervisor([sup_spec])
        s.lifecycle.on_start()
        self.assertEqual(s.lifecycle.state, State.RUNNING)

        s2 = list(s.instances)[0]
        self.assertEqual(s2.lifecycle.state, State.RUNNING)

        self.assertRaises(MaxRestarts, s2.add_child, Spec(BrokenWorker))
        self.assertEqual(s2.lifecycle.state, State.TERMINATED)
        self.assertEqual(s.lifecycle.state, State.TERMINATED)
