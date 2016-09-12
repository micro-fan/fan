from basictracer import BasicTracer
from fan.context import Context
from typing import List, Dict, Any


class Process:
    """
    Starts all applications
    establish connections to discovery, queues
    """
    service_groups = []  # type: List[Dict[str, Any]]

    def __init__(self, discovery):
        self.discovery = discovery
        self.instances = []

    def create_context(self, service=None):
        return Context(self.discovery, service)

    def start(self):
        for SG in self.service_groups:
            sg = SG(self.discovery)
            sg.start()
            self.instances.append(sg)

    def stop(self):
        for sg in self.instances:
            sg.stop()
