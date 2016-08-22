import asyncio
from fan.process import Process


class AIOProcess(Process):
    async def run(self):
        pass

    def start(self):
        self.loop = asyncio.get_event_loop()
        self.init_process()
        self.loop.run_forever()
