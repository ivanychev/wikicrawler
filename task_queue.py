import asyncio
import logging

from typing import Iterable

from task import Task

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("Queue")

class TaskQueue:
    def __init__(self, queue: asyncio.Queue):
        self.queue = queue


    @classmethod
    def create(cls, tasks: Iterable[Task]):
        queue = asyncio.Queue()
        for task in tasks:
            queue.put_nowait(task)
        return TaskQueue(queue)

    async def get_task(self):
        return await self.queue.get()

    def task_done(self):
        return self.queue.task_done()

    async def add_task(self, task: Task):
        task = await self.queue.put(task)
        if self.queue.qsize() % 1000 == 0:
            logger.info("queue size: %s", self.queue.qsize())
        return task

    async def join(self):
        return await self.queue.join()