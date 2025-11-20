import asyncio
import inspect
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Callable, Optional

from gatling.runtime.runtime_task_manager_base import RuntimeTaskManager


class RuntimeTaskManagerCoroutine(RuntimeTaskManager):
    """
        Run coroutine functions or generators in threads with stop control.
        """

    def __init__(self, fctn: Callable, args: tuple = (), kwargs: Optional[dict] = None, interval=0.001, logfctn=print):
        super().__init__(fctn, args, kwargs)
        self.interval = interval
        self.thread_stop_event: threading.Event = threading.Event()  # False
        self.thread_running_executor: Optional[ThreadPoolExecutor] = None
        self.thread_running_futures: list[Future] = []
        self.coroutine_tasks: list[asyncio.Task] = []
        self.logfctn = logfctn

    def __len__(self):
        return len(self.coroutine_tasks)

    def start(self, worker=1):
        if self.thread_running_executor is not None:
            raise RuntimeError(f"{str(self)} already started")

        is_async_fctn = inspect.iscoroutinefunction(self.fctn)
        is_async_iter = inspect.isasyncgenfunction(self.fctn)

        # -------------------- async coroutine --------------------
        def thread_fctn_async():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def worker_loop(cid: int):

                async def worker_task(cid):
                    try:
                        await self.fctn(*self.args, **self.kwargs)
                    except Exception:
                        self.logfctn(f"{self} {cid=} async fctn exception:")
                        self.logfctn(traceback.format_exc())

                if self.interval > 0:
                    while not self.thread_stop_event.is_set():
                        await worker_task(cid)
                        await asyncio.sleep(self.interval)
                else:
                    while not self.thread_stop_event.is_set():
                        await worker_task(cid)

            async def main():
                tasks = [asyncio.create_task(worker_loop(i)) for i in range(worker)]
                self.coroutine_tasks.extend(tasks)
                await asyncio.gather(*tasks, return_exceptions=True)

            try:
                loop.run_until_complete(main())
            except Exception:
                self.logfctn(f"{self} event loop exception:")
                self.logfctn(traceback.format_exc())
            finally:
                try:
                    pending = asyncio.all_tasks(loop)
                    for t in pending:
                        t.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    self.logfctn(traceback.format_exc())
                finally:
                    loop.close()

        # -------------------- async generator --------------------
        def thread_iter_async():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def worker_loop(cid: int):
                async def worker_task(cid):
                    agen = self.fctn(*self.args, **self.kwargs)
                    try:
                        async for val in agen:
                            if self.thread_stop_event.is_set():
                                break
                    except Exception:
                        self.logfctn(f"{str(self)} async iter {cid=} exception:")
                        self.logfctn(traceback.format_exc())

                if self.interval > 0:
                    while not self.thread_stop_event.is_set():
                        await worker_task(cid)
                        await asyncio.sleep(self.interval)
                else:
                    while not self.thread_stop_event.is_set():
                        await worker_task(cid)

            async def main():
                tasks = [asyncio.create_task(worker_loop(i)) for i in range(worker)]
                self.coroutine_tasks.extend(tasks)
                await asyncio.gather(*tasks, return_exceptions=True)

            try:
                loop.run_until_complete(main())
            except Exception:
                self.logfctn(f"{str(self)} generator exception:")
                self.logfctn(traceback.format_exc())
            finally:
                try:
                    pending = asyncio.all_tasks(loop)
                    for t in pending:
                        t.cancel()
                    if pending:
                        loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                except Exception:
                    self.logfctn(traceback.format_exc())
                finally:
                    loop.close()

        # ----------------------------------------------------------
        if is_async_fctn:
            thread_target = thread_fctn_async
        elif is_async_iter:
            thread_target = thread_iter_async
        else:
            raise RuntimeError("Invalid task type: must be async function or async generator")

        self.logfctn(f"{self} start triggered ...")

        # Launch one thread controlling all async workers
        self.thread_stop_event.clear()
        self.thread_running_executor = ThreadPoolExecutor(max_workers=1)
        rf = self.thread_running_executor.submit(thread_target)
        self.thread_running_futures = [rf]

        # Wait until coroutine workers exist

        local_interval = max(self.interval, 1e-9)
        for _ in range(int(1 / local_interval)):
            if len(self) >= worker:
                break
            time.sleep(local_interval)
        self.logfctn(f"{self} started >>>")
        return True

    # ------------------------------------------------------------------
    def stop(self):
        if self.thread_running_executor is None:
            return False
        if self.thread_stop_event.is_set():
            return False

        self.logfctn(f"{self} stop triggered ... ")
        self.thread_stop_event.set()

        for rf in self.thread_running_futures:
            try:
                rf.result()
            except Exception:
                self.logfctn(traceback.format_exc())

        self.thread_running_executor.shutdown(wait=True)
        self.thread_running_executor = None
        self.thread_running_futures.clear()
        self.thread_stop_event.clear()
        self.coroutine_tasks.clear()

        self.logfctn(f"{self} stopped !!!")
        return True


if __name__ == '__main__':
    pass

    from gatling.utility.sample_tasks import async_fake_fctn_io, wrap_async_iter

    task_async_fctn = async_fake_fctn_io

    task_async_iter = wrap_async_iter()(async_fake_fctn_io)

    sleep_time = 0.1

    print("\n=== Test: async task function ===")
    rtm = RuntimeTaskManagerCoroutine(task_async_fctn, args=(), kwargs={}, interval=0)
    rtm.start(worker=5)
    time.sleep(sleep_time)
    rtm.stop()

    print("\n=== Test: async generator function ===")
    rtm2 = RuntimeTaskManagerCoroutine(task_async_iter, args=(), kwargs={}, interval=0)
    rtm2.start(worker=5)
    time.sleep(sleep_time)
    rtm2.stop()

    print("\n=== Test: async generator function context manager ===")
    rtm3 = RuntimeTaskManagerCoroutine(task_async_fctn, args=(), kwargs={}, interval=0)
    with rtm3.execute(worker=5):
        time.sleep(sleep_time)

    print("\n=== Test: async generator iterator context manager ===")
    rtm4 = RuntimeTaskManagerCoroutine(task_async_iter, args=(), kwargs={}, interval=0)
    with rtm4.execute(worker=5):
        time.sleep(sleep_time)
