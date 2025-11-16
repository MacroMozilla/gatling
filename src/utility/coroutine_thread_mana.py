import asyncio
import threading
import inspect
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional


class CoroutineThreadManager:
    def __init__(self, task_func: Callable, args: tuple = (), kwargs: Optional[dict] = None):
        if not isinstance(args, tuple):
            raise TypeError("args must be a tuple")
        if kwargs is not None and not isinstance(kwargs, dict):
            raise TypeError("kwargs must be a dict")

        self.task_func = task_func
        self.args = args
        self.kwargs = kwargs or {}
        self.stop_event: Optional[threading.Event] = None
        self.executor: Optional[ThreadPoolExecutor] = None

    def start(self, thread_worker: int = 1, coroutine_worker: int = 0):
        """
        :param thread_worker: how many OS threads
        :param coroutine_worker: how many asyncio tasks per thread (only for async task_func)
        """
        if self.stop_event is not None and not self.stop_event.is_set():
            raise RuntimeError("Manager is already running")

        is_async = inspect.iscoroutinefunction(self.task_func)
        self.stop_event = threading.Event()

        # Legality Check
        if is_async and coroutine_worker <= 0:
            coroutine_worker = 1
        if (is_async and coroutine_worker == 0) or (not is_async and coroutine_worker > 0):
            raise ValueError(
                f"[{self.task_func.__name__}] invalid mode: async={is_async}, async_worker={coroutine_worker}"
            )

        if is_async:
            print(f"[Async start] threads={thread_worker}, async_workers={coroutine_worker}")
        else:
            print(f"[Sync start] threads={thread_worker}")

        # 异步线程目标
        def thread_target_async():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def worker_loop(worker_id: int):
                while not self.stop_event.is_set():
                    try:
                        await asyncio.wait_for(
                            self.task_func(*self.args, **self.kwargs), timeout=None
                        )
                    except asyncio.TimeoutError:
                        pass
                    except Exception as e:
                        print(f"async worker-{worker_id} exception:", e)
                        import traceback
                        traceback.print_exc()
                    await asyncio.sleep(0.05)

            async def main():
                tasks = [
                    asyncio.create_task(worker_loop(i))
                    for i in range(coroutine_worker)
                ]
                await asyncio.gather(*tasks)

            try:
                loop.run_until_complete(main())
            finally:
                loop.close()

        # Sync Thread Goals
        def thread_target_sync():
            while not self.stop_event.is_set():
                try:
                    self.task_func(*self.args, **self.kwargs)
                except Exception as e:
                    print("sync task exception:", e)
                import time
                time.sleep(0.05)

        self.executor = ThreadPoolExecutor(max_workers=thread_worker)
        target = thread_target_async if is_async else thread_target_sync
        for _ in range(thread_worker):
            self.executor.submit(target)

    def stop(self, force=True):
        """Safely stop all threads"""
        if not self.stop_event:
            return
        self.stop_event.set()
        if self.executor:
            self.executor.shutdown(wait=not force)
            self.executor = None
        self.stop_event = None
        print(f"TCM({self.task_func.__name__}) stopped !!!")


if __name__ == "__main__":
    pass
    import time


    async def async_worker_task(name, delay=0.5):
        print(f"async task {name} running")
        await asyncio.sleep(delay)


    def sync_worker_task(name, delay=0.5):
        print(f"sync task {name} running")
        import time
        time.sleep(delay)


    print("=== test async ===")
    manager = CoroutineThreadManager(async_worker_task, args=("A",), kwargs={"delay": 0.3})
    manager.start(thread_worker=2, coroutine_worker=2)
    time.sleep(2)
    manager.stop()

    print("=== test sync ===")
    manager = CoroutineThreadManager(sync_worker_task, args=("B",), kwargs={"delay": 0.2})
    manager.start(thread_worker=2, coroutine_worker=0)
    time.sleep(2)
    manager.stop()

    print("=== test (should raise error) ===")
    try:
        manager = CoroutineThreadManager(sync_worker_task)
        manager.start(thread_worker=2, coroutine_worker=2)  # invalid combination
    except Exception as e:
        print("Error:", e)
