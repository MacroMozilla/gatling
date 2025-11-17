import asyncio
import threading
import inspect
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional
import time
import traceback


class CoroutineThreadManager:
    """
    A flexible thread manager that can handle:
      - normal synchronous functions
      - asynchronous (coroutine) functions
      - synchronous generators (iterators)
      - asynchronous generators

    Each mode runs safely in background threads with cooperative shutdown.
    """

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

    # -------------------------------------------------------------------------
    def start(self, thread_worker: int = 1, coroutine_worker: int = 0):
        """
        Start the CoroutineThreadManager.

        :param thread_worker: Number of OS threads to run.
        :param coroutine_worker: Number of asyncio tasks per thread (for async task functions).
        """
        # Prevent starting if already running
        if self.stop_event is not None and not self.stop_event.is_set():
            raise RuntimeError("Manager is already running")

        # === Detect function type ===
        is_async = inspect.iscoroutinefunction(self.task_func)
        is_gen = inspect.isgeneratorfunction(self.task_func)
        is_asyncgen = inspect.isasyncgenfunction(self.task_func)

        self.stop_event = threading.Event()

        # === Validate configuration ===
        if is_async and coroutine_worker <= 0:
            coroutine_worker = 1
        if (is_async and coroutine_worker == 0) or (not is_async and coroutine_worker > 0):
            raise ValueError(
                f"[{self.task_func.__name__}] invalid mode: async={is_async}, async_worker={coroutine_worker}"
            )

        # === Display start mode ===
        if is_async:
            print(f"[Async start] threads={thread_worker}, async_workers={coroutine_worker}")
        elif is_gen or is_asyncgen:
            print(f"[Generator start] threads={thread_worker}")
        else:
            print(f"[Sync start] threads={thread_worker}")

        # ------------------------------------------------------------------
        # === Async coroutine worker ===
        def thread_target_async():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def worker_loop(worker_id: int):
                while not self.stop_event.is_set():
                    try:
                        await self.task_func(*self.args, **self.kwargs)
                    except Exception as e:
                        print(f"async worker-{worker_id} exception:", e)
                        traceback.print_exc()
                    await asyncio.sleep(0.05)

            async def main():
                tasks = [asyncio.create_task(worker_loop(i)) for i in range(coroutine_worker)]
                await asyncio.gather(*tasks, return_exceptions=True)

            try:
                loop.run_until_complete(main())
            except asyncio.CancelledError:
                pass
            except Exception as e:
                print("loop exception:", e)
            finally:
                loop.close()

        # ------------------------------------------------------------------
        # === Sync function worker ===
        def thread_target_sync():
            while not self.stop_event.is_set():
                try:
                    self.task_func(*self.args, **self.kwargs)
                except Exception as e:
                    print("sync task exception:", e)
                time.sleep(0.05)

        # ------------------------------------------------------------------
        # === Sync generator (iterator) worker ===
        def thread_target_gen():
            try:
                gen = self.task_func(*self.args, **self.kwargs)
                for val in gen:
                    if self.stop_event and self.stop_event.is_set():
                        break
                    # Optional: handle yielded values here (log, queue, etc.)
                    # print(f"[gen] yielded: {val}")
                    time.sleep(0.05)
            except Exception as e:
                print("generator exception:", e)
                traceback.print_exc()

        # ------------------------------------------------------------------
        # === Async generator worker ===
        def thread_target_asyncgen():
            async def run_asyncgen():
                agen = self.task_func(*self.args, **self.kwargs)
                try:
                    async for val in agen:
                        if self.stop_event and self.stop_event.is_set():
                            break
                        # Optional: handle yielded values here (log, queue, etc.)
                        # print(f"[asyncgen] yielded: {val}")
                        await asyncio.sleep(0.05)
                except Exception as e:
                    print("async generator exception:", e)
                    traceback.print_exc()

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(run_asyncgen())
            finally:
                loop.close()

        # ------------------------------------------------------------------
        # === Select the proper worker target ===
        self.executor = ThreadPoolExecutor(max_workers=thread_worker)
        if is_asyncgen:
            target = thread_target_asyncgen
        elif is_gen:
            target = thread_target_gen
        else:
            target = thread_target_async if is_async else thread_target_sync

        # === Launch worker threads ===
        for _ in range(thread_worker):
            self.executor.submit(target)

    # -------------------------------------------------------------------------
    def stop(self, force=True):
        """Safely stop all threads and event loops."""
        if not self.stop_event:
            return

        # Signal all workers to stop
        self.stop_event.set()

        # Shutdown the thread pool
        if self.executor:
            self.executor.shutdown(wait=not force)
            self.executor = None

        print(f"TCM({self.task_func.__name__}) stopped !!!")

        # Clear the stop event only after all threads are finished
        self.stop_event = None


# -------------------------------------------------------------------------
if __name__ == "__main__":

    async def async_worker_task(name, delay=0.5):
        print(f"async task {name} running")
        await asyncio.sleep(delay)

    def sync_worker_task(name, delay=0.5):
        print(f"sync task {name} running")
        time.sleep(delay)

    def sync_generator_worker(name, delay=0.2, count=5):
        for i in range(count):
            print(f"sync generator {name} yielding {i}")
            time.sleep(delay)
            yield i

    async def async_generator_worker(name, delay=0.2, count=5):
        for i in range(count):
            print(f"async generator {name} yielding {i}")
            await asyncio.sleep(delay)
            yield i

    # ---------------------------------------------------------------------
    print("=== test async ===")
    manager = CoroutineThreadManager(async_worker_task, args=("A",), kwargs={"delay": 0.3})
    manager.start(thread_worker=2, coroutine_worker=2)
    time.sleep(2)
    manager.stop()

    # ---------------------------------------------------------------------
    print("=== test sync ===")
    manager = CoroutineThreadManager(sync_worker_task, args=("B",), kwargs={"delay": 0.2})
    manager.start(thread_worker=2, coroutine_worker=0)
    time.sleep(2)
    manager.stop()

    # ---------------------------------------------------------------------
    print("=== test sync generator ===")
    manager = CoroutineThreadManager(sync_generator_worker, args=("SyncGen",), kwargs={"delay": 0.2, "count": 3})
    manager.start(thread_worker=1, coroutine_worker=0)
    time.sleep(2)
    manager.stop()

    # ---------------------------------------------------------------------
    print("=== test async generator ===")
    manager = CoroutineThreadManager(async_generator_worker, args=("AsyncGen",), kwargs={"delay": 0.2, "count": 3})
    manager.start(thread_worker=1, coroutine_worker=0)
    time.sleep(2)
    manager.stop()

    # ---------------------------------------------------------------------
    print("=== test invalid combination ===")
    try:
        manager = CoroutineThreadManager(sync_worker_task)
        manager.start(thread_worker=2, coroutine_worker=2)
    except Exception as e:
        print("Error:", e)
