import asyncio
import threading
import inspect
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional
import time
import traceback


class CoroutineThreadManager:
    """
    A general-purpose manager that can run:
      - Normal synchronous functions
      - Asynchronous coroutine functions
      - Synchronous generators (iterators)
      - Asynchronous generators

    Each type runs in its own proper mode (threaded or async),
    with cooperative shutdown.
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
        Start the manager and launch tasks.

        :param thread_worker: Number of OS threads to run.
        :param coroutine_worker: Number of asyncio tasks per thread (only for async functions).
        """
        if self.stop_event is not None and not self.stop_event.is_set():
            raise RuntimeError("Manager is already running")

        # === Detect the function type ===
        is_async = inspect.iscoroutinefunction(self.task_func)
        is_gen = inspect.isgeneratorfunction(self.task_func)
        is_asyncgen = inspect.isasyncgenfunction(self.task_func)

        self.stop_event = threading.Event()

        # === Validate config ===
        if is_async and coroutine_worker <= 0:
            coroutine_worker = 1
        if (is_async and coroutine_worker == 0) or (not is_async and coroutine_worker > 0):
            raise ValueError(f"[{self.task_func.__name__}] invalid mode: async={is_async}, async_worker={coroutine_worker}")

        # === Display mode info ===
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
                    except Exception:
                        print(f"async worker-{worker_id} exception:")
                        traceback.print_exc()
                    await asyncio.sleep(0.05)

            async def main():
                tasks = [asyncio.create_task(worker_loop(i)) for i in range(coroutine_worker)]
                await asyncio.gather(*tasks, return_exceptions=True)

            try:
                loop.run_until_complete(main())
            except asyncio.CancelledError:
                pass
            except Exception:
                print("loop exception:")
                traceback.print_exc()
            finally:
                loop.close()

        # ------------------------------------------------------------------
        # === Sync function worker ===
        def thread_target_sync():
            while not self.stop_event.is_set():
                try:
                    result = self.task_func(*self.args, **self.kwargs)
                    print(f"[sync result] => {result}")
                except Exception:
                    print("sync task exception:")
                    traceback.print_exc()
                time.sleep(0.05)

        # ------------------------------------------------------------------
        # === Sync generator (iterator) worker ===
        def thread_target_gen():
            try:
                gen = self.task_func(*self.args, **self.kwargs)
                if not hasattr(gen, "__next__"):
                    raise TypeError(f"{self.task_func.__name__} did not return an iterator")

                while not self.stop_event.is_set():
                    try:
                        val = next(gen)
                        print(f"[iterator yield] => {val}")
                    except StopIteration:
                        break
                    except Exception:
                        print("generator iteration exception:")
                        traceback.print_exc()
                        break
                    time.sleep(0.05)

            except Exception:
                print("generator init exception:")
                traceback.print_exc()

        # ------------------------------------------------------------------
        # === Async generator worker ===
        def thread_target_asyncgen():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            async def run_asyncgen():
                try:
                    agen = self.task_func(*self.args, **self.kwargs)
                    if not hasattr(agen, "__anext__"):
                        raise TypeError(f"{self.task_func.__name__} did not return an async iterator")

                    while not self.stop_event.is_set():
                        try:
                            val = await agen.__anext__()
                            print(f"[async iterator yield] => {val}")
                        except StopAsyncIteration:
                            break
                        except Exception:
                            print("async generator iteration exception:")
                            traceback.print_exc()
                            break
                        await asyncio.sleep(0.05)

                except Exception:
                    print("async generator init exception:")
                    traceback.print_exc()

            try:
                loop.run_until_complete(run_asyncgen())
            finally:
                loop.close()

        # ------------------------------------------------------------------
        # === Choose the right thread target ===
        self.executor = ThreadPoolExecutor(max_workers=thread_worker)
        if is_asyncgen:
            target = thread_target_asyncgen
        elif is_gen:
            target = thread_target_gen
        else:
            target = thread_target_async if is_async else thread_target_sync

        # === Launch threads ===
        for _ in range(thread_worker):
            self.executor.submit(target)

    # -------------------------------------------------------------------------
    def stop(self, force=True):
        """Safely stop all threads and event loops."""
        if not self.stop_event:
            return

        self.stop_event.set()

        if self.executor:
            self.executor.shutdown(wait=not force)
            self.executor = None

        print(f"CTM({self.task_func.__name__}) stopped !!!")
        self.stop_event = None


# -------------------------------------------------------------------------
if __name__ == "__main__":
    async def async_worker_task(name, delay=0.3):
        print(f"async task {name} running")
        await asyncio.sleep(delay)
        print(f"**2 (async result for {name})")
        return f"async-{name}-done"

    def sync_worker_task(name, delay=0.3):
        print(f"sync task {name} running")
        time.sleep(delay)
        result = "**2 (sync result)"
        print(result)
        return result

    def sync_generator_worker(name, delay=0.2, count=3):
        for i in range(count):
            val = f"**{i+2} (iterator {name})"
            print(f"sync generator yielding: {val}")
            time.sleep(delay)
            yield val

    async def async_generator_worker(name, delay=0.2, count=3):
        for i in range(count):
            val = f"**{i+2} (async iterator {name})"
            print(f"async generator yielding: {val}")
            await asyncio.sleep(delay)
            yield val

    # ---------------------------------------------------------------------
    print("\n=== Test: Async Function ===")
    m = CoroutineThreadManager(async_worker_task, args=("A",))
    m.start(thread_worker=1, coroutine_worker=2)
    time.sleep(2)
    m.stop()

    print("\n=== Test: Sync Function ===")
    m = CoroutineThreadManager(sync_worker_task, args=("B",))
    m.start(thread_worker=2)
    time.sleep(2)
    m.stop()

    print("\n=== Test: Sync Generator ===")
    m = CoroutineThreadManager(sync_generator_worker, args=("SyncGen",))
    m.start(thread_worker=1)
    time.sleep(2)
    m.stop()

    print("\n=== Test: Async Generator ===")
    m = CoroutineThreadManager(async_generator_worker, args=("AsyncGen",))
    m.start(thread_worker=1)
    time.sleep(2)
    m.stop()

    print("\n=== Test: Invalid Combination ===")
    try:
        m = CoroutineThreadManager(sync_worker_task)
        m.start(thread_worker=2, coroutine_worker=2)
    except Exception as e:
        print("Error:", e)
