import unittest
import asyncio
import time

from src.gatling.utility.coroutine_thread_mana import CoroutineThreadManager


# === Worker functions for testing ===
async def async_worker_task(name, delay=0.5):
    """Simple async worker used for testing."""
    print(f"async task {name} running")
    await asyncio.sleep(delay)
    return name


def sync_worker_task(name, delay=0.5):
    """Simple sync worker used for testing."""
    print(f"sync task {name} running")
    time.sleep(delay)
    return name


class TestCoroutineThreadManager(unittest.IsolatedAsyncioTestCase):
    """Unit tests for AsyncThreadManager covering async/sync and invalid configurations."""

    async def test_async_mode(self):
        """Test AsyncThreadManager running with async workers."""
        manager = CoroutineThreadManager(
            async_worker_task,
            args=("A",),
            kwargs={"delay": 0.1},
        )

        # Start 2 threads and 2 async workers
        manager.start(thread_worker=2, coroutine_worker=2)
        await asyncio.sleep(0.5)  # Allow tasks to run briefly
        manager.stop()

        # If we reach this point, no exception was raised → success
        self.assertTrue(True, "Async manager ran successfully")

    def test_sync_mode(self):
        """Test AsyncThreadManager running with sync workers."""
        manager = CoroutineThreadManager(
            sync_worker_task,
            args=("B",),
            kwargs={"delay": 0.1},
        )

        # Start 2 thread workers and no async workers
        manager.start(thread_worker=2, coroutine_worker=0)
        time.sleep(0.5)  # Allow tasks to run briefly
        manager.stop()

        self.assertTrue(True, "Sync manager ran successfully")

    def test_invalid_combination(self):
        """Test invalid configuration (sync function with async workers)."""
        with self.assertRaises(Exception):
            manager = CoroutineThreadManager(sync_worker_task)
            manager.start(thread_worker=2, coroutine_worker=2)


if __name__ == "__main__":
    # Run tests with detailed output
    unittest.main(verbosity=2)
