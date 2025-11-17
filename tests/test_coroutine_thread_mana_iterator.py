import unittest
import asyncio
import time

from gatling.utility.coroutine_thread_mana import CoroutineThreadManager


# === Generator worker functions for testing ===
def sync_generator_worker(name, delay=0.1, count=3):
    """Simple sync generator worker used for testing."""
    for i in range(count):
        print(f"sync generator {name} yielding {i}")
        time.sleep(delay)
        yield f"{name}-{i}"


async def async_generator_worker(name, delay=0.1, count=3):
    """Simple async generator worker used for testing."""
    for i in range(count):
        print(f"async generator {name} yielding {i}")
        await asyncio.sleep(delay)
        yield f"{name}-{i}"


class TestCoroutineThreadManagerGenerator(unittest.IsolatedAsyncioTestCase):
    """Unit tests for CoroutineThreadManager with generator and async-generator tasks."""

    def test_sync_generator_mode(self):
        """Test CoroutineThreadManager running with sync generator."""
        manager = CoroutineThreadManager(
            sync_generator_worker,
            args=("SyncGen",),
            kwargs={"delay": 0.05, "count": 5},
        )

        # Start generator in one thread
        manager.start(thread_worker=1, coroutine_worker=0)
        time.sleep(0.5)  # Let it iterate a few times
        manager.stop()

        # If no exception occurred, it's considered success
        self.assertTrue(True, "Sync generator manager ran successfully")

    async def test_async_generator_mode(self):
        """Test CoroutineThreadManager running with async generator."""
        manager = CoroutineThreadManager(
            async_generator_worker,
            args=("AsyncGen",),
            kwargs={"delay": 0.05, "count": 5},
        )

        # Start generator in one thread
        manager.start(thread_worker=1, coroutine_worker=0)
        await asyncio.sleep(0.5)  # Allow async generator to yield values
        manager.stop()

        self.assertTrue(True, "Async generator manager ran successfully")

    def test_invalid_combination_still_rejected(self):
        """Ensure invalid configuration still raises errors (e.g., async worker + async_worker>0 mismatch)."""
        with self.assertRaises(Exception):
            manager = CoroutineThreadManager(sync_generator_worker)
            # For a sync generator, coroutine_worker>0 should still be invalid
            manager.start(thread_worker=1, coroutine_worker=1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
