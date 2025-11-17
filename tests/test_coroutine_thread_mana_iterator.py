import unittest
import asyncio
import time
import io
import sys

from gatling.utility.coroutine_thread_mana import CoroutineThreadManager


# === Generator worker functions for testing ===
def sync_generator_worker(name, delay=0.05, count=3):
    """Simple sync generator worker used for testing."""
    for i in range(count):
        print(f"sync generator {name} yielding {i}")
        time.sleep(delay)
        yield f"{name}-{i}"


async def async_generator_worker(name, delay=0.05, count=3):
    """Simple async generator worker used for testing."""
    for i in range(count):
        print(f"async generator {name} yielding {i}")
        await asyncio.sleep(delay)
        yield f"{name}-{i}"


class TestCoroutineThreadManagerGenerator(unittest.IsolatedAsyncioTestCase):
    """Unit tests for CoroutineThreadManager with generator and async-generator tasks."""

    def setUp(self):
        # Redirect stdout to capture printed output
        self._stdout = sys.stdout
        self._buffer = io.StringIO()
        sys.stdout = self._buffer

    def tearDown(self):
        # Restore original stdout
        sys.stdout = self._stdout

    def get_output(self):
        """Helper to retrieve current captured stdout."""
        return self._buffer.getvalue()

    # ------------------------------------------------------------------
    def test_sync_generator_mode(self):
        """Test CoroutineThreadManager running with sync generator and verify outputs."""
        manager = CoroutineThreadManager(
            sync_generator_worker,
            args=("SyncGen",),
            kwargs={"delay": 0.05, "count": 3},
        )

        manager.start(thread_worker=1, coroutine_worker=0)
        time.sleep(0.5)  # Allow it to yield a few values
        manager.stop()

        output = self.get_output()
        print("Captured output:\n", output)

        # Verify generator yielded expected items
        for i in range(3):
            expected_text = f"sync generator SyncGen yielding {i}"
            self.assertIn(expected_text, output, f"Expected '{expected_text}' in output")

    # ------------------------------------------------------------------
    async def test_async_generator_mode(self):
        """Test CoroutineThreadManager running with async generator and verify outputs."""
        manager = CoroutineThreadManager(
            async_generator_worker,
            args=("AsyncGen",),
            kwargs={"delay": 0.05, "count": 3},
        )

        manager.start(thread_worker=1, coroutine_worker=0)
        await asyncio.sleep(0.4)
        manager.stop()

        output = self.get_output()
        print("Captured output:\n", output)

        # Verify async generator yielded expected items
        for i in range(3):
            expected_text = f"async generator AsyncGen yielding {i}"
            self.assertIn(expected_text, output, f"Expected '{expected_text}' in output")

    # ------------------------------------------------------------------
    def test_invalid_combination_still_rejected(self):
        """Ensure invalid configuration still raises errors."""
        with self.assertRaises(Exception):
            manager = CoroutineThreadManager(sync_generator_worker)
            manager.start(thread_worker=1, coroutine_worker=1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
