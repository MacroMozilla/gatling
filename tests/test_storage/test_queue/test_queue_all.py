import unittest
import threading
from queue import Empty, Full
from gatling.storage.queue.memory_queue import MemoryQueue


class TestMemoryQueue(unittest.TestCase):
    """Unit tests for MemoryQueue class."""

    def test_basic_put_get(self):
        """Test putting and getting items."""
        q = MemoryQueue()
        q.put("apple")
        q.put("banana")

        self.assertEqual(len(q), 2)
        self.assertEqual(q.get(), "apple")
        self.assertEqual(q.get(), "banana")
        self.assertTrue(q._queue.empty())

    def test_delete_clears_queue(self):
        """Test that delete() clears all items."""
        q = MemoryQueue()
        q.put("x")
        q.put("y")
        self.assertEqual(len(q), 2)

        q.clear()
        self.assertEqual(len(q), 0)
        self.assertTrue(q._queue.empty())

    def test_len_and_iter(self):
        """Test len() and iteration return expected values."""
        q = MemoryQueue()
        q.put("A")
        q.put("B")
        self.assertEqual(len(q), 2)

        items = list(q)
        self.assertEqual(items, ["A", "B"])

    def test_get_block_false_raises(self):
        """Test get() with block=False raises Empty if no item."""
        q = MemoryQueue()
        with self.assertRaises(Empty):
            q.get(block=False)

    def test_put_respects_maxsize(self):
        """Test put() respects maxsize limit."""
        q = MemoryQueue(maxsize=1)
        q.put("A")
        with self.assertRaises(Full):
            q.put("B", block=False)

    def test_multithreaded_put_get(self):
        """Test thread-safe concurrent put/get operations."""
        q = MemoryQueue()
        results = []

        def producer():
            for i in range(5):
                q.put(i)

        def consumer():
            for _ in range(5):
                results.append(q.get())

        t1 = threading.Thread(target=producer)
        t2 = threading.Thread(target=consumer)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        self.assertEqual(sorted(results), [0, 1, 2, 3, 4])
        self.assertTrue(q._queue.empty())

    def test_open_and_close_do_not_raise(self):
        """Test open() and close() can be called safely."""
        q = MemoryQueue()
        try:
            q.open()
            q.close()
        except Exception as e:
            self.fail(f"open/close raised unexpectedly: {e}")


if __name__ == "__main__":
    # TODO add more test
    unittest.main(verbosity=2)
