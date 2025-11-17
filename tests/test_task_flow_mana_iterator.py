import unittest
import os
import json
import tempfile
import time
import io
import sys
from queue import Queue

from gatling.utility.task_flow_manager import TaskFlowManager
from gatling.utility.io_fctns import save_jsonl


# ========= Stage functions (iterator pipeline) =========
def iterator_source(count: int, delay: float = 0.05):
    """Stage 1: Generator that yields synthetic JSON objects."""
    for i in range(count):
        print(f"[iterator_source] yielding item {i}")
        time.sleep(delay)  # simulate slow data generation
        yield {"id": i, "text": f"Sample text {i}"}


def process_item(entry: dict) -> dict:
    """Stage 2: Process each generated item."""
    text = entry.get("text", "")
    processed = {"id": entry["id"], "length": len(text)}
    print(f"[process_item] processed id={entry['id']} -> length={processed['length']}")
    return processed


def save_result(entries: list, output_file: str):
    """Stage 3: Save results to a JSONL file (append mode)."""
    # ✅ FIX: Use append mode so multiple calls don't overwrite the file
    save_jsonl(entries, output_file, mode='a')
    print(f"[save_result] wrote {len(entries)} entries to {output_file}")
    return output_file


# ========= Unit test =========
class TestTaskFlowIterator(unittest.TestCase):
    """End-to-end test for iterator-based TaskFlowManager pipeline."""

    def setUp(self):
        # Redirect stdout to capture all prints
        self._stdout = sys.stdout
        self._buffer = io.StringIO()
        sys.stdout = self._buffer

        # Temporary directory for JSONL output
        self.tempdir = tempfile.TemporaryDirectory()
        self.output_file = os.path.join(self.tempdir.name, "iterator_results.jsonl")

        # Initialize queues
        self.q_wait = Queue()
        self.q_done = Queue()

        # Build the taskflow manager
        self.tfm = TaskFlowManager(self.q_wait, self.q_done, retry_on_error=False)

        # Stage 1: iterator-based source (generator)
        def gen_wrapper():
            """Wrapper: feed iterator values into the queue."""
            for item in iterator_source(5):
                self.q_wait.put(item)
            print("[gen_wrapper] finished generating data")

        # Stage 2: process each item
        self.tfm.append_stagefctn(process_item, thread_worker=1)

        # Stage 3: save processed items
        def wrap_save(entry):
            """Wrapper to save a single entry to the output JSONL file."""
            return save_result([entry], self.output_file)

        self.tfm.append_stagefctn(wrap_save)

        # Start generator manually to populate input queue
        gen_wrapper()

    def tearDown(self):
        # Stop manager and restore stdout
        self.tfm.stop()
        sys.stdout = self._stdout
        self.tempdir.cleanup()

    # ----------------------------------------------------------------------
    def test_iterator_to_jsonl_flow(self):
        """Verify full iterator → processing → save pipeline executes successfully."""
        self.tfm.start()
        self.tfm.await_print(interval=0.5)
        time.sleep(0.5)
        self.tfm.stop()

        output_log = self._buffer.getvalue()
        print("Captured output:\n", output_log)

        # Verify pipeline stages printed expected messages
        self.assertIn("[iterator_source] yielding item 0", output_log)
        self.assertIn("[process_item] processed id=0", output_log)
        self.assertIn("[save_result]", output_log)

        # Validate the output JSONL file
        self.assertTrue(os.path.exists(self.output_file), "Output JSONL file missing")

        with open(self.output_file, "r", encoding="utf-8") as f:
            lines = [json.loads(line.strip()) for line in f if line.strip()]

        # ✅ Expect 5 total lines now (no overwriting)
        self.assertEqual(len(lines), 5, f"Expected 5 lines, got {len(lines)}")

        # Validate field structure and correctness
        for i, entry in enumerate(lines):
            self.assertIn("id", entry)
            self.assertIn("length", entry)
            self.assertEqual(entry["id"], i, f"ID mismatch for entry {i}")
            expected_len = len(f"Sample text {i}")
            self.assertEqual(entry["length"], expected_len, f"Length mismatch for entry {i}")

        print(f"\n✅ Successfully validated iterator pipeline output at {self.output_file}")
        print(f"Example entry: {lines[0]}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
