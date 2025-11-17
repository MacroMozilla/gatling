import unittest
import os
import json
import tempfile
from queue import Queue

from src.utility.io_fctns import save_jsonl
from src.utility.async_fetch_fctns import async_fetch_http, fwrap
from src.utility.task_flow_manager import TaskFlowManager  # Make sure this class is available


# ========= Stage functions =========
async def fetch_json_data(x: int) -> dict:
    """Stage 1: Fetch JSON data from the web using aiohttp."""
    url = f"https://jsonplaceholder.typicode.com/todos/{x + 1}"  # Public stable JSON API
    result, status, size = await fwrap(
        async_fetch_http,
        target_url=url,
        rtype="json",
        timeout=10.0,
        logfctn=print,
    )
    return {"id": x, "status": status, "size": size, "data": result}


def process_data(entry: dict) -> dict:
    """Stage 2: Process the fetched data."""
    data = entry.get("data", {}) or {}
    title = data.get("title", "")
    return {"id": entry["id"], "title_len": len(title), "status": entry["status"]}


def save_jsonl_result(entries: list, output_file: str):
    """Stage 3: Save results as JSONL file."""
    save_jsonl(entries, output_file)
    return output_file


# ========= Unit test =========
class TestTaskFlowHTTP(unittest.TestCase):
    """Full end-to-end test: async HTTP → data processing → save JSONL."""

    def setUp(self):
        # Create a temporary directory to store the output
        self.tempdir = tempfile.TemporaryDirectory()
        self.output_file = os.path.join(self.tempdir.name, "results.jsonl")

        # Create the task queues
        self.q_wait = Queue()
        self.q_done = Queue()

        # Initialize TaskFlowManager
        self.tfm = TaskFlowManager(self.q_wait, self.q_done, retry_on_error=False)

        # Register three processing stages
        self.tfm.append_stagefctn(fetch_json_data, coroutine_worker=2)  # async HTTP fetch
        self.tfm.append_stagefctn(process_data, thread_worker=1)  # CPU-bound sync task

        def wrap_save_jsonl_result(data):
            """Wrapper for saving a single data entry to the shared JSONL output file."""
            return save_jsonl_result([data], self.output_file)

        self.tfm.append_stagefctn(wrap_save_jsonl_result)  # save stage

        # Add tasks to the waiting queue
        for i in range(5):
            self.q_wait.put(i)

    def tearDown(self):
        # Stop the taskflow and clean up temporary files
        self.tfm.stop()
        self.tempdir.cleanup()

    def test_http_to_jsonl_flow(self):
        """Verify that the entire pipeline executes successfully."""
        self.tfm.start()
        self.tfm.await_print()
        self.tfm.stop()

        # Check that the JSONL output file exists and is not empty
        self.assertTrue(os.path.exists(self.output_file))
        with open(self.output_file, "r", encoding="utf-8") as f:
            lines = [json.loads(line.strip()) for line in f if line.strip()]
        self.assertGreater(len(lines), 0, "Output JSONL file is empty")

        # Validate the structure of each saved entry
        for entry in lines:
            self.assertIn("id", entry)
            self.assertIn("title_len", entry)
            self.assertIn("status", entry)

        print(f"\n Successfully saved to {self.output_file}")
        print(f"Example entry: {lines[0]}")


if __name__ == '__main__':
    unittest.main(verbosity=2)
