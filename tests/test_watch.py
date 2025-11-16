import unittest
import time
from datetime import timedelta

from utility.watch import Watch, watch_time  # adjust import path if needed


class TestWatchTime(unittest.TestCase):
    """Unit tests for Watch class and @watch_time decorator."""

    # ============================================================
    # Test the @watch_time decorator
    # ============================================================
    def test_watch_time_decorator(self):
        """Ensure the decorator executes correctly and returns expected result."""

        @watch_time
        def decorated_function(sleep_duration):
            """A sample function that simply sleeps for a given duration."""
            time.sleep(sleep_duration)
            return "Decorator test completed!"

        start = time.perf_counter()
        result = decorated_function(0.5)
        end = time.perf_counter()

        # Check return value
        self.assertEqual(result, "Decorator test completed!")

        # Check elapsed time was at least 0.5s (allowing for small differences)
        self.assertGreaterEqual(end - start, 0.48)

    # ============================================================
    # Test the Watch class step-by-step
    # ============================================================
    def test_watch_class_methods(self):
        """Verify all Watch methods produce valid and consistent timing values."""

        # 1. __init__
        watch = Watch()
        time.sleep(0.2)

        # 2. see_timedelta()
        td1 = watch.see_timedelta()
        self.assertIsInstance(td1, timedelta)
        self.assertGreater(td1.total_seconds(), 0.0)

        time.sleep(0.3)

        # 3. see_seconds()
        secs2 = watch.see_seconds()
        self.assertIsInstance(secs2, float)
        self.assertGreater(secs2, 0.0)

        # Internal record should contain at least 2 timedelta objects
        self.assertGreaterEqual(len(watch.records), 2)

        # 4. total_timedelta()
        total_td = watch.total_timedelta()
        self.assertIsInstance(total_td, timedelta)
        self.assertGreater(total_td.total_seconds(), 0.0)

        # 5. total_seconds()
        total_secs = watch.total_seconds()
        self.assertIsInstance(total_secs, float)
        self.assertAlmostEqual(total_secs, total_td.total_seconds(), delta=1e-6)

        # The total should approximately equal the sum of individual durations
        sum_parts = td1.total_seconds() + secs2
        diff = abs(sum_parts - total_secs)
        self.assertLess(diff, 0.05, f"Timing difference too large: {diff}")

        print("\n Watch class methods behave as expected.")


if __name__ == '__main__':
    unittest.main(verbosity=2)
