import unittest
import time
from datetime import timedelta

from gatling.utility.watch import Watch, watch_time  # adjust import path if needed


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
        result = decorated_function(0.4)
        end = time.perf_counter()

        # Check that the function returned correctly
        self.assertEqual(result, "Decorator test completed!")

        # Check elapsed time (allowing generous tolerance)
        elapsed = end - start
        print(f"\n[decorator] elapsed time = {elapsed:.3f}s")

        # Allow 0.35s to 0.7s range to avoid false failures on slow CI
        self.assertGreaterEqual(elapsed, 0.35)
        self.assertLess(elapsed, 0.8)

    # ============================================================
    # Test the Watch class step-by-step
    # ============================================================
    def test_watch_class_methods(self):
        """Verify all Watch methods produce valid and consistent timing values."""

        watch = Watch()
        time.sleep(0.2)

        # 1️⃣ see_timedelta()
        td1 = watch.see_timedelta()
        self.assertIsInstance(td1, timedelta)
        self.assertGreater(td1.total_seconds(), 0.0)

        time.sleep(0.25)

        # 2️⃣ see_seconds()
        secs2 = watch.see_seconds()
        self.assertIsInstance(secs2, float)
        self.assertGreater(secs2, 0.0)

        # Record list should have at least two durations
        self.assertGreaterEqual(len(watch.records), 2)

        # 3️⃣ total_timedelta()
        total_td = watch.total_timedelta()
        self.assertIsInstance(total_td, timedelta)
        self.assertGreater(total_td.total_seconds(), 0.0)

        # 4️⃣ total_seconds()
        total_secs = watch.total_seconds()
        self.assertIsInstance(total_secs, float)
        self.assertAlmostEqual(total_secs, total_td.total_seconds(), delta=0.002)

        # 5️⃣ Consistency check with tolerance (timing noise allowed)
        sum_parts = td1.total_seconds() + secs2
        diff = abs(sum_parts - total_secs)
        print(
            f"[watch] td1={td1.total_seconds():.3f}s, secs2={secs2:.3f}s, "
            f"total={total_secs:.3f}s, diff={diff:.4f}s"
        )

        # Relaxed tolerance (0.15s) for slower CI / test environments
        self.assertLess(diff, 0.15, f"Timing difference too large: {diff}")

        print("\n✅ Watch class methods behave as expected.")


if __name__ == "__main__":
    unittest.main(verbosity=2)
