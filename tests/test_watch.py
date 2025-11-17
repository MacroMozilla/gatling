import unittest
import time
from datetime import timedelta

from gatling.utility.watch import Watch, watch_time  # adjust path if needed


class TestWatchTime(unittest.TestCase):
    """Unit tests for Watch class and @watch_time decorator (stable version)."""

    # ------------------------------------------------------------
    def test_watch_time_decorator(self):
        """Verify that @watch_time runs and logs correctly without strict timing."""

        @watch_time
        def decorated_function(sleep_duration):
            """A sample function that simply sleeps for a given duration."""
            time.sleep(sleep_duration)
            return "Decorator test completed!"

        sleep_dur = 0.4
        start = time.perf_counter()
        result = decorated_function(sleep_dur)
        end = time.perf_counter()

        elapsed = end - start
        print(f"\n[decorator] elapsed = {elapsed:.4f}s (expected ≈ {sleep_dur}s)")

        # ✅ Function should return correct value
        self.assertEqual(result, "Decorator test completed!")

        # ✅ At least some time must have passed ( > 0.1s )
        self.assertGreater(elapsed, 0.1)

        # ✅ Should not be unreasonably long ( > 5× expected sleep )
        self.assertLess(elapsed, sleep_dur * 5)

    # ------------------------------------------------------------
    def test_watch_class_methods(self):
        """Validate Watch API consistency with generous tolerance."""

        watch = Watch()
        time.sleep(0.15)

        td1 = watch.see_timedelta()
        self.assertIsInstance(td1, timedelta)

        time.sleep(0.2)
        secs2 = watch.see_seconds()
        self.assertIsInstance(secs2, float)

        total_td = watch.total_timedelta()
        total_secs = watch.total_seconds()

        print(
            f"[watch] td1={td1.total_seconds():.3f}s, "
            f"secs2={secs2:.3f}s, total={total_secs:.3f}s"
        )

        # ✅ All positive
        self.assertGreater(td1.total_seconds(), 0)
        self.assertGreater(secs2, 0)
        self.assertGreater(total_secs, 0)

        # ✅ total_secs should roughly equal sum of parts (allow big tolerance)
        sum_parts = td1.total_seconds() + secs2
        diff = abs(sum_parts - total_secs)
        self.assertLess(diff, 0.5, f"Timing diff too large: {diff}")

        # ✅ Internal consistency: total_td ≈ total_secs
        self.assertAlmostEqual(total_td.total_seconds(), total_secs, delta=0.5)

        print("✅ Watch methods behaved consistently.")


if __name__ == "__main__":
    unittest.main(verbosity=2)
