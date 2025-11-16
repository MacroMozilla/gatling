import functools
import datetime
import time


class Watch:
    """A simple stopwatch class using the datetime library."""
    def __init__(self, fctn=datetime.datetime.now):
        self.fctn = fctn
        self.tick = self.fctn()
        self.records = []

    def see_timedelta(self):
        """Records and returns the timedelta since the last check."""
        tick = self.fctn()
        res = tick - self.tick
        self.records.append(res)
        self.tick = tick
        return res

    def see_seconds(self):
        """Records and returns the seconds since the last check."""
        return round(self.see_timedelta().total_seconds(), 6)

    def total_timedelta(self):
        """Returns the total accumulated timedelta from all records."""
        # Use datetime.timedelta() as the starting value for the sum
        totalTime = sum(self.records, datetime.timedelta())
        return totalTime

    def total_seconds(self):
        """Returns the total accumulated seconds from all records."""
        return round(self.total_timedelta().total_seconds(), 6)


def watch_time(func):
    """A decorator to measure and print the execution time of a function."""
    @functools.wraps(func)
    def wrap(*args, **kwargs):
        w = Watch()
        res = func(*args, **kwargs)

        # The first and only call to see_seconds() measures the total duration
        print(f"Time Cost : '{func.__name__}' took {w.see_seconds()} seconds")
        return res

    return wrap


if __name__ == '__main__':
    pass
