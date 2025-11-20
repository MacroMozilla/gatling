from typing import Callable, List, Any, Optional, Generic
from gatling.storage.queue.base_queue import BaseQueue

class CountOnlyQueue(BaseQueue):
    """
    A lightweight fake queue that only counts how many items are active.
    Behaves like Queue but doesn't actually store items.
    """

    def __init__(self):
        super().__init__()
        self.count = 0

    def put(self, item=None, block=True, timeout=None):
        self.count += 1

    def get(self, block=True, timeout=None):
        if self.count > 0:
            self.count -= 1
        return None  # No real object

    def clear(self):
        self.count = 0

    def __iter__(self):
        return [None for i in range(self.count)]

    def __len__(self):
        return self.count



if __name__ == '__main__':
    pass

    coq = CountOnlyQueue()
