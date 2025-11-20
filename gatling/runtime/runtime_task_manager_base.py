from contextlib import contextmanager
from typing import Callable, Optional
from abc import ABC, abstractmethod


class RuntimeTaskManager(ABC):

    def __init__(self, fctn: Callable, args: tuple = (), kwargs: Optional[dict] = None):
        self.fctn = fctn
        self.args = args
        self.kwargs = kwargs or {}

    @abstractmethod
    def start(self, worker=1):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def __len__(self):
        pass

    @contextmanager
    def execute(self, worker=1):
        """
        A context wrapper for use with the with syntax:
        Starts on entry and automatically stops on exit.
        """
        try:
            self.start(worker)
            yield self
        finally:
            self.stop()


    def __str__(self):
        return f"[{self.__class__.__name__}] {self.fctn.__name__}({len(self)})"


if __name__ == "__main__":
    pass
