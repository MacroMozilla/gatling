from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class BaseQueue(ABC, Generic[T]):
    """
    Abstract base class for queue implementations.

    Defines the minimal interface that any queue implementation should provide,
    including methods for adding, retrieving, deleting, and iterating over items.
    It also supports context management via open() and close() hooks.
    """

    def __init__(self):
        super().__init__()

    # === Abstract methods (must be implemented by subclasses) ===

    @abstractmethod
    def put(self, item: T, block=True, timeout=None):
        """
        Insert an item into the queue.
        Args:
            item: The item to insert.
            block (bool): Whether to block if the queue is full.
            timeout (float | None): Maximum time to wait if blocking.
        """
        pass

    @abstractmethod
    def get(self, block=True, timeout=None) -> T:
        """
        Remove and return an item from the queue.
        Args:
            block (bool): Whether to block until an item is available.
            timeout (float | None): Timeout in seconds if blocking.
        """
        pass

    @abstractmethod
    def clear(self):
        """Delete all data or destroy the underlying queue resource."""
        pass

    @abstractmethod
    def __len__(self):
        """Return the current number of items in the queue."""
        pass

    @abstractmethod
    def __iter__(self):
        """Return an iterator over the items currently in the queue."""
        pass

    def __repr__(self):
        return f"<{self.__class__.__name__}({len(self)})-{id(self)}>"

    # === Optional lifecycle hooks ===

    def open(self):
        """Initialize or connect to underlying resources (optional)."""
        pass

    def close(self):
        """Release or disconnect resources (optional)."""
        pass

    # === Default helper methods (no need to override) ===

    def __enter__(self):
        """Open the queue when entering a context."""
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the queue when exiting a context."""
        self.close()


if __name__ == '__main__':
    pass
