import unittest
from contextlib import contextmanager



class SubTestCase(unittest.TestCase):
    pass
    @contextmanager
    def subTestCase(self, **params):
        """subTestCase with setup/teardown"""
        with self.subTest(**params):
            # setup
            self.setUp()
            try:
                yield self
            finally:
                # teardown
                self.tearDown()
