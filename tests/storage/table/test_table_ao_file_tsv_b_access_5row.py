import unittest

from gatling.storage.g_table.help_tools.slice_tools import Slice
from helper.abstract_testcase import ConditionalTestSkipMeta
from storage.table.a_const_test import rand_row
from storage.table.abstract_test_table_ao_file_tsv_access_base import AbstractTestFileTableAccess0Row

s = Slice


class TestFileTableAccess5Row(AbstractTestFileTableAccess0Row, metaclass=ConditionalTestSkipMeta):
    """Unit tests for TestFileTable class."""
    const_local_error = [5, -6]
    const_local_index = [0, 1, 2, 3, 4, -1, -2, -3, -4, -5]
    const_local_slices = [s[:],
                          s[:0],
                          s[:1],
                          s[:2],

                          s[2:],
                          s[-1:],
                          s[-2:],

                          s[::],
                          s[::1],
                          s[::-1],
                          s[::2],
                          s[::-2],

                          ]

    def setUp(self):
        print(self.__class__.__name__)
        AbstractTestFileTableAccess0Row.setUp(self)
        for idx in range(5):
            row = rand_row()

            self.ft.append(row)
            self.rows.append(row)


if __name__ == "__main__":
    unittest.main(verbosity=2)
