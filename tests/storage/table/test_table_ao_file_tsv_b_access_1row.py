import unittest

from gatling.storage.g_table.help_tools.slice_tools import Slice
from helper.abstract_testcase import ConditionalTestSkipMeta
from storage.table.a_const_test import rand_row
from storage.table.abstract_test_table_ao_file_tsv_access_base import AbstractTestFileTableAccess0Row

s = Slice


class TestFileTableAccess1Row(AbstractTestFileTableAccess0Row, metaclass=ConditionalTestSkipMeta):
    """Unit tests for TestFileTable class."""
    const_local_error = [1, -2]
    const_local_index = [0, -1]
    const_local_slices = [s[:],
                          s[:1],
                          s[:0],
                          s[-1:],
                          s[2:],
                          s[::],
                          s[::1],
                          s[::-1],
                          s[::2],
                          ]

    def setUp(self):
        print(self.__class__.__name__)
        AbstractTestFileTableAccess0Row.setUp(self)

        row0 = rand_row()
        self.ft.append(row0)
        self.rows.append(row0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
