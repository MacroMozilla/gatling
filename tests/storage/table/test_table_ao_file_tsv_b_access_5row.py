import os
import tempfile
import unittest
from itertools import combinations

from gatling.storage.g_table.table_ao_file_tsv import TableAO_FileTSV, KEY_IDX
from gatling.storage.g_table.help_tools.slice_tools import Slice
from storage.table.a_const_test import const_key2type, rand_row, const_keys, filterbykeys
from storage.table.abstract_test_table_ao_file_tsv_b_access_0base import AbsctractTestFileTableAccess0Row

s = Slice


class TestFileTableAccess5Row(AbsctractTestFileTableAccess0Row, unittest.TestCase):
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
        AbsctractTestFileTableAccess0Row.setUp(self)
        for idx in range(5):
            row = rand_row()
            row_extra = {KEY_IDX: idx, **row}
            self.ft.append(row_extra)
            self.rows.append(row_extra)


if __name__ == "__main__":
    unittest.main(verbosity=2)
