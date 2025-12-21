import os
import tempfile
import unittest
from abc import ABC, abstractmethod
from itertools import combinations

from gatling.storage.g_table.table_ao_file_tsv import TableAO_FileTSV

from storage.table.a_const_test import const_key2type, const_keys, filterbykeys, rows2cols, const_keys_extra


class AbsctractTestFileTableAccess0Row(ABC):
    """Unit tests for TestFileTable class."""

    const_local_error: list
    const_local_index: list
    const_local_slices: list

    def setUp(self):
        """Create a temporary directory and test file path before each test."""

        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_fname = os.path.join(self.temp_dir.name, "test_table.tsv")
        print(f"Test file path: {self.test_fname}")

        self.ft = TableAO_FileTSV(self.test_fname).initialize(key2type=const_key2type)
        self.rows = []

    def tearDown(self):
        """Clean up temporary directory after each test."""
        self.temp_dir.cleanup()

    def test_getitem_00_index_error(self):
        for idx in self.const_local_error:
            with self.subTest(idx=idx, msg='get\t'):
                with self.assertRaises(IndexError):
                    _ = self.ft[idx]
            with self.subTest(idx=idx, msg='row\t'):
                with self.assertRaises(IndexError):
                    _ = self.ft.rows(idx)
            with self.subTest(idx=idx, msg='col\t'):
                with self.assertRaises(IndexError):
                    _ = self.ft.cols(None, idx)

    def test_getitem_01_index(self):
        for idx in self.const_local_index:
            with self.subTest(idx=idx, msg='get'):
                self.assertEqual(self.ft[idx], self.rows[idx])
            with self.subTest(idx=idx, msg='row'):
                self.assertEqual(self.ft.rows(idx), self.rows[idx])
            with self.subTest(idx=idx, msg='col'):
                self.assertEqual(self.ft.cols(None, idx), self.rows[idx])

    def test_getitem_02_index_1key(self):
        for idx in self.const_local_index:
            for key in const_keys:
                with self.subTest(idx=idx, key=key, msg='get'):
                    self.assertEqual(self.ft[idx, key], filterbykeys(self.rows[idx], [key]))
                with self.subTest(idx=idx, key=key, msg='row'):
                    self.assertEqual(self.ft.rows(idx, key), filterbykeys(self.rows[idx], [key]))
                with self.subTest(idx=idx, key=key, msg='col'):
                    self.assertEqual(self.ft.cols(key, idx), filterbykeys(self.rows[idx], [key]))

    def test_getitem_03_index_2key(self):
        for idx in self.const_local_index:
            for tpkeys in combinations(const_keys, 2):
                keys = list(tpkeys)
                with self.subTest(idx=idx, keys=keys, msg='get'):
                    self.assertEqual(self.ft[idx, keys], filterbykeys(self.rows[idx], keys))
                with self.subTest(idx=idx, keys=keys, msg='row'):
                    self.assertEqual(self.ft.rows(idx, keys), filterbykeys(self.rows[idx], keys))
                with self.subTest(idx=idx, keys=keys, msg='col'):
                    self.assertEqual(self.ft.cols(keys, idx), filterbykeys(self.rows[idx], keys))

    def test_getitem_04_slice(self):
        for slc in self.const_local_slices:
            with self.subTest(slc=slc, msg='get'):
                self.assertEqual(self.ft[slc], self.rows[slc])
            with self.subTest(slc=slc, msg='row'):
                self.assertEqual(self.ft.rows(slc), self.rows[slc])
            with self.subTest(slc=slc, msg='col'):
                self.assertEqual(self.ft.cols(None, slc), rows2cols(self.rows[slc], keys=const_keys_extra))

    def test_getitem_05_slice_1key(self):
        for slc in self.const_local_slices:
            for key in const_keys:
                with self.subTest(slc=slc, key=key, msg='get'):
                    self.assertEqual(self.ft[slc, key], filterbykeys(self.rows[slc], [key]))
                with self.subTest(slc=slc, key=key, msg='row'):
                    self.assertEqual(self.ft.rows(slc, key), filterbykeys(self.rows[slc], [key]))
                with self.subTest(slc=slc, key=key, msg='col'):
                    self.assertEqual(self.ft.cols(key, slc), rows2cols(filterbykeys(self.rows[slc], [key]), keys=[key]))

    def test_getitem_06_slice_2key(self):
        for slc in self.const_local_slices:
            for tpkeys in combinations(const_keys, 2):
                keys = list(tpkeys)
                with self.subTest(slc=slc, keys=keys, msg='get'):
                    self.assertEqual(self.ft[slc, keys], filterbykeys(self.rows[slc], keys))
                with self.subTest(slc=slc, keys=keys, msg='row'):
                    self.assertEqual(self.ft.rows(slc, keys), filterbykeys(self.rows[slc], keys))
                with self.subTest(slc=slc, keys=keys, msg='col'):
                    self.assertEqual(self.ft.cols(keys, slc), rows2cols(filterbykeys(self.rows[slc], keys), keys=keys))

    def test_getitem_ctxt_10_index_error(self):
        for idx in self.const_local_error:
            with self.subTest(idx=idx, msg='get\t'):
                with self.assertRaises(IndexError):
                    with self.ft:
                        _ = self.ft[idx]
            with self.subTest(idx=idx, msg='row\t'):
                with self.assertRaises(IndexError):
                    with self.ft:
                        _ = self.ft.rows(idx)
            with self.subTest(idx=idx, msg='col\t'):
                with self.assertRaises(IndexError):
                    with self.ft:
                        _ = self.ft.cols(None, idx)

    def test_getitem_ctxt_11_index(self):
        for idx in self.const_local_index:
            with self.subTest(idx=idx, msg='get'):
                with self.ft:
                    self.assertEqual(self.ft[idx], self.rows[idx])
            with self.subTest(idx=idx, msg='row'):
                with self.ft:
                    self.assertEqual(self.ft.rows(idx), self.rows[idx])
            with self.subTest(idx=idx, msg='col'):
                with self.ft:
                    self.assertEqual(self.ft.cols(None, idx), self.rows[idx])

    def test_getitem_ctxt_12_index_1key(self):
        for idx in self.const_local_index:
            for key in const_keys:
                with self.subTest(idx=idx, key=key, msg='get'):
                    with self.ft:
                        self.assertEqual(self.ft[idx, key], filterbykeys(self.rows[idx], [key]))
                with self.subTest(idx=idx, key=key, msg='row'):
                    with self.ft:
                        self.assertEqual(self.ft.rows(idx, key), filterbykeys(self.rows[idx], [key]))
                with self.subTest(idx=idx, key=key, msg='col'):
                    with self.ft:
                        self.assertEqual(self.ft.cols(key, idx), filterbykeys(self.rows[idx], [key]))

    def test_getitem_ctxt_13_index_2key(self):
        for idx in self.const_local_index:
            for tpkeys in combinations(const_keys, 2):
                keys = list(tpkeys)
                with self.subTest(idx=idx, keys=keys, msg='get'):
                    with self.ft:
                        self.assertEqual(self.ft[idx, keys], filterbykeys(self.rows[idx], keys))
                with self.subTest(idx=idx, keys=keys, msg='row'):
                    with self.ft:
                        self.assertEqual(self.ft.rows(idx, keys), filterbykeys(self.rows[idx], keys))
                with self.subTest(idx=idx, keys=keys, msg='col'):
                    with self.ft:
                        self.assertEqual(self.ft.cols(keys, idx), filterbykeys(self.rows[idx], keys))

    def test_getitem_ctxt_14_slice(self):
        for slc in self.const_local_slices:
            with self.subTest(slc=slc, msg='get'):
                with self.ft:
                    self.assertEqual(self.ft[slc], self.rows[slc])
            with self.subTest(slc=slc, msg='row'):
                with self.ft:
                    self.assertEqual(self.ft.rows(slc), self.rows[slc])
            with self.subTest(slc=slc, msg='col'):
                with self.ft:
                    self.assertEqual(self.ft.cols(None, slc), rows2cols(self.rows[slc], keys=const_keys_extra))

    def test_getitem_ctxt_15_slice_1key(self):
        for slc in self.const_local_slices:
            for key in const_keys:
                with self.subTest(slc=slc, key=key, msg='get'):
                    with self.ft:
                        self.assertEqual(self.ft[slc, key], filterbykeys(self.rows[slc], [key]))
                with self.subTest(slc=slc, key=key, msg='row'):
                    with self.ft:
                        self.assertEqual(self.ft.rows(slc, key), filterbykeys(self.rows[slc], [key]))
                with self.subTest(slc=slc, key=key, msg='col'):
                    with self.ft:
                        self.assertEqual(self.ft.cols(key, slc), rows2cols(filterbykeys(self.rows[slc], [key]), keys=[key]))

    def test_getitem_ctxt_16_slice_2key(self):
        for slc in self.const_local_slices:
            for tpkeys in combinations(const_keys, 2):
                keys = list(tpkeys)
                with self.subTest(slc=slc, keys=keys, msg='get'):
                    with self.ft:
                        self.assertEqual(self.ft[slc, keys], filterbykeys(self.rows[slc], keys))
                with self.subTest(slc=slc, keys=keys, msg='row'):
                    with self.ft:
                        self.assertEqual(self.ft.rows(slc, keys), filterbykeys(self.rows[slc], keys))
                with self.subTest(slc=slc, keys=keys, msg='col'):
                    with self.ft:
                        self.assertEqual(self.ft.cols(keys, slc), rows2cols(filterbykeys(self.rows[slc], keys), keys=keys))


if __name__ == "__main__":
    unittest.main(verbosity=2)
