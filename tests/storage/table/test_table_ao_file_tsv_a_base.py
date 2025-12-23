import os
import tempfile
import unittest

from gatling.storage.g_table.table_ao_file_tsv import TableAO_FileTSV, KEY_IDX
from gatling.utility.error_tools import FileAlreadyOpenedError, FileAlreadyOpenedForWriteError
from storage.table.a_const_test import const_key2type, rand_row


class TestFileTableBase(unittest.TestCase):
    """Unit tests for TestFileTable class."""

    def setUp(self):
        """Create a temporary directory and test file path before each test."""
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_fname = os.path.join(self.temp_dir.name, "test_table.tsv")
        print(f"Test file path: {self.test_fname}")

    def tearDown(self):
        """Clean up temporary directory after each test."""
        self.temp_dir.cleanup()

    def test_00_nofile_x_getkey(self):
        ft = TableAO_FileTSV(self.test_fname)
        with self.assertRaises(FileNotFoundError):
            ft.get_key2type()
        self.assertFalse(ft.exists())

    def test_01_nofile_x_getfirstrow(self):
        ft = TableAO_FileTSV(self.test_fname)
        with self.assertRaises(FileNotFoundError):
            ft.get_first_row()
        self.assertFalse(ft.exists())

    def test_02_nofile_x_getlastrow(self):
        ft = TableAO_FileTSV(self.test_fname)
        with self.assertRaises(FileNotFoundError):
            ft.get_last_row()
        self.assertFalse(ft.exists())

    def test_10_initialized_x_getkey(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)

        key2type_extra = {KEY_IDX: int, **const_key2type}
        self.assertTrue(ft.exists())
        self.assertEqual(ft.get_key2type(), key2type_extra)
        self.assertEqual(list(ft.get_key2type().keys()), list(key2type_extra.keys()))

    def test_11_initialized_x_getfirstrow(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        self.assertTrue(ft.exists())
        self.assertEqual(ft.get_first_row(), {})

    def test_12_initialized_x_getlastrow(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        self.assertTrue(ft.exists())
        self.assertEqual(ft.get_last_row(), {})

    def test_20_row1_x_getkey(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)
        key2type_extra = {KEY_IDX: int, **const_key2type}
        self.assertEqual(ft.get_key2type(), key2type_extra)
        self.assertEqual(list(ft.get_key2type().keys()), list(key2type_extra.keys()))

    def test_21_row1_x_getfirstrow(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)
        row0_extra = {KEY_IDX: 0, **row0}
        self.assertEqual(ft.get_first_row(), row0_extra)

    def test_22_row1_x_getlastrow(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)
        row0_extra = {KEY_IDX: 0, **row0}
        self.assertEqual(ft.get_last_row(), row0_extra)

    def test_30_row2_x_getkey(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        row1 = rand_row()
        ft.append(row0)
        ft.append(row1)

        key2type_extra = {KEY_IDX: int, **const_key2type}
        self.assertEqual(ft.get_key2type(), key2type_extra)
        self.assertEqual(list(ft.get_key2type().keys()), list(key2type_extra.keys()))

    def test_31_row2_x_getfirstrow(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)
        row1 = rand_row()
        ft.append(row1)

        row0_extra = {KEY_IDX: 0, **row0}
        self.assertEqual(ft.get_first_row(), row0_extra)

    def test_32_row2_x_getlastrow(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)
        row1 = rand_row()
        ft.append(row1)
        row1_extra = {KEY_IDX: 1, **row1}
        self.assertEqual(ft.get_last_row(), row1_extra)

    def test_40_clear(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)
        ft.clear()
        with self.assertRaises(FileNotFoundError):
            ft.get_key2type()

    def test_50_ctxt_errr_get_key2type(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)

        with self.assertRaises(FileAlreadyOpenedForWriteError):
            with ft:
                _ = ft.get_key2type()

    def test_51_ctxt_errr_get_first_row(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)

        with self.assertRaises(FileAlreadyOpenedForWriteError):
            with ft:
                _ = ft.get_first_row()

    def test_52_ctxt_errr_get_last_row(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)

        with self.assertRaises(FileAlreadyOpenedForWriteError):
            with ft:
                _ = ft.get_last_row()

    def test_52_ctxt_errr_clear(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)

        with self.assertRaises(FileAlreadyOpenedError):
            with ft:
                ft.clear()

    def test_53_ctxt_errr_initialize(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)

        with self.assertRaises(FileAlreadyOpenedError):
            with ft:
                ft.initialize(key2type=const_key2type)

    def test_53_ctxt_errr_build_state(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)

        with self.assertRaises(FileAlreadyOpenedForWriteError):
            with ft:
                ft._build_state()

    def test_54_ctxt_errr_enter(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.append(row0)

        with self.assertRaises(FileAlreadyOpenedForWriteError):
            with ft:
                with ft:
                    pass

    def test_60_row0_ctxt_append(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        with ft:
            pass

        self.assertEqual(ft.get_first_row(), {})
        self.assertEqual(ft.get_last_row(), {})

    def test_61_row1_ctxt_append(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        with ft:
            ft.append(row0)
        row0_extra = {KEY_IDX: 0, **row0}

        self.assertEqual(ft.get_first_row(), row0_extra)
        self.assertEqual(ft.get_last_row(), row0_extra)

    def test_62_row2_ctxt_append(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        row1 = rand_row()
        with ft:
            ft.append(row0)
            ft.append(row1)
        row0_extra = {KEY_IDX: 0, **row0}
        row1_extra = {KEY_IDX: 1, **row1}

        self.assertEqual(ft.get_first_row(), row0_extra)
        self.assertEqual(ft.get_last_row(), row1_extra)

    def test_70_row0_extend(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        ft.extend([])

        self.assertEqual(ft.get_first_row(), {})
        self.assertEqual(ft.get_last_row(), {})

    def test_71_row1_extend(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        ft.extend([row0])
        row0_extra = {KEY_IDX: 0, **row0}

        self.assertEqual(ft.get_first_row(), row0_extra)
        self.assertEqual(ft.get_last_row(), row0_extra)

    def test_72_row2_extend(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        row1 = rand_row()
        ft.extend([row0, row1])
        row0_extra = {KEY_IDX: 0, **row0}
        row1_extra = {KEY_IDX: 1, **row1}

        self.assertEqual(ft.get_first_row(), row0_extra)
        self.assertEqual(ft.get_last_row(), row1_extra)

    def test_70_row0_ctxt_extend(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        with ft:
            ft.extend([])

        self.assertEqual(ft.get_first_row(), {})
        self.assertEqual(ft.get_last_row(), {})

    def test_71_row1_ctxt_extend(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        with ft:
            ft.extend([row0])
        row0_extra = {KEY_IDX: 0, **row0}

        self.assertEqual(ft.get_first_row(), row0_extra)
        self.assertEqual(ft.get_last_row(), row0_extra)

    def test_72_row2_ctxt_extend(self):
        ft = TableAO_FileTSV(self.test_fname)
        ft.initialize(key2type=const_key2type)
        row0 = rand_row()
        row1 = rand_row()
        with ft:
            ft.extend([row0, row1])
        row0_extra = {KEY_IDX: 0, **row0}
        row1_extra = {KEY_IDX: 1, **row1}

        self.assertEqual(ft.get_first_row(), row0_extra)
        self.assertEqual(ft.get_last_row(), row1_extra)

    def test_80_0_getitem(self):
        # case : no file
        ft = TableAO_FileTSV(self.test_fname)
        with self.assertRaises(FileNotFoundError):
            _ = ft[:]

    def test_80_1_getitem(self):
        # case : no file
        ft = TableAO_FileTSV(self.test_fname)
        with self.assertRaises(FileNotFoundError):
            _ = ft.rows()

    def test_80_2_getitem(self):
        # case : no file
        ft = TableAO_FileTSV(self.test_fname)
        with self.assertRaises(FileNotFoundError):
            _ = ft.cols()

    def test_81_0_getitem(self):
        # case : empty file
        ft = TableAO_FileTSV(self.test_fname).initialize(key2type=const_key2type)

        res = ft[:]
        self.assertEqual(res, [])

    def test_81_1_getitem(self):
        # case : empty file
        ft = TableAO_FileTSV(self.test_fname).initialize(key2type=const_key2type)
        res = ft.rows()
        self.assertEqual(res, [])

    def test_81_2_getitem(self):
        # case : empty file
        ft = TableAO_FileTSV(self.test_fname).initialize(key2type=const_key2type)
        res = ft.cols()
        const_keys = [*const_key2type.keys()]
        self.assertEqual(res, {k: [] for k in const_keys})


if __name__ == "__main__":
    unittest.main(verbosity=2)
