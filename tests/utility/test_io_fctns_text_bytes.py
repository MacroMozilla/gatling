import unittest
import os
import tempfile

from gatling.utility.io_fctns import read_text, save_text, read_bytes, save_bytes, remove_file


class TestTextFunctions(unittest.TestCase):
    """Test read/save functions for text files"""

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.dpath = self.tempdir.name

    def tearDown(self):
        self.tempdir.cleanup()

    # ==================== read_text ====================
    def test_read_text_simple(self):
        fpath = os.path.join(self.dpath, 'simple.txt')
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write('hello world')

        result = read_text(fpath)
        self.assertEqual(result, 'hello world')

    def test_read_text_multiline(self):
        fpath = os.path.join(self.dpath, 'multi.txt')
        content = 'line1\nline2\nline3'
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(content)

        result = read_text(fpath)
        self.assertEqual(result, content)

    def test_read_text_unicode(self):
        fpath = os.path.join(self.dpath, 'unicode.txt')
        content = 'Hello 你好 こんにちは 🚀'
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(content)

        result = read_text(fpath)
        self.assertEqual(result, content)

    def test_read_text_empty(self):
        fpath = os.path.join(self.dpath, 'empty.txt')
        with open(fpath, 'w', encoding='utf-8') as _:
            pass

        result = read_text(fpath)
        self.assertEqual(result, '')

    def test_read_text_not_found(self):
        fpath = os.path.join(self.dpath, 'not_exist.txt')
        with self.assertRaises(FileNotFoundError):
            read_text(fpath)

    # ==================== save_text ====================
    def test_save_text_simple(self):
        fpath = os.path.join(self.dpath, 'save.txt')
        save_text('hello', fpath)

        self.assertEqual(read_text(fpath), 'hello')

    def test_save_text_unicode(self):
        fpath = os.path.join(self.dpath, 'save_unicode.txt')
        content = '中文测试 🎉'
        save_text(content, fpath)

        self.assertEqual(read_text(fpath), content)

    def test_save_text_overwrite(self):
        fpath = os.path.join(self.dpath, 'overwrite.txt')
        save_text('old', fpath)
        save_text('new', fpath)

        self.assertEqual(read_text(fpath), 'new')

    def test_save_text_append(self):
        fpath = os.path.join(self.dpath, 'append.txt')
        save_text('first', fpath)
        save_text('_second', fpath, mode='a')

        self.assertEqual(read_text(fpath), 'first_second')

    # ==================== read_bytes ====================
    def test_read_bytes_simple(self):
        fpath = os.path.join(self.dpath, 'simple.bin')
        data = b'\x00\x01\x02\xff'
        with open(fpath, 'wb') as f:
            f.write(data)

        result = read_bytes(fpath)
        self.assertEqual(result, data)

    def test_read_bytes_empty(self):
        fpath = os.path.join(self.dpath, 'empty.bin')
        with open(fpath, 'wb') as _:
            pass

        result = read_bytes(fpath)
        self.assertEqual(result, b'')

    def test_read_bytes_large(self):
        fpath = os.path.join(self.dpath, 'large.bin')
        data = os.urandom(10000)
        with open(fpath, 'wb') as f:
            f.write(data)

        result = read_bytes(fpath)
        self.assertEqual(result, data)

    def test_read_bytes_not_found(self):
        fpath = os.path.join(self.dpath, 'not_exist.bin')
        with self.assertRaises(FileNotFoundError):
            read_bytes(fpath)

    # ==================== save_bytes ====================
    def test_save_bytes_simple(self):
        fpath = os.path.join(self.dpath, 'save.bin')
        data = b'\xde\xad\xbe\xef'
        save_bytes(data, fpath)

        self.assertEqual(read_bytes(fpath), data)

    def test_save_bytes_overwrite(self):
        fpath = os.path.join(self.dpath, 'overwrite.bin')
        save_bytes(b'old', fpath)
        save_bytes(b'new', fpath)

        self.assertEqual(read_bytes(fpath), b'new')

    def test_save_bytes_append(self):
        fpath = os.path.join(self.dpath, 'append.bin')
        save_bytes(b'first', fpath)
        save_bytes(b'_second', fpath, mode='ab')

        self.assertEqual(read_bytes(fpath), b'first_second')

    # ==================== remove_file ====================
    def test_remove_file_exists(self):
        fpath = os.path.join(self.dpath, 'to_remove.txt')
        save_text('temp', fpath)
        self.assertTrue(os.path.exists(fpath))

        remove_file(fpath)
        self.assertFalse(os.path.exists(fpath))

    def test_remove_file_not_exists(self):
        fpath = os.path.join(self.dpath, 'not_exist.txt')
        remove_file(fpath)  # should not raise


if __name__ == '__main__':
    unittest.main(verbosity=2)
