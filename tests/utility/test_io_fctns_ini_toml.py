import unittest
import os
import tempfile

from gatling.utility.io_fctns import read_ini, read_toml


class TestINIFunctions(unittest.TestCase):
    """Test read_ini function"""

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.dpath = self.tempdir.name

    def tearDown(self):
        self.tempdir.cleanup()

    def _write_ini(self, name: str, content: str) -> str:
        fpath = os.path.join(self.dpath, name)
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(content)
        return fpath

    # ==================== read_ini ====================
    def test_read_ini_simple(self):
        fpath = self._write_ini('simple.ini', '[section]\nkey = value\n')

        result = read_ini(fpath)
        self.assertEqual(result, {'section': {'key': 'value'}})

    def test_read_ini_multiple_sections(self):
        fpath = self._write_ini('multi.ini',
                                '[database]\nhost = localhost\nport = 5432\n\n'
                                '[logging]\nlevel = debug\n')

        result = read_ini(fpath)
        self.assertEqual(result['database'], {'host': 'localhost', 'port': '5432'})
        self.assertEqual(result['logging'], {'level': 'debug'})

    def test_read_ini_unicode(self):
        fpath = self._write_ini('unicode.ini', '[app]\nname = 测试应用\n')

        result = read_ini(fpath)
        self.assertEqual(result['app']['name'], '测试应用')

    def test_read_ini_empty_value(self):
        fpath = self._write_ini('empty_val.ini', '[section]\nkey =\n')

        result = read_ini(fpath)
        self.assertEqual(result['section']['key'], '')

    def test_read_ini_empty_file(self):
        fpath = self._write_ini('empty.ini', '')

        result = read_ini(fpath)
        self.assertEqual(result, {})

    def test_read_ini_no_sections(self):
        """File with only comments and no sections"""
        fpath = self._write_ini('comments.ini', '# just a comment\n; another comment\n')

        result = read_ini(fpath)
        self.assertEqual(result, {})

    def test_read_ini_not_found(self):
        fpath = os.path.join(self.dpath, 'not_exist.ini')
        # configparser.read does not raise FileNotFoundError for missing files,
        # it silently returns empty. So result should be empty dict.
        result = read_ini(fpath)
        self.assertEqual(result, {})

    def test_read_ini_special_characters(self):
        fpath = self._write_ini('special.ini',
                                '[paths]\ndata_dir = /var/data/app\nlog_file = C:\\logs\\app.log\n')

        result = read_ini(fpath)
        self.assertEqual(result['paths']['data_dir'], '/var/data/app')

    def test_read_ini_multiline_value(self):
        """ConfigParser supports continuation lines with leading whitespace"""
        fpath = self._write_ini('multiline.ini',
                                '[section]\nkey = line1\n  line2\n  line3\n')

        result = read_ini(fpath)
        self.assertIn('line1', result['section']['key'])


class TestTOMLFunctions(unittest.TestCase):
    """Test read_toml function"""

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.dpath = self.tempdir.name

    def tearDown(self):
        self.tempdir.cleanup()

    def _write_toml(self, name: str, content: str) -> str:
        fpath = os.path.join(self.dpath, name)
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(content)
        return fpath

    # ==================== read_toml ====================
    def test_read_toml_simple(self):
        fpath = self._write_toml('simple.toml', 'name = "test"\nvalue = 123\n')

        result = read_toml(fpath)
        self.assertEqual(result['name'], 'test')
        self.assertEqual(result['value'], 123)

    def test_read_toml_sections(self):
        fpath = self._write_toml('sections.toml',
                                 '[database]\nhost = "localhost"\nport = 5432\n\n'
                                 '[logging]\nlevel = "debug"\n')

        result = read_toml(fpath)
        self.assertEqual(result['database']['host'], 'localhost')
        self.assertEqual(result['database']['port'], 5432)
        self.assertEqual(result['logging']['level'], 'debug')

    def test_read_toml_types(self):
        """TOML supports native types unlike INI"""
        fpath = self._write_toml('types.toml',
                                 'string = "hello"\n'
                                 'integer = 42\n'
                                 'float_val = 3.14\n'
                                 'boolean = true\n'
                                 'array = [1, 2, 3]\n')

        result = read_toml(fpath)
        self.assertEqual(result['string'], 'hello')
        self.assertEqual(result['integer'], 42)
        self.assertAlmostEqual(result['float_val'], 3.14)
        self.assertTrue(result['boolean'])
        self.assertEqual(result['array'], [1, 2, 3])

    def test_read_toml_nested(self):
        fpath = self._write_toml('nested.toml',
                                 '[server]\nhost = "0.0.0.0"\n\n'
                                 '[server.ssl]\nenabled = true\ncert = "/path/to/cert"\n')

        result = read_toml(fpath)
        self.assertEqual(result['server']['host'], '0.0.0.0')
        self.assertTrue(result['server']['ssl']['enabled'])

    def test_read_toml_unicode(self):
        fpath = self._write_toml('unicode.toml', 'name = "测试"\nemoji = "🚀"\n')

        result = read_toml(fpath)
        self.assertEqual(result['name'], '测试')
        self.assertEqual(result['emoji'], '🚀')

    def test_read_toml_empty(self):
        fpath = self._write_toml('empty.toml', '')

        result = read_toml(fpath)
        self.assertEqual(result, {})

    def test_read_toml_not_found(self):
        fpath = os.path.join(self.dpath, 'not_exist.toml')
        with self.assertRaises(FileNotFoundError):
            read_toml(fpath)

    def test_read_toml_invalid(self):
        fpath = self._write_toml('invalid.toml', '[invalid\nbroken syntax')
        with self.assertRaises(RuntimeError):
            read_toml(fpath)

    def test_read_toml_inline_table(self):
        fpath = self._write_toml('inline.toml', 'point = {x = 1, y = 2}\n')

        result = read_toml(fpath)
        self.assertEqual(result['point'], {'x': 1, 'y': 2})


if __name__ == '__main__':
    unittest.main(verbosity=2)
