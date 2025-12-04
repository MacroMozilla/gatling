import unittest
import os
import tempfile
import sys
from io import StringIO
from dataclasses import dataclass

from gatling.utility.io_fctns import save_pickle, read_pickle


@dataclass
class Person:
    name: str
    age: int


class TestPickleFunctions(unittest.TestCase):
    """Test save_pickle and read_pickle functions"""

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        self.dpath = self.tempdir.name
        self._stderr = sys.stderr
        sys.stderr = StringIO()
        self._stdout = sys.stdout
        sys.stdout = StringIO()

    def tearDown(self):
        self.tempdir.cleanup()
        sys.stderr = self._stderr
        sys.stdout = self._stdout

    # ==================== Basic Types ====================
    def test_none(self):
        fpath = os.path.join(self.dpath, 'none.pkl.zst')
        save_pickle(None, fpath)
        self.assertIsNone(read_pickle(fpath))

    def test_bool(self):
        fpath = os.path.join(self.dpath, 'bool.pkl.zst')
        save_pickle(True, fpath)
        self.assertTrue(read_pickle(fpath))
        save_pickle(False, fpath)
        self.assertFalse(read_pickle(fpath))

    def test_int(self):
        fpath = os.path.join(self.dpath, 'int.pkl.zst')
        for val in [0, 1, -1, 42, -9999, 10 ** 100]:
            save_pickle(val, fpath)
            self.assertEqual(read_pickle(fpath), val)

    def test_float(self):
        fpath = os.path.join(self.dpath, 'float.pkl.zst')
        for val in [0.0, 3.14159, -2.71828, 1e100, float('inf')]:
            save_pickle(val, fpath)
            self.assertEqual(read_pickle(fpath), val)

    def test_complex(self):
        fpath = os.path.join(self.dpath, 'complex.pkl.zst')
        save_pickle(3 + 4j, fpath)
        self.assertEqual(read_pickle(fpath), 3 + 4j)

    def test_string(self):
        fpath = os.path.join(self.dpath, 'string.pkl.zst')
        for val in ["", "hello", "你好世界 🚀", "x" * 10000]:
            save_pickle(val, fpath)
            self.assertEqual(read_pickle(fpath), val)

    def test_bytes(self):
        fpath = os.path.join(self.dpath, 'bytes.pkl.zst')
        for val in [b"", b"hello", bytes(range(256)), os.urandom(1000)]:
            save_pickle(val, fpath)
            self.assertEqual(read_pickle(fpath), val)

    # ==================== Collections ====================
    def test_list(self):
        fpath = os.path.join(self.dpath, 'list.pkl.zst')
        for val in [[], [1, 2, 3], list(range(1000)), [[1, 2], [3, 4]]]:
            save_pickle(val, fpath)
            self.assertEqual(read_pickle(fpath), val)

    def test_tuple(self):
        fpath = os.path.join(self.dpath, 'tuple.pkl.zst')
        for val in [(), (1, 2, 3), tuple(range(1000))]:
            save_pickle(val, fpath)
            self.assertEqual(read_pickle(fpath), val)

    def test_set(self):
        fpath = os.path.join(self.dpath, 'set.pkl.zst')
        for val in [set(), {1, 2, 3}, frozenset([1, 2, 3])]:
            save_pickle(val, fpath)
            self.assertEqual(read_pickle(fpath), val)

    def test_dict(self):
        fpath = os.path.join(self.dpath, 'dict.pkl.zst')
        for val in [{}, {"a": 1}, {"nested": {"deep": 42}}, {i: i ** 2 for i in range(100)}]:
            save_pickle(val, fpath)
            self.assertEqual(read_pickle(fpath), val)

    # ==================== Lambda ====================
    def test_lambda_simple(self):
        fpath = os.path.join(self.dpath, 'lambda.pkl.zst')
        save_pickle(lambda x: x + 1, fpath)
        self.assertEqual(read_pickle(fpath)(10), 11)

    def test_lambda_multi_args(self):
        fpath = os.path.join(self.dpath, 'lambda_multi.pkl.zst')
        save_pickle(lambda x, y, z: x * y + z, fpath)
        self.assertEqual(read_pickle(fpath)(2, 3, 4), 10)

    def test_lambda_closure(self):
        fpath = os.path.join(self.dpath, 'lambda_closure.pkl.zst')
        multiplier = 10
        save_pickle(lambda x: x * multiplier, fpath)
        self.assertEqual(read_pickle(fpath)(5), 50)

    def test_lambda_nested(self):
        fpath = os.path.join(self.dpath, 'lambda_nested.pkl.zst')
        save_pickle(lambda x: (lambda y: x + y), fpath)
        self.assertEqual(read_pickle(fpath)(10)(5), 15)

    def test_lambda_in_list(self):
        fpath = os.path.join(self.dpath, 'lambda_list.pkl.zst')
        save_pickle([lambda x: x * 2, lambda x: x ** 2], fpath)
        funcs = read_pickle(fpath)
        self.assertEqual(funcs[0](3), 6)
        self.assertEqual(funcs[1](3), 9)

    def test_lambda_in_dict(self):
        fpath = os.path.join(self.dpath, 'lambda_dict.pkl.zst')
        save_pickle({"add": lambda a, b: a + b, "mul": lambda a, b: a * b}, fpath)
        funcs = read_pickle(fpath)
        self.assertEqual(funcs["add"](2, 3), 5)
        self.assertEqual(funcs["mul"](2, 3), 6)

    # ==================== Functions ====================
    def test_function(self):
        fpath = os.path.join(self.dpath, 'func.pkl.zst')

        def add(a, b):
            return a + b

        save_pickle(add, fpath)
        self.assertEqual(read_pickle(fpath)(2, 3), 5)

    def test_recursive_function(self):
        fpath = os.path.join(self.dpath, 'func_rec.pkl.zst')

        def factorial(n):
            return 1 if n <= 1 else n * factorial(n - 1)

        save_pickle(factorial, fpath)
        self.assertEqual(read_pickle(fpath)(5), 120)

    # ==================== Objects ====================
    def test_dataclass(self):
        fpath = os.path.join(self.dpath, 'dataclass.pkl.zst')
        save_pickle(Person("Alice", 30), fpath)
        result = read_pickle(fpath)
        self.assertEqual(result.name, "Alice")
        self.assertEqual(result.age, 30)

    def test_object_list(self):
        fpath = os.path.join(self.dpath, 'obj_list.pkl.zst')
        data = [Person(f"User_{i}", 20 + i) for i in range(100)]
        save_pickle(data, fpath)
        result = read_pickle(fpath)
        self.assertEqual(len(result), 100)
        self.assertEqual(result[0].name, "User_0")
        self.assertEqual(result[99].age, 119)

    # ==================== Edge Cases ====================
    def test_circular_reference(self):
        fpath = os.path.join(self.dpath, 'circular.pkl.zst')
        data = [1, 2, 3]
        data.append(data)
        save_pickle(data, fpath)
        result = read_pickle(fpath)
        self.assertEqual(result[:3], [1, 2, 3])
        self.assertIs(result[3], result)

    def test_mixed_complex(self):
        fpath = os.path.join(self.dpath, 'mixed.pkl.zst')
        data = {
            "users": [Person(f"U{i}", i) for i in range(10)],
            "transform": lambda x: x * 2,
            "nested": {"a": {"b": {"c": [1, 2, 3]}}},
        }
        save_pickle(data, fpath)
        result = read_pickle(fpath)
        self.assertEqual(len(result["users"]), 10)
        self.assertEqual(result["transform"](5), 10)
        self.assertEqual(result["nested"]["a"]["b"]["c"], [1, 2, 3])

    # ==================== Compression Levels ====================
    def test_compression_levels(self):
        fpath = os.path.join(self.dpath, 'level.pkl.zst')
        data = list(range(10000))
        for level in [1, 3, 6, 9, 12, 19]:
            save_pickle(data, fpath, level=level)
            self.assertEqual(read_pickle(fpath), data)

    # ==================== Large Data ====================
    def test_large_list(self):
        fpath = os.path.join(self.dpath, 'large_list.pkl.zst')
        data = list(range(100000))
        save_pickle(data, fpath)
        self.assertEqual(read_pickle(fpath), data)

    def test_large_dict(self):
        fpath = os.path.join(self.dpath, 'large_dict.pkl.zst')
        data = {f"key_{i}": {"val": i, "data": list(range(10))} for i in range(1000)}
        save_pickle(data, fpath)
        self.assertEqual(read_pickle(fpath), data)


if __name__ == '__main__':
    unittest.main(verbosity=2)