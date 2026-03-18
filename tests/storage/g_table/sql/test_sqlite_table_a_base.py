import os
import tempfile
import unittest

from gatling.storage.g_table.sql.real_sqlite_table import SQLiteTable
from storage.g_table.sql.a_const_test import (
    MinimalSchema, PyModeSchema, SQLiteSQLModeSchema,
    CompositePKSchema,
    rand_row_minimal, rand_row_py, rand_row_sqlite_sql,
    reset_counter,
)


class TestSQLiteTableBase(unittest.TestCase):
    """Tests for table lifecycle: create, exists, drop, truncate, keys."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_base"
        self.ft = SQLiteTable(self.table_name, self.db_path)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    # ===================== Lifecycle =====================

    def test_exists_before_create(self):
        self.assertFalse(self.ft.exists())

    def test_create_and_exists(self):
        self.ft.create(MinimalSchema)
        self.assertTrue(self.ft.exists())

    def test_drop(self):
        self.ft.create(MinimalSchema)
        self.assertTrue(self.ft.exists())
        self.ft.drop()
        self.assertFalse(self.ft.exists())

    def test_drop_nonexistent(self):
        self.ft.drop()
        self.assertFalse(self.ft.exists())

    def test_truncate(self):
        self.ft.create(MinimalSchema)
        row = rand_row_minimal()
        self.ft.insert(row)
        self.assertEqual(self.ft.count(), 1)
        self.ft.truncate()
        self.assertEqual(self.ft.count(), 0)
        self.assertTrue(self.ft.exists())

    def test_keys_minimal(self):
        self.ft.create(MinimalSchema)
        self.assertEqual(self.ft.keys(), ['id', 'name'])

    def test_keys_pymode(self):
        self.ft.create(PyModeSchema)
        expected = ['id', 'username', 'secret', 'is_active', 'level',
                    'balance', 'birthday', 'alarm', 'created_at']
        self.assertEqual(self.ft.keys(), expected)

    def test_keys_sqlmode(self):
        self.ft.create(SQLiteSQLModeSchema)
        expected = ['id', 'small_val', 'big_val', 'float_val', 'numeric_val',
                    'name', 'bio', 'is_active', 'birthday', 'alarm',
                    'created_at', 'raw_data', 'avatar']
        self.assertEqual(self.ft.keys(), expected)

    # ===================== Insert + Fetch (Minimal) =====================

    def test_insert_fetch_minimal(self):
        self.ft.create(MinimalSchema)
        row = rand_row_minimal()
        self.ft.insert(row)
        rows = self.ft.fetch()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0], row)

    def test_insert_multi(self):
        self.ft.create(MinimalSchema)
        rows = [rand_row_minimal() for _ in range(5)]
        self.ft.insert(*rows)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 5)
        for expected, actual in zip(rows, fetched):
            self.assertEqual(actual, expected)

    def test_insert_empty(self):
        self.ft.create(MinimalSchema)
        self.ft.insert()
        self.assertEqual(self.ft.count(), 0)

    # ===================== Count =====================

    def test_count_empty(self):
        self.ft.create(MinimalSchema)
        self.assertEqual(self.ft.count(), 0)

    def test_count_after_insert(self):
        self.ft.create(MinimalSchema)
        for _ in range(3):
            self.ft.insert(rand_row_minimal())
        self.assertEqual(self.ft.count(), 3)

    def test_count_with_where(self):
        self.ft.create(MinimalSchema)
        self.ft.insert({'id': 1, 'name': 'Alice'})
        self.ft.insert({'id': 2, 'name': 'Bob'})
        self.ft.insert({'id': 3, 'name': 'Alice'})
        self.assertEqual(self.ft.count({'name': 'Alice'}), 2)
        self.assertEqual(self.ft.count({'name': 'Bob'}), 1)
        self.assertEqual(self.ft.count({'name': 'Nobody'}), 0)

    # ===================== Lifecycle Edge Cases =====================

    def test_create_idempotent(self):
        """Calling create twice on same schema should not raise."""
        self.ft.create(MinimalSchema)
        self.ft.create(MinimalSchema)
        self.assertTrue(self.ft.exists())

    def test_fetch_empty_table(self):
        """Fetch on empty table returns empty list."""
        self.ft.create(MinimalSchema)
        fetched = self.ft.fetch()
        self.assertEqual(fetched, [])

    def test_insert_positional_args(self):
        """Insert multiple rows as positional args (not *list)."""
        self.ft.create(MinimalSchema)
        r1 = rand_row_minimal()
        r2 = rand_row_minimal()
        r3 = rand_row_minimal()
        self.ft.insert(r1, r2, r3)
        self.assertEqual(self.ft.count(), 3)
        fetched = self.ft.fetch(order_by={'id': False})
        self.assertEqual(fetched[0], r1)
        self.assertEqual(fetched[1], r2)
        self.assertEqual(fetched[2], r3)

    # ===================== Repr =====================

    def test_repr(self):
        self.assertIn(self.table_name, repr(self.ft))


class TestSQLiteTableCompositePK(unittest.TestCase):
    """Tests for tables with composite primary keys."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_cpk"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(CompositePKSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_insert_fetch(self):
        self.ft.insert({'group_id': 1, 'item_id': 1, 'value': 'a'})
        self.ft.insert({'group_id': 1, 'item_id': 2, 'value': 'b'})
        self.ft.insert({'group_id': 2, 'item_id': 1, 'value': 'c'})
        self.assertEqual(self.ft.count(), 3)

    def test_duplicate_composite_pk_raises(self):
        self.ft.insert({'group_id': 1, 'item_id': 1, 'value': 'a'})
        with self.assertRaises(Exception):
            self.ft.insert({'group_id': 1, 'item_id': 1, 'value': 'dup'})

    def test_replace_composite_pk(self):
        self.ft.insert({'group_id': 1, 'item_id': 1, 'value': 'old'})
        self.ft.insert({'group_id': 1, 'item_id': 1, 'value': 'new'}, replace=True)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0]['value'], 'new')

    def test_replace_many_composite_pk(self):
        self.ft.insert(
            {'group_id': 1, 'item_id': 1, 'value': 'a'},
            {'group_id': 1, 'item_id': 2, 'value': 'b'},
        )
        self.ft.insert(
            {'group_id': 1, 'item_id': 1, 'value': 'a_v2'},
            {'group_id': 2, 'item_id': 1, 'value': 'c'},
            replace=True,
        )
        self.assertEqual(self.ft.count(), 3)
        fetched = self.ft.fetch(where={'group_id': 1, 'item_id': 1})
        self.assertEqual(fetched[0]['value'], 'a_v2')

    def test_where_partial_pk(self):
        self.ft.insert(
            {'group_id': 1, 'item_id': 1, 'value': 'a'},
            {'group_id': 1, 'item_id': 2, 'value': 'b'},
            {'group_id': 2, 'item_id': 1, 'value': 'c'},
        )
        fetched = self.ft.fetch(where={'group_id': 1})
        self.assertEqual(len(fetched), 2)

    def test_keys(self):
        self.assertEqual(self.ft.keys(), ['group_id', 'item_id', 'value'])


class TestSQLiteTableInsertPyMode(unittest.TestCase):
    """Test insert + fetch round-trip for all Python-mode types."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_pymode"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(PyModeSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_insert_fetch_single(self):
        row = rand_row_py()
        self.ft.insert(row)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self._assert_row_equal(fetched[0], row)

    def test_insert_fetch_many(self):
        rows = [rand_row_py() for _ in range(5)]
        self.ft.insert(*rows)
        fetched = self.ft.fetch(order_by={'id': False})
        self.assertEqual(len(fetched), 5)
        for expected, actual in zip(rows, fetched):
            self._assert_row_equal(actual, expected)

    def test_insert_replace(self):
        row = rand_row_py()
        self.ft.insert(row)
        updated_row = {**row, 'username': 'replaced_user'}
        self.ft.insert(updated_row, replace=True)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0]['username'], 'replaced_user')

    def test_fetch_with_keys(self):
        row = rand_row_py()
        self.ft.insert(row)
        fetched = self.ft.fetch(keys=['id', 'username'])
        self.assertEqual(len(fetched), 1)
        self.assertEqual(set(fetched[0].keys()), {'id', 'username'})
        self.assertEqual(fetched[0]['id'], row['id'])
        self.assertEqual(fetched[0]['username'], row['username'])

    def test_fetch_with_where(self):
        rows = [rand_row_py() for _ in range(3)]
        self.ft.insert(*rows)
        target = rows[1]
        fetched = self.ft.fetch(where={'id': target['id']})
        self.assertEqual(len(fetched), 1)
        self._assert_row_equal(fetched[0], target)

    def test_fetch_with_order_by_asc(self):
        rows = [rand_row_py() for _ in range(5)]
        self.ft.insert(*rows)
        fetched = self.ft.fetch(order_by={'id': False})
        ids = [r['id'] for r in fetched]
        self.assertEqual(ids, sorted(ids))

    def test_fetch_with_order_by_desc(self):
        rows = [rand_row_py() for _ in range(5)]
        self.ft.insert(*rows)
        fetched = self.ft.fetch(order_by={'id': True})
        ids = [r['id'] for r in fetched]
        self.assertEqual(ids, sorted(ids, reverse=True))

    def test_fetch_with_limit(self):
        rows = [rand_row_py() for _ in range(5)]
        self.ft.insert(*rows)
        fetched = self.ft.fetch(order_by={'id': False}, limit=2)
        self.assertEqual(len(fetched), 2)

    def test_fetch_with_offset(self):
        rows = [rand_row_py() for _ in range(5)]
        self.ft.insert(*rows)
        fetched = self.ft.fetch(order_by={'id': False}, limit=2, offset=3)
        self.assertEqual(len(fetched), 2)
        self._assert_row_equal(fetched[0], rows[3])

    def _assert_row_equal(self, actual, expected):
        for key in expected:
            self.assertEqual(actual[key], expected[key], msg=f"Mismatch on key={key}")


class TestSQLiteTableInsertSQLMode(unittest.TestCase):
    """Test insert + fetch round-trip for SQLite-compatible SQL-mode types."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_sqlmode"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(SQLiteSQLModeSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_insert_fetch_single(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self._assert_row_equal(fetched[0], row)

    def test_insert_fetch_many(self):
        rows = [rand_row_sqlite_sql() for _ in range(5)]
        self.ft.insert(*rows)
        fetched = self.ft.fetch(order_by={'id': False})
        self.assertEqual(len(fetched), 5)
        for expected, actual in zip(rows, fetched):
            self._assert_row_equal(actual, expected)

    def test_insert_replace_sql(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        updated = {**row, 'name': 'replaced_name', 'raw_data': {'theme': 'light'}}
        self.ft.insert(updated, replace=True)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0]['name'], 'replaced_name')
        self.assertEqual(fetched[0]['raw_data'], {'theme': 'light'})

    def test_type_smallint(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['small_val'], int)
        self.assertEqual(fetched['small_val'], row['small_val'])

    def test_type_bigint(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['big_val'], int)
        self.assertEqual(fetched['big_val'], row['big_val'])

    def test_type_float(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['float_val'], float)
        self.assertAlmostEqual(fetched['float_val'], row['float_val'], places=4)

    def test_type_numeric(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        # SQLite returns float for NUMERIC (unlike PG which returns Decimal)
        self.assertAlmostEqual(float(fetched['numeric_val']), row['numeric_val'], places=2)

    def test_type_boolean(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['is_active'], bool)
        self.assertEqual(fetched['is_active'], row['is_active'])

    def test_type_json(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['raw_data'], dict)
        self.assertEqual(fetched['raw_data'], row['raw_data'])

    def test_type_blob(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(fetched['avatar'], row['avatar'])

    def test_type_date(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(fetched['birthday'], row['birthday'])

    def test_type_time(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(fetched['alarm'], row['alarm'])

    def test_type_datetime(self):
        row = rand_row_sqlite_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(fetched['created_at'], row['created_at'])

    def _assert_row_equal(self, actual, expected):
        """Loose comparison for SQLite type differences."""
        for key in expected:
            a, e = actual[key], expected[key]
            if key == 'numeric_val':
                self.assertAlmostEqual(float(a), float(e), places=2, msg=f"key={key}")
            elif key == 'float_val':
                self.assertAlmostEqual(float(a), float(e), places=4, msg=f"key={key}")
            else:
                self.assertEqual(a, e, msg=f"key={key}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
