import os
import tempfile
import unittest

from gatling.storage.g_table.sql.real_sqlite_table import SQLiteTable
from storage.g_table.sql.a_const_test import (
    MinimalSchema, ThreeColSchema,
    rand_row_minimal, reset_counter,
)


class TestSQLiteTableUpdate(unittest.TestCase):
    """Tests for update operations."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_crud"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_update_single_row(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        affected = self.ft.update({'name': 'Alice_v2'}, {'id': 1})
        self.assertEqual(affected, 1)
        fetched = self.ft.fetch(where={'id': 1})
        self.assertEqual(fetched[0]['name'], 'Alice_v2')

    def test_update_multiple_rows(self):
        self.ft.insert({'id': 1, 'name': 'same'})
        self.ft.insert({'id': 2, 'name': 'same'})
        self.ft.insert({'id': 3, 'name': 'other'})
        affected = self.ft.update({'name': 'changed'}, {'name': 'same'})
        self.assertEqual(affected, 2)
        fetched = self.ft.fetch(where={'name': 'changed'})
        self.assertEqual(len(fetched), 2)

    def test_update_no_match(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        affected = self.ft.update({'name': 'changed'}, {'id': 999})
        self.assertEqual(affected, 0)
        fetched = self.ft.fetch()
        self.assertEqual(fetched[0]['name'], 'Alice')

    def test_update_all_rows(self):
        for i in range(5):
            self.ft.insert({'id': i + 1, 'name': f'user_{i}'})
        affected = self.ft.update({'name': 'all_same'}, {})
        self.assertEqual(affected, 5)
        fetched = self.ft.fetch()
        for row in fetched:
            self.assertEqual(row['name'], 'all_same')


class TestSQLiteTableDelete(unittest.TestCase):
    """Tests for delete operations."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_del"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_delete_single_row(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        self.ft.insert({'id': 2, 'name': 'Bob'})
        affected = self.ft.delete({'id': 1})
        self.assertEqual(affected, 1)
        self.assertEqual(self.ft.count(), 1)
        fetched = self.ft.fetch()
        self.assertEqual(fetched[0]['name'], 'Bob')

    def test_delete_multiple_rows(self):
        self.ft.insert({'id': 1, 'name': 'same'})
        self.ft.insert({'id': 2, 'name': 'same'})
        self.ft.insert({'id': 3, 'name': 'other'})
        affected = self.ft.delete({'name': 'same'})
        self.assertEqual(affected, 2)
        self.assertEqual(self.ft.count(), 1)

    def test_delete_no_match(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        affected = self.ft.delete({'id': 999})
        self.assertEqual(affected, 0)
        self.assertEqual(self.ft.count(), 1)

    def test_delete_empty_where_raises(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        with self.assertRaises(ValueError):
            self.ft.delete({})

    def test_delete_none_where_raises(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        with self.assertRaises(ValueError):
            self.ft.delete(None)


class TestSQLiteTableFetchAdvanced(unittest.TestCase):
    """Tests for advanced fetch operations: where, order_by, limit, offset, keys."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_fetch"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(MinimalSchema)
        self.rows = []
        for i in range(10):
            row = {'id': i + 1, 'name': f'user_{i:02d}'}
            self.rows.append(row)
            self.ft.insert(row)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_fetch_all(self):
        fetched = self.ft.fetch(order_by={'id': False})
        self.assertEqual(len(fetched), 10)
        self.assertEqual(fetched, self.rows)

    def test_fetch_where_single(self):
        fetched = self.ft.fetch(where={'id': 5})
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0], self.rows[4])

    def test_fetch_where_multi_cond(self):
        fetched = self.ft.fetch(where={'id': 5, 'name': 'user_04'})
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0], self.rows[4])

    def test_fetch_where_no_match(self):
        fetched = self.ft.fetch(where={'id': 999})
        self.assertEqual(len(fetched), 0)

    def test_fetch_order_asc(self):
        fetched = self.ft.fetch(order_by={'name': False})
        names = [r['name'] for r in fetched]
        self.assertEqual(names, sorted(names))

    def test_fetch_order_desc(self):
        fetched = self.ft.fetch(order_by={'id': True})
        ids = [r['id'] for r in fetched]
        self.assertEqual(ids, sorted(ids, reverse=True))

    def test_fetch_limit(self):
        fetched = self.ft.fetch(order_by={'id': False}, limit=3)
        self.assertEqual(len(fetched), 3)
        self.assertEqual(fetched, self.rows[:3])

    def test_fetch_offset(self):
        fetched = self.ft.fetch(order_by={'id': False}, limit=3, offset=5)
        self.assertEqual(len(fetched), 3)
        self.assertEqual(fetched, self.rows[5:8])

    def test_fetch_limit_beyond(self):
        fetched = self.ft.fetch(order_by={'id': False}, limit=100)
        self.assertEqual(len(fetched), 10)

    def test_fetch_offset_beyond(self):
        fetched = self.ft.fetch(order_by={'id': False}, offset=100)
        self.assertEqual(len(fetched), 0)

    def test_fetch_keys_single(self):
        fetched = self.ft.fetch(keys=['name'], order_by={'id': False})
        self.assertEqual(len(fetched), 10)
        for row in fetched:
            self.assertEqual(set(row.keys()), {'name'})

    def test_fetch_keys_multiple(self):
        fetched = self.ft.fetch(keys=['id', 'name'], order_by={'id': False})
        self.assertEqual(len(fetched), 10)
        for i, row in enumerate(fetched):
            self.assertEqual(set(row.keys()), {'id', 'name'})
            self.assertEqual(row['id'], self.rows[i]['id'])

    def test_fetch_combined(self):
        """Combine where + order_by + limit + offset + keys."""
        self.ft.truncate()
        for i in range(10):
            name = 'group_a' if i < 6 else 'group_b'
            self.ft.insert({'id': i + 1, 'name': name})

        fetched = self.ft.fetch(
            where={'name': 'group_a'},
            keys=['id'],
            order_by={'id': True},
            limit=3,
            offset=1,
        )
        self.assertEqual(len(fetched), 3)
        ids = [r['id'] for r in fetched]
        # group_a has ids 1-6, desc order: 6,5,4,3,2,1, offset=1 -> 5,4,3
        self.assertEqual(ids, [5, 4, 3])


class TestSQLiteTableInsertReplace(unittest.TestCase):
    """Tests for insert with replace (ON CONFLICT DO UPDATE)."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_replace"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_replace_update_existing(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        self.ft.insert({'id': 1, 'name': 'Alice_v2'}, replace=True)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0]['name'], 'Alice_v2')

    def test_replace_insert_new(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        self.ft.insert({'id': 2, 'name': 'Bob'}, replace=True)
        self.assertEqual(self.ft.count(), 2)

    def test_replace_many(self):
        self.ft.insert(
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'},
        )
        self.ft.insert(
            {'id': 1, 'name': 'Alice_v2'},
            {'id': 3, 'name': 'Charlie'},
            replace=True,
        )
        self.assertEqual(self.ft.count(), 3)
        fetched = self.ft.fetch(where={'id': 1})
        self.assertEqual(fetched[0]['name'], 'Alice_v2')

    def test_insert_duplicate_without_replace_raises(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        with self.assertRaises(Exception):
            self.ft.insert({'id': 1, 'name': 'Alice_dup'})


class TestSQLiteTableInsertBatch(unittest.TestCase):
    """Tests for insert with batch_size."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_batch"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_insert_small_batch(self):
        rows = [rand_row_minimal() for _ in range(10)]
        self.ft.insert(*rows, batch_size=3)
        self.assertEqual(self.ft.count(), 10)
        fetched = self.ft.fetch(order_by={'id': False})
        for expected, actual in zip(rows, fetched):
            self.assertEqual(actual, expected)

    def test_insert_batch_size_1(self):
        rows = [rand_row_minimal() for _ in range(5)]
        self.ft.insert(*rows, batch_size=1)
        self.assertEqual(self.ft.count(), 5)

    def test_insert_batch_larger_than_data(self):
        rows = [rand_row_minimal() for _ in range(3)]
        self.ft.insert(*rows, batch_size=1000)
        self.assertEqual(self.ft.count(), 3)


class TestSQLiteTableWhereNull(unittest.TestCase):
    """Tests for NULL handling in where clauses."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_null"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_insert_null_value(self):
        self.ft.insert({'id': 1, 'name': None})
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self.assertIsNone(fetched[0]['name'])

    def test_where_is_null(self):
        self.ft.insert({'id': 1, 'name': None})
        self.ft.insert({'id': 2, 'name': 'Bob'})
        fetched = self.ft.fetch(where={'name': None})
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0]['id'], 1)

    def test_count_with_null_where(self):
        self.ft.insert({'id': 1, 'name': None})
        self.ft.insert({'id': 2, 'name': None})
        self.ft.insert({'id': 3, 'name': 'Charlie'})
        self.assertEqual(self.ft.count({'name': None}), 2)

    def test_update_null_to_value(self):
        self.ft.insert({'id': 1, 'name': None})
        self.ft.update({'name': 'filled'}, {'id': 1})
        fetched = self.ft.fetch(where={'id': 1})
        self.assertEqual(fetched[0]['name'], 'filled')

    def test_update_value_to_null(self):
        self.ft.insert({'id': 1, 'name': 'Alice'})
        self.ft.update({'name': None}, {'id': 1})
        fetched = self.ft.fetch(where={'id': 1})
        self.assertIsNone(fetched[0]['name'])


class TestSQLiteTableMultiColumnOrderBy(unittest.TestCase):
    """Tests for multi-column ORDER BY."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_multiorder"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(ThreeColSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_order_by_two_columns(self):
        """ORDER BY group ASC, score DESC."""
        self.ft.insert(
            {'id': 1, 'group': 'B', 'score': 10},
            {'id': 2, 'group': 'A', 'score': 30},
            {'id': 3, 'group': 'A', 'score': 20},
            {'id': 4, 'group': 'B', 'score': 40},
        )
        fetched = self.ft.fetch(order_by={'group': False, 'score': True})
        ids = [r['id'] for r in fetched]
        # group asc: A first, then B. Within each group, score desc.
        # A: 30(id=2), 20(id=3). B: 40(id=4), 10(id=1).
        self.assertEqual(ids, [2, 3, 4, 1])

    def test_order_by_two_columns_reversed(self):
        """ORDER BY group DESC, score ASC."""
        self.ft.insert(
            {'id': 1, 'group': 'B', 'score': 10},
            {'id': 2, 'group': 'A', 'score': 30},
            {'id': 3, 'group': 'A', 'score': 20},
            {'id': 4, 'group': 'B', 'score': 40},
        )
        fetched = self.ft.fetch(order_by={'group': True, 'score': False})
        ids = [r['id'] for r in fetched]
        # group desc: B first, then A. Within each group, score asc.
        # B: 10(id=1), 40(id=4). A: 20(id=3), 30(id=2).
        self.assertEqual(ids, [1, 4, 3, 2])


class TestSQLiteTableMixedNullWhere(unittest.TestCase):
    """Tests for mixed NULL and non-NULL conditions in WHERE."""

    def setUp(self):
        reset_counter()
        self._tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self._tmpdir, "test.db")
        self.table_name = "test_sqlite_mixnull"
        self.ft = SQLiteTable(self.table_name, self.db_path)
        self.ft.drop()
        self.ft.create(ThreeColSchema)

    def tearDown(self):
        self.ft.drop()
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        if os.path.exists(self._tmpdir):
            os.rmdir(self._tmpdir)

    def test_where_null_and_value(self):
        """WHERE group IS NULL AND score = 10."""
        self.ft.insert(
            {'id': 1, 'group': None,  'score': 10},
            {'id': 2, 'group': None,  'score': 20},
            {'id': 3, 'group': 'A',   'score': 10},
        )
        fetched = self.ft.fetch(where={'group': None, 'score': 10})
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0]['id'], 1)

    def test_count_null_and_value(self):
        self.ft.insert(
            {'id': 1, 'group': None,  'score': 10},
            {'id': 2, 'group': None,  'score': 10},
            {'id': 3, 'group': None,  'score': 20},
            {'id': 4, 'group': 'A',   'score': 10},
        )
        self.assertEqual(self.ft.count({'group': None, 'score': 10}), 2)

    def test_delete_null_and_value(self):
        self.ft.insert(
            {'id': 1, 'group': None,  'score': 10},
            {'id': 2, 'group': None,  'score': 20},
            {'id': 3, 'group': 'A',   'score': 10},
        )
        affected = self.ft.delete({'group': None, 'score': 10})
        self.assertEqual(affected, 1)
        self.assertEqual(self.ft.count(), 2)

    def test_update_null_and_value(self):
        self.ft.insert(
            {'id': 1, 'group': None,  'score': 10},
            {'id': 2, 'group': None,  'score': 20},
            {'id': 3, 'group': 'A',   'score': 10},
        )
        affected = self.ft.update({'group': 'filled'}, {'group': None, 'score': 10})
        self.assertEqual(affected, 1)
        fetched = self.ft.fetch(where={'id': 1})
        self.assertEqual(fetched[0]['group'], 'filled')


if __name__ == "__main__":
    unittest.main(verbosity=2)
