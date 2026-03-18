import unittest

from gatling.storage.g_table.sql.real_pgsql_table import PGSQLTable
from storage.g_table.sql.a_const_test import (
    CONNINFO, MinimalSchema, PyModeSchema,
    rand_row_minimal, rand_row_py,
    reset_counter, create_pool, skip_pgsql,
)


@skip_pgsql
class TestPGSQLTableUpdate(unittest.TestCase):
    """Tests for update operations."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_crud"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()

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


@skip_pgsql
class TestPGSQLTableDelete(unittest.TestCase):
    """Tests for delete operations."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_del"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()

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


@skip_pgsql
class TestPGSQLTableFetchAdvanced(unittest.TestCase):
    """Tests for advanced fetch operations: where, order_by, limit, offset, keys."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_fetch"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()
        self.ft.create(MinimalSchema)
        self.rows = []
        for i in range(10):
            row = {'id': i + 1, 'name': f'user_{i:02d}'}
            self.rows.append(row)
            self.ft.insert(row)

    def tearDown(self):
        self.ft.drop()

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


@skip_pgsql
class TestPGSQLTableInsertReplace(unittest.TestCase):
    """Tests for insert with replace (ON CONFLICT DO UPDATE)."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_replace"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()

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


@skip_pgsql
class TestPGSQLTableInsertBatch(unittest.TestCase):
    """Tests for insert with batch_size."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_batch"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()

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


@skip_pgsql
class TestPGSQLTableWhereNull(unittest.TestCase):
    """Tests for NULL handling in where clauses."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_null"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()

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


if __name__ == "__main__":
    unittest.main(verbosity=2)
