import unittest

from gatling.storage.g_table.sql.real_pgsql_table import PGSQLTable
from storage.g_table.sql.a_const_test import (
    CONNINFO, MinimalSchema,
    rand_row_minimal, reset_counter, create_pool, skip_pgsql,
)


@skip_pgsql
class TestPGSQLTableContext(unittest.TestCase):
    """Tests for context manager behavior (transaction semantics)."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_ctxt"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()
        self.ft.create(MinimalSchema)

    def tearDown(self):
        self.ft.drop()

    def test_ctxt_insert_commit(self):
        """Insert inside context manager should be committed on normal exit."""
        row = rand_row_minimal()
        with self.ft:
            self.ft.insert(row)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0], row)

    def test_ctxt_insert_rollback_on_exception(self):
        """Insert inside context manager should be rolled back on exception."""
        row = rand_row_minimal()
        try:
            with self.ft:
                self.ft.insert(row)
                raise ValueError("deliberate error")
        except ValueError:
            pass
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 0)

    def test_ctxt_multiple_inserts_commit(self):
        """Multiple inserts inside context should all commit together."""
        rows = [rand_row_minimal() for _ in range(3)]
        with self.ft:
            for row in rows:
                self.ft.insert(row)
        fetched = self.ft.fetch(order_by={'id': False})
        self.assertEqual(len(fetched), 3)
        for expected, actual in zip(rows, fetched):
            self.assertEqual(actual, expected)

    def test_ctxt_multiple_inserts_rollback(self):
        """Multiple inserts inside context should all rollback on exception."""
        rows = [rand_row_minimal() for _ in range(3)]
        try:
            with self.ft:
                for row in rows:
                    self.ft.insert(row)
                raise RuntimeError("rollback test")
        except RuntimeError:
            pass
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 0)

    def test_ctxt_fetch_inside(self):
        """Fetch inside context should see uncommitted data from same transaction."""
        row = rand_row_minimal()
        with self.ft:
            self.ft.insert(row)
            fetched = self.ft.fetch()
            self.assertEqual(len(fetched), 1)
            self.assertEqual(fetched[0], row)

    def test_ctxt_count_inside(self):
        """Count inside context should reflect uncommitted inserts."""
        rows = [rand_row_minimal() for _ in range(3)]
        with self.ft:
            for row in rows:
                self.ft.insert(row)
            self.assertEqual(self.ft.count(), 3)

    def test_ctxt_update_commit(self):
        """Update inside context should be committed on normal exit."""
        row = rand_row_minimal()
        self.ft.insert(row)

        with self.ft:
            self.ft.update({'name': 'updated'}, {'id': row['id']})

        fetched = self.ft.fetch(where={'id': row['id']})
        self.assertEqual(fetched[0]['name'], 'updated')

    def test_ctxt_update_rollback(self):
        """Update inside context should be rolled back on exception."""
        row = rand_row_minimal()
        self.ft.insert(row)

        try:
            with self.ft:
                self.ft.update({'name': 'updated'}, {'id': row['id']})
                raise RuntimeError("rollback")
        except RuntimeError:
            pass

        fetched = self.ft.fetch(where={'id': row['id']})
        self.assertEqual(fetched[0]['name'], row['name'])

    def test_ctxt_delete_commit(self):
        """Delete inside context should be committed on normal exit."""
        row = rand_row_minimal()
        self.ft.insert(row)

        with self.ft:
            self.ft.delete({'id': row['id']})

        self.assertEqual(self.ft.count(), 0)

    def test_ctxt_delete_rollback(self):
        """Delete inside context should be rolled back on exception."""
        row = rand_row_minimal()
        self.ft.insert(row)

        try:
            with self.ft:
                self.ft.delete({'id': row['id']})
                raise RuntimeError("rollback")
        except RuntimeError:
            pass

        self.assertEqual(self.ft.count(), 1)

    def test_ctxt_mixed_operations(self):
        """Mixed insert + update + delete inside context, commit on success."""
        r1 = rand_row_minimal()
        r2 = rand_row_minimal()
        self.ft.insert(r1)

        with self.ft:
            self.ft.insert(r2)
            self.ft.update({'name': 'changed'}, {'id': r1['id']})

        fetched = self.ft.fetch(order_by={'id': False})
        self.assertEqual(len(fetched), 2)
        self.assertEqual(fetched[0]['name'], 'changed')
        self.assertEqual(fetched[1], r2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
