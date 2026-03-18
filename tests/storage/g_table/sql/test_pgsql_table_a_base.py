import unittest

from gatling.storage.g_table.sql.real_pgsql_table import PGSQLTable
from gatling.storage.g_table.sql.a_pgsql_base import create_pool
from storage.g_table.sql.a_const_test import (
    CONNINFO, MinimalSchema, PyModeSchema, SQLModeSchema,
    rand_row_minimal, rand_row_py, rand_row_sql,
    reset_counter, skip_pgsql,
)


@skip_pgsql
class TestPGSQLTableBase(unittest.TestCase):
    """Tests for table lifecycle: create, exists, drop, truncate, keys."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_base"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()

    def tearDown(self):
        self.ft.drop()

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
        self.ft.create(SQLModeSchema)
        expected = ['id', 'small_val', 'big_val', 'float_val', 'numeric_val',
                    'double_val', 'name', 'bio', 'is_active', 'birthday',
                    'alarm', 'created_at', 'duration', 'settings', 'raw_data',
                    'token', 'avatar', 'login_ip', 'network', 'device_mac']
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

    # ===================== Repr =====================

    def test_repr(self):
        self.assertIn(self.table_name, repr(self.ft))


@skip_pgsql
class TestPGSQLTableInsertPyMode(unittest.TestCase):
    """Test insert + fetch round-trip for all Python-mode types."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_pymode"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()
        self.ft.create(PyModeSchema)

    def tearDown(self):
        self.ft.drop()

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


@skip_pgsql
class TestPGSQLTableInsertSQLMode(unittest.TestCase):
    """Test insert + fetch round-trip for all SQL-mode (PostgreSQL-specific) types."""

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = "test_pgsql_sqlmode"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()
        self.ft.create(SQLModeSchema)

    def tearDown(self):
        self.ft.drop()

    def test_insert_fetch_single(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self._assert_row_equal(fetched[0], row)

    def test_insert_fetch_many(self):
        rows = [rand_row_sql() for _ in range(5)]
        self.ft.insert(*rows)
        fetched = self.ft.fetch(order_by={'id': False})
        self.assertEqual(len(fetched), 5)
        for expected, actual in zip(rows, fetched):
            self._assert_row_equal(actual, expected)

    def test_insert_replace_sql(self):
        row = rand_row_sql()
        self.ft.insert(row)
        updated = {**row, 'name': 'replaced_name', 'settings': {'theme': 'light'}}
        self.ft.insert(updated, replace=True)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0]['name'], 'replaced_name')
        self.assertEqual(fetched[0]['settings'], {'theme': 'light'})

    def test_type_smallint(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['small_val'], int)
        self.assertEqual(fetched['small_val'], row['small_val'])

    def test_type_bigint(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['big_val'], int)
        self.assertEqual(fetched['big_val'], row['big_val'])

    def test_type_numeric(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        from decimal import Decimal
        self.assertIsInstance(fetched['numeric_val'], Decimal)
        self.assertAlmostEqual(float(fetched['numeric_val']), row['numeric_val'], places=2)

    def test_type_double(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['double_val'], float)
        self.assertAlmostEqual(fetched['double_val'], row['double_val'], places=5)

    def test_type_jsonb(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['settings'], dict)
        self.assertEqual(fetched['settings'], row['settings'])

    def test_type_json(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIsInstance(fetched['raw_data'], dict)
        self.assertEqual(fetched['raw_data'], row['raw_data'])

    def test_type_uuid(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(str(fetched['token']), row['token'])

    def test_type_bytea(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(bytes(fetched['avatar']), row['avatar'])

    def test_type_inet(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(str(fetched['login_ip']), row['login_ip'])

    def test_type_cidr(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertIn('/', str(fetched['network']))

    def test_type_macaddr(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(str(fetched['device_mac']).lower().replace('-', ':'),
                         row['device_mac'].lower().replace('-', ':'))

    def test_type_date(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(fetched['birthday'], row['birthday'])

    def test_type_time(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(fetched['alarm'], row['alarm'])

    def test_type_timestamp_tz(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        expected = row['created_at']
        actual = fetched['created_at']
        self.assertEqual(actual.replace(tzinfo=None), expected.replace(tzinfo=None))

    def test_type_interval(self):
        row = rand_row_sql()
        self.ft.insert(row)
        fetched = self.ft.fetch()[0]
        self.assertEqual(fetched['duration'], row['duration'])

    def _assert_row_equal(self, actual, expected):
        """Loose comparison that handles type coercion (Decimal, UUID, memoryview, etc.)."""
        for key in expected:
            a, e = actual[key], expected[key]
            if key == 'numeric_val':
                self.assertAlmostEqual(float(a), float(e), places=2, msg=f"key={key}")
            elif key == 'double_val':
                self.assertAlmostEqual(float(a), float(e), places=5, msg=f"key={key}")
            elif key == 'token':
                self.assertEqual(str(a), str(e), msg=f"key={key}")
            elif key == 'avatar':
                self.assertEqual(bytes(a), bytes(e), msg=f"key={key}")
            elif key == 'login_ip':
                self.assertEqual(str(a), str(e), msg=f"key={key}")
            elif key == 'network':
                self.assertIn('/', str(a), msg=f"key={key}")
            elif key == 'device_mac':
                self.assertEqual(str(a).lower().replace('-', ':'),
                                 str(e).lower().replace('-', ':'), msg=f"key={key}")
            elif key == 'created_at':
                self.assertEqual(a.replace(tzinfo=None), e.replace(tzinfo=None), msg=f"key={key}")
            else:
                self.assertEqual(a, e, msg=f"key={key}")


@skip_pgsql
class TestPGSQLTableSchema(unittest.TestCase):
    """Tests for schema-qualified table names (e.g. 'myschema.mytable')."""

    TEST_SCHEMA = "test_gatling_schema"

    @classmethod
    def setUpClass(cls):
        cls.pool = create_pool(CONNINFO, max_size=5)

    @classmethod
    def tearDownClass(cls):
        # clean up the test schema
        with cls.pool.connection() as conn:
            conn.execute(f'DROP SCHEMA IF EXISTS "{cls.TEST_SCHEMA}" CASCADE')
            conn.commit()
        cls.pool.close()

    def setUp(self):
        reset_counter()
        self.table_name = f"{self.TEST_SCHEMA}.test_schema_table"
        self.ft = PGSQLTable(self.table_name, self.pool)
        self.ft.drop()

    def tearDown(self):
        self.ft.drop()

    # ===================== Parsing =====================

    def test_parse_schema_qualified(self):
        ft = PGSQLTable("myschema.mytable", self.pool)
        self.assertEqual(ft._schema, "myschema")
        self.assertEqual(ft._bare_table, "mytable")

    def test_parse_plain_name(self):
        ft = PGSQLTable("mytable", self.pool)
        self.assertIsNone(ft._schema)
        self.assertEqual(ft._bare_table, "mytable")

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
        self.ft.insert(rand_row_minimal())
        self.assertEqual(self.ft.count(), 1)
        self.ft.truncate()
        self.assertEqual(self.ft.count(), 0)
        self.assertTrue(self.ft.exists())

    def test_keys(self):
        self.ft.create(MinimalSchema)
        self.assertEqual(self.ft.keys(), ['id', 'name'])

    # ===================== CRUD =====================

    def test_insert_fetch(self):
        self.ft.create(MinimalSchema)
        row = rand_row_minimal()
        self.ft.insert(row)
        fetched = self.ft.fetch()
        self.assertEqual(len(fetched), 1)
        self.assertEqual(fetched[0], row)

    def test_insert_multi(self):
        self.ft.create(MinimalSchema)
        rows = [rand_row_minimal() for _ in range(5)]
        self.ft.insert(*rows)
        fetched = self.ft.fetch(order_by={'id': False})
        self.assertEqual(len(fetched), 5)
        for expected, actual in zip(rows, fetched):
            self.assertEqual(actual, expected)

    def test_count(self):
        self.ft.create(MinimalSchema)
        self.assertEqual(self.ft.count(), 0)
        self.ft.insert(rand_row_minimal())
        self.assertEqual(self.ft.count(), 1)

    def test_update(self):
        self.ft.create(MinimalSchema)
        row = rand_row_minimal()
        self.ft.insert(row)
        self.ft.update({'name': 'updated'}, {'id': row['id']})
        fetched = self.ft.fetch()
        self.assertEqual(fetched[0]['name'], 'updated')

    def test_delete(self):
        self.ft.create(MinimalSchema)
        row = rand_row_minimal()
        self.ft.insert(row)
        self.ft.delete({'id': row['id']})
        self.assertEqual(self.ft.count(), 0)

    # ===================== Context Manager =====================

    def test_ctxt_insert_commit(self):
        self.ft.create(MinimalSchema)
        with self.ft:
            self.ft.insert(rand_row_minimal())
        self.assertEqual(self.ft.count(), 1)

    def test_ctxt_insert_rollback(self):
        self.ft.create(MinimalSchema)
        try:
            with self.ft:
                self.ft.insert(rand_row_minimal())
                raise RuntimeError("force rollback")
        except RuntimeError:
            pass
        self.assertEqual(self.ft.count(), 0)

    # ===================== Isolation =====================

    def test_schema_does_not_clash_with_public(self):
        """Table in custom schema does not conflict with same-named table in public."""
        public_ft = PGSQLTable("test_schema_table", self.pool)
        public_ft.drop()
        try:
            public_ft.create(MinimalSchema)
            self.ft.create(MinimalSchema)

            public_ft.insert({'id': 1, 'name': 'public'})
            self.ft.insert({'id': 1, 'name': 'schema'})

            self.assertEqual(public_ft.fetch()[0]['name'], 'public')
            self.assertEqual(self.ft.fetch()[0]['name'], 'schema')
        finally:
            public_ft.drop()

    def test_repr(self):
        self.assertIn(self.table_name, repr(self.ft))


if __name__ == "__main__":
    unittest.main(verbosity=2)
