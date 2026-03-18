import datetime
import unittest

from enum import auto
from gatling.define.constdefine import ConstDefine
from gatling.define.tabledefine import TableDefine, Field

from sqlalchemy import Integer, SmallInteger, BigInteger, Float, Numeric, Boolean
from sqlalchemy import String, Text, LargeBinary, Date, Time, DateTime, Interval
from sqlalchemy.dialects.postgresql import JSONB, JSON, UUID, BYTEA, INET, DOUBLE_PRECISION


# ===================== ConstDefine Schemas =====================

class AppConfig(ConstDefine):
    Port      = 8080
    Name      = "my_app"
    Rate      = 0.001
    Debug     = False
    username  = auto()
    email     = auto()
    Single    = None


class TestConstDefine(unittest.TestCase):

    def test_value(self):
        self.assertEqual(AppConfig.Port.value, 8080)
        self.assertEqual(AppConfig.Name.value, "my_app")
        self.assertEqual(AppConfig.Rate.value, 0.001)
        self.assertEqual(AppConfig.Debug.value, False)

    def test_auto_value_is_name(self):
        self.assertEqual(AppConfig.username.value, "username")
        self.assertEqual(AppConfig.email.value, "email")

    def test_none_value(self):
        self.assertIsNone(AppConfig.Single.value)
        self.assertEqual(AppConfig.Single.name, "Single")
        self.assertEqual(str(AppConfig.Single), "Single")

    def test_str(self):
        self.assertEqual(str(AppConfig.Port), "Port")
        self.assertEqual(str(AppConfig.username), "username")

    def test_contains(self):
        self.assertIn("Port", AppConfig)
        self.assertIn("username", AppConfig)
        self.assertNotIn("nope", AppConfig)

    def test_keys(self):
        self.assertEqual(AppConfig.keys(), ['Port', 'Name', 'Rate', 'Debug', 'username', 'email', 'Single'])

    def test_items(self):
        items = AppConfig.items()
        self.assertEqual(items[0], ('Port', 8080))
        self.assertEqual(items[1], ('Name', "my_app"))
        self.assertEqual(items[4], ('username', "username"))

    def test_dict_from_items(self):
        d = dict(AppConfig.items())
        self.assertEqual(d['Port'], 8080)
        self.assertEqual(d['username'], "username")

    def test_dict_direct(self):
        d = dict(AppConfig)
        self.assertEqual(d['Port'], 8080)
        self.assertEqual(d['username'], "username")
        self.assertIsNone(d['Single'])

    def test_getitem(self):
        self.assertEqual(AppConfig['Port'], 8080)
        self.assertEqual(AppConfig['username'], "username")

    def test_get(self):
        self.assertEqual(AppConfig.get('Port'), 8080)
        self.assertIsNone(AppConfig.get('nope'))
        self.assertEqual(AppConfig.get('nope', 'fallback'), 'fallback')


# ===================== TableDefine Schemas =====================

class ConstSchema(TableDefine):
    AppName   = Field(str, default="my_app")
    Port      = Field(int, default=8080)
    Lr        = Field(float, default=0.001)
    Debug     = Field(bool, default=False)
    StartDate = Field(datetime.date, default=datetime.date(2025, 1, 1))
    AlarmTime = Field(datetime.time, default=datetime.time(8, 0))
    CreatedAt = Field(datetime.datetime, default=datetime.datetime(2025, 1, 1, 12, 0))


class TableSchema(TableDefine):
    id   = Field(int, primary=True)
    name = Field(str)
    score = Field(float)


class SQLModeSchema(TableDefine):
    id       = Field(Integer, primary=True)
    age      = Field(SmallInteger, default=0)
    count    = Field(BigInteger)
    price    = Field(Numeric(10, 2))
    rating   = Field(Float)
    double   = Field(DOUBLE_PRECISION)
    label    = Field(String(64))
    bio      = Field(Text)
    active   = Field(Boolean, default=True)
    birthday = Field(Date)
    alarm    = Field(Time)
    created  = Field(DateTime)
    dur      = Field(Interval)
    settings = Field(JSONB)
    raw      = Field(JSON)
    token    = Field(UUID)
    avatar   = Field(BYTEA)
    ip       = Field(INET)


class CompositePK(TableDefine):
    group_id = Field(int, primary=True)
    item_id  = Field(int, primary=True)
    value    = Field(str)


class EmptyDefault(TableDefine):
    name = Field(str)
    num  = Field(int)
    flag = Field(bool)


# ===================== TableDefine Tests =====================

class TestTableDefineBasic(unittest.TestCase):

    def test_keys(self):
        self.assertEqual(ConstSchema.keys(), [
            'AppName', 'Port', 'Lr', 'Debug', 'StartDate', 'AlarmTime', 'CreatedAt',
        ])

    def test_items(self):
        items = ConstSchema.items()
        d = dict(items)
        self.assertEqual(d['AppName'].default, "my_app")
        self.assertEqual(d['Port'].default, 8080)
        self.assertEqual(d['Lr'].default, 0.001)
        self.assertEqual(d['Debug'].default, False)
        self.assertEqual(d['StartDate'].default, datetime.date(2025, 1, 1))

    def test_access_by_name(self):
        self.assertEqual(ConstSchema['Port'].default, 8080)
        self.assertEqual(ConstSchema['AppName'].default, "my_app")

    def test_access_by_attr(self):
        self.assertEqual(ConstSchema.Port.value.default, 8080)
        self.assertEqual(ConstSchema.AppName.value.default, "my_app")

    def test_contains(self):
        self.assertIn("Port", ConstSchema)
        self.assertNotIn("NotExist", ConstSchema)

    def test_get(self):
        self.assertEqual(ConstSchema.get('Port').default, 8080)
        self.assertIsNone(ConstSchema.get('NotExist'))
        self.assertEqual(ConstSchema.get('NotExist', 'fallback'), 'fallback')

    def test_len(self):
        self.assertEqual(len(ConstSchema), 7)

    def test_iter(self):
        names = [m.name for m in ConstSchema]
        self.assertEqual(names[0], 'AppName')
        self.assertEqual(names[1], 'Port')

    def test_dtype(self):
        self.assertEqual(ConstSchema.Port.value.dtype, int)
        self.assertEqual(ConstSchema.AppName.value.dtype, str)
        self.assertEqual(ConstSchema.Debug.value.dtype, bool)
        self.assertEqual(ConstSchema.StartDate.value.dtype, datetime.date)

    def test_tostr(self):
        self.assertEqual(ConstSchema.Port.value.tostr(8080), "8080")
        self.assertEqual(ConstSchema.Debug.value.tostr(True), "1")
        self.assertEqual(ConstSchema.StartDate.value.tostr(datetime.date(2025, 6, 15)), "2025-06-15")

    def test_fmstr(self):
        self.assertEqual(ConstSchema.Port.value.fmstr("8080"), 8080)
        self.assertEqual(ConstSchema.Debug.value.fmstr("0"), False)
        self.assertEqual(ConstSchema.StartDate.value.fmstr("2025-06-15"), datetime.date(2025, 6, 15))

    def test_nullable(self):
        self.assertTrue(ConstSchema.AppName.value.nullable)

    def test_reject_non_field(self):
        with self.assertRaises(TypeError):
            class Bad(TableDefine):
                name = "hello"


# ===================== Field Properties =====================

class TestFieldProperties(unittest.TestCase):

    def test_py_mode(self):
        f = Field(int, default=0)
        self.assertEqual(f.mode, "py")
        self.assertEqual(f.dtype, int)

    def test_sql_mode(self):
        f = Field(Integer)
        self.assertEqual(f.mode, "sql")
        self.assertEqual(f.dtype, int)

    def test_sql_mode_parametrized(self):
        f = Field(Numeric(10, 2))
        self.assertEqual(f.mode, "sql")
        self.assertEqual(f.dtype, float)

    def test_primary(self):
        f = Field(int, primary=True)
        self.assertTrue(f.primary)

    def test_default_auto(self):
        """Fields without explicit default get type-based defaults."""
        self.assertEqual(Field(str).default, "")
        self.assertEqual(Field(int).default, 0)
        self.assertEqual(Field(float).default, 0.0)
        self.assertEqual(Field(bool).default, False)

    def test_default_explicit(self):
        f = Field(int, default=42)
        self.assertEqual(f.default, 42)

    def test_nullable(self):
        f1 = Field(str, nullable=True)
        f2 = Field(str, nullable=False)
        self.assertTrue(f1.nullable)
        self.assertFalse(f2.nullable)

    def test_repr(self):
        f = Field(int, default=0, primary=True)
        r = repr(f)
        self.assertIn("int", r)
        self.assertIn("primary=True", r)


# ===================== Schema for Tables =====================

class TestTableDefineAsSchema(unittest.TestCase):

    def test_keys(self):
        self.assertEqual(TableSchema.keys(), ['id', 'name', 'score'])

    def test_get_name2dtype(self):
        k2t = TableSchema.get_name2dtype()
        self.assertEqual(k2t, {'id': int, 'name': str, 'score': float})

    def test_primary_detection(self):
        primaries = [m.name for m in TableSchema if m.value.primary]
        self.assertEqual(primaries, ['id'])

    def test_composite_pk(self):
        primaries = [m.name for m in CompositePK if m.value.primary]
        self.assertEqual(primaries, ['group_id', 'item_id'])

    def test_get_sql_table(self):
        t = TableSchema.get_sql_table()
        col_names = [c.name for c in t.columns]
        self.assertEqual(col_names, ['id', 'name', 'score'])

    def test_get_sql_create(self):
        sql = TableSchema.get_sql_create()
        self.assertIn("CREATE TABLE", sql)
        self.assertIn("id", sql)

    def test_get_sql_drop(self):
        sql = TableSchema.get_sql_drop()
        self.assertIn("DROP TABLE", sql)

    def test_defaults_in_items(self):
        d = dict(EmptyDefault.items())
        self.assertEqual(d['name'].default, "")
        self.assertEqual(d['num'].default, 0)
        self.assertEqual(d['flag'].default, False)


# ===================== SQL Mode Types =====================

class TestSQLModeTypes(unittest.TestCase):

    def test_integer_types(self):
        self.assertEqual(SQLModeSchema.id.value.dtype, int)
        self.assertEqual(SQLModeSchema.age.value.dtype, int)
        self.assertEqual(SQLModeSchema.count.value.dtype, int)

    def test_float_types(self):
        self.assertEqual(SQLModeSchema.price.value.dtype, float)
        self.assertEqual(SQLModeSchema.rating.value.dtype, float)
        self.assertEqual(SQLModeSchema.double.value.dtype, float)

    def test_string_types(self):
        self.assertEqual(SQLModeSchema.label.value.dtype, str)
        self.assertEqual(SQLModeSchema.bio.value.dtype, str)

    def test_bool_type(self):
        self.assertEqual(SQLModeSchema.active.value.dtype, bool)

    def test_date_types(self):
        self.assertEqual(SQLModeSchema.birthday.value.dtype, datetime.date)
        self.assertEqual(SQLModeSchema.alarm.value.dtype, datetime.time)
        self.assertEqual(SQLModeSchema.created.value.dtype, datetime.datetime)
        self.assertEqual(SQLModeSchema.dur.value.dtype, datetime.timedelta)

    def test_json_types(self):
        self.assertEqual(SQLModeSchema.settings.value.dtype, dict)
        self.assertEqual(SQLModeSchema.raw.value.dtype, dict)

    def test_binary_types(self):
        self.assertEqual(SQLModeSchema.avatar.value.dtype, bytes)

    def test_network_types(self):
        self.assertEqual(SQLModeSchema.ip.value.dtype, str)
        self.assertEqual(SQLModeSchema.token.value.dtype, str)

    def test_sql_table_columns(self):
        t = SQLModeSchema.get_sql_table()
        col_names = [c.name for c in t.columns]
        self.assertEqual(len(col_names), len(SQLModeSchema))


if __name__ == "__main__":
    unittest.main(verbosity=2)
