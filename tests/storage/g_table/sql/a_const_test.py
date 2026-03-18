import datetime
import json
import uuid
from collections import defaultdict

from sqlalchemy import (
    SmallInteger, Integer, BigInteger, Float, Numeric, Boolean,
    String, Text, Date, Time, DateTime, Interval, LargeBinary,
)
from sqlalchemy.dialects.postgresql import (
    DOUBLE_PRECISION, JSONB, JSON, UUID, BYTEA,
    INET, CIDR, MACADDR, TIMESTAMP,
)

from gatling.define.schema import TableDefine, Field
from gatling.storage.g_table.sql.a_pgsql_base import create_pool
from gatling.utility.rand_tools import (
    rand_bool, rand_uint8, rand_int32, rand_float_01,
    rand_name_en, rand_url, rand_ip, rand_username, rand_password,
    rand_date, rand_time, rand_datetime, rand_mac,
)

import os
import unittest

CONNINFO = os.environ.get("PGSQL_CONNINFO", "postgresql://admin:pass@localhost:5432/test")
SKIP_PGSQL = os.environ.get("SKIP_PGSQL_TESTS", "") == "1"
skip_pgsql = unittest.skipIf(SKIP_PGSQL, "PostgreSQL not available")


# ===================== Schemas =====================

# --- Minimal schema for lifecycle tests ---
MinimalSchema = TableDefine('MinimalSchema', {
    'id':   Field(int, primary=True),
    'name': Field(str),
})

# --- Python-mode schema (all Python types) ---
PyModeSchema = TableDefine('PyModeSchema', {
    'id':          Field(int, primary=True),
    'username':    Field(str),
    'secret':      Field(str),
    'is_active':   Field(bool),
    'level':       Field(int),
    'balance':     Field(float),
    'birthday':    Field(datetime.date),
    'alarm':       Field(datetime.time),
    'created_at':  Field(datetime.datetime),
})

# --- SQL-mode schema (PostgreSQL-specific types) ---
SQLModeSchema = TableDefine('SQLModeSchema', {
    'id':          Field(Integer, primary=True),
    'small_val':   Field(SmallInteger),
    'big_val':     Field(BigInteger),
    'float_val':   Field(Float),
    'numeric_val': Field(Numeric(10, 2)),
    'double_val':  Field(DOUBLE_PRECISION),
    'name':        Field(String(64)),
    'bio':         Field(Text),
    'is_active':   Field(Boolean),
    'birthday':    Field(Date),
    'alarm':       Field(Time),
    'created_at':  Field(TIMESTAMP(timezone=True)),
    'duration':    Field(Interval),
    'settings':    Field(JSONB),
    'raw_data':    Field(JSON),
    'token':       Field(UUID),
    'avatar':      Field(BYTEA),
    'login_ip':    Field(INET),
    'network':     Field(CIDR),
    'device_mac':  Field(MACADDR),
})


# ===================== Random Row Generators =====================

_counter = 0

def _next_id():
    global _counter
    _counter += 1
    return _counter

def reset_counter():
    global _counter
    _counter = 0


def rand_row_py():
    return {
        'id':          _next_id(),
        'username':    rand_username(),
        'secret':      rand_password(),
        'is_active':   rand_bool(),
        'level':       rand_uint8(),
        'balance':     round(rand_float_01() * 1000, 2),
        'birthday':    rand_date(),
        'alarm':       rand_time(),
        'created_at':  rand_datetime(),
    }


def rand_row_sql():
    return {
        'id':          _next_id(),
        'small_val':   rand_uint8(),
        'big_val':     rand_int32(),
        'float_val':   round(rand_float_01() * 100, 4),
        'numeric_val': round(rand_float_01() * 10000, 2),
        'double_val':  round(rand_float_01() * 1000, 6),
        'name':        rand_name_en(),
        'bio':         rand_url(),
        'is_active':   rand_bool(),
        'birthday':    rand_date(),
        'alarm':       rand_time(),
        'created_at':  rand_datetime().replace(tzinfo=datetime.timezone.utc),
        'duration':    datetime.timedelta(seconds=rand_uint8()),
        'settings':    {'theme': 'dark', 'lang': 'en', 'count': rand_uint8()},
        'raw_data':    {'key': rand_username(), 'values': [1, 2, 3]},
        'token':       str(uuid.uuid4()),
        'avatar':      rand_password().encode('utf-8'),
        'login_ip':    rand_ip(),
        'network':     '.'.join(rand_ip().split('.')[:3]) + '.0/24',
        'device_mac':  rand_mac(),
    }


def rand_row_minimal():
    return {
        'id':   _next_id(),
        'name': rand_name_en(),
    }


# ===================== Helpers =====================

def filterbykeys(data, keys):
    if isinstance(data, dict):
        return {k: data[k] for k in keys}
    elif isinstance(data, list):
        return [filterbykeys(row, keys) for row in data]


def rows2cols(rows, keys):
    if isinstance(rows, dict):
        return rows
    elif isinstance(rows, list):
        if len(rows) == 0:
            return {k: [] for k in keys}
        else:
            res = defaultdict(list)
            for row in rows:
                for k, v in row.items():
                    res[k].append(v)
            return dict(res)


# --- Composite primary key schema ---
CompositePKSchema = TableDefine('CompositePKSchema', {
    'group_id': Field(int, primary=True),
    'item_id':  Field(int, primary=True),
    'value':    Field(str),
})

# --- 3-column schema for multi-column order_by tests ---
ThreeColSchema = TableDefine('ThreeColSchema', {
    'id':    Field(int, primary=True),
    'group': Field(str),
    'score': Field(int),
})


# ===================== SQLite Schemas =====================

# --- SQLite SQL-mode schema (cross-database compatible types) ---
SQLiteSQLModeSchema = TableDefine('SQLiteSQLModeSchema', {
    'id':          Field(Integer, primary=True),
    'small_val':   Field(SmallInteger),
    'big_val':     Field(BigInteger),
    'float_val':   Field(Float),
    'numeric_val': Field(Numeric(10, 2)),
    'name':        Field(String(64)),
    'bio':         Field(Text),
    'is_active':   Field(Boolean),
    'birthday':    Field(Date),
    'alarm':       Field(Time),
    'created_at':  Field(DateTime),
    'raw_data':    Field(JSON),
    'avatar':      Field(LargeBinary),
})


def rand_row_sqlite_sql():
    return {
        'id':          _next_id(),
        'small_val':   rand_uint8(),
        'big_val':     rand_int32(),
        'float_val':   round(rand_float_01() * 100, 4),
        'numeric_val': round(rand_float_01() * 10000, 2),
        'name':        rand_name_en(),
        'bio':         rand_url(),
        'is_active':   rand_bool(),
        'birthday':    rand_date(),
        'alarm':       rand_time(),
        'created_at':  rand_datetime(),
        'raw_data':    {'key': rand_username(), 'values': [1, 2, 3]},
        'avatar':      rand_password().encode('utf-8'),
    }
