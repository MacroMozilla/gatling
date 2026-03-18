"""
PostgreSQL Type Coverage for PGSQLTable Tests
==============================================

This file documents all PostgreSQL column types covered by the test suite.
Each type is tested for: insert, fetch, update, delete, where-filter round-trip.

## Python-mode types (auto-mapped via _PY_TO_SQL)
  1. int          -> Integer
  2. float        -> Float
  3. str          -> Text
  4. bool         -> Boolean
  5. bytes        -> LargeBinary
  6. datetime.date      -> Date
  7. datetime.time      -> Time
  8. datetime.datetime  -> DateTime
  9. datetime.timedelta -> Interval

## SQL-mode types (PostgreSQL-specific)
  10. SmallInteger
  11. Integer
  12. BigInteger
  13. Float
  14. Numeric(10, 2)
  15. DOUBLE_PRECISION
  16. String(64)
  17. Text
  18. Boolean
  19. Date
  20. Time
  21. TIMESTAMP(timezone=True)
  22. Interval
  23. JSONB
  24. JSON
  25. UUID
  26. BYTEA
  27. INET
  28. CIDR
  29. MACADDR

## Test Schemas

### PyModeSchema (a_const_test.py)
  - Uses Python-mode Field definitions (int, float, str, bool, date, time, datetime)
  - Primary key: id (int, manual)
  - Covers basic CRUD round-trip for all Python types

### SQLModeSchema (a_const_test.py)
  - Uses SQL-mode Field definitions (SmallInteger, BigInteger, Numeric, etc.)
  - Primary key: id (Integer, manual)
  - Covers PostgreSQL-specific types: JSONB, JSON, UUID, INET, CIDR, MACADDR, TIMESTAMP, etc.

### MinimalSchema (a_const_test.py)
  - Minimal 2-column schema for testing table lifecycle (create/exists/drop/truncate)
"""
