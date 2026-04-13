`_handlers.py` provides the public runtime API for resolving and launching thread-local SQLite connections, shutting them down cleanly, and decorating time-series fetchers with the local cache behavior.
- `_util` : Supplies DDL helpers, Arrow table shaping, and SQLite type inference used when cached tables are first created or expanded.
- `_sql` : Supplies the reusable SQL statement builders used for range reads, max-timestamp lookups, and metadata-table queries.

`_util.py` contains lower-level helpers for DB path discovery, time utilities, SQLite schema inspection, CREATE TABLE statement generation, and Arrow table normalization before ingest.
- `_sql` : Provides table constants and SQL statement builders that `_util.py` uses to inspect tables and generate insert statements consistently.

`_sql.py` centralizes reusable SQL fragments and tiny caches for generated SQL so the runtime can build common statements without repeating formatting work.
