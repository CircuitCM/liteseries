`_handlers.py` provides the public runtime API for launching thread-local SQLite ADBC connections, priming file-backed databases for WAL usage, shutting thread pools down cleanly, and decorating time-series fetchers with the local cache flow.
- `_util` : Resolves database paths, shapes Arrow tables for ingest, and builds CREATE TABLE statements used during cache initialization and refresh.
- `_sql` : Supplies the quoted SQL statement builders used for metadata lookups, cached range reads, and max-timestamp queries.

`_util.py` contains lower-level helpers for default database discovery, timestamp utilities, SQLite table inspection, CREATE TABLE statement generation, and Arrow table normalization before ingest.
- `_sql` : Provides identifier quoting, table constants, and insert/query helpers that `_util.py` reuses when inspecting or defining SQLite tables.

`_sql.py` centralizes reusable quoted SQL fragments and tiny caches for generated SQL so the runtime can build common SQLite statements without repeating formatting work.
