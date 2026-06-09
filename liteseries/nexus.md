`_handlers.py` provides the public runtime API for launching thread-local SQLite ADBC connections, priming file-backed databases for WAL usage, shutting thread pools down cleanly, and decorating time-series fetchers with the local cache flow including metadata-row inserts, refresh updates, bounded historical watermarks, and optional expired-key lockouts.
- `_util` : Resolves database paths, shapes Arrow tables for ingest, and builds the `CREATE TABLE IF NOT EXISTS` statements used during cache initialization.
- `_sql` : Supplies the quoted SQL statement builders used for metadata lookups, metadata row inserts/updates, cached range reads, and max-timestamp queries.

`_util.py` contains lower-level helpers for default database discovery, SQLite file creation from default/directory/file paths, timestamp utilities, SQLite table inspection, `CREATE TABLE IF NOT EXISTS` generation, and Arrow table normalization before ingest while reusing existing Arrow column buffers.
- `_sql` : Provides identifier quoting, table constants, and insert/query helpers that `_util.py` reuses when inspecting or defining SQLite tables.

`_sql.py` centralizes reusable SQL fragments and tiny caches for generated SQL so the runtime can build common SQLite statements without repeating formatting work, including metadata-row insert/update statements for the cache info tables. Identifier handling is selected at import time: `LITESERIES_PROTECTNAMES=true` keeps the original double-quoted compatibility path, while the default path passes already-valid Python-style names through unchanged.
- os : Reads `LITESERIES_PROTECTNAMES` once during module import to choose the identifier function used by the SQL builders.

`README.md` introduces the stable public API, the single-threaded launch/cache/close lifecycle, the supported database and identifier environment variables, and beginner-oriented `ls_cache` usage examples for Arrow-backed endpoint functions.
