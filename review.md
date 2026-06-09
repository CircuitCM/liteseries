tests/integration/test_public_api.py: Public API integration coverage for launch and cache behavior.
- liteseries._handlers: Exercises `launch_ls`, `close_ls`, `threadpool_shutdown_ls`, and decorated `ls_cache` fetchers through the package exports.
- launch_ls: Covers env/default discovery plus missing file, directory, and suffixless explicit paths creating the resolved SQLite database file.
- ls_cache: Covers cache initialization, initial full-history seed trimming back to the requested start, bounded historical extension, freshness-gated cache hits, open-ended reads, empty seeds, pre-max empty ranges, expiration floor behavior, expiration locking for both nonempty cached selections and empty post-max selections, and protected-name handling for yfinance columns when `LITESERIES_PROTECTNAMES=true`.

tests/integration/test_sql_names.py: SQL identifier mode coverage for the environment-selected query builders.
- liteseries._sql: Exercises `LITESERIES_PROTECTNAMES` import-time selection for the default pass-through path, the case-insensitive `true` protected path, and non-true env values.
- qident: Verifies valid names pass through unchanged by default and whitespace/quote-containing names use the original double-quoted escaping only in protected mode.
- colreq: Verifies cached column-list construction reflects the active identifier function after module reload.

Unreachable or residual coverage notes:
- liteseries/_handlers.py: The `mxt_row is None` branch after `SELECT MAX(...)` remains uncovered because SQLite returns a row containing `NULL` for empty aggregates rather than no row.
- liteseries/_handlers.py: The shared-cache URI branch for preformatted `file:` database URIs remains uncovered.
- liteseries/_handlers.py: Persistent-threadpool concurrent stale-write coverage is kept as a non-running xfail because executing the deferred lock path leaves ADBC cursor finalizers noisy during pytest shutdown.
- liteseries/_util.py: Remaining misses are internal helper edge cases and preserved preformatted URI returns not currently covered through the public API tests.
