![Liteseries](Liteseries-1.png)

An SQLite-backed replicator and cache for timeseries data.

Liteseries works as an invisible layer around a timeseries endpoint. Your function still returns the rows you would
normally get from the vendor, while liteseries stores them in SQLite for the next matching call. When a request reaches
past the saved range, it reads the local slice first and asks the vendor only for the missing forward period. Liteseries
is stable for single-threaded use, with parallel reads available where the Arrow backend supports them.

```python
from liteseries import Rollback, close_ls, launch_ls, ls_cache, threadpool_shutdown_ls
```

- `launch_ls()` opens the local SQLite/ADBC runtime for the current thread.
- `ls_cache(...)` decorates endpoint functions that return `pyarrow.Table`
  objects.
- `Rollback(...)` enables overlap checks for adjusted historical series.
- `close_ls()` closes the runtime connection when your process is done with it.
- `threadpool_shutdown_ls(...)` for use in a multithreaded environment.

## Usage

Install the package, then call `launch_ls()` once before cached functions run:

```python
from __future__ import annotations

from datetime import UTC, datetime, time, timedelta

import pyarrow as pa

from liteseries import Rollback, close_ls, launch_ls, ls_cache


def unix_micros(year: int, month: int, day: int) -> int:
    """Return a UTC timestamp in the microsecond units liteseries stores."""
    return int(datetime(year, month, day, tzinfo=UTC).timestamp() * 1_000_000)


launch_ls()
```

By default, `launch_ls()` looks for an existing `.sqlite` file near the entry
script, prefers one with `liteseries` in its name, and otherwise creates
`liteseries_db.sqlite`. Pass a path when you want to choose the file yourself:

```python
launch_ls("cache/prices.sqlite")
```

You can also set `LITESERIES_DB` before launch when configuration belongs in the
environment:

```powershell
$env:LITESERIES_DB = "D:\market-cache\prices.sqlite"
```

### Wrap An Endpoint

Your endpoint function should accept keyword arguments for the requested time
range and key values, then return a `pyarrow.Table`. The time column is expected
to use Unix microseconds, and `None` means an open-ended side of the range.

This example uses a local data source so the shape is easy to see. A real
function can call an HTTP API, a vendor SDK, or a file reader.

```python
def vendor_prices(start, end, symbol, interval) -> pa.Table:
    """Fetch rows from the source system and return only endpoint columns."""
    rows = [
        (unix_micros(2026, 1, 2), 101.25, 102.10),
        (unix_micros(2026, 1, 3), 102.00, 103.40),
        (unix_micros(2026, 1, 4), 103.25, 103.05),
    ]

    if start is not None:
        rows = [row for row in rows if row[0] >= start]
    if end is not None:
        rows = [row for row in rows if row[0] <= end]

    return pa.table(
        {
            "ts": [row[0] for row in rows],
            "open": [row[1] for row in rows],
            "close": [row[2] for row in rows],
        }
    )
```

Decorate it with `ls_cache`. `columns` is the full SQLite schema, including key
columns that may come from function arguments instead of the returned Arrow
table. `column_keys` identify separate series inside one table, while
`table_keys` can split families such as intervals into separate SQLite tables.

```python
@ls_cache(
    columns=("symbol", "ts", "open", "close"),
    time_keys=("start", "end"),
    time_col="ts",
    column_keys=("symbol",),
    out_cols=("ts", "open", "close"),
    table="prices",
    refresh_period=timedelta(days=1),
    active_in=time(0, tzinfo=UTC),
)
def prices(start, end, symbol):
    """Return cached prices, refreshing from the endpoint when needed."""
    return vendor_prices(start, end, symbol, interval="1d")
```

Now call the wrapped function with keyword arguments:

```python
try:
    first = prices(
        start=unix_micros(2026, 1, 2),
        end=unix_micros(2026, 1, 4),
        symbol="MSFT",
    )

    second = prices(
        start=unix_micros(2026, 1, 2),
        end=unix_micros(2026, 1, 4),
        symbol="MSFT",
    )

    assert second.equals(first)
finally:
    close_ls()
```

On the first call, liteseries creates the data table and its metadata table,
fills missing key columns, writes the Arrow rows, and returns the requested
slice. On later calls, it serves rows from SQLite unless the request reaches
past the cached freshness boundary.

### Refresh Windows

`refresh_period` and `active_in` tell liteseries when it is worth checking the
endpoint again.

For daily or slower data, pass one `time` that marks when the latest period is
expected to be complete:

```python
@ls_cache(
    columns=("symbol", "ts", "open", "close"),
    time_keys=("start", "end"),
    time_col="ts",
    column_keys=("symbol",),
    table="daily_prices",
    refresh_period=timedelta(days=1),
    active_in=time(16, 1, tzinfo=UTC),
)
def daily_prices(start, end, symbol):
    """Cache daily bars after the market close boundary has passed."""
    return vendor_prices(start, end, symbol, interval="1d")
```

For intraday data, pass a `(start_time, end_time)` window. Liteseries advances
the freshness boundary by `refresh_period` steps inside that window:

```python
@ls_cache(
    columns=("symbol", "ts", "open", "close"),
    time_keys=("start", "end"),
    time_col="ts",
    column_keys=("symbol",),
    table_keys=("interval",),
    out_cols=("ts", "open", "close"),
    table="intraday_prices",
    refresh_period=timedelta(minutes=5),
    active_in=(time(9, 30, tzinfo=UTC), time(16, 0, tzinfo=UTC)),
)
def intraday_prices(start, end, symbol, interval):
    """Cache one interval per SQLite table, keyed by symbol within each table."""
    return vendor_prices(start, end, symbol, interval=interval)
```

### Rollback Adjustments

Some historical series are restated when a contract rolls, a split is applied, or
a vendor back-adjusts prior bars. Pass `rollback=Rollback(...)` when a stale
refresh should overlap the latest cached row and compare selected value columns.

With the default multiplier strategy, liteseries compares the last cached row
with the first endpoint row from the overlap. If any `adjust_columns` value has
changed beyond floating-point tolerance, it multiplies the cached history for
that key by the ratio between cached and endpoint values, then appends the new
endpoint rows after the overlap:

```python
@ls_cache(
    columns=("symbol", "ts", "open", "close"),
    time_keys=("start", "end"),
    time_col="ts",
    column_keys=("symbol",),
    table="continuous_prices",
    refresh_period=timedelta(days=1),
    active_in=time(16, 1, tzinfo=UTC),
    rollback=Rollback(adjust_columns=("open", "close")),
)
def continuous_prices(start, end, symbol):
    """Cache a back-adjusted continuous series."""
    return vendor_prices(start, end, symbol, interval="1d")
```

Use `included_keys` to limit rollback checks to selected key values. `None`
enables rollback for every request, which is the default.

```python
rollback=Rollback(
    adjust_columns=("open", "close"),
    included_keys={"symbol": {"ES_CONT"}},
)
```

For providers where a simple multiplier is not enough, set `rebuild=True`.
When an overlap changes, liteseries requests historical data again with
`start=None`, stages the returned adjusted columns, and copies them into the
existing cached rows without recreating the primary keys:

```python
rollback=Rollback(
    adjust_columns=("open", "close"),
    rebuild=True,
)
```

### Columns And Names

Use plain Python-style column and table names when you can. That is the default
fast path. If your endpoint already produces names with spaces or quotes, set
`LITESERIES_PROTECTNAMES=true` before importing liteseries so SQL identifiers
are double-quoted:

```powershell
$env:LITESERIES_PROTECTNAMES = "true"
```

`out_cols` controls the columns returned by the decorated function. This is handy
when the database needs key columns such as `symbol`, but callers only want the
time-series values.

### Empty Tails

Some providers stop returning rows after a symbol expires or a contract ends.
Pass `expires_after` when liteseries should stop asking forward for that key
after repeated empty tail checks:

```python
@ls_cache(
    columns=("symbol", "ts", "open", "close"),
    time_keys=("start", "end"),
    time_col="ts",
    column_keys=("symbol",),
    table="expired_prices",
    refresh_period=timedelta(hours=1),
    active_in=(time(0, tzinfo=UTC), time(23, 59, tzinfo=UTC)),
    expires_after=timedelta(days=7),
)
def expired_prices(start, end, symbol):
    """Cache sparse series without querying forever beyond the final row."""
    return vendor_prices(start, end, symbol, interval="1h")
```

For a live provider example with pandas/yfinance conversion, see `demo.py`.
