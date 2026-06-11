from __future__ import annotations

import gc
import os
import sqlite3
import threading as th
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from time import sleep

import pyarrow as pa
import pytest

import __main__
import liteseries._handlers as handlers
from liteseries import Rollback, close_ls, launch_ls, ls_cache, threadpool_shutdown_ls

STEP_1H_US = 3_600_000_000
UTC_WINDOW = (time(0, tzinfo=UTC), time(23, 59, tzinfo=UTC))


def protect_names_enabled() -> bool:
    """Return whether tests should exercise whitespace-protected identifiers."""
    return os.getenv("LITESERIES_PROTECTNAMES", "").casefold() == "true"


def dt_micros(year: int, month: int, day: int, hour: int = 0) -> int:
    return int(datetime(year, month, day, hour, tzinfo=UTC).timestamp() * 1_000_000)


def empty_prices() -> pa.Table:
    return pa.table(
        {
            "ts": pa.array([], type=pa.int64()),
            "open": pa.array([], type=pa.float64()),
            "close": pa.array([], type=pa.float64()),
        }
    )


def price_table(rows: list[tuple[int, float, float]]) -> pa.Table:
    if not rows:
        return empty_prices()

    return pa.table(
        {
            "ts": pa.array([row[0] for row in rows], type=pa.int64()),
            "open": pa.array([row[1] for row in rows], type=pa.float64()),
            "close": pa.array([row[2] for row in rows], type=pa.float64()),
        }
    )


def rows_in_range(rows: list[tuple[int, float, float]], start: int | None, end: int | None) -> list[tuple[int, float, float]]:
    selected = rows
    if start is not None:
        selected = [row for row in selected if row[0] >= start]
    if end is not None:
        selected = [row for row in selected if row[0] <= end]
    return selected


def make_hourly_rows(start: int, count: int, base: float) -> list[tuple[int, float, float]]:
    return [(start + i * STEP_1H_US, base + i + 0.25, base + i + 0.75) for i in range(count)]


def wipe_db_files(db_path: Path) -> None:
    for _ in range(5):
        gc.collect()
        locked = False
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(f"{db_path}{suffix}")
            if not candidate.exists():
                continue
            try:
                candidate.unlink()
            except PermissionError:
                locked = True
        if not locked:
            return
        sleep(0.05)


class DeterministicEndpoint:
    def __init__(self, datasets: dict[tuple[str, str, str], list[tuple[int, float, float]]]) -> None:
        self.datasets = datasets
        self.calls: list[dict[str, object]] = []
        self.lock = th.Lock()
        self.write_gate: th.Barrier | None = None
        self.blocked_keys: set[tuple[str, str, str]] = set()

    def block_writes_for(self, *keys: tuple[str, str, str]) -> None:
        self.blocked_keys = set(keys)
        self.write_gate = th.Barrier(len(keys), timeout=10)

    def clear_gate(self) -> None:
        self.blocked_keys.clear()
        self.write_gate = None

    def __call__(self, start, end, instrument, interval, flavor="base") -> pa.Table:
        key = (interval, instrument, flavor)
        with self.lock:
            self.calls.append(
                {
                    "key": key,
                    "start": start,
                    "end": end,
                    "thread": th.get_ident(),
                }
            )
        gate = self.write_gate
        if False and gate is not None and key in self.blocked_keys and start is not None:
            gate.wait()

        rows = rows_in_range(self.datasets[key], start, end)
        return price_table(rows)


@pytest.fixture(scope="session", autouse=True)
def init_default_db() -> Path:
    db_path = Path.cwd() / "liteseries_db.sqlite"
    for candidate in Path.cwd().glob("*.sqlite"):
        wipe_db_files(candidate)
    return db_path


@pytest.fixture()
def default_db_path(monkeypatch: pytest.MonkeyPatch, init_default_db: Path) -> Path:
    monkeypatch.delenv("LITESERIES_DB", raising=False)
    monkeypatch.setattr("liteseries._util._FIRST_IMPORT_ROOT", Path.cwd())
    if hasattr(handlers, "local_adbc"):
        try:
            handlers.local_adbc.close()
        except Exception:
            pass
    return init_default_db


def test_launch_ls_touches_missing_file_and_directory_paths(tmp_path: Path) -> None:
    """Covers explicit launch paths creating the SQLite file when it does not exist yet."""
    explicit_db = tmp_path / "explicit.sqlite"
    launch_ls(str(explicit_db))
    close_ls()

    db_dir = tmp_path / "db_root"
    db_dir.mkdir()
    launch_ls(str(db_dir))
    close_ls()

    suffixless_db = tmp_path / "suffixless"
    launch_ls(str(suffixless_db))
    close_ls()

    assert explicit_db.is_file()
    assert (db_dir / "liteseries_db.sqlite").is_file()
    assert suffixless_db.with_suffix(".sqlite").is_file()
    wipe_db_files(explicit_db)
    wipe_db_files(db_dir / "liteseries_db.sqlite")
    wipe_db_files(suffixless_db.with_suffix(".sqlite"))


def test_launch_ls_uses_env_path_and_cwd_discovery(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Covers the environment-path and cwd-based default database discovery branches exposed through launch_ls."""
    env_db = tmp_path / "env.sqlite"
    monkeypatch.setenv("LITESERIES_DB", str(env_db))
    launch_ls()
    close_ls()
    assert env_db.is_file()

    monkeypatch.delenv("LITESERIES_DB")
    monkeypatch.setattr(__main__, "__file__", None, raising=False)
    monkeypatch.chdir(tmp_path)
    preferred = tmp_path / "preferred_liteseries.sqlite"
    other = tmp_path / "other.sqlite"
    preferred.touch()
    other.touch()

    launch_ls()

    @ls_cache(
        columns=("instrument", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument",),
        table="discovered_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def fetch_prices(start, end, instrument):
        return price_table(make_hourly_rows(dt_micros(2026, 3, 1, 9), 2, 10.0))

    try:
        fetch_prices(
            start=dt_micros(2026, 3, 1, 9),
            end=dt_micros(2026, 3, 1, 10),
            instrument="AAA",
        )
    finally:
        close_ls()

    with sqlite3.connect(preferred) as conn:
        names = {name for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
    with sqlite3.connect(other) as conn:
        other_names = {name for (name,) in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}

    assert "discovered_prices" in names
    assert "discovered_prices_info" in names
    assert "discovered_prices" not in other_names
    wipe_db_files(env_db)
    wipe_db_files(preferred)
    wipe_db_files(other)


def test_launch_ls_creates_default_db_when_none_exists(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    """Covers the fallback branch that creates liteseries_db.sqlite when launch_ls receives no explicit path."""
    monkeypatch.delenv("LITESERIES_DB", raising=False)
    monkeypatch.setattr(__main__, "__file__", None, raising=False)
    monkeypatch.setattr("liteseries._util._FIRST_IMPORT_ROOT", None)
    monkeypatch.chdir(tmp_path)

    launch_ls()
    close_ls()

    assert (tmp_path / "liteseries_db.sqlite").is_file()
    wipe_db_files(tmp_path / "liteseries_db.sqlite")


def test_ls_cache_wraps_a_yfinance_endpoint_and_reuses_the_cached_slice(default_db_path: Path) -> None:
    """Covers fixed-schema raw and adjusted yfinance wrappers, then verifies repeat calls stay in-cache."""
    if not protect_names_enabled():
        pytest.skip("requires LITESERIES_PROTECTNAMES=true for yfinance columns with spaces")
    yf = pytest.importorskip("yfinance")
    pd = pytest.importorskip("pandas")

    launch_ls()
    raw_calls: list[tuple[int | None, int | None, str, str]] = []
    adj_calls: list[tuple[int | None, int | None, str, str]] = []

    @ls_cache(
        columns=("Adj Close", "Close", "High", "Low", "Open", "Volume", "ts", "ticker"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("ticker",),
        table_keys=("interval",),
        out_cols=("Adj Close", "Close", "High", "Low", "Open", "Volume", "ts"),
        table="yf_prices",
        refresh_period=timedelta(days=1),
        active_in=time(0, tzinfo=UTC),
    )
    def fetch_prices(start, end, ticker, interval):
        raw_calls.append((start, end, ticker, interval))
        start_dt = pd.Timestamp(start, unit="us", tz="UTC").to_pydatetime() if start is not None else None
        end_dt = pd.Timestamp(end, unit="us", tz="UTC").to_pydatetime() + pd.Timedelta(days=1) if end is not None else None
        frame = yf.download(
            tickers=ticker,
            start=start_dt,
            end=end_dt,
            interval=interval,
            auto_adjust=False,
            actions=False,
            progress=False,
            threads=False,
            multi_level_index=False,
        )
        frame = frame.rename_axis("Date").reset_index()
        frame["ts"] = (pd.to_datetime(frame.pop("Date"), utc=True).astype("int64") // 1_000).astype("int64")
        frame["Volume"] = frame["Volume"].fillna(0).astype("int64")
        return pa.Table.from_pandas(frame, preserve_index=False)

    @ls_cache(
        columns=("Close", "High", "Low", "Open", "Volume", "ts", "ticker"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("ticker",),
        table_keys=("interval",),
        out_cols=("Close", "High", "Low", "Open", "Volume", "ts"),
        table="yf_prices_adj",
        refresh_period=timedelta(days=1),
        active_in=time(0, tzinfo=UTC),
    )
    def fetch_prices_adj(start, end, ticker, interval):
        adj_calls.append((start, end, ticker, interval))
        start_dt = pd.Timestamp(start, unit="us", tz="UTC").to_pydatetime() if start is not None else None
        end_dt = pd.Timestamp(end, unit="us", tz="UTC").to_pydatetime() + pd.Timedelta(days=1) if end is not None else None
        frame = yf.download(
            tickers=ticker,
            start=start_dt,
            end=end_dt,
            interval=interval,
            auto_adjust=True,
            actions=False,
            progress=False,
            threads=False,
            multi_level_index=False,
        )
        frame = frame.rename_axis("Date").reset_index()
        frame["ts"] = (pd.to_datetime(frame.pop("Date"), utc=True).astype("int64") // 1_000).astype("int64")
        frame["Volume"] = frame["Volume"].fillna(0).astype("int64")
        return pa.Table.from_pandas(frame, preserve_index=False)

    try:
        start = dt_micros(2024, 1, 2)
        end = dt_micros(2024, 1, 12)
        first = fetch_prices(start=start, end=end, ticker="MSFT", interval="1d")
        first_adj = fetch_prices_adj(start=start, end=end, ticker="MSFT", interval="1d")
        if first.num_rows == 0 or first_adj.num_rows == 0:
            pytest.skip("yfinance returned no rows for the fixed historical window")

        second = fetch_prices(start=start, end=end, ticker="MSFT", interval="1d")
        second_adj = fetch_prices_adj(start=start, end=end, ticker="MSFT", interval="1d")
        expected_ts = [ts for ts in first["ts"].to_pylist() if ts >= start]
        expected_ts_adj = [ts for ts in first_adj["ts"].to_pylist() if ts >= start]

        assert first.num_rows > 0
        assert second["ts"].to_pylist() == expected_ts
        assert len(raw_calls) == 1
        assert first.column_names == ["Adj Close", "Close", "High", "Low", "Open", "Volume", "ts"]
        assert second_adj["ts"].to_pylist() == expected_ts_adj
        assert len(adj_calls) == 1
        assert first_adj.column_names == ["Close", "High", "Low", "Open", "Volume", "ts"]
    finally:
        close_ls()


def test_ls_cache_covers_init_refresh_fresh_hits_and_empty_tail(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers missing-table init, stale tail fetches, fresh cache hits, new key initialization, and empty extensions."""
    start = dt_micros(2026, 1, 5, 9)
    datasets = {
        ("1h", "AAA", "base"): make_hourly_rows(start, 6, 100.0),
        ("1h", "BBB", "base"): make_hourly_rows(start, 3, 200.0),
    }
    endpoint = DeterministicEndpoint(datasets)

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "flavor", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument", "flavor"),
        table_keys=("interval",),
        out_cols=("ts", "open", "close"),
        table="deterministic_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def fetch_prices(start, end, instrument, interval, flavor="base"):
        return endpoint(start, end, instrument, interval, flavor)

    try:
        first_end = datasets["1h", "AAA", "base"][2][0]
        initial = fetch_prices(start=start, end=first_end, instrument="AAA", interval="1h", flavor="base")
        assert initial.num_rows == 3
        assert len(endpoint.calls) == 1
        assert endpoint.calls[0]["start"] is None

        refreshed_now = datasets["1h", "AAA", "base"][-1][0] + STEP_1H_US
        monkeypatch.setattr("liteseries._util.sys_micros", lambda: refreshed_now)
        extended_end = datasets["1h", "AAA", "base"][4][0]
        refreshed = fetch_prices(start=start, end=extended_end, instrument="AAA", interval="1h", flavor="base")
        assert refreshed.num_rows == 5
        assert len(endpoint.calls) == 2
        assert endpoint.calls[1]["start"] == datasets["1h", "AAA", "base"][3][0]

        cached = fetch_prices(start=start, end=extended_end, instrument="AAA", interval="1h", flavor="base")
        assert cached.equals(refreshed)
        assert len(endpoint.calls) == 2

        monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
        new_key_end = datasets["1h", "BBB", "base"][-1][0]
        second_key = fetch_prices(start=start, end=new_key_end, instrument="BBB", interval="1h", flavor="base")
        assert second_key.num_rows == 3
        assert len(endpoint.calls) == 3
        assert endpoint.calls[2]["key"] == ("1h", "BBB", "base")
        assert endpoint.calls[2]["start"] is None

        empty = fetch_prices(
            start=datasets["1h", "BBB", "base"][-1][0] + STEP_1H_US,
            end=datasets["1h", "BBB", "base"][-1][0] + 2 * STEP_1H_US,
            instrument="BBB",
            interval="1h",
            flavor="base",
        )
        assert empty.num_rows == 0
        assert len(endpoint.calls) == 4
        assert endpoint.calls[3]["start"] == datasets["1h", "BBB", "base"][-1][0] + STEP_1H_US
    finally:
        close_ls()


def test_ls_cache_initial_seed_trims_return_to_requested_start(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers initial full-history seed returning only rows at or after the caller's requested start."""
    start = dt_micros(2026, 1, 8, 9)
    requested_start = start + STEP_1H_US
    requested_end = start + 2 * STEP_1H_US
    datasets = {
        ("1h", "AAA", "base"): make_hourly_rows(start, 3, 150.0),
    }
    endpoint = DeterministicEndpoint(datasets)

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument",),
        out_cols=("ts", "open", "close"),
        table="initial_seed_trim_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def fetch_prices(start, end, instrument):
        return endpoint(start, end, instrument, "1h")

    try:
        seeded = fetch_prices(start=requested_start, end=requested_end, instrument="AAA")
        cached_prefix = fetch_prices(start=None, end=start, instrument="AAA")

        assert endpoint.calls[0]["start"] is None
        assert seeded["ts"].to_pylist() == [requested_start, requested_end]
        assert cached_prefix["ts"].to_pylist() == [start]
        assert len(endpoint.calls) == 1
    finally:
        close_ls()


def test_ls_cache_extends_recent_bounded_historical_loads(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers bounded historical metadata using edate instead of a fresh wall-clock update."""
    start = dt_micros(2026, 1, 12, 9)
    datasets = {
        ("1h", "AAA", "base"): make_hourly_rows(start, 6, 125.0),
    }
    endpoint = DeterministicEndpoint(datasets)

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: dt_micros(2026, 6, 5, 12))
    launch_ls()

    @ls_cache(
        columns=("instrument", "flavor", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument", "flavor"),
        table_keys=("interval",),
        out_cols=("ts", "open", "close"),
        table="bounded_history_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def fetch_prices(start, end, instrument, interval, flavor="base"):
        return endpoint(start, end, instrument, interval, flavor)

    try:
        first_end = datasets["1h", "AAA", "base"][2][0]
        extended_end = datasets["1h", "AAA", "base"][4][0]

        seeded = fetch_prices(start=start, end=first_end, instrument="AAA", interval="1h", flavor="base")
        extended = fetch_prices(start=start, end=extended_end, instrument="AAA", interval="1h", flavor="base")

        assert seeded.num_rows == 3
        assert extended.num_rows == 5
        assert len(endpoint.calls) == 2
        assert endpoint.calls[0]["start"] is None
        assert endpoint.calls[1]["start"] == datasets["1h", "AAA", "base"][3][0]
    finally:
        close_ls()


def test_ls_cache_daily_active_time_updates_last_qualified_boundary(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers daily refresh metadata before and after the configured active time."""
    start = dt_micros(2026, 6, 10, 9)
    end = start
    marker = 987_654_321

    class BeforeActiveDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 6, 10, 11, tzinfo=tz)

    class AfterActiveDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 6, 10, 13, tzinfo=tz)

    def make_fetch(table_name: str):
        @ls_cache(
            columns=("instrument", "ts", "open", "close"),
            time_keys=("start", "end"),
            time_col="ts",
            column_keys=("instrument",),
            out_cols=("ts", "open", "close"),
            table=table_name,
            refresh_period=timedelta(days=1),
            active_in=time(12, tzinfo=UTC),
        )
        def fetch_prices(start, end, instrument):
            return price_table([(dt_micros(2026, 6, 10, 9), 10.0, 11.0)])

        return fetch_prices

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: marker)
    launch_ls()
    try:
        monkeypatch.setattr(handlers, "datetime", BeforeActiveDateTime)
        before_fetch = make_fetch("daily_before_active_prices")
        before_fetch(start=start, end=end, instrument="AAA")

        monkeypatch.setattr(handlers, "datetime", AfterActiveDateTime)
        after_fetch = make_fetch("daily_after_active_prices")
        after_fetch(start=start, end=end, instrument="AAA")

        cur = handlers.local_adbc.cur
        cur.execute("SELECT last_upd FROM daily_before_active_prices_info WHERE instrument = ?", ("AAA",))
        before_last_upd = cur.fetchone()[0]
        cur.execute("SELECT last_upd FROM daily_after_active_prices_info WHERE instrument = ?", ("AAA",))
        after_last_upd = cur.fetchone()[0]

        assert before_last_upd == end
        assert after_last_upd == marker
    finally:
        close_ls()


def test_ls_cache_rollback_multiplier_adjusts_cached_history(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers rollback overlap refreshes that multiply cached history in place."""
    start = dt_micros(2026, 2, 9, 9)
    rows_seed = [
        (start, 100.0, 200.0),
        (start + STEP_1H_US, 110.0, 220.0),
    ]
    rows_tail = [
        (start + STEP_1H_US, 55.0, 110.0),
        (start + 2 * STEP_1H_US, 60.0, 120.0),
        (start + 3 * STEP_1H_US, 65.0, 130.0),
    ]
    calls: list[tuple[int | None, int | None]] = []

    def endpoint(start, end, instrument):
        calls.append((start, end))
        return price_table(rows_in_range(rows_seed if len(calls) == 1 else rows_tail, start, end))

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument",),
        out_cols=("ts", "open", "close"),
        table="rollback_multiplier_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
        rollback=Rollback(("open", "close")),
    )
    def fetch_prices(start, end, instrument):
        return endpoint(start, end, instrument)

    try:
        seed_end = rows_seed[-1][0]
        final_end = rows_tail[-1][0]
        seeded = fetch_prices(start=start, end=seed_end, instrument="AAA")
        adjusted = fetch_prices(start=start, end=final_end, instrument="AAA")
        cached = fetch_prices(start=start, end=final_end, instrument="AAA")

        assert seeded["open"].to_pylist() == [100.0, 110.0]
        assert calls[1][0] == seed_end
        assert adjusted["ts"].to_pylist() == [row[0] for row in (*rows_seed, *rows_tail[1:])]
        assert adjusted["open"].to_pylist() == [200.0, 220.0, 60.0, 65.0]
        assert adjusted["close"].to_pylist() == [400.0, 440.0, 120.0, 130.0]
        assert cached.equals(adjusted)
        assert len(calls) == 2
    finally:
        close_ls()


def test_ls_cache_rollback_included_keys_leave_other_series_on_normal_refresh(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers rollback key filtering so nonincluded keys keep the normal nonoverlap refresh."""
    start = dt_micros(2026, 2, 16, 9)
    datasets = {
        "AAA": make_hourly_rows(start, 4, 10.0),
        "BBB": make_hourly_rows(start, 4, 20.0),
    }
    calls: list[tuple[str, int | None, int | None]] = []

    def endpoint(start, end, instrument):
        calls.append((instrument, start, end))
        return price_table(rows_in_range(datasets[instrument], start, end))

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument",),
        out_cols=("ts", "open", "close"),
        table="rollback_included_key_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
        rollback=Rollback(("open",), included_keys={"instrument": {"AAA"}}),
    )
    def fetch_prices(start, end, instrument):
        return endpoint(start, end, instrument)

    try:
        seed_end = start + STEP_1H_US
        final_end = start + 3 * STEP_1H_US

        fetch_prices(start=start, end=seed_end, instrument="AAA")
        fetch_prices(start=start, end=final_end, instrument="AAA")
        fetch_prices(start=start, end=seed_end, instrument="BBB")
        fetch_prices(start=start, end=final_end, instrument="BBB")

        assert calls[1] == ("AAA", seed_end, final_end)
        assert calls[3] == ("BBB", seed_end + STEP_1H_US, final_end)
    finally:
        close_ls()


def test_ls_cache_rollback_rebuild_copies_backfilled_values(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers rollback rebuilds that stage endpoint history and copy adjusted values into cached rows."""
    start = dt_micros(2026, 2, 23, 9)
    seed_rows = [
        (start, 100.0, 200.0),
        (start + STEP_1H_US, 110.0, 220.0),
    ]
    tail_rows = [
        (start + STEP_1H_US, 150.0, 250.0),
        (start + 2 * STEP_1H_US, 160.0, 260.0),
        (start + 3 * STEP_1H_US, 170.0, 270.0),
    ]
    rebuild_rows = [
        (start, 140.0, 240.0),
        (start + STEP_1H_US, 150.0, 250.0),
    ]
    calls: list[tuple[int | None, int | None]] = []

    def endpoint(start, end, instrument):
        calls.append((start, end))
        if len(calls) == 1:
            rows = seed_rows
        elif start is None:
            rows = rebuild_rows
        else:
            rows = tail_rows
        return price_table(rows_in_range(rows, start, end))

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument",),
        out_cols=("ts", "open", "close"),
        table="rollback_rebuild_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
        rollback=Rollback(("open", "close"), rebuild=True),
    )
    def fetch_prices(start, end, instrument):
        return endpoint(start, end, instrument)

    try:
        seed_end = seed_rows[-1][0]
        final_end = tail_rows[-1][0]
        seeded = fetch_prices(start=start, end=seed_end, instrument="AAA")
        rebuilt = fetch_prices(start=start, end=final_end, instrument="AAA")
        cached = fetch_prices(start=start, end=final_end, instrument="AAA")

        assert seeded["open"].to_pylist() == [100.0, 110.0]
        assert calls == [(None, seed_end), (seed_end, final_end), (None, seed_end)]
        assert rebuilt["open"].to_pylist() == [140.0, 150.0, 160.0, 170.0]
        assert rebuilt["close"].to_pylist() == [240.0, 250.0, 260.0, 270.0]
        assert cached.equals(rebuilt)
    finally:
        close_ls()


def test_ls_cache_expiration_floor_allows_late_forward_data(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers expires_after being floored to two refresh periods before a key can lock."""
    start = dt_micros(2026, 1, 19, 9)
    late_row = start + 3 * STEP_1H_US
    datasets = {
        ("1h", "AAA", "base"): [(start, 200.25, 200.75), (late_row, 203.25, 203.75)],
    }
    endpoint = DeterministicEndpoint(datasets)

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: dt_micros(2026, 6, 5, 12))
    launch_ls()

    @ls_cache(
        columns=("instrument", "flavor", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument", "flavor"),
        table_keys=("interval",),
        out_cols=("ts", "open", "close"),
        table="expiration_floor_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
        expires_after=timedelta(minutes=30),
    )
    def fetch_prices(start, end, instrument, interval, flavor="base"):
        return endpoint(start, end, instrument, interval, flavor)

    try:
        seeded = fetch_prices(start=start, end=start, instrument="AAA", interval="1h", flavor="base")
        empty_tail = fetch_prices(
            start=start,
            end=start + 2 * STEP_1H_US,
            instrument="AAA",
            interval="1h",
            flavor="base",
        )
        late = fetch_prices(start=start, end=late_row, instrument="AAA", interval="1h", flavor="base")

        assert seeded.num_rows == 1
        assert empty_tail.num_rows == 1
        assert late.num_rows == 2
        assert len(endpoint.calls) == 3
        assert endpoint.calls[2]["start"] == start + STEP_1H_US
    finally:
        close_ls()


def test_ls_cache_expires_keys_after_attempted_forward_horizon(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers locked nonempty and empty cached selections once no forward data exists past expires_after."""
    start = dt_micros(2026, 1, 26, 9)
    datasets = {
        ("1h", "AAA", "base"): [(start, 300.25, 300.75)],
    }
    endpoint = DeterministicEndpoint(datasets)

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: dt_micros(2026, 6, 5, 12))
    launch_ls()

    @ls_cache(
        columns=("instrument", "flavor", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument", "flavor"),
        table_keys=("interval",),
        out_cols=("ts", "open", "close"),
        table="expired_key_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
        expires_after=timedelta(hours=2),
    )
    def fetch_prices(start, end, instrument, interval, flavor="base"):
        return endpoint(start, end, instrument, interval, flavor)

    try:
        seeded = fetch_prices(start=start, end=start, instrument="AAA", interval="1h", flavor="base")
        attempted = fetch_prices(
            start=start,
            end=start + 3 * STEP_1H_US,
            instrument="AAA",
            interval="1h",
            flavor="base",
        )
        locked_nonempty = fetch_prices(
            start=start,
            end=start + 4 * STEP_1H_US,
            instrument="AAA",
            interval="1h",
            flavor="base",
        )
        locked_empty = fetch_prices(
            start=start + 4 * STEP_1H_US,
            end=start + 4 * STEP_1H_US,
            instrument="AAA",
            interval="1h",
            flavor="base",
        )

        assert seeded.num_rows == 1
        assert attempted.num_rows == 1
        assert locked_nonempty.num_rows == 1
        assert locked_empty.num_rows == 0
        assert len(endpoint.calls) == 2
        assert endpoint.calls[1]["start"] == start + STEP_1H_US
    finally:
        close_ls()


def test_ls_cache_current_watermarks_preserve_premax_cache_hits(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers current/future metadata still using sys_micros while older ranges stay cache-only."""
    start = dt_micros(2026, 12, 1, 9)
    datasets = {
        ("1h", "AAA", "base"): [(start, 400.25, 400.75)],
    }
    endpoint = DeterministicEndpoint(datasets)

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "flavor", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument", "flavor"),
        table_keys=("interval",),
        out_cols=("ts", "open", "close"),
        table="current_watermark_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def fetch_prices(start, end, instrument, interval, flavor="base"):
        return endpoint(start, end, instrument, interval, flavor)

    try:
        seeded = fetch_prices(start=start, end=start, instrument="AAA", interval="1h", flavor="base")
        premax = fetch_prices(
            start=start - 2 * STEP_1H_US,
            end=start - STEP_1H_US,
            instrument="AAA",
            interval="1h",
            flavor="base",
        )

        assert seeded.num_rows == 1
        assert premax.num_rows == 0
        assert len(endpoint.calls) == 1
    finally:
        close_ls()


def test_ls_cache_uses_default_out_cols_and_open_ended_reads(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers default out-col inference plus the <=, >=, and fully open SQL range selection paths."""
    start = dt_micros(2026, 4, 1, 9)
    datasets = {
        ("AAA",): make_hourly_rows(start, 4, 50.0),
    }

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: datasets["AAA",][-1][0] + STEP_1H_US)
    launch_ls()

    @ls_cache(
        columns={"instrument": 0, "ts": 1, "open": 2, "close": 3},
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument",),
        table="open_range_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def fetch_prices(start, end, instrument):
        return price_table(rows_in_range(datasets[instrument,], start, end))

    try:
        seeded = fetch_prices(start=start, end=datasets["AAA",][-1][0], instrument="AAA")
        left_open = fetch_prices(start=None, end=datasets["AAA",][1][0], instrument="AAA")
        right_open = fetch_prices(start=datasets["AAA",][2][0], end=None, instrument="AAA")
        fully_open = fetch_prices(start=None, end=None, instrument="AAA")

        assert seeded.column_names == ["ts", "open", "close"]
        assert left_open.num_rows == 2
        assert right_open.num_rows == 2
        assert fully_open.num_rows == 4
    finally:
        close_ls()


def test_ls_cache_returns_empty_for_seedless_ranges(monkeypatch: pytest.MonkeyPatch, default_db_path: Path) -> None:
    """Covers the empty-seed early return path where the wrapped endpoint has no rows to initialize."""
    start = dt_micros(2026, 5, 1, 9)

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument",),
        table="empty_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def empty_fetch(start, end, instrument):
        return empty_prices()

    try:
        assert empty_fetch(start=start, end=start, instrument="AAA").num_rows == 0
    finally:
        close_ls()


def test_ls_cache_returns_empty_for_premax_ranges(monkeypatch: pytest.MonkeyPatch, default_db_path: Path) -> None:
    """Covers the stale empty-range branch that returns immediately when the requested end is older than cached max."""
    start = dt_micros(2026, 5, 1, 9)
    datasets = {
        ("AAA",): make_hourly_rows(start, 3, 75.0),
    }

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument",),
        table="premax_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def fetch_prices(start, end, instrument):
        return price_table(rows_in_range(datasets[instrument,], start, end))

    try:
        fetch_prices(start=start, end=datasets["AAA",][-1][0], instrument="AAA")
        empty = fetch_prices(
            start=start - 4 * STEP_1H_US,
            end=start - STEP_1H_US,
            instrument="AAA",
        )
        assert empty.num_rows == 0
    finally:
        close_ls()


def test_ls_cache_extends_empty_requested_tail_with_later_endpoint_rows(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers stale extensions where the cached slice is empty but the endpoint returns later rows."""
    start = dt_micros(2026, 5, 8, 9)
    late = start + 3 * STEP_1H_US
    datasets = {
        ("AAA",): [
            (start, 90.25, 90.75),
            (start + STEP_1H_US, 91.25, 91.75),
            (late, 93.25, 93.75),
        ],
    }

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument",),
        table="empty_tail_extension_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def fetch_prices(start, end, instrument):
        return price_table(rows_in_range(datasets[instrument,], start, end))

    try:
        fetch_prices(start=start, end=start + STEP_1H_US, instrument="AAA")
        extended = fetch_prices(start=late, end=late, instrument="AAA")

        assert extended["ts"].to_pylist() == [late]
        assert extended["open"].to_pylist() == [93.25]
    finally:
        close_ls()


def test_threadpool_shutdown_ls_handles_empty_executors() -> None:
    """Covers the shutdown fast path when a thread pool has not started any worker threads yet."""
    pool = ThreadPoolExecutor(max_workers=1, thread_name_prefix="liteseries-empty")
    threadpool_shutdown_ls(pool)


def test_launch_ls_accepts_mem_rep_flag_with_existing_files(default_db_path: Path) -> None:
    """Covers the mem_rep launch branch without asserting replication behavior yet."""
    launch_ls(mem_rep=True)
    close_ls()


@pytest.mark.timeout(10)
@pytest.mark.xfail(
    run=False,
    reason="Concurrent stale-write support is deferred; current multithread boilerplate is intentionally incomplete.",
)
def test_ls_cache_supports_persistent_threadpool_reads_and_writes(
    monkeypatch: pytest.MonkeyPatch, default_db_path: Path
) -> None:
    """Covers thread-local connections across a persistent executor while readers and stale writers share one SQLite file."""
    start = dt_micros(2026, 2, 2, 9)
    datasets = {
        ("1h", "AAA", "base"): make_hourly_rows(start, 6, 100.0),
        ("1h", "BBB", "base"): make_hourly_rows(start, 6, 200.0),
        ("1h", "CCC", "base"): make_hourly_rows(start, 6, 300.0),
    }
    endpoint = DeterministicEndpoint(datasets)

    monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
    launch_ls()

    @ls_cache(
        columns=("instrument", "flavor", "ts", "open", "close"),
        time_keys=("start", "end"),
        time_col="ts",
        column_keys=("instrument", "flavor"),
        table_keys=("interval",),
        out_cols=("ts", "open", "close"),
        table="threaded_prices",
        refresh_period=timedelta(hours=1),
        active_in=UTC_WINDOW,
    )
    def fetch_prices(start, end, instrument, interval, flavor="base"):
        return endpoint(start, end, instrument, interval, flavor)

    try:
        seed_end = datasets["1h", "AAA", "base"][2][0]
        fetch_prices(start=start, end=seed_end, instrument="AAA", interval="1h", flavor="base")
        fetch_prices(start=start, end=seed_end, instrument="BBB", interval="1h", flavor="base")
        fetch_prices(start=start, end=seed_end, instrument="CCC", interval="1h", flavor="base")

        fresh_now = datasets["1h", "CCC", "base"][-1][0] + STEP_1H_US
        monkeypatch.setattr("liteseries._util.sys_micros", lambda: fresh_now)
        full_end = datasets["1h", "CCC", "base"][4][0]
        expected_reader = fetch_prices(start=start, end=full_end, instrument="CCC", interval="1h", flavor="base")

        monkeypatch.setattr("liteseries._util.sys_micros", lambda: 0)
        endpoint.block_writes_for(("1h", "AAA", "base"), ("1h", "BBB", "base"))
        writer_end = datasets["1h", "AAA", "base"][4][0]

        def writer(symbol: str) -> pa.Table:
            return fetch_prices(start=start, end=writer_end, instrument=symbol, interval="1h", flavor="base")

        def reader() -> pa.Table:
            return fetch_prices(start=start, end=full_end, instrument="CCC", interval="1h", flavor="base")

        pool = ThreadPoolExecutor(max_workers=3, thread_name_prefix="liteseries-test")
        try:
            futures = [
                pool.submit(writer, "AAA"),
                pool.submit(writer, "BBB"),
                pool.submit(reader),
                pool.submit(reader),
            ]
            results = [future.result() for future in futures]
        finally:
            endpoint.clear_gate()
            threadpool_shutdown_ls(pool)

        assert results[0].num_rows == 5
        assert results[1].num_rows == 5
        assert results[2].equals(expected_reader)
        assert results[3].equals(expected_reader)
        worker_threads = {call["thread"] for call in endpoint.calls if isinstance(call["thread"], int)}
        assert len(worker_threads) >= 2
    finally:
        close_ls()
