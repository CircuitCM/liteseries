from __future__ import annotations

import os
from datetime import UTC
from datetime import datetime
from datetime import time
from datetime import timedelta

import pandas as pd
import pyarrow as pa
import yfinance as yf
from pyarrow import concat_tables

from liteseries import close_ls
from liteseries import launch_ls
from liteseries import ls_cache

os.environ["LITESERIES_PROTECTNAMES"]='true'

launch_ls()


def dt_micros(year: int, month: int, day: int) -> int:
    return int(datetime(year, month, day, tzinfo=UTC).timestamp() * 1_000_000)


def yf_to_arrow(frame: pd.DataFrame|None) -> pa.Table|None:
    if frame is None: return None
    frame = frame.rename_axis("Date").reset_index()
    frame["ts"] = pd.to_datetime(frame.pop("Date"),utc=True).astype("int64")*1_000_000
    frame["Volume"] = frame["Volume"].fillna(0).astype("int64")
    return pa.Table.from_pandas(frame, preserve_index=False)

    

def yf_prices_full(start, end, ticker, interval, auto_adjust=False) -> pa.Table|None:
    start_dt = pd.Timestamp(start, unit="us", tz="UTC").to_pydatetime() if start is not None else None
    end_dt = pd.Timestamp(end, unit="us", tz="UTC").to_pydatetime() + pd.Timedelta(days=1) if end is not None else None
    now_utc = datetime.now(UTC)

    def download_window(start_dt, end_dt)->pd.DataFrame|None:
        return yf.download(
            tickers=ticker,
            start=start_dt,
            end=end_dt,
            interval=interval,
            auto_adjust=auto_adjust,
            actions=False,
            progress=False,
            threads=True,
            multi_level_index=False,
        )

    if interval != "1m" and start_dt is None:
        end_dt = None

    if interval == "1m":
        if start_dt is None:
            start_dt = now_utc - timedelta(days=30)
        if end_dt is None:
            end_dt = now_utc

        if end_dt - start_dt > timedelta(days=8):
            chunks: list[pa.Table] = []
            chunk_start = start_dt
            while chunk_start < end_dt:
                chunk_end = min(chunk_start + timedelta(days=8), end_dt)
                frame = download_window(chunk_start, chunk_end)
                if frame is not None and not frame.empty:
                    chunks.append(yf_to_arrow(frame)) # type: ignore
                chunk_start = chunk_end
            if len(chunks)!=0:
                return concat_tables(chunks, promote_options="none")
            if not chunks:
                return yf_to_arrow(download_window(start_dt, end_dt))
            return concat_tables(chunks, promote_options="none")

    return yf_to_arrow(download_window(start_dt, end_dt))


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
def yf_prices(start, end, ticker, interval):
    return yf_prices_full(start, end, ticker, interval, auto_adjust=False)


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
def yf_prices_adj(start, end, ticker, interval):
    return yf_prices_full(start, end, ticker, interval, auto_adjust=True)


def main() -> None:
    try:
        start = None #dt_micros(2026, 4, 8)
        end = None #dt_micros(2026, 3, 25)
        print(start, end)
        
        raw_first = yf_prices(start=start, end=end, ticker="MSFT", interval="1m")
        raw_second = yf_prices(start=start, end=end, ticker="MSFT", interval="1m")
        adj_first = yf_prices_adj(start=start, end=end, ticker="MSFT", interval="5m")
        adj_second = yf_prices_adj(start=start, end=end, ticker="MSFT", interval="5m")

        print("raw columns:", raw_first.column_names)
        print("raw rows:", raw_first.num_rows)
        print("raw cached second call:", raw_second.num_rows == raw_first.num_rows)
        print(raw_second)
        print()
        print("adjusted columns:", adj_first.column_names)
        print("adjusted rows:", adj_first.num_rows)
        print("adjusted cached second call:", adj_second.num_rows == adj_first.num_rows)
        print(adj_second)
        
        #print(yf_prices_full(start,end,"MSFT",'1m'))
    finally:
        close_ls()


if __name__ == "__main__":
    main()
