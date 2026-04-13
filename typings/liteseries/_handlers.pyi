import threading as th
from datetime import time, timedelta
from typing import Callable, TypeAlias
from pyarrow import Table

CacheDecorator: TypeAlias = Callable[[SeriesFn], SeriesFn]

def close_ls() -> None: ...
def threadpool_shutdown_ls(thp) -> None: ...
def launch_ls(pathuri=None, mem_rep: bool = False, schema: str = "liteseries") -> None: ...
def ls_cache(
    columns,
    time_keys: tuple[str, str],
    time_col: str,
    column_keys: tuple,
    refresh_period: timedelta = ...,
    active_in: time | tuple[time, time] = ...,
    out_cols=None,
    table_keys=None,
    rollback: bool = False,
    table=None,
) -> CacheDecorator: ...
