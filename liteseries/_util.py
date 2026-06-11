from __future__ import annotations

import os
import time
from collections.abc import Sequence
from itertools import chain
from pathlib import Path
from typing import NamedTuple

import pyarrow as pa

from . import _sql

LAST_UPD = _sql.LAST_UPD


_FIRST_IMPORT_ROOT: Path | None = None
_DEFAULT_DB_NAME = "liteseries_db.sqlite"


def _cached_import_root() -> Path:
    global _FIRST_IMPORT_ROOT
    if _FIRST_IMPORT_ROOT is None:
        main_file = getattr(__import__("__main__"), "__file__", None)
        _FIRST_IMPORT_ROOT = Path(main_file).resolve().parent if main_file is not None else Path.cwd()
    return _FIRST_IMPORT_ROOT


def _pick_sqlite_file(root: Path) -> Path | None:
    sqlite_files = list(root.glob("*.sqlite"))
    if not sqlite_files:
        return None

    for path in sqlite_files:
        if "liteseries" in path.stem.casefold():
            return path

    return sqlite_files[0]


def _is_file_uri(path: str) -> bool:
    return path.startswith("file:")


def _looks_like_dir(path: str) -> bool:
    return path.endswith(("/", "\\"))


def _sqlite_path(path: str) -> Path:
    db_path = Path(path).expanduser()
    if db_path.is_dir() or _looks_like_dir(path):
        return db_path / _DEFAULT_DB_NAME
    if db_path.suffix.casefold() != ".sqlite":
        return db_path.with_suffix(".sqlite")
    return db_path


def _touch_sqlite(db_path: Path) -> str:
    pte = db_path.exists()
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_path.touch(exist_ok=True)
    if not pte:
        print(f"Created new sqlite db at {db_path}")
    return str(db_path.resolve())


def get_dburi(path: str | None) -> str:
    if path is not None:
        if _is_file_uri(path):
            return path
        return _touch_sqlite(_sqlite_path(path))

    env_path = os.getenv("LITESERIES_DB")
    if env_path:
        if _is_file_uri(env_path):
            return env_path
        return _touch_sqlite(_sqlite_path(env_path))

    root = _cached_import_root()
    sqlite_file = _pick_sqlite_file(root)
    if sqlite_file is not None:
        return _touch_sqlite(sqlite_file)

    return _touch_sqlite(root / _DEFAULT_DB_NAME)


def sys_micros() -> int:
    # The less expensive call, not perf counter.
    return int(time.time() * 1_000_000)


def table_exists(cur, table: str) -> bool:  # pragma: no cover
    cur.execute(_sql.TABLE_EXISTS, (table,))
    return cur.fetchone() is not None


def insert_row_stmt(cur, table: str) -> str:  # pragma: no cover
    cur.execute(_sql.TABLE_COLUMNS, (table,))
    return _sql.insert_row(table, len(cur.fetchall()))


def infer_sqlite_types(cur, data: pa.Table, sample_rows: int = 2) -> dict[str, str]:
    tb = "_temp_types"
    sample = data.slice(0, sample_rows)
    cur.adbc_ingest(tb, sample, mode="create", temporary=True)
    cur.execute(f"PRAGMA table_info({_sql.qident(tb)})")
    type_rows = cur.fetchall()
    cur.execute(f"DROP TABLE {_sql.qident(tb)}")
    return {column_name: column_type for _, column_name, column_type, *_ in type_rows}


def define_ls_table(
    table_ref: str,
    col_ord: dict[str, int],
    col_types: dict[str, str],
    column_keys: Sequence[str],
    time_col: str,
) -> str:
    cols = sorted(col_ord, key=col_ord.__getitem__)  # in case we change the system later...
    defs = (f"{_sql.qident(col)} {col_types[col]} NOT NULL" for col in cols)
    pk = f"PRIMARY KEY ({', '.join(_sql.qident(col) for col in chain(column_keys, (time_col,)))})"
    return f"CREATE TABLE IF NOT EXISTS {_sql.qident(table_ref)} ({', '.join((*defs, pk))}) STRICT, WITHOUT ROWID"


def define_ls_infotable(
    table_ref: str,
    col_types: dict[str, str],
    column_keys: Sequence[str],
) -> str:
    nfks = column_keys
    # Everything but the final rightmost key which is the unix micros.
    defs = (f"{_sql.qident(col)} {col_types.get(col, 'INTEGER')} NOT NULL" for col in chain(nfks, (LAST_UPD,)))
    pk = f"PRIMARY KEY ({', '.join(_sql.qident(col) for col in nfks)})"
    return f"CREATE TABLE IF NOT EXISTS {_sql.qident(table_ref)} ({', '.join((*defs, pk))}) STRICT, WITHOUT ROWID"


def mk_fullarrow(ar_tbl: pa.Table, full_cols, col_k, col_v):
    names0 = set(ar_tbl.column_names)
    ln = ar_tbl.num_rows
    # Reuse existing Arrow column views; only missing key columns allocate filled arrays.
    new_cols = {
        name: pa.repeat(v, ln) for name, v in zip(col_k, col_v, strict=True) if name not in names0
    }
    names = sorted((*ar_tbl.column_names, *new_cols), key=lambda name: full_cols.get(name, len(full_cols)))
    cols = [ar_tbl.column(name) if name in names0 else new_cols[name] for name in names]

    return pa.Table.from_arrays(cols, names=names)


class Rollback(NamedTuple):
    adjust_columns: Sequence[str]
    included_keys: dict[str, set[str | None]] | None = None
    rebuild: bool = False
