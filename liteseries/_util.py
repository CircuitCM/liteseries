from __future__ import annotations

import os
import time
from collections.abc import Sequence
from itertools import chain
from pathlib import Path

import pyarrow as pa

from . import _sql

LAST_UPD = _sql.LAST_UPD


_FIRST_IMPORT_ROOT: Path | None = None


def _cached_import_root() -> Path:
    global _FIRST_IMPORT_ROOT
    if _FIRST_IMPORT_ROOT is None:
        main_file = getattr(__import__("__main__"), "__file__", None)
        if main_file is not None:
            _FIRST_IMPORT_ROOT = Path(main_file).resolve().parent
        else:
            _FIRST_IMPORT_ROOT = Path.cwd()
    return _FIRST_IMPORT_ROOT


def _pick_sqlite_file(root: Path) -> Path | None:
    sqlite_files = list(root.glob("*.sqlite"))
    if not sqlite_files:
        return None

    for path in sqlite_files:
        if "liteseries" in path.stem.casefold():
            return path

    return sqlite_files[0]


def get_dburi(path: str | None) -> str:
    if path is not None:
        db_path = Path(path).expanduser()
        if not db_path.is_file():
            raise FileNotFoundError(f"SQLite database file does not exist: {db_path}")
        return str(db_path.resolve())

    env_path = os.getenv("LITESERIES_DB")
    if env_path:
        return env_path

    root = _cached_import_root()
    sqlite_file = _pick_sqlite_file(root)
    if sqlite_file is not None:
        return str(sqlite_file.resolve())

    db_path = root / "liteseries_db.sqlite"
    db_path.touch(exist_ok=True)
    print(f"Created new sqlite db at {db_path}")
    return str(db_path.resolve())


def sys_micros() -> int:
    # The less expensive call, not perf counter.
    return int(time.time() * 1_000_000)


def table_exists(cur, table: str, schema: str | None = None) -> bool:
    if schema is None:
        cur.execute(_sql.TABLE_EXISTS, (table,))
    else:
        cur.execute(_sql.TABLE_EXISTS_IN_SCHEMA, (schema, table))

    return cur.fetchone() is not None


def insert_row_stmt(cur, table: str) -> str:
    cur.execute(_sql.TABLE_COLUMNS, (table,))
    return _sql.insert_row(table, len(cur.fetchall()))


def infer_sqlite_types(cur, data: pa.Table, sample_rows: int = 2) -> dict[str, str]:
    tb = "_temp_types"
    sample = data.slice(0, sample_rows)
    cur.adbc_ingest(tb, sample, mode="create", temporary=True)
    cur.execute(f"PRAGMA table_info('{tb}')")
    type_rows = cur.fetchall()
    cur.execute(f"DROP TABLE {tb}")
    return {column_name: column_type for _, column_name, column_type, *_ in type_rows}


def define_ls_table(
    table_ref: str,
    col_ord: dict[str, int],
    col_types: dict[str, str],
    column_keys: Sequence[str],
    time_col: str,
) -> str:
    cols = sorted(col_ord, key=col_ord.__getitem__)  # in case we change the system later...
    defs = (f"{col} {col_types[col]} NOT NULL" for col in cols)
    pk = f"PRIMARY KEY ({', '.join(chain(column_keys, (time_col,)))})"
    ddl = f"CREATE TABLE {table_ref} ({', '.join((*defs, pk))}) STRICT, WITHOUT ROWID"
    return ddl


def define_ls_infotable(
    table_ref: str,
    col_types: dict[str, str],
    column_keys: Sequence[str],
) -> str:
    nfks = column_keys
    # Everything but the final rightmost key which is the unix micros.
    defs = (f"{col} {col_types.get(col, 'INTEGER')} NOT NULL" for col in chain(nfks, (LAST_UPD,)))
    pk = f"PRIMARY KEY ({', '.join(nfks)})"
    ddl = f"CREATE TABLE {table_ref} ({', '.join((*defs, pk))}) STRICT, WITHOUT ROWID"
    return ddl


def mk_fullarrow(ar_tbl: pa.Table, full_cols, col_k, col_v):
    names0 = ar_tbl.column_names
    ln = ar_tbl.num_rows
    # Fills the table with values if not in ar_tbl already.
    cols = {name: ar_tbl[name] for name in names0} | {
        name: pa.repeat(v, ln) for name, v in zip(col_k, col_v, strict=True) if name not in names0
    }

    names = sorted(cols, key=lambda name: full_cols.get(name, len(full_cols)))
    return pa.table([cols[name] for name in names], names=names)
