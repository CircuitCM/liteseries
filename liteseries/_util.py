import time
from collections.abc import Sequence
from itertools import chain

import pyarrow as pa

from . import _sql

LAST_UPD=_sql.LAST_UPD

def get_dburi():
    pass

def sys_micros():
    #The less expensive call, not perf counter.
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
    tb='_temp_types'
    sample = data.slice(0, sample_rows)
    cur.adbc_ingest(tb, sample, mode="create", temporary=True)
    cur.execute(f"PRAGMA table_info('{tb}')")
    type_rows = cur.fetchall()
    cur.execute(f'DROP TABLE {tb}')
    return {column_name: column_type for _, column_name, column_type, *_ in type_rows}


def define_ls_table(
    table_ref: str,
    col_ord: dict[str, int],
    col_types: dict[str, str],
    column_keys: Sequence[str],
    time_col:str,
) -> str:
    cols = sorted(col_ord, key=col_ord.__getitem__) #in case we change the system later...
    defs = (f"{col} {col_types[col]} NOT NULL" for col in cols)
    pk = f"PRIMARY KEY ({', '.join(chain(column_keys,(time_col,)))})"
    ddl = f"CREATE TABLE {table_ref} ({', '.join((*defs, pk))}) STRICT, WITHOUT ROWID"
    return ddl

def define_ls_infotable(    
    table_ref: str,
    col_types: dict[str, str],
    column_keys: Sequence[str],
):
    nfks=column_keys
    #Everything but the final rightmost key which is the unix micros.
    defs = (f"{col} {col_types.get(col,'INTEGER')} NOT NULL" for col in chain(nfks,(LAST_UPD,)))
    pk = f"PRIMARY KEY ({', '.join(nfks)})"
    ddl = f"CREATE TABLE {table_ref} ({', '.join((*defs, pk))}) STRICT, WITHOUT ROWID"
    return ddl

def mk_fullarrow(ar_tbl: pa.Table,full_cols,col_k, col_v):
    names0 = ar_tbl.column_names
    ln=ar_tbl.num_columns
    #Fills the table with values if not in ar_tbl already.
    cols = {name: ar_tbl[name] for name in names0
            } | {name: pa.repeat(v,ln) for name, v in zip(col_k, col_v) if name not in names0}

    names = sorted(cols, key=lambda name: full_cols.get(name, len(full_cols)))
    return pa.table([cols[name] for name in names], names=names)
