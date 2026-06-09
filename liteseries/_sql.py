from __future__ import annotations

import os

TABLE_EXISTS = "SELECT 1 FROM pragma_table_list WHERE name = ?   AND type = 'table' LIMIT 1"

TABLE_COLUMNS = "SELECT name FROM pragma_table_xinfo(?) WHERE hidden = 0 ORDER BY cid"

LAST_UPD = "last_upd"

_ASEQ_C = {}
_QMRK_C = {}
_REQ_C = {}
_INS_C = {}
_UPD_C = {}


def _protect_names_enabled() -> bool:
    """Return whether SQL identifiers should be double-quoted."""
    return os.getenv("LITESERIES_PROTECTNAMES", "").casefold() == "true"


def _qident_protected(ident: str) -> str:
    """Quote an SQLite identifier and escape embedded double quotes."""
    dquote, escaped_dquote = '"', '""'
    return f'"{ident.replace(dquote, escaped_dquote)}"'


def _qident_plain(ident: str) -> str:
    """Return an already-valid SQLite identifier without allocating a wrapper."""
    return ident


# Keep the hot path as a direct function binding chosen once at import time.
qident = _qident_protected if _protect_names_enabled() else _qident_plain


def and_seq(cols: tuple):
    rs = _ASEQ_C.get(cols)
    if rs is None:
        rs = " AND ".join(f"{qident(col)} = ?" for col in cols)
        _ASEQ_C[cols] = rs
    return rs


def qmarks(width: int):  # pragma: no cover
    rs = _QMRK_C.get(width)
    if rs is None:
        rs = ", ".join("?" for _ in range(width))
        _QMRK_C[width] = rs
    return rs


def colreq(cols: tuple):
    rs = _REQ_C.get(cols)
    if rs is None:
        rs = ", ".join(qident(col) for col in cols)
        _REQ_C[cols] = rs
    return rs


def series_range_select(table_ref: str, req_cols: tuple, pidx: tuple, vidx, time_col, srange, erange):
    sn, en = srange is None, erange is None
    cl, ps = colreq(req_cols), and_seq(pidx)
    tb, tc = qident(table_ref), qident(time_col)
    if sn and en:
        return f"SELECT {cl} FROM {tb} WHERE {ps}", vidx
    if sn:
        return f"SELECT {cl} FROM {tb} WHERE {ps} AND {tc} <= ?", (*vidx, erange)
    if en:
        return f"SELECT {cl} FROM {tb} WHERE {ps} AND {tc} >= ?", (*vidx, srange)
    # other is case 2
    return (f"SELECT {cl} FROM {tb} WHERE {ps} AND {tc} >= ? AND {tc} <= ?", (*vidx, srange, erange))


def series_tmax_select(table_ref: str, pidx: tuple, time_col) -> str:
    return f"SELECT MAX({qident(time_col)}) FROM {qident(table_ref)} WHERE {and_seq(pidx)}"


def last_upd_select(table_ref: str, pidx: tuple) -> str:
    return f"SELECT {qident(LAST_UPD)} FROM {qident(table_ref)} WHERE {and_seq(pidx)}"


def insert_cols(table_ref: str, cols: tuple[str, ...]) -> str:
    key = (table_ref, cols)
    rs = _INS_C.get(key)
    if rs is None:
        rs = f"INSERT INTO {qident(table_ref)} ({colreq(cols)}) VALUES ({qmarks(len(cols))})"
        _INS_C[key] = rs
    return rs


def update_last_upd(table_ref: str, pidx: tuple[str, ...]) -> str:
    key = (table_ref, pidx)
    rs = _UPD_C.get(key)
    if rs is None:
        rs = f"UPDATE {qident(table_ref)} SET {qident(LAST_UPD)} = ? WHERE {and_seq(pidx)}"
        _UPD_C[key] = rs
    return rs


# change this to a cached statement later, we can keep using ingest, but for small
def insert_row(table_ref: str, width: int) -> str:  # pragma: no cover
    return f"INSERT INTO {qident(table_ref)} VALUES ({qmarks(width)})"
