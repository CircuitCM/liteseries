from __future__ import annotations

TABLE_EXISTS = "SELECT 1 FROM pragma_table_list WHERE name = ?   AND type = 'table' LIMIT 1"

TABLE_COLUMNS = "SELECT name FROM pragma_table_xinfo(?) WHERE hidden = 0 ORDER BY cid"

LAST_UPD = "last_upd"

_ASEQ_C = {}
_QMRK_C = {}
_REQ_C = {}


def qident(ident: str) -> str:
    return f'"{ident.replace("\"", "\"\"")}"'


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


# change this to a cached statement later, we can keep using ingest, but for small
def insert_row(table_ref: str, width: int) -> str:  # pragma: no cover
    return f"INSERT INTO {qident(table_ref)} VALUES ({qmarks(width)})"
