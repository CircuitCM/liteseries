TABLE_EXISTS = (
    "SELECT 1 "
    "FROM pragma_table_list "
    "WHERE name = ? "
    "  AND type = 'table' "
    "LIMIT 1"
)

TABLE_EXISTS_IN_SCHEMA = (
    "SELECT 1 "
    "FROM pragma_table_list "
    "WHERE schema = ? "
    "  AND name = ? "
    "  AND type = 'table' "
    "LIMIT 1"
)

TABLE_COLUMNS = (
    "SELECT name "
    "FROM pragma_table_xinfo(?) "
    "WHERE hidden = 0 "
    "ORDER BY cid"
)

LAST_UPD='last_upd'

_ASEQ_C={}
_QMRK_C={}
_REQ_C={}

def and_seq(cols:tuple):
    if (rs:=_ASEQ_C.get(cols,None)) is None:
        rs=" AND ".join(f"{col} = ?" for col in cols)
        _ASEQ_C[cols]=rs
    return rs

def qmarks(width):
    if (rs:=_QMRK_C.get(width,None)) is None:
        rs=', '.join('?' for _ in range(width))
        _QMRK_C[width]=rs
    return rs

def colreq(cols:tuple):
    if (rs:=_REQ_C.get(cols,None)) is None:
        rs=", ".join(cols)
        _REQ_C[cols]=rs
    return rs


def series_range_select(table_ref:str, req_cols:tuple, pidx:tuple, vidx, time_col, srange, erange):
    sn,en=srange is None, erange is None
    cl,ps=colreq(req_cols),and_seq(pidx)
    if sn and en: return f"SELECT {cl} FROM {table_ref} WHERE {ps}", vidx
    if sn: return f"SELECT {cl} FROM {table_ref} WHERE {ps} AND {time_col} <= ?", (*vidx, erange)
    if en: return f"SELECT {cl} FROM {table_ref} WHERE {ps} AND {time_col} >= ?", (*vidx, srange)
    #other is case 2
    return (f"SELECT {cl} FROM {table_ref} WHERE {ps} AND {time_col} >= ? AND {time_col} <= ?",
            (*vidx,srange,erange))

def series_tmax_select(table_ref:str, pidx:tuple, time_col):
    return f"SELECT MAX({time_col}) FROM {table_ref} WHERE {and_seq(pidx)}"

def last_upd_select(table_ref: str,pidx:tuple):
    return f"SELECT {LAST_UPD} FROM {table_ref} WHERE {and_seq(pidx)}"

#change this to a cached statement later, we can keep using ingest, but for small
def insert_row(table_ref: str, width: int) -> str:
    return f"INSERT INTO {table_ref} VALUES ({qmarks(width)})"
