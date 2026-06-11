from __future__ import annotations

import sqlite3
import threading as th
from collections.abc import Callable
from datetime import datetime, time, timedelta
from functools import wraps
from typing import Any
from urllib.parse import quote

import pyarrow as pa
import pyarrow.compute as pc
from adbc_driver_sqlite import dbapi
from dateutil import tz
from pyarrow import Table, concat_tables

from . import _util as ut
from ._sql import (
    insert_cols,
    last_upd_select,
    qident,
    rollback_adjust_update,
    rollback_rebuild_update,
    series_range_select,
    series_tmax_select,
    update_last_upd,
)
from ._util import Rollback

LT = list | tuple
TimeWindow = tuple[time, time]
SeriesFn = Callable[..., Table]
CacheDecorator = Callable[[SeriesFn], SeriesFn]
DEFAULT_ACTIVE_IN = time(hour=16, second=1, tzinfo=tz.gettz("US/Eastern"))
F32_WIDTH = 32
EP32 = 16 * (2**-23)
EP64 = 16 * (2**-52)
NEW_KEY = 1
NEW_TABLE = 2


class LocalADBC(th.local):
    uri = None  # set from another scope

    def __init__(self) -> None:
        self.sqlite = dbapi.connect(uri=self.uri, autocommit=False)
        self.cur = self.sqlite.cursor()
        self.cur.execute("PRAGMA busy_timeout = 1000")
        #self.sqlite.commit()

    def close(self) -> None:
        self.cur.close()
        self.sqlite.close()


local_adbc: LocalADBC


def close_ls() -> None:
    local_adbc.close()


def threadpool_shutdown_ls(thp) -> None:
    live = len(thp._threads)
    if not live:
        return thp.shutdown(wait=True)

    gate = th.Barrier(live + 1)
    for _ in range(live):
        thp.submit(lambda: (gate.wait(), close_ls()))
    gate.wait()
    return thp.shutdown(wait=True)


def launch_ls(pathuri=None, mem_rep: bool = False) -> None:
    dburi = ut.get_dburi(pathuri)
    if not mem_rep:
        sqlite_con = sqlite3.connect(dburi)
        try:
            sqlite_con.execute("PRAGMA journal_mode=WAL")  # ...idk man
            sqlite_con.commit()
        finally:
            sqlite_con.close()

    if mem_rep:
        LocalADBC.uri = dburi
    elif dburi.startswith("file:"):
        sep = "&" if "?" in dburi else "?"
        LocalADBC.uri = f"{dburi}{sep}cache=shared"
    else:
        qpath = quote(dburi.replace("\\", "/"), safe="/:")
        LocalADBC.uri = f"file:{qpath}?mode=rwc&cache=shared"
    global local_adbc
    local_adbc = LocalADBC()
    # The connection container is now initialized for the current thread.
    # But because it's a threading local, a new object is created for each new thread, also notice that this is not
    # a new connection every time a task is launched in a thread. So long as the thread stays alive and receives new
    # work, this connection will stay alive with it. This also makes the system universally compatible with any thread
    # executor because it simply doesn't interact with them explicitly.

def inftime_bound(edate, lsq) -> int:
    if edate is None or edate >= lsq:
        return ut.sys_micros()
    return edate


_24H = timedelta(days=1)
_0D = timedelta()
_1MC = timedelta(microseconds=1)


def ls_cache(
    columns,
    time_keys: tuple[str, str],
    time_col: str,
    column_keys: tuple,  # need at least one. may change this req at some point.
    refresh_period: timedelta = timedelta(days=1),
    active_in: time | tuple[time, time] = DEFAULT_ACTIVE_IN,
    out_cols=None,
    table_keys=None,
    expires_after: timedelta | None = None,
    rollback: Rollback | None = None,
    table=None,
) -> CacheDecorator:
    """

    Note: ``keys`` refer to a named kwargs that are used as a column value for all rows, or as an appended
    extension of the table name. Assumption is they appear as input values in the data function but not in the output
    array.

    :param columns: All column names that come are included in the sqlite table. Implicitly # of rows, and order of 
    columns. If the func endpoint doesn't have all those columns, they will be filled by those in column_keys, it's
    possible that we can do without the extra column keys need.
    :param refresh_period: The period of passed time necessary to elicit an update from the timeseries endpoint. This
        time period pass is calculated using the active_in inclusive range for less-than daily. For daily
        periods or greater, we assume time_keys is a single time that marks (assuming the current day) when it is valid
        to query the timeseries endpoint, think EOD OHLC at 4 pm EST. Or we take the second time of the sequence.
    :param active_in: Intervals are generated starting at active_in[0] and updated additively from the period. When the
        most recent update period is less than today + min(floor_last_pd,active_in[1]), we request an update. Outside of
        0 and 1 range, we only load data up to that previous range, until the current time intersects with active_in[0]
        again.
    :param time_keys: The two named args that represent the starting and ending date time of the query selection.
    :param table: Table name, if None we use the data function name.
    :param table_keys: keys that make the full table name (example timeframes 1m, 1h, 1s).
    :param column_keys: keys that are included into the database as values for it's column. These will default to the
        function output columns, and otherwise fill values from the matching input kwargs.
    :param out_cols: Specifically, the ordered columns of the arrow table that will be produced by this wrapper. Always
    less than or equal to columns.
    :param rollback: Optional overlap adjustment for series whose historical values can be restated.
        If we are querying new data, then we include the latest existing datetime in our new data query,
        if the returned row is not equal to the row from our database, then we log a warning, assume
        that timeseries entries for that specific matching key group are obsolete, remove them then
        place them again backfilled to the first date (note replacing row values) likely much quicker
        for this strategy than full removal then reload.
    :return:
    """
    tk, tak, ck = time_keys, () if table_keys is None else table_keys, column_keys
    if not isinstance(columns, dict):
        columns = {columns[i]: i for i in range(len(columns))}
    if out_cols is None:
        # This is a backup that could fail, recommended to set the actual
        # output columns of the function in matching order.
        out_cols = (*(cl for cl in columns if cl not in column_keys),)

    refr_micros = int(refresh_period / _1MC)
    if expires_after is not None:
        exp_micros = max(int(expires_after / _1MC), refr_micros * 2)

    def make_keys(kg):
        sdate, edate = kg[tk[0]], kg[tk[1]]  # intentional fail if NE
        tav = (*(kg[k] for k in tak),)
        cv = (*(kg[k] for k in ck),)
        return sdate, edate, tav, cv

    def fix_range(sdt, edt, kg):
        # for simplicity now we will assume the datetime start and end has also been converted to micros pre-wrapper
        kg[tk[0]] = sdt
        kg[tk[1]] = edt

        return kg

    if refresh_period >= _24H:
        #So the current method actually retries on a 24hr cycle... basically anything larger is just treated as 1 day.
        #In a future version this will be handled correctly, and support relativedelta and daylight savings.
        #time after this period is valid.
        doff = active_in[1] if not isinstance(active_in, time) else active_in

        def last_qual() -> int:
            # ltime is unix micros
            # curn=datetime.fromtimestamp(ltime,dt.UTC) #timezone should be irrelevant but if issues, use doff's
            # today
            td = datetime.now(doff.tzinfo)
            tod = td.date()
            # yesterday or today if after the inactive period.
            pperiod = datetime.combine(tod, doff)
            if td > pperiod:
                pperiod = datetime.combine((tod - _24H), doff)
            # if we want to support > 24h periods, we will need relativedelta support.
            return int(pperiod.timestamp() * 1_000_000)
    else:
        active_window: TimeWindow = active_in  # type: ignore[bad-assignment]

        def last_qual() -> int:
            # Note on timechange days this can be an hour off, but it's not
            # really an issue for data queries.
            # If active_in is not a tuple with two time objects, will fail,
            # correct behavior.
            nw = datetime.now(active_window[1].tzinfo)
            tod = nw.date()
            sdt = datetime.combine(tod, active_window[0])
            day = (tod - _24H) if sdt > nw else tod
            sdt = datetime.combine(day, active_window[0])
            edt = datetime.combine(day, active_window[1])
            nw = min(nw, edt)

            n = (nw - sdt) // refresh_period
            cp = sdt + refresh_period * n
            return int(cp.timestamp() * 1_000_000)

    if rollback is not None:
        #func to see if rollback is qualified given provided columns.
        rebuild = rollback.rebuild
        adj_cols = tuple(rollback.adjust_columns)

        def is_rb(kwargs):
            if rollback.included_keys is None:
                return True
            return any(kwargs[k] in v for k, v in rollback.included_keys.items())

        # Takes the first endpoint result and compares it with the last cached result.

        def adj_floats(db_tbl, func_tbl):
            if db_tbl.num_rows == 0 or func_tbl.num_rows == 0:
                return None
            changed = False
            vals = []
            for col in adj_cols:
                db_val = db_tbl[col][-1].as_py()
                func_val = func_tbl[col][0].as_py()
                bit_width = db_tbl[col].type.bit_width
                eps = EP32 if bit_width == F32_WIDTH else EP64
                changed = changed or abs(db_val - func_val) > eps * abs(db_val)
                vals.append((db_val, func_val))
            if not changed:
                return None
            return (*(db_val / func_val for db_val, func_val in vals),)

    else:
        rebuild = None

        def is_rb(kwargs): return False

        def adj_floats(db_tbl, func_tbl): pass #this branch will never be reached so no impl.

    def slice_from(tbl, start):
        if start is None or tbl.num_rows == 0:
            return tbl
        mask = pc.greater_equal(tbl[time_col], start)
        offset = pc.index(mask, value=True).as_py()
        return tbl.schema.empty_table() if offset == -1 else tbl.slice(offset)

    def _w(func: SeriesFn) -> SeriesFn:
        tbn = func.__qualname__ if table is None else table
        tbe = tbn
        tbe_info = f"{tbe}_info"

        @wraps(func)
        def get_series(**kwargs: Any) -> Table:
            con = local_adbc.sqlite
            cur = local_adbc.cur
            sdate, edate, tav, cv = make_keys(kwargs)
            table_ref = tbe
            info_table_ref = tbe_info
            if tav:
                table_ref = "_".join((tbn, *(str(v) for v in tav)))
                info_table_ref = f"{table_ref}_info"
            # three paths if nfo select fails because no table, init table process (already below)
            # if primary key not in the table, that means new data init.
            # otherwise normal get process.
            last_upd = None
            fl = 0
            try:
                # at this point we are assuming there is at least one index column that isn't time. Fix this later.
                # and only input columns can act as column keys (this is actually needed).
                cur.execute(last_upd_select(info_table_ref, ck), cv)
                last_upd = cur.fetchone()
            except dbapi.DatabaseError:
                fl = NEW_TABLE
            if last_upd is None and fl != NEW_TABLE:
                fl = NEW_KEY

            if fl > 0:
                fix_range(None, edate, kwargs)
                ltb = func(**kwargs)
                if not isinstance(ltb, Table) or ltb.num_rows == 0:
                    return ltb
                fl_tb = ut.mk_fullarrow(ltb, columns, ck, cv)
                inft: dict[str, str] | None = None
                if fl == NEW_TABLE:
                    inft = ut.infer_sqlite_types(cur, fl_tb)

                    ddl_nfo = ut.define_ls_infotable(info_table_ref, inft, ck)
                    cur.execute(ddl_nfo)

                # Assumption, take away the timestamp, then the endpoint request only captures a single 'id' for the
                # instrument. Otherwise re-enable the full pass check.
                # Update: if we need multi-id support, it should now be possible just by changing it to the full agg.
                # actually, would still need to handle the info table differently.
                # init info and last update unix micros timestamp
                cur.execute(insert_cols(info_table_ref, (*ck, ut.LAST_UPD)), (*cv, inftime_bound(edate, last_qual())))

                if fl == NEW_TABLE:
                    # init the actual lite series table.
                    ddl = ut.define_ls_table(
                        table_ref, columns, inft, ck, time_col
                    )  # pyrefly: ignore[bad-argument-type]
                    cur.execute(ddl)
                cur.adbc_ingest(table_ref, fl_tb, "append")
                con.commit()
                ltb=fl_tb.select(out_cols)
                if sdate is not None:
                    ltb = slice_from(ltb, sdate)
            else:
                last_upd = last_upd[0]  # pyrefly: ignore[unsupported-operation]
                en = edate is None
                isrb = is_rb(kwargs)
                exm = 0 if isrb else refr_micros
                lsq = last_qual()
                # The cache is older than the latest allowable freshness
                # boundary, and the request extends beyond what's known fresh.
                if lsq > last_upd and (en or edate > last_upd):
                    # Then the period we are asking for is not fully contained in our database.
                    # Selects the data here.
                    cur.execute(*series_range_select(table_ref, out_cols, ck, cv, time_col, sdate, edate))
                    ltb_s = ltb_ps = cur.fetchallarrow()
                    if ltb_s.num_rows == 0:
                        cur.execute(series_tmax_select(table_ref, ck, time_col), cv)
                        ltb_ps = cur.fetchallarrow()
                        if ltb_ps.num_rows == 0:
                            return ltb_s
                        mxt = ltb_ps[time_col][0].as_py()
                        #mxt = mxt_row[0]  # assuming data exists now.
                        # we know it queries too far back and the endpoint
                        # doesn't have data there.
                        if not en and edate < mxt:
                            return ltb_s
                        if expires_after is not None and mxt + exp_micros < last_upd:  # probably not <=
                            return ltb_s
                        # if sdate>mxt: #we don't actually need this, as it's self evident for this case
                        n_sdate = mxt + exm
                    else:
                        prv_tm = ltb_s[time_col][-1].as_py()
                        if expires_after is not None and prv_tm + exp_micros < last_upd:
                            return ltb_s
                        n_sdate = prv_tm + exm
                    fix_range(n_sdate, edate, kwargs)
                    ltb_t = func(**kwargs)
                    if not isinstance(ltb_t, Table) or ltb_t.num_rows == 0:
                        cur.execute(update_last_upd(info_table_ref, ck), (inftime_bound(edate, lsq), *cv))
                        con.commit()
                        return ltb_s
                    if isrb:
                        #why we have at least one column for ltb_s, we also know that all adjust_columns are contained
                        #in both arrow tables, even tho ltb_t isn't in out_cols form yet.
                        cols = adj_floats(ltb_ps, ltb_t)
                        if cols:
                            if rebuild:
                                prev_rows = ltb_s.num_rows
                                prev_empty = ltb_s
                                fix_range(None, n_sdate, kwargs)
                                ltb_b = func(**kwargs)
                                if not isinstance(ltb_b, Table) or ltb_b.num_rows == 0:
                                    cur.execute(update_last_upd(info_table_ref, ck), (inftime_bound(edate, lsq), *cv))
                                    con.commit()
                                    return ltb_s
                                rb_tb = ut.mk_fullarrow(ltb_b, columns, ck, cv)
                                temp_ref = f"{table_ref}_rollback"
                                #cur.execute(f"DROP TABLE IF EXISTS {qident(temp_ref)}")
                                cur.adbc_ingest(temp_ref, rb_tb.select((time_col, *adj_cols)), "create", temporary=True)
                                cur.execute(rollback_rebuild_update(table_ref, temp_ref, adj_cols, ck, time_col), cv)
                                cur.execute(f"DROP TABLE {qident(temp_ref)}")
                                ltb_s = rb_tb.select(out_cols)
                                ltb_s = ltb_s.slice(ltb_s.num_rows - prev_rows) if prev_rows > 0 else prev_empty
                            else:
                                # Execute the back-adjust query and adjust the existing in-memory slice.
                                cur.execute(rollback_adjust_update(table_ref, adj_cols, ck), (*cols, *cv))
                                #Note this is not in-place. in the future make one that is.
                                for col, val in zip(adj_cols, cols, strict=True):
                                    idx = ltb_s.column_names.index(col)
                                    ltb_s = ltb_s.set_column(idx, col, pc.multiply(ltb_s[col], pa.scalar(val)))

                        ltb_t = ltb_t.slice(1)
                    # ideally slicing ltb_t won't force any mem copy's when making the full arrow db write table.
                    cur.execute(update_last_upd(info_table_ref, ck), (inftime_bound(edate, lsq), *cv))
                    fl_tb = ut.mk_fullarrow(ltb_t, columns, ck, cv)
                    cur.adbc_ingest(table_ref, fl_tb, "append")
                    con.commit()
                    # we do this before sending the data
                    #ltb_s = ltb_s.select(out_cols) #already fetching by out_cols
                    # we use fl_tb instead of og ltb_t because out cols could contain more than what func produces
                    ltb_t = fl_tb.select(out_cols)
                    #ltb_s and ltb_t should be correct columns now.

                    if ltb_s.num_rows == 0:
                        ltb = slice_from(ltb_t, sdate)
                    else:
                        # we know that sdate is in the first row of ltb_s, so no need to return a slice but do concat.
                        ltb = concat_tables([ltb_s, ltb_t], promote_options="none")

                else:  # We are requesting for data inside of edate or the new data query happened recently enough.
                    cur.execute(*series_range_select(table_ref, out_cols, ck, cv, time_col, sdate, edate))
                    ltb = cur.fetchallarrow()#.select(out_cols)
            return ltb

        return get_series

    return _w
