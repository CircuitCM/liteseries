from __future__ import annotations

import sqlite3
import threading as th
from datetime import datetime, time, timedelta
from functools import wraps
from typing import Any, Callable
from urllib.parse import quote

import pyarrow.compute as pc
from adbc_driver_sqlite import dbapi
from dateutil import tz
from pyarrow import Table, concat_tables

from . import _util as ut
from ._sql import insert_cols, last_upd_select, series_range_select, series_tmax_select, update_last_upd

LT = list | tuple
TimeWindow = tuple[time, time]
SeriesFn = Callable[..., Table]
CacheDecorator = Callable[[SeriesFn], SeriesFn]
DEFAULT_ACTIVE_IN = time(hour=16, second=1, tzinfo=tz.gettz("US/Eastern"))


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
    expires_after: timedelta|None=None,
    rollback: bool = False,
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
    :param out_cols: Specifically the ordered columns of the arrow table that will be produced by this wrapper. Always 
    less than or equal to columns.
    :param rollback:  Not implemented yet (mainly for continuous futures that are backwards adjusted on the next roll date).
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
        doff = active_in[1] if not isinstance(active_in, time) else active_in

        def last_qual() -> int:
            # ltime is unix micros
            # curn=datetime.fromtimestamp(ltime,dt.UTC) #timezone should be irrelevant but if issues, use doff's
            tod = datetime.now(doff.tzinfo).date()
            pperiod = tod - _24H
            comp = int(datetime.combine(pperiod, doff).timestamp() * 1_000_000)
            return comp
    else:
        active_window: TimeWindow = active_in  # pyrefly: ignore[bad-assignment]

        def last_qual() -> int:
            # Note on timechange days this can be an hour off, but it's not
            # really an issue for data queries.
            # If active_in is not a tuple with two time objects, will fail,
            # correct behavior.
            nw = datetime.now(active_window[1].tzinfo)
            tod = nw.date()
            sdt = datetime.combine(tod, active_window[0])
            day = tod - (_24H if sdt > nw else _0D)
            sdt = datetime.combine(day, active_window[0])
            edt = datetime.combine(day, active_window[1])
            nw = min(nw, edt)

            n = (nw - sdt) // refresh_period
            cp = sdt + refresh_period * n
            return int(cp.timestamp() * 1_000_000)

    def cache_upd(edate, lsq) -> int:
        if edate is None or edate >= lsq:
            return ut.sys_micros()
        return edate

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
                fl = 2
            if last_upd is None and fl != 2:
                fl = 1

            if fl > 0:
                fix_range(None, edate, kwargs)
                ltb = func(**kwargs)
                if not isinstance(ltb, Table) or ltb.num_rows == 0:
                    return ltb
                fl_tb = ut.mk_fullarrow(ltb, columns, ck, cv)
                inft: dict[str, str] | None = None
                if fl == 2:
                    inft = ut.infer_sqlite_types(cur, fl_tb)

                    ddl_nfo = ut.define_ls_infotable(info_table_ref, inft, ck)
                    cur.execute(ddl_nfo)
                # Assumption, take away the timestamp, then the endpoint request only captures a single 'id' for the
                # instrument. Otherwise re-enable the full pass check.
                # Update: if we need multi-id support, it should now be possible just by changing it to the full agg.
                # actually, would still need to handle the info table differently.
                # init info and last update unix micros timestamp
                cur.execute(insert_cols(info_table_ref, (*ck, ut.LAST_UPD)), (*cv, cache_upd(edate, last_qual())))

                if fl == 2:
                    # init the actual lite series table.
                    ddl = ut.define_ls_table(table_ref, columns, inft, ck, time_col)  # pyrefly: ignore[bad-argument-type]
                    cur.execute(ddl)
                cur.adbc_ingest(table_ref, fl_tb, "append")
                con.commit()
                ltb=fl_tb.select(out_cols)
                if sdate is not None:
                    mask = pc.greater_equal(ltb[time_col], sdate)
                    offset = pc.index(mask, value=True).as_py()
                    ltb = ltb.slice(ltb.num_rows, 0) if offset == -1 else ltb.slice(offset)
            else:
                last_upd = last_upd[0]  # pyrefly: ignore[unsupported-operation]
                lsq = last_qual()
                en = edate is None
                # “the cache is older than the latest allowable freshness
                # boundary, and the request extends beyond what’s known fresh”
                if lsq > last_upd and (en or edate > last_upd):
                    # Then the period we are asking for is not fully contained in our database.
                    # Selects the data here.
                    cur.execute(*series_range_select(table_ref, out_cols, ck, cv, time_col, sdate, edate))
                    ltb_s = cur.fetchallarrow()
                    if ltb_s.num_rows == 0:
                        cur.execute(series_tmax_select(table_ref, ck, time_col), cv)
                        mxt = cur.fetchone()[0]
                        if mxt is None:
                            return ltb_s
                        #mxt = mxt_row[0]  # assuming data exists now.
                        # we know it queries too far back and the endpoint
                        # doesn't have data there.
                        if not en and edate < mxt:
                            return ltb_s
                        if expires_after is not None and mxt + exp_micros < last_upd:
                            return ltb_s
                        # if sdate>mxt: #we don't actually need this, as it's self evident for this case
                        n_sdate = mxt + refr_micros
                    else:
                        prv_tm = ltb_s[time_col][-1].as_py()
                        if expires_after is not None and prv_tm + exp_micros < last_upd:
                            return ltb_s
                        n_sdate = prv_tm + refr_micros
                    fix_range(n_sdate, edate, kwargs)
                    ltb_t = func(**kwargs)
                    if not isinstance(ltb_t, Table) or ltb_t.num_rows == 0:
                        cur.execute(update_last_upd(info_table_ref, ck), (cache_upd(edate, lsq), *cv))
                        con.commit()
                        return ltb_s
                    fl_tb = ut.mk_fullarrow(ltb_t, columns, ck, cv)
                    # can be built from columns as well
                    cur.execute(update_last_upd(info_table_ref, ck), (cache_upd(edate, lsq), *cv))

                    cur.adbc_ingest(table_ref, fl_tb, "append")
                    con.commit()
                    # we do this before sending the data
                    #ltb_s = ltb_s.select(out_cols) #already fetching by out_cols
                    # we use fl_tb instead of ltb_t because out cols could contain more than what func produces
                    ltb_t = fl_tb.select(out_cols)
                    ltb = concat_tables([ltb_s, ltb_t], promote_options="none")

                else:  # We are requesting for data inside of edate or the new data query happened recently enough.
                    cur.execute(*series_range_select(table_ref, out_cols, ck, cv, time_col, sdate, edate))
                    ltb = cur.fetchallarrow()#.select(out_cols)
            return ltb

        return get_series

    return _w
