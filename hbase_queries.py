"""
hbase_queries.py
Query examples for the HBase tables created by load_hbase.py.

  Q1  Latest sessions of one user         (prefix scan; reverse timestamp => newest first)
  Q2  Full page-view stream of a session  (single-row get, wide-column 'pv' family)
  Q3  Sessions in a time window           (row-key range scan)
  Q4  Column-family pruning               (read only the 'info' family)
  Q5  Product performance over time       (range scan on product_id#yyyymmdd)

Usage:  python hbase_queries.py            (picks a user and a product automatically)
        python hbase_queries.py user_000042
"""
import json
import sys
import time

import happybase

from load_hbase import HBASE_HOST, HBASE_PORT, MAX_TS, epoch_seconds


def decode(d):
    """{b'family:col': b'value'} -> {'family:col': 'value'}"""
    return {c.decode(): v.decode() for c, v in d.items()}


def latest_sessions(table, user_id, n=5):
    """Q1: newest-first sessions of one user. Only the 'info' family is read."""
    rows = table.scan(row_prefix=f"{user_id}#".encode(), columns=[b"info"], limit=n)
    return [(k.decode(), decode(d)) for k, d in rows]


def session_page_views(table, row_key):
    """Q2: all page views of one session, in order (pv:001, pv:002, ...)."""
    row = table.row(row_key.encode(), columns=[b"pv"])
    return [json.loads(v) for _, v in sorted(row.items())]


def sessions_in_window(table, user_id, newest_start, days=30):
    """Q3: sessions of a user in [newest - days, newest], via a row-key range."""
    newest = epoch_seconds(newest_start)
    oldest = newest - days * 86400
    row_start = f"{user_id}#{MAX_TS - newest:010d}".encode()        # newest edge
    row_stop = f"{user_id}#{MAX_TS - oldest + 1:010d}".encode()     # oldest edge (exclusive)
    rows = table.scan(row_start=row_start, row_stop=row_stop,
                      columns=[b"info:start_time", b"info:conversion_status"])
    return [(k.decode(), decode(d)) for k, d in rows]


def family_cells(table, user_id, columns=None):
    """Q4: number of cells returned for a user's rows, with and without family pruning."""
    rows = table.scan(row_prefix=f"{user_id}#".encode(), columns=columns)
    return sum(len(d) for _, d in rows)


def product_range(table, product_id, start_day, stop_day):
    """Q5: daily metrics of one product between two days (stop day exclusive)."""
    rows = table.scan(row_start=f"{product_id}#{start_day}".encode(),
                      row_stop=f"{product_id}#{stop_day}".encode())
    out = []
    for k, d in rows:
        m = decode(d)
        out.append((k.decode().split("#")[1], int(m["m:views"]),
                    int(m["m:cart_adds"]), int(m["m:purchases"])))
    return out


def timed(fn, *args, **kwargs):
    t0 = time.perf_counter()
    result = fn(*args, **kwargs)
    return result, (time.perf_counter() - t0) * 1000


def main():
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=60000)
        sessions = connection.table("user_sessions")
        products = connection.table("product_daily")
        first_session = next(iter(sessions.scan(limit=1, columns=[b"info:session_id"])))
        first_product = next(iter(products.scan(limit=1, columns=[b"m:views"])))
    except Exception as exc:
        sys.exit(f"ERROR: cannot read HBase ({exc}). Is 'hbase-auca' running and loaded?")

    user_id = sys.argv[1] if len(sys.argv) > 1 else first_session[0].decode().split("#")[0]
    product_id = first_product[0].decode().split("#")[0]

    # Q1 ---------------------------------------------------------------------
    rows, ms = timed(latest_sessions, sessions, user_id)
    print(f"\n=== Q1 Latest sessions of {user_id} ({ms:.0f} ms) ===")
    if not rows:
        sys.exit("No sessions found for this user in HBase.")
    for key, d in rows:
        print(f"{key} | {d['info:start_time']} | {d['info:referrer']:<14} | "
              f"{d['info:conversion_status']:<10} | {d['info:duration_seconds']:>5}s | "
              f"{d['info:page_view_count']} page views")

    # Q2 ---------------------------------------------------------------------
    newest_key, newest = rows[0]
    pvs, ms = timed(session_page_views, sessions, newest_key)
    print(f"\n=== Q2 Page views of the newest session ({ms:.0f} ms) ===")
    for pv in pvs:
        print(f"{pv['timestamp']} | {pv['page_type']:<16} | "
              f"{str(pv['product_id']):<11} | {pv['view_duration']}s")

    # Q3 ---------------------------------------------------------------------
    win, ms = timed(sessions_in_window, sessions, user_id, newest["info:start_time"], 30)
    print(f"\n=== Q3 Sessions in the 30 days up to the newest one: {len(win)} ({ms:.0f} ms) ===")
    for key, d in win:
        print(f"{d['info:start_time']} | {d['info:conversion_status']}")

    # Q4 ---------------------------------------------------------------------
    only_info = family_cells(sessions, user_id, [b"info"])
    everything = family_cells(sessions, user_id)
    print("\n=== Q4 Column-family pruning for the same rows ===")
    print(f"Reading only 'info' family : {only_info} cells")
    print(f"Reading all families       : {everything} cells")

    # Q5 ---------------------------------------------------------------------
    days, ms = timed(product_range, products, product_id, "20260901", "20261001")
    print(f"\n=== Q5 {product_id}: daily metrics, Sept 2026 ({ms:.0f} ms) ===")
    print("day       views  cart_adds  purchases")
    for day, v, c, p in days:
        print(f"{day}  {v:>5}  {c:>9}  {p:>9}")
    if days:
        print(f"TOTAL     {sum(d[1] for d in days):>5}  {sum(d[2] for d in days):>9}  "
              f"{sum(d[3] for d in days):>9}")
    connection.close()


if __name__ == "__main__":
    main()