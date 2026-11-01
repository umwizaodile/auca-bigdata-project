"""
load_hbase.py
Loads a subset of browsing sessions into HBase.

Tables (created earlier in the HBase shell):
  user_sessions  row key = user_id#reverse_timestamp#session_id
                 families: info (session facts), pv (page views), cart (cart contents)
  product_daily  row key = product_id#yyyymmdd
                 family:   m (views, cart_adds, purchases)

Reverse timestamp = MAX_TS - epoch_seconds, zero-padded, so that NEWER sessions
sort FIRST inside one user's block of rows (fast "latest sessions" scans).
"""
import json
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

import happybase

HBASE_HOST = "127.0.0.1"
HBASE_PORT = 9090
SESSION_FILE = "sessions_0.json"
SESSION_LIMIT = 50000      # sessions loaded into HBase (raise later if all goes well)
BATCH_SIZE = 1000          # rows per batched write
MAX_TS = 9_999_999_999     # 10-digit ceiling used for the reverse timestamp


def epoch_seconds(iso_string):
    """ISO date string -> epoch seconds (treated as UTC)."""
    dt = datetime.fromisoformat(iso_string).replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def make_row_key(user_id, start_time, session_id):
    """user_id#reverse_timestamp#session_id  (newest session first per user)."""
    reverse_ts = MAX_TS - epoch_seconds(start_time)
    return f"{user_id}#{reverse_ts:010d}#{session_id}"


def b(value):
    """HBase stores bytes: convert any value to UTF-8 bytes."""
    return str(value).encode("utf-8")


def build_session_row(s):
    """Turn one session document into (row_key, {column: value}) for user_sessions."""
    row_key = make_row_key(s["user_id"], s["start_time"], s["session_id"])
    geo = s.get("geo_data") or {}
    dev = s.get("device_profile") or {}
    data = {
        b"info:session_id": b(s["session_id"]),
        b"info:start_time": b(s["start_time"]),
        b"info:end_time": b(s.get("end_time") or ""),
        b"info:duration_seconds": b(s.get("duration_seconds") or 0),
        b"info:referrer": b(s.get("referrer") or ""),
        b"info:conversion_status": b(s.get("conversion_status") or ""),
        b"info:device_type": b(dev.get("type") or ""),
        b"info:device_os": b(dev.get("os") or ""),
        b"info:device_browser": b(dev.get("browser") or ""),
        b"info:city": b(geo.get("city") or ""),
        b"info:state": b(geo.get("state") or ""),
        b"info:country": b(geo.get("country") or ""),
        b"info:page_view_count": b(len(s.get("page_views") or [])),
    }
    # pv family: one column per page view, zero-padded so order is preserved
    for i, pv in enumerate(s.get("page_views") or [], start=1):
        data[b(f"pv:{i:03d}")] = json.dumps(pv, separators=(",", ":")).encode("utf-8")
    # cart family: one column per product in the cart (sparse: most sessions have none)
    for product_id, item in (s.get("cart_contents") or {}).items():
        data[b(f"cart:{product_id}")] = json.dumps(item, separators=(",", ":")).encode("utf-8")
    return row_key.encode("utf-8"), data


def update_product_metrics(metrics, s):
    """Accumulate per-product, per-day views / cart_adds / purchases for one session."""
    for pv in s.get("page_views") or []:
        if pv.get("page_type") == "product_detail" and pv.get("product_id"):
            day = pv["timestamp"][:10].replace("-", "")
            metrics[(pv["product_id"], day)][0] += 1
    day = s["start_time"][:10].replace("-", "")
    converted = s.get("conversion_status") == "converted"
    for product_id, item in (s.get("cart_contents") or {}).items():
        qty = item.get("quantity", 1)
        metrics[(product_id, day)][1] += qty
        if converted:
            metrics[(product_id, day)][2] += qty


def main():
    if not os.path.exists(SESSION_FILE):
        sys.exit(f"ERROR: {SESSION_FILE} not found. Run dataset_generator.py first.")

    # --- Connect and check the tables exist --------------------------------
    try:
        connection = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=60000)
        tables = connection.tables()
    except Exception as exc:
        sys.exit(f"ERROR: cannot reach HBase Thrift on {HBASE_HOST}:{HBASE_PORT} ({exc}). "
                 "Is the 'hbase-auca' container running? (docker ps)")
    for needed in (b"user_sessions", b"product_daily"):
        if needed not in tables:
            sys.exit(f"ERROR: table {needed.decode()} not found. Create it in the HBase shell first.")
    sessions_table = connection.table("user_sessions")
    product_table = connection.table("product_daily")
    print("Connected to HBase.")

    # --- Read sessions ------------------------------------------------------
    print(f"Reading {SESSION_FILE} (188 MB, please wait)...")
    with open(SESSION_FILE, encoding="utf-8") as f:
        sessions = json.load(f)[:SESSION_LIMIT]
    print(f"Loaded {len(sessions):,} sessions into memory.")

    # --- Write user_sessions + accumulate product metrics --------------------
    metrics = defaultdict(lambda: [0, 0, 0])   # (product, day) -> [views, cart_adds, purchases]
    written = 0
    with sessions_table.batch(batch_size=BATCH_SIZE) as batch:
        for s in sessions:
            row_key, data = build_session_row(s)
            batch.put(row_key, data)
            update_product_metrics(metrics, s)
            written += 1
            if written % 10000 == 0:
                print(f"  user_sessions: {written:,} rows written")
    print(f"user_sessions: {written:,} rows written in total")

    # --- Write product_daily -------------------------------------------------
    with product_table.batch(batch_size=BATCH_SIZE) as batch:
        for (product_id, day), (views, cart_adds, purchases) in metrics.items():
            batch.put(f"{product_id}#{day}".encode("utf-8"), {
                b"m:views": b(views),
                b"m:cart_adds": b(cart_adds),
                b"m:purchases": b(purchases),
            })
    print(f"product_daily: {len(metrics):,} rows written")

    # --- Verification (count rows, reading only the first cell of each) ------
    try:
        n = sum(1 for _ in sessions_table.scan(filter=b"FirstKeyOnlyFilter()"))
        print(f"\nVerification: user_sessions now holds {n:,} rows")
    except Exception as exc:
        print(f"(row count skipped: {exc})")
    connection.close()
    print("Done.")


if __name__ == "__main__":
    main()