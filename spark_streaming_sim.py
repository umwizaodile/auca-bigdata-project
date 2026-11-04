"""
spark_streaming_sim.py
Simulated STREAM processing with Apache Spark (micro-batch replay).

Sessions are replayed in time order, one micro-batch per time window (default: one day), as if
they were arriving from a live feed. For every micro-batch Spark computes the batch metrics,
and the driver keeps running STATE across batches:
  - cumulative and rolling 7-batch conversion rate
  - trending products (product page views in the last 3 batches)
  - an ALERT when a batch's conversion rate leaves the 3-standard-deviation noise band
At the end the streaming totals are checked against the batch totals (they must be identical).

In production the same logic would read from Kafka with Structured Streaming (readStream,
foreachBatch, watermarks); here the replay is a loop so it runs on a laptop and on Windows
without checkpoint files.
Output: results/stream_batches.csv and charts/06_streaming_conversion.png
"""
import math
import os
import time
from collections import Counter, deque
from datetime import datetime, timezone

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import functions as F

from spark_common import clean_all, get_spark

BATCH_HOURS = 24        # micro-batch interval of the simulated stream
ROLLING = 7             # batches in the rolling window
TRENDING_WINDOW = 3     # batches used for "trending products"
DELAY_SECONDS = 0.0     # set to e.g. 0.5 to slow the replay down like a real feed


def main():
    spark = get_spark("stream-simulation")
    _, clean = clean_all(spark)
    s = clean["sessions"].filter(F.col("end_time").isNotNull())

    # Event time = the moment the session ends (when the event "arrives" in the stream)
    t0 = s.agg(F.min(F.unix_timestamp("end_time"))).collect()[0][0]
    base = t0 - (t0 % 86400)                                   # align batches to whole days (UTC)
    step = BATCH_HOURS * 3600
    s = s.withColumn("batch", F.floor((F.unix_timestamp("end_time") - F.lit(base)) / step).cast("int"))

    events = (s.select("batch", "session_id", "n_page_views", "duration_seconds",
                       (F.col("conversion_status") == "converted").cast("int").alias("converted"),
                       F.col("has_cart").cast("int").alias("has_cart")).cache())
    views = (s.select("batch", F.explode("page_views").alias("p"))
             .filter((F.col("p.page_type") == "product_detail") & F.col("p.product_id").isNotNull())
             .select("batch", F.col("p.product_id").alias("product_id")).cache())
    n_events = events.count()
    n_batches = events.agg(F.max("batch")).collect()[0][0] + 1
    views.count()
    print(f"Replaying {n_events:,} session events in {n_batches} micro-batches of {BATCH_HOURS} h\n")

    cum_sessions = cum_conv = 0
    roll = deque(maxlen=ROLLING)
    recent_views = deque(maxlen=TRENDING_WINDOW)
    rows, alerts = [], 0
    print(f"{'batch':>5} {'window start (UTC)':<17} {'sessions':>8} {'conv':>5} {'conv%':>6} "
          f"{'cum%':>6} {'roll7%':>7}  trending product (views in last {TRENDING_WINDOW} batches)")

    for k in range(n_batches):                                   # <- each iteration = one micro-batch
        m = (events.filter(F.col("batch") == k)
             .agg(F.count("*").alias("sessions"), F.sum("converted").alias("converted"),
                  F.sum("has_cart").alias("carts"), F.sum("n_page_views").alias("page_views"),
                  F.avg("duration_seconds").alias("avg_duration")).collect()[0])
        sessions, conv = m["sessions"], int(m["converted"] or 0)
        batch_views = Counter({r["product_id"]: r["count"] for r in
                               views.filter(F.col("batch") == k).groupBy("product_id").count().collect()})

        # ---- update the running state ----
        cum_sessions += sessions
        cum_conv += conv
        roll.append((sessions, conv))
        recent_views.append(batch_views)
        trending = sum(recent_views, Counter()).most_common(1)
        rate = conv / sessions if sessions else float("nan")
        p_run = cum_conv / cum_sessions if cum_sessions else 0
        band = 3 * math.sqrt(p_run * (1 - p_run) / sessions) if sessions else float("inf")
        alert = sessions >= 200 and abs(rate - p_run) > band
        alerts += alert
        roll_rate = sum(c for _, c in roll) / max(1, sum(n for n, _ in roll))
        start = datetime.fromtimestamp(base + k * step, tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        top = f"{trending[0][0]} ({trending[0][1]})" if trending else "-"
        print(f"{k:>5} {start:<17} {sessions:>8,} {conv:>5,} {100 * rate:>6.1f} "
              f"{100 * p_run:>6.1f} {100 * roll_rate:>7.1f}  {top}{'  ALERT' if alert else ''}")
        rows.append({"batch": k, "window_start": start, "sessions": sessions, "converted": conv,
                     "conversion_rate": rate, "cumulative_rate": p_run, "rolling_rate": roll_rate,
                     "trending_product": trending[0][0] if trending else None, "alert": bool(alert)})
        if DELAY_SECONDS:
            time.sleep(DELAY_SECONDS)

    # ---- validation: streaming totals must equal the batch totals ----
    total = events.agg(F.count("*"), F.sum("converted")).collect()[0]
    ok = (cum_sessions == total[0]) and (cum_conv == total[1])
    print(f"\nStreaming totals: {cum_sessions:,} sessions, {cum_conv:,} converted "
          f"({100 * cum_conv / cum_sessions:.1f}%) | batch totals: {total[0]:,} / {total[1]:,} "
          f"-> {'IDENTICAL' if ok else 'MISMATCH'}")
    print(f"Micro-batches: {n_batches} | alerts raised: {alerts}")

    df = pd.DataFrame(rows)
    os.makedirs("results", exist_ok=True)
    os.makedirs("charts", exist_ok=True)
    df.to_csv("results/stream_batches.csv", index=False)

    # ---- chart ----
    full = df[df["sessions"] >= 200]                              # skip tiny edge batches
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(9, 6), sharex=True,
                                 gridspec_kw={"height_ratios": [1, 2]})
    a1.bar(df["batch"], df["sessions"], color="#9ca3af")
    a1.set_ylabel("Sessions per batch")
    a1.set_title("Simulated stream: daily micro-batches and running conversion rate", fontweight="bold")
    a2.plot(full["batch"], 100 * full["conversion_rate"], color="#d1d5db", label="Per batch")
    a2.plot(df["batch"], 100 * df["rolling_rate"], color="#2563eb", lw=2, label=f"Rolling {ROLLING} batches")
    a2.plot(df["batch"], 100 * df["cumulative_rate"], color="#111827", ls="--", label="Cumulative")
    if df["alert"].any():
        al = df[df["alert"]]
        a2.scatter(al["batch"], 100 * al["conversion_rate"], color="#dc2626", zorder=5, label="Alert")
    a2.set_ylabel("Conversion rate (%)")
    a2.set_xlabel(f"Micro-batch number ({BATCH_HOURS} h each)")
    a2.legend(frameon=False, ncol=4, fontsize=8)
    for a in (a1, a2):
        a.spines[["top", "right"]].set_visible(False)
    fig.text(0.01, 0.005, "Source: Spark micro-batch replay of 200,000 cleaned sessions", fontsize=7.5, color="#6b7280")
    fig.tight_layout(rect=(0, 0.02, 1, 1))
    fig.savefig("charts/06_streaming_conversion.png", dpi=200)
    print("saved charts/06_streaming_conversion.png and results/stream_batches.csv")
    spark.stop()


if __name__ == "__main__":
    main()