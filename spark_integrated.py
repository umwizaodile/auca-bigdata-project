"""
spark_integrated.py
Part 3: integrated FUNNEL CONVERSION analysis across HBase, MongoDB and Spark.

Business question: where do we lose customers between viewing a product and a recorded
purchase, and does the loss differ by referrer or device?

Data sources and roles
  HBase   user_sessions  -> browsing stages (product view, cart, checkout page) per session
  MongoDB transactions   -> the recorded purchases, linked to a session by session_id
  Spark                  -> joins both sources, computes the funnel, validates it against the
                            funnel computed directly on all 200,000 sessions

Funnel (each stage is a subset of the previous one):
  1 sessions -> 2 viewed a product -> 3 added to cart -> 4 reached checkout
  -> 5 completed (status 'converted') -> 6 purchase recorded in MongoDB
"""
import json
import math
import os
import sys

import happybase
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from pyspark.sql import functions as F
from pyspark.sql import types as T

from load_hbase import HBASE_HOST, HBASE_PORT
from spark_common import clean_all, get_spark

MONGO_URI = "mongodb://127.0.0.1:27018"
RECOVERY_SCENARIO = 0.10     # "what if we recovered 10% of abandoned carts?"

HBASE_SCHEMA = T.StructType([
    T.StructField("session_id", T.StringType()), T.StructField("user_id", T.StringType()),
    T.StructField("referrer", T.StringType()), T.StructField("device_type", T.StringType()),
    T.StructField("conversion_status", T.StringType()),
    T.StructField("viewed_product", T.BooleanType()), T.StructField("added_to_cart", T.BooleanType()),
    T.StructField("reached_checkout", T.BooleanType()),
    T.StructField("cart_units", T.IntegerType()), T.StructField("cart_value", T.DoubleType())])

MONGO_SCHEMA = T.StructType([
    T.StructField("session_id", T.StringType()), T.StructField("transaction_id", T.StringType()),
    T.StructField("total", T.DoubleType())])

STAGES = ["sessions", "viewed_product", "added_to_cart", "reached_checkout",
          "completed", "purchase_recorded"]


# ---------------------------------------------------------------------------
# Extraction: each system is read with its own native client
# ---------------------------------------------------------------------------
def parse_hbase_row(key, data):
    """One HBase row (user_sessions) -> one flat tuple matching HBASE_SCHEMA."""
    user_id, _, session_id = key.decode().split("#", 2)
    page_types, cart_units, cart_value = set(), 0, 0.0
    for col, val in data.items():
        if col.startswith(b"pv:"):                       # pv family: one column per page view
            page_types.add(json.loads(val).get("page_type"))
        elif col.startswith(b"cart:"):                   # cart family: one column per product
            item = json.loads(val)
            cart_units += item.get("quantity", 0)
            cart_value += item.get("quantity", 0) * item.get("price", 0.0)

    def text(name):
        return data.get(name, b"").decode().strip().lower() or "unknown"

    return (session_id, user_id, text(b"info:referrer"), text(b"info:device_type"),
            text(b"info:conversion_status"), "product_detail" in page_types, cart_units > 0,
            bool({"checkout", "confirmation"} & page_types), int(cart_units), round(cart_value, 2))


def extract_hbase_sessions():
    """Scan the whole user_sessions table (only the column families we need)."""
    try:
        conn = happybase.Connection(HBASE_HOST, port=HBASE_PORT, timeout=120000)
        table = conn.table("user_sessions")
        rows = []
        for key, data in table.scan(columns=[b"info:referrer", b"info:device_type",
                                             b"info:conversion_status", b"pv", b"cart"],
                                    batch_size=1000):
            rows.append(parse_hbase_row(key, data))
            if len(rows) % 10000 == 0:
                print(f"  HBase: {len(rows):,} sessions read")
        conn.close()
        return rows
    except Exception as exc:
        sys.exit(f"ERROR: cannot read HBase ({exc}). Is 'hbase-auca' running and loaded?")


def extract_mongo_transactions():
    """Transactions that are linked to a session (the purchases we can place in a funnel)."""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)
    try:
        client.admin.command("ping")
    except ServerSelectionTimeoutError:
        sys.exit("ERROR: cannot reach MongoDB. Is 'mongo-auca' running? (docker ps)")
    cursor = client["ecommerce"]["transactions"].find(
        {"session_id": {"$ne": None}},
        {"_id": 0, "session_id": 1, "transaction_id": 1, "total": 1})
    return [(d["session_id"], d["transaction_id"], float(d["total"])) for d in cursor]


# ---------------------------------------------------------------------------
# Funnel computation (Spark)
# ---------------------------------------------------------------------------
def funnel(df, group_cols=()):
    """Stage counts and step conversion rates, overall or per group."""
    b2 = F.col("viewed_product")
    b3 = b2 & F.col("added_to_cart")
    b4 = b3 & F.col("reached_checkout")
    b5 = b4 & (F.col("conversion_status") == "converted")
    b6 = b5 & F.col("has_txn")
    out = df.groupBy(*group_cols).agg(
        F.count("*").alias("sessions"),
        F.sum(b2.cast("int")).alias("viewed_product"),
        F.sum(b3.cast("int")).alias("added_to_cart"),
        F.sum(b4.cast("int")).alias("reached_checkout"),
        F.sum(b5.cast("int")).alias("completed"),
        F.sum(b6.cast("int")).alias("purchase_recorded")).toPandas()
    out["view_rate"] = out["viewed_product"] / out["sessions"]
    out["cart_rate"] = out["added_to_cart"] / out["viewed_product"]
    out["checkout_rate"] = out["reached_checkout"] / out["added_to_cart"]
    out["complete_rate"] = out["completed"] / out["reached_checkout"]
    out["record_rate"] = out["purchase_recorded"] / out["completed"]
    out["overall_rate"] = out["purchase_recorded"] / out["sessions"]
    return out


def pct(df, cols):
    shown = df.copy()
    for c in cols:
        shown[c] = (100 * shown[c]).round(1).astype(str) + "%"
    return shown


RATE_COLS = ["view_rate", "cart_rate", "checkout_rate", "complete_rate", "record_rate", "overall_rate"]


def noise_check(by_group, key):
    """Does any group's overall conversion differ from the average by more than random noise?"""
    p = by_group["purchase_recorded"].sum() / by_group["sessions"].sum()
    out = by_group[[key, "sessions", "overall_rate"]].copy()
    out["band_3sd"] = 3 * (p * (1 - p) / out["sessions"]) ** 0.5
    out["difference"] = (out["overall_rate"] - p).abs()
    out["outside_noise"] = out["difference"] > out["band_3sd"]
    return p, out


def report(spark, hb_rows, mongo_rows, clean):
    os.makedirs("results", exist_ok=True)
    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 30)

    hb = spark.createDataFrame(hb_rows, HBASE_SCHEMA)
    tx = spark.createDataFrame(mongo_rows, MONGO_SCHEMA).dropDuplicates(["session_id"])
    df = (hb.join(tx, "session_id", "left")
            .withColumn("has_txn", F.col("transaction_id").isNotNull()).cache())
    n = df.count()
    print(f"\nHBase sessions: {n:,} | MongoDB linked transactions: {len(mongo_rows):,} | "
          f"matched to these sessions: {df.filter('has_txn').count():,}")

    # --- Integrity checks between the two systems ------------------------------
    cwt = df.filter((F.col("conversion_status") == "converted") & ~F.col("has_txn")).count()
    twc = df.filter(F.col("has_txn") & (F.col("conversion_status") != "converted")).count()
    print(f"Integrity: converted sessions without a MongoDB transaction: {cwt:,} | "
          f"transactions on non-converted sessions: {twc:,}")

    # --- 1. Overall funnel -----------------------------------------------------------
    overall = funnel(df)
    overall.to_csv("results/funnel_overall.csv", index=False)
    row = overall.iloc[0]
    print("\n=== 1. Overall funnel (HBase sessions + MongoDB purchases) ===")
    counts = pd.DataFrame({"stage": STAGES, "sessions": [int(row[s]) for s in STAGES]})
    counts["share_of_all_sessions"] = (100 * counts["sessions"] / counts["sessions"].iloc[0]).round(1)
    counts["step_conversion"] = [None] + [
        round(100 * counts["sessions"].iloc[i] / counts["sessions"].iloc[i - 1], 1)
        for i in range(1, len(STAGES))]
    print(counts.to_string(index=False))
    counts.to_csv("results/funnel_stages.csv", index=False)

    # --- 2. By referrer and device --------------------------------------------------
    for key in ["referrer", "device_type"]:
        by = funnel(df, [key])
        by.to_csv(f"results/funnel_by_{key}.csv", index=False)
        p, check = noise_check(by, key)
        print(f"\n=== 2. Funnel by {key} ===")
        print(pct(by[[key, "sessions"] + RATE_COLS], RATE_COLS).to_string(index=False))
        print(f"Average overall conversion {100 * p:.1f}%. Groups outside the random-noise band "
              f"(3 standard deviations): {int(check['outside_noise'].sum())} of {len(check)}")

    # --- 3. Abandoned carts vs purchased revenue --------------------------------
    ab = df.filter(F.col("conversion_status") == "abandoned").agg(
        F.count("*").alias("n"), F.sum("cart_value").alias("value")).collect()[0]
    rev = df.filter("has_txn").agg(F.sum("total")).collect()[0][0] or 0.0
    print("\n=== 3. Abandoned carts (HBase cart family) vs purchases (MongoDB totals) ===")
    print(f"Abandoned carts: {ab['n']:,} worth {ab['value']:,.0f} | purchased revenue: {rev:,.0f}")
    gain = RECOVERY_SCENARIO * ab["value"]
    print(f"Scenario: recovering {int(100 * RECOVERY_SCENARIO)}% of abandoned carts would add about "
          f"{gain:,.0f} ({100 * gain / rev:.1f}% more revenue; cart value is before discounts)")
    pd.DataFrame([{"abandoned_carts": ab["n"], "abandoned_value": ab["value"],
                   "purchased_revenue": rev, "recovery_scenario": RECOVERY_SCENARIO,
                   "extra_revenue": gain}]).to_csv("results/funnel_abandoned_carts.csv", index=False)

    # --- 4. Cross-check against the funnel on ALL 200,000 sessions ----------------
    full = clean["sessions"].select(
        "session_id", "user_id", "referrer", "device_type", "conversion_status",
        (F.col("n_product_views") > 0).alias("viewed_product"),
        F.col("has_cart").alias("added_to_cart"),
        F.coalesce(F.col("visited_checkout") | F.col("visited_confirmation"),
                   F.lit(False)).alias("reached_checkout"),
        F.col("has_transaction").alias("has_txn"))
    f = funnel(full).iloc[0]
    print("\n=== 4. Cross-check: HBase subset vs all sessions in Spark ===")
    cmp_ = pd.DataFrame({
        "step": ["view_rate", "cart_rate", "checkout_rate", "complete_rate", "record_rate"],
        "hbase_subset": [row[c] for c in RATE_COLS[:5]],
        "all_sessions": [f[c] for c in RATE_COLS[:5]]})
    cmp_["difference_pts"] = (100 * (cmp_["hbase_subset"] - cmp_["all_sessions"])).round(2)
    cmp_.to_csv("results/funnel_subset_vs_full.csv", index=False)
    print(pct(cmp_, ["hbase_subset", "all_sessions"]).to_string(index=False))
    print(f"All sessions: {int(f['sessions']):,} | completed: {int(f['completed']):,} | "
          f"with a transaction: {int(f['purchase_recorded']):,}")


def main():
    spark = get_spark("integrated-funnel")
    print("Reading sessions from HBase ...")
    hb_rows = extract_hbase_sessions()
    print(f"HBase: {len(hb_rows):,} sessions")
    print("Reading linked transactions from MongoDB ...")
    mongo_rows = extract_mongo_transactions()
    print(f"MongoDB: {len(mongo_rows):,} linked transactions")
    _, clean = clean_all(spark)
    report(spark, hb_rows, mongo_rows, clean)
    spark.stop()


if __name__ == "__main__":
    main()