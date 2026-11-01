"""
spark_clean.py
Spark batch job 1: clean and normalize the raw dataset and produce a data-quality report.
Output: results/data_quality_report.csv (and optionally cleaned Parquet files).
"""
import os

import pandas as pd
from pyspark.sql import functions as F

from spark_common import clean_all, get_spark

WRITE_PARQUET = False   # needs winutils on Windows; set True on Linux/Mac or a cluster


def flag_sums(df, flags):
    """One pass over the data: row count + number of rows where each flag is true."""
    exprs = [F.count("*").alias("rows")]
    exprs += [F.sum(F.col(f).cast("int")).alias(f) for f in flags]
    return df.agg(*exprs).collect()[0].asDict()


def main():
    spark = get_spark("clean-and-quality-report")
    raw, clean = clean_all(spark)
    report = []   # (table, check, rows_affected, rows_total)

    # --- Duplicates / invalid keys removed (raw count vs clean count) ----------
    for name in ["users", "products", "transactions", "sessions"]:
        raw_n, clean_n = raw[name].count(), clean[name].count()
        report.append((name, "rows removed (duplicate or missing key)", raw_n - clean_n, raw_n))

    # --- Session flags (single pass) -------------------------------------------
    s = flag_sums(clean["sessions"], ["cart_page_without_items", "checkout_without_cart",
                                      "converted_without_txn", "duration_mismatch", "geo_suspect"])
    labels = {"cart_page_without_items": "visited cart page but cart is empty",
              "checkout_without_cart": "reached checkout/confirmation with empty cart",
              "converted_without_txn": "marked 'converted' but no transaction found",
              "duration_mismatch": "duration_seconds does not match end - start",
              "geo_suspect": "country is not US although state is a US state code"}
    for k, label in labels.items():
        report.append(("sessions", label, s[k] or 0, s["rows"]))

    # --- Transaction flags -----------------------------------------------------
    t = flag_sums(clean["transactions"], ["subtotal_mismatch", "total_mismatch"])
    report.append(("transactions", "sum of items != subtotal", t["subtotal_mismatch"] or 0, t["rows"]))
    report.append(("transactions", "subtotal - discount != total", t["total_mismatch"] or 0, t["rows"]))
    unlinked = clean["transactions"].filter(~F.col("is_linked_to_session")).count()
    report.append(("transactions", "no session link (session_id is null)", unlinked, t["rows"]))

    # --- Referential integrity (orphans) --------------------------------------
    items = clean["transactions"].select(F.explode("items").alias("it"))
    orphan_items = items.join(clean["products"].select(F.col("product_id").alias("pid")),
                              F.col("it.product_id") == F.col("pid"), "left_anti").count()
    report.append(("transactions", "line items pointing to unknown product", orphan_items,
                   items.count()))
    orphan_users = clean["sessions"].join(clean["users"].select("user_id"), "user_id", "left_anti").count()
    report.append(("sessions", "sessions of unknown user", orphan_users, s["rows"]))

    # --- Users: last_active vs purchases --------------------------------------
    last_txn = clean["transactions"].groupBy("user_id").agg(F.max("timestamp").alias("last_txn"))
    u = (clean["users"].join(last_txn, "user_id", "left")
         .withColumn("last_active_before_purchase", F.col("last_active") < F.col("last_txn")))
    uf = flag_sums(u, ["last_active_before_purchase", "geo_suspect"])
    report.append(("users", "last_active is earlier than the user's last purchase",
                   uf["last_active_before_purchase"] or 0, uf["rows"]))
    report.append(("users", "country is not US although state is a US state code",
                   uf["geo_suspect"] or 0, uf["rows"]))

    # --- Products --------------------------------------------------------------
    p = flag_sums(clean["products"], ["out_of_stock"])
    report.append(("products", "out of stock", p["out_of_stock"] or 0, p["rows"]))

    # --- Print and save the report --------------------------------------------
    df = pd.DataFrame(report, columns=["table", "check", "rows_affected", "rows_total"])
    df["percent"] = (100 * df["rows_affected"] / df["rows_total"]).round(2)
    os.makedirs("results", exist_ok=True)
    df.to_csv("results/data_quality_report.csv", index=False)
    print("\n=== DATA QUALITY REPORT ===")
    print(df.to_string(index=False))

    print("\n=== Cleaned sessions: schema ===")
    clean["sessions"].printSchema()
    print("=== Cleaned sessions: sample ===")
    (clean["sessions"].select("session_id", "start_time", "referrer", "conversion_status",
                              "n_page_views", "n_cart_items", "has_transaction")
     .show(5, truncate=False))

    if WRITE_PARQUET:
        for name, frame in clean.items():
            try:
                frame.write.mode("overwrite").parquet(f"clean/{name}")
                print(f"wrote clean/{name}")
            except Exception as exc:
                print(f"could not write {name}: {exc}")
    spark.stop()


if __name__ == "__main__":
    main()