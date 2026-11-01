"""
spark_recommend.py
Spark batch job 2: product recommendation indicators.

  A. "Products frequently viewed together"  (co-views in sessions)
     support / confidence / lift for every product pair, plus a CHANCE BASELINE:
     how many pairs would reach each count if products were viewed at random?
  B. Category affinity ("categories bought together") with lift, from transactions.
  C. Fallback recommender: best-selling products of the same category.
Outputs: CSV files in results/.
"""
import math
import os

import pandas as pd
from pyspark.sql import functions as F

from spark_common import clean_all, get_spark

MIN_SUPPORT = 5   # a pair must occur in at least this many sessions to count as a signal


def pair_column(items):
    """All unordered pairs (a < b) inside one row's array, built WITHOUT a self-join."""
    arr = F.array_sort(F.array_distinct(items))
    return F.flatten(F.transform(arr, lambda a: F.transform(
        F.filter(arr, lambda b: b > a), lambda b: F.struct(a.alias("a"), b.alias("b")))))


def pair_stats(df, items, n_baskets):
    """Return (pair_counts, singles, stats) with support, confidence and lift."""
    pairs = df.select(F.explode(pair_column(items)).alias("p")).select("p.a", "p.b")
    pair_counts = pairs.groupBy("a", "b").count().withColumnRenamed("count", "c_ab").cache()
    singles = (df.select(F.explode(F.array_distinct(items)).alias("x"))
                 .groupBy("x").count().withColumnRenamed("count", "c").cache())
    sa = F.broadcast(singles.select(F.col("x").alias("a"), F.col("c").alias("c_a")))
    sb = F.broadcast(singles.select(F.col("x").alias("b"), F.col("c").alias("c_b")))
    stats = (pair_counts.join(sa, "a").join(sb, "b")
             .withColumn("support", F.col("c_ab") / F.lit(n_baskets))
             .withColumn("confidence_a_to_b", F.col("c_ab") / F.col("c_a"))
             .withColumn("confidence_b_to_a", F.col("c_ab") / F.col("c_b"))
             .withColumn("lift", F.col("c_ab") * F.lit(n_baskets) / (F.col("c_a") * F.col("c_b"))))
    return pair_counts, singles, stats


def poisson_expected(total_pair_occurrences, n_possible_pairs, ks):
    """Expected number of pairs seen exactly k times if pairs occurred at random."""
    lam = total_pair_occurrences / n_possible_pairs
    return {k: n_possible_pairs * math.exp(-lam) * lam ** k / math.factorial(k) for k in ks}


def main():
    spark = get_spark("recommendation-indicators")
    _, clean = clean_all(spark)
    sessions, transactions, products = clean["sessions"], clean["transactions"], clean["products"]
    os.makedirs("results", exist_ok=True)

    # ======================================================================
    # A. Products viewed together (sessions)
    # ======================================================================
    n_sessions = sessions.count()
    pair_counts, singles, stats = pair_stats(sessions, "viewed_products", n_sessions)
    pair_counts.count()                                   # materialize the cache once
    n_products_seen = singles.count()

    hist = {r["c_ab"]: r["count"] for r in
            pair_counts.groupBy("c_ab").count().collect()}
    total_occ = sum(k * v for k, v in hist.items())
    n_possible = n_products_seen * (n_products_seen - 1) / 2
    ks = list(range(1, 7))
    expected = poisson_expected(total_occ, n_possible, ks)

    print(f"\n=== A. Products viewed together: {n_sessions:,} sessions, "
          f"{n_products_seen:,} products, {total_occ:,} pair occurrences ===")
    base = pd.DataFrame({"times_pair_seen": ks,
                         "observed_pairs": [hist.get(k, 0) for k in ks],
                         "expected_if_random": [round(expected[k], 1) for k in ks]})
    print("\nChance baseline (observed vs expected if products were viewed at random):")
    print(base.to_string(index=False))
    base.to_csv("results/chance_baseline.csv", index=False)

    qualified = stats.filter(F.col("c_ab") >= MIN_SUPPORT)
    n_qualified = qualified.count()
    print(f"\nPairs with support >= {MIN_SUPPORT} sessions: {n_qualified}")
    top = (stats.orderBy(F.desc("c_ab"), F.desc("lift")).limit(15).toPandas())
    top = top.round({"support": 6, "confidence_a_to_b": 4, "confidence_b_to_a": 4, "lift": 1})
    top.to_csv("results/coview_top_pairs.csv", index=False)
    print("\nMost frequent co-viewed pairs (high lift here is just small-number luck):")
    print(top[["a", "b", "c_ab", "c_a", "c_b", "confidence_a_to_b", "lift"]].to_string(index=False))

    # ======================================================================
    # B. Category affinity (transactions)
    # ======================================================================
    prod_cat = products.select("product_id", "category_id", F.col("name").alias("product_name"))
    items = (transactions.select("transaction_id", F.explode("items").alias("it"))
             .select("transaction_id", F.col("it.product_id").alias("product_id"),
                     F.col("it.quantity").alias("quantity")))
    items_cat = items.join(prod_cat, "product_id")
    baskets = items_cat.groupBy("transaction_id").agg(F.collect_set("category_id").alias("cats"))
    n_baskets = baskets.count()
    _, _, cstats = pair_stats(baskets, "cats", n_baskets)
    cat_aff = cstats.toPandas().round(
        {"support": 6, "confidence_a_to_b": 4, "confidence_b_to_a": 4, "lift": 3})
    # In small baskets of DISTINCT categories a plain lift of 1.0 is not "no association",
    # so each pair's lift is compared with the typical (median) pair instead.
    median_lift = cat_aff["lift"].median()
    cat_aff["relative_lift"] = (cat_aff["lift"] / median_lift).round(3)
    cat_aff = cat_aff.sort_values("relative_lift", ascending=False)
    cat_aff.to_csv("results/category_affinity.csv", index=False)
    noise = 3 / math.sqrt(cat_aff["c_ab"].median())    # ~3 standard deviations of count noise
    print(f"\n=== B. Category affinity from {n_baskets:,} transactions: "
          f"{len(cat_aff)} category pairs ===")
    print(f"relative lift (1.0 = typical pair): min {cat_aff['relative_lift'].min():.3f} | "
          f"max {cat_aff['relative_lift'].max():.3f}")
    print(f"Random fluctuation alone explains about +/- {noise:.2f} "
          f"(3 / sqrt(median pair count {cat_aff['c_ab'].median():.0f}))")
    print("\nStrongest pairs:")
    print(cat_aff.head(5)[["a", "b", "c_ab", "lift", "relative_lift"]].to_string(index=False))

    # ======================================================================
    # C. Fallback recommender: best sellers of the same category
    # ======================================================================
    sold = (items_cat.groupBy("product_id", "product_name", "category_id")
            .agg(F.sum("quantity").alias("units_sold")))
    from pyspark.sql import Window
    w = Window.partitionBy("category_id").orderBy(F.desc("units_sold"), "product_id")
    best = (sold.withColumn("rank", F.row_number().over(w)).filter(F.col("rank") <= 4)
            .toPandas())
    best.to_csv("results/category_bestsellers.csv", index=False)

    print("\n=== C. Recommendations: 'customers viewing X also see the best sellers of its category' ===")
    sample = products.select("product_id", "name", "category_id").orderBy("product_id").limit(5).toPandas()
    for _, row in sample.iterrows():
        recs = best[(best.category_id == row.category_id) & (best.product_id != row.product_id)].head(3)
        print(f"\n{row.product_id} ({row['name']}, {row.category_id}) ->")
        for _, r in recs.iterrows():
            print(f"    {r.product_id}  {r.product_name}  ({int(r.units_sold)} units sold)")
    spark.stop()


if __name__ == "__main__":
    main()