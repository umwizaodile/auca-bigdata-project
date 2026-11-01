"""
spark_sql.py
Spark batch job 3: Spark SQL analytics.

The cleaned DataFrames are registered as SQL views NAMED AFTER WHERE THE DATA WOULD LIVE
in the full architecture (mongo_* = MongoDB collections, hbase_* = HBase tables), then
joined with plain SQL. This shows the query logic of the cross-system analytics without
needing a connector (see spark_integrated.py for the version that reads the real databases).

  Q1  Monthly revenue and growth             (CTE + LAG window)
  Q2  Top 3 products per category            (RANK + SUM OVER windows)
  Q3  Spend vs engagement quartiles          (MongoDB users JOIN HBase sessions, NTILE)
  Q4  Inventory risk of active products      (3-way join: MongoDB + HBase)
  Q5  Cohort analysis by registration month  (cohort x purchase-month matrix)
"""
import os

import pandas as pd

from spark_common import clean_all, get_spark


def build_views(spark, clean):
    """Register the cleaned data as SQL views, named after the system that would hold it."""
    clean["products"].createOrReplaceTempView("mongo_products")
    clean["transactions"].createOrReplaceTempView("mongo_transactions")
    clean["users"].createOrReplaceTempView("users_clean")
    clean["sessions"].createOrReplaceTempView("sessions_clean")

    # MongoDB: one document per line item (the $unwind of the embedded items array)
    spark.sql("""
        SELECT t.transaction_id, t.user_id, t.timestamp, i.product_id,
               i.quantity, i.unit_price, i.subtotal
        FROM mongo_transactions t LATERAL VIEW explode(t.items) x AS i
    """).createOrReplaceTempView("mongo_items")

    # MongoDB: users with the computed purchase summary (same idea as users.summary)
    spark.sql("""
        SELECT u.user_id, u.registration_month,
               COALESCE(t.orders, 0) AS orders, COALESCE(t.spent, 0.0) AS spent
        FROM users_clean u
        LEFT JOIN (SELECT user_id, COUNT(*) AS orders, SUM(total) AS spent
                   FROM mongo_transactions GROUP BY user_id) t ON u.user_id = t.user_id
    """).createOrReplaceTempView("mongo_users")

    # HBase user_sessions: narrow projection (the 'info' family), cached because it is reused
    sess = spark.sql("""
        SELECT user_id, session_id, start_time, duration_seconds, n_page_views, n_cart_items,
               conversion_status, referrer, device_type FROM sessions_clean
    """).cache()
    sess.createOrReplaceTempView("hbase_user_sessions")

    # HBase product_daily: views and cart additions per product per day (full data)
    daily = spark.sql("""
        WITH v AS (
            SELECT pv.product_id, to_date(pv.timestamp) AS day, COUNT(*) AS views
            FROM sessions_clean LATERAL VIEW explode(page_views) x AS pv
            WHERE pv.page_type = 'product_detail' AND pv.product_id IS NOT NULL
            GROUP BY pv.product_id, to_date(pv.timestamp)),
        c AS (
            SELECT cart_key AS product_id, to_date(start_time) AS day,
                   SUM(cart_val.quantity) AS cart_adds
            FROM sessions_clean LATERAL VIEW explode(cart_contents) y AS cart_key, cart_val
            GROUP BY cart_key, to_date(start_time))
        SELECT COALESCE(v.product_id, c.product_id) AS product_id,
               COALESCE(v.day, c.day) AS day,
               COALESCE(v.views, 0) AS views, COALESCE(c.cart_adds, 0) AS cart_adds
        FROM v FULL OUTER JOIN c ON v.product_id = c.product_id AND v.day = c.day
    """).cache()
    daily.createOrReplaceTempView("hbase_product_daily")
    print(f"hbase_user_sessions: {sess.count():,} rows | hbase_product_daily: {daily.count():,} rows")


QUERIES = {
    "monthly_revenue": """
        WITH monthly AS (
            SELECT date_format(timestamp, 'yyyy-MM') AS month,
                   COUNT(*) AS orders,
                   COUNT(DISTINCT to_date(timestamp)) AS days_with_data,
                   ROUND(SUM(total), 2) AS revenue,
                   ROUND(AVG(total), 2) AS avg_order_value
            FROM mongo_transactions
            GROUP BY date_format(timestamp, 'yyyy-MM'))
        SELECT month, orders, days_with_data, revenue, avg_order_value,
               ROUND(revenue / days_with_data, 2) AS revenue_per_day,
               ROUND(100 * (revenue / days_with_data
                     - LAG(revenue / days_with_data) OVER (ORDER BY month))
                     / LAG(revenue / days_with_data) OVER (ORDER BY month), 1) AS per_day_growth_pct
        FROM monthly ORDER BY month""",

    "top_products_per_category": """
        WITH product_revenue AS (
            SELECT p.category_id, p.category_name, i.product_id, p.name AS product_name,
                   SUM(i.quantity) AS units, ROUND(SUM(i.subtotal), 2) AS revenue
            FROM mongo_items i JOIN mongo_products p ON i.product_id = p.product_id
            GROUP BY p.category_id, p.category_name, i.product_id, p.name),
        ranked AS (
            SELECT *, RANK() OVER (PARTITION BY category_id ORDER BY revenue DESC) AS rnk,
                   ROUND(100 * revenue / SUM(revenue) OVER (PARTITION BY category_id), 2)
                       AS pct_of_category
            FROM product_revenue)
        SELECT category_id, category_name, rnk, product_id, product_name, units, revenue,
               pct_of_category
        FROM ranked WHERE rnk <= 3 ORDER BY category_id, rnk""",

    "engagement_vs_spend": """
        WITH sess AS (
            SELECT user_id, COUNT(*) AS sessions,
                   AVG(duration_seconds) AS avg_duration, AVG(n_page_views) AS avg_pages
            FROM hbase_user_sessions GROUP BY user_id),
        joined AS (
            SELECT u.user_id, u.orders, u.spent, COALESCE(s.sessions, 0) AS sessions,
                   s.avg_duration, s.avg_pages
            FROM mongo_users u LEFT JOIN sess s ON u.user_id = s.user_id),
        quartiles AS (SELECT *, NTILE(4) OVER (ORDER BY spent) AS spend_quartile FROM joined)
        SELECT spend_quartile, COUNT(*) AS users,
               ROUND(MIN(spent), 0) AS min_spent, ROUND(MAX(spent), 0) AS max_spent,
               ROUND(AVG(spent), 0) AS avg_spent, ROUND(AVG(orders), 1) AS avg_orders,
               ROUND(AVG(sessions), 1) AS avg_sessions,
               ROUND(AVG(avg_duration), 0) AS avg_session_seconds,
               ROUND(AVG(spent / NULLIF(sessions, 0)), 1) AS spend_per_session
        FROM quartiles GROUP BY spend_quartile ORDER BY spend_quartile""",

    "inventory_risk": """
        WITH span AS (
            SELECT DATEDIFF(MAX(to_date(timestamp)), MIN(to_date(timestamp))) + 1 AS days
            FROM mongo_transactions),
        velocity AS (
            SELECT product_id, SUM(quantity) AS units_sold,
                   SUM(quantity) / (SELECT days FROM span) AS units_per_day
            FROM mongo_items GROUP BY product_id),
        demand AS (
            SELECT product_id, SUM(views) AS views, SUM(cart_adds) AS cart_adds
            FROM hbase_product_daily GROUP BY product_id)
        SELECT p.product_id, p.name, p.category_name, p.current_stock, p.is_active,
               v.units_sold, ROUND(v.units_per_day, 2) AS units_per_day,
               ROUND(p.current_stock / v.units_per_day, 1) AS days_of_stock_left,
               d.views, d.cart_adds
        FROM mongo_products p
        JOIN velocity v ON p.product_id = v.product_id
        LEFT JOIN demand d ON p.product_id = d.product_id
        WHERE p.is_active = true
        ORDER BY days_of_stock_left ASC, v.units_per_day DESC
        LIMIT 10""",

    "cohort_matrix": """
        SELECT u.registration_month AS cohort, date_format(t.timestamp, 'yyyy-MM') AS purchase_month,
               COUNT(DISTINCT t.user_id) AS active_buyers, ROUND(SUM(t.total), 2) AS revenue
        FROM mongo_transactions t JOIN mongo_users u ON t.user_id = u.user_id
        GROUP BY u.registration_month, date_format(t.timestamp, 'yyyy-MM')
        ORDER BY cohort, purchase_month""",
}


def main():
    spark = get_spark("spark-sql-analytics")
    _, clean = clean_all(spark)
    build_views(spark, clean)
    os.makedirs("results", exist_ok=True)
    pd.set_option("display.width", 220)
    pd.set_option("display.max_columns", 30)

    results = {}
    for name, sql in QUERIES.items():
        results[name] = spark.sql(sql).toPandas()
        results[name].to_csv(f"results/sql_{name}.csv", index=False)

    print("\n=== Q1 Monthly revenue and per-day growth (first/last months are partial) ===")
    print(results["monthly_revenue"].to_string(index=False))

    print("\n=== Q2 Top 3 products per category by revenue (first 12 of 75 rows) ===")
    print(results["top_products_per_category"].head(12).to_string(index=False))

    print("\n=== Q3 Spend quartiles: do bigger spenders engage more? (MongoDB users + HBase sessions) ===")
    print(results["engagement_vs_spend"].to_string(index=False))

    print("\n=== Q4 Active products closest to running out (stock / daily sales rate) ===")
    print(results["inventory_risk"].to_string(index=False))

    print("\n=== Q5 Cohort analysis: revenue per cohort user, by purchase month ===")
    m = results["cohort_matrix"]
    cohort_size = (spark.sql("SELECT registration_month AS cohort, COUNT(*) AS size "
                             "FROM mongo_users GROUP BY registration_month").toPandas()
                   .set_index("cohort")["size"])
    m["revenue_per_user"] = (m["revenue"] / m["cohort"].map(cohort_size)).round(2)
    matrix = m.pivot(index="cohort", columns="purchase_month", values="revenue_per_user")
    matrix.insert(0, "cohort_users", cohort_size)
    print(matrix.fillna(0).to_string())
    matrix.to_csv("results/sql_cohort_revenue_per_user.csv")
    spark.stop()


if __name__ == "__main__":
    main()