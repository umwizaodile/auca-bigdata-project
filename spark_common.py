"""
spark_common.py
Shared Spark code for the project: session builder, explicit schemas, and the
cleaning functions. Every Spark job imports clean_all() so that all analyses
work on exactly the same cleaned data.
"""
import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql import types as T

DATA_DIR = "data"


def get_spark(app_name="ecommerce-analytics"):
    """Local Spark session using all CPU cores (same code would run on a cluster)."""
    os.environ["PYSPARK_PYTHON"] = sys.executable          # workers use this venv's Python
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (SparkSession.builder.master("local[*]").appName(app_name)
             .config("spark.driver.memory", "3g")            # fits an 8 GB laptop
             .config("spark.sql.shuffle.partitions", "8")    # default 200 is far too many locally
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


# ---------------------------------------------------------------------------
# Explicit schemas. Better than inference: no extra pass over the data, and
# cart_contents (keys are product ids) is read as a MAP instead of creating
# thousands of struct fields.
# ---------------------------------------------------------------------------
GEO = T.StructType([
    T.StructField("city", T.StringType()), T.StructField("state", T.StringType()),
    T.StructField("country", T.StringType()), T.StructField("ip_address", T.StringType())])

CATEGORY_SCHEMA = T.StructType([
    T.StructField("category_id", T.StringType()), T.StructField("name", T.StringType()),
    T.StructField("subcategories", T.ArrayType(T.StructType([
        T.StructField("subcategory_id", T.StringType()), T.StructField("name", T.StringType()),
        T.StructField("profit_margin", T.DoubleType())])))])

PRODUCT_SCHEMA = T.StructType([
    T.StructField("product_id", T.StringType()), T.StructField("name", T.StringType()),
    T.StructField("category_id", T.StringType()), T.StructField("base_price", T.DoubleType()),
    T.StructField("current_stock", T.IntegerType()), T.StructField("is_active", T.BooleanType()),
    T.StructField("price_history", T.ArrayType(T.StructType([
        T.StructField("price", T.DoubleType()), T.StructField("date", T.StringType())]))),
    T.StructField("creation_date", T.StringType())])

USER_SCHEMA = T.StructType([
    T.StructField("user_id", T.StringType()), T.StructField("geo_data", GEO),
    T.StructField("registration_date", T.StringType()), T.StructField("last_active", T.StringType())])

TRANSACTION_SCHEMA = T.StructType([
    T.StructField("transaction_id", T.StringType()), T.StructField("session_id", T.StringType()),
    T.StructField("user_id", T.StringType()), T.StructField("timestamp", T.StringType()),
    T.StructField("items", T.ArrayType(T.StructType([
        T.StructField("product_id", T.StringType()), T.StructField("quantity", T.IntegerType()),
        T.StructField("unit_price", T.DoubleType()), T.StructField("subtotal", T.DoubleType())]))),
    T.StructField("subtotal", T.DoubleType()), T.StructField("discount", T.DoubleType()),
    T.StructField("total", T.DoubleType()), T.StructField("payment_method", T.StringType()),
    T.StructField("status", T.StringType())])

SESSION_SCHEMA = T.StructType([
    T.StructField("session_id", T.StringType()), T.StructField("user_id", T.StringType()),
    T.StructField("start_time", T.StringType()), T.StructField("end_time", T.StringType()),
    T.StructField("duration_seconds", T.IntegerType()), T.StructField("geo_data", GEO),
    T.StructField("device_profile", T.StructType([
        T.StructField("type", T.StringType()), T.StructField("os", T.StringType()),
        T.StructField("browser", T.StringType())])),
    T.StructField("viewed_products", T.ArrayType(T.StringType())),
    T.StructField("page_views", T.ArrayType(T.StructType([
        T.StructField("timestamp", T.StringType()), T.StructField("page_type", T.StringType()),
        T.StructField("product_id", T.StringType()), T.StructField("category_id", T.StringType()),
        T.StructField("view_duration", T.IntegerType())]))),
    T.StructField("cart_contents", T.MapType(T.StringType(), T.StructType([
        T.StructField("quantity", T.IntegerType()), T.StructField("price", T.DoubleType())]))),
    T.StructField("conversion_status", T.StringType()), T.StructField("referrer", T.StringType())])


def read_raw(spark):
    """Read the JSON Lines files produced by convert_to_jsonl.py."""
    def path(name):
        full = f"{DATA_DIR}/{name}"
        if "*" not in name and not os.path.exists(full):
            sys.exit(f"ERROR: {full} not found. Run convert_to_jsonl.py first.")
        return full
    return {
        "categories": spark.read.schema(CATEGORY_SCHEMA).json(path("categories.jsonl")),
        "products": spark.read.schema(PRODUCT_SCHEMA).json(path("products.jsonl")),
        "users": spark.read.schema(USER_SCHEMA).json(path("users.jsonl")),
        "transactions": spark.read.schema(TRANSACTION_SCHEMA).json(path("transactions.jsonl")),
        "sessions": spark.read.schema(SESSION_SCHEMA).json(path("sessions_*.jsonl")),
    }


# ---------------------------------------------------------------------------
# Cleaning functions: parse types, normalize text, remove duplicates and
# invalid keys, and ADD FLAG COLUMNS for data-quality problems (flag, don't delete).
# ---------------------------------------------------------------------------
def _norm(col):
    """Trim and lower-case a text column; empty strings become NULL."""
    c = F.lower(F.trim(F.col(col)))
    return F.when(c == "", None).otherwise(c)


def clean_transactions(df):
    df = (df.filter(F.col("transaction_id").isNotNull() & F.col("user_id").isNotNull())
            .dropDuplicates(["transaction_id"])
            .withColumn("timestamp", F.to_timestamp("timestamp"))
            .withColumn("payment_method", F.coalesce(_norm("payment_method"), F.lit("unknown")))
            .withColumn("status", F.coalesce(_norm("status"), F.lit("unknown")))
            .withColumn("items_subtotal",
                        F.aggregate("items", F.lit(0.0), lambda acc, x: acc + x["subtotal"]))
            .withColumn("item_count",
                        F.aggregate("items", F.lit(0), lambda acc, x: acc + x["quantity"]))
            .withColumn("is_linked_to_session", F.col("session_id").isNotNull())
            .withColumn("subtotal_mismatch", F.abs(F.col("items_subtotal") - F.col("subtotal")) > 0.05)
            .withColumn("total_mismatch",
                        F.abs(F.col("subtotal") - F.col("discount") - F.col("total")) > 0.05))
    return df


def clean_sessions(df, transactions):
    pv = F.transform("page_views", lambda x: F.struct(
        F.to_timestamp(x["timestamp"]).alias("timestamp"),
        F.lower(F.trim(x["page_type"])).alias("page_type"),
        x["product_id"].alias("product_id"),
        x["category_id"].alias("category_id"),
        x["view_duration"].alias("view_duration")))
    txn_sessions = (transactions.filter(F.col("session_id").isNotNull())
                    .select("session_id").distinct().withColumn("has_transaction", F.lit(True)))
    df = (df.filter(F.col("session_id").isNotNull() & F.col("user_id").isNotNull())
            .dropDuplicates(["session_id"])
            .withColumn("start_time", F.to_timestamp("start_time"))
            .withColumn("end_time", F.to_timestamp("end_time"))
            .withColumn("page_views", pv)
            .withColumn("referrer", F.coalesce(_norm("referrer"), F.lit("unknown")))
            .withColumn("conversion_status", F.coalesce(_norm("conversion_status"), F.lit("unknown")))
            .withColumn("device_type", F.coalesce(_norm("device_profile.type"), F.lit("unknown")))
            .withColumn("country", F.upper(F.trim(F.col("geo_data.country"))))
            .withColumn("n_page_views", F.size("page_views"))
            .withColumn("n_cart_items", F.size("cart_contents"))
            .withColumn("has_cart", F.col("n_cart_items") > 0)
            .withColumn("visited_cart_page", F.exists("page_views", lambda x: x["page_type"] == "cart"))
            .withColumn("visited_checkout", F.exists("page_views", lambda x: x["page_type"] == "checkout"))
            .withColumn("visited_confirmation",
                        F.exists("page_views", lambda x: x["page_type"] == "confirmation"))
            .withColumn("n_product_views",
                        F.size(F.filter("page_views", lambda x: x["page_type"] == "product_detail")))
            .join(txn_sessions, "session_id", "left")
            .withColumn("has_transaction", F.coalesce(F.col("has_transaction"), F.lit(False)))
            # ---- data-quality flags ----
            .withColumn("cart_page_without_items", F.col("visited_cart_page") & ~F.col("has_cart"))
            .withColumn("checkout_without_cart",
                        (F.col("visited_checkout") | F.col("visited_confirmation")) & ~F.col("has_cart"))
            .withColumn("converted_without_txn",
                        (F.col("conversion_status") == "converted") & ~F.col("has_transaction"))
            .withColumn("duration_mismatch",
                        F.abs((F.unix_timestamp("end_time") - F.unix_timestamp("start_time"))
                              - F.col("duration_seconds")) > 1)
            .withColumn("geo_suspect", F.col("country") != "US"))   # state codes are US states
    return df


def clean_users(df):
    return (df.filter(F.col("user_id").isNotNull())
              .dropDuplicates(["user_id"])
              .withColumn("registration_date", F.to_timestamp("registration_date"))
              .withColumn("last_active", F.to_timestamp("last_active"))
              .withColumn("country", F.coalesce(F.upper(F.trim(F.col("geo_data.country"))), F.lit("UNKNOWN")))
              .withColumn("state", F.coalesce(F.upper(F.trim(F.col("geo_data.state"))), F.lit("UNKNOWN")))
              .withColumn("registration_month", F.date_format("registration_date", "yyyy-MM"))
              .withColumn("geo_suspect", F.col("country") != "US"))


def clean_products(df, categories):
    cat = categories.select("category_id", F.col("name").alias("category_name"))
    return (df.filter(F.col("product_id").isNotNull())
              .dropDuplicates(["product_id"])
              .withColumn("creation_date", F.to_timestamp("creation_date"))
              .withColumn("price_history", F.transform("price_history", lambda x: F.struct(
                  x["price"].alias("price"), F.to_timestamp(x["date"]).alias("date"))))
              .withColumn("current_price",
                          F.coalesce(F.element_at("price_history", -1)["price"], F.col("base_price")))
              .withColumn("out_of_stock", F.col("current_stock") == 0)
              .join(cat, "category_id", "left"))


def clean_all(spark):
    """Return the raw and the cleaned DataFrames."""
    raw = read_raw(spark)
    categories = raw["categories"].dropDuplicates(["category_id"])
    transactions = clean_transactions(raw["transactions"])
    clean = {
        "categories": categories,
        "transactions": transactions,
        "sessions": clean_sessions(raw["sessions"], transactions),
        "users": clean_users(raw["users"]),
        "products": clean_products(raw["products"], categories),
    }
    return raw, clean